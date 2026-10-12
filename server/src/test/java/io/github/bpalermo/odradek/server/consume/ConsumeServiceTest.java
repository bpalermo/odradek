package io.github.bpalermo.odradek.server.consume;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.Duration;
import com.google.protobuf.util.Durations;
import io.github.bpalermo.odradek.config.v1.Cluster;
import io.github.bpalermo.odradek.config.v1.Codec;
import io.github.bpalermo.odradek.config.v1.Consume;
import io.github.bpalermo.odradek.config.v1.Consumer;
import io.github.bpalermo.odradek.config.v1.OffsetReset;
import io.github.bpalermo.odradek.config.v1.OnDecodeError;
import io.github.bpalermo.odradek.config.v1.Pipeline;
import io.github.bpalermo.odradek.config.v1.ServerConfig;
import io.github.bpalermo.odradek.config.v1.Transform;
import io.github.bpalermo.odradek.kafka.v1.Ack;
import io.github.bpalermo.odradek.kafka.v1.ConsumeRequest;
import io.github.bpalermo.odradek.kafka.v1.ConsumeResponse;
import io.github.bpalermo.odradek.kafka.v1.ConsumedRecord;
import io.github.bpalermo.odradek.kafka.v1.Credits;
import io.github.bpalermo.odradek.kafka.v1.OdradekServiceGrpc;
import io.github.bpalermo.odradek.kafka.v1.RecordBatch;
import io.github.bpalermo.odradek.kafka.v1.RevokeDone;
import io.github.bpalermo.odradek.kafka.v1.Subscribe;
import io.github.bpalermo.odradek.server.codec.CodecRegistry;
import io.github.bpalermo.odradek.server.codec.ContractRegistry;
import io.github.bpalermo.odradek.server.produce.SuffixTransform;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** The Consume RPC over a real gRPC stream (in-process transport), against a mock consumer. */
class ConsumeServiceTest {
  private static final TopicPartition P0 = new TopicPartition("orders", 0);
  private static final TopicPartition P1 = new TopicPartition("orders", 1);

  /**
   * Remembers commits on the side: the mock's own committed() cannot be asked after close, for a
   * partition no longer assigned, or while the poll thread is inside poll().
   */
  private static final class RecordingConsumer extends MockConsumer<byte[], byte[]> {
    final Map<TopicPartition, Long> commits = new ConcurrentHashMap<>();

    RecordingConsumer() {
      super("earliest");
    }

    private void record(Map<TopicPartition, OffsetAndMetadata> offsets) {
      offsets.forEach((partition, offset) -> commits.put(partition, offset.offset()));
    }

    @Override
    public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
      record(offsets);
      super.commitSync(offsets);
    }

    @Override
    public synchronized void commitAsync(
        Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
      record(offsets);
      super.commitAsync(offsets, callback);
    }
  }

  private final RecordingConsumer kafka = new RecordingConsumer();
  private Map<String, Object> consumerProperties;
  private CodecRegistry codecs;
  private Server server;
  private ManagedChannel channel;
  private ConsumeService service;

  /** The app's end of one Consume stream. */
  private static final class AppStream {
    final BlockingQueue<ConsumeResponse> responses = new LinkedBlockingQueue<>();
    final CompletableFuture<Status> closed = new CompletableFuture<>();
    StreamObserver<ConsumeRequest> requests;

    void subscribe(String... topics) {
      requests.onNext(
          ConsumeRequest.newBuilder()
              .setSubscribe(Subscribe.newBuilder().addAllTopics(List.of(topics)))
              .build());
    }

    void credits(int records) {
      requests.onNext(
          ConsumeRequest.newBuilder().setCredits(Credits.newBuilder().setRecords(records)).build());
    }

    void ack(TopicPartition partition, long epoch, long first, long last) {
      requests.onNext(
          ConsumeRequest.newBuilder()
              .setAck(
                  Ack.newBuilder()
                      .setTopic(partition.topic())
                      .setPartition(partition.partition())
                      .setAssignmentEpoch(epoch)
                      .setFirstOffset(first)
                      .setLastOffset(last))
              .build());
    }

    void revokeDone(long id) {
      requests.onNext(
          ConsumeRequest.newBuilder()
              .setRevokeDone(RevokeDone.newBuilder().setRevokeId(id))
              .build());
    }

    ConsumeResponse next() throws InterruptedException {
      ConsumeResponse response = responses.poll(10, TimeUnit.SECONDS);
      assertThat(response).as("a response within 10s").isNotNull();
      return response;
    }

    RecordBatch batch() throws InterruptedException {
      ConsumeResponse response = next();
      assertThat(response.hasRecords()).as("%s", response).isTrue();
      return response.getRecords();
    }

    void nothing() throws InterruptedException {
      assertThat(responses.poll(200, TimeUnit.MILLISECONDS)).isNull();
    }

    Status status() throws Exception {
      return closed.get(10, TimeUnit.SECONDS);
    }
  }

  private static Duration millis(long ms) {
    return Durations.fromMillis(ms);
  }

  private static Consumer.Builder consumer(String group) {
    return Consumer.newBuilder().setCluster("main").setGroupId(group);
  }

  private static ServerConfig.Builder config() {
    return ServerConfig.newBuilder()
        .putClusters("main", Cluster.getDefaultInstance())
        .putConsumers("orders", consumer("billing").build())
        .putConsumers("refunds", consumer("billing").build())
        .putConsumers("audit", consumer("auditors").build())
        .setConsume(
            Consume.newBuilder().setCommitInterval(millis(20)).setRevokeDeadline(millis(5000)));
  }

  private void start(ServerConfig.Builder builder) throws Exception {
    start(builder, System::nanoTime);
  }

  private void start(ServerConfig.Builder builder, LongSupplier clock) throws Exception {
    ServerConfig config = builder.build();
    codecs = CodecRegistry.create(config, ContractRegistry.empty(), getClass().getClassLoader());
    service =
        new ConsumeService(
            config,
            codecs,
            "epoch-1",
            (cluster, properties) -> {
              consumerProperties = properties;
              return kafka;
            },
            clock);
    String name = InProcessServerBuilder.generateName();
    server =
        InProcessServerBuilder.forName(name)
            .addService(
                new OdradekServiceGrpc.OdradekServiceImplBase() {
                  @Override
                  public StreamObserver<ConsumeRequest> consume(
                      StreamObserver<ConsumeResponse> responses) {
                    return service.consume(responses);
                  }
                })
            .build()
            .start();
    channel = InProcessChannelBuilder.forName(name).build();
  }

  @AfterEach
  void stop() throws InterruptedException {
    if (channel != null) {
      channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
      server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
      service.shutdown(java.time.Duration.ofSeconds(5));
      codecs.close();
    }
  }

  private AppStream stream() {
    AppStream app = new AppStream();
    app.requests =
        OdradekServiceGrpc.newStub(channel)
            .consume(
                new StreamObserver<ConsumeResponse>() {
                  @Override
                  public void onNext(ConsumeResponse response) {
                    app.responses.add(response);
                  }

                  @Override
                  public void onError(Throwable t) {
                    app.closed.complete(Status.fromThrowable(t));
                  }

                  @Override
                  public void onCompleted() {
                    app.closed.complete(Status.OK);
                  }
                });
    return app;
  }

  /** Subscribes to "orders" and consumes the handshake reply. */
  private AppStream subscribed() throws InterruptedException {
    AppStream app = stream();
    app.subscribe("orders");
    assertThat(app.next().hasSubscribed()).isTrue();
    return app;
  }

  /** Gives the consumer these partitions, each starting at offset 0, on its poll thread. */
  private void assign(TopicPartition... partitions) {
    kafka.schedulePollTask(
        () -> {
          for (TopicPartition partition : partitions) {
            kafka.updateBeginningOffsets(Map.of(partition, 0L));
            kafka.updateEndOffsets(Map.of(partition, 0L));
          }
          kafka.rebalance(List.of(partitions));
        });
  }

  private void add(TopicPartition partition, long offset, String value) {
    kafka.schedulePollTask(
        () ->
            kafka.addRecord(
                new ConsumerRecord<>(
                    partition.topic(),
                    partition.partition(),
                    offset,
                    ("k" + offset).getBytes(StandardCharsets.UTF_8),
                    value.getBytes(StandardCharsets.UTF_8))));
  }

  private static void eventually(String what, BooleanSupplier condition)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
      Thread.sleep(5);
    }
    assertThat(condition.getAsBoolean()).as(what).isTrue();
  }

  private long committed(TopicPartition partition) {
    return kafka.commits.getOrDefault(partition, -1L);
  }

  private static List<String> values(RecordBatch batch) {
    return batch.getRecordsList().stream().map(r -> r.getValue().toStringUtf8()).toList();
  }

  @Test
  void handshakeReportsTheResolvedClusterGroupAndDeadline() throws Exception {
    start(
        config()
            .setConsume(
                Consume.newBuilder()
                    .setDefaultMaxProcessing(millis(60_000))
                    .setMaxMaxProcessing(millis(120_000))));
    AppStream app = stream();
    app.subscribe("orders", "refunds");
    var subscribed = app.next().getSubscribed();
    assertThat(subscribed.getServerEpoch()).isEqualTo("epoch-1");
    assertThat(subscribed.getCluster()).isEqualTo("main");
    assertThat(subscribed.getGroupId()).isEqualTo("billing");
    assertThat(subscribed.getMaxProcessingMs()).isEqualTo(60_000);
    eventually(
        "subscribed", () -> kafka.subscription().equals(java.util.Set.of("orders", "refunds")));
  }

  @Test
  void requestedDeadlineIsClampedToTheMaximum() throws Exception {
    start(config().setConsume(Consume.newBuilder().setMaxMaxProcessing(millis(120_000))));
    AppStream app = stream();
    app.requests.onNext(
        ConsumeRequest.newBuilder()
            .setSubscribe(Subscribe.newBuilder().addTopics("orders").setMaxProcessingMs(999_999))
            .build());
    assertThat(app.next().getSubscribed().getMaxProcessingMs()).isEqualTo(120_000);
  }

  @Test
  void consumerPropertiesAlwaysCarryTheGuarantees() throws Exception {
    start(config());
    subscribed();
    assertThat(consumerProperties)
        .containsEntry("group.id", "billing")
        .containsEntry("enable.auto.commit", "false")
        .containsEntry("auto.offset.reset", "none")
        .containsEntry(
            "partition.assignment.strategy",
            "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
    // Default deadline 5m, plus the 5s revoke deadline of this test, plus the margin.
    assertThat(Long.parseLong(consumerProperties.get("max.poll.interval.ms").toString()))
        .isEqualTo(300_000 + 5_000 + 30_000);
  }

  @Test
  void unknownTopicIsNotFound() throws Exception {
    start(config());
    AppStream app = stream();
    app.subscribe("orders", "nowhere");
    assertThat(app.status().getCode()).isEqualTo(Status.Code.NOT_FOUND);
  }

  @Test
  void topicsOfDifferentGroupsCannotShareAStream() throws Exception {
    start(config());
    AppStream app = stream();
    app.subscribe("orders", "audit");
    assertThat(app.status().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
  }

  @Test
  void firstMessageMustBeSubscribeWithATopic() throws Exception {
    start(config());
    AppStream noHandshake = stream();
    noHandshake.credits(1);
    assertThat(noHandshake.status().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);

    AppStream noTopics = stream();
    noTopics.subscribe();
    assertThat(noTopics.status().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
  }

  @Test
  void nothingIsDeliveredWithoutCredits() throws Exception {
    start(config());
    AppStream app = subscribed();
    assign(P0);
    var assigned = app.next().getAssigned();
    assertThat(assigned.getPartitionsList())
        .singleElement()
        .satisfies(
            p -> {
              assertThat(p.getTopic()).isEqualTo("orders");
              assertThat(p.getPartition()).isZero();
              assertThat(p.getAssignmentEpoch()).isPositive();
            });
    eventually("paused", () -> kafka.paused().contains(P0));
    add(P0, 0, "a");
    app.nothing();

    app.credits(10);
    RecordBatch batch = app.batch();
    assertThat(batch.getTopic()).isEqualTo("orders");
    assertThat(batch.getAssignmentEpoch())
        .isEqualTo(assigned.getPartitions(0).getAssignmentEpoch());
    ConsumedRecord record = batch.getRecords(0);
    assertThat(record.getOffset()).isZero();
    assertThat(record.getKey().toStringUtf8()).isEqualTo("k0");
    assertThat(record.getValue().toStringUtf8()).isEqualTo("a");
    assertThat(record.hasDecodeError()).isFalse();
  }

  @Test
  void creditsBoundDeliveryAndTheConsumerIsRewoundOverTheRest() throws Exception {
    start(config());
    AppStream app = subscribed();
    assign(P0);
    app.next();
    app.credits(2);
    kafka.schedulePollTask(
        () -> {
          for (int i = 0; i < 5; i++) {
            kafka.addRecord(
                new ConsumerRecord<>("orders", 0, i, new byte[0], ("v" + i).getBytes()));
          }
        });
    assertThat(values(app.batch())).containsExactly("v0", "v1");
    app.nothing();
    assertThat(kafka.position(P0)).isEqualTo(2);
    eventually("paused again", () -> kafka.paused().contains(P0));
  }

  @Test
  void ackedRecordsAreCommittedOnTheInterval() throws Exception {
    start(config());
    AppStream app = subscribed();
    assign(P0);
    long epoch = app.next().getAssigned().getPartitions(0).getAssignmentEpoch();
    app.credits(10);
    add(P0, 0, "a");
    add(P0, 1, "b");
    add(P0, 2, "c");
    int seen = 0;
    while (seen < 3) {
      seen += app.batch().getRecordsCount();
    }

    app.ack(P0, epoch, 2, 2);
    Thread.sleep(150);
    assertThat(committed(P0)).as("offset 0 is not acked").isEqualTo(-1);

    app.ack(P0, epoch, 0, 1);
    eventually("committed past the acked run", () -> committed(P0) == 3);
  }

  @Test
  void ackWithAnotherEpochIsIgnored() throws Exception {
    start(config());
    AppStream app = subscribed();
    assign(P0);
    long epoch = app.next().getAssigned().getPartitions(0).getAssignmentEpoch();
    app.credits(10);
    add(P0, 0, "a");
    app.batch();
    app.ack(P0, epoch + 1, 0, 0);
    Thread.sleep(150);
    assertThat(committed(P0)).isEqualTo(-1);
  }

  @Test
  void revocationWaitsForRevokeDoneAndCommitsTheFinalAcks() throws Exception {
    start(config());
    AppStream app = subscribed();
    assign(P0, P1);
    var partitions = app.next().getAssigned().getPartitionsList();
    long epoch0 =
        partitions.stream()
            .filter(p -> p.getPartition() == 0)
            .findFirst()
            .get()
            .getAssignmentEpoch();
    app.credits(10);
    add(P0, 0, "a");
    app.batch();

    // P0 is taken away; P1 stays.
    kafka.schedulePollTask(() -> kafka.rebalance(List.of(P1)));
    var revoked = app.next().getRevoked();
    assertThat(revoked.getPartitionsList())
        .singleElement()
        .satisfies(
            p -> {
              assertThat(p.getTopic()).isEqualTo("orders");
              assertThat(p.getPartition()).isZero();
            });
    assertThat(revoked.getDeadlineMs()).isEqualTo(5000);
    Thread.sleep(100);
    assertThat(committed(P0)).as("still waiting for the app").isEqualTo(-1);

    app.ack(P0, epoch0, 0, 0);
    app.revokeDone(revoked.getRevokeId());
    eventually("the final ack is committed", () -> committed(P0) == 1);
    eventually("P0 is no longer assigned", () -> kafka.assignment().equals(java.util.Set.of(P1)));

    // P1 is still owned and still served.
    add(P1, 0, "still-here");
    assertThat(values(app.batch())).containsExactly("still-here");
  }

  @Test
  void revocationProceedsAfterItsDeadlineWithWhatWasAcked() throws Exception {
    start(
        config()
            .setConsume(
                Consume.newBuilder()
                    .setCommitInterval(millis(60_000))
                    .setRevokeDeadline(millis(200))));
    AppStream app = subscribed();
    assign(P0);
    long epoch = app.next().getAssigned().getPartitions(0).getAssignmentEpoch();
    app.credits(10);
    add(P0, 0, "a");
    add(P0, 1, "b");
    int seen = 0;
    while (seen < 2) {
      seen += app.batch().getRecordsCount();
    }
    app.ack(P0, epoch, 0, 0);
    Thread.sleep(100);

    kafka.schedulePollTask(() -> kafka.rebalance(List.of()));
    assertThat(app.next().hasRevoked()).isTrue();
    // No RevokeDone: after 200 ms the server commits the acked record only.
    eventually("committed without the app's answer", () -> committed(P0) == 1);
  }

  @Test
  void newOwnershipOfAPartitionGetsANewEpoch() throws Exception {
    start(config());
    AppStream app = subscribed();
    assign(P0);
    long first = app.next().getAssigned().getPartitions(0).getAssignmentEpoch();
    kafka.schedulePollTask(() -> kafka.rebalance(List.of()));
    app.revokeDone(app.next().getRevoked().getRevokeId());
    assign(P0);
    long second = app.next().getAssigned().getPartitions(0).getAssignmentEpoch();
    assertThat(second).isGreaterThan(first);
  }

  @Test
  void appThatHoldsARecordTooLongIsAbortedAfterCommittingWhatItAcked() throws Exception {
    AtomicLong clock = new AtomicLong(1);
    start(
        config()
            .setConsume(
                Consume.newBuilder()
                    .setDefaultMaxProcessing(millis(1000))
                    .setCommitInterval(millis(3_600_000))),
        clock::get);
    AppStream app = subscribed();
    assign(P0);
    long epoch = app.next().getAssigned().getPartitions(0).getAssignmentEpoch();
    app.credits(10);
    add(P0, 0, "a");
    add(P0, 1, "b");
    int seen = 0;
    while (seen < 2) {
      seen += app.batch().getRecordsCount();
    }
    app.ack(P0, epoch, 0, 0);
    Thread.sleep(100);
    assertThat(app.closed).isNotDone();

    clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(1001));
    assertThat(app.status().getCode()).isEqualTo(Status.Code.ABORTED);
    assertThat(committed(P0)).isEqualTo(1);
    assertThat(kafka.closed()).isTrue();
  }

  @Test
  void halfCloseCommitsLeavesTheGroupAndCompletes() throws Exception {
    start(config().setConsume(Consume.newBuilder().setCommitInterval(millis(3_600_000))));
    AppStream app = subscribed();
    assign(P0);
    long epoch = app.next().getAssigned().getPartitions(0).getAssignmentEpoch();
    app.credits(10);
    add(P0, 0, "a");
    app.batch();
    app.ack(P0, epoch, 0, 0);
    app.requests.onCompleted();
    assertThat(app.status().getCode()).isEqualTo(Status.Code.OK);
    assertThat(committed(P0)).isEqualTo(1);
    assertThat(kafka.closed()).isTrue();
  }

  @Test
  void serverShutdownCommitsAndEndsTheStreamAsUnavailable() throws Exception {
    start(config().setConsume(Consume.newBuilder().setCommitInterval(millis(3_600_000))));
    AppStream app = subscribed();
    assign(P0);
    long epoch = app.next().getAssigned().getPartitions(0).getAssignmentEpoch();
    app.credits(10);
    add(P0, 0, "a");
    app.batch();
    app.ack(P0, epoch, 0, 0);
    Thread.sleep(100);

    service.shutdown(java.time.Duration.ofSeconds(5));
    assertThat(app.status().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
    assertThat(committed(P0)).isEqualTo(1);
    assertThat(kafka.closed()).isTrue();
  }

  @Test
  void partitionWithoutCommittedOffsetStartsAtTheBeginningByDefault() throws Exception {
    start(config());
    AppStream app = subscribed();
    kafka.schedulePollTask(
        () -> {
          kafka.updateBeginningOffsets(Map.of(P0, 7L));
          kafka.updateEndOffsets(Map.of(P0, 9L));
          kafka.rebalance(List.of(P0));
        });
    app.next();
    app.credits(10);
    eventually("positioned at the beginning", () -> kafka.position(P0) == 7);
  }

  @Test
  void offsetResetLatestStartsAtTheEnd() throws Exception {
    start(
        config()
            .putConsumers(
                "orders",
                consumer("billing").setOffsetReset(OffsetReset.OFFSET_RESET_LATEST).build()));
    AppStream app = subscribed();
    kafka.schedulePollTask(
        () -> {
          kafka.updateBeginningOffsets(Map.of(P0, 7L));
          kafka.updateEndOffsets(Map.of(P0, 9L));
          kafka.rebalance(List.of(P0));
        });
    app.next();
    app.credits(10);
    eventually("positioned at the end", () -> kafka.position(P0) == 9);
  }

  private static ServerConfig.Builder withSuffixCodec(OnDecodeError onDecodeError) {
    return config()
        .putCodecs(
            "suffix",
            Codec.newBuilder()
                .setValue(
                    Pipeline.newBuilder()
                        .addTransforms(
                            Transform.newBuilder().setClassName(SuffixTransform.class.getName())))
                .build())
        .putConsumers(
            "orders",
            consumer("billing").setCodec("suffix").setOnDecodeError(onDecodeError).build());
  }

  @Test
  void recordsAreDecodedByTheTopicCodec() throws Exception {
    start(withSuffixCodec(OnDecodeError.ON_DECODE_ERROR_UNSPECIFIED));
    AppStream app = subscribed();
    assign(P0);
    app.next();
    app.credits(10);
    add(P0, 0, "stored!");
    assertThat(values(app.batch())).containsExactly("stored");
  }

  @Test
  void undecodableRecordEndsTheStreamWithDataLossByDefault() throws Exception {
    start(withSuffixCodec(OnDecodeError.ON_DECODE_ERROR_UNSPECIFIED));
    AppStream app = subscribed();
    assign(P0);
    app.next();
    app.credits(10);
    kafka.schedulePollTask(
        () -> {
          kafka.addRecord(new ConsumerRecord<>("orders", 0, 0, new byte[0], "good!".getBytes()));
          kafka.addRecord(new ConsumerRecord<>("orders", 0, 1, new byte[0], "bad".getBytes()));
        });
    // The record before the undecodable one is still delivered.
    assertThat(values(app.batch())).containsExactly("good");
    Status status = app.status();
    assertThat(status.getCode()).isEqualTo(Status.Code.DATA_LOSS);
    assertThat(status.getDescription()).contains("orders-0 offset 1");
    assertThat(committed(P0)).as("nothing was acked").isEqualTo(-1);
  }

  @Test
  void undecodableRecordCanBeDeliveredAsStored() throws Exception {
    start(withSuffixCodec(OnDecodeError.ON_DECODE_ERROR_DELIVER_UNDECODED));
    AppStream app = subscribed();
    assign(P0);
    app.next();
    app.credits(10);
    add(P0, 0, "bad");
    ConsumedRecord record = app.batch().getRecords(0);
    assertThat(record.getValue().toStringUtf8()).isEqualTo("bad");
    assertThat(record.getDecodeError()).contains("codec suffix value");
  }
}
