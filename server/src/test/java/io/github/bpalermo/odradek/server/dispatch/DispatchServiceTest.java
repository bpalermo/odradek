package io.github.bpalermo.odradek.server.dispatch;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.util.Durations;
import io.github.bpalermo.odradek.config.v1.Cluster;
import io.github.bpalermo.odradek.config.v1.Consume;
import io.github.bpalermo.odradek.config.v1.Dispatch;
import io.github.bpalermo.odradek.config.v1.ServerConfig;
import io.github.bpalermo.odradek.delivery.v1.Accepted;
import io.github.bpalermo.odradek.delivery.v1.Ack;
import io.github.bpalermo.odradek.delivery.v1.Credits;
import io.github.bpalermo.odradek.delivery.v1.Declare;
import io.github.bpalermo.odradek.delivery.v1.DeliverRequest;
import io.github.bpalermo.odradek.delivery.v1.DeliverResponse;
import io.github.bpalermo.odradek.delivery.v1.DeliveryServiceGrpc;
import io.github.bpalermo.odradek.delivery.v1.OffsetReset;
import io.github.bpalermo.odradek.delivery.v1.OnExhausted;
import io.github.bpalermo.odradek.delivery.v1.Ordering;
import io.github.bpalermo.odradek.delivery.v1.Record;
import io.github.bpalermo.odradek.delivery.v1.RecordBatch;
import io.github.bpalermo.odradek.delivery.v1.Reject;
import io.github.bpalermo.odradek.delivery.v1.Replica;
import io.github.bpalermo.odradek.delivery.v1.Settings;
import io.github.bpalermo.odradek.server.Version;
import io.github.bpalermo.odradek.server.telemetry.ServerMetrics;
import io.github.bpalermo.odradek.server.tls.TlsSources;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The Delivery service over a real gRPC stream, against Kafka's mock consumer: the handshake and
 * its refusals, and what the dispatcher does with the consumer. The broker's side of it (groups,
 * rebalances between replicas, committed offsets) is the end-to-end suite's.
 */
class DispatchServiceTest {
  private static final TopicPartition P0 = new TopicPartition("orders", 0);
  private static final TopicPartition P1 = new TopicPartition("orders", 1);
  private static final TopicPartition R0 = new TopicPartition("refunds", 0);

  /** Remembers commits on the side: the mock cannot be asked from another thread or once closed. */
  private static final class RecordingConsumer extends MockConsumer<byte[], byte[]> {
    final Map<TopicPartition, Long> commits = new ConcurrentHashMap<>();
    final List<Set<String>> subscriptions = new CopyOnWriteArrayList<>();

    RecordingConsumer() {
      super("none");
    }

    @Override
    public synchronized void subscribe(
        java.util.Collection<String> topics,
        org.apache.kafka.clients.consumer.ConsumerRebalanceListener listener) {
      subscriptions.add(Set.copyOf(topics));
      super.subscribe(topics, listener);
    }

    @Override
    public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
      offsets.forEach((partition, offset) -> commits.put(partition, offset.offset()));
      super.commitSync(offsets);
    }

    @Override
    public synchronized void commitAsync(
        Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
      offsets.forEach((partition, offset) -> commits.put(partition, offset.offset()));
      super.commitAsync(offsets, callback);
    }
  }

  /** The consumers the service asked for, in order, with the properties it gave. */
  private final List<RecordingConsumer> consumers = new CopyOnWriteArrayList<>();

  private final List<Map<String, Object>> consumerProperties = new CopyOnWriteArrayList<>();
  private Server server;
  private ManagedChannel channel;
  private DispatchService service;

  /** A sidecar's end of one Deliver stream. */
  private static final class Sidecar {
    final BlockingQueue<DeliverResponse> responses = new LinkedBlockingQueue<>();
    final CompletableFuture<Status> closed = new CompletableFuture<>();
    StreamObserver<DeliverRequest> requests;

    void send(DeliverRequest.Builder request) {
      requests.onNext(request.build());
    }

    void declare(Declare.Builder declare) {
      send(DeliverRequest.newBuilder().setDeclare(declare));
    }

    void credits(int records) {
      send(DeliverRequest.newBuilder().setCredits(Credits.newBuilder().setRecords(records)));
    }

    void ack(int partition, long first, long last) {
      send(
          DeliverRequest.newBuilder()
              .setAck(
                  Ack.newBuilder()
                      .setPartition(partition)
                      .setFirstOffset(first)
                      .setLastOffset(last)));
    }

    void reject(int partition, long offset) {
      send(
          DeliverRequest.newBuilder()
              .setReject(
                  Reject.newBuilder().setPartition(partition).setOffset(offset).setReason("test")));
    }

    DeliverResponse next() throws InterruptedException {
      DeliverResponse response = responses.poll(10, TimeUnit.SECONDS);
      assertThat(response).as("a response within 10s").isNotNull();
      return response;
    }

    Accepted accepted() throws InterruptedException {
      DeliverResponse response = next();
      assertThat(response.hasAccepted()).as("%s", response).isTrue();
      return response.getAccepted();
    }

    RecordBatch batch() throws InterruptedException {
      DeliverResponse response = next();
      assertThat(response.hasRecords()).as("%s", response).isTrue();
      return response.getRecords();
    }

    /** Records until {@code count} have arrived, as "partition/offset/attempt". */
    List<String> records(int count) throws InterruptedException {
      List<String> records = new ArrayList<>();
      while (records.size() < count) {
        RecordBatch batch = batch();
        for (Record record : batch.getRecordsList()) {
          records.add(batch.getPartition() + "/" + record.getOffset() + "/" + record.getAttempt());
        }
      }
      return records;
    }

    void nothing() throws InterruptedException {
      assertThat(responses.poll(200, TimeUnit.MILLISECONDS)).isNull();
    }

    Status status() throws Exception {
      return closed.get(10, TimeUnit.SECONDS);
    }
  }

  private static ServerConfig.Builder config() {
    return ServerConfig.newBuilder()
        .putClusters("main", Cluster.getDefaultInstance())
        .setDispatch(Dispatch.newBuilder().setIdleGrace(Durations.fromSeconds(60)))
        .setConsume(
            Consume.newBuilder()
                .setCommitInterval(Durations.fromMillis(20))
                .setRevokeDeadline(Durations.fromMillis(300)));
  }

  private void start(ServerConfig.Builder config) throws Exception {
    service =
        new DispatchService(
            config.build(),
            TlsSources.none(),
            ServerMetrics.noop(),
            (cluster, properties) -> {
              consumerProperties.add(properties);
              RecordingConsumer consumer = new RecordingConsumer();
              consumers.add(consumer);
              return consumer;
            },
            System::nanoTime);
    service.advertise("10.1.2.3:9465");
    String name = InProcessServerBuilder.generateName();
    server = InProcessServerBuilder.forName(name).addService(service).build().start();
    channel = InProcessChannelBuilder.forName(name).build();
  }

  @AfterEach
  void stop() throws InterruptedException {
    if (channel != null) {
      channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
      service.shutdown(Duration.ofSeconds(5));
      server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private Sidecar stream() {
    Sidecar sidecar = new Sidecar();
    sidecar.requests =
        DeliveryServiceGrpc.newStub(channel)
            .deliver(
                new StreamObserver<DeliverResponse>() {
                  @Override
                  public void onNext(DeliverResponse response) {
                    sidecar.responses.add(response);
                  }

                  @Override
                  public void onError(Throwable t) {
                    sidecar.closed.complete(Status.fromThrowable(t));
                  }

                  @Override
                  public void onCompleted() {
                    sidecar.closed.complete(Status.OK);
                  }
                });
    return sidecar;
  }

  private static Declare.Builder declare(String topic) {
    return Declare.newBuilder()
        .setCluster("main")
        .setGroupId("billing")
        .setTopic(topic)
        .setSettings(Settings.newBuilder().setMaxProcessingMs(60_000));
  }

  private static Declare.Builder declare(String topic, Consumer<Settings.Builder> settings) {
    Declare.Builder declare = declare(topic);
    settings.accept(declare.getSettingsBuilder());
    return declare;
  }

  /** A sidecar whose declaration was accepted. */
  private Sidecar declared(Declare.Builder declare) throws InterruptedException {
    Sidecar sidecar = stream();
    sidecar.declare(declare);
    sidecar.accepted();
    return sidecar;
  }

  private RecordingConsumer kafka() throws InterruptedException {
    eventually("a consumer was created", () -> !consumers.isEmpty());
    return consumers.get(consumers.size() - 1);
  }

  /** Gives the consumer these partitions, each empty and starting at offset 0. */
  private void assign(TopicPartition... partitions) throws InterruptedException {
    RecordingConsumer kafka = kafka();
    kafka.schedulePollTask(
        () -> {
          for (TopicPartition partition : partitions) {
            kafka.updateBeginningOffsets(Map.of(partition, 0L));
            kafka.updateEndOffsets(Map.of(partition, 0L));
          }
          kafka.rebalance(List.of(partitions));
        });
  }

  private void add(TopicPartition partition, long offset, String key) throws InterruptedException {
    RecordingConsumer kafka = kafka();
    kafka.schedulePollTask(
        () ->
            kafka.addRecord(
                new ConsumerRecord<>(
                    partition.topic(),
                    partition.partition(),
                    offset,
                    key == null ? null : key.getBytes(StandardCharsets.UTF_8),
                    ("v" + offset).getBytes(StandardCharsets.UTF_8))));
  }

  private static void eventually(String what, BooleanSupplier condition)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
      Thread.sleep(5);
    }
    assertThat(condition.getAsBoolean()).as(what).isTrue();
  }

  private long committed(TopicPartition partition) throws InterruptedException {
    return kafka().commits.getOrDefault(partition, -1L);
  }

  // -------------------------------------------------------------------------------------------
  // The handshake
  // -------------------------------------------------------------------------------------------

  @Test
  void theFirstMessageMustBeDeclare() throws Exception {
    start(config());
    Sidecar sidecar = stream();
    sidecar.credits(1);
    Status status = sidecar.status();
    assertThat(status.getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    assertThat(status.getDescription()).isEqualTo("the first message must be Declare");
    assertThat(consumers).isEmpty();
  }

  @Test
  void aMalformedDeclarationIsRefused() throws Exception {
    start(config());
    Sidecar sidecar = stream();
    sidecar.declare(declare("orders").clearSettings());
    assertThat(sidecar.status().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    assertThat(consumers).isEmpty();
  }

  @Test
  void aClusterTheDispatcherLacksIsRefused() throws Exception {
    start(config());
    Sidecar sidecar = stream();
    sidecar.declare(declare("orders").setCluster("elsewhere"));
    assertThat(sidecar.status().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
    assertThat(consumers).isEmpty();
  }

  @Test
  void deadLetteringIsRefusedForNow() throws Exception {
    start(config());
    Sidecar sidecar = stream();
    sidecar.declare(
        declare(
            "orders",
            s ->
                s.setMaxAttempts(3)
                    .setOnExhausted(OnExhausted.ON_EXHAUSTED_DEAD_LETTER)
                    .setDeadLetterTopic("orders.dlq")));
    assertThat(sidecar.status().getCode()).isEqualTo(Status.Code.UNIMPLEMENTED);
  }

  @Test
  void acceptedCarriesTheSettingsInForceAndThisReplicaAlone() throws Exception {
    start(config());
    Sidecar sidecar = stream();
    sidecar.declare(declare("orders"));
    Accepted accepted = sidecar.accepted();
    assertThat(accepted.getSettings())
        .isEqualTo(
            Settings.newBuilder()
                .setOrdering(Ordering.ORDERING_KEY)
                .setMaxProcessingMs(60_000)
                .setOffsetReset(OffsetReset.OFFSET_RESET_EARLIEST)
                .setMaxAttempts(0)
                .setOnExhausted(OnExhausted.ON_EXHAUSTED_BLOCK)
                .build());
    assertThat(accepted.getReplicas().getGeneration()).isEqualTo(1);
    assertThat(accepted.getReplicas().getReplicasList().stream().map(Replica::getAddress).toList())
        .containsExactly("10.1.2.3:9465");
    assertThat(accepted.getDispatcher().getVersion()).isEqualTo(Version.VERSION).isNotEmpty();
    eventually("subscribed", () -> consumers.get(0).subscriptions.contains(Set.of("orders")));
  }

  @Test
  void theConsumerCarriesTheGuaranteesAndCreatesNoTopic() throws Exception {
    start(config());
    declared(declare("orders"));
    kafka();
    assertThat(consumerProperties.get(0))
        .containsEntry("group.id", "billing")
        .containsEntry("enable.auto.commit", "false")
        .containsEntry("auto.offset.reset", "none")
        .containsEntry("allow.auto.create.topics", "false")
        .containsEntry("client.id", "odradek-dispatch-billing-1")
        .containsEntry(
            "partition.assignment.strategy",
            "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
  }

  @Test
  void aDeclarationWithOtherSettingsIsRefusedWhileASidecarHoldsTheTopic() throws Exception {
    start(config());
    Sidecar first = declared(declare("orders"));
    Sidecar second = stream();
    second.declare(declare("orders", s -> s.setOrdering(Ordering.ORDERING_NONE).setMaxAttempts(3)));
    Status status = second.status();
    assertThat(status.getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
    assertThat(status.getDescription())
        .contains("group billing already consumes topic orders on cluster main")
        .contains(
            "In force: ordering ORDERING_KEY, offset_reset OFFSET_RESET_EARLIEST, no max_attempts")
        .contains("Declared: ordering ORDERING_NONE");

    // Another topic of the same group is nobody's business but its own.
    declared(declare("refunds", s -> s.setOrdering(Ordering.ORDERING_NONE)));

    // Once nobody holds the topic with the old settings, the new ones are accepted.
    first.requests.onCompleted();
    assertThat(first.status().isOk()).isTrue();
    Sidecar third = stream();
    third.declare(declare("orders", s -> s.setOrdering(Ordering.ORDERING_NONE).setMaxAttempts(3)));
    Settings settings = third.accepted().getSettings();
    assertThat(settings.getOrdering()).isEqualTo(Ordering.ORDERING_NONE);
    assertThat(settings.getMaxAttempts()).isEqualTo(3);
    assertThat(consumers).hasSize(1);
  }

  @Test
  void theProcessingDeadlineFollowsTheLatestDeclaration() throws Exception {
    start(config());
    declared(declare("orders"));
    Sidecar newer = stream();
    newer.declare(declare("orders", s -> s.setMaxProcessingMs(90_000)));
    assertThat(newer.accepted().getSettings().getMaxProcessingMs()).isEqualTo(90_000);
    // And an old pod that reconnects sets it back: the latest declaration, not the largest.
    Sidecar older = stream();
    older.declare(declare("orders"));
    assertThat(older.accepted().getSettings().getMaxProcessingMs()).isEqualTo(60_000);
  }

  @Test
  void aNewDeadlineAppliesToWhatIsSentFromThenOn() throws Exception {
    start(config());
    Sidecar slow = declared(declare("orders", s -> s.setOrdering(Ordering.ORDERING_NONE)));
    assign(P0);
    add(P0, 0, "a");
    slow.credits(1);
    assertThat(slow.records(1)).containsExactly("0/0/1");
    // A version with a deadline of 100 ms: what it is sent expires, what was sent before does not.
    Sidecar fast =
        declared(
            declare("orders", s -> s.setOrdering(Ordering.ORDERING_NONE).setMaxProcessingMs(100)));
    add(P0, 1, "b");
    fast.credits(1);
    assertThat(fast.records(1)).containsExactly("0/1/1");
    slow.credits(1);
    assertThat(slow.records(1)).containsExactly("0/1/2");
    slow.nothing();
  }

  @Test
  void aSecondDeclareBreaksTheProtocol() throws Exception {
    start(config());
    Sidecar sidecar = declared(declare("orders"));
    sidecar.declare(declare("orders"));
    Status status = sidecar.status();
    assertThat(status.getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    assertThat(status.getDescription()).isEqualTo("Declare was already sent");
  }

  @Test
  void anEmptyMessageBreaksTheProtocol() throws Exception {
    start(config());
    Sidecar sidecar = declared(declare("orders"));
    sidecar.send(DeliverRequest.newBuilder());
    assertThat(sidecar.status().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
  }

  // -------------------------------------------------------------------------------------------
  // Records
  // -------------------------------------------------------------------------------------------

  @Test
  void recordsAreDeliveredAgainstCreditAsTheyAreStored() throws Exception {
    start(config());
    Sidecar sidecar = declared(declare("orders"));
    assign(P0);
    RecordingConsumer kafka = kafka();
    kafka.schedulePollTask(
        () -> {
          ConsumerRecord<byte[], byte[]> record =
              new ConsumerRecord<>("orders", 0, 0, new byte[] {1, 2}, new byte[] {3});
          record.headers().add(new RecordHeader("h", new byte[] {9}));
          record.headers().add(new RecordHeader("none", null));
          kafka.addRecord(record);
          kafka.addRecord(new ConsumerRecord<>("orders", 0, 1, null, null));
        });
    sidecar.nothing();
    sidecar.credits(10);
    RecordBatch batch = sidecar.batch();
    assertThat(batch.getPartition()).isZero();
    assertThat(batch.getRecordsCount()).isEqualTo(2);
    Record first = batch.getRecords(0);
    assertThat(first.getOffset()).isZero();
    assertThat(first.getAttempt()).isEqualTo(1);
    assertThat(first.getKey().toByteArray()).containsExactly(1, 2);
    assertThat(first.getValue().toByteArray()).containsExactly(3);
    assertThat(first.getHeadersCount()).isEqualTo(2);
    assertThat(first.getHeaders(0).getKey()).isEqualTo("h");
    assertThat(first.getHeaders(0).getValue().toByteArray()).containsExactly(9);
    assertThat(first.getHeaders(1).hasValue()).isFalse();
    Record tombstone = batch.getRecords(1);
    assertThat(tombstone.getOffset()).isEqualTo(1);
    assertThat(tombstone.hasKey()).isFalse();
    assertThat(tombstone.hasValue()).isFalse();
  }

  @Test
  void acknowledgedRecordsAreCommittedAtTheInterval() throws Exception {
    start(config());
    Sidecar sidecar = declared(declare("orders"));
    assign(P0);
    // The position found for a partition without a committed offset is committed.
    eventually("the start position is committed", () -> committedNow(P0) == 0);
    for (int offset = 0; offset < 4; offset++) {
      add(P0, offset, "k" + offset);
    }
    sidecar.credits(10);
    assertThat(sidecar.records(4)).containsExactly("0/0/1", "0/1/1", "0/2/1", "0/3/1");
    sidecar.ack(0, 1, 2);
    sidecar.nothing();
    assertThat(committed(P0)).isZero();
    sidecar.ack(0, 0, 0);
    eventually("offsets 0 to 2 are committed", () -> committedNow(P0) == 3);
  }

  private long committedNow(TopicPartition partition) {
    return consumers.get(consumers.size() - 1).commits.getOrDefault(partition, -1L);
  }

  @Test
  void whatASidecarHeldWhenItWentIsDeliveredToAnotherUnderTheSameAttempt() throws Exception {
    start(config());
    Sidecar doomed = declared(declare("orders"));
    Sidecar survivor = declared(declare("orders"));
    assign(P0);
    add(P0, 0, "a");
    add(P0, 1, "b");
    doomed.credits(10);
    assertThat(doomed.records(2)).containsExactly("0/0/1", "0/1/1");
    survivor.credits(10);
    survivor.nothing();
    doomed.requests.onError(Status.CANCELLED.asRuntimeException());
    assertThat(survivor.records(2)).containsExactly("0/0/1", "0/1/1");
  }

  @Test
  void aRejectedRecordIsDeliveredAgainWithTheNextAttempt() throws Exception {
    start(config());
    Sidecar sidecar = declared(declare("orders"));
    assign(P0);
    add(P0, 0, "a");
    sidecar.credits(10);
    assertThat(sidecar.records(1)).containsExactly("0/0/1");
    sidecar.reject(0, 0);
    assertThat(sidecar.records(1)).containsExactly("0/0/2");
  }

  @Test
  void aRecordThatUsesItsAttemptsBlocksItsPartitionAndNothingElse() throws Exception {
    start(config());
    Sidecar sidecar =
        declared(
            declare("orders", s -> s.setOrdering(Ordering.ORDERING_PARTITION).setMaxAttempts(2)));
    assign(P0, P1);
    add(P0, 0, "a");
    add(P0, 1, "b");
    add(P0, 2, "c");
    sidecar.credits(10);
    assertThat(sidecar.records(1)).containsExactly("0/0/1");
    sidecar.ack(0, 0, 0);
    assertThat(sidecar.records(1)).containsExactly("0/1/1");
    sidecar.reject(0, 1);
    assertThat(sidecar.records(1)).containsExactly("0/1/2");
    sidecar.reject(0, 1);
    // Offset 2 is never sent; the other partition goes on.
    add(P1, 0, "z");
    assertThat(sidecar.records(1)).containsExactly("1/0/1");
    sidecar.ack(1, 0, 0);
    sidecar.nothing();
    eventually("the other partition is committed", () -> committedNow(P1) == 1);
    eventually("the blocked one is committed up to the record", () -> committedNow(P0) == 1);
    sidecar.nothing();
    assertThat(committed(P0)).isEqualTo(1);
  }

  // -------------------------------------------------------------------------------------------
  // Partitions, topics and the end
  // -------------------------------------------------------------------------------------------

  @Test
  void aRevokedPartitionWaitsForWhatIsInFlightThenIsCommittedAndReleased() throws Exception {
    start(config().setConsume(Consume.newBuilder().setRevokeDeadline(Durations.fromSeconds(30))));
    Sidecar sidecar = declared(declare("orders"));
    assign(P0, P1);
    add(P0, 0, "a");
    add(P1, 0, "b");
    sidecar.credits(10);
    assertThat(sidecar.records(2)).containsExactlyInAnyOrder("0/0/1", "1/0/1");
    assign(P1);
    // The acknowledgement arrives while the partition is being handed over, and is committed.
    sidecar.nothing();
    assertThat(committed(P0)).isLessThanOrEqualTo(0);
    sidecar.ack(0, 0, 0);
    eventually("the revoked partition is committed", () -> committedNow(P0) == 1);
    // Nothing was left with the sidecar, so it is told nothing; the other partition goes on.
    add(P1, 1, "c");
    sidecar.ack(1, 0, 0);
    assertThat(sidecar.records(1)).containsExactly("1/1/1");
  }

  @Test
  void whatIsStillHeldAfterTheGraceIsReleased() throws Exception {
    start(config());
    Sidecar sidecar = declared(declare("orders"));
    assign(P0, P1);
    add(P0, 0, "a");
    add(P0, 1, "b");
    sidecar.credits(10);
    assertThat(sidecar.records(2)).containsExactly("0/0/1", "0/1/1");
    sidecar.ack(0, 0, 0);
    assign(P1);
    DeliverResponse response = sidecar.next();
    assertThat(response.hasReleased()).as("%s", response).isTrue();
    assertThat(response.getReleased().getPartitionsList()).containsExactly(0);
    assertThat(committed(P0)).isEqualTo(1);
  }

  @Test
  void aSecondTopicGrowsTheSubscriptionOfTheSameConsumer() throws Exception {
    start(config());
    Sidecar orders = declared(declare("orders"));
    assign(P0);
    add(P0, 0, "a");
    orders.credits(10);
    assertThat(orders.records(1)).containsExactly("0/0/1");

    Sidecar refunds = declared(declare("refunds", s -> s.setOrdering(Ordering.ORDERING_NONE)));
    eventually(
        "subscribed to both",
        () -> consumers.get(0).subscriptions.contains(Set.of("orders", "refunds")));
    assign(P0, R0);
    add(R0, 0, "r");
    add(P0, 1, "b");
    refunds.credits(10);
    assertThat(refunds.records(1)).containsExactly("0/0/1");
    // The first topic went on from where it was: nothing of it is delivered again.
    assertThat(orders.records(1)).containsExactly("0/1/1");
    orders.nothing();
    assertThat(consumers).hasSize(1);
  }

  @Test
  void aTopicWithoutSidecarsIsDroppedAfterTheGraceAndTheConsumerClosedWithTheLast()
      throws Exception {
    start(config().setDispatch(Dispatch.newBuilder().setIdleGrace(Durations.fromMillis(300))));
    Sidecar orders = declared(declare("orders"));
    Sidecar refunds = declared(declare("refunds"));
    RecordingConsumer kafka = kafka();
    eventually(
        "subscribed to both", () -> kafka.subscriptions.contains(Set.of("orders", "refunds")));
    assign(P0, R0);
    add(R0, 0, "r");
    refunds.credits(1);
    assertThat(refunds.records(1)).containsExactly("0/0/1");
    refunds.ack(0, 0, 0);

    refunds.requests.onCompleted();
    assertThat(refunds.status().isOk()).isTrue();
    eventually(
        "refunds is dropped",
        () ->
            kafka.subscriptions.size() >= 2
                && kafka
                    .subscriptions
                    .get(kafka.subscriptions.size() - 1)
                    .equals(Set.of("orders")));
    assertThat(kafka.commits).containsEntry(R0, 1L);
    assertThat(kafka.closed()).isFalse();

    orders.requests.onCompleted();
    assertThat(orders.status().isOk()).isTrue();
    eventually("the consumer is closed", kafka::closed);

    // The group is gone, not broken: the next declaration starts a new consumer.
    declared(declare("orders"));
    eventually("a second consumer", () -> consumers.size() == 2);
  }

  @Test
  void aSidecarThatComesBackWithinTheGraceFindsTheTopicStillConsumed() throws Exception {
    start(config());
    Sidecar first = declared(declare("orders"));
    assign(P0);
    add(P0, 0, "a");
    add(P0, 1, "b");
    first.credits(1);
    assertThat(first.records(1)).containsExactly("0/0/1");
    first.requests.onCompleted();
    assertThat(first.status().isOk()).isTrue();

    Sidecar second = declared(declare("orders"));
    second.credits(10);
    assertThat(second.records(2)).containsExactly("0/0/1", "0/1/1");
    assertThat(consumers).hasSize(1);
    assertThat(consumers.get(0).subscriptions).containsExactly(Set.of("orders"));
  }

  @Test
  void shutdownCommitsEndsTheStreamsAndRefusesDeclarations() throws Exception {
    start(config());
    Sidecar sidecar = declared(declare("orders"));
    assign(P0);
    add(P0, 0, "a");
    add(P0, 1, "b");
    sidecar.credits(10);
    assertThat(sidecar.records(2)).containsExactly("0/0/1", "0/1/1");
    sidecar.ack(0, 0, 0);
    RecordingConsumer kafka = kafka();

    service.shutdown(Duration.ofSeconds(10));
    Status status = sidecar.status();
    assertThat(status.getCode()).isEqualTo(Status.Code.UNAVAILABLE);
    assertThat(kafka.commits).containsEntry(P0, 1L);
    assertThat(kafka.closed()).isTrue();

    Sidecar late = stream();
    late.declare(declare("orders"));
    assertThat(late.status().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
    assertThat(consumers).hasSize(1);
  }

  @Test
  void aSidecarThatSitsOnItsRecordsDoesNotStopTheOthers() throws Exception {
    start(config());
    Sidecar silent = declared(declare("orders", s -> s.setOrdering(Ordering.ORDERING_NONE)));
    Sidecar working = declared(declare("orders", s -> s.setOrdering(Ordering.ORDERING_NONE)));
    assign(P0);
    silent.credits(2);
    add(P0, 0, "a");
    add(P0, 1, "b");
    assertThat(silent.records(2)).containsExactly("0/0/1", "0/1/1");
    // It holds them and says nothing; records keep flowing to the other.
    working.credits(100);
    for (int offset = 2; offset < 50; offset++) {
      add(P0, offset, "k");
    }
    assertThat(working.records(48)).hasSize(48);
  }
}
