package io.github.bpalermo.odradek.server.produce;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import io.github.bpalermo.odradek.config.v1.Cluster;
import io.github.bpalermo.odradek.config.v1.Codec;
import io.github.bpalermo.odradek.config.v1.Pipeline;
import io.github.bpalermo.odradek.config.v1.Produce;
import io.github.bpalermo.odradek.config.v1.ProduceTarget;
import io.github.bpalermo.odradek.config.v1.Producer;
import io.github.bpalermo.odradek.config.v1.ServerConfig;
import io.github.bpalermo.odradek.config.v1.Transform;
import io.github.bpalermo.odradek.kafka.v1.ErrorCode;
import io.github.bpalermo.odradek.kafka.v1.Header;
import io.github.bpalermo.odradek.kafka.v1.OdradekServiceGrpc;
import io.github.bpalermo.odradek.kafka.v1.ProduceOpen;
import io.github.bpalermo.odradek.kafka.v1.ProduceRecord;
import io.github.bpalermo.odradek.kafka.v1.ProduceRequest;
import io.github.bpalermo.odradek.kafka.v1.ProduceResponse;
import io.github.bpalermo.odradek.kafka.v1.ProduceResult;
import io.github.bpalermo.odradek.server.OdradekService;
import io.github.bpalermo.odradek.server.codec.CodecRegistry;
import io.github.bpalermo.odradek.server.codec.ContractRegistry;
import io.github.bpalermo.odradek.server.consume.ConsumeService;
import io.github.bpalermo.odradek.server.tls.TlsSources;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** The Produce RPC over a real gRPC stream (in-process transport), against mock Kafka producers. */
class ProduceServiceTest {
  private static final long NOW = 1_800_000_000_000L;

  private final Map<String, MockProducer<byte[], byte[]>> kafka = new HashMap<>();
  private CodecRegistry codecs;
  private Server server;
  private ManagedChannel channel;
  private ProduceService service;

  /** The app's end of one Produce stream. */
  private static final class AppStream {
    final BlockingQueue<ProduceResponse> responses = new LinkedBlockingQueue<>();
    final CompletableFuture<Status> closed = new CompletableFuture<>();
    StreamObserver<ProduceRequest> requests;

    void open(String session) {
      requests.onNext(
          ProduceRequest.newBuilder()
              .setOpen(ProduceOpen.newBuilder().setSessionId(session))
              .build());
    }

    void send(ProduceRecord.Builder record) {
      requests.onNext(ProduceRequest.newBuilder().setRecord(record).build());
    }

    ProduceResponse next() throws InterruptedException {
      ProduceResponse response = responses.poll(10, TimeUnit.SECONDS);
      assertThat(response).as("a response within 10s").isNotNull();
      return response;
    }

    ProduceResult result() throws InterruptedException {
      ProduceResponse response = next();
      assertThat(response.hasResult()).isTrue();
      return response.getResult();
    }

    Status status() throws Exception {
      return closed.get(10, TimeUnit.SECONDS);
    }
  }

  private static ServerConfig.Builder config() {
    return ServerConfig.newBuilder()
        .putClusters("a", Cluster.getDefaultInstance())
        .putClusters("b", Cluster.getDefaultInstance())
        .putProducers("single", producer(target("a")))
        .putProducers("both", producer(target("a"), target("b")))
        .putProducers("copy", producer(target("a"), target("b").setBestEffort(true)));
  }

  private static ProduceTarget.Builder target(String cluster) {
    return ProduceTarget.newBuilder().setCluster(cluster);
  }

  private static Producer producer(ProduceTarget.Builder... targets) {
    Producer.Builder producer = Producer.newBuilder();
    for (ProduceTarget.Builder target : targets) {
      producer.addClusters(target);
    }
    return producer.build();
  }

  private void start(ServerConfig.Builder builder) throws Exception {
    ServerConfig config = builder.build();
    codecs = CodecRegistry.create(config, ContractRegistry.empty(), getClass().getClassLoader());
    ClusterProducers producers =
        ClusterProducers.create(
            config,
            (cluster, properties) -> {
              MockProducer<byte[], byte[]> mock =
                  new MockProducer<>(
                      false,
                      new RoundRobinPartitioner(),
                      new ByteArraySerializer(),
                      new ByteArraySerializer());
              kafka.put(cluster, mock);
              return mock;
            });
    service =
        new ProduceService(config, ProduceRoutes.create(config, producers, codecs), () -> NOW);
    String name = InProcessServerBuilder.generateName();
    server =
        InProcessServerBuilder.forName(name)
            .addService(
                new OdradekService(
                    service,
                    new ConsumeService(config, codecs, TlsSources.none(), service.serverEpoch())))
            .build()
            .start();
    channel = InProcessChannelBuilder.forName(name).build();
  }

  @AfterEach
  void stop() throws InterruptedException {
    if (channel != null) {
      channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
      server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
      codecs.close();
    }
  }

  private AppStream stream() {
    AppStream app = new AppStream();
    app.requests =
        OdradekServiceGrpc.newStub(channel)
            .produce(
                new StreamObserver<ProduceResponse>() {
                  @Override
                  public void onNext(ProduceResponse response) {
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

  /** Opens a stream on {@code session} and consumes the handshake reply. */
  private AppStream opened(String session) throws InterruptedException {
    AppStream app = stream();
    app.open(session);
    assertThat(app.next().hasReady()).isTrue();
    return app;
  }

  private static ProduceRecord.Builder record(long seq, String topic, String value) {
    return ProduceRecord.newBuilder()
        .setSeq(seq)
        .setLowWatermark(1)
        .setTopic(topic)
        .setValue(ByteString.copyFromUtf8(value));
  }

  /** Waits until {@code cluster} has been handed {@code count} records. */
  private MockProducer<byte[], byte[]> sent(String cluster, int count) throws InterruptedException {
    MockProducer<byte[], byte[]> mock = kafka.get(cluster);
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (mock.history().size() < count && System.nanoTime() < deadline) {
      Thread.sleep(5);
    }
    assertThat(mock.history()).hasSize(count);
    return mock;
  }

  private static String text(byte[] bytes) {
    return new String(bytes, StandardCharsets.UTF_8);
  }

  @Test
  void handshakeAnnouncesEpochAndWindow() throws Exception {
    start(
        config()
            .setProduce(Produce.newBuilder().setMaxInFlightRecords(7).setMaxInFlightBytes(4096)));
    AppStream app = stream();
    app.open("s1");
    var ready = app.next().getReady();
    assertThat(ready.getServerEpoch()).isEqualTo(service.serverEpoch()).isNotEmpty();
    assertThat(ready.getMaxInFlightRecords()).isEqualTo(7);
    assertThat(ready.getMaxInFlightBytes()).isEqualTo(4096);
  }

  @Test
  void windowDefaultsApplyWhenUnset() throws Exception {
    start(config());
    AppStream app = stream();
    app.open("s1");
    var ready = app.next().getReady();
    assertThat(ready.getMaxInFlightRecords()).isEqualTo(1000);
    assertThat(ready.getMaxInFlightBytes()).isEqualTo(16L * 1024 * 1024);
  }

  @Test
  void resultComesOnlyAfterTheBrokerAcknowledges() throws Exception {
    start(config());
    AppStream app = opened("s1");
    app.send(record(1, "single", "v"));
    MockProducer<byte[], byte[]> a = sent("a", 1);
    assertThat(app.responses.poll(200, TimeUnit.MILLISECONDS)).as("no result yet").isNull();

    a.completeNext();
    ProduceResult result = app.result();
    assertThat(result.getSeq()).isEqualTo(1);
    assertThat(result.hasError()).isFalse();
    assertThat(result.getWritesList())
        .singleElement()
        .satisfies(
            w -> {
              assertThat(w.getCluster()).isEqualTo("a");
              assertThat(w.getTopic()).isEqualTo("single");
              assertThat(w.getOffset()).isZero();
            });
    assertThat(kafka.get("b").history()).isEmpty();
  }

  @Test
  void recordFieldsReachKafkaUnchanged() throws Exception {
    start(config());
    AppStream app = opened("s1");
    app.send(
        record(1, "single", "v")
            .setKey(ByteString.copyFromUtf8("k"))
            .setPartition(3)
            .setTimestampMs(1234)
            .addHeaders(Header.newBuilder().setKey("h").setValue(ByteString.copyFromUtf8("x")))
            .addHeaders(Header.newBuilder().setKey("null")));
    ProducerRecord<byte[], byte[]> stored = sent("a", 1).history().get(0);
    assertThat(stored.topic()).isEqualTo("single");
    assertThat(stored.partition()).isEqualTo(3);
    assertThat(stored.timestamp()).isEqualTo(1234);
    assertThat(text(stored.key())).isEqualTo("k");
    assertThat(text(stored.value())).isEqualTo("v");
    assertThat(text(stored.headers().lastHeader("h").value())).isEqualTo("x");
    assertThat(stored.headers().lastHeader("null").value()).isNull();
  }

  @Test
  void nullKeyAndTombstoneStayNullAndTimestampDefaultsToTheServerClock() throws Exception {
    start(config());
    AppStream app = opened("s1");
    app.send(ProduceRecord.newBuilder().setSeq(1).setTopic("single"));
    ProducerRecord<byte[], byte[]> stored = sent("a", 1).history().get(0);
    assertThat(stored.key()).isNull();
    assertThat(stored.value()).isNull();
    assertThat(stored.partition()).isNull();
    assertThat(stored.timestamp()).isEqualTo(NOW);
  }

  @Test
  void topicOnTwoClustersNeedsBothAcknowledgements() throws Exception {
    start(config());
    AppStream app = opened("s1");
    app.send(record(1, "both", "v"));
    MockProducer<byte[], byte[]> a = sent("a", 1);
    MockProducer<byte[], byte[]> b = sent("b", 1);
    assertThat(a.history().get(0).timestamp()).isEqualTo(b.history().get(0).timestamp());

    a.completeNext();
    assertThat(app.responses.poll(200, TimeUnit.MILLISECONDS)).as("b has not answered").isNull();
    b.completeNext();
    ProduceResult result = app.result();
    assertThat(result.hasError()).isFalse();
    assertThat(result.getWritesList()).extracting(w -> w.getCluster()).containsExactly("a", "b");
  }

  @Test
  void bestEffortClusterIsNotWaitedForAndItsFailureIsNotReported() throws Exception {
    start(config());
    AppStream app = opened("s1");
    app.send(record(1, "copy", "v"));
    sent("b", 1);
    sent("a", 1).completeNext();
    ProduceResult result = app.result();
    assertThat(result.hasError()).isFalse();
    assertThat(result.getWritesList()).extracting(w -> w.getCluster()).containsExactly("a");

    kafka.get("b").errorNext(new TimeoutException("slow"));
    assertThat(app.responses.poll(200, TimeUnit.MILLISECONDS)).as("one result per record").isNull();
  }

  @Test
  void requiredFailureAfterAnotherClusterAcknowledgedIsOutcomeUnknown() throws Exception {
    start(config());
    AppStream app = opened("s1");
    app.send(record(1, "both", "v"));
    sent("a", 1).completeNext();
    sent("b", 1).errorNext(new RecordTooLargeException("too large"));
    ProduceResult result = app.result();
    assertThat(result.getError().getCode()).isEqualTo(ErrorCode.ERROR_CODE_OUTCOME_UNKNOWN);
    assertThat(result.getError().getKafkaException()).isEqualTo("RecordTooLargeException");
    assertThat(result.getError().getMessage()).contains("cluster b");
    assertThat(result.getWritesList()).extracting(w -> w.getCluster()).containsExactly("a");
  }

  @Test
  void requiredFailureWaitsForABestEffortClusterToSayWhereTheRecordIs() throws Exception {
    start(config());
    AppStream app = opened("s1");
    app.send(record(1, "copy", "v"));
    sent("b", 1);
    sent("a", 1).errorNext(new RecordTooLargeException("too large"));
    assertThat(app.responses.poll(200, TimeUnit.MILLISECONDS)).as("b has not answered").isNull();

    kafka.get("b").completeNext();
    ProduceResult result = app.result();
    assertThat(result.getError().getCode()).isEqualTo(ErrorCode.ERROR_CODE_OUTCOME_UNKNOWN);
    assertThat(result.getWritesList()).extracting(w -> w.getCluster()).containsExactly("b");
  }

  @Test
  void definiteFailuresKeepTheirCodeWhenNoClusterHasTheRecord() throws Exception {
    start(config());
    AppStream app = opened("s1");
    app.send(record(1, "single", "v"));
    sent("a", 1).errorNext(new RecordTooLargeException("too large"));
    assertThat(app.result().getError().getCode()).isEqualTo(ErrorCode.ERROR_CODE_INVALID_RECORD);

    app.send(record(2, "single", "v"));
    sent("a", 2).errorNext(new TopicAuthorizationException("denied"));
    assertThat(app.result().getError().getCode())
        .isEqualTo(ErrorCode.ERROR_CODE_AUTHORIZATION_FAILED);

    app.send(record(3, "single", "v"));
    sent("a", 3).errorNext(new TimeoutException("expired"));
    ProduceResult timedOut = app.result();
    assertThat(timedOut.getError().getCode()).isEqualTo(ErrorCode.ERROR_CODE_OUTCOME_UNKNOWN);
    assertThat(timedOut.getWritesList()).isEmpty();
  }

  @Test
  void unconfiguredTopicFailsOnlyThatRecord() throws Exception {
    start(config());
    AppStream app = opened("s1");
    app.send(record(1, "nowhere", "v"));
    assertThat(app.result().getError().getCode())
        .isEqualTo(ErrorCode.ERROR_CODE_TOPIC_NOT_CONFIGURED);

    app.send(record(2, "single", "v"));
    sent("a", 1).completeNext();
    assertThat(app.result().hasError()).isFalse();
  }

  @Test
  void recordsReachKafkaInArrivalOrder() throws Exception {
    start(config());
    AppStream app = opened("s1");
    for (int i = 1; i <= 50; i++) {
      app.send(record(i, "both", "v" + i));
    }
    for (String cluster : new String[] {"a", "b"}) {
      assertThat(sent(cluster, 50).history())
          .extracting(r -> text(r.value()))
          .containsExactlyElementsOf(
              java.util.stream.IntStream.rangeClosed(1, 50).mapToObj(i -> "v" + i).toList());
    }
  }

  @Test
  void codecIsAppliedOnceAndEveryClusterGetsTheSameBytes() throws Exception {
    start(
        config()
            .setDefaultCodec("suffix")
            .putCodecs(
                "suffix",
                Codec.newBuilder()
                    .setValue(
                        Pipeline.newBuilder()
                            .addTransforms(
                                Transform.newBuilder()
                                    .setClassName(SuffixTransform.class.getName())))
                    .build()));
    AppStream app = opened("s1");
    app.send(record(1, "both", "v"));
    for (String cluster : new String[] {"a", "b"}) {
      ProducerRecord<byte[], byte[]> stored = sent(cluster, 1).history().get(0);
      assertThat(text(stored.value())).isEqualTo("v!");
      assertThat(stored.headers().headers("suffixed")).hasSize(1);
    }
  }

  @Test
  void recordTheCodecCannotEncodeIsInvalidAndNeverSent() throws Exception {
    start(
        config()
            .setDefaultCodec("suffix")
            .putCodecs(
                "suffix",
                Codec.newBuilder()
                    .setValue(
                        Pipeline.newBuilder()
                            .addTransforms(
                                Transform.newBuilder()
                                    .setClassName(SuffixTransform.class.getName())))
                    .build()));
    AppStream app = opened("s1");
    app.send(record(1, "single", "boom"));
    assertThat(app.result().getError().getCode()).isEqualTo(ErrorCode.ERROR_CODE_INVALID_RECORD);
    assertThat(kafka.get("a").history()).isEmpty();
  }

  @Test
  void duplicateOfARecordInFlightIsNotSentAgain() throws Exception {
    start(config());
    AppStream app = opened("s1");
    app.send(record(1, "single", "v"));
    app.send(record(1, "single", "v"));
    app.send(record(2, "single", "w"));
    MockProducer<byte[], byte[]> a = sent("a", 2);
    assertThat(a.history()).extracting(r -> text(r.value())).containsExactly("v", "w");
    a.completeNext();
    a.completeNext();
    assertThat(app.result().getSeq()).isEqualTo(1);
    assertThat(app.result().getSeq()).isEqualTo(2);
    assertThat(app.responses.poll(200, TimeUnit.MILLISECONDS)).isNull();
  }

  @Test
  void reconnectingSessionGetsStoredResultsWithoutProducingAgain() throws Exception {
    start(config());
    AppStream first = opened("s1");
    first.send(record(1, "single", "v"));
    sent("a", 1).completeNext();
    assertThat(first.result().getSeq()).isEqualTo(1);
    first.requests.onError(Status.CANCELLED.asRuntimeException());

    AppStream second = opened("s1");
    second.send(record(1, "single", "v"));
    ProduceResult replayed = second.result();
    assertThat(replayed.getSeq()).isEqualTo(1);
    assertThat(replayed.getWritesList()).hasSize(1);
    assertThat(kafka.get("a").history()).hasSize(1);
  }

  @Test
  void recordInFlightAcrossAReconnectIsAnsweredOnTheNewStream() throws Exception {
    start(config());
    AppStream first = opened("s1");
    first.send(record(1, "single", "v"));
    MockProducer<byte[], byte[]> a = sent("a", 1);
    first.requests.onError(Status.CANCELLED.asRuntimeException());
    first.status();

    AppStream second = opened("s1");
    second.send(record(1, "single", "v"));
    a.completeNext();
    assertThat(second.result().getSeq()).isEqualTo(1);
    assertThat(a.history()).hasSize(1);
  }

  @Test
  void resultOfARecordCompletedWhileDisconnectedIsKeptForTheResend() throws Exception {
    start(config());
    AppStream first = opened("s1");
    first.send(record(1, "single", "v"));
    MockProducer<byte[], byte[]> a = sent("a", 1);
    first.requests.onError(Status.CANCELLED.asRuntimeException());
    first.status();
    a.completeNext();

    AppStream second = opened("s1");
    second.send(record(1, "single", "v"));
    assertThat(second.result().hasError()).isFalse();
    assertThat(a.history()).hasSize(1);
  }

  @Test
  void secondStreamOnASessionDisplacesTheFirst() throws Exception {
    start(config());
    AppStream first = opened("s1");
    AppStream second = opened("s1");
    assertThat(first.status().getCode()).isEqualTo(Status.Code.ABORTED);
    second.send(record(1, "single", "v"));
    sent("a", 1).completeNext();
    assertThat(second.result().getSeq()).isEqualTo(1);
  }

  @Test
  void sessionsDoNotShareSequenceNumbers() throws Exception {
    start(config());
    AppStream one = opened("s1");
    AppStream two = opened("s2");
    one.send(record(1, "single", "from-one"));
    two.send(record(1, "single", "from-two"));
    MockProducer<byte[], byte[]> a = sent("a", 2);
    a.completeNext();
    a.completeNext();
    assertThat(one.result().getSeq()).isEqualTo(1);
    assertThat(two.result().getSeq()).isEqualTo(1);
  }

  @Test
  void resultsBelowTheLowWatermarkAreForgotten() throws Exception {
    start(config());
    AppStream app = opened("s1");
    app.send(record(1, "single", "v"));
    sent("a", 1).completeNext();
    app.result();
    app.send(record(2, "single", "w").setLowWatermark(2));
    sent("a", 2).completeNext();
    app.result();

    // seq 1 was pruned by low_watermark 2: it is neither replayed nor produced again.
    app.send(record(1, "single", "v").setLowWatermark(2));
    ProduceResult stale = app.result();
    assertThat(stale.getSeq()).isEqualTo(1);
    assertThat(stale.getError().getCode()).isEqualTo(ErrorCode.ERROR_CODE_PROTOCOL_VIOLATION);
    assertThat(kafka.get("a").history()).hasSize(2);
  }

  @Test
  void recordBeyondTheWindowIsRejectedAndASlotReturnsWithEachResult() throws Exception {
    start(config().setProduce(Produce.newBuilder().setMaxInFlightRecords(2)));
    AppStream app = opened("s1");
    app.send(record(1, "single", "a"));
    app.send(record(2, "single", "b"));
    app.send(record(3, "single", "c"));
    ProduceResult rejected = app.result();
    assertThat(rejected.getSeq()).isEqualTo(3);
    assertThat(rejected.getError().getCode()).isEqualTo(ErrorCode.ERROR_CODE_PROTOCOL_VIOLATION);
    MockProducer<byte[], byte[]> a = sent("a", 2);

    a.completeNext();
    assertThat(app.result().getSeq()).isEqualTo(1);
    app.send(record(4, "single", "d"));
    sent("a", 3);
  }

  @Test
  void byteWindowCountsKeyValueAndHeaders() throws Exception {
    start(config().setProduce(Produce.newBuilder().setMaxInFlightBytes(10)));
    AppStream app = opened("s1");
    app.send(
        record(1, "single", "12345")
            .setKey(ByteString.copyFromUtf8("123"))
            .addHeaders(Header.newBuilder().setKey("h").setValue(ByteString.copyFromUtf8("1"))));
    sent("a", 1);
    app.send(record(2, "single", "1"));
    assertThat(app.result().getError().getCode())
        .isEqualTo(ErrorCode.ERROR_CODE_PROTOCOL_VIOLATION);
  }

  @Test
  void firstMessageMustBeTheHandshake() throws Exception {
    start(config());
    AppStream app = stream();
    app.send(record(1, "single", "v"));
    assertThat(app.status().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    assertThat(kafka.get("a").history()).isEmpty();
  }

  @Test
  void handshakeNeedsASessionIdAndIsSentOnce() throws Exception {
    start(config());
    AppStream noId = stream();
    noId.open("");
    assertThat(noId.status().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);

    AppStream twice = opened("s1");
    twice.open("s1");
    assertThat(twice.status().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
  }

  @Test
  void sequenceNumbersStartAtOne() throws Exception {
    start(config());
    AppStream app = opened("s1");
    app.send(record(0, "single", "v"));
    assertThat(app.status().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
  }

  @Test
  void halfCloseCompletesAfterEveryRecordHasItsResult() throws Exception {
    start(config());
    AppStream app = opened("s1");
    app.send(record(1, "single", "v"));
    MockProducer<byte[], byte[]> a = sent("a", 1);
    app.requests.onCompleted();
    assertThat(app.closed).as("a record is still in flight").isNotDone();

    a.completeNext();
    assertThat(app.result().getSeq()).isEqualTo(1);
    assertThat(app.status().getCode()).isEqualTo(Status.Code.OK);
  }

  @Test
  void halfCloseWithNothingInFlightCompletesAtOnce() throws Exception {
    start(config());
    AppStream app = opened("s1");
    app.requests.onCompleted();
    assertThat(app.status().getCode()).isEqualTo(Status.Code.OK);
  }

  @Test
  void producerPropertiesAlwaysCarryTheGuarantees() {
    Map<String, Object> properties =
        ClusterProducers.properties(
            "a",
            Cluster.newBuilder()
                .putProperties("bootstrap.servers", "k:9092")
                .putProducerProperties("linger.ms", "5")
                .build());
    assertThat(properties)
        .containsEntry("bootstrap.servers", "k:9092")
        .containsEntry("linger.ms", "5")
        .containsEntry("acks", "all")
        .containsEntry("enable.idempotence", "true")
        .containsEntry("client.id", "odradek-a");
  }
}
