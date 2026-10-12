package io.github.bpalermo.odradek.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import io.github.bpalermo.odradek.kafka.v1.ConsumeRequest;
import io.github.bpalermo.odradek.kafka.v1.ConsumeResponse;
import io.github.bpalermo.odradek.kafka.v1.Credits;
import io.github.bpalermo.odradek.kafka.v1.OdradekServiceGrpc;
import io.github.bpalermo.odradek.kafka.v1.ProduceOpen;
import io.github.bpalermo.odradek.kafka.v1.ProduceRecord;
import io.github.bpalermo.odradek.kafka.v1.ProduceRequest;
import io.github.bpalermo.odradek.kafka.v1.ProduceResponse;
import io.github.bpalermo.odradek.kafka.v1.Subscribe;
import io.github.bpalermo.odradek.server.telemetry.FakeCollector;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The packaged server pushes its metrics over OTLP/gRPC to the collector its configuration names.
 * Needs the server as a process: the in-process harness records metrics nowhere.
 */
class TelemetryE2eTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(60);
  private static final int RECORDS = 5;

  private final String id = UUID.randomUUID().toString();
  private final String topic = "odradek-e2e-telemetry-" + id;

  private ServerHarness harness;
  private FakeCollector collector;

  @BeforeEach
  void prepare() throws Exception {
    String executable = System.getenv("ODRADEK_E2E_SERVER");
    Assumptions.assumeTrue(
        executable != null && !executable.isBlank(), "needs the packaged server");
    ServerHarness.createTopic(ServerHarness.KAFKA_A, topic, 1);
    collector = FakeCollector.start();
  }

  @AfterEach
  void stop() throws Exception {
    if (harness != null) {
      harness.stop();
    }
    if (collector != null) {
      collector.close();
      ServerHarness.deleteTopics(ServerHarness.KAFKA_A, List.of(topic));
    }
  }

  @Test
  void producedAndConsumedRecordsShowUpInThePushedMetrics() throws Exception {
    harness =
        ServerHarness.start(
            """
            listener:
              socketPath: %s/kafka.sock
            telemetry:
              endpoint: %s
              exportInterval: 1s
              resourceAttributes:
                k8s.pod.name: pod-%s
            clusters:
              a:
                properties:
                  bootstrap.servers: %s
            producers:
              %s:
                clusters: [{cluster: a}]
            consumers:
              %s:
                cluster: a
                groupId: group-%s
            """,
            collector.endpoint(), id, ServerHarness.KAFKA_A, topic, topic, id);

    BlockingQueue<ProduceResponse> results = new LinkedBlockingQueue<>();
    StreamObserver<ProduceRequest> produce =
        OdradekServiceGrpc.newStub(harness.channel).produce(collecting(results));
    produce.onNext(
        ProduceRequest.newBuilder()
            .setOpen(ProduceOpen.newBuilder().setSessionId(UUID.randomUUID().toString()))
            .build());
    for (int i = 1; i <= RECORDS; i++) {
      produce.onNext(
          ProduceRequest.newBuilder()
              .setRecord(
                  ProduceRecord.newBuilder()
                      .setSeq(i)
                      .setLowWatermark(1)
                      .setTopic(topic)
                      .setValue(ByteString.copyFromUtf8("record-" + i)))
              .build());
    }
    for (int answered = 0; answered < RECORDS; ) {
      ProduceResponse response = results.poll(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
      assertThat(response).as("a response within %s", TIMEOUT).isNotNull();
      if (response.hasResult()) {
        assertThat(response.getResult().hasError()).as("%s", response).isFalse();
        answered++;
      }
    }
    produce.onCompleted();

    BlockingQueue<ConsumeResponse> consumed = new LinkedBlockingQueue<>();
    StreamObserver<ConsumeRequest> consume =
        OdradekServiceGrpc.newStub(harness.channel).consume(collecting(consumed));
    consume.onNext(
        ConsumeRequest.newBuilder().setSubscribe(Subscribe.newBuilder().addTopics(topic)).build());
    consume.onNext(
        ConsumeRequest.newBuilder().setCredits(Credits.newBuilder().setRecords(100)).build());
    int seen = 0;
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (seen < RECORDS && System.nanoTime() < deadline) {
      ConsumeResponse response = consumed.poll(1, TimeUnit.SECONDS);
      if (response != null && response.hasRecords()) {
        seen += response.getRecords().getRecordsCount();
      }
    }
    assertThat(seen).isEqualTo(RECORDS);

    // One export carries every metric that has a value, with the resource they belong to.
    collector.await(
        TIMEOUT,
        "odradek.produce.records",
        "odradek.produce.cluster.writes",
        "odradek.consume.records",
        "odradek.consume.streams",
        "odradek.ready",
        topic,
        "group-" + id,
        "pod-" + id);
    consume.onCompleted();
  }

  private static <T> StreamObserver<T> collecting(BlockingQueue<T> into) {
    return new StreamObserver<>() {
      @Override
      public void onNext(T response) {
        into.add(response);
      }

      @Override
      public void onError(Throwable t) {}

      @Override
      public void onCompleted() {}
    };
  }
}
