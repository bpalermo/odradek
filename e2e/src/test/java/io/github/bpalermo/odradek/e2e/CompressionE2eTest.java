package io.github.bpalermo.odradek.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import io.github.bpalermo.odradek.kafka.v1.ConsumeRequest;
import io.github.bpalermo.odradek.kafka.v1.ConsumeResponse;
import io.github.bpalermo.odradek.kafka.v1.ConsumedRecord;
import io.github.bpalermo.odradek.kafka.v1.Credits;
import io.github.bpalermo.odradek.kafka.v1.OdradekServiceGrpc;
import io.github.bpalermo.odradek.kafka.v1.ProduceOpen;
import io.github.bpalermo.odradek.kafka.v1.ProduceRecord;
import io.github.bpalermo.odradek.kafka.v1.ProduceRequest;
import io.github.bpalermo.odradek.kafka.v1.ProduceResponse;
import io.github.bpalermo.odradek.kafka.v1.Subscribe;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Records survive every compression codec of the Kafka clients, in both directions through the
 * server: it compresses what it produces and decompresses what it consumes. Three of the four
 * codecs are native libraries (snappy, lz4, zstd) loaded by the client, which a native image of the
 * server has to get right.
 */
class CompressionE2eTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(60);
  private static final int RECORDS = 50;

  private final String id = UUID.randomUUID().toString();
  private final String topic = "odradek-e2e-compression-" + id;

  private ServerHarness harness;

  @BeforeEach
  void createTopic() throws Exception {
    ServerHarness.createTopic(ServerHarness.KAFKA_A, topic, 2);
  }

  @AfterEach
  void stop() throws Exception {
    if (harness != null) {
      harness.stop();
    }
    ServerHarness.deleteTopics(ServerHarness.KAFKA_A, List.of(topic));
  }

  @Test
  void gzip() throws Exception {
    roundTrip("gzip");
  }

  @Test
  void snappy() throws Exception {
    roundTrip("snappy");
  }

  @Test
  void lz4() throws Exception {
    roundTrip("lz4");
  }

  @Test
  void zstd() throws Exception {
    roundTrip("zstd");
  }

  private void roundTrip(String codec) throws Exception {
    harness =
        ServerHarness.start(
            """
            listener:
              socketPath: %s/kafka.sock
            clusters:
              a:
                properties:
                  bootstrap.servers: %s
                producerProperties:
                  compression.type: %s
                  # Batch everything: a compressed batch is only worth testing if it has records.
                  linger.ms: 100
            producers:
              %s:
                clusters: [{cluster: a}]
            consumers:
              %s:
                cluster: a
                groupId: group-%s
            """,
            ServerHarness.KAFKA_A, codec, topic, topic, id);

    Set<String> expected = new TreeSet<>();
    BlockingQueue<ProduceResponse> results = new LinkedBlockingQueue<>();
    StreamObserver<ProduceRequest> produce =
        OdradekServiceGrpc.newStub(harness.channel)
            .produce(
                new StreamObserver<ProduceResponse>() {
                  @Override
                  public void onNext(ProduceResponse response) {
                    results.add(response);
                  }

                  @Override
                  public void onError(Throwable t) {}

                  @Override
                  public void onCompleted() {}
                });
    produce.onNext(
        ProduceRequest.newBuilder()
            .setOpen(ProduceOpen.newBuilder().setSessionId(UUID.randomUUID().toString()))
            .build());
    for (int i = 1; i <= RECORDS; i++) {
      // Repetitive on purpose: it compresses, so the codec really runs.
      String value = ("record-" + i + "-").repeat(40);
      expected.add(value);
      produce.onNext(
          ProduceRequest.newBuilder()
              .setRecord(
                  ProduceRecord.newBuilder()
                      .setSeq(i)
                      .setLowWatermark(1)
                      .setTopic(topic)
                      .setKey(ByteString.copyFromUtf8("key-" + i))
                      .setValue(ByteString.copyFromUtf8(value)))
              .build());
    }
    for (int i = 0; i <= RECORDS; i++) {
      ProduceResponse response = results.poll(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
      assertThat(response).as("a response within %s", TIMEOUT).isNotNull();
      assertThat(response.getResult().hasError()).as("%s", response).isFalse();
    }
    produce.onCompleted();

    BlockingQueue<ConsumeResponse> consumed = new LinkedBlockingQueue<>();
    StreamObserver<ConsumeRequest> consume =
        OdradekServiceGrpc.newStub(harness.channel)
            .consume(
                new StreamObserver<ConsumeResponse>() {
                  @Override
                  public void onNext(ConsumeResponse response) {
                    consumed.add(response);
                  }

                  @Override
                  public void onError(Throwable t) {}

                  @Override
                  public void onCompleted() {}
                });
    consume.onNext(
        ConsumeRequest.newBuilder().setSubscribe(Subscribe.newBuilder().addTopics(topic)).build());
    consume.onNext(
        ConsumeRequest.newBuilder().setCredits(Credits.newBuilder().setRecords(1000)).build());
    Set<String> seen = new TreeSet<>();
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (seen.size() < RECORDS && System.nanoTime() < deadline) {
      ConsumeResponse response = consumed.poll(1, TimeUnit.SECONDS);
      if (response != null && response.hasRecords()) {
        for (ConsumedRecord record : response.getRecords().getRecordsList()) {
          seen.add(record.getValue().toStringUtf8());
        }
      }
    }
    consume.onCompleted();
    assertThat(seen).isEqualTo(expected);
  }
}
