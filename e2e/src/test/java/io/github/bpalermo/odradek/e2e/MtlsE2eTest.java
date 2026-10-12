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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
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
 * The server against a broker that only accepts mutual TLS (e2e/tls/broker.sh): it presents a
 * client certificate read from files, accepts the broker by its SPIFFE ID, and picks up replaced
 * files without a restart.
 */
class MtlsE2eTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(60);

  /** The TLS listener, and a plaintext one of the same broker for creating topics. */
  private static final String KAFKA_TLS = ServerHarness.bootstrap("ODRADEK_E2E_KAFKA_TLS");

  private static final String KAFKA_TLS_ADMIN =
      ServerHarness.bootstrap("ODRADEK_E2E_KAFKA_TLS_ADMIN");

  /** Where broker.sh left the certificates: client/ and client-next/. */
  private static final Path CERTIFICATES = Path.of(ServerHarness.bootstrap("ODRADEK_E2E_TLS_DIR"));

  private static final String BROKER_ID = "spiffe://e2e.test/broker";

  private static final String CONFIG =
      """
      listener:
        socketPath: %s/kafka.sock
      clusters:
        a:
          properties:
            bootstrap.servers: %s
          producerProperties:
            # Fail fast when the broker is refused, instead of after Kafka's two minutes.
            max.block.ms: 5000
            delivery.timeout.ms: 5000
            request.timeout.ms: 2000
          tls:
            files:
              dir: %s
            acceptedSpiffeIds: ["%s"]
      producers:
        %s:
          clusters: [{cluster: a}]
      consumers:
        %s:
          cluster: a
          groupId: group-%s
      """;

  private final String id = UUID.randomUUID().toString();
  private final String topic = "odradek-e2e-mtls-" + id;

  private Path credentials;
  private ServerHarness harness;

  @BeforeEach
  void prepare() throws Exception {
    ServerHarness.createTopic(KAFKA_TLS_ADMIN, topic, 1);
    // The server's own copy, so that a test can replace the files.
    credentials = Files.createTempDirectory(Path.of("/tmp"), "odradek-e2e-tls");
    install("client");
  }

  @AfterEach
  void stop() throws Exception {
    if (harness != null) {
      harness.stop();
    }
    ServerHarness.deleteTopics(KAFKA_TLS_ADMIN, List.of(topic));
    try (var files = Files.list(credentials)) {
      for (Path file : files.toList()) {
        Files.delete(file);
      }
    }
    Files.delete(credentials);
  }

  @Test
  void producesAndConsumesOverMutualTls() throws Exception {
    harness = ServerHarness.start(CONFIG, KAFKA_TLS, credentials, BROKER_ID, topic, topic, id);
    assertThat(produce(1, 20)).isEmpty();
    assertThat(consume(20)).hasSize(20);
  }

  @Test
  void replacedCredentialFilesAreUsedWithoutARestart() throws Exception {
    harness = ServerHarness.start(CONFIG, KAFKA_TLS, credentials, BROKER_ID, topic, topic, id);
    assertThat(produce(1, 10)).isEmpty();

    install("client-next");
    // Longer than the server's poll of the directory (5 s).
    Thread.sleep(8_000);

    // A new consume stream is a new Kafka consumer: new connections, new handshakes.
    assertThat(produce(11, 20)).isEmpty();
    assertThat(consume(20)).hasSize(20);
  }

  @Test
  void brokerWithAnotherSpiffeIdIsNotTalkedTo() throws Exception {
    harness =
        ServerHarness.start(
            CONFIG, KAFKA_TLS, credentials, "spiffe://e2e.test/another-broker", topic, topic, id);
    assertThat(produce(1, 1)).hasSize(1);
  }

  private void install(String name) throws Exception {
    for (String file : List.of("tls.crt", "tls.key", "ca.crt")) {
      Files.copy(
          CERTIFICATES.resolve(name).resolve(file),
          credentials.resolve(file),
          StandardCopyOption.REPLACE_EXISTING);
    }
  }

  /** Produces records {@code from..to} and returns the errors of those that failed. */
  private List<String> produce(int from, int to) throws Exception {
    BlockingQueue<ProduceResponse> responses = new LinkedBlockingQueue<>();
    StreamObserver<ProduceRequest> produce =
        OdradekServiceGrpc.newStub(harness.channel)
            .produce(
                new StreamObserver<ProduceResponse>() {
                  @Override
                  public void onNext(ProduceResponse response) {
                    responses.add(response);
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
    for (int i = from; i <= to; i++) {
      produce.onNext(
          ProduceRequest.newBuilder()
              .setRecord(
                  ProduceRecord.newBuilder()
                      .setSeq(i - from + 1)
                      .setLowWatermark(1)
                      .setTopic(topic)
                      .setValue(ByteString.copyFromUtf8("record-" + i)))
              .build());
    }
    List<String> errors = new ArrayList<>();
    int results = 0;
    while (results < to - from + 1) {
      ProduceResponse response = responses.poll(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
      assertThat(response).as("a response within %s", TIMEOUT).isNotNull();
      if (response.hasResult()) {
        results++;
        if (response.getResult().hasError()) {
          errors.add(response.getResult().getError().toString());
        }
      }
    }
    produce.onCompleted();
    return errors;
  }

  /** Consumes on a new stream until {@code count} distinct records were seen, or the timeout. */
  private Set<String> consume(int count) throws Exception {
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
    while (seen.size() < count && System.nanoTime() < deadline) {
      ConsumeResponse response = consumed.poll(1, TimeUnit.SECONDS);
      if (response != null && response.hasRecords()) {
        for (ConsumedRecord record : response.getRecords().getRecordsList()) {
          seen.add(record.getValue().toStringUtf8());
        }
      }
    }
    consume.onCompleted();
    return seen;
  }
}
