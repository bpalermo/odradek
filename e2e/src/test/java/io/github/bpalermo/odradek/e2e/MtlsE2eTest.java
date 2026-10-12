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
import io.github.bpalermo.odradek.server.tls.FakeWorkloadApi;
import io.grpc.stub.StreamObserver;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The server against a broker that only accepts mutual TLS (e2e/tls/broker.sh): it presents a
 * client certificate read from files or received from a Workload API, accepts the broker by its
 * SPIFFE ID, and picks up a replaced certificate without a restart.
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
            %s
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
  private FakeWorkloadApi agent;

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
    if (agent != null) {
      agent.close();
    }
    ServerHarness.deleteTopics(KAFKA_TLS_ADMIN, List.of(topic));
    try (var files = Files.list(credentials)) {
      for (Path file : files.toList()) {
        Files.delete(file);
      }
    }
    Files.delete(credentials);
  }

  private ServerHarness startWithFiles(String brokerId) throws Exception {
    return ServerHarness.start(
        CONFIG, KAFKA_TLS, "files: {dir: " + credentials + "}", brokerId, topic, topic, id);
  }

  /** Starts a stand-in SPIRE agent that hands out the client certificate, and the server on it. */
  private ServerHarness startWithWorkloadApi() throws Exception {
    Path socket = credentials.resolve("agent.sock");
    agent = FakeWorkloadApi.start(socket);
    publish("client");
    return ServerHarness.start(
        CONFIG,
        KAFKA_TLS,
        "spiffe: {workloadApiSocket: \"unix://" + socket + "\"}",
        BROKER_ID,
        topic,
        topic,
        id);
  }

  private void publish(String name) throws Exception {
    Path dir = CERTIFICATES.resolve(name);
    agent.publish(
        "spiffe://e2e.test/odradek",
        Files.readString(dir.resolve("tls.crt")),
        Files.readString(dir.resolve("tls.key")),
        Files.readString(dir.resolve("ca.crt")),
        Map.of());
  }

  @Test
  void producesAndConsumesOverMutualTls() throws Exception {
    harness = startWithFiles(BROKER_ID);
    assertThat(produce(1, 20)).isEmpty();
    assertThat(consume(20)).hasSize(20);
  }

  @Test
  void replacedCredentialFilesAreUsedWithoutARestart() throws Exception {
    harness = startWithFiles(BROKER_ID);
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
    harness = startWithFiles("spiffe://e2e.test/another-broker");
    assertThat(produce(1, 1)).hasSize(1);
  }

  @Test
  void producesAndConsumesWithAnSvidFromTheWorkloadApi() throws Exception {
    harness = startWithWorkloadApi();
    assertThat(produce(1, 10)).isEmpty();

    // A rotation, pushed by the agent.
    publish("client-next");
    Thread.sleep(2_000);

    assertThat(produce(11, 20)).isEmpty();
    assertThat(consume(20)).hasSize(20);
  }

  /**
   * The packaged server's readiness endpoint: not ready while it waits for its first SVID, ready
   * once the agent has handed one out. Needs the server as a process; the in-process harness does
   * not run the admin endpoint.
   */
  @Test
  void readinessFollowsTheFirstSvid() throws Exception {
    String executable = System.getenv("ODRADEK_E2E_SERVER");
    Assumptions.assumeTrue(
        executable != null && !executable.isBlank(), "needs the packaged server");
    Path socket = credentials.resolve("agent.sock");
    agent = FakeWorkloadApi.start(socket);
    int port;
    try (ServerSocket free = new ServerSocket(0)) {
      port = free.getLocalPort();
    }
    Path file = credentials.resolve("odradek.yaml");
    Files.writeString(
        file,
        "admin: {port: "
            + port
            + "}\n"
            + CONFIG.formatted(
                credentials,
                KAFKA_TLS,
                "spiffe: {workloadApiSocket: \"unix://" + socket + "\"}",
                BROKER_ID,
                topic,
                topic,
                id));
    Process server =
        new ProcessBuilder(
                Path.of(executable).toAbsolutePath().toString(), "--config", file.toString())
            .inheritIO()
            .start();
    try (HttpClient http = HttpClient.newHttpClient()) {
      HttpRequest ready =
          HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/readyz")).build();
      // The agent has nothing to hand out yet.
      HttpResponse<String> waiting = await(http, ready, 503);
      assertThat(waiting.body()).contains("loading TLS credentials");
      assertThat(Files.exists(credentials.resolve("kafka.sock"))).isFalse();

      publish("client");
      assertThat(await(http, ready, 200).body()).isEqualTo("ready\n");
      assertThat(Files.exists(credentials.resolve("kafka.sock"))).isTrue();
    } finally {
      server.destroy();
      if (!server.waitFor(60, TimeUnit.SECONDS)) {
        server.destroyForcibly();
      }
      Files.deleteIfExists(file);
      Files.deleteIfExists(credentials.resolve("kafka.sock"));
    }
  }

  /** Polls until the endpoint answers with {@code status}, the server needing a moment to start. */
  private static HttpResponse<String> await(HttpClient http, HttpRequest request, int status)
      throws Exception {
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (true) {
      try {
        HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == status) {
          return response;
        }
      } catch (java.io.IOException e) {
        // Not listening yet.
      }
      assertThat(System.nanoTime()).as("answer %s within %s", status, TIMEOUT).isLessThan(deadline);
      Thread.sleep(100);
    }
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
