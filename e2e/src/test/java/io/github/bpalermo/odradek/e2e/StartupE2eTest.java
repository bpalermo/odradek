package io.github.bpalermo.odradek.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.bpalermo.odradek.server.admin.HealthService;
import io.github.bpalermo.odradek.server.admin.HealthService.ServingStatus;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.ClientCalls;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The packaged server's startup gate: it neither listens on its socket nor answers SERVING until
 * every cluster marked {@code required} has answered a metadata request. Needs the server as a
 * process; the in-process harness runs neither {@code Main} nor the health service.
 */
class StartupE2eTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(60);

  private static final String BROKER_ID = "spiffe://e2e.test/broker";

  private Path dir;
  private Process server;
  private ManagedChannel probe;

  @BeforeEach
  void prepare() throws Exception {
    String executable = System.getenv("ODRADEK_E2E_SERVER");
    Assumptions.assumeTrue(
        executable != null && !executable.isBlank(), "needs the packaged server");
    dir = Files.createTempDirectory(Path.of("/tmp"), "odradek-e2e-startup");
  }

  @AfterEach
  void stop() throws Exception {
    if (probe != null) {
      probe.shutdownNow();
    }
    if (server != null) {
      server.destroy();
      if (!server.waitFor(60, TimeUnit.SECONDS)) {
        server.destroyForcibly();
      }
    }
    if (dir != null) {
      Files.deleteIfExists(dir.resolve("odradek.yaml"));
      Files.deleteIfExists(socket());
      Files.deleteIfExists(dir);
    }
  }

  private Path socket() {
    return dir.resolve("kafka.sock");
  }

  /** A local port that nothing listens on: a connection to it is refused. */
  private static int unusedPort() throws Exception {
    try (ServerSocket free = new ServerSocket(0)) {
      return free.getLocalPort();
    }
  }

  /** Starts the server with these clusters and opens a channel to its health service. */
  private void start(String clusters) throws Exception {
    int port = unusedPort();
    Path file = dir.resolve("odradek.yaml");
    Files.writeString(
        file,
        "admin: {port: %d}\nlistener:\n  socketPath: %s\nclusters:\n%s"
            .formatted(port, socket(), clusters));
    server =
        new ProcessBuilder(
                Path.of(System.getenv("ODRADEK_E2E_SERVER")).toAbsolutePath().toString(),
                "--config",
                file.toString())
            .inheritIO()
            .start();
    probe = NettyChannelBuilder.forAddress("localhost", port).usePlaintext().build();
  }

  private ServingStatus check(String service) {
    return ClientCalls.blockingUnaryCall(
        probe,
        HealthService.CHECK,
        CallOptions.DEFAULT.withDeadlineAfter(5, TimeUnit.SECONDS),
        service);
  }

  /** Asks until the answer is {@code expected}, the server needing a moment to start. */
  private void await(String service, ServingStatus expected) throws Exception {
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (true) {
      try {
        if (check(service) == expected) {
          return;
        }
      } catch (StatusRuntimeException e) {
        // Not listening yet.
      }
      assertThat(server.isAlive()).as("the server is running").isTrue();
      assertThat(System.nanoTime()).as("%s within %s", expected, TIMEOUT).isLessThan(deadline);
      Thread.sleep(100);
    }
  }

  @Test
  void aRequiredClusterThatDoesNotAnswerKeepsTheServerFromStarting() throws Exception {
    start(
        """
          a:
            required: true
            properties:
              bootstrap.servers: localhost:%d
        """
            .formatted(unusedPort()));

    await("", ServingStatus.NOT_SERVING);
    await(HealthService.LIVENESS, ServingStatus.SERVING);
    // It stays that way, over several attempts' worth of time, and the process does not give up.
    for (int i = 0; i < 30; i++) {
      Thread.sleep(500);
      assertThat(check("")).isEqualTo(ServingStatus.NOT_SERVING);
      assertThat(Files.exists(socket())).as("the app's socket exists").isFalse();
      assertThat(server.isAlive()).isTrue();
    }
  }

  @Test
  void aRequiredClusterThatAnswersLetsTheServerStart() throws Exception {
    // The optional cluster answers nobody, and nobody asks it.
    start(
        """
          a:
            required: true
            properties:
              bootstrap.servers: %s
          b:
            properties:
              bootstrap.servers: localhost:%d
        """
            .formatted(ServerHarness.KAFKA_A, unusedPort()));

    await("", ServingStatus.SERVING);
    assertThat(Files.exists(socket())).as("the app's socket exists").isTrue();
  }

  /** The broker refuses a client without a certificate, so the Admin client presented one. */
  @Test
  void aRequiredClusterBehindMutualTlsIsAskedWithTheClientCertificate() throws Exception {
    Path certificates = Path.of(ServerHarness.bootstrap("ODRADEK_E2E_TLS_DIR")).resolve("client");
    start(
        """
          a:
            required: true
            properties:
              bootstrap.servers: %s
            tls:
              files: {dir: %s}
              acceptedSpiffeIds: ["%s"]
        """
            .formatted(ServerHarness.bootstrap("ODRADEK_E2E_KAFKA_TLS"), certificates, BROKER_ID));

    await("", ServingStatus.SERVING);
    assertThat(Files.exists(socket())).as("the app's socket exists").isTrue();
  }

  /** The same broker under another identity: the Admin client applies the peer policy too. */
  @Test
  void aRequiredClusterWithAnotherSpiffeIdIsNotAccepted() throws Exception {
    Path certificates = Path.of(ServerHarness.bootstrap("ODRADEK_E2E_TLS_DIR")).resolve("client");
    start(
        """
          a:
            required: true
            properties:
              bootstrap.servers: %s
            tls:
              files: {dir: %s}
              acceptedSpiffeIds: ["spiffe://e2e.test/another-broker"]
        """
            .formatted(ServerHarness.bootstrap("ODRADEK_E2E_KAFKA_TLS"), certificates));

    await("", ServingStatus.NOT_SERVING);
    for (int i = 0; i < 10; i++) {
      Thread.sleep(500);
      assertThat(check("")).isEqualTo(ServingStatus.NOT_SERVING);
      assertThat(Files.exists(socket())).as("the app's socket exists").isFalse();
    }
  }
}
