package io.github.bpalermo.odradek.server.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.bpalermo.odradek.config.v1.Cluster;
import io.github.bpalermo.odradek.config.v1.FileSource;
import io.github.bpalermo.odradek.config.v1.ServerConfig;
import io.github.bpalermo.odradek.config.v1.Tls;
import io.github.bpalermo.odradek.server.admin.HealthService.ServingStatus;
import io.github.bpalermo.odradek.server.tls.TestPki;
import io.github.bpalermo.odradek.server.tls.TlsSources;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.ClientCalls;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AdminServerTest {
  @TempDir Path dir;

  private final Readiness readiness = new Readiness();
  private AdminServer admin;
  private ManagedChannel channel;

  private void start(Clock clock) throws Exception {
    stop();
    admin = AdminServer.start(0, new HealthService(readiness, clock));
    channel = NettyChannelBuilder.forAddress("localhost", admin.port()).usePlaintext().build();
  }

  @AfterEach
  void stop() {
    if (channel != null) {
      channel.shutdownNow();
    }
    if (admin != null) {
      admin.close();
    }
  }

  private ServingStatus check(String service) {
    return ClientCalls.blockingUnaryCall(
        channel, HealthService.CHECK, CallOptions.DEFAULT, service);
  }

  @Test
  void notServingWhileStartingThenServingThenNotWhileStopping() throws Exception {
    start(Clock.systemUTC());
    assertThat(check("")).isEqualTo(ServingStatus.NOT_SERVING);
    assertThat(check(HealthService.LIVENESS)).isEqualTo(ServingStatus.SERVING);

    readiness.starting("loading TLS credentials");
    assertThat(check("")).isEqualTo(ServingStatus.NOT_SERVING);

    readiness.started(TlsSources.none());
    assertThat(check("")).isEqualTo(ServingStatus.SERVING);

    readiness.stopping();
    assertThat(check("")).isEqualTo(ServingStatus.NOT_SERVING);
    assertThat(check(HealthService.LIVENESS)).isEqualTo(ServingStatus.SERVING);
  }

  @Test
  void notServingOnceAClientCertificateHasExpired() throws Exception {
    TestPki pki = new TestPki(dir, "ca");
    TestPki.write(
        dir.resolve("tls"), pki.issue("client", "uri:spiffe://example.org/app"), pki.caPem());
    ServerConfig config =
        ServerConfig.newBuilder()
            .putClusters(
                "orders",
                Cluster.newBuilder()
                    .setTls(
                        Tls.newBuilder()
                            .setFiles(
                                FileSource.newBuilder().setDir(dir.resolve("tls").toString())))
                    .build())
            .build();
    try (TlsSources tls = TlsSources.create(config)) {
      readiness.started(tls);
      start(Clock.systemUTC());
      assertThat(check("")).isEqualTo(ServingStatus.SERVING);

      // The certificate lasts a day; nothing renewed it.
      Clock later = Clock.offset(Clock.systemUTC(), Duration.ofDays(3));
      assertThat(readiness.problems(later.instant()))
          .singleElement()
          .asString()
          .startsWith("clusters.orders.tls: the client certificate expired");
      start(later);
      assertThat(check("")).isEqualTo(ServingStatus.NOT_SERVING);
      assertThat(check(HealthService.LIVENESS)).isEqualTo(ServingStatus.SERVING);
    }
  }

  @Test
  void unknownServiceIsNotFound() throws Exception {
    start(Clock.systemUTC());
    assertThatThrownBy(() -> check("odradek.kafka.v1.OdradekService"))
        .isInstanceOfSatisfying(
            StatusRuntimeException.class,
            e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND));
  }
}
