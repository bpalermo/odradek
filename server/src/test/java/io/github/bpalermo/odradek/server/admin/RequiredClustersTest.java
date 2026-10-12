package io.github.bpalermo.odradek.server.admin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.bpalermo.odradek.config.v1.Cluster;
import io.github.bpalermo.odradek.config.v1.FileSource;
import io.github.bpalermo.odradek.config.v1.ServerConfig;
import io.github.bpalermo.odradek.config.v1.Tls;
import io.github.bpalermo.odradek.server.tls.TestPki;
import io.github.bpalermo.odradek.server.tls.TlsSources;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RequiredClustersTest {
  private static final Duration NO_DELAY = Duration.ofMillis(1);
  private static final Duration PROGRESS = Duration.ofSeconds(30);

  @TempDir Path dir;

  private final Readiness readiness = new Readiness();

  private static Cluster cluster(String bootstrap, boolean required) {
    return Cluster.newBuilder()
        .setRequired(required)
        .putProperties("bootstrap.servers", bootstrap)
        .putProperties("client.dns.lookup", "use_all_dns_ips")
        // Not for an Admin client.
        .putProducerProperties("linger.ms", "5")
        .putConsumerProperties("max.poll.records", "10")
        .build();
  }

  private List<String> problems() {
    return readiness.problems(Instant.now());
  }

  @Test
  void asksOnlyRequiredClustersInNameOrderWithTheirSharedProperties() throws Exception {
    ServerConfig config =
        ServerConfig.newBuilder()
            .putClusters("orders", cluster("orders:9092", true))
            .putClusters("analytics", cluster("analytics:9092", false))
            .putClusters("billing", cluster("billing:9092", true))
            .build();
    List<String> asked = new ArrayList<>();
    List<List<String>> steps = new ArrayList<>();
    List<Map<String, Object>> seen = new ArrayList<>();

    RequiredClusters.await(
        config,
        TlsSources.none(),
        readiness,
        (name, properties) -> {
          asked.add(name);
          steps.add(problems());
          seen.add(properties);
        },
        NO_DELAY,
        PROGRESS);

    assertThat(asked).containsExactly("billing", "orders");
    assertThat(steps)
        .containsExactly(
            List.of("starting: waiting for cluster billing"),
            List.of("starting: waiting for cluster orders"));
    assertThat(seen.get(0))
        .containsOnly(
            Map.entry("bootstrap.servers", "billing:9092"),
            Map.entry("client.dns.lookup", "use_all_dns_ips"),
            Map.entry("client.id", "odradek-billing-startup"),
            Map.entry("metadata.recovery.strategy", "none"));
  }

  @Test
  void retriesUntilTheClusterAnswersAndIsNotReadyMeanwhile() throws Exception {
    ServerConfig config =
        ServerConfig.newBuilder().putClusters("orders", cluster("orders:9092", true)).build();
    List<List<String>> steps = new ArrayList<>();

    RequiredClusters.await(
        config,
        TlsSources.none(),
        readiness,
        (name, properties) -> {
          steps.add(problems());
          switch (steps.size()) {
            case 1 -> throw new ExecutionException(new TimeoutException("no node"));
            case 2 -> throw new ExecutionException(new TimeoutException("no node"));
            case 3 -> throw new IllegalStateException("no resolvable bootstrap urls");
            default -> {}
          }
        },
        NO_DELAY,
        // Every failed attempt also logs the wait.
        Duration.ZERO);

    assertThat(steps).hasSize(4).containsOnly(List.of("starting: waiting for cluster orders"));
    // Still starting: becoming ready is for the caller, once the socket is listening.
    assertThat(problems()).containsExactly("starting: waiting for cluster orders");
    readiness.started(TlsSources.none());
    assertThat(problems()).isEmpty();
  }

  @Test
  void aConfigurationWithoutRequiredClustersAsksNothing() throws Exception {
    ServerConfig config =
        ServerConfig.newBuilder().putClusters("orders", cluster("orders:9092", false)).build();
    readiness.starting("loading TLS credentials");

    RequiredClusters.await(
        config,
        TlsSources.none(),
        readiness,
        (name, properties) -> {
          throw new AssertionError("asked " + name);
        },
        NO_DELAY,
        PROGRESS);

    assertThat(problems()).containsExactly("starting: loading TLS credentials");
  }

  @Test
  void aClusterWithTlsIsAskedWithTheServersTlsSettings() throws Exception {
    TestPki pki = new TestPki(dir, "ca");
    TestPki.write(
        dir.resolve("tls"), pki.issue("client", "uri:spiffe://example.org/app"), pki.caPem());
    ServerConfig config =
        ServerConfig.newBuilder()
            .putClusters(
                "orders",
                cluster("orders:9093", true).toBuilder()
                    .setTls(
                        Tls.newBuilder()
                            .setFiles(
                                FileSource.newBuilder().setDir(dir.resolve("tls").toString())))
                    .build())
            .build();
    AtomicReference<Map<String, Object>> seen = new AtomicReference<>();

    try (TlsSources tls = TlsSources.create(config)) {
      RequiredClusters.await(
          config, tls, readiness, (name, properties) -> seen.set(properties), NO_DELAY, PROGRESS);
    }

    assertThat(seen.get())
        .containsEntry("security.protocol", "SSL")
        .containsKey("ssl.engine.factory.class");
  }

  @Test
  void anInterruptEndsTheWait() throws Exception {
    ServerConfig config =
        ServerConfig.newBuilder().putClusters("orders", cluster("orders:9092", true)).build();
    CountDownLatch asked = new CountDownLatch(1);
    AtomicReference<Throwable> thrown = new AtomicReference<>();
    Thread waiter =
        new Thread(
            () -> {
              try {
                RequiredClusters.await(
                    config,
                    TlsSources.none(),
                    readiness,
                    (name, properties) -> {
                      asked.countDown();
                      throw new IllegalStateException("unreachable");
                    },
                    Duration.ofMinutes(1),
                    PROGRESS);
              } catch (Throwable e) {
                thrown.set(e);
              }
            });
    waiter.start();
    assertThat(asked.await(10, TimeUnit.SECONDS)).isTrue();
    waiter.interrupt();
    waiter.join(10_000);

    assertThat(thrown.get()).isInstanceOf(InterruptedException.class);
  }
}
