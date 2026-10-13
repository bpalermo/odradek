package io.github.bpalermo.odradek.server.telemetry;

import static io.opentelemetry.sdk.testing.assertj.OpenTelemetryAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.util.Durations;
import io.github.bpalermo.odradek.config.v1.Cluster;
import io.github.bpalermo.odradek.config.v1.Dispatch;
import io.github.bpalermo.odradek.config.v1.Dispatcher;
import io.github.bpalermo.odradek.config.v1.FileSource;
import io.github.bpalermo.odradek.config.v1.ListenerTls;
import io.github.bpalermo.odradek.config.v1.ServerConfig;
import io.github.bpalermo.odradek.config.v1.Tls;
import io.github.bpalermo.odradek.server.admin.Readiness;
import io.github.bpalermo.odradek.server.tls.TestPki;
import io.github.bpalermo.odradek.server.tls.TlsSources;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.Collection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TelemetryTest {
  @TempDir Path dir;

  private final InMemoryMetricReader reader = InMemoryMetricReader.create();
  private final SdkMeterProvider provider =
      SdkMeterProvider.builder().registerMetricReader(reader).build();
  private final ServerMetrics metrics = new ServerMetrics(provider.get("odradek"));

  @AfterEach
  void stop() {
    provider.close();
  }

  private MetricData metric(String name) {
    Collection<MetricData> all = reader.collectAllMetrics();
    return all.stream()
        .filter(m -> m.getName().equals(name))
        .findFirst()
        .orElseThrow(() -> new AssertionError(name + " not among " + all));
  }

  @Test
  void produceOutcomesAreCountedByTopicAndCode() {
    metrics.produced("orders", null);
    metrics.produced("orders", null);
    metrics.produced("orders", "ERROR_CODE_OUTCOME_UNKNOWN");
    metrics.clusterWrite("orders", "analytics", false, false);

    assertThat(metric("odradek.produce.records"))
        .hasLongSumSatisfying(
            sum ->
                sum.isMonotonic()
                    .hasPointsSatisfying(
                        point ->
                            point
                                .hasValue(2)
                                .hasAttributes(
                                    Attributes.of(
                                        ServerMetrics.TOPIC,
                                        "orders",
                                        ServerMetrics.OUTCOME,
                                        "ok")),
                        point ->
                            point
                                .hasValue(1)
                                .hasAttributes(
                                    Attributes.of(
                                        ServerMetrics.TOPIC,
                                        "orders",
                                        ServerMetrics.OUTCOME,
                                        "error",
                                        ServerMetrics.ERROR_CODE,
                                        "ERROR_CODE_OUTCOME_UNKNOWN"))));
    assertThat(metric("odradek.produce.cluster.writes"))
        .hasLongSumSatisfying(
            sum ->
                sum.hasPointsSatisfying(
                    point ->
                        point
                            .hasValue(1)
                            .hasAttributes(
                                Attributes.of(
                                    ServerMetrics.TOPIC,
                                    "orders",
                                    ServerMetrics.CLUSTER,
                                    "analytics",
                                    ServerMetrics.REQUIRED,
                                    false,
                                    ServerMetrics.OUTCOME,
                                    "error"))));
  }

  @Test
  void streamsGoUpAndDown() {
    metrics.produceStreamOpened();
    metrics.produceStreamOpened();
    metrics.produceStreamClosed();
    metrics.consumeStreamOpened("orders", "billing");
    metrics.consumeStreamClosed("orders", "billing", "ABORTED");
    metrics.delivered("orders.created", 10, 2);
    metrics.rebalanced("orders", "billing", "lost");

    assertThat(metric("odradek.produce.streams"))
        .hasLongSumSatisfying(sum -> sum.hasPointsSatisfying(point -> point.hasValue(1)));
    assertThat(metric("odradek.consume.streams"))
        .hasLongSumSatisfying(sum -> sum.hasPointsSatisfying(point -> point.hasValue(0)));
    assertThat(metric("odradek.consume.stream.ends"))
        .hasLongSumSatisfying(
            sum ->
                sum.hasPointsSatisfying(
                    point ->
                        point
                            .hasValue(1)
                            .hasAttributes(
                                Attributes.of(
                                    ServerMetrics.CLUSTER,
                                    "orders",
                                    ServerMetrics.GROUP,
                                    "billing",
                                    ServerMetrics.REASON,
                                    "ABORTED"))));
    assertThat(metric("odradek.consume.records"))
        .hasLongSumSatisfying(sum -> sum.hasPointsSatisfying(point -> point.hasValue(10)));
    assertThat(metric("odradek.consume.decode.errors"))
        .hasLongSumSatisfying(sum -> sum.hasPointsSatisfying(point -> point.hasValue(2)));
    assertThat(metric("odradek.consume.rebalances"))
        .hasLongSumSatisfying(sum -> sum.hasPointsSatisfying(point -> point.hasValue(1)));
  }

  @Test
  void certificateExpiryReloadsAndReadinessAreObserved() throws Exception {
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
    Readiness readiness = new Readiness();
    try (TlsSources tls = TlsSources.create(config)) {
      metrics.observe(tls, readiness, Clock.systemUTC());
      long notAfter = tls.sources().get("orders").current().notAfter().getEpochSecond();
      Attributes cluster = Attributes.of(ServerMetrics.CLUSTER, "orders");

      assertThat(metric("odradek.tls.certificate.not_after"))
          .hasLongGaugeSatisfying(
              gauge ->
                  gauge.hasPointsSatisfying(
                      point -> point.hasValue(notAfter).hasAttributes(cluster)));
      assertThat(metric("odradek.tls.credential.reloads"))
          .hasLongSumSatisfying(
              sum -> sum.hasPointsSatisfying(point -> point.hasValue(1).hasAttributes(cluster)));
      assertThat(metric("odradek.tls.credential.reload.failures"))
          .hasLongSumSatisfying(
              sum -> sum.hasPointsSatisfying(point -> point.hasValue(0).hasAttributes(cluster)));
      assertThat(metric("odradek.ready"))
          .hasLongGaugeSatisfying(gauge -> gauge.hasPointsSatisfying(point -> point.hasValue(0)));

      readiness.started(tls);
      assertThat(metric("odradek.ready"))
          .hasLongGaugeSatisfying(gauge -> gauge.hasPointsSatisfying(point -> point.hasValue(1)));
    }
  }

  @Test
  void theCredentialsOfTheDeliveryPortAreObservedUnderTheirSection() throws Exception {
    TestPki pki = new TestPki(dir, "ca");
    TestPki.write(
        dir.resolve("tls"), pki.issue("server", "uri:spiffe://example.org/app"), pki.caPem());
    FileSource files = FileSource.newBuilder().setDir(dir.resolve("tls").toString()).build();
    ServerConfig config =
        ServerConfig.newBuilder()
            .setDispatch(Dispatch.newBuilder().setTls(ListenerTls.newBuilder().setFiles(files)))
            .setDispatcher(
                Dispatcher.newBuilder()
                    .setAddress("dispatcher:9465")
                    .setTls(Tls.newBuilder().setFiles(files).setAcceptedTrustDomain("example.org")))
            .build();
    Readiness readiness = new Readiness();
    try (TlsSources tls = TlsSources.create(config)) {
      metrics.observe(tls, readiness, Clock.systemUTC());
      long notAfter = tls.listener().current().notAfter().getEpochSecond();
      Attributes listener = Attributes.of(ServerMetrics.DELIVERY, "dispatch.tls");
      Attributes dispatcher = Attributes.of(ServerMetrics.DELIVERY, "dispatcher.tls");

      assertThat(metric("odradek.tls.certificate.not_after"))
          .hasLongGaugeSatisfying(
              gauge ->
                  gauge.hasPointsSatisfying(
                      point -> point.hasValue(notAfter).hasAttributes(listener),
                      point -> point.hasValue(notAfter).hasAttributes(dispatcher)));
      assertThat(metric("odradek.tls.credential.reloads"))
          .hasLongSumSatisfying(
              sum ->
                  sum.hasPointsSatisfying(
                      point -> point.hasValue(1).hasAttributes(listener),
                      point -> point.hasValue(1).hasAttributes(dispatcher)));

      // Ready with them, and not once either has expired.
      readiness.started(tls);
      assertThat(readiness.problems(Clock.systemUTC().instant())).isEmpty();
      assertThat(readiness.problems(Clock.systemUTC().instant().plus(Duration.ofDays(3))))
          .hasSize(2)
          .allMatch(problem -> problem.contains("the certificate expired at"));
    }
  }

  @Test
  void metricsArePushedToTheConfiguredEndpoint() throws Exception {
    FakeCollector collector = FakeCollector.start();
    ServerConfig.Builder config = ServerConfig.newBuilder();
    config
        .getTelemetryBuilder()
        .setEndpoint(collector.endpoint())
        .setExportInterval(Durations.fromMillis(1000))
        .putResourceAttributes("k8s.pod.name", "orders-7d9f");
    try (Telemetry telemetry = Telemetry.create(config.build())) {
      telemetry.metrics().produced("orders.created", null);
      collector.await(
          Duration.ofSeconds(20),
          "odradek.produce.records",
          "orders.created",
          "service.name",
          "odradek",
          "k8s.pod.name",
          "orders-7d9f");
    } finally {
      collector.close();
    }
  }

  @Test
  void whatWasCountedLastIsPushedAtClose() throws Exception {
    FakeCollector collector = FakeCollector.start();
    ServerConfig.Builder config = ServerConfig.newBuilder();
    // An interval that never comes: only the flush at close can deliver.
    config
        .getTelemetryBuilder()
        .setEndpoint(collector.endpoint())
        .setExportInterval(Durations.fromSeconds(3600));
    try {
      Telemetry telemetry = Telemetry.create(config.build());
      telemetry.metrics().produced("last.words", null);
      telemetry.close();
      org.assertj.core.api.Assertions.assertThat(collector.received("last.words")).isTrue();
    } finally {
      collector.close();
    }
  }

  @Test
  void withoutAnEndpointNothingIsExported() {
    try (Telemetry telemetry = Telemetry.create(ServerConfig.getDefaultInstance())) {
      telemetry.metrics().produced("orders", null);
    }
  }

  @Test
  void malformedEndpointIsRefused() {
    ServerConfig.Builder config = ServerConfig.newBuilder();
    config.getTelemetryBuilder().setEndpoint("collector:4317");
    assertThatThrownBy(() -> Telemetry.create(config.build()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("telemetry.endpoint");
  }
}
