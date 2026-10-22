package io.github.bpalermo.odradek.e2eclient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.Map;
import org.junit.jupiter.api.Test;

class OtlpMetricsTest {
  @Test
  void withoutAnEndpointNothingIsExported() {
    Metrics metrics = OtlpMetrics.create(Map.of(), "a", "kafka", "pod-0-abc");

    assertThat(metrics).isNotInstanceOf(OtlpMetrics.class);
    metrics.produceLatency(1);
    metrics.close();
  }

  @Test
  void anEndpointStartsTheExporter() {
    Metrics metrics =
        OtlpMetrics.create(
            Map.of(
                "OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317",
                "OTEL_RESOURCE_ATTRIBUTES", "k8s.pod.name=lane-a-0"),
            "a",
            "kafka",
            "o");
    try {
      assertThat(metrics).isInstanceOf(OtlpMetrics.class);
      metrics.produceLatency(1.5);
      metrics.endToEndLatency(2.5, true);
      metrics.endToEndLatency(-0.5, false);
      metrics.sent();
      metrics.acked();
      metrics.processed();
    } finally {
      // Nothing listens: the push at close fails, and is not waited for long.
      metrics.close();
    }
  }

  @Test
  void aNegativeCrossClockLatencyIsExportedAsZeroAndNotDropped() {
    InMemoryMetricReader reader = InMemoryMetricReader.create();
    OtlpMetrics metrics =
        OtlpMetrics.of(
            SdkMeterProvider.builder().registerMetricReader(reader).build(), "a", "kafka");
    metrics.endToEndLatency(-3, false);
    metrics.endToEndLatency(2, false);

    MetricData data =
        reader.collectAllMetrics().stream()
            .filter(metric -> metric.getName().equals("e2e.end_to_end.latency"))
            .findFirst()
            .orElseThrow();
    HistogramPointData point = data.getHistogramData().getPoints().iterator().next();
    assertThat(point.getAttributes().get(AttributeKey.stringKey("clock"))).isEqualTo("cross");
    assertThat(point.getAttributes().get(AttributeKey.stringKey("lane"))).isEqualTo("a");
    assertThat(point.getCount()).isEqualTo(2);
    assertThat(point.getMin()).isZero();
    assertThat(point.getSum()).isEqualTo(2.0);
    metrics.close();
  }

  @Test
  void otelServiceNameWinsOverTheResourceAttributesWhichWinOverTheDefault() {
    AttributeKey<String> name = AttributeKey.stringKey("service.name");
    Map<String, String> attributes =
        OtlpMetrics.resourceAttributes("service.name=from-attributes,k8s.pod.name=lane-a-0");

    assertThat(OtlpMetrics.resource(null, Map.of(), "o").get(name)).isEqualTo("odradek-e2e-client");
    assertThat(OtlpMetrics.resource(null, attributes, "o").get(name)).isEqualTo("from-attributes");
    assertThat(OtlpMetrics.resource(" ", attributes, "o").get(name)).isEqualTo("from-attributes");
    Attributes both = OtlpMetrics.resource("from-variable", attributes, "o");
    assertThat(both.get(name)).isEqualTo("from-variable");
    assertThat(both.get(AttributeKey.stringKey("k8s.pod.name"))).isEqualTo("lane-a-0");
  }

  @Test
  void eachProcessIsItsOwnServiceInstanceUnlessTheEnvironmentNamesOne() {
    AttributeKey<String> id = AttributeKey.stringKey("service.instance.id");
    assertThat(OtlpMetrics.resource(null, Map.of(), "lane-a-0-1f2e3d4c").get(id))
        .isEqualTo("lane-a-0-1f2e3d4c");
    assertThat(
            OtlpMetrics.resource(null, Map.of("service.instance.id", "set"), "lane-a-0-1f2e3d4c")
                .get(id))
        .isEqualTo("set");
  }

  @Test
  void onlyGrpcAndWellFormedVariablesAreAccepted() {
    assertThatThrownBy(
            () ->
                OtlpMetrics.create(
                    Map.of(
                        "OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318",
                        "OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf"),
                    "a",
                    "kafka",
                    "o"))
        .hasMessageContaining("must be grpc");
    assertThatThrownBy(
            () ->
                OtlpMetrics.create(
                    Map.of("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317"), "a", "kafka", "o"))
        .hasMessageContaining("http://host:port");
    assertThatThrownBy(
            () ->
                OtlpMetrics.create(
                    Map.of(
                        "OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317",
                        "OTEL_METRIC_EXPORT_INTERVAL", "soon"),
                    "a",
                    "kafka",
                    "o"))
        .hasMessageContaining("OTEL_METRIC_EXPORT_INTERVAL");
  }

  @Test
  void resourceAttributesAreParsedAsTheSdksDo() {
    assertThat(OtlpMetrics.resourceAttributes("k8s.pod.name=lane-a-0, lane=a%2Cb,,x=1+1"))
        .containsExactly(
            Map.entry("k8s.pod.name", "lane-a-0"), Map.entry("lane", "a,b"), Map.entry("x", "1+1"));
    assertThat(OtlpMetrics.resourceAttributes(null)).isEmpty();
    assertThatThrownBy(() -> OtlpMetrics.resourceAttributes("novalue"))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
