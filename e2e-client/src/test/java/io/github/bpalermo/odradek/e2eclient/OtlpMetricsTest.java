package io.github.bpalermo.odradek.e2eclient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.junit.jupiter.api.Test;

class OtlpMetricsTest {
  @Test
  void withoutAnEndpointNothingIsExported() {
    Metrics metrics = OtlpMetrics.create(Map.of(), "a", "kafka");

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
            "kafka");
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
  void onlyGrpcAndWellFormedVariablesAreAccepted() {
    assertThatThrownBy(
            () ->
                OtlpMetrics.create(
                    Map.of(
                        "OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318",
                        "OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf"),
                    "a",
                    "kafka"))
        .hasMessageContaining("must be grpc");
    assertThatThrownBy(
            () ->
                OtlpMetrics.create(
                    Map.of("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317"), "a", "kafka"))
        .hasMessageContaining("http://host:port");
    assertThatThrownBy(
            () ->
                OtlpMetrics.create(
                    Map.of(
                        "OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317",
                        "OTEL_METRIC_EXPORT_INTERVAL", "soon"),
                    "a",
                    "kafka"))
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
