package io.github.bpalermo.odradek.server.telemetry;

import static io.opentelemetry.sdk.testing.assertj.OpenTelemetryAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class KafkaClientMetricsTest {
  private static final String FETCH = "consumer-fetch-manager-metrics";
  private static final String COORDINATOR = "consumer-coordinator-metrics";
  private static final String LAG = "odradek.kafka.consumer.records.lag";
  private static final Attributes BILLING =
      Attributes.of(ServerMetrics.CLUSTER, "orders", ServerMetrics.GROUP, "billing");

  private final InMemoryMetricReader reader = InMemoryMetricReader.create();
  private final SdkMeterProvider provider =
      SdkMeterProvider.builder().registerMetricReader(reader).build();
  private final KafkaClientMetrics kafka =
      new ServerMetrics(provider.get("odradek")).kafkaClients();

  @AfterEach
  void stop() {
    provider.close();
  }

  private Optional<MetricData> find(String name) {
    return reader.collectAllMetrics().stream().filter(m -> m.getName().equals(name)).findFirst();
  }

  private MetricData metric(String name) {
    Collection<MetricData> all = reader.collectAllMetrics();
    return all.stream()
        .filter(m -> m.getName().equals(name))
        .findFirst()
        .orElseThrow(() -> new AssertionError(name + " not among " + all));
  }

  /** The metrics of a consumer of group billing on cluster orders, as Kafka would hold them. */
  private Metrics consumer(String clientId, String... topics) {
    Map<String, Object> properties = new HashMap<>();
    kafka.applyToConsumer("orders", "billing", List.of(topics), properties);
    properties.put("client.id", clientId);
    KafkaMetricsReporter reporter = new KafkaMetricsReporter();
    reporter.configure(properties);
    return new Metrics(new MetricConfig(), List.of(reporter), Time.SYSTEM);
  }

  private static MetricName name(Metrics metrics, String name, String group, String... tags) {
    Map<String, String> all = new HashMap<>();
    all.put("client-id", "c");
    for (int i = 0; i < tags.length; i += 2) {
      all.put(tags[i], tags[i + 1]);
    }
    return metrics.metricName(name, group, all);
  }

  private static void set(Metrics metrics, MetricName name, double value) {
    metrics.addMetric(name, (Measurable) (config, now) -> value);
  }

  private static Attributes partition(String topic, long partition) {
    return BILLING.toBuilder()
        .put(KafkaClientMetrics.PARTITION, partition)
        .put(ServerMetrics.TOPIC, topic)
        .build();
  }

  @Test
  void lagIsReportedPerPartitionUnderTheTopicsRealName() {
    try (Metrics metrics = consumer("c1", "orders.created")) {
      // Kafka's fetch metrics name the topic with its dots replaced.
      set(
          metrics,
          name(metrics, "records-lag", FETCH, "topic", "orders_created", "partition", "0"),
          42);
      set(
          metrics,
          name(metrics, "records-lag", FETCH, "topic", "orders_created", "partition", "1"),
          7);

      assertThat(metric(LAG))
          .hasUnit("{record}")
          .hasDoubleGaugeSatisfying(
              gauge ->
                  gauge.hasPointsSatisfying(
                      point -> point.hasValue(42).hasAttributes(partition("orders.created", 0)),
                      point -> point.hasValue(7).hasAttributes(partition("orders.created", 1))));
    }
  }

  @Test
  void aRevokedPartitionsLagIsNoLongerReported() {
    try (Metrics metrics = consumer("c1", "orders")) {
      MetricName zero = name(metrics, "records-lag", FETCH, "topic", "orders", "partition", "0");
      set(metrics, zero, 42);
      set(metrics, name(metrics, "records-lag", FETCH, "topic", "orders", "partition", "1"), 7);
      metrics.removeMetric(zero);

      assertThat(metric(LAG))
          .hasDoubleGaugeSatisfying(
              gauge ->
                  gauge.hasPointsSatisfying(
                      point -> point.hasValue(7).hasAttributes(partition("orders", 1))));
    }
  }

  @Test
  void aClosedConsumersGaugesDisappearAndItsTotalsStay() {
    Metrics first = consumer("c1", "orders");
    Metrics second = consumer("c2", "orders");
    set(first, name(first, "records-lag", FETCH, "topic", "orders", "partition", "0"), 42);
    set(second, name(second, "records-lag", FETCH, "topic", "orders", "partition", "1"), 7);
    set(first, name(first, "records-lag-max", FETCH), 42);
    set(second, name(second, "records-lag-max", FETCH), 7);
    set(first, name(first, "assigned-partitions", COORDINATOR), 1);
    set(second, name(second, "assigned-partitions", COORDINATOR), 1);
    set(first, name(first, "rebalance-total", COORDINATOR), 3);
    set(second, name(second, "rebalance-total", COORDINATOR), 2);

    // The consumers of one group are one series: the worst lag, the sum of counts and totals.
    assertThat(metric("odradek.kafka.consumer.records.lag.max"))
        .hasDoubleGaugeSatisfying(
            gauge -> gauge.hasPointsSatisfying(point -> point.hasValue(42).hasAttributes(BILLING)));
    assertThat(metric("odradek.kafka.consumer.assigned_partitions"))
        .hasDoubleGaugeSatisfying(
            gauge -> gauge.hasPointsSatisfying(point -> point.hasValue(2).hasAttributes(BILLING)));
    assertThat(metric("odradek.kafka.consumer.rebalances"))
        .hasLongSumSatisfying(
            sum ->
                sum.isMonotonic()
                    .hasPointsSatisfying(point -> point.hasValue(5).hasAttributes(BILLING)));

    first.close();

    assertThat(metric(LAG))
        .hasDoubleGaugeSatisfying(
            gauge ->
                gauge.hasPointsSatisfying(
                    point -> point.hasValue(7).hasAttributes(partition("orders", 1))));
    assertThat(metric("odradek.kafka.consumer.records.lag.max"))
        .hasDoubleGaugeSatisfying(
            gauge -> gauge.hasPointsSatisfying(point -> point.hasValue(7).hasAttributes(BILLING)));
    assertThat(metric("odradek.kafka.consumer.assigned_partitions"))
        .hasDoubleGaugeSatisfying(
            gauge -> gauge.hasPointsSatisfying(point -> point.hasValue(1).hasAttributes(BILLING)));
    // A cumulative counter that dropped would read as a restart: what the closed one counted stays.
    assertThat(metric("odradek.kafka.consumer.rebalances"))
        .hasLongSumSatisfying(
            sum -> sum.hasPointsSatisfying(point -> point.hasValue(5).hasAttributes(BILLING)));

    second.close();

    assertThat(kafka.live()).isEmpty();
    assertThat(find(LAG)).isEmpty();
    assertThat(find("odradek.kafka.consumer.records.lag.max")).isEmpty();
    assertThat(find("odradek.kafka.consumer.assigned_partitions")).isEmpty();
    assertThat(metric("odradek.kafka.consumer.rebalances"))
        .hasLongSumSatisfying(
            sum -> sum.hasPointsSatisfying(point -> point.hasValue(5).hasAttributes(BILLING)));
  }

  @Test
  void aMetricWithoutAValueOrOffTheListIsNotReported() {
    try (Metrics metrics = consumer("c1", "orders")) {
      // An average of no samples.
      set(metrics, name(metrics, "commit-latency-avg", COORDINATOR), Double.NaN);
      set(metrics, name(metrics, "records-consumed-rate", FETCH), 5);
      // The same name at another level.
      set(metrics, name(metrics, "records-lag", FETCH, "topic", "orders"), 5);

      assertThat(find("odradek.kafka.consumer.commit.latency.avg")).isEmpty();
      assertThat(find(LAG)).isEmpty();
      assertThat(kafka.live()).containsExactly("odradek.kafka.consumer.commit.latency.avg");
    }
  }

  @Test
  void theReporterIsAddedToTheOnesAlreadyConfigured() {
    Map<String, Object> defaults = new HashMap<>();
    kafka.applyToProducer("orders", defaults);
    assertThat(defaults.get("metric.reporters"))
        .isEqualTo(
            "org.apache.kafka.common.metrics.JmxReporter," + KafkaMetricsReporter.class.getName());
    // What a client without the setting gets must stay what Kafka itself would give it.
    assertThat(ProducerConfig.configDef().defaultValues().get("metric.reporters"))
        .isEqualTo(List.of(KafkaClientMetrics.DEFAULT_REPORTERS));

    Map<String, Object> configured = new HashMap<>(Map.of("metric.reporters", "com.example.Mine"));
    kafka.applyToConsumer("orders", "billing", List.of("orders"), configured);
    assertThat(configured.get("metric.reporters"))
        .isEqualTo("com.example.Mine," + KafkaMetricsReporter.class.getName());

    Map<String, Object> none = new HashMap<>(Map.of("metric.reporters", ""));
    kafka.applyToProducer("orders", none);
    assertThat(none.get("metric.reporters")).isEqualTo(KafkaMetricsReporter.class.getName());
  }

  @Test
  void withoutTelemetryTheClientsAreLeftAlone() {
    Map<String, Object> properties = new HashMap<>();
    ServerMetrics.noop().kafkaClients().applyToProducer("orders", properties);
    ServerMetrics.noop()
        .kafkaClients()
        .applyToConsumer("orders", "billing", List.of("orders"), properties);
    assertThat(properties).isEmpty();
  }

  @Test
  void theReporterCannotBeNamedInAClustersProperties() {
    try (KafkaMetricsReporter reporter = new KafkaMetricsReporter()) {
      assertThatThrownBy(() -> reporter.configure(Map.of()))
          .hasMessageContaining("installed by the server");
    }
  }

  /**
   * Real clients, which Kafka gives the reporter by class name: every metric on the list must be
   * one they have, so that a renamed Kafka metric fails here and not silently in a dashboard.
   */
  @Test
  void kafkasOwnClientsHaveEveryMetricOnTheList() {
    Set<String> expected =
        KafkaClientMetrics.SPECS.stream()
            .filter(spec -> !spec.perPartition())
            .map(KafkaClientMetrics.Spec::name)
            .collect(Collectors.toSet());

    Map<String, Object> producing = new HashMap<>();
    producing.put("bootstrap.servers", "localhost:1");
    kafka.applyToProducer("orders", producing);
    Map<String, Object> consuming = new HashMap<>();
    consuming.put("bootstrap.servers", "localhost:1");
    consuming.put("group.id", "billing");
    kafka.applyToConsumer("orders", "billing", List.of("orders"), consuming);

    try (KafkaProducer<byte[], byte[]> producer =
            new KafkaProducer<>(producing, new ByteArraySerializer(), new ByteArraySerializer());
        KafkaConsumer<byte[], byte[]> consumer =
            new KafkaConsumer<>(
                consuming, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
      assertThat(kafka.live()).containsExactlyInAnyOrderElementsOf(expected);
      assertThat(metric("odradek.kafka.producer.buffer.available"))
          .hasUnit("By")
          .hasDoubleGaugeSatisfying(
              gauge ->
                  gauge.hasPointsSatisfying(
                      point ->
                          point.hasAttributes(Attributes.of(ServerMetrics.CLUSTER, "orders"))));
      assertThat(metric("odradek.kafka.consumer.connections"))
          .hasDoubleGaugeSatisfying(
              gauge ->
                  gauge.hasPointsSatisfying(point -> point.hasValue(0).hasAttributes(BILLING)));
      producer.close(Duration.ZERO);
      consumer.close(CloseOptions.timeout(Duration.ZERO));
    }
    assertThat(kafka.live()).isEmpty();
    assertThat(find("odradek.kafka.producer.buffer.available")).isEmpty();
    assertThat(find("odradek.kafka.consumer.connections")).isEmpty();
  }
}
