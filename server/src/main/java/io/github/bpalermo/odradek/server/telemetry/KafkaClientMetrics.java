package io.github.bpalermo.odradek.server.telemetry;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.Meter;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.ObjDoubleConsumer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetric;

/**
 * The Kafka clients' own metrics, exported with the server's.
 *
 * <p>Every client the server creates is given {@link KafkaMetricsReporter}, which tells this class
 * which metrics the client has. A fixed set of instruments, one per entry of {@link #SPECS}, reads
 * the live Kafka metrics on each collection. Nothing is created or closed in the OpenTelemetry SDK
 * when a consumer comes or goes.
 *
 * <p>Only the metrics in {@link #SPECS} are exported: Kafka has hundreds, and the per-node and
 * per-topic ones multiply. The client id is not an attribute, because a consumer's changes with
 * every stream. Clients with the same attributes (the consumers of one group on one cluster) are
 * therefore reported as one value: a count or a total is their sum, anything else is the largest
 * among them. A total keeps what closed clients counted, so it never goes down.
 */
public final class KafkaClientMetrics {
  /** Under this key a client's properties carry its {@link Binding}. */
  static final String BINDING_CONFIG = "odradek.metrics.binding";

  /**
   * What {@code metric.reporters} is when a client does not set it. Ours is added to it, not put in
   * its place, so that the clients stay visible over JMX.
   */
  static final String DEFAULT_REPORTERS = JmxReporter.class.getName();

  static final AttributeKey<Long> PARTITION = AttributeKey.longKey("partition");

  private static final String CONSUMER = "consumer-metrics";
  private static final String COORDINATOR = "consumer-coordinator-metrics";
  private static final String FETCH = "consumer-fetch-manager-metrics";
  private static final String PRODUCER = "producer-metrics";

  /** How a Kafka metric becomes an instrument. */
  enum Kind {
    /** A value that goes up and down; several clients report the largest. */
    GAUGE_MAX,
    /** A value that goes up and down; several clients report the sum. */
    GAUGE_SUM,
    /** A Kafka {@code -total}: cumulative, summed, and kept when its client closes. */
    COUNTER
  }

  /** One exported metric: the Kafka metric it reads and the instrument it becomes. */
  record Spec(
      String name,
      Kind kind,
      String unit,
      String description,
      String kafkaGroup,
      String kafkaName,
      boolean perPartition) {}

  private static Spec spec(
      String name, Kind kind, String unit, String kafkaGroup, String kafkaName, String text) {
    return new Spec(name, kind, unit, text, kafkaGroup, kafkaName, false);
  }

  /** The allow-list. Names and attributes are a contract: add, do not rename. */
  static final List<Spec> SPECS =
      List.of(
          new Spec(
              "odradek.kafka.consumer.records.lag",
              Kind.GAUGE_MAX,
              "{record}",
              "Records between a partition's end and the consumer's position, as of its last"
                  + " fetch. Reported while a stream holds the partition.",
              FETCH,
              "records-lag",
              true),
          spec(
              "odradek.kafka.consumer.records.lag.max",
              Kind.GAUGE_MAX,
              "{record}",
              FETCH,
              "records-lag-max",
              "Largest lag of any partition of the group's consumers, over Kafka's sample window."),
          spec(
              "odradek.kafka.consumer.commit.latency.avg",
              Kind.GAUGE_MAX,
              "ms",
              COORDINATOR,
              "commit-latency-avg",
              "Average time an offset commit took."),
          spec(
              "odradek.kafka.consumer.commit.latency.max",
              Kind.GAUGE_MAX,
              "ms",
              COORDINATOR,
              "commit-latency-max",
              "Longest time an offset commit took."),
          spec(
              "odradek.kafka.consumer.commits",
              Kind.COUNTER,
              "{commit}",
              COORDINATOR,
              "commit-total",
              "Offset commits sent."),
          spec(
              "odradek.kafka.consumer.last_poll.age",
              Kind.GAUGE_MAX,
              "s",
              CONSUMER,
              "last-poll-seconds-ago",
              "Seconds since a consumer last polled; the longest among the group's consumers."),
          spec(
              "odradek.kafka.consumer.rebalances",
              Kind.COUNTER,
              "{rebalance}",
              COORDINATOR,
              "rebalance-total",
              "Rebalances the group's consumers took part in."),
          spec(
              "odradek.kafka.consumer.rebalance.failures",
              Kind.COUNTER,
              "{rebalance}",
              COORDINATOR,
              "failed-rebalance-total",
              "Rebalances that failed."),
          spec(
              "odradek.kafka.consumer.rebalance.latency.avg",
              Kind.GAUGE_MAX,
              "ms",
              COORDINATOR,
              "rebalance-latency-avg",
              "Average time a rebalance took."),
          spec(
              "odradek.kafka.consumer.rebalance.latency.max",
              Kind.GAUGE_MAX,
              "ms",
              COORDINATOR,
              "rebalance-latency-max",
              "Longest time a rebalance took."),
          spec(
              "odradek.kafka.consumer.assigned_partitions",
              Kind.GAUGE_SUM,
              "{partition}",
              COORDINATOR,
              "assigned-partitions",
              "Partitions assigned to the group's consumers."),
          spec(
              "odradek.kafka.consumer.connections",
              Kind.GAUGE_SUM,
              "{connection}",
              CONSUMER,
              "connection-count",
              "Open connections to brokers."),
          spec(
              "odradek.kafka.consumer.authentication.failures",
              Kind.COUNTER,
              "{connection}",
              CONSUMER,
              "failed-authentication-total",
              "Connections a broker refused to authenticate, a failed TLS handshake included."),
          spec(
              "odradek.kafka.producer.request.latency.avg",
              Kind.GAUGE_MAX,
              "ms",
              PRODUCER,
              "request-latency-avg",
              "Average time a request to a broker took."),
          spec(
              "odradek.kafka.producer.request.latency.max",
              Kind.GAUGE_MAX,
              "ms",
              PRODUCER,
              "request-latency-max",
              "Longest time a request to a broker took."),
          spec(
              "odradek.kafka.producer.record.error.rate",
              Kind.GAUGE_MAX,
              "{record}/s",
              PRODUCER,
              "record-error-rate",
              "Records per second whose send failed."),
          spec(
              "odradek.kafka.producer.record.errors",
              Kind.COUNTER,
              "{record}",
              PRODUCER,
              "record-error-total",
              "Records whose send failed."),
          spec(
              "odradek.kafka.producer.record.retries",
              Kind.COUNTER,
              "{record}",
              PRODUCER,
              "record-retry-total",
              "Record sends that were retried."),
          spec(
              "odradek.kafka.producer.buffer.available",
              Kind.GAUGE_SUM,
              "By",
              PRODUCER,
              "buffer-available-bytes",
              "Bytes of the producer's buffer not in use. At zero, sends block."),
          spec(
              "odradek.kafka.producer.record.queue_time.avg",
              Kind.GAUGE_MAX,
              "ms",
              PRODUCER,
              "record-queue-time-avg",
              "Average time a batch waited in the buffer before it was sent."),
          spec(
              "odradek.kafka.producer.record.queue_time.max",
              Kind.GAUGE_MAX,
              "ms",
              PRODUCER,
              "record-queue-time-max",
              "Longest time a batch waited in the buffer before it was sent."),
          spec(
              "odradek.kafka.producer.batch.size.avg",
              Kind.GAUGE_MAX,
              "By",
              PRODUCER,
              "batch-size-avg",
              "Average size of a batch sent to a partition."),
          spec(
              "odradek.kafka.producer.batch.size.max",
              Kind.GAUGE_MAX,
              "By",
              PRODUCER,
              "batch-size-max",
              "Largest batch sent to a partition."),
          spec(
              "odradek.kafka.producer.connections",
              Kind.GAUGE_SUM,
              "{connection}",
              PRODUCER,
              "connection-count",
              "Open connections to brokers."),
          spec(
              "odradek.kafka.producer.authentication.failures",
              Kind.COUNTER,
              "{connection}",
              PRODUCER,
              "failed-authentication-total",
              "Connections a broker refused to authenticate, a failed TLS handshake included."));

  /**
   * What a reporter needs to know about its client, handed to it through the client's properties.
   *
   * @param group null for a producer
   * @param topics the topics a consumer subscribes to, by the name Kafka's metrics give them
   */
  record Binding(
      KafkaClientMetrics metrics, String cluster, String group, Map<String, String> topics) {}

  /** The live Kafka metrics behind one instrument. */
  private static final class Series {
    private final Spec spec;
    private final Map<KafkaMetric, Attributes> live = new IdentityHashMap<>();

    /** For a counter: what clients that have closed counted. */
    private final Map<Attributes, Double> retired = new HashMap<>();

    Series(Spec spec) {
      this.spec = spec;
    }

    synchronized void add(KafkaMetric metric, Attributes attributes) {
      live.put(metric, attributes);
    }

    synchronized void remove(KafkaMetric metric) {
      Attributes attributes = live.remove(metric);
      if (attributes != null && spec.kind() == Kind.COUNTER) {
        double last = value(metric);
        if (!Double.isNaN(last)) {
          retired.merge(attributes, last, Double::sum);
        }
      }
    }

    synchronized boolean isLive() {
      return !live.isEmpty();
    }

    synchronized void observe(ObjDoubleConsumer<Attributes> into) {
      Map<Attributes, Double> values = new HashMap<>(retired);
      live.forEach(
          (metric, attributes) -> {
            double value = value(metric);
            if (!Double.isNaN(value)) {
              values.merge(
                  attributes, value, spec.kind() == Kind.GAUGE_MAX ? Math::max : Double::sum);
            }
          });
      values.forEach((attributes, value) -> into.accept(attributes, value));
    }

    /** A metric's value now; NaN when it has none, as an average or a maximum of no samples. */
    private static double value(KafkaMetric metric) {
      try {
        if (metric.metricValue() instanceof Number number
            && Double.isFinite(number.doubleValue())) {
          return number.doubleValue();
        }
      } catch (RuntimeException e) {
        // A metric that cannot be read is one without a value.
      }
      return Double.NaN;
    }
  }

  private record Source(String group, String name) {}

  private final Map<Source, Series> series = new HashMap<>();
  private final boolean enabled;

  KafkaClientMetrics(Meter meter, boolean enabled) {
    this.enabled = enabled;
    if (!enabled) {
      return;
    }
    for (Spec spec : SPECS) {
      Series one = new Series(spec);
      series.put(new Source(spec.kafkaGroup(), spec.kafkaName()), one);
      if (spec.kind() == Kind.COUNTER) {
        meter
            .counterBuilder(spec.name())
            .setDescription(spec.description())
            .setUnit(spec.unit())
            .buildWithCallback(
                measurement ->
                    one.observe(
                        (attributes, value) -> measurement.record((long) value, attributes)));
      } else {
        meter
            .gaugeBuilder(spec.name())
            .setDescription(spec.description())
            .setUnit(spec.unit())
            .buildWithCallback(
                measurement ->
                    one.observe((attributes, value) -> measurement.record(value, attributes)));
      }
    }
  }

  /** Makes the producer of {@code cluster}, created from {@code properties}, report here. */
  public void applyToProducer(String cluster, Map<String, Object> properties) {
    apply(properties, new Binding(this, cluster, null, Map.of()));
  }

  /**
   * Makes a consumer of {@code group} on {@code cluster}, created from {@code properties}, report
   * here. {@code topics} is what it subscribes to.
   */
  public void applyToConsumer(
      String cluster, String group, Collection<String> topics, Map<String, Object> properties) {
    // Kafka's fetch metrics name a topic with its dots replaced.
    Map<String, String> byTag = new HashMap<>();
    topics.forEach(topic -> byTag.put(topic.replace('.', '_'), topic));
    apply(properties, new Binding(this, cluster, group, byTag));
  }

  /** Does nothing when no metric is exported: the clients then run as if this did not exist. */
  private void apply(Map<String, Object> properties, Binding binding) {
    if (!enabled) {
      return;
    }
    Object configured = properties.get(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG);
    String reporters = configured == null ? DEFAULT_REPORTERS : configured.toString().strip();
    properties.put(
        CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG,
        (reporters.isEmpty() ? "" : reporters + ",") + KafkaMetricsReporter.class.getName());
    properties.put(BINDING_CONFIG, binding);
  }

  /**
   * Starts exporting {@code metric} if it is on the allow-list.
   *
   * @return whether it is, and so whether {@link #remove} must be called for it
   */
  boolean add(Binding binding, KafkaMetric metric) {
    MetricName name = metric.metricName();
    Series one = series.get(new Source(name.group(), name.name()));
    if (one == null) {
      return false;
    }
    Map<String, String> tags = name.tags();
    boolean partition = tags.containsKey("partition");
    if (partition != one.spec.perPartition() || (!partition && tags.containsKey("topic"))) {
      // The same name at another level, as records-lag-max per topic.
      return false;
    }
    AttributesBuilder attributes =
        Attributes.builder().put(ServerMetrics.CLUSTER, binding.cluster());
    if (binding.group() != null) {
      attributes.put(ServerMetrics.GROUP, binding.group());
    }
    if (partition) {
      String topic = tags.get("topic");
      if (topic == null) {
        return false;
      }
      try {
        attributes.put(PARTITION, Long.parseLong(tags.get("partition")));
      } catch (NumberFormatException e) {
        return false;
      }
      attributes.put(ServerMetrics.TOPIC, binding.topics().getOrDefault(topic, topic));
    }
    one.add(metric, attributes.build());
    return true;
  }

  /** Stops exporting a metric {@link #add} accepted. */
  void remove(KafkaMetric metric) {
    MetricName name = metric.metricName();
    Series one = series.get(new Source(name.group(), name.name()));
    if (one != null) {
      one.remove(metric);
    }
  }

  /** The names of the metrics some live client has, for tests. */
  Set<String> live() {
    Set<String> names = new TreeSet<>();
    series.values().stream().filter(Series::isLive).forEach(one -> names.add(one.spec.name()));
    return names;
  }
}
