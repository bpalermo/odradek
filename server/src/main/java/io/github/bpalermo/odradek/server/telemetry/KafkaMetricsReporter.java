package io.github.bpalermo.odradek.server.telemetry;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;

/**
 * Tells {@link KafkaClientMetrics} which metrics one Kafka client has, from its creation to its
 * close.
 *
 * <p>Kafka creates one per client from the class name in {@code metric.reporters}, so this cannot
 * be handed anything through a constructor. It finds its {@link KafkaClientMetrics.Binding} in the
 * client's configuration, where {@link KafkaClientMetrics} put it.
 */
public final class KafkaMetricsReporter implements MetricsReporter {
  private final Set<KafkaMetric> exported =
      Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<>()));
  private KafkaClientMetrics.Binding binding;

  @Override
  public void configure(Map<String, ?> configs) {
    if (!(configs.get(KafkaClientMetrics.BINDING_CONFIG)
        instanceof KafkaClientMetrics.Binding found)) {
      throw new ConfigException(
          KafkaMetricsReporter.class.getName()
              + " is installed by the server; it cannot be named in metric.reporters");
    }
    binding = found;
  }

  @Override
  public void init(List<KafkaMetric> metrics) {
    metrics.forEach(this::metricChange);
  }

  @Override
  public void metricChange(KafkaMetric metric) {
    if (binding.metrics().add(binding, metric)) {
      exported.add(metric);
    }
  }

  @Override
  public void metricRemoval(KafkaMetric metric) {
    if (exported.remove(metric)) {
      binding.metrics().remove(metric);
    }
  }

  /** A closing client does not report its metrics removed one by one: all of them go here. */
  @Override
  public void close() {
    if (binding == null) {
      return;
    }
    List<KafkaMetric> remaining;
    synchronized (exported) {
      remaining = List.copyOf(exported);
      exported.clear();
    }
    remaining.forEach(binding.metrics()::remove);
  }
}
