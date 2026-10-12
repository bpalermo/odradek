package io.github.bpalermo.odradek.server.produce;

import io.github.bpalermo.odradek.config.v1.Cluster;
import io.github.bpalermo.odradek.config.v1.ProduceTarget;
import io.github.bpalermo.odradek.config.v1.ServerConfig;
import java.io.Closeable;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeSet;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One Kafka producer per cluster that some topic is produced to, shared by every Produce stream.
 */
public final class ClusterProducers implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(ClusterProducers.class);

  /** Creates the producer of one cluster. Tests substitute a mock. */
  @FunctionalInterface
  public interface Factory {
    Producer<byte[], byte[]> create(String cluster, Map<String, Object> properties);
  }

  private final Map<String, Producer<byte[], byte[]>> producers;

  private ClusterProducers(Map<String, Producer<byte[], byte[]>> producers) {
    this.producers = producers;
  }

  /** Creates real Kafka producers. Creating one does not connect to the cluster. */
  public static ClusterProducers create(ServerConfig config) {
    return create(
        config,
        (cluster, properties) ->
            new KafkaProducer<>(properties, new ByteArraySerializer(), new ByteArraySerializer()));
  }

  /**
   * Creates a producer for each cluster named by an entry under {@code producers}.
   *
   * @throws UnsupportedOperationException if such a cluster configures {@code tls}, which the
   *     server cannot honour yet; connecting without it would be worse than not starting
   */
  public static ClusterProducers create(ServerConfig config, Factory factory) {
    TreeSet<String> used = new TreeSet<>();
    config
        .getProducersMap()
        .values()
        .forEach(
            p -> p.getClustersList().stream().map(ProduceTarget::getCluster).forEach(used::add));
    Map<String, Producer<byte[], byte[]>> producers = new LinkedHashMap<>();
    try {
      for (String name : used) {
        Cluster cluster = config.getClustersOrThrow(name);
        if (cluster.hasTls()) {
          throw new UnsupportedOperationException(
              "clusters." + name + ".tls is set, and mTLS to brokers is not implemented yet");
        }
        producers.put(name, factory.create(name, properties(name, cluster)));
      }
    } catch (RuntimeException e) {
      producers.values().forEach(p -> p.close(Duration.ZERO));
      throw e;
    }
    return new ClusterProducers(producers);
  }

  /**
   * The producer configuration of a cluster: its shared and producer properties, with the settings
   * the delivery guarantees rest on set last. Validation already rejected weaker values, so this
   * only fills in what the file left out.
   */
  static Map<String, Object> properties(String name, Cluster cluster) {
    Map<String, Object> properties = new HashMap<>();
    properties.putAll(cluster.getPropertiesMap());
    properties.putAll(cluster.getProducerPropertiesMap());
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    properties.putIfAbsent(ProducerConfig.CLIENT_ID_CONFIG, "odradek-" + name);
    return properties;
  }

  /** Returns the producer of a cluster that some topic is produced to. */
  Producer<byte[], byte[]> get(String cluster) {
    Producer<byte[], byte[]> producer = producers.get(cluster);
    if (producer == null) {
      throw new IllegalArgumentException("no producer for cluster " + cluster);
    }
    return producer;
  }

  /** Sends everything buffered and waits for the brokers' answers. */
  public void flush() {
    producers.values().forEach(Producer::flush);
  }

  /** Closes every producer, waiting up to {@code timeout} each for buffered records. */
  public void close(Duration timeout) {
    producers.forEach(
        (name, producer) -> {
          try {
            producer.close(timeout);
          } catch (RuntimeException e) {
            LOG.warn("closing the producer of cluster {} failed", name, e);
          }
        });
  }

  @Override
  public void close() {
    close(Duration.ofSeconds(30));
  }
}
