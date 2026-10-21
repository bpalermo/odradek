package io.github.bpalermo.odradek.e2eclient;

import io.github.bpalermo.odradek.config.v1.Cluster;
import io.github.bpalermo.odradek.config.v1.FileSource;
import io.github.bpalermo.odradek.config.v1.ServerConfig;
import io.github.bpalermo.odradek.config.v1.SpiffeSource;
import io.github.bpalermo.odradek.config.v1.Tls;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/**
 * The configuration of the Kafka clients of the {@code kafka} transport, built as the server builds
 * its own (ClusterProducers and ConsumeService), so that the lanes differ in the path the records
 * take and not in how Kafka is configured:
 *
 * <ul>
 *   <li>the producer: the overrides, then {@code acks=all} and {@code enable.idempotence=true},
 *       which the server enforces; everything else (linger, batch size, compression, in-flight
 *       requests) is the Kafka client's default, as on the server's, from the same kafka-clients;
 *   <li>the consumer: the overrides, then the group, manual commits and the cooperative-sticky
 *       assignor, which the server enforces; a partition the group has never committed starts at
 *       its oldest record unless the overrides say otherwise, as a server consumer does by default
 *       ({@code offsetReset} {@code EARLIEST}); fetch and poll settings are the client's defaults.
 * </ul>
 *
 * <p>TLS is the server's own: the same {@code clusters.<name>.tls} section, loaded by the server's
 * {@code TlsSources}, which applies its rotating engine factory.
 */
public final class KafkaClients {
  /** The name of the one cluster in the configuration handed to TlsSources. */
  static final String CLUSTER = "kafka";

  /** What the results show of the clients' effective configuration. */
  static final List<String> PRODUCER_SHOWN =
      List.of(
          ProducerConfig.ACKS_CONFIG,
          ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
          ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
          ProducerConfig.LINGER_MS_CONFIG,
          ProducerConfig.BATCH_SIZE_CONFIG,
          ProducerConfig.COMPRESSION_TYPE_CONFIG,
          ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,
          ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,
          ProducerConfig.PARTITIONER_CLASS_CONFIG,
          CommonClientConfigs.SECURITY_PROTOCOL_CONFIG);

  static final List<String> CONSUMER_SHOWN =
      List.of(
          ConsumerConfig.GROUP_ID_CONFIG,
          ConsumerConfig.GROUP_PROTOCOL_CONFIG,
          ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
          ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
          ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
          ConsumerConfig.FETCH_MIN_BYTES_CONFIG,
          ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,
          ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
          ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,
          ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,
          ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,
          CommonClientConfigs.SECURITY_PROTOCOL_CONFIG);

  private KafkaClients() {}

  /** The properties of {@code --kafka-properties}; none when it is not set. */
  static Map<String, String> overrides(String file) throws IOException {
    Map<String, String> out = new TreeMap<>();
    if (file.isEmpty()) {
      return out;
    }
    Properties properties = new Properties();
    try (Reader reader = Files.newBufferedReader(Path.of(file), StandardCharsets.UTF_8)) {
      properties.load(reader);
    }
    properties.stringPropertyNames().forEach(name -> out.put(name, properties.getProperty(name)));
    return out;
  }

  /** The producer's properties, before TLS. */
  public static Map<String, Object> producer(
      Settings.Kafka kafka, String clientId, Map<String, String> overrides) {
    Map<String, Object> properties = new HashMap<>();
    properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers());
    properties.putAll(overrides);
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    properties.putIfAbsent(ProducerConfig.CLIENT_ID_CONFIG, clientId);
    return properties;
  }

  /** The consumer's properties, before TLS. */
  public static Map<String, Object> consumer(
      Settings.Kafka kafka, String clientId, Map<String, String> overrides) {
    Map<String, Object> properties = new HashMap<>();
    properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers());
    properties.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.putAll(overrides);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, kafka.group());
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    properties.put(
        ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
        CooperativeStickyAssignor.class.getName());
    properties.putIfAbsent(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
    return properties;
  }

  /**
   * Keeps a batch's handling within {@code max.poll.interval.ms}. The server's consumers poll
   * whatever the app does; this one handles records on its poll thread, so a slow handler (up to
   * {@code perRecord} a record: {@code --handler-delay} plus {@code --fail-hold}) must not hold a
   * whole batch past the interval, or Kafka drops the consumer from the group mid-batch. {@code
   * max.poll.records} is lowered, when needed, so that a batch takes at most half the interval.
   * Without a handler delay nothing changes, so the latency lanes keep Kafka's defaults.
   *
   * @throws IllegalArgumentException if even one record a poll can take longer than that
   */
  static void boundPollRecords(Map<String, Object> properties, Duration perRecord) {
    long perRecordMs = perRecord.toMillis();
    if (perRecordMs <= 0) {
      return;
    }
    long intervalMs = longValue(properties, ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG);
    long records = longValue(properties, ConsumerConfig.MAX_POLL_RECORDS_CONFIG);
    long fit = intervalMs / 2 / perRecordMs;
    if (fit < 1) {
      throw new IllegalArgumentException(
          "--handler-delay plus --fail-hold ("
              + perRecordMs
              + " ms a record) is more than half of max.poll.interval.ms ("
              + intervalMs
              + " ms): the consumer would leave its group while handling one record; raise"
              + " max.poll.interval.ms in --kafka-properties");
    }
    if (records > fit) {
      properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(fit));
    }
  }

  /** A consumer setting as set, or the Kafka client's default. */
  private static long longValue(Map<String, Object> properties, String name) {
    Object value = properties.get(name);
    if (value == null) {
      value = ConsumerConfig.configDef().defaultValues().get(name);
    }
    try {
      return Long.parseLong(String.valueOf(value).trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(name + " must be an integer, not " + value, e);
    }
  }

  /**
   * The server configuration whose one cluster has the TLS of these settings, for TlsSources; null
   * for plaintext. The overrides are the cluster's properties, so that TlsSources refuses what it
   * refuses of a server's cluster: a {@code security.protocol} without TLS beside a {@code tls}
   * section, which would otherwise turn TLS off without a word.
   */
  static ServerConfig tlsConfig(Settings.Kafka kafka, Map<String, String> overrides) {
    Tls.Builder tls = Tls.newBuilder();
    switch (kafka.tls()) {
      case NONE -> {
        return null;
      }
      case SPIFFE ->
          tls.setSpiffe(SpiffeSource.newBuilder().setWorkloadApiSocket(kafka.spiffeSocket()));
      case FILES -> tls.setFiles(FileSource.newBuilder().setDir(kafka.tlsDir()));
    }
    tls.addAllAcceptedSpiffeIds(kafka.brokerSpiffeIds())
        .setAcceptedTrustDomain(kafka.brokerTrustDomain());
    return ServerConfig.newBuilder()
        .putClusters(
            CLUSTER,
            Cluster.newBuilder()
                .putAllProperties(overrides)
                .putProperties(
                    CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers())
                .setTls(tls)
                .build())
        .build();
  }

  /** The values of {@code shown} the client ends up with, its defaults included. */
  static Map<String, Object> effective(Map<String, Object> properties, boolean producer) {
    // The (de)serializers are given to the clients' constructors, not in the properties.
    Map<String, Object> complete = new HashMap<>(properties);
    complete.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    complete.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    complete.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    complete.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    Map<String, ?> values =
        producer ? new ProducerConfig(complete).values() : new ConsumerConfig(complete).values();
    Map<String, Object> out = new TreeMap<>();
    for (String name : producer ? PRODUCER_SHOWN : CONSUMER_SHOWN) {
      out.put(name, show(values.get(name)));
    }
    return out;
  }

  private static String show(Object value) {
    return switch (value) {
      case Class<?> type -> type.getName();
      case List<?> list -> String.join(",", list.stream().map(KafkaClients::show).toList());
      case null -> "null";
      default -> value.toString();
    };
  }
}
