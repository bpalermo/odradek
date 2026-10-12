package io.github.bpalermo.odradek.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

/**
 * Smoke test of the two e2e brokers, using kafka-clients directly (no sidecar involved yet).
 *
 * <p>The bootstrap addresses come from {@code ODRADEK_E2E_KAFKA_A} and {@code ODRADEK_E2E_KAFKA_B};
 * a missing variable fails the test.
 */
class BrokerSmokeTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(30);

  private static String bootstrap(String variable) {
    String value = System.getenv(variable);
    if (value == null || value.isBlank()) {
      return fail(
          "%s is not set: export the bootstrap address of the e2e broker (e.g. localhost:9092)"
              + " and run with --config=e2e so Bazel passes it to the test",
          variable);
    }
    return value.trim();
  }

  @Test
  void brokerA() throws Exception {
    roundTrip(bootstrap("ODRADEK_E2E_KAFKA_A"));
  }

  @Test
  void brokerB() throws Exception {
    roundTrip(bootstrap("ODRADEK_E2E_KAFKA_B"));
  }

  /** Creates a topic, produces one record with acks=all and idempotence, reads it back. */
  private static void roundTrip(String bootstrap) throws Exception {
    String topic = "odradek-e2e-" + UUID.randomUUID();
    String key = "key-" + UUID.randomUUID();
    String value = "value-" + UUID.randomUUID();
    long timestamp = System.currentTimeMillis() - 1000;

    try (Admin admin =
        Admin.create(
            Map.<String, Object>of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
      admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
      try {
        RecordMetadata metadata = produce(bootstrap, topic, key, value, timestamp);
        assertThat(metadata.topic()).isEqualTo(topic);
        assertThat(metadata.hasOffset()).isTrue();

        ConsumerRecord<String, String> record = consumeOne(bootstrap, topic);
        assertThat(record.key()).isEqualTo(key);
        assertThat(record.value()).isEqualTo(value);
        assertThat(record.timestamp()).isEqualTo(timestamp);
        assertThat(record.offset()).isEqualTo(metadata.offset());
      } finally {
        admin.deleteTopics(List.of(topic)).all().get();
      }
    }
  }

  private static RecordMetadata produce(
      String bootstrap, String topic, String key, String value, long timestamp) throws Exception {
    Map<String, Object> config =
        Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            bootstrap,
            ProducerConfig.ACKS_CONFIG,
            "all",
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
            true,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class);
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(config)) {
      return producer
          .send(new ProducerRecord<>(topic, null, timestamp, key, value))
          .get(TIMEOUT.toSeconds(), java.util.concurrent.TimeUnit.SECONDS);
    }
  }

  private static ConsumerRecord<String, String> consumeOne(String bootstrap, String topic) {
    Map<String, Object> config =
        Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            bootstrap,
            ConsumerConfig.GROUP_ID_CONFIG,
            "odradek-e2e-" + UUID.randomUUID(),
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
            "earliest",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
            false,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class);
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
      consumer.subscribe(List.of(topic));
      long deadline = System.nanoTime() + TIMEOUT.toNanos();
      while (System.nanoTime() < deadline) {
        for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
          return record;
        }
      }
    }
    return fail("no record read back from %s on %s within %s", topic, bootstrap, TIMEOUT);
  }
}
