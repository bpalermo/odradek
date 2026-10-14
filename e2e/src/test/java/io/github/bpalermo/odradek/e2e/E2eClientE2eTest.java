package io.github.bpalermo.odradek.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.bpalermo.odradek.client.OdradekClient;
import io.github.bpalermo.odradek.e2eclient.ConsumerRun;
import io.github.bpalermo.odradek.e2eclient.Events;
import io.github.bpalermo.odradek.e2eclient.ProducerRun;
import io.github.bpalermo.odradek.e2eclient.Settings;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The e2e-client program (//e2e-client), both modes, against a server and a real broker: what the
 * in-cluster run does, in one JVM. The consumer fails some records on purpose, which it must count
 * as expected redeliveries and nothing worse.
 */
class E2eClientE2eTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(90);
  private static final int RECORDS = 300;
  private static final int KEYS = 6;

  private final String id = UUID.randomUUID().toString();
  private final String topic = "odradek-e2e-client-program-" + id;
  private final List<Map<String, Object>> anomalies = new CopyOnWriteArrayList<>();
  private final Events events =
      (event, fields) -> {
        if (event.equals("anomaly")) {
          anomalies.add(fields);
        }
      };

  private ServerHarness harness;

  @BeforeEach
  void start() throws Exception {
    ServerHarness.createTopic(ServerHarness.KAFKA_A, topic, 3);
    harness =
        ServerHarness.start(
            """
            listener:
              socketPath: %s/kafka.sock
            clusters:
              a:
                properties:
                  bootstrap.servers: %s
                consumerProperties:
                  session.timeout.ms: 10000
                  heartbeat.interval.ms: 1000
            consume:
              commitInterval: 0.2s
              revokeDeadline: 5s
            producers:
              %s:
                clusters: [{cluster: a}]
            consumers:
              %s:
                cluster: a
                groupId: e2e-client-program-%s
            """,
            ServerHarness.KAFKA_A, topic, topic, id);
  }

  @AfterEach
  void stop() throws Exception {
    harness.stop();
    ServerHarness.deleteTopics(ServerHarness.KAFKA_A, List.of(topic));
  }

  private Settings settings(String... args) {
    List<String> all = new ArrayList<>();
    Collections.addAll(
        all,
        "--topic",
        topic,
        "--socket",
        harness.socket().toString(),
        "--run-id",
        id,
        "--http-port",
        "0");
    Collections.addAll(all, args);
    return Settings.parse(all, Map.of());
  }

  @Test
  void aProducerAndAConsumerThatFailsSomeRecordsOnPurposeAgreeOnACleanRun() throws Exception {
    ConsumerRun consumer =
        new ConsumerRun(
            settings(
                "--mode",
                "consumer",
                "--fail-every",
                "50",
                "--fail-times",
                "1",
                "--restart-delay",
                "0"),
            events);
    ProducerRun producer =
        new ProducerRun(
            settings(
                "--mode",
                "producer",
                "--rate",
                "500",
                "--count",
                String.valueOf(RECORDS),
                "--keys",
                String.valueOf(KEYS)),
            events);

    try (OdradekClient client = OdradekClient.connect(harness.socket())) {
      AtomicReference<Throwable> consumerError = new AtomicReference<>();
      Thread consuming =
          Thread.ofPlatform()
              .start(
                  () -> {
                    try {
                      consumer.run(client);
                    } catch (Throwable e) {
                      consumerError.set(e);
                    }
                  });

      producer.run(client);
      Map<String, Object> produced = producer.results();
      assertThat(produced)
          .as("producer results")
          .containsEntry("state", "done")
          .containsEntry("clean", true)
          .containsEntry("sent", (long) RECORDS)
          .containsEntry("acked", (long) RECORDS);

      long deadline = System.nanoTime() + TIMEOUT.toNanos();
      while (consumer.ledger().distinctProcessed() < RECORDS && System.nanoTime() < deadline) {
        Thread.sleep(50);
      }
      // A moment for anything that should not come.
      Thread.sleep(500);
      consumer.stop();
      consuming.join(TIMEOUT.toMillis());
      assertThat(consumerError.get()).isNull();

      Map<String, Object> consumed = consumer.results();
      // 50 records per key: seq 50 of each key fails once and comes back.
      long failures = KEYS;
      assertThat(consumed)
          .as("consumer results: %s", consumed)
          .containsEntry("state", "stopped")
          .containsEntry("clean", true)
          .containsEntry("processed", (long) RECORDS)
          .containsEntry("duplicates", 0L)
          .containsEntry("gaps", 0L)
          .containsEntry("missing", 0L)
          .containsEntry("reorders", 0L)
          .containsEntry("checksumFailures", 0L)
          .containsEntry("malformed", 0L)
          .containsEntry("deliberateFailures", failures)
          .containsEntry("expectedRedeliveries", failures)
          .containsEntry("consumerRestarts", failures)
          .containsEntry("unresolvedFailures", 0L)
          .containsEntry("consumerFailures", Map.of());
      assertThat(anomalies).isEmpty();
    }
  }
}
