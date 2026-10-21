package io.github.bpalermo.odradek.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.bpalermo.odradek.client.OdradekClient;
import io.github.bpalermo.odradek.e2eclient.BothRun;
import io.github.bpalermo.odradek.e2eclient.ConsumerRun;
import io.github.bpalermo.odradek.e2eclient.Events;
import io.github.bpalermo.odradek.e2eclient.KafkaTransport;
import io.github.bpalermo.odradek.e2eclient.Latency;
import io.github.bpalermo.odradek.e2eclient.Metrics;
import io.github.bpalermo.odradek.e2eclient.OdradekTransport;
import io.github.bpalermo.odradek.e2eclient.ProducerRun;
import io.github.bpalermo.odradek.e2eclient.Settings;
import io.github.bpalermo.odradek.e2eclient.Transport;
import io.github.bpalermo.odradek.server.tls.FakeWorkloadApi;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The e2e-client program (//e2e-client) against a server and a real broker: what the in-cluster run
 * does, in one JVM. Every mode, through the sidecar and with the Kafka clients directly; the
 * consumers fail some records on purpose, which they must count as expected redeliveries and
 * nothing worse.
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

  /** The settings of the kafka transport against the first plaintext broker. */
  private Settings kafkaSettings(String... args) {
    List<String> all = new ArrayList<>();
    Collections.addAll(
        all,
        "--transport",
        "kafka",
        "--bootstrap-servers",
        ServerHarness.KAFKA_A,
        "--group",
        "e2e-client-kafka-" + id,
        "--commit-interval",
        "200ms",
        "--kafka-properties",
        "");
    Collections.addAll(all, args);
    return settings(all.toArray(String[]::new));
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
      Transport transport = OdradekTransport.of(client);
      AtomicReference<Throwable> consumerError = new AtomicReference<>();
      Thread consuming =
          Thread.ofPlatform()
              .start(
                  () -> {
                    try {
                      consumer.run(transport);
                    } catch (Throwable e) {
                      consumerError.set(e);
                    }
                  });

      producer.run(transport);
      Map<String, Object> produced = producer.results();
      assertThat(produced)
          .as("producer results")
          .containsEntry("state", "done")
          .containsEntry("clean", true)
          .containsEntry("sent", (long) RECORDS)
          .containsEntry("acked", (long) RECORDS);
      assertThat(cumulative(produced, "produce")).containsEntry("count", (long) RECORDS);

      long deadline = System.nanoTime() + TIMEOUT.toNanos();
      while (consumer.ledger().distinctProcessed() < RECORDS && System.nanoTime() < deadline) {
        Thread.sleep(50);
      }
      // A moment for anything that should not come.
      Thread.sleep(500);
      consumer.stop();
      consuming.join(TIMEOUT.toMillis());
      assertThat(consumerError.get()).isNull();
      assertCleanWithDeliberateFailures(consumer.results(), RECORDS);
      // Another process produced them: nothing timed on this one's clock.
      assertThat(cumulative(consumer.results(), "endToEndSelf")).containsEntry("count", 0L);
      assertThat(cumulative(consumer.results(), "endToEndCross"))
          .containsEntry("count", (long) RECORDS);
    }
  }

  @Test
  void bothModesInOneProcessThroughTheSidecarTimeTheirOwnRecords() throws Exception {
    BothRun both =
        new BothRun(
            settings(
                "--mode",
                "both",
                "--rate",
                "500",
                "--count",
                String.valueOf(RECORDS),
                "--keys",
                String.valueOf(KEYS),
                "--lane",
                "b"),
            events,
            latency());
    try (OdradekClient client = OdradekClient.connect(harness.socket())) {
      runUntilConsumed(List.of(both), List.of(OdradekTransport.of(client)), RECORDS);
    }
    assertBothClean(both, RECORDS);
  }

  @Test
  void theKafkaTransportProducesAndConsumesInOneProcessAndFailsRecordsOnPurpose() throws Exception {
    Settings settings =
        kafkaSettings(
            "--mode",
            "both",
            "--rate",
            "500",
            "--count",
            String.valueOf(RECORDS),
            "--keys",
            String.valueOf(KEYS),
            "--fail-every",
            "50",
            "--restart-delay",
            "0",
            "--lane",
            "a");
    BothRun both = new BothRun(settings, events, latency());
    try (KafkaTransport transport = KafkaTransport.create(settings)) {
      assertThat(transport.describe().toString()).contains("acks=-1").contains("linger.ms=5");
      runUntilConsumed(List.of(both), List.of(transport), RECORDS);
    }
    Map<String, Object> results = both.results();
    assertThat(results).containsEntry("transport", "kafka").containsEntry("lane", "a");
    assertThat(producerOf(results))
        .containsEntry("clean", true)
        .containsEntry("acked", (long) RECORDS);
    assertCleanWithDeliberateFailures(consumerOf(results), RECORDS);
    assertThat(cumulative(results, "endToEndSelf")).containsEntry("count", (long) RECORDS);
    assertThat(anomalies).isEmpty();
  }

  @Test
  void twoProcessesShareATopicAndAGroupAndTheirLedgersAddUpByOrigin() throws Exception {
    List<BothRun> pods = new ArrayList<>();
    List<Transport> transports = new ArrayList<>();
    try {
      for (int pod = 0; pod < 2; pod++) {
        Settings settings =
            kafkaSettings(
                "--mode",
                "both",
                "--rate",
                "300",
                "--count",
                String.valueOf(RECORDS),
                "--keys",
                String.valueOf(KEYS),
                "--sole-consumer",
                "false",
                "--origin",
                "pod-" + pod + "-" + id.substring(0, 8));
        pods.add(new BothRun(settings, events, latency()));
        transports.add(KafkaTransport.create(settings));
      }
      runUntilConsumed(pods, transports, 2 * RECORDS);
    } finally {
      transports.forEach(Transport::close);
    }

    // Every (origin, key) a producer acknowledged is covered by the consumers' processed ranges,
    // once: no record lost, none processed twice, whichever pod had its partition.
    Map<String, List<List<Long>>> processed = new TreeMap<>();
    long selfTimed = 0;
    long ownRecords = 0;
    for (BothRun pod : pods) {
      Map<String, Object> results = pod.results();
      String origin = (String) results.get("origin");
      for (Map<String, Object> key : keys(consumerOf(results))) {
        @SuppressWarnings("unchecked")
        List<List<Long>> ranges = (List<List<Long>>) key.get("processedRanges");
        processed
            .computeIfAbsent(key.get("origin") + "/" + key.get("key"), _ -> new ArrayList<>())
            .addAll(ranges);
        if (origin.equals(key.get("origin"))) {
          ownRecords += (long) key.get("processed");
        }
      }
      selfTimed += (long) cumulative(results, "endToEndSelf").get("count");
      assertThat(consumerOf(results))
          .containsEntry("duplicates", 0L)
          .containsEntry("reorders", 0L)
          .containsEntry("checksumFailures", 0L);
    }
    Map<String, List<List<Long>>> expected = new TreeMap<>();
    for (BothRun pod : pods) {
      for (Map<String, Object> key : keys(producerOf(pod.results()))) {
        assertThat(key.get("lastAckedSeq")).isEqualTo((long) RECORDS / KEYS);
        expected.put(
            key.get("origin") + "/" + key.get("key"),
            List.of(List.of(1L, (long) key.get("lastAckedSeq"))));
      }
    }
    Map<String, List<List<Long>>> merged = new TreeMap<>();
    processed.forEach((k, ranges) -> merged.put(k, merge(ranges)));
    assertThat(merged).isEqualTo(expected);
    // Each pod timed on its own clock the records of its own that it consumed itself.
    assertThat(selfTimed).isEqualTo(ownRecords).isPositive();
    assertThat(anomalies).isEmpty();
  }

  @Test
  void theKafkaTransportTakesItsCertificateFromTheWorkloadApiAndChecksTheBrokersSpiffeId()
      throws Exception {
    String kafkaTls = ServerHarness.bootstrap("ODRADEK_E2E_KAFKA_TLS");
    String kafkaTlsAdmin = ServerHarness.bootstrap("ODRADEK_E2E_KAFKA_TLS_ADMIN");
    Path certificates = Path.of(ServerHarness.bootstrap("ODRADEK_E2E_TLS_DIR")).resolve("client");
    String tlsTopic = "odradek-e2e-client-mtls-" + id;
    ServerHarness.createTopic(kafkaTlsAdmin, tlsTopic, 1);
    Path dir = Files.createTempDirectory(Path.of("/tmp"), "odradek-e2e-client-tls");
    Path socket = dir.resolve("agent.sock");
    FakeWorkloadApi agent = FakeWorkloadApi.start(socket);
    try {
      agent.publish(
          "spiffe://e2e.test/odradek",
          Files.readString(certificates.resolve("tls.crt")),
          Files.readString(certificates.resolve("tls.key")),
          Files.readString(certificates.resolve("ca.crt")),
          Map.of());
      Settings settings =
          Settings.parse(
              List.of(
                  "--mode",
                  "both",
                  "--transport",
                  "kafka",
                  "--topic",
                  tlsTopic,
                  "--bootstrap-servers",
                  kafkaTls,
                  "--commit-interval",
                  "200ms",
                  "--tls",
                  "spiffe",
                  "--spiffe-socket",
                  "unix://" + socket,
                  "--broker-spiffe-ids",
                  "spiffe://e2e.test/broker",
                  "--rate",
                  "200",
                  "--count",
                  "60",
                  "--keys",
                  "3"),
              Map.of());
      BothRun both = new BothRun(settings, events, latency());
      try (KafkaTransport transport = KafkaTransport.create(settings)) {
        assertThat(transport.describe().toString()).contains("security.protocol=SSL");
        runUntilConsumed(List.of(both), List.of(transport), 60);
      }
      assertBothClean(both, 60);
    } finally {
      agent.close();
      Files.deleteIfExists(socket);
      Files.delete(dir);
      ServerHarness.deleteTopics(kafkaTlsAdmin, List.of(tlsTopic));
    }
  }

  /**
   * Runs each process over its transport until every producer is done and the consumers have
   * processed {@code records} between them, then stops them.
   */
  private static void runUntilConsumed(
      List<BothRun> runs, List<? extends Transport> transports, long records) throws Exception {
    List<Thread> threads = new ArrayList<>();
    List<Throwable> errors = new CopyOnWriteArrayList<>();
    for (int i = 0; i < runs.size(); i++) {
      BothRun run = runs.get(i);
      Transport transport = transports.get(i);
      threads.add(
          Thread.ofPlatform()
              .start(
                  () -> {
                    try {
                      run.run(transport);
                    } catch (Throwable e) {
                      errors.add(e);
                    }
                  }));
    }
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (System.nanoTime() < deadline
        && (runs.stream().anyMatch(run -> !run.producer().state().equals("done"))
            || runs.stream().mapToLong(run -> run.consumer().ledger().distinctProcessed()).sum()
                < records)) {
      Thread.sleep(50);
    }
    // A moment for anything that should not come.
    Thread.sleep(500);
    runs.forEach(BothRun::stop);
    for (Thread thread : threads) {
      thread.join(TIMEOUT.toMillis());
    }
    assertThat(errors).isEmpty();
  }

  private void assertBothClean(BothRun both, long records) {
    Map<String, Object> results = both.results();
    assertThat(results).as("results: %s", results).containsEntry("clean", true);
    assertThat(producerOf(results)).containsEntry("acked", records);
    assertThat(consumerOf(results))
        .containsEntry("processed", records)
        .containsEntry("duplicates", 0L)
        .containsEntry("missing", 0L)
        .containsEntry("reorders", 0L);
    assertThat(cumulative(results, "produce")).containsEntry("count", records);
    // Every record was this process's own: all timed on its monotonic clock, and on the wall's.
    assertThat(cumulative(results, "endToEndSelf")).containsEntry("count", records);
    assertThat(cumulative(results, "endToEndCross")).containsEntry("count", records);
    assertThat((long) cumulative(results, "endToEndSelf").get("p50Us")).isPositive();
    assertThat(anomalies).isEmpty();
  }

  private void assertCleanWithDeliberateFailures(Map<String, Object> consumed, long records) {
    // 50 records per key: seq 50 of each key fails once and comes back.
    long failures = KEYS;
    assertThat(consumed)
        .as("consumer results: %s", consumed)
        .containsEntry("clean", true)
        .containsEntry("processed", records)
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

  private static Latency latency() {
    return new Latency(Duration.ofSeconds(60), Metrics.none());
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> cumulative(Map<String, Object> results, String name) {
    Map<String, Object> latency = (Map<String, Object>) results.get("latency");
    return (Map<String, Object>) ((Map<String, Object>) latency.get(name)).get("cumulative");
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> producerOf(Map<String, Object> results) {
    return (Map<String, Object>) results.get("producer");
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> consumerOf(Map<String, Object> results) {
    return results.containsKey("consumer")
        ? (Map<String, Object>) results.get("consumer")
        : results;
  }

  @SuppressWarnings("unchecked")
  private static List<Map<String, Object>> keys(Map<String, Object> ledger) {
    return (List<Map<String, Object>>) ledger.get("keys");
  }

  /** Inclusive ranges merged where they touch; an overlap is kept apart, so that it shows. */
  private static List<List<Long>> merge(List<List<Long>> ranges) {
    List<List<Long>> sorted = new ArrayList<>(ranges);
    sorted.sort((a, b) -> Long.compare(a.get(0), b.get(0)));
    List<List<Long>> out = new ArrayList<>();
    for (List<Long> range : sorted) {
      if (!out.isEmpty() && out.getLast().get(1) + 1 == range.get(0)) {
        out.set(out.size() - 1, List.of(out.getLast().get(0), range.get(1)));
      } else {
        out.add(range);
      }
    }
    return out;
  }
}
