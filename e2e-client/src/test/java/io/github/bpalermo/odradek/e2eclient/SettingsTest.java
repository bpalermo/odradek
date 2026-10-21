package io.github.bpalermo.odradek.e2eclient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class SettingsTest {
  private static Settings parse(Map<String, String> env, String... args) {
    return Settings.parse(List.of(args), env);
  }

  @Test
  void aProducerWithDefaults() {
    Settings settings = parse(Map.of(), "--mode", "producer", "--topic", "orders");

    assertThat(settings.mode()).isEqualTo(Settings.Mode.PRODUCER);
    assertThat(settings.transport()).isEqualTo(Settings.TransportKind.ODRADEK);
    assertThat(settings.topic()).isEqualTo("orders");
    assertThat(settings.socket()).isEqualTo(Path.of("/var/run/kafka/kafka.sock"));
    assertThat(settings.httpPort()).isEqualTo(8080);
    assertThat(settings.rateSchedule()).isEqualTo(RateSchedule.constant(10));
    assertThat(settings.count()).isZero();
    assertThat(settings.duration()).isEqualTo(Duration.ZERO);
    assertThat(settings.keys()).isEqualTo(8);
    assertThat(settings.exitWhenDone()).isFalse();
    assertThat(settings.latencyWindow()).isEqualTo(Duration.ofSeconds(60));
    assertThat(settings.lane()).isEmpty();
    // A producer always has a run id, and tracks none.
    assertThat(settings.runId()).hasSize(36);
    assertThat(settings.trackedRunId()).isEmpty();
    assertThat(settings.origin()).matches("e2e-client-[0-9a-f]{8}");
  }

  @Test
  void aConsumerWithDefaults() {
    Settings settings = parse(Map.of(), "--mode=consumer", "--topic=orders");

    assertThat(settings.mode()).isEqualTo(Settings.Mode.CONSUMER);
    assertThat(settings.runId()).isEmpty();
    assertThat(settings.trackedRunId()).isEmpty();
    assertThat(settings.maxProcessing()).isEqualTo(Duration.ZERO);
    assertThat(settings.maxUnacked()).isZero();
    assertThat(settings.failEvery()).isZero();
    assertThat(settings.failTimes()).isEqualTo(1);
    assertThat(settings.poisonKeys()).isEmpty();
    assertThat(settings.failHold()).isEqualTo(Duration.ZERO);
    assertThat(settings.handlerDelay()).isEqualTo(Duration.ZERO);
    assertThat(settings.restartDelay()).isEqualTo(Duration.ofSeconds(1));
    assertThat(settings.soleConsumer()).isTrue();
    assertThat(settings.stopTimeout()).isEqualTo(Duration.ofSeconds(25));
    assertThat(settings.kafka().group()).isEqualTo("e2e-client-orders");
  }

  @Test
  void bothProducesWithARunIdOfItsOwnAndTracksEveryRun() {
    Settings settings = parse(Map.of(), "--mode", "both", "--topic", "t");

    assertThat(settings.mode()).isEqualTo(Settings.Mode.BOTH);
    assertThat(settings.mode().produces()).isTrue();
    assertThat(settings.mode().consumes()).isTrue();
    assertThat(settings.runId()).hasSize(36);
    assertThat(settings.trackedRunId()).isEmpty();

    Settings given = parse(Map.of(), "--mode", "both", "--topic", "t", "--run-id", "r");
    assertThat(given.runId()).isEqualTo("r");
    assertThat(given.trackedRunId()).isEqualTo("r");
  }

  @Test
  void theOriginIsThePodAndASuffixOfThisProcess() {
    String first = parse(Map.of("POD_NAME", "lane-b-0"), "--mode", "both", "--topic", "t").origin();
    String second =
        parse(Map.of("POD_NAME", "lane-b-0"), "--mode", "both", "--topic", "t").origin();

    assertThat(first).matches("lane-b-0-[0-9a-f]{8}");
    // A restarted pod is a new origin.
    assertThat(second).matches("lane-b-0-[0-9a-f]{8}").isNotEqualTo(first);
    assertThat(parse(Map.of("HOSTNAME", "host"), "--mode", "both", "--topic", "t").origin())
        .matches("host-[0-9a-f]{8}");
    assertThat(
            parse(
                    Map.of("POD_NAME", "lane-b-0"),
                    "--mode",
                    "both",
                    "--topic",
                    "t",
                    "--origin",
                    "fixed")
                .origin())
        .isEqualTo("fixed");
  }

  @Test
  void everyFlag() {
    Settings settings =
        parse(
            Map.of(),
            "--mode",
            "CONSUMER",
            "--transport",
            "kafka",
            "--socket",
            "/tmp/s.sock",
            "--topic",
            "t",
            "--run-id",
            "r",
            "--origin",
            "o",
            "--lane",
            "a",
            "--http-port",
            "0",
            "--exit-when-done",
            "true",
            "--rate-schedule",
            "20:10m,50:5m,20:0",
            "--schedule-start",
            "2026-10-22T12:00:00Z",
            "--count",
            "100",
            "--duration",
            "10m",
            "--keys",
            "3",
            "--latency-window",
            "5m",
            "--max-processing",
            "3s",
            "--max-unacked",
            "1",
            "--fail-every",
            "10",
            "--fail-times",
            "2",
            "--poison-keys",
            "key-1, key-2,",
            "--fail-hold",
            "4.5s",
            "--handler-delay",
            "250ms",
            "--restart-delay",
            "0",
            "--sole-consumer",
            "false",
            "--stop-timeout",
            "5s",
            "--bootstrap-servers",
            "broker:9093",
            "--group",
            "g",
            "--commit-interval",
            "200ms",
            "--kafka-properties",
            "/etc/kafka.properties",
            "--tls",
            "spiffe",
            "--spiffe-socket",
            "unix:///run/spire/agent.sock",
            "--tls-dir",
            "/certs",
            "--broker-spiffe-ids",
            "spiffe://td/broker",
            "--broker-trust-domain",
            "td");

    assertThat(settings)
        .isEqualTo(
            new Settings(
                Settings.Mode.CONSUMER,
                Settings.TransportKind.KAFKA,
                Path.of("/tmp/s.sock"),
                "t",
                "r",
                "r",
                "o",
                "a",
                0,
                true,
                RateSchedule.parse("20:10m,50:5m,20:0"),
                Instant.parse("2026-10-22T12:00:00Z"),
                100,
                Duration.ofMinutes(10),
                3,
                Duration.ofMinutes(5),
                Duration.ofSeconds(3),
                1,
                10,
                2,
                Set.of("key-1", "key-2"),
                Duration.ofMillis(4500),
                Duration.ofMillis(250),
                Duration.ZERO,
                false,
                Duration.ofSeconds(5),
                new Settings.Kafka(
                    "broker:9093",
                    "g",
                    Duration.ofMillis(200),
                    "/etc/kafka.properties",
                    Settings.TlsMode.SPIFFE,
                    "unix:///run/spire/agent.sock",
                    "/certs",
                    List.of("spiffe://td/broker"),
                    "td")));
  }

  @Test
  void environmentVariablesAreReadAndFlagsWinOverThem() {
    Settings settings =
        parse(
            Map.of(
                "E2E_MODE", "producer",
                "E2E_TOPIC", "from-env",
                "E2E_RATE", "50",
                "E2E_RUN_ID", "env-run",
                "E2E_EXIT_WHEN_DONE", "yes",
                "E2E_TRANSPORT", "kafka",
                "E2E_BOOTSTRAP_SERVERS", "localhost:9092",
                "E2E_LANE", "b"),
            "--rate",
            "7");

    assertThat(settings.topic()).isEqualTo("from-env");
    assertThat(settings.runId()).isEqualTo("env-run");
    assertThat(settings.rateSchedule()).isEqualTo(RateSchedule.constant(7));
    assertThat(settings.exitWhenDone()).isTrue();
    assertThat(settings.transport()).isEqualTo(Settings.TransportKind.KAFKA);
    assertThat(settings.kafka().bootstrapServers()).isEqualTo("localhost:9092");
    assertThat(settings.kafka().tls()).isEqualTo(Settings.TlsMode.NONE);
    assertThat(settings.lane()).isEqualTo("b");
  }

  @Test
  void durations() {
    assertThat(Settings.parseDuration("0")).isEqualTo(Duration.ZERO);
    assertThat(Settings.parseDuration("250ms")).isEqualTo(Duration.ofMillis(250));
    assertThat(Settings.parseDuration("1.5s")).isEqualTo(Duration.ofMillis(1500));
    assertThat(Settings.parseDuration("10m")).isEqualTo(Duration.ofMinutes(10));
    assertThat(Settings.parseDuration("2h")).isEqualTo(Duration.ofHours(2));
    assertThatThrownBy(() -> Settings.parseDuration("10"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> Settings.parseDuration("-1s"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void mistakesAreRefusedWithAMessage() {
    assertRefused("--mode is required", "--topic", "t");
    assertRefused("--mode must be producer, consumer or both", "--mode", "neither", "--topic", "t");
    assertRefused("--topic is required", "--mode", "producer");
    assertRefused("unknown flag --rat", "--mode", "producer", "--topic", "t", "--rat", "1");
    assertRefused("--rate needs a value", "--mode", "producer", "--topic", "t", "--rate");
    assertRefused("unexpected argument: producer", "producer");
    assertRefused("--rate must be above 0", "--mode", "producer", "--topic", "t", "--rate", "0");
    assertRefused("--rate must be a number", "--mode", "producer", "--topic", "t", "--rate", "x");
    assertRefused("--keys must be between 1", "--mode", "producer", "--topic", "t", "--keys", "0");
    assertRefused(
        "--http-port must be between 0 and 65535",
        "--mode",
        "producer",
        "--topic",
        "t",
        "--http-port",
        "70000");
    assertRefused(
        "--count must be an integer", "--mode", "producer", "--topic", "t", "--count", "1.5");
    assertRefused(
        "--fail-times must be between 1",
        "--mode",
        "consumer",
        "--topic",
        "t",
        "--fail-times",
        "0");
    assertRefused(
        "--fail-hold: not a duration", "--mode", "consumer", "--topic", "t", "--fail-hold", "5");
    assertRefused(
        "--sole-consumer must be true or false",
        "--mode",
        "consumer",
        "--topic",
        "t",
        "--sole-consumer",
        "maybe");
    assertRefused(
        "--transport must be odradek or kafka",
        "--mode",
        "both",
        "--topic",
        "t",
        "--transport",
        "http");
    assertRefused(
        "--bootstrap-servers is required",
        "--mode",
        "both",
        "--topic",
        "t",
        "--transport",
        "kafka");
    assertRefused(
        "--tls is for --transport kafka", "--mode", "both", "--topic", "t", "--tls", "spiffe");
    assertRefused(
        "--tls spiffe needs --broker-spiffe-ids or --broker-trust-domain",
        "--mode",
        "both",
        "--topic",
        "t",
        "--transport",
        "kafka",
        "--bootstrap-servers",
        "b:1",
        "--tls",
        "spiffe");
    assertRefused(
        "--broker-spiffe-ids: not a SPIFFE ID",
        "--mode",
        "both",
        "--topic",
        "t",
        "--transport",
        "kafka",
        "--bootstrap-servers",
        "b:1",
        "--tls",
        "spiffe",
        "--broker-spiffe-ids",
        "broker");
    assertRefused(
        "--spiffe-socket must be unix:///path",
        "--mode",
        "both",
        "--topic",
        "t",
        "--transport",
        "kafka",
        "--bootstrap-servers",
        "b:1",
        "--tls",
        "spiffe",
        "--broker-trust-domain",
        "td",
        "--spiffe-socket",
        "/run/agent.sock");
    assertRefused(
        "--tls files needs --tls-dir",
        "--mode",
        "both",
        "--topic",
        "t",
        "--transport",
        "kafka",
        "--bootstrap-servers",
        "b:1",
        "--tls",
        "files");
    assertRefused(
        "--rate-schedule: only the last step",
        "--mode",
        "producer",
        "--topic",
        "t",
        "--rate-schedule",
        "20:0,50:5m");
    assertRefused(
        "--latency-window must be at least 1s",
        "--mode",
        "producer",
        "--topic",
        "t",
        "--latency-window",
        "10ms");
  }

  private static void assertRefused(String message, String... args) {
    assertThatThrownBy(() -> parse(Map.of(), args))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(message);
  }

  @Test
  void helpAndUsage() {
    assertThat(Settings.wantsHelp(List.of("--mode", "producer", "--help"))).isTrue();
    assertThat(Settings.wantsHelp(List.of("-h"))).isTrue();
    assertThat(Settings.wantsHelp(List.of("--mode", "producer"))).isFalse();
    assertThat(Settings.usage())
        .contains("--mode")
        .contains("E2E_MODE")
        .contains("--poison-keys")
        .contains("E2E_POISON_KEYS")
        .contains("--broker-trust-domain")
        .contains("E2E_RATE_SCHEDULE")
        .contains("[/var/run/kafka/kafka.sock]");
  }

  @Test
  void theResultsShowTheSettingsOfTheMode() {
    assertThat(parse(Map.of(), "--mode", "producer", "--topic", "t", "--run-id", "r").describe())
        .containsEntry("mode", "producer")
        .containsEntry("transport", "odradek")
        .containsEntry("runId", "r")
        .containsEntry("rateSchedule", "10:0")
        .containsKey("rate")
        .containsKey("socket")
        .doesNotContainKey("failEvery");
    assertThat(parse(Map.of(), "--mode", "consumer", "--topic", "t").describe())
        .containsEntry("mode", "consumer")
        .containsKey("failEvery")
        .doesNotContainKey("rate");
    assertThat(
            parse(
                    Map.of(),
                    "--mode",
                    "both",
                    "--topic",
                    "t",
                    "--transport",
                    "kafka",
                    "--bootstrap-servers",
                    "b:1",
                    "--lane",
                    "a")
                .describe())
        .containsEntry("mode", "both")
        .containsEntry("transport", "kafka")
        .containsEntry("lane", "a")
        .containsEntry("bootstrapServers", "b:1")
        .containsEntry("group", "e2e-client-t")
        .containsEntry("tls", "none")
        .containsKey("rate")
        .containsKey("failEvery")
        .doesNotContainKey("socket");
  }

  @Test
  void aScheduleStartMustBeAnInstant() {
    assertThatThrownBy(
            () -> parse(Map.of(), "--mode", "producer", "--topic", "t", "--schedule-start", "noon"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("--schedule-start");
    assertThat(parse(Map.of(), "--mode", "producer", "--topic", "t").scheduleStart()).isNull();
  }
}
