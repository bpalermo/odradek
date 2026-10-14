package io.github.bpalermo.odradek.e2eclient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.time.Duration;
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
    assertThat(settings.topic()).isEqualTo("orders");
    assertThat(settings.socket()).isEqualTo(Path.of("/var/run/kafka/kafka.sock"));
    assertThat(settings.httpPort()).isEqualTo(8080);
    assertThat(settings.rate()).isEqualTo(10.0);
    assertThat(settings.count()).isZero();
    assertThat(settings.duration()).isEqualTo(Duration.ZERO);
    assertThat(settings.keys()).isEqualTo(8);
    assertThat(settings.exitWhenDone()).isFalse();
    // A producer always has a run id.
    assertThat(settings.runId()).hasSize(36);
  }

  @Test
  void aConsumerWithDefaults() {
    Settings settings = parse(Map.of(), "--mode=consumer", "--topic=orders");

    assertThat(settings.mode()).isEqualTo(Settings.Mode.CONSUMER);
    assertThat(settings.runId()).isEmpty();
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
  }

  @Test
  void everyFlag() {
    Settings settings =
        parse(
            Map.of(),
            "--mode",
            "CONSUMER",
            "--socket",
            "/tmp/s.sock",
            "--topic",
            "t",
            "--run-id",
            "r",
            "--http-port",
            "0",
            "--exit-when-done",
            "true",
            "--rate",
            "2.5",
            "--count",
            "100",
            "--duration",
            "10m",
            "--keys",
            "3",
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
            "5s");

    assertThat(settings)
        .isEqualTo(
            new Settings(
                Settings.Mode.CONSUMER,
                Path.of("/tmp/s.sock"),
                "t",
                "r",
                0,
                true,
                2.5,
                100,
                Duration.ofMinutes(10),
                3,
                Duration.ofSeconds(3),
                1,
                10,
                2,
                Set.of("key-1", "key-2"),
                Duration.ofMillis(4500),
                Duration.ofMillis(250),
                Duration.ZERO,
                false,
                Duration.ofSeconds(5)));
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
                "E2E_EXIT_WHEN_DONE", "yes"),
            "--rate",
            "7");

    assertThat(settings.topic()).isEqualTo("from-env");
    assertThat(settings.runId()).isEqualTo("env-run");
    assertThat(settings.rate()).isEqualTo(7.0);
    assertThat(settings.exitWhenDone()).isTrue();
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
    assertRefused("--mode must be producer or consumer", "--mode", "both", "--topic", "t");
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
        .contains("[/var/run/kafka/kafka.sock]");
  }

  @Test
  void theResultsShowTheSettingsOfTheMode() {
    assertThat(parse(Map.of(), "--mode", "producer", "--topic", "t", "--run-id", "r").describe())
        .containsEntry("mode", "producer")
        .containsEntry("runId", "r")
        .containsKey("rate")
        .doesNotContainKey("failEvery");
    assertThat(parse(Map.of(), "--mode", "consumer", "--topic", "t").describe())
        .containsEntry("mode", "consumer")
        .containsKey("failEvery")
        .doesNotContainKey("rate");
  }
}
