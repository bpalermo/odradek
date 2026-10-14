package io.github.bpalermo.odradek.e2eclient;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * What a run is told to do. Every setting is a flag ({@code --name value} or {@code --name=value})
 * and an environment variable ({@code E2E_NAME}, upper case, dashes as underscores); a flag wins
 * over the variable, and the variable over the default.
 *
 * @param mode producer or consumer
 * @param socket the sidecar's Unix socket
 * @param topic the topic to produce to or consume from
 * @param runId the run's identifier. A producer writes it into every record (a random UUID when
 *     unset); a consumer given one ignores the records of other runs, and otherwise tracks every
 *     run it sees
 * @param httpPort where {@code /results} and {@code /healthz} are served; 0 picks a free port
 * @param exitWhenDone whether a producer exits once it has sent everything, instead of serving its
 *     results until it is stopped
 * @param rate records per second, over all keys
 * @param count records to send in all; 0 for no limit
 * @param duration how long to send; zero for no limit
 * @param keys how many keys ({@code key-0} …) the records are spread over, round robin
 * @param maxProcessing the processing deadline the consumer asks for; zero for the server's
 * @param maxUnacked records delivered ahead of the handler; 0 for the client's default
 * @param failEvery fail each record whose {@code seq} is a multiple of this; 0 for none
 * @param failTimes how many times such a record fails before it succeeds
 * @param poisonKeys keys whose records always fail
 * @param failHold how long the handler holds a record it fails before it throws
 * @param handlerDelay how long the handler takes for each record it processes
 * @param restartDelay the pause before a consumer that stopped is started again
 * @param soleConsumer whether this process is expected to see every record of the keys it sees.
 *     When it is not (several pods share the group through a dispatcher), gaps are the other pods'
 *     records: counted, but neither logged nor held against {@code clean}
 * @param stopTimeout how long a stop may take before the final results line is written anyway
 */
public record Settings(
    Mode mode,
    Path socket,
    String topic,
    String runId,
    int httpPort,
    boolean exitWhenDone,
    double rate,
    long count,
    Duration duration,
    int keys,
    Duration maxProcessing,
    int maxUnacked,
    long failEvery,
    int failTimes,
    Set<String> poisonKeys,
    Duration failHold,
    Duration handlerDelay,
    Duration restartDelay,
    boolean soleConsumer,
    Duration stopTimeout) {

  /** What a run does. */
  public enum Mode {
    PRODUCER,
    CONSUMER
  }

  public Settings {
    poisonKeys = Set.copyOf(poisonKeys);
  }

  /** One setting: its flag, default and help. */
  private record Option(String name, String defaultValue, String help) {
    String env() {
      return "E2E_" + name.toUpperCase(Locale.ROOT).replace('-', '_');
    }
  }

  private static final List<Option> OPTIONS =
      List.of(
          new Option("mode", "", "producer or consumer (required)"),
          new Option("socket", "/var/run/kafka/kafka.sock", "the sidecar's Unix socket"),
          new Option("topic", "", "topic to produce to or consume from (required)"),
          new Option(
              "run-id",
              "",
              "run identifier; producer: written into each record (default: a random UUID);"
                  + " consumer: only records of this run are tracked (default: every run)"),
          new Option(
              "stop-timeout",
              "25s",
              "how long a stop may take before the final results line is written anyway"),
          new Option("http-port", "8080", "port of GET /results and GET /healthz; 0 picks one"),
          new Option(
              "exit-when-done",
              "false",
              "producer: exit once everything is sent, instead of serving results until stopped"),
          new Option("rate", "10", "producer: records per second, over all keys"),
          new Option("count", "0", "producer: records to send; 0 for no limit"),
          new Option("duration", "0", "producer: how long to send, e.g. 10m; 0 for no limit"),
          new Option("keys", "8", "producer: number of keys, key-0 to key-<K-1>, round robin"),
          new Option(
              "max-processing", "0", "consumer: processing deadline asked for; 0 for the server's"),
          new Option("max-unacked", "0", "consumer: records delivered ahead of the handler"),
          new Option("fail-every", "0", "consumer: fail records whose seq is a multiple of N"),
          new Option("fail-times", "1", "consumer: how many times each such record fails"),
          new Option("poison-keys", "", "consumer: comma-separated keys whose records always fail"),
          new Option(
              "fail-hold",
              "0",
              "consumer: how long a failing record is held before the handler throws; above the"
                  + " processing deadline for a dispatcher to count the failure"),
          new Option("handler-delay", "0", "consumer: time the handler takes per record"),
          new Option("restart-delay", "1s", "consumer: pause before restarting a failed consumer"),
          new Option(
              "sole-consumer",
              "true",
              "consumer: false when several pods share the group through a dispatcher (gaps are"
                  + " then the other pods' records)"));

  private static final Pattern DURATION = Pattern.compile("(\\d+(?:\\.\\d+)?)(ms|s|m|h)");

  /** Whether the arguments ask for the usage text. */
  public static boolean wantsHelp(List<String> args) {
    return args.contains("--help") || args.contains("-h");
  }

  /** The usage text: every flag, its variable, its default. */
  public static String usage() {
    StringBuilder out = new StringBuilder("usage: e2e-client --mode producer|consumer [flags]\n\n");
    for (Option option : OPTIONS) {
      out.append(String.format(Locale.ROOT, "  --%-16s %-20s ", option.name(), option.env()))
          .append(option.help());
      if (!option.defaultValue().isEmpty()) {
        out.append(" [").append(option.defaultValue()).append(']');
      }
      out.append('\n');
    }
    return out.toString();
  }

  /**
   * Reads the settings from flags and environment.
   *
   * @throws IllegalArgumentException with a message for the user when something is wrong
   */
  public static Settings parse(List<String> args, Map<String, String> env) {
    Map<String, String> values = new LinkedHashMap<>();
    for (Option option : OPTIONS) {
      String fromEnv = env.get(option.env());
      values.put(option.name(), fromEnv != null ? fromEnv.trim() : option.defaultValue());
    }
    for (int i = 0; i < args.size(); i++) {
      String arg = args.get(i);
      if (!arg.startsWith("--")) {
        throw new IllegalArgumentException("unexpected argument: " + arg);
      }
      String name = arg.substring(2);
      String value;
      int equals = name.indexOf('=');
      if (equals >= 0) {
        value = name.substring(equals + 1);
        name = name.substring(0, equals);
      } else if (i + 1 < args.size()) {
        value = args.get(++i);
      } else {
        throw new IllegalArgumentException("--" + name + " needs a value");
      }
      if (!values.containsKey(name)) {
        throw new IllegalArgumentException("unknown flag --" + name);
      }
      values.put(name, value.trim());
    }

    Mode mode =
        switch (values.get("mode").toLowerCase(Locale.ROOT)) {
          case "producer" -> Mode.PRODUCER;
          case "consumer" -> Mode.CONSUMER;
          case "" -> throw new IllegalArgumentException("--mode is required");
          default ->
              throw new IllegalArgumentException(
                  "--mode must be producer or consumer, not " + values.get("mode"));
        };
    String topic = values.get("topic");
    if (topic.isEmpty()) {
      throw new IllegalArgumentException("--topic is required");
    }
    String runId = values.get("run-id");
    if (runId.isEmpty() && mode == Mode.PRODUCER) {
      runId = UUID.randomUUID().toString();
    }
    String socket = values.get("socket");
    if (socket.isEmpty()) {
      throw new IllegalArgumentException("--socket must not be empty");
    }

    Settings settings =
        new Settings(
            mode,
            Path.of(socket),
            topic,
            runId,
            (int) integer(values, "http-port", 0, 65_535),
            bool(values, "exit-when-done"),
            positive(values, "rate"),
            integer(values, "count", 0, Long.MAX_VALUE),
            duration(values, "duration"),
            (int) integer(values, "keys", 1, 1_000_000),
            duration(values, "max-processing"),
            (int) integer(values, "max-unacked", 0, Integer.MAX_VALUE),
            integer(values, "fail-every", 0, Long.MAX_VALUE),
            (int) integer(values, "fail-times", 1, Integer.MAX_VALUE),
            list(values.get("poison-keys")),
            duration(values, "fail-hold"),
            duration(values, "handler-delay"),
            duration(values, "restart-delay"),
            bool(values, "sole-consumer"),
            duration(values, "stop-timeout"));
    if (settings.maxProcessing().toMillis() > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("--max-processing is too long");
    }
    return settings;
  }

  /** The settings as they appear in the results. */
  public Map<String, Object> describe() {
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("mode", mode.name().toLowerCase(Locale.ROOT));
    out.put("socket", socket.toString());
    out.put("topic", topic);
    out.put("runId", runId);
    out.put("httpPort", httpPort);
    out.put("stopTimeoutMs", stopTimeout.toMillis());
    if (mode == Mode.PRODUCER) {
      out.put("exitWhenDone", exitWhenDone);
      out.put("rate", rate);
      out.put("count", count);
      out.put("durationMs", duration.toMillis());
      out.put("keys", keys);
    } else {
      out.put("maxProcessingMs", maxProcessing.toMillis());
      out.put("maxUnacked", maxUnacked);
      out.put("failEvery", failEvery);
      out.put("failTimes", failTimes);
      out.put("poisonKeys", poisonKeys.stream().sorted().toList());
      out.put("failHoldMs", failHold.toMillis());
      out.put("handlerDelayMs", handlerDelay.toMillis());
      out.put("restartDelayMs", restartDelay.toMillis());
      out.put("soleConsumer", soleConsumer);
    }
    return out;
  }

  private static long integer(Map<String, String> values, String name, long min, long max) {
    long value;
    try {
      value = Long.parseLong(values.get(name));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "--" + name + " must be an integer, not " + values.get(name), e);
    }
    if (value < min || value > max) {
      throw new IllegalArgumentException(
          "--" + name + " must be between " + min + " and " + max + ", not " + value);
    }
    return value;
  }

  private static double positive(Map<String, String> values, String name) {
    double value;
    try {
      value = Double.parseDouble(values.get(name));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "--" + name + " must be a number, not " + values.get(name), e);
    }
    if (!(value > 0) || Double.isInfinite(value)) {
      throw new IllegalArgumentException("--" + name + " must be above 0, not " + value);
    }
    return value;
  }

  private static boolean bool(Map<String, String> values, String name) {
    return switch (values.get(name).toLowerCase(Locale.ROOT)) {
      case "true", "1", "yes" -> true;
      case "false", "0", "no", "" -> false;
      default ->
          throw new IllegalArgumentException(
              "--" + name + " must be true or false, not " + values.get(name));
    };
  }

  /** {@code 0}, or a number with a unit: {@code 250ms}, {@code 1.5s}, {@code 10m}, {@code 1h}. */
  static Duration parseDuration(String text) {
    if (text.equals("0")) {
      return Duration.ZERO;
    }
    Matcher matcher = DURATION.matcher(text);
    if (!matcher.matches()) {
      throw new IllegalArgumentException(
          "not a duration: " + text + " (examples: 0, 250ms, 1.5s, 10m, 1h)");
    }
    double amount = Double.parseDouble(matcher.group(1));
    long unitMillis =
        switch (matcher.group(2)) {
          case "ms" -> 1;
          case "s" -> 1000;
          case "m" -> 60_000;
          default -> 3_600_000;
        };
    return Duration.ofMillis(Math.round(amount * unitMillis));
  }

  private static Duration duration(Map<String, String> values, String name) {
    try {
      return parseDuration(values.get(name));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("--" + name + ": " + e.getMessage(), e);
    }
  }

  private static Set<String> list(String text) {
    Set<String> out = new LinkedHashSet<>();
    Arrays.stream(text.split(","))
        .map(String::trim)
        .filter(item -> !item.isEmpty())
        .forEach(out::add);
    return out;
  }
}
