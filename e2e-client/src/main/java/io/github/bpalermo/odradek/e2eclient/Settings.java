package io.github.bpalermo.odradek.e2eclient;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * What a run is told to do. Every setting is a flag ({@code --name value} or {@code --name=value})
 * and an environment variable ({@code E2E_NAME}, upper case, dashes as underscores); a flag wins
 * over the variable, and the variable over the default.
 *
 * @param mode producer, consumer, or both in one process
 * @param transport how records reach Kafka: through the sidecar, or with the Kafka clients here
 * @param socket the sidecar's Unix socket
 * @param topic the topic to produce to or consume from
 * @param runId written into every record this process produces: as given, or a random UUID; empty
 *     for a consumer that was given none
 * @param trackedRunId the run a consumer tracks, ignoring the records of others; empty to track
 *     every run it sees, each on its own
 * @param origin this process, written into every record it produces: by default the pod's name and
 *     a random suffix, so that a restarted pod is a new origin
 * @param lane a label for the results, the {@code started} event and the metrics
 * @param httpPort where {@code /results} and {@code /healthz} are served; 0 picks a free port
 * @param exitWhenDone whether a producer exits once it has sent everything, instead of serving its
 *     results until it is stopped
 * @param rateSchedule records per second over all keys, over time
 * @param count records to send in all; 0 for no limit
 * @param duration how long to send; zero for no limit
 * @param keys how many keys ({@code key-0} …) the records are spread over, round robin
 * @param latencyWindow the rolling window of the latency histograms
 * @param maxProcessing the processing deadline the consumer asks for; zero for the server's
 * @param maxUnacked records delivered ahead of the handler; 0 for the client's default
 * @param failEvery fail each record whose {@code seq} is a multiple of this; 0 for none
 * @param failTimes how many times such a record fails before it succeeds
 * @param poisonKeys keys whose records always fail
 * @param failHold how long the handler holds a record it fails before it throws
 * @param handlerDelay how long the handler takes for each record it processes
 * @param restartDelay the pause before a consumer that stopped is started again
 * @param soleConsumer whether this process is expected to see every record of the keys it sees.
 *     When it is not (several pods share the group), gaps are the other pods' records: counted, but
 *     neither logged nor held against {@code clean}
 * @param stopTimeout how long a stop may take before the final results line is written anyway
 * @param kafka the settings of the {@code kafka} transport
 */
public record Settings(
    Mode mode,
    TransportKind transport,
    Path socket,
    String topic,
    String runId,
    String trackedRunId,
    String origin,
    String lane,
    int httpPort,
    boolean exitWhenDone,
    RateSchedule rateSchedule,
    long count,
    Duration duration,
    int keys,
    Duration latencyWindow,
    Duration maxProcessing,
    int maxUnacked,
    long failEvery,
    int failTimes,
    Set<String> poisonKeys,
    Duration failHold,
    Duration handlerDelay,
    Duration restartDelay,
    boolean soleConsumer,
    Duration stopTimeout,
    Kafka kafka) {

  /** What a run does. */
  public enum Mode {
    PRODUCER,
    CONSUMER,
    BOTH;

    public boolean produces() {
      return this != CONSUMER;
    }

    public boolean consumes() {
      return this != PRODUCER;
    }
  }

  /** How records reach Kafka. */
  public enum TransportKind {
    ODRADEK,
    KAFKA
  }

  /** Where the {@code kafka} transport's client certificate comes from. */
  public enum TlsMode {
    NONE,
    SPIFFE,
    FILES
  }

  /**
   * The settings of the {@code kafka} transport.
   *
   * @param bootstrapServers the brokers
   * @param group the consumer group
   * @param commitInterval how often what was handled is committed, as the server's {@code
   *     consume.commitInterval}
   * @param propertiesFile a Java properties file of Kafka client settings; empty for none
   * @param tls plaintext, or mutual TLS with credentials from the Workload API or from files
   * @param spiffeSocket the Workload API, {@code unix:///path}; empty for {@code
   *     SPIFFE_ENDPOINT_SOCKET}
   * @param tlsDir the directory of {@code tls.crt}, {@code tls.key} and {@code ca.crt}
   * @param brokerSpiffeIds the SPIFFE IDs accepted of the brokers
   * @param brokerTrustDomain a trust domain whose every SPIFFE ID is accepted of the brokers
   */
  public record Kafka(
      String bootstrapServers,
      String group,
      Duration commitInterval,
      String propertiesFile,
      TlsMode tls,
      String spiffeSocket,
      String tlsDir,
      List<String> brokerSpiffeIds,
      String brokerTrustDomain) {
    public Kafka {
      brokerSpiffeIds = List.copyOf(brokerSpiffeIds);
    }
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
          new Option("mode", "", "producer, consumer or both (required)"),
          new Option(
              "transport",
              "odradek",
              "odradek (through the sidecar) or kafka (the Kafka clients in this process)"),
          new Option("socket", "/var/run/kafka/kafka.sock", "odradek: the sidecar's Unix socket"),
          new Option("topic", "", "topic to produce to or consume from (required)"),
          new Option(
              "run-id",
              "",
              "run identifier; producing: written into each record (default: a random UUID);"
                  + " consuming: only records of this run are tracked (default: every run)"),
          new Option(
              "origin",
              "",
              "this process in the records it produces (default: $POD_NAME or $HOSTNAME, and a"
                  + " random suffix)"),
          new Option("lane", "", "a label for the results, the started event and the metrics"),
          new Option(
              "stop-timeout",
              "25s",
              "how long a stop may take before the final results line is written anyway"),
          new Option("http-port", "8080", "port of GET /results and GET /healthz; 0 picks one"),
          new Option(
              "latency-window", "60s", "the rolling window of the latency histograms, e.g. 5m"),
          new Option(
              "exit-when-done",
              "false",
              "producer: exit once everything is sent, instead of serving results until stopped"),
          new Option("rate", "10", "producing: records per second, over all keys"),
          new Option(
              "rate-schedule",
              "",
              "producing: rate:duration steps, e.g. 20:10m,50:5m,20:0; a last step of 0 holds for"
                  + " good. Replaces --rate"),
          new Option("count", "0", "producing: records to send; 0 for no limit"),
          new Option("duration", "0", "producing: how long to send, e.g. 10m; 0 for no limit"),
          new Option("keys", "8", "producing: number of keys, key-0 to key-<K-1>, round robin"),
          new Option(
              "max-processing",
              "0",
              "consuming: processing deadline asked for; 0 for the server's"),
          new Option("max-unacked", "0", "consuming: records delivered ahead of the handler"),
          new Option("fail-every", "0", "consuming: fail records whose seq is a multiple of N"),
          new Option("fail-times", "1", "consuming: how many times each such record fails"),
          new Option(
              "poison-keys", "", "consuming: comma-separated keys whose records always fail"),
          new Option(
              "fail-hold",
              "0",
              "consuming: how long a failing record is held before the handler throws; above the"
                  + " processing deadline for a dispatcher to count the failure"),
          new Option("handler-delay", "0", "consuming: time the handler takes per record"),
          new Option("restart-delay", "1s", "consuming: pause before restarting a failed consumer"),
          new Option(
              "sole-consumer",
              "true",
              "consuming: false when several pods share the group (gaps are then the other pods'"
                  + " records)"),
          new Option("bootstrap-servers", "", "kafka: the brokers (required)"),
          new Option("group", "", "kafka: the consumer group (default: e2e-client-<topic>)"),
          new Option("commit-interval", "5s", "kafka: how often what was handled is committed"),
          new Option("kafka-properties", "", "kafka: a properties file of client settings"),
          new Option("tls", "none", "kafka: none, spiffe (Workload API) or files (--tls-dir)"),
          new Option(
              "spiffe-socket",
              "",
              "kafka: the Workload API, unix:///path (default: $SPIFFE_ENDPOINT_SOCKET)"),
          new Option("tls-dir", "", "kafka: the directory of tls.crt, tls.key and ca.crt"),
          new Option(
              "broker-spiffe-ids", "", "kafka: comma-separated SPIFFE IDs accepted of the brokers"),
          new Option(
              "broker-trust-domain",
              "",
              "kafka: a trust domain whose SPIFFE IDs are accepted of the brokers"));

  private static final Pattern DURATION = Pattern.compile("(\\d+(?:\\.\\d+)?)(ms|s|m|h)");

  /** Whether the arguments ask for the usage text. */
  public static boolean wantsHelp(List<String> args) {
    return args.contains("--help") || args.contains("-h");
  }

  /** The usage text: every flag, its variable, its default. */
  public static String usage() {
    StringBuilder out =
        new StringBuilder("usage: e2e-client --mode producer|consumer|both [flags]\n\n");
    for (Option option : OPTIONS) {
      out.append(String.format(Locale.ROOT, "  --%-19s %-23s ", option.name(), option.env()))
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
          case "both" -> Mode.BOTH;
          case "" -> throw new IllegalArgumentException("--mode is required");
          default ->
              throw new IllegalArgumentException(
                  "--mode must be producer, consumer or both, not " + values.get("mode"));
        };
    TransportKind transport =
        switch (values.get("transport").toLowerCase(Locale.ROOT)) {
          case "odradek" -> TransportKind.ODRADEK;
          case "kafka" -> TransportKind.KAFKA;
          default ->
              throw new IllegalArgumentException(
                  "--transport must be odradek or kafka, not " + values.get("transport"));
        };
    String topic = values.get("topic");
    if (topic.isEmpty()) {
      throw new IllegalArgumentException("--topic is required");
    }
    String trackedRunId = values.get("run-id");
    String runId = trackedRunId;
    if (runId.isEmpty() && mode.produces()) {
      runId = UUID.randomUUID().toString();
    }
    String socket = values.get("socket");
    if (socket.isEmpty()) {
      throw new IllegalArgumentException("--socket must not be empty");
    }
    String origin = values.get("origin");
    if (origin.isEmpty()) {
      origin = defaultOrigin(env);
    }
    RateSchedule schedule;
    if (values.get("rate-schedule").isEmpty()) {
      schedule = RateSchedule.constant(positive(values, "rate"));
    } else {
      try {
        schedule = RateSchedule.parse(values.get("rate-schedule"));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("--rate-schedule: " + e.getMessage(), e);
      }
    }
    Duration latencyWindow = duration(values, "latency-window");
    if (latencyWindow.compareTo(Duration.ofSeconds(1)) < 0) {
      throw new IllegalArgumentException("--latency-window must be at least 1s");
    }

    Settings settings =
        new Settings(
            mode,
            transport,
            Path.of(socket),
            topic,
            runId,
            trackedRunId,
            origin,
            values.get("lane"),
            (int) integer(values, "http-port", 0, 65_535),
            bool(values, "exit-when-done"),
            schedule,
            integer(values, "count", 0, Long.MAX_VALUE),
            duration(values, "duration"),
            (int) integer(values, "keys", 1, 1_000_000),
            latencyWindow,
            duration(values, "max-processing"),
            (int) integer(values, "max-unacked", 0, Integer.MAX_VALUE),
            integer(values, "fail-every", 0, Long.MAX_VALUE),
            (int) integer(values, "fail-times", 1, Integer.MAX_VALUE),
            list(values.get("poison-keys")),
            duration(values, "fail-hold"),
            duration(values, "handler-delay"),
            duration(values, "restart-delay"),
            bool(values, "sole-consumer"),
            duration(values, "stop-timeout"),
            kafka(values, transport, topic));
    if (settings.maxProcessing().toMillis() > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("--max-processing is too long");
    }
    return settings;
  }

  private static Kafka kafka(Map<String, String> values, TransportKind transport, String topic) {
    TlsMode tls =
        switch (values.get("tls").toLowerCase(Locale.ROOT)) {
          case "none", "" -> TlsMode.NONE;
          case "spiffe" -> TlsMode.SPIFFE;
          case "files" -> TlsMode.FILES;
          default ->
              throw new IllegalArgumentException(
                  "--tls must be none, spiffe or files, not " + values.get("tls"));
        };
    String group = values.get("group").isEmpty() ? "e2e-client-" + topic : values.get("group");
    List<String> brokerIds = new ArrayList<>(list(values.get("broker-spiffe-ids")));
    Kafka kafka =
        new Kafka(
            values.get("bootstrap-servers"),
            group,
            duration(values, "commit-interval"),
            values.get("kafka-properties"),
            tls,
            values.get("spiffe-socket"),
            values.get("tls-dir"),
            brokerIds,
            values.get("broker-trust-domain"));
    if (transport == TransportKind.ODRADEK) {
      if (tls != TlsMode.NONE) {
        throw new IllegalArgumentException(
            "--tls is for --transport kafka: the sidecar has a tls section of its own");
      }
      return kafka;
    }
    if (kafka.bootstrapServers().isEmpty()) {
      throw new IllegalArgumentException("--bootstrap-servers is required with --transport kafka");
    }
    if (kafka.commitInterval().isZero()) {
      throw new IllegalArgumentException("--commit-interval must be above 0");
    }
    for (String id : brokerIds) {
      if (!id.startsWith("spiffe://")) {
        throw new IllegalArgumentException("--broker-spiffe-ids: not a SPIFFE ID: " + id);
      }
    }
    switch (tls) {
      case SPIFFE -> {
        if (!kafka.spiffeSocket().isEmpty() && !kafka.spiffeSocket().startsWith("unix://")) {
          throw new IllegalArgumentException("--spiffe-socket must be unix:///path");
        }
        if (brokerIds.isEmpty() && kafka.brokerTrustDomain().isEmpty()) {
          throw new IllegalArgumentException(
              "--tls spiffe needs --broker-spiffe-ids or --broker-trust-domain");
        }
      }
      case FILES -> {
        if (!kafka.tlsDir().startsWith("/")) {
          throw new IllegalArgumentException("--tls files needs --tls-dir, an absolute path");
        }
      }
      case NONE -> {}
    }
    return kafka;
  }

  /** The pod's name, or the host's, and a random suffix: a new origin each time the JVM starts. */
  static String defaultOrigin(Map<String, String> env) {
    String name = env.getOrDefault("POD_NAME", "").trim();
    if (name.isEmpty()) {
      name = env.getOrDefault("HOSTNAME", "").trim();
    }
    if (name.isEmpty()) {
      name = "e2e-client";
    }
    byte[] suffix = new byte[4];
    ThreadLocalRandom.current().nextBytes(suffix);
    return name + "-" + HexFormat.of().formatHex(suffix);
  }

  /** The settings as they appear in the results. */
  public Map<String, Object> describe() {
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("mode", mode.name().toLowerCase(Locale.ROOT));
    out.put("transport", transport.name().toLowerCase(Locale.ROOT));
    out.put("lane", lane);
    out.put("origin", origin);
    out.put("topic", topic);
    out.put("runId", runId);
    out.put("trackedRunId", trackedRunId);
    out.put("httpPort", httpPort);
    out.put("stopTimeoutMs", stopTimeout.toMillis());
    out.put("latencyWindowMs", latencyWindow.toMillis());
    if (transport == TransportKind.ODRADEK) {
      out.put("socket", socket.toString());
    } else {
      out.put("bootstrapServers", kafka.bootstrapServers());
      out.put("commitIntervalMs", kafka.commitInterval().toMillis());
      out.put("kafkaProperties", kafka.propertiesFile());
      out.put("tls", kafka.tls().name().toLowerCase(Locale.ROOT));
      if (kafka.tls() == TlsMode.SPIFFE) {
        out.put("spiffeSocket", kafka.spiffeSocket());
      } else if (kafka.tls() == TlsMode.FILES) {
        out.put("tlsDir", kafka.tlsDir());
      }
      if (kafka.tls() != TlsMode.NONE) {
        out.put("brokerSpiffeIds", kafka.brokerSpiffeIds());
        out.put("brokerTrustDomain", kafka.brokerTrustDomain());
      }
    }
    if (mode.produces()) {
      out.put("exitWhenDone", exitWhenDone);
      out.put("rate", rateSchedule.steps().get(0).rate());
      out.put("rateSchedule", rateSchedule.toString());
      out.put("count", count);
      out.put("durationMs", duration.toMillis());
      out.put("keys", keys);
    }
    if (mode.consumes()) {
      if (transport == TransportKind.KAFKA) {
        out.put("group", kafka.group());
      }
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
