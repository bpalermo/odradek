package io.github.bpalermo.odradek.e2e;

import static org.assertj.core.api.Assertions.fail;

import io.github.bpalermo.odradek.config.v1.ServerConfig;
import io.github.bpalermo.odradek.server.config.ConfigLoader;
import io.github.bpalermo.odradek.server.dispatch.DeliveryServer;
import io.github.bpalermo.odradek.server.dispatch.DispatchService;
import io.github.bpalermo.odradek.server.dispatch.Membership;
import io.github.bpalermo.odradek.server.telemetry.ServerMetrics;
import io.github.bpalermo.odradek.server.tls.TlsSources;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A running dispatcher: a server whose configuration has a {@code dispatch} section and neither
 * producers nor consumers.
 *
 * <p>By default it is assembled in this JVM the way {@code Main} assembles it. When {@code
 * ODRADEK_E2E_SERVER} names an executable, that executable is started instead with {@code
 * --config}: the packaged server, JVM or native.
 *
 * <p>Both the delivery port and the health port are 0 in the configuration, because several suites
 * run on one machine at the same time. The ports the server picked are read from what it logs.
 */
final class DispatcherHarness {
  private static final Duration START_TIMEOUT = Duration.ofSeconds(60);
  private static final Pattern DELIVERY_PORT = Pattern.compile("delivery service on port (\\d+)");
  private static final Pattern HEALTH_PORT = Pattern.compile("health service on port (\\d+)");
  private static final Pattern REPLICAS =
      Pattern.compile("the dispatcher's replicas, generation (\\d+): \\[(.*)]");
  private static final Pattern JOINED =
      Pattern.compile(
          "groupId=([^\\]]+)] Successfully joined group with generation"
              + " Generation\\{generationId=(\\d+)");

  final Path dir;
  private final String body;
  private final String yaml;
  private final ServerConfig config;
  private final List<ManagedChannel> channels = new CopyOnWriteArrayList<>();

  // In-process dispatcher.
  private TlsSources tls;
  private DispatchService service;
  private DeliveryServer delivery;
  private Membership membership;

  // External dispatcher.
  private Process process;
  private volatile CompletableFuture<Integer> deliveryPort;
  private volatile CompletableFuture<Integer> healthPort;

  /** The newest list of replicas the dispatcher said it holds, as "generation: [addresses]". */
  private volatile String replicas = "";

  private int port;

  /** What the server logged, when it is a process: for assertions about its warnings. */
  final List<String> log = new CopyOnWriteArrayList<>();

  private DispatcherHarness(String body) throws Exception {
    dir = Files.createTempDirectory(Path.of("/tmp"), "odradek-e2e-dispatch");
    this.body = body;
    yaml = "admin: {port: 0}\nlistener: {socketPath: %s}\n".formatted(socket()) + body;
    config = ConfigLoader.parse(yaml);
    startDispatcher();
  }

  /**
   * Starts a dispatcher with this configuration, a YAML template for {@code arguments}.
   *
   * <p>A template that names no coordination group is given one of its own: every suite runs
   * against the same brokers, and dispatchers that shared a group would be each other's replicas.
   */
  static DispatcherHarness start(String yamlTemplate, Object... arguments) throws Exception {
    String body = yamlTemplate.formatted(arguments);
    if (!body.contains("coordination:")) {
      String section = "\ndispatch:\n";
      int at = ("\n" + body).indexOf(section);
      if (at < 0) {
        fail("the configuration has no dispatch section:%n%s", body);
      }
      at += section.length() - 1;
      body =
          body.substring(0, at)
              + "  coordination:\n    groupId: odradek-e2e-replicas-"
              + UUID.randomUUID()
              + "\n"
              + body.substring(at);
    }
    return new DispatcherHarness(body);
  }

  /**
   * Starts another replica of this dispatcher: the same configuration, and so the same coordination
   * group, on a port of its own.
   */
  DispatcherHarness replica() throws Exception {
    return new DispatcherHarness(body);
  }

  /** Whether the dispatcher is the packaged server, running as a process. */
  static boolean packaged() {
    String executable = System.getenv("ODRADEK_E2E_SERVER");
    return executable != null && !executable.isBlank();
  }

  /** Where the configuration puts the app's Unix socket, which a pure dispatcher never opens. */
  Path socket() {
    return dir.resolve("kafka.sock");
  }

  /** The port sidecars connect to. It changes when the dispatcher is restarted. */
  int port() {
    return port;
  }

  /** The port of the health service; only when the dispatcher is a process. */
  int healthPort() throws Exception {
    return healthPort.get(START_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
  }

  /** A connection of its own to the delivery port, as each sidecar has. */
  ManagedChannel connect() {
    ManagedChannel channel =
        NettyChannelBuilder.forAddress("localhost", port).usePlaintext().build();
    channels.add(channel);
    return channel;
  }

  void startDispatcher() throws Exception {
    if (!packaged()) {
      tls = TlsSources.create(config);
      service = new DispatchService(config, tls, ServerMetrics.noop());
      delivery = DeliveryServer.start(config.getDispatch(), service, tls);
      port = delivery.port();
      membership =
          Membership.start(
              config,
              tls,
              ServerMetrics.noop(),
              delivery.advertisedAddress(),
              list -> {
                service.replicas(list);
                replicas =
                    list.getGeneration()
                        + ": "
                        + list.getReplicasList().stream().map(r -> r.getAddress()).toList();
              });
      if (!membership.awaitJoined(START_TIMEOUT)) {
        fail("the dispatcher did not join its coordination group within %s", START_TIMEOUT);
      }
      return;
    }
    Path file = dir.resolve("odradek.yaml");
    Files.writeString(file, yaml);
    CompletableFuture<Integer> foundDelivery = new CompletableFuture<>();
    CompletableFuture<Integer> foundHealth = new CompletableFuture<>();
    CompletableFuture<Boolean> joined = new CompletableFuture<>();
    deliveryPort = foundDelivery;
    healthPort = foundHealth;
    String executable = Path.of(System.getenv("ODRADEK_E2E_SERVER")).toAbsolutePath().toString();
    Process started =
        new ProcessBuilder(executable, "--config", file.toString())
            .redirectErrorStream(true)
            .start();
    process = started;
    Thread pump =
        new Thread(
            () -> {
              try (BufferedReader reader =
                  new BufferedReader(
                      new InputStreamReader(started.getInputStream(), StandardCharsets.UTF_8))) {
                for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                  System.out.println(line);
                  log.add(line);
                  Matcher matcher = DELIVERY_PORT.matcher(line);
                  if (matcher.find()) {
                    foundDelivery.complete(Integer.parseInt(matcher.group(1)));
                  }
                  matcher = HEALTH_PORT.matcher(line);
                  if (matcher.find()) {
                    foundHealth.complete(Integer.parseInt(matcher.group(1)));
                  }
                  matcher = REPLICAS.matcher(line);
                  if (matcher.find()) {
                    replicas = matcher.group(1) + ": [" + matcher.group(2) + "]";
                    joined.complete(true);
                  }
                }
              } catch (IOException e) {
                // The process is gone.
              }
              foundDelivery.completeExceptionally(
                  new IllegalStateException("the server exited before it listened"));
              foundHealth.completeExceptionally(
                  new IllegalStateException("the server exited before it listened"));
              joined.completeExceptionally(
                  new IllegalStateException("the server exited before it joined its replicas"));
            },
            "dispatcher-output");
    pump.setDaemon(true);
    pump.start();
    try {
      port = foundDelivery.get(START_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
      // Until it knows its replicas it refuses every declaration, and is not ready.
      joined.get(START_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
    } catch (Exception e) {
      started.destroyForcibly();
      fail(
          "%s did not serve the delivery port and join its coordination group within %s: %s",
          executable, START_TIMEOUT, e);
    }
  }

  /**
   * The newest list of replicas this dispatcher holds, as "generation: [addresses]", the addresses
   * in the list's order.
   */
  String replicas() {
    return replicas;
  }

  /** How many replicas are on the list this dispatcher holds. */
  int replicaCount() {
    String list = replicas;
    String addresses = list.substring(list.indexOf('[') + 1, list.length() - 1);
    return addresses.isBlank() ? 0 : addresses.split(",").length;
  }

  /**
   * The highest generation with which this dispatcher's consumer joined a group, by what the Kafka
   * client logged; -1 when it never did. Only when the dispatcher is a process: a dispatcher
   * assembled in this JVM logs to the suite's own output.
   */
  int generation(String group) {
    int highest = -1;
    for (String line : log) {
      Matcher matcher = JOINED.matcher(line);
      if (matcher.find() && matcher.group(1).equals(group)) {
        highest = Math.max(highest, Integer.parseInt(matcher.group(2)));
      }
    }
    return highest;
  }

  /**
   * The pod is gone: the process is killed where it stands, without a commit, a word to its
   * sidecars, or leaving a group. A dispatcher assembled in this JVM cannot be killed, and is
   * stopped in good order instead.
   */
  void kill() throws Exception {
    if (process != null) {
      process.destroyForcibly();
      process.waitFor(60, TimeUnit.SECONDS);
      process = null;
    } else {
      stopDispatcher();
    }
  }

  /** Stops the dispatcher: in process the way its shutdown hook does, otherwise with SIGTERM. */
  void stopDispatcher() throws Exception {
    if (process != null) {
      process.destroy();
      if (!process.waitFor(60, TimeUnit.SECONDS)) {
        process.destroyForcibly();
        fail("the dispatcher did not stop within 60s of SIGTERM");
      }
      process = null;
    } else if (delivery != null) {
      delivery.stopAccepting();
      service.shutdown(service.shutdownTimeout());
      membership.close();
      delivery.close();
      tls.close();
      delivery = null;
    }
  }

  void stop() throws Exception {
    for (ManagedChannel channel : channels) {
      channel.shutdownNow();
    }
    stopDispatcher();
    for (ManagedChannel channel : channels) {
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
    try (var files = Files.walk(dir)) {
      for (Path file : files.sorted(Comparator.reverseOrder()).toList()) {
        Files.deleteIfExists(file);
      }
    }
  }
}
