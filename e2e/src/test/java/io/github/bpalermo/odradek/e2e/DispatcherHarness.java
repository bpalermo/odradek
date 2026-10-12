package io.github.bpalermo.odradek.e2e;

import static org.assertj.core.api.Assertions.fail;

import io.github.bpalermo.odradek.config.v1.ServerConfig;
import io.github.bpalermo.odradek.server.config.ConfigLoader;
import io.github.bpalermo.odradek.server.dispatch.DeliveryServer;
import io.github.bpalermo.odradek.server.dispatch.DispatchService;
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

  final Path dir;
  private final String yaml;
  private final ServerConfig config;
  private final List<ManagedChannel> channels = new CopyOnWriteArrayList<>();

  // In-process dispatcher.
  private TlsSources tls;
  private DispatchService service;
  private DeliveryServer delivery;

  // External dispatcher.
  private Process process;
  private volatile CompletableFuture<Integer> deliveryPort;
  private volatile CompletableFuture<Integer> healthPort;

  private int port;

  /** What the server logged, when it is a process: for assertions about its warnings. */
  final List<String> log = new CopyOnWriteArrayList<>();

  private DispatcherHarness(String yamlTemplate, Object... arguments) throws Exception {
    dir = Files.createTempDirectory(Path.of("/tmp"), "odradek-e2e-dispatch");
    yaml =
        "admin: {port: 0}\nlistener: {socketPath: %s}\n".formatted(socket())
            + yamlTemplate.formatted(arguments);
    config = ConfigLoader.parse(yaml);
    startDispatcher();
  }

  /** Starts a dispatcher with this configuration, a YAML template for {@code arguments}. */
  static DispatcherHarness start(String yamlTemplate, Object... arguments) throws Exception {
    return new DispatcherHarness(yamlTemplate, arguments);
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
      delivery = DeliveryServer.start(config.getDispatch(), service);
      port = delivery.port();
      return;
    }
    Path file = dir.resolve("odradek.yaml");
    Files.writeString(file, yaml);
    CompletableFuture<Integer> foundDelivery = new CompletableFuture<>();
    CompletableFuture<Integer> foundHealth = new CompletableFuture<>();
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
                }
              } catch (IOException e) {
                // The process is gone.
              }
              foundDelivery.completeExceptionally(
                  new IllegalStateException("the server exited before it listened"));
              foundHealth.completeExceptionally(
                  new IllegalStateException("the server exited before it listened"));
            },
            "dispatcher-output");
    pump.setDaemon(true);
    pump.start();
    try {
      port = foundDelivery.get(START_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
    } catch (Exception e) {
      started.destroyForcibly();
      fail("%s did not serve the delivery port within %s: %s", executable, START_TIMEOUT, e);
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
