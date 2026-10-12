package io.github.bpalermo.odradek.server.admin;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The HTTP endpoint kubelet probes: gRPC probes cannot target a Unix socket.
 *
 * <ul>
 *   <li>{@code GET /healthz}: 200 while the process runs. For the liveness probe.
 *   <li>{@code GET /readyz}: 200 when {@link Readiness} has no problem, otherwise 503 with one
 *       problem per line. For the startup and readiness probes.
 * </ul>
 */
public final class AdminServer implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(AdminServer.class);

  /** The port when the configuration does not name one. */
  public static final int DEFAULT_PORT = 9464;

  private final HttpServer server;
  private final ExecutorService executor;

  private AdminServer(HttpServer server, ExecutorService executor) {
    this.server = server;
    this.executor = executor;
  }

  /**
   * Starts serving on {@code port}; 0 picks a free one.
   *
   * @throws IOException if the port cannot be bound
   */
  public static AdminServer start(int port, Readiness readiness, Clock clock) throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
    ExecutorService executor =
        Executors.newSingleThreadExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "admin");
              thread.setDaemon(true);
              return thread;
            });
    server.setExecutor(executor);
    server.createContext("/healthz", exchange -> respond(exchange, 200, "ok\n"));
    server.createContext(
        "/readyz",
        exchange -> {
          List<String> problems = readiness.problems(clock.instant());
          if (problems.isEmpty()) {
            respond(exchange, 200, "ready\n");
          } else {
            respond(exchange, 503, String.join("\n", problems) + "\n");
          }
        });
    server.start();
    AdminServer admin = new AdminServer(server, executor);
    LOG.info("admin endpoint on port {}", admin.port());
    return admin;
  }

  /** The port being served, which is the one asked for unless that was 0. */
  public int port() {
    return server.getAddress().getPort();
  }

  private static void respond(HttpExchange exchange, int status, String body) throws IOException {
    try (exchange) {
      if (!exchange.getRequestMethod().equals("GET")) {
        exchange.sendResponseHeaders(405, -1);
        return;
      }
      byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
      exchange.sendResponseHeaders(status, bytes.length);
      try (OutputStream out = exchange.getResponseBody()) {
        out.write(bytes);
      }
    }
  }

  @Override
  public void close() {
    server.stop(0);
    executor.shutdownNow();
  }
}
