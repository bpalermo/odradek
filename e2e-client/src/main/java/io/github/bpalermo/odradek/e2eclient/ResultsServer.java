package io.github.bpalermo.odradek.e2eclient;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

/**
 * {@code GET /results} (the run's results as JSON) and {@code GET /healthz}, on the JDK's server.
 */
public final class ResultsServer implements AutoCloseable {
  private final HttpServer server;

  private ResultsServer(HttpServer server) {
    this.server = server;
  }

  /** Listens on every interface at {@code port}; 0 picks a free one. */
  public static ResultsServer start(int port, Supplier<Map<String, Object>> results)
      throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
    server.createContext(
        "/results", exchange -> answer(exchange, "application/json", Json.write(results.get())));
    server.createContext("/healthz", exchange -> answer(exchange, "text/plain", "ok\n"));
    server.setExecutor(
        Executors.newSingleThreadExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "e2e-client-http");
              thread.setDaemon(true);
              return thread;
            }));
    server.start();
    return new ResultsServer(server);
  }

  public int port() {
    return server.getAddress().getPort();
  }

  private static void answer(HttpExchange exchange, String type, String body) throws IOException {
    try (exchange) {
      if (!"GET".equals(exchange.getRequestMethod())
          && !"HEAD".equals(exchange.getRequestMethod())) {
        exchange.getResponseHeaders().set("Allow", "GET, HEAD");
        exchange.sendResponseHeaders(405, -1);
        return;
      }
      // Only the exact paths: a context also matches every path below it.
      String path = exchange.getRequestURI().getPath();
      if (!path.equals(exchange.getHttpContext().getPath())) {
        exchange.sendResponseHeaders(404, -1);
        return;
      }
      byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().set("Content-Type", type + "; charset=utf-8");
      if ("HEAD".equals(exchange.getRequestMethod())) {
        exchange.sendResponseHeaders(200, -1);
        return;
      }
      exchange.sendResponseHeaders(200, bytes.length);
      try (OutputStream out = exchange.getResponseBody()) {
        out.write(bytes);
      }
    }
  }

  @Override
  public void close() {
    server.stop(0);
  }
}
