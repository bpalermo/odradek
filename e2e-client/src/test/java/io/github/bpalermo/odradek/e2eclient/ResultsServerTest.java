package io.github.bpalermo.odradek.e2eclient;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class ResultsServerTest {
  @Test
  void servesResultsAndHealth() throws Exception {
    AtomicLong calls = new AtomicLong();
    try (ResultsServer server =
            ResultsServer.start(0, () -> Map.of("calls", calls.incrementAndGet()));
        HttpClient http = HttpClient.newHttpClient()) {
      String base = "http://127.0.0.1:" + server.port();

      HttpResponse<String> results = get(http, base + "/results");
      assertThat(results.statusCode()).isEqualTo(200);
      assertThat(results.headers().firstValue("Content-Type"))
          .hasValue("application/json; charset=utf-8");
      assertThat(Json.parse(results.body())).isEqualTo(Map.of("calls", 1L));
      assertThat(Json.parse(get(http, base + "/results").body())).isEqualTo(Map.of("calls", 2L));

      HttpResponse<String> health = get(http, base + "/healthz");
      assertThat(health.statusCode()).isEqualTo(200);
      assertThat(health.body()).isEqualTo("ok\n");

      assertThat(get(http, base + "/results/x").statusCode()).isEqualTo(404);
      assertThat(get(http, base + "/").statusCode()).isEqualTo(404);
      HttpResponse<String> post =
          http.send(
              HttpRequest.newBuilder(URI.create(base + "/results"))
                  .POST(HttpRequest.BodyPublishers.noBody())
                  .build(),
              HttpResponse.BodyHandlers.ofString());
      assertThat(post.statusCode()).isEqualTo(405);
    }
  }

  private static HttpResponse<String> get(HttpClient http, String url) throws Exception {
    return http.send(
        HttpRequest.newBuilder(URI.create(url)).GET().build(),
        HttpResponse.BodyHandlers.ofString());
  }
}
