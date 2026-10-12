package io.github.bpalermo.odradek.server.admin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.bpalermo.odradek.config.v1.Cluster;
import io.github.bpalermo.odradek.config.v1.FileSource;
import io.github.bpalermo.odradek.config.v1.ServerConfig;
import io.github.bpalermo.odradek.config.v1.Tls;
import io.github.bpalermo.odradek.server.tls.TestPki;
import io.github.bpalermo.odradek.server.tls.TlsSources;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AdminServerTest {
  @TempDir Path dir;

  private final Readiness readiness = new Readiness();
  private final HttpClient http = HttpClient.newHttpClient();
  private AdminServer admin;

  @AfterEach
  void stop() {
    if (admin != null) {
      admin.close();
    }
    http.close();
  }

  private HttpResponse<String> get(String path) throws Exception {
    return http.send(
        HttpRequest.newBuilder(URI.create("http://localhost:" + admin.port() + path)).build(),
        HttpResponse.BodyHandlers.ofString());
  }

  @Test
  void notReadyWhileStartingThenReadyThenNotWhileStopping() throws Exception {
    admin = AdminServer.start(0, readiness, Clock.systemUTC());
    assertThat(get("/healthz").statusCode()).isEqualTo(200);
    assertThat(get("/readyz").statusCode()).isEqualTo(503);

    readiness.starting("loading TLS credentials");
    HttpResponse<String> waiting = get("/readyz");
    assertThat(waiting.statusCode()).isEqualTo(503);
    assertThat(waiting.body()).isEqualTo("starting: loading TLS credentials\n");
    assertThat(get("/healthz").statusCode()).isEqualTo(200);

    readiness.started(TlsSources.none());
    HttpResponse<String> ready = get("/readyz");
    assertThat(ready.statusCode()).isEqualTo(200);
    assertThat(ready.body()).isEqualTo("ready\n");

    readiness.stopping();
    assertThat(get("/readyz").statusCode()).isEqualTo(503);
    assertThat(get("/healthz").statusCode()).isEqualTo(200);
  }

  @Test
  void notReadyOnceAClientCertificateHasExpired() throws Exception {
    TestPki pki = new TestPki(dir, "ca");
    TestPki.write(
        dir.resolve("tls"), pki.issue("client", "uri:spiffe://example.org/app"), pki.caPem());
    ServerConfig config =
        ServerConfig.newBuilder()
            .putClusters(
                "orders",
                Cluster.newBuilder()
                    .setTls(
                        Tls.newBuilder()
                            .setFiles(
                                FileSource.newBuilder().setDir(dir.resolve("tls").toString())))
                    .build())
            .build();
    try (TlsSources tls = TlsSources.create(config)) {
      readiness.started(tls);
      admin = AdminServer.start(0, readiness, Clock.systemUTC());
      assertThat(get("/readyz").statusCode()).isEqualTo(200);
      admin.close();

      // The certificate lasts a day; nothing renewed it.
      admin = AdminServer.start(0, readiness, Clock.offset(Clock.systemUTC(), Duration.ofDays(3)));
      HttpResponse<String> expired = get("/readyz");
      assertThat(expired.statusCode()).isEqualTo(503);
      assertThat(expired.body()).startsWith("clusters.orders.tls: the client certificate expired");
    }
  }

  @Test
  void onlyGetIsAnswered() throws Exception {
    admin = AdminServer.start(0, readiness, Clock.systemUTC());
    HttpResponse<String> response =
        http.send(
            HttpRequest.newBuilder(URI.create("http://localhost:" + admin.port() + "/readyz"))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build(),
            HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).isEqualTo(405);
  }
}
