package io.github.bpalermo.odradek.server.tls;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.bpalermo.odradek.config.v1.Cluster;
import io.github.bpalermo.odradek.config.v1.ServerConfig;
import io.github.bpalermo.odradek.config.v1.SpiffeSource;
import io.github.bpalermo.odradek.config.v1.Tls;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;
import javax.net.ssl.SSLException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WorkloadApiTest {
  private static final String APP_ID = "spiffe://example.org/app";
  private static final String BROKER_ID = "spiffe://example.org/kafka/broker";
  private static final String PARTNER_BROKER_ID = "spiffe://partner.test/kafka/broker";
  private static final Duration WAIT = Duration.ofSeconds(20);

  @TempDir static Path shared;

  private static String ca;
  private static String partnerCa;
  private static TestPki.Issued app;
  private static TestPki.Issued appNext;
  private static TestPki.Issued broker;
  private static TestPki.Issued partnerBroker;

  /** Signed by the partner's authority, but claiming an identity of example.org. */
  private static TestPki.Issued impostor;

  private Path socket;
  private FakeWorkloadApi agent;
  private WorkloadApiSource source;

  @BeforeAll
  static void issue() throws Exception {
    TestPki pki = new TestPki(shared, "ca");
    TestPki partner = new TestPki(shared, "partner");
    ca = pki.caPem();
    partnerCa = partner.caPem();
    app = pki.issue("app", "uri:" + APP_ID);
    appNext = pki.issue("app-next", "uri:" + APP_ID);
    broker = pki.issue("broker", "uri:" + BROKER_ID);
    partnerBroker = partner.issue("partner-broker", "uri:" + PARTNER_BROKER_ID);
    impostor = partner.issue("impostor", "uri:" + BROKER_ID);
  }

  @BeforeEach
  void startAgent() throws Exception {
    // Unix socket paths are short; the test's temporary directory may not be.
    socket = Files.createTempDirectory(Path.of("/tmp"), "wl").resolve("agent.sock");
    agent = FakeWorkloadApi.start(socket);
  }

  @AfterEach
  void stop() throws Exception {
    if (source != null) {
      source.close();
    }
    agent.close();
    Files.deleteIfExists(socket);
    Files.deleteIfExists(socket.getParent());
  }

  private WorkloadApiSource connect() throws Exception {
    source = new WorkloadApiSource(socket.toString(), Clock.systemUTC(), WAIT);
    return source;
  }

  @Test
  void firstSvidIsLoaded() throws Exception {
    agent.publish(APP_ID, app.certPem(), app.keyPem(), ca, Map.of());
    Credentials credentials = connect().current();
    assertThat(subject(credentials)).isEqualTo("CN=app");
    assertThat(credentials.trust()).hasSize(1);
    assertThat(credentials.bundles()).containsOnlyKeys("example.org");
  }

  @Test
  void waitsForTheFirstSvid() throws Exception {
    Thread publisher =
        new Thread(
            () -> {
              try {
                Thread.sleep(500);
                agent.publish(APP_ID, app.certPem(), app.keyPem(), ca, Map.of());
              } catch (Exception e) {
                throw new IllegalStateException(e);
              }
            });
    publisher.start();
    assertThat(subject(connect().current())).isEqualTo("CN=app");
    publisher.join();
  }

  @Test
  void noAgentIsReportedWithTheReason() {
    Path nowhere = socket.resolveSibling("nobody.sock");
    assertThatThrownBy(
            () ->
                new WorkloadApiSource(nowhere.toString(), Clock.systemUTC(), Duration.ofSeconds(2)))
        .isInstanceOf(CredentialException.class)
        .hasMessageContaining("nobody.sock")
        .hasMessageContaining("UNAVAILABLE");
  }

  @Test
  void rotatedSvidReplacesTheCurrentOne() throws Exception {
    agent.publish(APP_ID, app.certPem(), app.keyPem(), ca, Map.of());
    WorkloadApiSource watch = connect();
    agent.publish(APP_ID, appNext.certPem(), appNext.keyPem(), ca, Map.of());
    await(() -> subject(watch.current()).equals("CN=app-next"));
    assertThat(watch.reloads()).isEqualTo(2);
  }

  @Test
  void unusableUpdateKeepsTheCurrentSvid() throws Exception {
    agent.publish(APP_ID, app.certPem(), app.keyPem(), ca, Map.of());
    WorkloadApiSource watch = connect();
    // A certificate with the key of another.
    agent.publish(APP_ID, appNext.certPem(), app.keyPem(), ca, Map.of());
    await(() -> watch.failures() == 1);
    assertThat(subject(watch.current())).isEqualTo("CN=app");

    agent.publishRaw(new byte[0]);
    await(() -> watch.failures() == 2);
    assertThat(subject(watch.current())).isEqualTo("CN=app");
  }

  @Test
  void endedStreamIsOpenedAgainAndTheSvidKept() throws Exception {
    agent.publish(APP_ID, app.certPem(), app.keyPem(), ca, Map.of());
    WorkloadApiSource watch = connect();
    agent.dropStreams();
    await(() -> agent.streamsOpened() == 2);
    assertThat(subject(watch.current())).isEqualTo("CN=app");

    agent.publish(APP_ID, appNext.certPem(), appNext.keyPem(), ca, Map.of());
    await(() -> subject(watch.current()).equals("CN=app-next"));
  }

  @Test
  void brokerIsVerifiedAgainstTheBundleOfItsOwnTrustDomain() throws Exception {
    agent.publish(APP_ID, app.certPem(), app.keyPem(), ca, Map.of("partner.test", partnerCa));
    WorkloadApiSource watch = connect();
    assertThat(watch.current().bundles()).containsOnlyKeys("example.org", "partner.test");
    ReloadingSslEngineFactory factory =
        factory(watch, new PeerPolicy(Set.of(PARTNER_BROKER_ID), "example.org"));

    assertThat(TlsTest.handshake(factory, "10.0.0.1", broker, ca)).isEqualTo("CN=app");
    assertThat(TlsTest.handshake(factory, "10.0.0.1", partnerBroker, ca)).isEqualTo("CN=app");
    // The partner's authority is trusted, but only for the partner's own identities.
    assertThatThrownBy(() -> TlsTest.handshake(factory, "10.0.0.1", impostor, ca))
        .isInstanceOf(SSLException.class);
  }

  @Test
  void clustersOnOneSocketShareOneWatch() throws Exception {
    agent.publish(APP_ID, app.certPem(), app.keyPem(), ca, Map.of());
    Cluster cluster =
        Cluster.newBuilder()
            .setTls(
                Tls.newBuilder()
                    .setSpiffe(SpiffeSource.getDefaultInstance())
                    .setAcceptedTrustDomain("example.org"))
            .build();
    ServerConfig config =
        ServerConfig.newBuilder().putClusters("a", cluster).putClusters("b", cluster).build();
    try (TlsSources tls = TlsSources.create(config, "unix://" + socket, WAIT)) {
      Map<String, Object> a = new HashMap<>();
      Map<String, Object> b = new HashMap<>();
      tls.apply("a", a);
      tls.apply("b", b);
      TlsBinding first = (TlsBinding) a.get(ReloadingSslEngineFactory.BINDING_CONFIG);
      TlsBinding second = (TlsBinding) b.get(ReloadingSslEngineFactory.BINDING_CONFIG);
      assertThat(first.source()).isSameAs(second.source());
      assertThat(agent.streamsOpened()).isEqualTo(1);
    }
  }

  @Test
  void missingSocketSettingIsRefused() {
    ServerConfig config =
        ServerConfig.newBuilder()
            .putClusters(
                "a",
                Cluster.newBuilder()
                    .setTls(Tls.newBuilder().setSpiffe(SpiffeSource.getDefaultInstance()))
                    .build())
            .build();
    assertThatThrownBy(() -> TlsSources.create(config, null, WAIT))
        .isInstanceOf(CredentialException.class)
        .hasMessageContaining("clusters.a.tls.spiffe")
        .hasMessageContaining("SPIFFE_ENDPOINT_SOCKET");
  }

  private static ReloadingSslEngineFactory factory(CredentialSource source, PeerPolicy policy) {
    ReloadingSslEngineFactory factory = new ReloadingSslEngineFactory();
    factory.configure(
        Map.of(ReloadingSslEngineFactory.BINDING_CONFIG, new TlsBinding(source, policy)));
    return factory;
  }

  private static String subject(Credentials credentials) {
    X509Certificate leaf = credentials.chain().get(0);
    return leaf.getSubjectX500Principal().getName();
  }

  private static void await(BooleanSupplier condition) throws InterruptedException {
    long deadline = System.nanoTime() + WAIT.toNanos();
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("condition not met within " + WAIT);
      }
      Thread.sleep(20);
    }
  }
}
