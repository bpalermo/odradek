package io.github.bpalermo.odradek.server.tls;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.bpalermo.odradek.config.v1.Cluster;
import io.github.bpalermo.odradek.config.v1.Dispatch;
import io.github.bpalermo.odradek.config.v1.Dispatcher;
import io.github.bpalermo.odradek.config.v1.FileSource;
import io.github.bpalermo.odradek.config.v1.ListenerTls;
import io.github.bpalermo.odradek.config.v1.ProduceTarget;
import io.github.bpalermo.odradek.config.v1.Producer;
import io.github.bpalermo.odradek.config.v1.ServerConfig;
import io.github.bpalermo.odradek.config.v1.Tls;
import io.github.bpalermo.odradek.server.produce.ClusterProducers;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SslEngineFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TlsTest {
  private static final String BROKER_ID = "spiffe://example.org/kafka/broker";

  @TempDir static Path shared;

  private static TestPki pki;
  private static String ca;
  private static TestPki.Issued broker;
  private static TestPki.Issued client;
  private static TestPki.Issued clientNext;

  @TempDir Path dir;

  @BeforeAll
  static void issue() throws Exception {
    pki = new TestPki(shared, "ca");
    ca = pki.caPem();
    broker = pki.issue("broker", "uri:" + BROKER_ID + ",dns:localhost");
    client = pki.issue("client", "uri:spiffe://example.org/app");
    clientNext = pki.issue("client-next", "uri:spiffe://example.org/app");
  }

  private FileCredentialSource source() throws Exception {
    return new FileCredentialSource(
        "a",
        dir.resolve("tls.crt"),
        dir.resolve("tls.key"),
        dir.resolve("ca.crt"),
        Clock.systemUTC());
  }

  // --- The files source ---

  @Test
  void loadsTheFiles() throws Exception {
    TestPki.write(dir, client, ca);
    Credentials credentials = source().current();
    assertThat(subject(credentials.chain().get(0))).isEqualTo("CN=client");
    assertThat(credentials.trust()).hasSize(1);
  }

  @Test
  void missingFileIsRefusedAtStart() throws Exception {
    TestPki.write(dir, client, ca);
    Files.delete(dir.resolve("tls.key"));
    assertThatThrownBy(this::source)
        .isInstanceOf(CredentialException.class)
        .hasMessageContaining("tls.key");
  }

  @Test
  void keyOfAnotherCertificateIsRefusedAtStart() throws Exception {
    TestPki.write(dir, new TestPki.Issued(client.certPem(), clientNext.keyPem()), ca);
    assertThatThrownBy(this::source)
        .isInstanceOf(CredentialException.class)
        .hasMessageContaining("does not belong");
  }

  @Test
  void expiredCertificateIsRefused() throws Exception {
    TestPki.write(dir, client, ca);
    Clock later = Clock.offset(Clock.systemUTC(), Duration.ofDays(3));
    assertThatThrownBy(
            () ->
                new FileCredentialSource(
                    "a",
                    dir.resolve("tls.crt"),
                    dir.resolve("tls.key"),
                    dir.resolve("ca.crt"),
                    later))
        .isInstanceOf(CredentialException.class)
        .hasMessageContaining("not valid now");
  }

  @Test
  void changedFilesReplaceTheCredentials() throws Exception {
    TestPki.write(dir, client, ca);
    FileCredentialSource source = source();
    source.poll();
    assertThat(source.reloads()).isEqualTo(1);

    TestPki.write(dir, clientNext, ca);
    source.poll();
    assertThat(subject(source.current().chain().get(0))).isEqualTo("CN=client-next");
    assertThat(source.reloads()).isEqualTo(2);
    assertThat(source.failures()).isZero();
  }

  @Test
  void halfWrittenRotationKeepsTheOldCredentialsUntilItIsWhole() throws Exception {
    TestPki.write(dir, client, ca);
    FileCredentialSource source = source();

    // The certificate arrives before its key.
    Files.writeString(dir.resolve("tls.crt"), clientNext.certPem());
    source.poll();
    source.poll();
    assertThat(subject(source.current().chain().get(0))).isEqualTo("CN=client");
    assertThat(source.failures()).isEqualTo(1);

    Files.writeString(dir.resolve("tls.key"), clientNext.keyPem());
    source.poll();
    assertThat(subject(source.current().chain().get(0))).isEqualTo("CN=client-next");
    assertThat(source.reloads()).isEqualTo(2);
  }

  @Test
  void unreadableFilesKeepTheOldCredentials() throws Exception {
    TestPki.write(dir, client, ca);
    FileCredentialSource source = source();
    Files.delete(dir.resolve("ca.crt"));
    source.poll();
    source.poll();
    assertThat(subject(source.current().chain().get(0))).isEqualTo("CN=client");
    assertThat(source.failures()).isEqualTo(1);

    Files.writeString(dir.resolve("ca.crt"), ca);
    source.poll();
    assertThat(source.failures()).isEqualTo(1);
  }

  // --- Handshakes ---

  @Test
  void handshakePresentsTheClientCertificateAndAcceptsTheBrokerId() throws Exception {
    TestPki.write(dir, client, ca);
    ReloadingSslEngineFactory factory = factory(source(), new PeerPolicy(Set.of(BROKER_ID), ""));
    assertThat(handshake(factory, "10.0.0.1")).isEqualTo("CN=client");
  }

  @Test
  void rotatedCertificateIsUsedByTheNextHandshake() throws Exception {
    TestPki.write(dir, client, ca);
    FileCredentialSource source = source();
    ReloadingSslEngineFactory factory = factory(source, new PeerPolicy(Set.of(BROKER_ID), ""));
    assertThat(handshake(factory, "10.0.0.1")).isEqualTo("CN=client");

    TestPki.write(dir, clientNext, ca);
    source.poll();
    assertThat(handshake(factory, "10.0.0.1")).isEqualTo("CN=client-next");
  }

  @Test
  void brokerInAcceptedTrustDomainIsAccepted() throws Exception {
    TestPki.write(dir, client, ca);
    ReloadingSslEngineFactory factory = factory(source(), new PeerPolicy(Set.of(), "Example.org"));
    assertThat(handshake(factory, "10.0.0.1")).isEqualTo("CN=client");
  }

  @Test
  void brokerWithAnotherSpiffeIdIsRejected() throws Exception {
    TestPki.write(dir, client, ca);
    ReloadingSslEngineFactory factory =
        factory(source(), new PeerPolicy(Set.of("spiffe://example.org/kafka/other"), "other.org"));
    assertThatThrownBy(() -> handshake(factory, "10.0.0.1"))
        .isInstanceOf(SSLException.class)
        .hasStackTraceContaining("is not accepted");
  }

  @Test
  void brokerOfAnotherAuthorityIsRejected() throws Exception {
    TestPki other = new TestPki(dir, "other");
    TestPki.write(dir, client, other.caPem());
    ReloadingSslEngineFactory factory = factory(source(), new PeerPolicy(Set.of(BROKER_ID), ""));
    assertThatThrownBy(() -> handshake(factory, "10.0.0.1")).isInstanceOf(SSLException.class);
  }

  @Test
  void withoutSpiffePolicyTheHostNameIsChecked() throws Exception {
    TestPki.write(dir, client, ca);
    ReloadingSslEngineFactory factory = factory(source(), new PeerPolicy(Set.of(), ""));
    assertThat(handshake(factory, "localhost")).isEqualTo("CN=client");
    assertThatThrownBy(() -> handshake(factory, "broker.example.com"))
        .isInstanceOf(SSLException.class);
  }

  // --- Kafka wiring ---

  @Test
  void kafkaProducerIsBuiltWithTheFactory() throws Exception {
    TestPki.write(dir, client, ca);
    ServerConfig config =
        ServerConfig.newBuilder()
            .putClusters(
                "a",
                Cluster.newBuilder()
                    .putProperties("bootstrap.servers", "localhost:1")
                    .setTls(
                        Tls.newBuilder()
                            .setFiles(FileSource.newBuilder().setDir(dir.toString()))
                            .addAcceptedSpiffeIds(BROKER_ID))
                    .build())
            .putProducers(
                "t",
                Producer.newBuilder()
                    .addClusters(ProduceTarget.newBuilder().setCluster("a"))
                    .build())
            .build();
    try (TlsSources tls = TlsSources.create(config)) {
      Map<String, Object> seen = new HashMap<>();
      // Kafka's own producer: creating it configures the factory, which fails without the binding.
      try (ClusterProducers producers = ClusterProducers.create(config, tls)) {
        assertThat(producers).isNotNull();
      }
      try (ClusterProducers producers =
          ClusterProducers.create(
              config,
              tls,
              (cluster, properties) -> {
                seen.putAll(properties);
                return null;
              })) {
        assertThat(producers).isNotNull();
      } catch (NullPointerException e) {
        // Closing the null producer the capturing factory returned.
      }
      assertThat(seen)
          .containsEntry("security.protocol", "SSL")
          .containsEntry(
              SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, ReloadingSslEngineFactory.class.getName())
          .containsKey(ReloadingSslEngineFactory.BINDING_CONFIG);
    }
  }

  @Test
  void factoryWithoutBindingIsRefused() {
    try (ReloadingSslEngineFactory factory = new ReloadingSslEngineFactory()) {
      assertThatThrownBy(() -> factory.configure(Map.of()))
          .isInstanceOf(KafkaException.class)
          .hasMessageContaining(ReloadingSslEngineFactory.BINDING_CONFIG);
    }
  }

  @Test
  void plaintextProtocolWithTlsIsRefused() throws Exception {
    TestPki.write(dir, client, ca);
    ServerConfig config =
        ServerConfig.newBuilder()
            .putClusters(
                "a",
                Cluster.newBuilder()
                    .putProperties("security.protocol", "SASL_PLAINTEXT")
                    .setTls(
                        Tls.newBuilder().setFiles(FileSource.newBuilder().setDir(dir.toString())))
                    .build())
            .build();
    assertThatThrownBy(() -> TlsSources.create(config))
        .isInstanceOf(CredentialException.class)
        .hasMessageContaining("SASL_PLAINTEXT");
  }

  // --- The delivery port's credentials ---

  private static ServerConfig delivery(Path listenerDir, Path dispatcherDir) {
    ServerConfig.Builder config = ServerConfig.newBuilder();
    if (listenerDir != null) {
      config.setDispatch(
          Dispatch.newBuilder()
              .setTls(
                  ListenerTls.newBuilder()
                      .setFiles(FileSource.newBuilder().setDir(listenerDir.toString()))));
    }
    if (dispatcherDir != null) {
      config.setDispatcher(
          Dispatcher.newBuilder()
              .setAddress("dispatcher:9465")
              .setTls(
                  Tls.newBuilder()
                      .setFiles(FileSource.newBuilder().setDir(dispatcherDir.toString()))
                      .addAcceptedSpiffeIds(BROKER_ID)));
    }
    return config.build();
  }

  @Test
  void deliveryCredentialsAreLoadedForBothRoles() throws Exception {
    TestPki.write(dir.resolve("listener"), broker, ca);
    TestPki.write(dir.resolve("dispatcher"), client, ca);
    try (TlsSources tls =
        TlsSources.create(delivery(dir.resolve("listener"), dir.resolve("dispatcher")))) {
      assertThat(subject(tls.listener().current().chain().get(0))).isEqualTo("CN=broker");
      assertThat(subject(tls.dispatcher().source().current().chain().get(0)))
          .isEqualTo("CN=client");
      assertThat(tls.dispatcher().policy()).isEqualTo(new PeerPolicy(Set.of(BROKER_ID), ""));
      assertThat(tls.deliverySources()).containsOnlyKeys("dispatch.tls", "dispatcher.tls");
      // Not clusters: nothing is given to a Kafka client for them.
      assertThat(tls.sources()).isEmpty();
    }
  }

  @Test
  void withoutDeliveryTlsThereAreNoDeliveryCredentials() throws Exception {
    try (TlsSources tls = TlsSources.create(delivery(null, null))) {
      assertThat(tls.listener()).isNull();
      assertThat(tls.dispatcher()).isNull();
      assertThat(tls.deliverySources()).isEmpty();
    }
    assertThat(TlsSources.none().listener()).isNull();
    assertThat(TlsSources.none().dispatcher()).isNull();
  }

  @Test
  void anExpiredDeliveryCertificateIsAProblemInEitherRole() throws Exception {
    TestPki.write(dir.resolve("listener"), broker, ca);
    TestPki.write(dir.resolve("dispatcher"), client, ca);
    try (TlsSources listener = TlsSources.create(delivery(dir.resolve("listener"), null));
        TlsSources sidecar = TlsSources.create(delivery(null, dir.resolve("dispatcher")))) {
      Instant now = Instant.now();
      assertThat(listener.problems(now)).isEmpty();
      assertThat(sidecar.problems(now)).isEmpty();
      // The test authority issues for one day.
      Instant later = now.plus(Duration.ofDays(3));
      assertThat(listener.problems(later))
          .singleElement()
          .asString()
          .startsWith("dispatch.tls: the certificate expired at ");
      assertThat(sidecar.problems(later))
          .singleElement()
          .asString()
          .startsWith("dispatcher.tls: the certificate expired at ");
    }
  }

  @Test
  void missingDeliveryCredentialsNameTheirSection() {
    assertThatThrownBy(() -> TlsSources.create(delivery(dir.resolve("nowhere"), null)))
        .isInstanceOf(CredentialException.class)
        .hasMessageStartingWith("dispatch.tls: cannot read ");
    assertThatThrownBy(() -> TlsSources.create(delivery(null, dir.resolve("nowhere"))))
        .isInstanceOf(CredentialException.class)
        .hasMessageStartingWith("dispatcher.tls: cannot read ");
  }

  // --- Helpers ---

  private static ReloadingSslEngineFactory factory(CredentialSource source, PeerPolicy policy) {
    ReloadingSslEngineFactory factory = new ReloadingSslEngineFactory();
    factory.configure(
        Map.of(ReloadingSslEngineFactory.BINDING_CONFIG, new TlsBinding(source, policy)));
    return factory;
  }

  private static String subject(X509Certificate certificate) {
    return certificate.getSubjectX500Principal().getName();
  }

  /**
   * Completes a handshake between an engine of the factory and a broker that demands a client
   * certificate, in memory. Returns the subject of the client certificate the broker saw.
   */
  private static String handshake(ReloadingSslEngineFactory factory, String host) throws Exception {
    return handshake(factory, host, broker, ca);
  }

  /** As above, with the broker's certificate and the authorities it accepts clients of. */
  static String handshake(
      SslEngineFactory factory, String host, TestPki.Issued broker, String clientCa)
      throws Exception {
    SSLEngine clientEngine = factory.createClientSslEngine(host, 9093, "https");
    SSLEngine brokerEngine = brokerContext(broker, clientCa).createSSLEngine();
    brokerEngine.setUseClientMode(false);
    brokerEngine.setNeedClientAuth(true);
    handshake(clientEngine, brokerEngine);
    X509Certificate seen = (X509Certificate) brokerEngine.getSession().getPeerCertificates()[0];
    return subject(seen);
  }

  /**
   * Completes a handshake between two engines, in memory.
   *
   * @throws SSLException if either end refuses the other
   */
  static void handshake(SSLEngine clientEngine, SSLEngine serverEngine) throws SSLException {
    int size = clientEngine.getSession().getPacketBufferSize();
    ByteBuffer clientToServer = ByteBuffer.allocate(size * 4);
    ByteBuffer serverToClient = ByteBuffer.allocate(size * 4);
    ByteBuffer empty = ByteBuffer.allocate(0);
    ByteBuffer sink = ByteBuffer.allocate(clientEngine.getSession().getApplicationBufferSize());

    clientEngine.beginHandshake();
    serverEngine.beginHandshake();
    for (int round = 0; round < 100; round++) {
      boolean progress = false;
      progress |= step(clientEngine, serverToClient, clientToServer, empty, sink);
      progress |= step(serverEngine, clientToServer, serverToClient, empty, sink);
      if (done(clientEngine) && done(serverEngine) && !progress) {
        return;
      }
    }
    throw new AssertionError("the handshake did not finish");
  }

  private static boolean done(SSLEngine engine) {
    HandshakeStatus status = engine.getHandshakeStatus();
    return status == HandshakeStatus.NOT_HANDSHAKING || status == HandshakeStatus.FINISHED;
  }

  /** One wrap or unwrap of an engine; returns whether it produced or consumed anything. */
  private static boolean step(
      SSLEngine engine, ByteBuffer in, ByteBuffer out, ByteBuffer empty, ByteBuffer sink)
      throws SSLException {
    switch (engine.getHandshakeStatus()) {
      case NEED_WRAP -> {
        SSLEngineResult result = engine.wrap(empty, out);
        return result.bytesProduced() > 0;
      }
      case NEED_UNWRAP, NEED_UNWRAP_AGAIN -> {
        in.flip();
        try {
          sink.clear();
          SSLEngineResult result = engine.unwrap(in, sink);
          return result.bytesConsumed() > 0;
        } finally {
          in.compact();
        }
      }
      case NEED_TASK -> {
        Runnable task;
        while ((task = engine.getDelegatedTask()) != null) {
          task.run();
        }
        return true;
      }
      default -> {
        // TLS 1.3 sends session tickets after the handshake: drain what the peer left.
        if (in.position() > 0) {
          in.flip();
          try {
            sink.clear();
            return engine.unwrap(in, sink).bytesConsumed() > 0;
          } finally {
            in.compact();
          }
        }
        return false;
      }
    }
  }

  private static SSLContext brokerContext(TestPki.Issued broker, String ca) throws Exception {
    char[] password = "test".toCharArray();
    KeyStore keys = KeyStore.getInstance("PKCS12");
    keys.load(null, null);
    List<X509Certificate> chain =
        Pem.certificates(broker.certPem().getBytes(StandardCharsets.US_ASCII), "broker");
    keys.setKeyEntry(
        "broker",
        Pem.privateKey(broker.keyPem().getBytes(StandardCharsets.US_ASCII), "broker"),
        password,
        chain.toArray(X509Certificate[]::new));
    KeyManagerFactory keyManagers =
        KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    keyManagers.init(keys, password);
    KeyStore trust = KeyStore.getInstance("PKCS12");
    trust.load(null, null);
    trust.setCertificateEntry(
        "ca", Pem.certificates(ca.getBytes(StandardCharsets.US_ASCII), "ca").get(0));
    TrustManagerFactory trustManagers =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trustManagers.init(trust);
    SSLContext context = SSLContext.getInstance("TLS");
    context.init(keyManagers.getKeyManagers(), trustManagers.getTrustManagers(), null);
    return context;
  }
}
