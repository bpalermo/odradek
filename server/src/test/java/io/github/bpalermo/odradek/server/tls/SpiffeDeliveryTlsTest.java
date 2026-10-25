package io.github.bpalermo.odradek.server.tls;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.bpalermo.kafka.spiffe.id.SpiffeId;
import io.github.bpalermo.kafka.spiffe.x509.PeerRejectedException;
import io.github.bpalermo.kafka.spiffe.x509.SpiffeTrustManager;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.SslContext;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The delivery port's TLS with {@code spiffe} sources (ADR 9): contexts of kafka-spiffe's
 * snapshots, peers verified by its trust manager, one handshake at a time and in memory.
 */
class SpiffeDeliveryTlsTest {
  private static final String DISPATCHER_ID = "spiffe://example.org/ns/kafka/sa/dispatcher";
  private static final String SIDECAR_ID = "spiffe://example.org/ns/billing/sa/billing";
  private static final String PARTNER_ID = "spiffe://partner.test/ns/billing/sa/billing";
  private static final String STRANGER_ID = "spiffe://elsewhere.test/ns/billing/sa/billing";
  private static final Duration WAIT = Duration.ofSeconds(20);

  @TempDir static Path shared;

  private static String ca;
  private static String partnerCa;
  private static String elsewhereCa;
  private static TestPki.Issued dispatcher;
  private static TestPki.Issued dispatcherNext;
  private static TestPki.Issued sidecar;
  private static TestPki.Issued partnerSidecar;
  private static TestPki.Issued impostorSidecar;
  private static TestPki.Issued strangerSidecar;
  private static TestPki.Issued plain;

  @TempDir Path dir;

  private final List<FakeWorkloadApi> agents = new CopyOnWriteArrayList<>();
  private final List<Path> sockets = new CopyOnWriteArrayList<>();
  private final List<AutoCloseable> sources = new CopyOnWriteArrayList<>();

  @BeforeAll
  static void issue() throws Exception {
    TestPki pki = new TestPki(shared, "ca");
    TestPki partner = new TestPki(shared, "partner");
    TestPki elsewhere = new TestPki(shared, "elsewhere");
    ca = pki.caPem();
    partnerCa = partner.caPem();
    elsewhereCa = elsewhere.caPem();
    dispatcher = pki.issue("dispatcher", "uri:" + DISPATCHER_ID);
    dispatcherNext = pki.issue("dispatcher-next", "uri:" + DISPATCHER_ID);
    sidecar = pki.issue("sidecar", "uri:" + SIDECAR_ID);
    partnerSidecar = partner.issue("partner-sidecar", "uri:" + PARTNER_ID);
    impostorSidecar = partner.issue("impostor-sidecar", "uri:" + SIDECAR_ID);
    strangerSidecar = elsewhere.issue("stranger-sidecar", "uri:" + STRANGER_ID);
    plain = pki.issue("plain", "dns:plain.example.org");
  }

  @AfterEach
  void stop() throws Exception {
    for (AutoCloseable source : sources) {
      source.close();
    }
    for (FakeWorkloadApi agent : agents) {
      agent.close();
    }
    for (Path socket : sockets) {
      Files.deleteIfExists(socket);
      Files.deleteIfExists(socket.getParent());
    }
  }

  /** What a handshake's verification came to, as the observer heard it. */
  private record Event(boolean peerIsServer, String id, String refusal) {}

  private static final class Recorder implements SpiffeTrustManager.Observer {
    final List<Event> events = new CopyOnWriteArrayList<>();

    @Override
    public void accepted(boolean server, SpiffeId id) {
      events.add(new Event(server, id.toString(), null));
    }

    @Override
    public void rejected(boolean server, PeerRejectedException rejection) {
      events.add(new Event(server, null, rejection.reason().name()));
    }
  }

  /** An agent serving one SVID with example.org's bundle and the partner's, and a handle on it. */
  private SpiffeCredentialSource agent(FakeWorkloadApi[] out, String id, TestPki.Issued issued)
      throws Exception {
    // Unix socket paths are short; the test's temporary directory may not be.
    Path socket = Files.createTempDirectory(Path.of("/tmp"), "dl").resolve("agent.sock");
    sockets.add(socket);
    FakeWorkloadApi agent = FakeWorkloadApi.start(socket);
    agents.add(agent);
    agent.publish(id, issued.certPem(), issued.keyPem(), ca, Map.of("partner.test", partnerCa));
    if (out != null) {
      out[0] = agent;
    }
    SpiffeCredentialSource source = SpiffeCredentialSource.open(socket.toString(), WAIT);
    sources.add(source);
    return source;
  }

  /** A files source of these credentials, in a directory of its own. */
  private FileCredentialSource files(TestPki.Issued issued, String caPem) throws Exception {
    Path where = Files.createTempDirectory(dir, "files");
    TestPki.write(where, issued, caPem);
    return new FileCredentialSource(
        "test",
        where.resolve("tls.crt"),
        where.resolve("tls.key"),
        where.resolve("ca.crt"),
        Clock.systemUTC());
  }

  /** A sidecar's engine, as grpc-netty makes one: for an address, and with the HTTPS check on. */
  private static SSLEngine connecting(SslContext context) {
    SSLEngine engine = context.newEngine(ByteBufAllocator.DEFAULT, "10.0.0.9", 9465);
    SSLParameters parameters = engine.getSSLParameters();
    parameters.setEndpointIdentificationAlgorithm("HTTPS");
    engine.setSSLParameters(parameters);
    return engine;
  }

  /** The subjects each end saw: the sidecar's, then the dispatcher's. */
  private static List<String> handshake(SslContext sidecarEnd, SslContext dispatcherEnd)
      throws Exception {
    SSLEngine client = connecting(sidecarEnd);
    SSLEngine server = dispatcherEnd.newEngine(ByteBufAllocator.DEFAULT);
    TlsTest.handshake(client, server);
    assertThat(client.getSession().getProtocol()).isEqualTo("TLSv1.3");
    assertThat(client.getApplicationProtocol()).isEqualTo("h2");
    return List.of(
        subject(server.getSession().getPeerCertificates()[0]),
        subject(client.getSession().getPeerCertificates()[0]));
  }

  private static String subject(java.security.cert.Certificate certificate) {
    return ((X509Certificate) certificate).getSubjectX500Principal().getName();
  }

  private static TlsBinding accepting(CredentialSource source, String... ids) {
    return new TlsBinding(source, new PeerPolicy(Set.of(ids), ""));
  }

  @Test
  void bothEndsVerifyEachOtherWithTheLibraryAndSayWhatTheyAccepted() throws Exception {
    Recorder atListener = new Recorder();
    Recorder atSidecar = new Recorder();
    SslContext server =
        ReloadingSslContext.forListener(agent(null, DISPATCHER_ID, dispatcher), atListener);
    SslContext client =
        ReloadingSslContext.forDispatcher(
            accepting(agent(null, SIDECAR_ID, sidecar), DISPATCHER_ID), atSidecar);

    // The engine asks for the HTTPS check against 10.0.0.9, which the SVIDs do not name.
    assertThat(handshake(client, server)).containsExactly("CN=sidecar", "CN=dispatcher");
    assertThat(atListener.events).containsExactly(new Event(false, SIDECAR_ID, null));
    assertThat(atSidecar.events).containsExactly(new Event(true, DISPATCHER_ID, null));
  }

  @Test
  void theListenerAcceptsAnySidecarOfATrustDomainItHasABundleOf() throws Exception {
    Recorder atListener = new Recorder();
    SslContext server =
        ReloadingSslContext.forListener(agent(null, DISPATCHER_ID, dispatcher), atListener);
    String all = ca + partnerCa + elsewhereCa;

    assertThat(
            handshake(
                    ReloadingSslContext.forDispatcher(
                        accepting(files(partnerSidecar, all), DISPATCHER_ID)),
                    server)
                .get(0))
        .isEqualTo("CN=partner-sidecar");
    // The partner's authority is trusted, but only for the partner's own identities.
    SslContext impostor =
        ReloadingSslContext.forDispatcher(accepting(files(impostorSidecar, all), DISPATCHER_ID));
    assertThatThrownBy(() -> handshake(impostor, server)).isInstanceOf(SSLException.class);
    // No bundle of elsewhere.test.
    SslContext stranger =
        ReloadingSslContext.forDispatcher(accepting(files(strangerSidecar, all), DISPATCHER_ID));
    assertThatThrownBy(() -> handshake(stranger, server)).isInstanceOf(SSLException.class);
    // With a spiffe source a certificate without a SPIFFE ID ends the handshake.
    SslContext nobody =
        ReloadingSslContext.forDispatcher(accepting(files(plain, all), DISPATCHER_ID));
    assertThatThrownBy(() -> handshake(nobody, server)).isInstanceOf(SSLException.class);

    assertThat(atListener.events)
        .containsExactly(
            new Event(false, PARTNER_ID, null),
            new Event(false, null, "CHAIN"),
            new Event(false, null, "UNKNOWN_TRUST_DOMAIN"),
            new Event(false, null, "MALFORMED_SVID"));
  }

  @Test
  void aSidecarRefusesADispatcherItDoesNotAccept() throws Exception {
    Recorder atSidecar = new Recorder();
    SslContext server = ReloadingSslContext.forListener(agent(null, DISPATCHER_ID, dispatcher));
    SslContext client =
        ReloadingSslContext.forDispatcher(
            accepting(agent(null, SIDECAR_ID, sidecar), "spiffe://example.org/other"), atSidecar);
    assertThatThrownBy(() -> handshake(client, server)).isInstanceOf(SSLException.class);
    assertThat(atSidecar.events).containsExactly(new Event(true, null, "ID_NOT_ALLOWED"));
  }

  @Test
  void aDispatcherInTheAcceptedTrustDomainIsAccepted() throws Exception {
    SslContext server = ReloadingSslContext.forListener(agent(null, DISPATCHER_ID, dispatcher));
    SslContext client =
        ReloadingSslContext.forDispatcher(
            new TlsBinding(
                agent(null, SIDECAR_ID, sidecar), new PeerPolicy(Set.of(), "example.org")));
    assertThat(handshake(client, server).get(1)).isEqualTo("CN=dispatcher");
  }

  @Test
  void aPolicyThatNamesNoIdentityIsRefused() throws Exception {
    SpiffeCredentialSource source = agent(null, SIDECAR_ID, sidecar);
    assertThatThrownBy(
            () ->
                ReloadingSslContext.forDispatcher(
                    new TlsBinding(source, new PeerPolicy(Set.of(), ""))))
        .isInstanceOf(CredentialException.class)
        .hasMessageContaining("dispatcher.tls")
        .hasMessageContaining("SPIFFE ID");
  }

  @Test
  void aRotatedSvidIsPresentedFromTheNextHandshake() throws Exception {
    FakeWorkloadApi[] agent = new FakeWorkloadApi[1];
    SpiffeCredentialSource source = agent(agent, DISPATCHER_ID, dispatcher);
    SslContext server = ReloadingSslContext.forListener(source);
    SslContext client =
        ReloadingSslContext.forDispatcher(
            accepting(agent(null, SIDECAR_ID, sidecar), DISPATCHER_ID));
    SSLEngine before = server.newEngine(ByteBufAllocator.DEFAULT);
    assertThat(handshake(client, server).get(1)).isEqualTo("CN=dispatcher");

    agent[0].publish(
        DISPATCHER_ID,
        dispatcherNext.certPem(),
        dispatcherNext.keyPem(),
        ca,
        Map.of("partner.test", partnerCa));
    await(() -> subject(source.current().chain().get(0)).equals("CN=dispatcher-next"));
    assertThat(handshake(client, server).get(1)).isEqualTo("CN=dispatcher-next");
    // An engine made before keeps what it was made with.
    SSLEngine sidecarEngine = connecting(client);
    TlsTest.handshake(sidecarEngine, before);
    assertThat(subject(sidecarEngine.getSession().getPeerCertificates()[0]))
        .isEqualTo("CN=dispatcher");
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
