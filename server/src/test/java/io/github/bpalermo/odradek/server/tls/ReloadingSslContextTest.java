package io.github.bpalermo.odradek.server.tls;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.SslContext;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The TLS context of the delivery port, one handshake at a time and in memory: a sidecar's end
 * against a dispatcher's. What gRPC makes of a refused handshake is {@code DeliveryTlsTest}'s.
 */
class ReloadingSslContextTest {
  private static final String DISPATCHER_ID = "spiffe://example.org/ns/kafka/sa/dispatcher";
  private static final String SIDECAR_ID = "spiffe://example.org/ns/billing/sa/billing";
  private static final String PARTNER_ID = "spiffe://partner.test/ns/billing/sa/billing";

  @TempDir static Path shared;

  private static String ca;
  private static String otherCa;
  private static String partnerCa;
  private static TestPki.Issued dispatcher;
  private static TestPki.Issued dispatcherNext;
  private static TestPki.Issued sidecar;
  private static TestPki.Issued sidecarNext;

  /** No SPIFFE ID: a certificate of some other PKI. */
  private static TestPki.Issued plain;

  /** The right identities, signed by an authority nobody trusts. */
  private static TestPki.Issued strangeDispatcher;

  private static TestPki.Issued strangeSidecar;

  /** Of the partner's trust domain, signed by the partner's authority. */
  private static TestPki.Issued partnerSidecar;

  private static TestPki.Issued partnerDispatcher;

  /** Signed by the partner's authority, but claiming identities of example.org. */
  private static TestPki.Issued impostorSidecar;

  private static TestPki.Issued impostorDispatcher;

  @BeforeAll
  static void issue() throws Exception {
    TestPki pki = new TestPki(shared, "ca");
    TestPki other = new TestPki(shared, "other");
    TestPki partner = new TestPki(shared, "partner");
    ca = pki.caPem();
    otherCa = other.caPem();
    partnerCa = partner.caPem();
    dispatcher = pki.issue("dispatcher", "uri:" + DISPATCHER_ID);
    dispatcherNext = pki.issue("dispatcher-next", "uri:" + DISPATCHER_ID);
    sidecar = pki.issue("sidecar", "uri:" + SIDECAR_ID);
    sidecarNext = pki.issue("sidecar-next", "uri:" + SIDECAR_ID);
    plain = pki.issue("plain", "dns:plain.example.org");
    strangeDispatcher = other.issue("strange-dispatcher", "uri:" + DISPATCHER_ID);
    strangeSidecar = other.issue("strange-sidecar", "uri:" + SIDECAR_ID);
    partnerSidecar = partner.issue("partner-sidecar", "uri:" + PARTNER_ID);
    partnerDispatcher = partner.issue("partner-dispatcher", "uri:spiffe://partner.test/dispatcher");
    impostorSidecar = partner.issue("impostor-sidecar", "uri:" + SIDECAR_ID);
    impostorDispatcher = partner.issue("impostor-dispatcher", "uri:" + DISPATCHER_ID);
  }

  /** Credentials that a test replaces, as a rotation does. */
  private static final class Holder implements CredentialSource {
    private volatile Credentials current;

    Holder(Credentials first) {
      current = first;
    }

    void rotate(Credentials next) {
      current = next;
    }

    @Override
    public Credentials current() {
      return current;
    }

    @Override
    public long reloads() {
      return 1;
    }

    @Override
    public long failures() {
      return 0;
    }
  }

  private static List<X509Certificate> certificates(String pem) throws CredentialException {
    return Pem.certificates(pem.getBytes(StandardCharsets.US_ASCII), "test");
  }

  /** As a files source loads them: one authority, no trust domains. */
  private static Credentials files(TestPki.Issued issued, String caPem) throws Exception {
    return Credentials.validated(
        Pem.privateKey(issued.keyPem().getBytes(StandardCharsets.US_ASCII), "test"),
        certificates(issued.certPem()),
        certificates(caPem),
        Instant.now());
  }

  /** As the Workload API hands them out: example.org's bundle, and the partner's federated. */
  private static Credentials federated(TestPki.Issued issued) throws Exception {
    Map<String, List<X509Certificate>> bundles = new HashMap<>();
    bundles.put("example.org", certificates(ca));
    bundles.put("partner.test", certificates(partnerCa));
    return Credentials.validated(
        Pem.privateKey(issued.keyPem().getBytes(StandardCharsets.US_ASCII), "test"),
        certificates(issued.certPem()),
        certificates(ca),
        bundles,
        Instant.now());
  }

  private static SslContext listener(Credentials credentials) throws Exception {
    return ReloadingSslContext.forListener(new Holder(credentials));
  }

  private static SslContext connector(Credentials credentials, PeerPolicy policy) throws Exception {
    return ReloadingSslContext.forDispatcher(new TlsBinding(new Holder(credentials), policy));
  }

  private static PeerPolicy accepting(String... spiffeIds) {
    return new PeerPolicy(Set.of(spiffeIds), "");
  }

  /** What each end saw of the other. */
  private record Seen(String sidecar, String dispatcher, String protocol, String application) {}

  /** A sidecar's engine, as grpc-netty makes one: for an address, and with the HTTPS check on. */
  private static SSLEngine connecting(SslContext context) {
    SSLEngine engine = context.newEngine(ByteBufAllocator.DEFAULT, "10.0.0.9", 9465);
    SSLParameters parameters = engine.getSSLParameters();
    parameters.setEndpointIdentificationAlgorithm("HTTPS");
    engine.setSSLParameters(parameters);
    return engine;
  }

  private static Seen handshake(SslContext sidecarEnd, SslContext dispatcherEnd) throws Exception {
    return handshake(connecting(sidecarEnd), dispatcherEnd.newEngine(ByteBufAllocator.DEFAULT));
  }

  private static Seen handshake(SSLEngine client, SSLEngine server) throws Exception {
    TlsTest.handshake(client, server);
    return new Seen(
        subject(server.getSession().getPeerCertificates()[0]),
        subject(client.getSession().getPeerCertificates()[0]),
        client.getSession().getProtocol(),
        client.getApplicationProtocol());
  }

  private static String subject(java.security.cert.Certificate certificate) {
    return ((X509Certificate) certificate).getSubjectX500Principal().getName();
  }

  // --- Both ends ---

  @Test
  void bothEndsPresentTheirCertificateOverTls13AndHttp2() throws Exception {
    SslContext server = listener(files(dispatcher, ca));
    SslContext client = connector(files(sidecar, ca), accepting(DISPATCHER_ID));
    assertThat(server.isServer()).isTrue();
    assertThat(client.isClient()).isTrue();
    assertThat(handshake(client, server))
        .isEqualTo(new Seen("CN=sidecar", "CN=dispatcher", "TLSv1.3", "h2"));
  }

  @Test
  void rotatedCredentialsAreUsedByTheNextHandshakeOnBothEnds() throws Exception {
    Holder dispatcherCredentials = new Holder(files(dispatcher, ca));
    Holder sidecarCredentials = new Holder(files(sidecar, ca));
    SslContext server = ReloadingSslContext.forListener(dispatcherCredentials);
    SslContext client =
        ReloadingSslContext.forDispatcher(
            new TlsBinding(sidecarCredentials, accepting(DISPATCHER_ID)));
    assertThat(handshake(client, server).sidecar()).isEqualTo("CN=sidecar");

    // The same contexts: nothing is rebuilt by whoever holds them.
    sidecarCredentials.rotate(files(sidecarNext, ca));
    Seen seen = handshake(client, server);
    assertThat(seen.sidecar()).isEqualTo("CN=sidecar-next");
    assertThat(seen.dispatcher()).isEqualTo("CN=dispatcher");

    dispatcherCredentials.rotate(files(dispatcherNext, ca));
    seen = handshake(client, server);
    assertThat(seen.sidecar()).isEqualTo("CN=sidecar-next");
    assertThat(seen.dispatcher()).isEqualTo("CN=dispatcher-next");
  }

  @Test
  void aRotatedAuthorityIsTheOneTheNextHandshakeVerifiesWith() throws Exception {
    Holder dispatcherCredentials = new Holder(files(dispatcher, ca));
    SslContext server = ReloadingSslContext.forListener(dispatcherCredentials);
    SslContext stranger = connector(files(strangeSidecar, ca), accepting(DISPATCHER_ID));
    assertThatThrownBy(() -> handshake(stranger, server)).isInstanceOf(SSLException.class);

    // The bundle now holds both authorities, as during the rotation of one.
    dispatcherCredentials.rotate(files(dispatcher, ca + otherCa));
    assertThat(handshake(stranger, server).sidecar()).isEqualTo("CN=strange-sidecar");
  }

  @Test
  void anEngineMadeBeforeARotationKeepsTheCredentialsItWasMadeWith() throws Exception {
    Holder dispatcherCredentials = new Holder(files(dispatcher, ca));
    Holder sidecarCredentials = new Holder(files(sidecar, ca));
    SslContext server = ReloadingSslContext.forListener(dispatcherCredentials);
    SslContext client =
        ReloadingSslContext.forDispatcher(
            new TlsBinding(sidecarCredentials, accepting(DISPATCHER_ID)));
    SSLEngine clientEngine = connecting(client);
    SSLEngine serverEngine = server.newEngine(ByteBufAllocator.DEFAULT);

    dispatcherCredentials.rotate(files(dispatcherNext, ca));
    sidecarCredentials.rotate(files(sidecarNext, ca));
    Seen seen = handshake(clientEngine, serverEngine);
    assertThat(seen.sidecar()).isEqualTo("CN=sidecar");
    assertThat(seen.dispatcher()).isEqualTo("CN=dispatcher");
  }

  // --- The sidecar's end: who is the dispatcher ---

  @Test
  void aDispatcherInTheAcceptedTrustDomainIsAccepted() throws Exception {
    SslContext client = connector(files(sidecar, ca), new PeerPolicy(Set.of(), "Example.org"));
    assertThat(handshake(client, listener(files(dispatcher, ca))).dispatcher())
        .isEqualTo("CN=dispatcher");
  }

  @Test
  void aDispatcherWithAnotherSpiffeIdIsRejected() throws Exception {
    // A sidecar's certificate is of the same authority, and no dispatcher.
    SslContext client = connector(files(sidecar, ca), accepting(DISPATCHER_ID));
    assertThatThrownBy(() -> handshake(client, listener(files(sidecarNext, ca))))
        .isInstanceOf(SSLException.class)
        .hasStackTraceContaining(SIDECAR_ID + " is not accepted");
  }

  @Test
  void aDispatcherWithoutSpiffeIdIsRejected() throws Exception {
    SslContext client = connector(files(sidecar, ca), new PeerPolicy(Set.of(), "example.org"));
    assertThatThrownBy(() -> handshake(client, listener(files(plain, ca))))
        .isInstanceOf(SSLException.class)
        .hasStackTraceContaining("exactly one SPIFFE ID");
  }

  @Test
  void aDispatcherOfAnotherAuthorityIsRejected() throws Exception {
    SslContext client = connector(files(sidecar, ca), accepting(DISPATCHER_ID));
    // It trusts the sidecar's authority, so only the sidecar can object.
    assertThatThrownBy(() -> handshake(client, listener(files(strangeDispatcher, otherCa + ca))))
        .isInstanceOf(SSLException.class);
  }

  @Test
  void theHostNameIsNotChecked() throws Exception {
    // The engine is made for 10.0.0.9 with the HTTPS check on, and the certificate names neither
    // that nor anything else but a SPIFFE ID: this is every handshake above. Here, the opposite
    // case, so that the test would notice the check coming back: the JDK's own verification.
    SslContext server = listener(files(dispatcher, ca));
    SSLEngine jdk = trusting(ca).createSSLEngine("10.0.0.9", 9465);
    jdk.setUseClientMode(true);
    SSLParameters parameters = jdk.getSSLParameters();
    parameters.setEndpointIdentificationAlgorithm("HTTPS");
    jdk.setSSLParameters(parameters);
    assertThatThrownBy(() -> TlsTest.handshake(jdk, server.newEngine(ByteBufAllocator.DEFAULT)))
        .isInstanceOf(SSLException.class)
        .hasStackTraceContaining("No subject alternative names");
  }

  @Test
  void aPolicyThatNamesNoIdentityIsRefused() throws Exception {
    Credentials credentials = files(sidecar, ca);
    assertThatThrownBy(() -> connector(credentials, new PeerPolicy(Set.of(), "")))
        .isInstanceOf(CredentialException.class)
        .hasMessageContaining("dispatcher.tls")
        .hasMessageContaining("SPIFFE ID");
  }

  @Test
  void aDispatcherIsVerifiedAgainstTheBundleOfItsOwnTrustDomain() throws Exception {
    SslContext client =
        connector(federated(sidecar), accepting(DISPATCHER_ID, "spiffe://partner.test/dispatcher"));
    String both = ca + partnerCa;
    assertThat(handshake(client, listener(files(dispatcher, both))).dispatcher())
        .isEqualTo("CN=dispatcher");
    assertThat(handshake(client, listener(files(partnerDispatcher, both))).dispatcher())
        .isEqualTo("CN=partner-dispatcher");
    // The partner's authority is trusted, but only for the partner's own identities.
    assertThatThrownBy(() -> handshake(client, listener(files(impostorDispatcher, both))))
        .isInstanceOf(SSLException.class);
  }

  // --- The dispatcher's end: who may connect ---

  @Test
  void aSidecarOfAnotherAuthorityIsRejected() throws Exception {
    // It trusts the dispatcher's authority, so only the dispatcher can object.
    SslContext client = connector(files(strangeSidecar, ca), accepting(DISPATCHER_ID));
    assertThatThrownBy(() -> handshake(client, listener(files(dispatcher, ca))))
        .isInstanceOf(SSLException.class);
  }

  @Test
  void aSidecarWithoutCertificateIsRejected() throws Exception {
    SslContext server = listener(files(dispatcher, ca));
    SSLEngine anonymous = trusting(ca).createSSLEngine();
    anonymous.setUseClientMode(true);
    assertThatThrownBy(
            () -> TlsTest.handshake(anonymous, server.newEngine(ByteBufAllocator.DEFAULT)))
        .isInstanceOf(SSLException.class);
  }

  @Test
  void anOlderTlsVersionIsNotSpoken() throws Exception {
    SslContext server = listener(files(dispatcher, ca));
    SslContext client = connector(files(sidecar, ca), accepting(DISPATCHER_ID));
    SSLEngine old = connecting(client);
    assertThat(old.getEnabledProtocols()).containsExactly("TLSv1.3");
    assertThat(server.newEngine(ByteBufAllocator.DEFAULT).getEnabledProtocols())
        .containsExactly("TLSv1.3");
  }

  @Test
  void withAFilesSourceASidecarWithoutSpiffeIdCompletesTheHandshake() throws Exception {
    // Who it is, and so that it is nobody the policy knows, is said per declaration, where a
    // refusal has words: see DispatchService.
    SslContext client = connector(files(plain, ca), accepting(DISPATCHER_ID));
    assertThat(handshake(client, listener(files(dispatcher, ca))).sidecar()).isEqualTo("CN=plain");
  }

  @Test
  void aSidecarIsVerifiedAgainstTheBundleOfItsOwnTrustDomain() throws Exception {
    SslContext server = listener(federated(dispatcher));
    PeerPolicy policy = accepting(DISPATCHER_ID);
    String both = ca + partnerCa;

    assertThat(handshake(connector(files(sidecar, both), policy), server).sidecar())
        .isEqualTo("CN=sidecar");
    assertThat(handshake(connector(files(partnerSidecar, both), policy), server).sidecar())
        .isEqualTo("CN=partner-sidecar");
    // The partner's authority is trusted, but only for the partner's own identities.
    SslContext impostor = connector(files(impostorSidecar, both), policy);
    assertThatThrownBy(() -> handshake(impostor, server)).isInstanceOf(SSLException.class);
    // Without a SPIFFE ID there is no trust domain to verify it in.
    SslContext nobody = connector(files(plain, both), policy);
    assertThatThrownBy(() -> handshake(nobody, server)).isInstanceOf(SSLException.class);
  }

  @Test
  void theListenerNamesEveryAuthorityItAcceptsClientsOf() throws Exception {
    // A client chooses its certificate by these names: one of the partner's must find its own.
    SSLEngine engine = listener(federated(dispatcher)).newEngine(ByteBufAllocator.DEFAULT);
    assertThat(engine.getUseClientMode()).isFalse();
    assertThat(engine.getNeedClientAuth()).isTrue();
    assertThat(
            List.of(PeerTrust.ofSidecar(federated(dispatcher)).getAcceptedIssuers()).stream()
                .map(ReloadingSslContextTest::subject))
        .containsExactlyInAnyOrder("CN=ca", "CN=partner");
  }

  /** A JDK context with no certificate of its own, which trusts one authority. */
  private static SSLContext trusting(String caPem) throws Exception {
    KeyStore trust = KeyStore.getInstance("PKCS12");
    trust.load(null, null);
    trust.setCertificateEntry("ca", certificates(caPem).get(0));
    TrustManagerFactory trustManagers =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trustManagers.init(trust);
    SSLContext context = SSLContext.getInstance("TLS");
    context.init(null, trustManagers.getTrustManagers(), null);
    return context;
  }
}
