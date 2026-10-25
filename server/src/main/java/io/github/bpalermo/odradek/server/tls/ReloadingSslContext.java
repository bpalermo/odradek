package io.github.bpalermo.odradek.server.tls;

import io.github.bpalermo.kafka.spiffe.id.SpiffeIdMatcher;
import io.github.bpalermo.kafka.spiffe.id.TrustDomain;
import io.github.bpalermo.kafka.spiffe.x509.SnapshotSslContext;
import io.github.bpalermo.kafka.spiffe.x509.SpiffeTrustManager;
import io.github.bpalermo.kafka.spiffe.x509.X509Context;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolConfig.Protocol;
import io.netty.handler.ssl.ApplicationProtocolConfig.SelectedListenerFailureBehavior;
import io.netty.handler.ssl.ApplicationProtocolConfig.SelectorFailureBehavior;
import io.netty.handler.ssl.ApplicationProtocolNegotiator;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.IdentityCipherSuiteFilter;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSessionContext;
import javax.net.ssl.X509ExtendedTrustManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The TLS context of the delivery port (ADR 6), for both of its ends: what grpc-netty asks for an
 * engine whenever a connection is made or accepted.
 *
 * <p>Netty's contexts are immutable and grpc-netty takes one for the life of a server or channel,
 * so this is a context that is only a pointer to another: each new connection gets its engine from
 * a context built from the credentials of that moment. A rotated certificate is therefore used from
 * the next handshake on, the listener is never restarted, and a connection that is open keeps the
 * session it negotiated.
 *
 * <p>With a {@code spiffe} source each context is kafka-spiffe's {@code SnapshotSslContext} of the
 * library's snapshot, and peers are verified by its {@code SpiffeTrustManager} (ADR 9), which tells
 * an observer what it accepted and refused. With a {@code files} source it is the server's own
 * {@link PeerTrust}, as for the Kafka clients of such a cluster.
 *
 * <p>A context per set of credentials also means a TLS session cache per set: a connection made
 * after a rotation cannot resume a session negotiated before it, so the certificate a peer is seen
 * with is always one it presented in that handshake.
 *
 * <p>The JDK's TLS, never OpenSSL's: nothing is loaded that the native image would have to carry.
 * TLS 1.3 only, because both ends are this server.
 */
@SuppressWarnings("deprecation") // ApplicationProtocolNegotiator: what grpc-netty reads ALPN from.
public final class ReloadingSslContext extends SslContext {
  private static final Logger LOG = LoggerFactory.getLogger(ReloadingSslContext.class);

  private static final String[] PROTOCOLS = {"TLSv1.3"};

  /** HTTP/2 by ALPN, as grpc-netty configures its own contexts. */
  private static final ApplicationProtocolConfig HTTP2 =
      new ApplicationProtocolConfig(
          Protocol.ALPN,
          SelectorFailureBehavior.NO_ADVERTISE,
          SelectedListenerFailureBehavior.ACCEPT,
          "h2");

  /** The JDK context of one snapshot of the source: its certificate, and how peers are verified. */
  @FunctionalInterface
  private interface Contexts {
    SSLContext of(Object snapshot) throws GeneralSecurityException, IOException;
  }

  private final String name;
  private final Supplier<Object> snapshots;
  private final Function<Object, Instant> notAfter;
  private final boolean client;
  private final Contexts contexts;
  private volatile Built built;

  /** A snapshot a context could not be built from, so as to say so once. */
  private volatile Object rejected;

  /** A context and the snapshot it was built from. */
  private record Built(Object snapshot, JdkSslContext context) {}

  private ReloadingSslContext(
      String name,
      Supplier<Object> snapshots,
      Function<Object, Instant> notAfter,
      boolean client,
      Contexts contexts)
      throws CredentialException {
    this.name = name;
    this.snapshots = snapshots;
    this.notAfter = notAfter;
    this.client = client;
    this.contexts = contexts;
    Object first = snapshots.get();
    try {
      // Fail here, when the server starts, rather than at the first connection.
      built = new Built(first, build(first));
    } catch (GeneralSecurityException | IOException | RuntimeException e) {
      throw new CredentialException(name + ": cannot build a TLS context: " + e.getMessage(), e);
    }
  }

  /** Contexts of the server's own credentials, verifying peers with {@code trust}. */
  private static ReloadingSslContext ofCredentials(
      String name, CredentialSource source, boolean client, PeerTrustOf trust)
      throws CredentialException {
    return new ReloadingSslContext(
        name,
        source::current,
        snapshot -> ((Credentials) snapshot).notAfter(),
        client,
        snapshot -> {
          Credentials credentials = (Credentials) snapshot;
          return PeerTrust.context(credentials, trust.of(credentials));
        });
  }

  /** Contexts of the library's snapshots, verifying peers' IDs with what {@code peers} gives. */
  private static ReloadingSslContext ofSnapshots(
      String name,
      SpiffeCredentialSource source,
      boolean client,
      Function<X509Context, SpiffeIdMatcher> peers,
      SpiffeTrustManager.Observer observer)
      throws CredentialException {
    return new ReloadingSslContext(
        name,
        source::snapshot,
        snapshot -> ((X509Context) snapshot).expiresAt(),
        client,
        snapshot -> {
          X509Context context = (X509Context) snapshot;
          return SnapshotSslContext.create(
              context,
              peers.apply(context),
              SnapshotSslContext.Settings.DEFAULT,
              Clock.systemUTC(),
              observer);
        });
  }

  /** How the server's own verification of a peer is made of the credentials of the moment. */
  @FunctionalInterface
  private interface PeerTrustOf {
    X509ExtendedTrustManager of(Credentials credentials)
        throws GeneralSecurityException, IOException;
  }

  /** As below, counting nothing. */
  public static ReloadingSslContext forListener(CredentialSource source)
      throws CredentialException {
    return forListener(source, SpiffeTrustManager.Observer.NONE);
  }

  /**
   * The context of the dispatcher's listener: it presents the credentials' certificate and requires
   * one of every client. What a sidecar may do is decided per declaration, after the handshake, so
   * the handshake accepts any identity the source can verify: with a {@code spiffe} source, any
   * SPIFFE ID of a trust domain the snapshot has a bundle of, the chain verified against that
   * bundle, and {@code observer} told of each; with {@code files}, as {@link PeerTrust#ofSidecar}
   * says.
   *
   * @throws CredentialException if the current credentials cannot be used
   */
  public static ReloadingSslContext forListener(
      CredentialSource source, SpiffeTrustManager.Observer observer) throws CredentialException {
    String name = TlsSources.LISTENER;
    if (source instanceof SpiffeCredentialSource spiffe) {
      return ofSnapshots(name, spiffe, false, ReloadingSslContext::anyOfTheBundles, observer);
    }
    return ofCredentials(name, source, false, PeerTrust::ofSidecar);
  }

  /** As below, counting nothing. */
  public static ReloadingSslContext forDispatcher(TlsBinding binding) throws CredentialException {
    return forDispatcher(binding, SpiffeTrustManager.Observer.NONE);
  }

  /**
   * The context of a sidecar's connections to the dispatcher: it presents the credentials'
   * certificate and accepts a dispatcher whose SPIFFE ID the binding's policy accepts; with a
   * {@code spiffe} source, {@code observer} is told of each.
   *
   * @throws CredentialException if the current credentials cannot be used, or the policy names no
   *     identity
   */
  public static ReloadingSslContext forDispatcher(
      TlsBinding binding, SpiffeTrustManager.Observer observer) throws CredentialException {
    String name = TlsSources.DISPATCHER;
    PeerPolicy policy = binding.policy();
    if (binding.source() instanceof SpiffeCredentialSource spiffe) {
      if (!policy.spiffe()) {
        throw new CredentialException(
            name + ": a dispatcher is verified by SPIFFE ID; none is accepted");
      }
      SpiffeIdMatcher accepted =
          SpiffeIdMatcher.none()
              .withIds(policy.spiffeIds())
              .withTrustDomains(
                  policy.trustDomain().isEmpty() ? List.of() : List.of(policy.trustDomain()));
      return ofSnapshots(name, spiffe, true, snapshot -> accepted, observer);
    }
    return ofCredentials(
        name, binding.source(), true, credentials -> PeerTrust.ofDispatcher(credentials, policy));
  }

  /** Any SPIFFE ID of a trust domain the snapshot holds a bundle of. */
  private static SpiffeIdMatcher anyOfTheBundles(X509Context snapshot) {
    return SpiffeIdMatcher.none()
        .withTrustDomains(
            snapshot.bundles().trustDomains().stream().map(TrustDomain::name).toList());
  }

  private JdkSslContext build(Object snapshot) throws GeneralSecurityException, IOException {
    return new JdkSslContext(
        contexts.of(snapshot),
        client,
        null,
        IdentityCipherSuiteFilter.INSTANCE,
        HTTP2,
        client ? ClientAuth.NONE : ClientAuth.REQUIRE,
        PROTOCOLS,
        false);
  }

  /**
   * The context of the current snapshot, rebuilt only when it was replaced. A snapshot that no
   * context can be built from leaves the previous one in use: it was validated when it was loaded,
   * so this is not expected, and refusing every connection would be worse.
   */
  private JdkSslContext current() {
    Object snapshot = snapshots.get();
    Built latest = built;
    if (latest.snapshot() == snapshot || rejected == snapshot) {
      return latest.context();
    }
    try {
      latest = new Built(snapshot, build(snapshot));
    } catch (GeneralSecurityException | IOException | RuntimeException e) {
      rejected = snapshot;
      LOG.error(
          "{}: cannot build a TLS context from the new credentials; keeping the certificate that"
              + " expires {}",
          name,
          notAfter.apply(latest.snapshot()),
          e);
      return latest.context();
    }
    built = latest;
    return latest.context();
  }

  @Override
  public boolean isClient() {
    return client;
  }

  @Override
  public List<String> cipherSuites() {
    return current().cipherSuites();
  }

  @Override
  public ApplicationProtocolNegotiator applicationProtocolNegotiator() {
    return current().applicationProtocolNegotiator();
  }

  @Override
  public SSLEngine newEngine(ByteBufAllocator alloc) {
    return current().newEngine(alloc);
  }

  @Override
  public SSLEngine newEngine(ByteBufAllocator alloc, String peerHost, int peerPort) {
    return current().newEngine(alloc, peerHost, peerPort);
  }

  @Override
  protected SslHandler newHandler(ByteBufAllocator alloc, boolean startTls) {
    return new SslHandler(newEngine(alloc), startTls);
  }

  @Override
  protected SslHandler newHandler(
      ByteBufAllocator alloc, String peerHost, int peerPort, boolean startTls) {
    return new SslHandler(newEngine(alloc, peerHost, peerPort), startTls);
  }

  @Override
  public SSLSessionContext sessionContext() {
    return current().sessionContext();
  }
}
