package io.github.bpalermo.odradek.server.tls;

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
import java.util.List;
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
 * session it negotiated. It is the arrangement {@link ReloadingSslEngineFactory} has for the Kafka
 * clients, with the same verification of the peer ({@link PeerTrust}).
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

  /** How the peer is verified, given the credentials of the moment. */
  @FunctionalInterface
  private interface Trust {
    X509ExtendedTrustManager of(Credentials credentials)
        throws GeneralSecurityException, IOException;
  }

  private final String name;
  private final CredentialSource source;
  private final boolean client;
  private final Trust trust;
  private volatile Built built;

  /** Credentials a context could not be built from, so as to say so once. */
  private volatile Credentials rejected;

  /** A context and the credentials it was built from. */
  private record Built(Credentials credentials, JdkSslContext context) {}

  private ReloadingSslContext(String name, CredentialSource source, boolean client, Trust trust)
      throws CredentialException {
    this.name = name;
    this.source = source;
    this.client = client;
    this.trust = trust;
    Credentials first = source.current();
    try {
      // Fail here, when the server starts, rather than at the first connection.
      built = new Built(first, build(first));
    } catch (GeneralSecurityException | IOException e) {
      throw new CredentialException(name + ": cannot build a TLS context: " + e.getMessage(), e);
    }
  }

  /**
   * The context of the dispatcher's listener: it presents the credentials' certificate and requires
   * one of every client, verified as {@link PeerTrust#ofSidecar} says.
   *
   * @throws CredentialException if the current credentials cannot be used
   */
  public static ReloadingSslContext forListener(CredentialSource source)
      throws CredentialException {
    return new ReloadingSslContext("dispatch.tls", source, false, PeerTrust::ofSidecar);
  }

  /**
   * The context of a sidecar's connections to the dispatcher: it presents the credentials'
   * certificate and accepts a dispatcher whose SPIFFE ID the binding's policy accepts.
   *
   * @throws CredentialException if the current credentials cannot be used, or the policy names no
   *     identity
   */
  public static ReloadingSslContext forDispatcher(TlsBinding binding) throws CredentialException {
    return new ReloadingSslContext(
        "dispatcher.tls",
        binding.source(),
        true,
        credentials -> PeerTrust.ofDispatcher(credentials, binding.policy()));
  }

  private JdkSslContext build(Credentials credentials)
      throws GeneralSecurityException, IOException {
    return new JdkSslContext(
        PeerTrust.context(credentials, trust.of(credentials)),
        client,
        null,
        IdentityCipherSuiteFilter.INSTANCE,
        HTTP2,
        client ? ClientAuth.NONE : ClientAuth.REQUIRE,
        PROTOCOLS,
        false);
  }

  /**
   * The context of the current credentials, rebuilt only when they were replaced. Credentials that
   * no context can be built from leave the previous one in use: they were validated when they were
   * loaded, so this is not expected, and refusing every connection would be worse.
   */
  private JdkSslContext current() {
    Credentials credentials = source.current();
    Built latest = built;
    if (latest.credentials() == credentials || rejected == credentials) {
      return latest.context();
    }
    try {
      latest = new Built(credentials, build(credentials));
    } catch (GeneralSecurityException | IOException | RuntimeException e) {
      rejected = credentials;
      LOG.error(
          "{}: cannot build a TLS context from the new credentials; keeping the certificate that"
              + " expires {}",
          name,
          latest.credentials().notAfter(),
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
