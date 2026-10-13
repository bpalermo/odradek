package io.github.bpalermo.odradek.server.tls;

import java.io.IOException;
import java.net.Socket;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;

/**
 * How a peer's certificate is verified against one set of {@link Credentials}, for the three peers
 * the server has, and the JDK context made of both.
 *
 * <p>Everything here is built from one immutable snapshot: a rotation makes a new one, it never
 * changes one in use.
 */
final class PeerTrust {
  /** In-memory key store entries need a password; it protects nothing. */
  private static final char[] PASSWORD = "in-memory".toCharArray();

  private PeerTrust() {}

  /**
   * A broker, verified by the server as its client. Without SPIFFE identities in the policy this is
   * the JDK's own verification, host name included.
   */
  static X509ExtendedTrustManager ofBroker(Credentials credentials, PeerPolicy policy)
      throws GeneralSecurityException, IOException {
    X509ExtendedTrustManager pkix = pkix(credentials.trust());
    if (!policy.spiffe()) {
      return pkix;
    }
    return new Manager(pkix, byDomain(credentials), policy, true, true);
  }

  /**
   * The dispatcher, verified by a sidecar as its client: the chain, then the SPIFFE ID against the
   * policy, which must name one. The host name is never checked, whatever the engine asks for:
   * replicas are connected to by IP address, and grpc-netty turns the HTTPS check on in every
   * engine it creates, after the context has handed it over.
   */
  static X509ExtendedTrustManager ofDispatcher(Credentials credentials, PeerPolicy policy)
      throws GeneralSecurityException, IOException {
    if (!policy.spiffe()) {
      throw new GeneralSecurityException("a dispatcher is verified by SPIFFE ID; none is accepted");
    }
    return new Manager(pkix(credentials.trust()), byDomain(credentials), policy, true, false);
  }

  /**
   * A sidecar, verified by the dispatcher as its server: the chain only. What the identity may do
   * is decided per declaration, where a refusal can say why; a handshake can only fail.
   */
  static X509ExtendedTrustManager ofSidecar(Credentials credentials)
      throws GeneralSecurityException, IOException {
    return new Manager(pkix(credentials.trust()), byDomain(credentials), null, false, true);
  }

  /**
   * A JDK context that presents the credentials' certificate and verifies peers with {@code trust}.
   */
  static SSLContext context(Credentials credentials, TrustManager trust)
      throws GeneralSecurityException, IOException {
    KeyStore keys = KeyStore.getInstance("PKCS12");
    keys.load(null, null);
    keys.setKeyEntry(
        "own", credentials.key(), PASSWORD, credentials.chain().toArray(X509Certificate[]::new));
    KeyManagerFactory keyManagers =
        KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    keyManagers.init(keys, PASSWORD);
    SSLContext context = SSLContext.getInstance("TLS");
    context.init(keyManagers.getKeyManagers(), new TrustManager[] {trust}, null);
    return context;
  }

  private static Map<String, X509ExtendedTrustManager> byDomain(Credentials credentials)
      throws GeneralSecurityException, IOException {
    Map<String, X509ExtendedTrustManager> byDomain = new HashMap<>();
    for (var bundle : credentials.bundles().entrySet()) {
      byDomain.put(bundle.getKey().toLowerCase(Locale.ROOT), pkix(bundle.getValue()));
    }
    return byDomain;
  }

  /** The JDK's chain validation against a set of authorities. */
  private static X509ExtendedTrustManager pkix(List<X509Certificate> authorities)
      throws GeneralSecurityException, IOException {
    KeyStore store = KeyStore.getInstance("PKCS12");
    store.load(null, null);
    int n = 0;
    for (X509Certificate certificate : authorities) {
      store.setCertificateEntry("ca-" + n++, certificate);
    }
    TrustManagerFactory factory =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    factory.init(store);
    for (TrustManager manager : factory.getTrustManagers()) {
      if (manager instanceof X509ExtendedTrustManager extended) {
        return extended;
      }
    }
    throw new GeneralSecurityException("the JDK offers no X.509 trust manager");
  }

  /**
   * Chain validation by the JDK, then the SPIFFE ID of the leaf against the policy, if there is
   * one. When the credentials carry a bundle per trust domain, the chain must verify against the
   * bundle of the trust domain the leaf's SPIFFE ID names.
   */
  private static final class Manager extends X509ExtendedTrustManager {
    private final X509ExtendedTrustManager pkix;
    private final Map<String, X509ExtendedTrustManager> byDomain;
    private final PeerPolicy policy;

    /** Whether the peers verified are servers; the other kind is refused. */
    private final boolean servers;

    /** Whether the JDK is shown the connection, and so applies what its parameters ask for. */
    private final boolean withConnection;

    Manager(
        X509ExtendedTrustManager pkix,
        Map<String, X509ExtendedTrustManager> byDomain,
        PeerPolicy policy,
        boolean servers,
        boolean withConnection) {
      this.pkix = pkix;
      this.byDomain = byDomain;
      this.policy = policy;
      this.servers = servers;
      this.withConnection = withConnection;
    }

    private X509ExtendedTrustManager authorityOf(X509Certificate[] chain, boolean server)
        throws CertificateException {
      if (server != servers) {
        throw new CertificateException(
            servers ? "this end only connects to servers" : "this end only accepts clients");
      }
      if (chain == null || chain.length == 0) {
        throw new CertificateException("the peer presented no certificate");
      }
      if (byDomain.isEmpty()) {
        return pkix;
      }
      String domain = PeerPolicy.trustDomain(PeerPolicy.spiffeId(chain[0]));
      X509ExtendedTrustManager manager = byDomain.get(domain);
      if (manager == null) {
        throw new CertificateException("no trust bundle for the peer's trust domain " + domain);
      }
      return manager;
    }

    private void identity(X509Certificate[] chain) throws CertificateException {
      if (policy != null) {
        policy.check(chain[0]);
      }
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
        throws CertificateException {
      X509ExtendedTrustManager authority = authorityOf(chain, true);
      if (withConnection) {
        authority.checkServerTrusted(chain, authType, engine);
      } else {
        authority.checkServerTrusted(chain, authType);
      }
      identity(chain);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket)
        throws CertificateException {
      X509ExtendedTrustManager authority = authorityOf(chain, true);
      if (withConnection) {
        authority.checkServerTrusted(chain, authType, socket);
      } else {
        authority.checkServerTrusted(chain, authType);
      }
      identity(chain);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType)
        throws CertificateException {
      authorityOf(chain, true).checkServerTrusted(chain, authType);
      identity(chain);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
        throws CertificateException {
      X509ExtendedTrustManager authority = authorityOf(chain, false);
      if (withConnection) {
        authority.checkClientTrusted(chain, authType, engine);
      } else {
        authority.checkClientTrusted(chain, authType);
      }
      identity(chain);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket)
        throws CertificateException {
      X509ExtendedTrustManager authority = authorityOf(chain, false);
      if (withConnection) {
        authority.checkClientTrusted(chain, authType, socket);
      } else {
        authority.checkClientTrusted(chain, authType);
      }
      identity(chain);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType)
        throws CertificateException {
      authorityOf(chain, false).checkClientTrusted(chain, authType);
      identity(chain);
    }

    /**
     * Every authority a peer may chain to. A server sends their names with its request for a
     * certificate, and a client may offer none when its own authority is not among them.
     */
    @Override
    public X509Certificate[] getAcceptedIssuers() {
      List<X509Certificate> issuers = new ArrayList<>(List.of(pkix.getAcceptedIssuers()));
      for (X509ExtendedTrustManager manager : byDomain.values()) {
        for (X509Certificate issuer : manager.getAcceptedIssuers()) {
          if (!issuers.contains(issuer)) {
            issuers.add(issuer);
          }
        }
      }
      return issuers.toArray(X509Certificate[]::new);
    }
  }
}
