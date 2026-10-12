package io.github.bpalermo.odradek.server.tls;

import java.net.Socket;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SslEngineFactory;

/**
 * The TLS engine factory the server gives every Kafka client of a cluster with {@code tls}.
 *
 * <p>Each new connection gets an engine built from the credentials of that moment, so a rotated
 * certificate is used from the next handshake on and no Kafka client is ever rebuilt for it.
 * Established connections keep the session they negotiated.
 *
 * <p>Kafka creates the factory by class name and hands it the client's configuration; the server
 * puts the cluster's {@link TlsBinding} in that configuration under {@link #BINDING_CONFIG}.
 */
public final class ReloadingSslEngineFactory implements SslEngineFactory {
  /** Configuration key of the {@link TlsBinding}. Set by the server, never by a file. */
  public static final String BINDING_CONFIG = "odradek.tls.binding";

  /** In-memory key store entries need a password; it protects nothing. */
  private static final char[] PASSWORD = "in-memory".toCharArray();

  private TlsBinding binding;
  private List<String> protocols;
  private List<String> cipherSuites;
  private volatile Built built;

  /** An SSLContext and the credentials it was built from. */
  private record Built(Credentials credentials, SSLContext context) {}

  public ReloadingSslEngineFactory() {}

  @Override
  public void configure(Map<String, ?> configs) {
    if (!(configs.get(BINDING_CONFIG) instanceof TlsBinding found)) {
      throw new KafkaException(BINDING_CONFIG + " is not set: this factory is for odradek only");
    }
    binding = found;
    protocols = strings(configs.get(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG));
    cipherSuites = strings(configs.get(SslConfigs.SSL_CIPHER_SUITES_CONFIG));
    // Fail here, when the client is created, rather than at the first connection.
    context();
  }

  @Override
  public SSLEngine createClientSslEngine(
      String peerHost, int peerPort, String endpointIdentification) {
    SSLEngine engine = context().createSSLEngine(peerHost, peerPort);
    engine.setUseClientMode(true);
    SSLParameters parameters = engine.getSSLParameters();
    if (!protocols.isEmpty()) {
      parameters.setProtocols(protocols.toArray(String[]::new));
    }
    if (!cipherSuites.isEmpty()) {
      parameters.setCipherSuites(cipherSuites.toArray(String[]::new));
    }
    // A SPIFFE identity replaces the host name check; see SpiffeTrustManager.
    parameters.setEndpointIdentificationAlgorithm(
        binding.policy().spiffe() ? "" : endpointIdentification);
    engine.setSSLParameters(parameters);
    return engine;
  }

  @Override
  public SSLEngine createServerSslEngine(String peerHost, int peerPort) {
    throw new UnsupportedOperationException("odradek only connects to brokers");
  }

  /** Never: credentials change underneath, not through the client's configuration. */
  @Override
  public boolean shouldBeRebuilt(Map<String, Object> nextConfigs) {
    return false;
  }

  @Override
  public Set<String> reconfigurableConfigs() {
    return Set.of();
  }

  @Override
  public KeyStore keystore() {
    return null;
  }

  @Override
  public KeyStore truststore() {
    return null;
  }

  @Override
  public void close() {}

  /** The context of the current credentials, rebuilt only when they were replaced. */
  private SSLContext context() {
    Credentials credentials = binding.source().current();
    Built latest = built;
    if (latest == null || latest.credentials() != credentials) {
      try {
        latest = new Built(credentials, build(credentials, binding.policy()));
      } catch (GeneralSecurityException | java.io.IOException e) {
        throw new KafkaException("cannot build a TLS context from the credentials", e);
      }
      built = latest;
    }
    return latest.context();
  }

  private static SSLContext build(Credentials credentials, PeerPolicy policy)
      throws GeneralSecurityException, java.io.IOException {
    KeyStore keys = KeyStore.getInstance("PKCS12");
    keys.load(null, null);
    keys.setKeyEntry(
        "client", credentials.key(), PASSWORD, credentials.chain().toArray(X509Certificate[]::new));
    KeyManagerFactory keyManagers =
        KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    keyManagers.init(keys, PASSWORD);

    KeyStore trust = KeyStore.getInstance("PKCS12");
    trust.load(null, null);
    int n = 0;
    for (X509Certificate certificate : credentials.trust()) {
      trust.setCertificateEntry("ca-" + n++, certificate);
    }
    TrustManagerFactory trustManagers =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trustManagers.init(trust);
    X509ExtendedTrustManager pkix = null;
    for (TrustManager manager : trustManagers.getTrustManagers()) {
      if (manager instanceof X509ExtendedTrustManager extended) {
        pkix = extended;
        break;
      }
    }
    if (pkix == null) {
      throw new GeneralSecurityException("the JDK offers no X.509 trust manager");
    }

    SSLContext context = SSLContext.getInstance("TLS");
    context.init(
        keyManagers.getKeyManagers(),
        new TrustManager[] {policy.spiffe() ? new SpiffeTrustManager(pkix, policy) : pkix},
        null);
    return context;
  }

  private static List<String> strings(Object value) {
    if (value instanceof List<?> list) {
      return list.stream().map(Object::toString).toList();
    }
    if (value instanceof String text && !text.isBlank()) {
      return List.of(text.split("\\s*,\\s*"));
    }
    return List.of();
  }

  /** Chain validation by the JDK, then the SPIFFE ID of the leaf against the policy. */
  private static final class SpiffeTrustManager extends X509ExtendedTrustManager {
    private final X509ExtendedTrustManager pkix;
    private final PeerPolicy policy;

    SpiffeTrustManager(X509ExtendedTrustManager pkix, PeerPolicy policy) {
      this.pkix = pkix;
      this.policy = policy;
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
        throws CertificateException {
      pkix.checkServerTrusted(chain, authType, engine);
      policy.check(chain[0]);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket)
        throws CertificateException {
      pkix.checkServerTrusted(chain, authType, socket);
      policy.check(chain[0]);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType)
        throws CertificateException {
      pkix.checkServerTrusted(chain, authType);
      policy.check(chain[0]);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
        throws CertificateException {
      throw new CertificateException("odradek only connects to brokers");
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket)
        throws CertificateException {
      throw new CertificateException("odradek only connects to brokers");
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType)
        throws CertificateException {
      throw new CertificateException("odradek only connects to brokers");
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
      return pkix.getAcceptedIssuers();
    }
  }
}
