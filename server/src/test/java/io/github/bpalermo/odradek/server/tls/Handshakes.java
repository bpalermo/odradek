package io.github.bpalermo.odradek.server.tls;

import org.apache.kafka.common.security.auth.SslEngineFactory;

/** In-memory handshakes for tests outside this package. */
public final class Handshakes {
  private Handshakes() {}

  /**
   * Completes a handshake between an engine of the factory and a broker that demands a client
   * certificate. Returns the subject of the client certificate the broker saw.
   *
   * @throws javax.net.ssl.SSLException if either end refuses the other
   */
  public static String withBroker(
      SslEngineFactory factory, String host, TestPki.Issued broker, String clientCa)
      throws Exception {
    return TlsTest.handshake(factory, host, broker, clientCa);
  }
}
