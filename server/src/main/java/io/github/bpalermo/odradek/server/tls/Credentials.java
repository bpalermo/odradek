package io.github.bpalermo.odradek.server.tls;

import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * One consistent set of TLS material: the client's private key, its certificate chain (leaf first)
 * and the certificates brokers are verified against. Immutable, so a handshake that starts from a
 * snapshot never sees half of a rotation.
 *
 * @param trust the authorities of the client's own trust domain, or of the one PKI when the
 *     credentials are not SPIFFE's
 * @param bundles authorities per SPIFFE trust domain, the client's own included; empty when the
 *     source knows no trust domains. A broker is then verified against the bundle of the trust
 *     domain its SPIFFE ID names and no other, so that an authority federated with can only vouch
 *     for its own identities.
 */
public record Credentials(
    PrivateKey key,
    List<X509Certificate> chain,
    List<X509Certificate> trust,
    Map<String, List<X509Certificate>> bundles) {

  public Credentials {
    chain = List.copyOf(chain);
    trust = List.copyOf(trust);
    bundles = Map.copyOf(bundles);
  }

  /** As below, from a source that knows no trust domains. */
  public static Credentials validated(
      PrivateKey key, List<X509Certificate> chain, List<X509Certificate> trust, Instant now)
      throws CredentialException {
    return validated(key, chain, trust, Map.of(), now);
  }

  /**
   * Returns the set if it is usable at {@code now}.
   *
   * @throws CredentialException if a part is missing, the key does not belong to the leaf
   *     certificate, or the leaf is expired or not yet valid
   */
  public static Credentials validated(
      PrivateKey key,
      List<X509Certificate> chain,
      List<X509Certificate> trust,
      Map<String, List<X509Certificate>> bundles,
      Instant now)
      throws CredentialException {
    if (chain.isEmpty()) {
      throw new CredentialException("the certificate chain is empty");
    }
    if (trust.isEmpty()) {
      throw new CredentialException("the trust bundle is empty");
    }
    X509Certificate leaf = chain.get(0);
    try {
      leaf.checkValidity(Date.from(now));
    } catch (CertificateException e) {
      throw new CredentialException(
          "the certificate is not valid now (valid from "
              + leaf.getNotBefore().toInstant()
              + " to "
              + leaf.getNotAfter().toInstant()
              + ")",
          e);
    }
    if (!matches(key, leaf)) {
      throw new CredentialException("the private key does not belong to the certificate");
    }
    return new Credentials(key, chain, trust, bundles);
  }

  /** When the leaf certificate expires. */
  public Instant notAfter() {
    return chain.get(0).getNotAfter().toInstant();
  }

  /** Signs with the key and verifies with the certificate: true only for a pair. */
  private static boolean matches(PrivateKey key, X509Certificate leaf) {
    String algorithm =
        switch (key.getAlgorithm()) {
          case "RSA" -> "SHA256withRSA";
          case "EC" -> "SHA256withECDSA";
          default -> key.getAlgorithm();
        };
    byte[] challenge = {'o', 'd', 'r', 'a', 'd', 'e', 'k'};
    try {
      Signature signer = Signature.getInstance(algorithm);
      signer.initSign(key);
      signer.update(challenge);
      byte[] signature = signer.sign();
      Signature verifier = Signature.getInstance(algorithm);
      verifier.initVerify(leaf.getPublicKey());
      verifier.update(challenge);
      return verifier.verify(signature);
    } catch (GeneralSecurityException e) {
      return false;
    }
  }
}
