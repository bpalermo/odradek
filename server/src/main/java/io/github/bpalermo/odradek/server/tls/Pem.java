package io.github.bpalermo.odradek.server.tls;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/** Reads PEM certificates and PKCS#8 private keys. */
final class Pem {
  private static final String KEY_BEGIN = "-----BEGIN PRIVATE KEY-----";
  private static final String KEY_END = "-----END PRIVATE KEY-----";

  /** The key algorithms tried in turn: a PKCS#8 blob does not say which one it is in plain. */
  private static final List<String> KEY_ALGORITHMS = List.of("EC", "RSA", "Ed25519");

  private Pem() {}

  /** Returns every certificate in the text, in order. */
  static List<X509Certificate> certificates(byte[] pem, String what) throws CredentialException {
    List<X509Certificate> certificates = new ArrayList<>();
    try {
      CertificateFactory factory = CertificateFactory.getInstance("X.509");
      for (var certificate : factory.generateCertificates(new ByteArrayInputStream(pem))) {
        certificates.add((X509Certificate) certificate);
      }
    } catch (CertificateException e) {
      throw new CredentialException(what + " is not a PEM list of certificates", e);
    }
    return certificates;
  }

  /** Returns the one unencrypted PKCS#8 private key in the text. */
  static PrivateKey privateKey(byte[] pem, String what) throws CredentialException {
    String text = new String(pem, StandardCharsets.US_ASCII);
    int begin = text.indexOf(KEY_BEGIN);
    int end = text.indexOf(KEY_END);
    if (begin < 0 || end < begin) {
      throw new CredentialException(
          what + " holds no unencrypted PKCS#8 private key (BEGIN PRIVATE KEY)");
    }
    byte[] der;
    try {
      der = Base64.getMimeDecoder().decode(text.substring(begin + KEY_BEGIN.length(), end));
    } catch (IllegalArgumentException e) {
      throw new CredentialException(what + " is not valid base64", e);
    }
    PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(der);
    for (String algorithm : KEY_ALGORITHMS) {
      try {
        return KeyFactory.getInstance(algorithm).generatePrivate(spec);
      } catch (InvalidKeySpecException | java.security.NoSuchAlgorithmException e) {
        // Not this algorithm; try the next.
      }
    }
    throw new CredentialException(what + " is not an EC, RSA or Ed25519 private key");
  }
}
