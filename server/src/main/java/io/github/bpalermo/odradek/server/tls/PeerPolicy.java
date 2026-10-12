package io.github.bpalermo.odradek.server.tls;

import io.github.bpalermo.odradek.config.v1.Tls;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Which broker identities are accepted.
 *
 * <p>With no SPIFFE ID and no trust domain configured the broker is verified the usual way: its
 * certificate must chain to the trust bundle and name the host connected to. With either, the host
 * name is not checked (an X.509-SVID usually has no DNS name) and the certificate's SPIFFE ID must
 * be accepted instead.
 */
public record PeerPolicy(Set<String> spiffeIds, String trustDomain) {
  private static final int URI_NAME = 6;
  private static final String SCHEME = "spiffe://";

  public PeerPolicy {
    spiffeIds = Set.copyOf(spiffeIds);
    trustDomain = trustDomain.toLowerCase(Locale.ROOT);
  }

  public static PeerPolicy of(Tls tls) {
    return new PeerPolicy(Set.copyOf(tls.getAcceptedSpiffeIdsList()), tls.getAcceptedTrustDomain());
  }

  /** Whether brokers are identified by SPIFFE ID rather than by host name. */
  public boolean spiffe() {
    return !spiffeIds.isEmpty() || !trustDomain.isEmpty();
  }

  /**
   * Checks the SPIFFE ID of a broker certificate that already chains to the trust bundle.
   *
   * @throws CertificateException if it has no single SPIFFE ID or the ID is not accepted
   */
  public void check(X509Certificate leaf) throws CertificateException {
    String id = spiffeId(leaf);
    if (spiffeIds.contains(id)) {
      return;
    }
    if (!trustDomain.isEmpty() && trustDomain(id).equals(trustDomain)) {
      return;
    }
    throw new CertificateException("broker SPIFFE ID " + id + " is not accepted");
  }

  /** The certificate's SPIFFE ID: its one URI subject alternative name. */
  static String spiffeId(X509Certificate leaf) throws CertificateException {
    Collection<List<?>> names = leaf.getSubjectAlternativeNames();
    List<String> uris = new ArrayList<>();
    if (names != null) {
      for (List<?> name : names) {
        if (name.get(0) instanceof Integer type && type == URI_NAME) {
          uris.add((String) name.get(1));
        }
      }
    }
    if (uris.size() != 1 || !uris.get(0).startsWith(SCHEME)) {
      throw new CertificateException(
          "broker certificate must carry exactly one SPIFFE ID as its URI name, found " + uris);
    }
    return uris.get(0);
  }

  private static String trustDomain(String id) {
    String rest = id.substring(SCHEME.length());
    int slash = rest.indexOf('/');
    return (slash < 0 ? rest : rest.substring(0, slash)).toLowerCase(Locale.ROOT);
  }
}
