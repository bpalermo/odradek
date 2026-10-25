package io.github.bpalermo.odradek.server.tls;

import io.github.bpalermo.kafka.spiffe.id.SpiffeId;
import io.github.bpalermo.kafka.spiffe.kafka.PeerVerificationStats;
import io.github.bpalermo.kafka.spiffe.x509.PeerRejectedException;
import io.github.bpalermo.kafka.spiffe.x509.SpiffeTrustManager;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/**
 * What the library's verification of one end of the delivery port's peers came to (ADR 9): the
 * peers accepted, and those refused by reason, by the side the server took in the handshake. The
 * library's trust manager calls it on each handshake's thread.
 */
public final class PeerVerifications implements SpiffeTrustManager.Observer {
  private final Counts asServer = new Counts();
  private final Counts asClient = new Counts();

  /** One side's counts. */
  private static final class Counts {
    final LongAdder accepted = new LongAdder();
    final Map<PeerRejectedException.Reason, LongAdder> rejected =
        new EnumMap<>(PeerRejectedException.Reason.class);

    Counts() {
      for (PeerRejectedException.Reason reason : PeerRejectedException.Reason.values()) {
        rejected.put(reason, new LongAdder());
      }
    }

    PeerVerificationStats.Side read() {
      Map<PeerRejectedException.Reason, Long> counts =
          new EnumMap<>(PeerRejectedException.Reason.class);
      rejected.forEach((reason, count) -> counts.put(reason, count.sum()));
      return new PeerVerificationStats.Side(accepted.sum(), counts);
    }
  }

  @Override
  public void accepted(boolean server, SpiffeId id) {
    (server ? asClient : asServer).accepted.increment();
  }

  @Override
  public void rejected(boolean server, PeerRejectedException rejection) {
    (server ? asClient : asServer).rejected.get(rejection.reason()).increment();
  }

  /** Handshakes in which the server was the server: the peers are clients. */
  public PeerVerificationStats.Side server() {
    return asServer.read();
  }

  /** Handshakes in which the server was the client: the peers are servers. */
  public PeerVerificationStats.Side client() {
    return asClient.read();
  }
}
