package io.github.bpalermo.odradek.server.tls;

import io.github.bpalermo.kafka.spiffe.workload.X509MaterialSource;
import io.github.bpalermo.kafka.spiffe.workload.X509Source;
import io.github.bpalermo.kafka.spiffe.workload.X509SourceRegistry;
import io.github.bpalermo.kafka.spiffe.x509.X509Bundle;
import io.github.bpalermo.kafka.spiffe.x509.X509Context;
import java.io.Closeable;
import java.nio.file.Path;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Credentials from the SPIFFE Workload API (typically a SPIRE agent): a handle on kafka-spiffe's
 * shared source of the socket (ADR 9). The source keeps one long-lived stream, validates every
 * update before it replaces the snapshot, and reconnects with a growing pause; the private key
 * never touches disk.
 *
 * <p>The source is the one {@code SpiffeConfig.acquireSource()} names for the keys {@link
 * TlsSources#apply} gives a cluster's Kafka clients (the socket, no SVID selection, the library's
 * MBeans off), so whatever reads a socket here, the delivery port or a cluster's clients, reads one
 * stream.
 */
public final class SpiffeCredentialSource implements CredentialSource, Closeable {
  private final X509MaterialSource source;
  private volatile Converted converted;

  /** A snapshot and the credentials made of it. */
  private record Converted(X509Context snapshot, Credentials credentials) {}

  private SpiffeCredentialSource(X509MaterialSource source) {
    this.source = source;
  }

  /**
   * The settings of a socket's shared source: the first SVID, every bundle, the default backoff.
   */
  static X509Source.Settings settings(String socket) {
    return X509Source.Settings.of(Path.of(socket));
  }

  /**
   * Takes a handle on the socket's shared source and waits for its first snapshot.
   *
   * @param socket path of the Workload API's Unix socket
   * @throws CredentialException if none arrives within {@code timeout}
   */
  public static SpiffeCredentialSource open(String socket, Duration timeout)
      throws CredentialException, InterruptedException {
    X509MaterialSource handle = X509SourceRegistry.shared(null).acquire(settings(socket));
    try {
      handle.awaitFirst(timeout);
    } catch (TimeoutException e) {
      handle.close();
      throw new CredentialException(
          "no X.509-SVID from the Workload API at "
              + socket
              + " within "
              + timeout
              + ": "
              + e.getMessage(),
          e);
    } catch (InterruptedException | RuntimeException e) {
      handle.close();
      throw e;
    }
    return new SpiffeCredentialSource(handle);
  }

  /** The newest snapshot, as the library has it. */
  public X509Context snapshot() {
    return source.current();
  }

  /** The source's counts and whether its stream is open. */
  public X509MaterialSource.Stats stats() {
    return source.stats();
  }

  @Override
  public Credentials current() {
    X509Context snapshot = source.current();
    Converted latest = converted;
    if (latest == null || latest.snapshot() != snapshot) {
      latest = new Converted(snapshot, credentials(snapshot));
      converted = latest;
    }
    return latest.credentials();
  }

  /** Updates that replaced the snapshot, the first included. */
  @Override
  public long reloads() {
    return source.stats().updates();
  }

  /** Updates rejected, plus the times the stream ended or could not be opened. */
  @Override
  public long failures() {
    X509MaterialSource.Stats stats = source.stats();
    return stats.rejectedUpdates() + stats.streamErrors();
  }

  @Override
  public void close() {
    source.close();
  }

  /**
   * A snapshot as {@link Credentials}: the SVID's key and chain, the bundle of its own trust domain
   * and a bundle per trust domain. The library validated it before it became the snapshot, own
   * bundle included.
   */
  static Credentials credentials(X509Context snapshot) {
    Map<String, List<X509Certificate>> bundles = new HashMap<>();
    for (X509Bundle bundle : snapshot.bundles().bundles()) {
      bundles.put(bundle.trustDomain().name(), bundle.authorities());
    }
    List<X509Certificate> trust =
        bundles.getOrDefault(snapshot.svid().id().trustDomain().name(), List.of());
    return new Credentials(snapshot.svid().key(), snapshot.svid().chain(), trust, bundles);
  }
}
