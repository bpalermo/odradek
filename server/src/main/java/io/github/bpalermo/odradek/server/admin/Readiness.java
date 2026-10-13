package io.github.bpalermo.odradek.server.admin;

import io.github.bpalermo.odradek.server.tls.TlsSources;
import java.time.Instant;
import java.util.List;

/**
 * Whether the server can do its work right now, and if not, why.
 *
 * <p>Not ready while starting (which includes waiting for the first TLS credentials and for every
 * required cluster to answer), while a certificate in use has expired (a cluster's, or that of the
 * delivery port on either of its ends), and once shutdown has begun. A cluster that stops answering
 * after startup does not change it.
 */
public final class Readiness {
  /** What startup or shutdown is doing; null between the two. */
  private volatile String phase = "starting";

  private volatile TlsSources tls = TlsSources.none();

  /** Guarded by this. */
  private boolean stopped;

  /** Names the startup step in progress. */
  public void starting(String step) {
    phase = "starting: " + step;
  }

  /** The server is listening, with these credentials. Not after {@link #stopping}. */
  public synchronized void started(TlsSources credentials) {
    if (stopped) {
      return;
    }
    tls = credentials;
    phase = null;
  }

  /** Shutdown has begun, for good: a startup that completes after this changes nothing. */
  public synchronized void stopping() {
    stopped = true;
    phase = "stopping";
  }

  /** Why the server is not ready at {@code now}; empty when it is. */
  public List<String> problems(Instant now) {
    String current = phase;
    if (current != null) {
      return List.of(current);
    }
    return tls.problems(now);
  }
}
