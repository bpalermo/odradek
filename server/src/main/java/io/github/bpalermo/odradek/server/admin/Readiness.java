package io.github.bpalermo.odradek.server.admin;

import io.github.bpalermo.odradek.server.tls.TlsSources;
import java.time.Instant;
import java.util.List;

/**
 * Whether the server can do its work right now, and if not, why.
 *
 * <p>Not ready while starting (which includes waiting for the first TLS credentials), while any
 * cluster's client certificate has expired, and once shutdown has begun.
 */
public final class Readiness {
  /** What startup or shutdown is doing; null between the two. */
  private volatile String phase = "starting";

  private volatile TlsSources tls = TlsSources.none();

  /** Names the startup step in progress. */
  public void starting(String step) {
    phase = "starting: " + step;
  }

  /** The server is listening, with these credentials. */
  public void started(TlsSources credentials) {
    tls = credentials;
    phase = null;
  }

  /** Shutdown has begun. */
  public void stopping() {
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
