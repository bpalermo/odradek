package io.github.bpalermo.odradek.client;

import java.util.concurrent.ThreadLocalRandom;

/** Delay between attempts to open a stream: doubling from 100 ms to 5 s, with jitter. */
final class Backoff {
  private static final long INITIAL_MS = 100;
  private static final long MAX_MS = 5000;

  private long nextMs = INITIAL_MS;

  /** The delay before the next attempt, in milliseconds. */
  long next() {
    long delay = nextMs / 2 + ThreadLocalRandom.current().nextLong(nextMs / 2 + 1);
    nextMs = Math.min(MAX_MS, nextMs * 2);
    return delay;
  }

  /** Called when a stream got as far as its handshake reply. */
  void reset() {
    nextMs = INITIAL_MS;
  }
}
