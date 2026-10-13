package io.github.bpalermo.odradek.server.receive;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongUnaryOperator;

/**
 * Delay between attempts to open a stream to the dispatcher: doubling from 100 ms to 5 s, each
 * delay drawn at random from the upper half of its step. It is the backoff the client libraries use
 * towards the server, so that a restarted dispatcher is not met by every sidecar at once.
 */
final class Backoff {
  static final long INITIAL_MS = 100;
  static final long MAX_MS = 5000;

  private final LongUnaryOperator random;
  private long nextMs = INITIAL_MS;

  Backoff() {
    this(bound -> ThreadLocalRandom.current().nextLong(bound));
  }

  /**
   * @param random returns a number from 0 inclusive to its argument exclusive
   */
  Backoff(LongUnaryOperator random) {
    this.random = random;
  }

  /** The delay before the next attempt, in milliseconds. */
  long nextMs() {
    long delay = nextMs / 2 + random.applyAsLong(nextMs / 2 + 1);
    nextMs = Math.min(MAX_MS, nextMs * 2);
    return delay;
  }

  /** Called when a stream got as far as its handshake reply. */
  void reset() {
    nextMs = INITIAL_MS;
  }
}
