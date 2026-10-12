package io.github.bpalermo.odradek.server.dispatch;

import java.time.Duration;

/**
 * How one {@link DispatchCore} dispatches: the settings sidecars declare for a group and topic,
 * plus the bounds the dispatcher sets itself.
 *
 * @param ordering which records may be unacknowledged at the same time
 * @param maxProcessing longest a delivered record may stay unacknowledged before the attempt counts
 *     as failed; positive. The value the core starts with: {@link DispatchCore#maxProcessing}
 *     changes it
 * @param maxAttempts how many times a record is delivered before it is exhausted; zero means there
 *     is no limit and {@code onExhausted} never applies
 * @param onExhausted what happens to a record that has used all its attempts
 * @param window largest span, in offsets, between a partition's commit position and its newest
 *     delivered record: a record at offset {@code o} is delivered for the first time only while
 *     {@code o - commit < window}; at least 1
 * @param maxBufferedRecords number of fetched records of one partition that have never been
 *     delivered at which the core asks for fetching to pause; at least 1
 * @param maxBufferedBytes the same bound in bytes, as the sum of the sizes given with the records;
 *     at least 1
 * @param revokeGrace longest a revoked partition waits for the acknowledgements of what is in
 *     flight before it is released; zero releases at once
 */
public record DispatchSettings(
    Ordering ordering,
    Duration maxProcessing,
    int maxAttempts,
    OnExhausted onExhausted,
    long window,
    int maxBufferedRecords,
    long maxBufferedBytes,
    Duration revokeGrace) {

  /** Which records of a partition may be unacknowledged at the same time. */
  public enum Ordering {
    /** No earlier record of the partition with the same key. Records without a key: no rule. */
    KEY,
    /** No earlier record of the partition. */
    PARTITION,
    /** No rule. */
    NONE
  }

  /** What happens to a record that has used all its attempts. */
  public enum OnExhausted {
    /** The record is not delivered again and nothing after it in its partition is sent. */
    BLOCK,
    /** The record is written to a dead-letter topic and then counts as acknowledged. */
    DEAD_LETTER
  }

  public DispatchSettings {
    if (ordering == null || onExhausted == null) {
      throw new IllegalArgumentException("ordering and onExhausted are required");
    }
    if (maxProcessing == null || maxProcessing.isZero() || maxProcessing.isNegative()) {
      throw new IllegalArgumentException("maxProcessing must be positive");
    }
    if (revokeGrace == null || revokeGrace.isNegative()) {
      throw new IllegalArgumentException("revokeGrace must not be negative");
    }
    if (maxAttempts < 0) {
      throw new IllegalArgumentException("maxAttempts must not be negative");
    }
    if (window < 1 || maxBufferedRecords < 1 || maxBufferedBytes < 1) {
      throw new IllegalArgumentException(
          "window, maxBufferedRecords and maxBufferedBytes must be at least 1");
    }
  }
}
