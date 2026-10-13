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
 * @param revokeGrace longest a revoked partition takes to be released: during the first half of it
 *     the core still sends what lies below the newest record it had sent, and until its end it
 *     waits for the acknowledgements of what is in flight; zero releases at once
 * @param redeliveryBackoff how long a record waits before it is sent again after its first failed
 *     attempt; the wait doubles with each further failed attempt, up to {@code
 *     redeliveryBackoffMax}. Zero sends again at once
 * @param redeliveryBackoffMax the longest such wait; not less than {@code redeliveryBackoff}
 * @param deadLetterRetry how long the core waits before it asks again for a dead letter whose write
 *     failed; the wait doubles with each further failure, up to {@code deadLetterRetryMax}. Zero
 *     asks again at once
 * @param deadLetterRetryMax the longest such wait; not less than {@code deadLetterRetry}
 */
public record DispatchSettings(
    Ordering ordering,
    Duration maxProcessing,
    int maxAttempts,
    OnExhausted onExhausted,
    long window,
    int maxBufferedRecords,
    long maxBufferedBytes,
    Duration revokeGrace,
    Duration redeliveryBackoff,
    Duration redeliveryBackoffMax,
    Duration deadLetterRetry,
    Duration deadLetterRetryMax) {

  /** Settings under which nothing waits: a failed record, and a failed dead letter, go at once. */
  public DispatchSettings(
      Ordering ordering,
      Duration maxProcessing,
      int maxAttempts,
      OnExhausted onExhausted,
      long window,
      int maxBufferedRecords,
      long maxBufferedBytes,
      Duration revokeGrace) {
    this(
        ordering,
        maxProcessing,
        maxAttempts,
        onExhausted,
        window,
        maxBufferedRecords,
        maxBufferedBytes,
        revokeGrace,
        Duration.ZERO,
        Duration.ZERO,
        Duration.ZERO,
        Duration.ZERO);
  }

  /** These settings with another wait before a redelivery. */
  public DispatchSettings withRedeliveryBackoff(Duration first, Duration longest) {
    return new DispatchSettings(
        ordering,
        maxProcessing,
        maxAttempts,
        onExhausted,
        window,
        maxBufferedRecords,
        maxBufferedBytes,
        revokeGrace,
        first,
        longest,
        deadLetterRetry,
        deadLetterRetryMax);
  }

  /** These settings with another wait before a dead letter is written again. */
  public DispatchSettings withDeadLetterRetry(Duration first, Duration longest) {
    return new DispatchSettings(
        ordering,
        maxProcessing,
        maxAttempts,
        onExhausted,
        window,
        maxBufferedRecords,
        maxBufferedBytes,
        revokeGrace,
        redeliveryBackoff,
        redeliveryBackoffMax,
        first,
        longest);
  }

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
    checkWait("redeliveryBackoff", redeliveryBackoff, redeliveryBackoffMax);
    checkWait("deadLetterRetry", deadLetterRetry, deadLetterRetryMax);
  }

  private static void checkWait(String name, Duration first, Duration longest) {
    if (first == null || longest == null || first.isNegative() || first.compareTo(longest) > 0) {
      throw new IllegalArgumentException(
          name + " must not be negative, nor more than " + name + "Max");
    }
  }
}
