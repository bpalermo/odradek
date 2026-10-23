package io.github.bpalermo.odradek.server.dispatch;

import java.util.List;

/**
 * Something a {@link DispatchCore} wants done. Actions are returned in the order they must be
 * carried out.
 *
 * @param <R> what the caller attached to a record
 */
public sealed interface DispatchAction<R> {

  /** Why a record is sent again, or was given up on. */
  enum Cause {
    /** Nothing happened to it before: a first delivery. */
    NONE,
    /** The receiver that had it gave it back. */
    REJECTED,
    /** Its processing deadline passed. */
    DEADLINE,
    /** The receiver that had it left. Not a failed attempt. */
    RECEIVER_LEFT,
    /** The receiver that had it gave it back unread (ADR 7). Not a failed attempt. */
    RETURNED
  }

  /**
   * Send one record to a receiver. It uses one of the receiver's credits.
   *
   * <p>Consecutive deliveries to one receiver from one partition may share a batch only while their
   * offsets increase.
   *
   * @param attempt which attempt at the record this is, from 1: one more than the attempts that
   *     failed. A record sent again because its receiver left, or gave it back unread, is sent
   *     under the same number
   * @param cause why it is sent again; {@link Cause#NONE} when this core never sent it before
   */
  record Deliver<R>(
      String receiver, int partition, long offset, R payload, int attempt, Cause cause)
      implements DispatchAction<R> {

    /** A first delivery. */
    public Deliver(String receiver, int partition, long offset, R payload, int attempt) {
      this(receiver, partition, offset, payload, attempt, Cause.NONE);
    }
  }

  /**
   * The partition's commit position advanced to {@code offset}: every record below it is
   * acknowledged or dead-lettered. Positions of one partition only grow, so a caller that commits
   * less often than this is emitted keeps the last one.
   */
  record Commit<R>(int partition, long offset) implements DispatchAction<R> {}

  /**
   * Write an exhausted record to the dead-letter topic, then answer with {@link
   * DispatchEvent.DeadLetterWritten} or {@link DispatchEvent.DeadLetterFailed}. Until the first of
   * these the record is unacknowledged: the commit position stays at or below it.
   *
   * @param attempts how many attempts at the record failed: all it had
   * @param cause how the last of them failed: {@link Cause#REJECTED} or {@link Cause#DEADLINE}
   * @param detail what the receiver said when it rejected the record; null when it said nothing,
   *     and after a missed deadline
   * @param retries how many writes of this record failed before this one; 0 the first time
   */
  record DeadLetter<R>(
      int partition, long offset, R payload, int attempts, Cause cause, String detail, int retries)
      implements DispatchAction<R> {}

  /**
   * The record at {@code offset} used all its attempts and blocks its partition. Emitted for each
   * record that does; the lowest such offset is where the partition stops.
   */
  record PartitionBlocked<R>(int partition, long offset) implements DispatchAction<R> {}

  /** No record blocks the partition any more: the last one was acknowledged late. */
  record PartitionUnblocked<R>(int partition) implements DispatchAction<R> {}

  /** Stop fetching the partition: enough of it is buffered. */
  record PauseFetching<R>(int partition) implements DispatchAction<R> {}

  /** Fetch the partition again. */
  record ResumeFetching<R>(int partition) implements DispatchAction<R> {}

  /**
   * Tell a receiver that the replica gave these partitions up, so that it drops the records of them
   * it holds unacknowledged. The core has already forgotten them.
   */
  record Drop<R>(String receiver, List<Integer> partitions) implements DispatchAction<R> {
    public Drop {
      partitions = List.copyOf(partitions);
    }
  }

  /**
   * The hand-over of a revoked partition is complete: once the last {@link Commit} for it is
   * committed, the partition can be given up. Not emitted for a lost partition.
   *
   * @param inTime whether everything sent was answered before the hand-over wait ended; false when
   *     the wait ended with records still in flight or a dead letter still being written, which the
   *     next owner then sends again while a receiver may still be working on them. It says nothing
   *     of records that were held back and could not be sent in the time there was: nobody has
   *     those
   * @param resent how many records this core had sent and leaves at or above the commit position:
   *     what the next owner sends again, whether they were acknowledged here or not
   */
  record PartitionReleased<R>(int partition, boolean inTime, int resent)
      implements DispatchAction<R> {}
}
