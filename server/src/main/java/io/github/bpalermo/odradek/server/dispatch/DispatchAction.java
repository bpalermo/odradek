package io.github.bpalermo.odradek.server.dispatch;

import java.util.List;

/**
 * Something a {@link DispatchCore} wants done. Actions are returned in the order they must be
 * carried out.
 *
 * @param <R> what the caller attached to a record
 */
public sealed interface DispatchAction<R> {

  /**
   * Send one record to a receiver. It uses one of the receiver's credits.
   *
   * <p>Consecutive deliveries to one receiver from one partition may share a batch only while their
   * offsets increase.
   *
   * @param attempt which delivery of the record this is, from 1
   */
  record Deliver<R>(String receiver, int partition, long offset, R payload, int attempt)
      implements DispatchAction<R> {}

  /**
   * The partition's commit position advanced to {@code offset}: every record below it is
   * acknowledged or dead-lettered. Positions of one partition only grow, so a caller that commits
   * less often than this is emitted keeps the last one.
   */
  record Commit<R>(int partition, long offset) implements DispatchAction<R> {}

  /**
   * Write an exhausted record to the dead-letter topic, then answer with {@link
   * DispatchEvent.DeadLetterWritten} or {@link DispatchEvent.DeadLetterFailed}.
   *
   * @param attempts how many times the record was delivered
   */
  record DeadLetter<R>(int partition, long offset, R payload, int attempts)
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
   */
  record PartitionReleased<R>(int partition) implements DispatchAction<R> {}
}
