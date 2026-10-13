package io.github.bpalermo.odradek.server.dispatch;

import com.google.protobuf.ByteString;
import java.util.List;

/**
 * Something that happened, told to a {@link DispatchCore}.
 *
 * <p>Receivers are named by an identifier the caller chooses, one per {@code Deliver} stream; it
 * must not be reused for another stream while the core still knows the first.
 *
 * @param <R> what the caller attaches to a record, handed back when the record is to be sent
 */
public sealed interface DispatchEvent<R> {

  /**
   * The replica owns a partition and will fetch it from {@code position}. Fetching is not paused. A
   * partition the core already has is treated as lost first.
   */
  record PartitionAssigned<R>(int partition, long position) implements DispatchEvent<R> {}

  /**
   * The replica is giving partitions up and can still commit for them. The core stops sending their
   * records and answers each with one {@link DispatchAction.PartitionReleased}, at once or after
   * the hand-over wait.
   */
  record PartitionsRevoked<R>(List<Integer> partitions) implements DispatchEvent<R> {
    public PartitionsRevoked {
      partitions = List.copyOf(partitions);
    }
  }

  /** The replica no longer owns partitions and cannot commit for them. */
  record PartitionsLost<R>(List<Integer> partitions) implements DispatchEvent<R> {
    public PartitionsLost {
      partitions = List.copyOf(partitions);
    }
  }

  /**
   * One fetched record.
   *
   * @param offset its offset
   * @param key its key as stored, or null when it has none; an empty key is a key
   * @param sizeBytes what it counts for against {@link DispatchSettings#maxBufferedBytes}
   * @param payload handed back in {@link DispatchAction.Deliver} and {@link
   *     DispatchAction.DeadLetter}
   */
  record Fetched<R>(long offset, ByteString key, int sizeBytes, R payload) {}

  /**
   * Records fetched from one partition, in offset order and after everything fetched before.
   * Records of a partition the core does not have, or that is being handed over, are dropped.
   */
  record RecordsFetched<R>(int partition, List<Fetched<R>> records) implements DispatchEvent<R> {
    public RecordsFetched {
      records = List.copyOf(records);
    }
  }

  /** A receiver connected. It has no credit yet. */
  record ReceiverJoined<R>(String receiver) implements DispatchEvent<R> {}

  /**
   * A receiver's stream ended. What was in flight to it is delivered again, under the same attempt
   * numbers: a receiver that leaves does not use up an attempt.
   */
  record ReceiverLeft<R>(String receiver) implements DispatchEvent<R> {}

  /** A receiver allows {@code records} more deliveries. Grants add up. */
  record CreditsGranted<R>(String receiver, long records) implements DispatchEvent<R> {}

  /**
   * A receiver acknowledged the inclusive offset range of one partition. It applies to the records
   * of the range that this receiver was sent and that are still unacknowledged, and to nothing
   * else.
   */
  record Acknowledged<R>(String receiver, int partition, long firstOffset, long lastOffset)
      implements DispatchEvent<R> {}

  /**
   * A receiver gave one record back: a failed attempt. Ignored unless the receiver holds it.
   *
   * @param reason what the receiver said of it, handed back with {@link DispatchAction.DeadLetter}
   *     if this was the record's last attempt; null or empty when it said nothing
   */
  record Rejected<R>(String receiver, int partition, long offset, String reason)
      implements DispatchEvent<R> {}

  /** The dead letter asked for by {@link DispatchAction.DeadLetter} is written. */
  record DeadLetterWritten<R>(int partition, long offset) implements DispatchEvent<R> {}

  /**
   * The dead letter could not be written. The core asks again after a wait that doubles with each
   * failure ({@link DispatchSettings#deadLetterRetry}), for as long as it owns the partition.
   */
  record DeadLetterFailed<R>(int partition, long offset) implements DispatchEvent<R> {}

  /**
   * Time passed, and nothing else happened. Processing deadlines, the waits before a redelivery and
   * before a dead letter is written again, and the hand-over wait are checked on this event only;
   * {@link DispatchCore#nextDeadlineNanos()} says when it is next needed.
   */
  record TimePassed<R>() implements DispatchEvent<R> {}
}
