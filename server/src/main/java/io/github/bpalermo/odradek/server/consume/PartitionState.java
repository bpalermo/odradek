package io.github.bpalermo.odradek.server.consume;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.TreeMap;
import org.apache.kafka.common.TopicPartition;

/**
 * What the server knows about one owned partition: which delivered records the app has acked, and
 * from that the offset that may be committed.
 *
 * <p>The commit offset only ever moves over records that were delivered and acked, in order, so a
 * record the app has not acked is never committed past. Used by the poll thread only.
 */
final class PartitionState {
  final TopicPartition partition;
  final long epoch;

  /** Delivered records not yet committed past, oldest first: {offset, delivery time in nanos}. */
  private final ArrayDeque<long[]> delivered = new ArrayDeque<>();

  /** Acked offset ranges at or above the head of {@link #delivered}: first to last, inclusive. */
  private final TreeMap<Long, Long> acked = new TreeMap<>();

  private long lastDelivered = -1;
  private long commitOffset = -1;
  private boolean dirty;

  PartitionState(TopicPartition partition, long epoch) {
    this.partition = partition;
    this.epoch = epoch;
  }

  void delivered(long offset, long nowNanos) {
    delivered.add(new long[] {offset, nowNanos});
    lastDelivered = offset;
  }

  /**
   * Marks the inclusive range as processed. Offsets that were never delivered are ignored: an ack
   * must not make the server skip a record the app has not seen.
   */
  void ack(long first, long last) {
    last = Math.min(last, lastDelivered);
    if (delivered.isEmpty() || last < first || last < delivered.peekFirst()[0]) {
      return;
    }
    first = Math.max(first, delivered.peekFirst()[0]);
    Map.Entry<Long, Long> before = acked.floorEntry(first);
    if (before != null && before.getValue() >= first - 1) {
      first = before.getKey();
      last = Math.max(last, before.getValue());
    }
    for (Map.Entry<Long, Long> next = acked.ceilingEntry(first);
        next != null && next.getKey() <= last + 1;
        next = acked.higherEntry(next.getKey())) {
      last = Math.max(last, next.getValue());
      acked.remove(next.getKey());
    }
    acked.put(first, last);

    // Advance over delivered records, not over offsets: a compacted or transactional partition
    // has gaps, and a gap between two acked records is not something left unprocessed.
    Map.Entry<Long, Long> lowest = acked.firstEntry();
    while (!delivered.isEmpty()
        && lowest.getKey() <= delivered.peekFirst()[0]
        && delivered.peekFirst()[0] <= lowest.getValue()) {
      commitOffset = delivered.pollFirst()[0] + 1;
      dirty = true;
    }
    if (lowest.getValue() < commitOffset) {
      acked.remove(lowest.getKey());
    }
  }

  /** Whether the commit offset moved since {@link #committed()} was last called. */
  boolean dirty() {
    return dirty;
  }

  /** The offset to commit: one past the highest record of the acked run starting at the oldest. */
  long commitOffset() {
    return commitOffset;
  }

  void committed() {
    dirty = false;
  }

  /** Delivery time of the oldest record still unacked, or -1 when there is none. */
  long oldestUnackedNanos() {
    return delivered.isEmpty() ? -1 : delivered.peekFirst()[1];
  }
}
