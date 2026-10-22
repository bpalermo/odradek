package io.github.bpalermo.odradek.server.receive;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.LongConsumer;

/**
 * What the app holds of the topics a dispatcher delivers, and how that is told in the words of the
 * app's protocol, which speaks of assigned partitions. No I/O.
 *
 * <p>The app's pod owns no partition. What stands in for ownership is <b>the stream that last
 * delivered a partition's records</b>:
 *
 * <ul>
 *   <li>A partition is announced ({@code Assigned}) when the first record of it is about to be
 *       handed to the app, under a new epoch. It is never revoked.
 *   <li>It is taken back ({@code Lost}) when the dispatcher releases it or the stream that
 *       delivered it ends: what the app holds of it will be delivered again, and can no longer be
 *       acknowledged. Its next record announces it again, under a new epoch.
 *   <li>A record of it that arrives on <em>another</em> stream means another replica owns it now,
 *       even if the first has not said so yet: the partition is taken back and announced again in
 *       one go.
 * </ul>
 *
 * <p>An epoch therefore names one stream, and every record held under it came from that stream.
 * Epochs count up from 1 over the whole Consume stream, so no two announcements share one.
 *
 * <p>Not thread-safe: one instance belongs to one Consume stream's thread.
 */
final class Holdings {

  /** A partition of a topic. */
  record Partition(String topic, int partition) {}

  /**
   * What the app must be told before records of a partition are handed to it.
   *
   * @param lost the partition was held from another stream: {@code Lost} first
   * @param assigned the partition is announced: {@code Assigned}, with {@code epoch}
   * @param epoch the epoch the records are handed under
   */
  record Announcement(boolean lost, boolean assigned, long epoch) {}

  /** An inclusive range of offsets. */
  record Range(long first, long last) {}

  /**
   * An acknowledgement to pass on.
   *
   * @param stream the stream that delivered the records
   * @param ranges the records acknowledged, as the fewest ranges of consecutive offsets
   */
  record Relay(long stream, List<Range> ranges) {}

  private static final class Owned {
    final long stream;
    final long epoch;

    /** The records the app holds, by offset, with the order they were handed in. */
    final NavigableMap<Long, Long> held = new TreeMap<>();

    Owned(long stream, long epoch) {
      this.stream = stream;
      this.epoch = epoch;
    }
  }

  private final Map<Partition, Owned> owned = new HashMap<>();

  /** When each held record was handed to the app, in the order they were handed. */
  private final NavigableMap<Long, Long> handedNanos = new TreeMap<>();

  private long epochs;
  private long sequence;

  /**
   * Records of a partition are about to be handed to the app from this stream: says what the app
   * must be told first, and under which epoch they go.
   */
  Announcement arriving(String topic, int partition, long stream) {
    Partition key = new Partition(topic, partition);
    Owned current = owned.get(key);
    if (current != null && current.stream == stream) {
      return new Announcement(false, false, current.epoch);
    }
    if (current != null) {
      forget(current);
    }
    Owned announced = new Owned(stream, ++epochs);
    owned.put(key, announced);
    return new Announcement(current != null, true, announced.epoch);
  }

  /**
   * A record was handed to the app. Its partition must have been announced by {@link #arriving}. A
   * record the app already holds is held once, from now.
   */
  void handed(String topic, int partition, long offset, long nowNanos) {
    Owned current = owned.get(new Partition(topic, partition));
    Long before = current.held.put(offset, ++sequence);
    if (before != null) {
      handedNanos.remove(before);
    }
    handedNanos.put(sequence, nowNanos);
  }

  /**
   * The app acknowledged a range of a partition under an epoch. Returns the records of it that the
   * app held, which it no longer does, or null when there is none: the epoch is not the one the
   * partition is held under, or nothing in the range was delivered here and is still held.
   *
   * <p>The ranges never span an offset that was not held. The app's range may: the records between
   * two of this app's went to other pods, and one of them may since have been delivered here again
   * and be on its way, where an acknowledgement spanning it would cover it unprocessed.
   */
  Relay ack(String topic, int partition, long epoch, long first, long last) {
    return ack(topic, partition, epoch, first, last, handed -> {});
  }

  /** As above, telling {@code handedAt} when each record the app no longer holds was handed. */
  Relay ack(String topic, int partition, long epoch, long first, long last, LongConsumer handedAt) {
    Owned current = owned.get(new Partition(topic, partition));
    if (current == null || current.epoch != epoch || first > last) {
      return null;
    }
    NavigableMap<Long, Long> acked = current.held.subMap(first, true, last, true);
    if (acked.isEmpty()) {
      return null;
    }
    List<Range> ranges = new ArrayList<>();
    long start = -1;
    long end = -1;
    for (Map.Entry<Long, Long> record : acked.entrySet()) {
      long offset = record.getKey();
      Long handed = handedNanos.remove(record.getValue());
      if (handed != null) {
        handedAt.accept(handed);
      }
      if (start >= 0 && offset == end + 1) {
        end = offset;
        continue;
      }
      if (start >= 0) {
        ranges.add(new Range(start, end));
      }
      start = offset;
      end = offset;
    }
    ranges.add(new Range(start, end));
    acked.clear();
    return new Relay(current.stream, ranges);
  }

  /**
   * The replica behind a stream no longer owns these partitions of a topic. Returns those the app
   * was holding from that stream, which it must be told are lost.
   */
  List<Integer> released(long stream, String topic, Collection<Integer> partitions) {
    List<Integer> lost = new ArrayList<>();
    for (int partition : partitions) {
      Partition key = new Partition(topic, partition);
      Owned current = owned.get(key);
      if (current != null && current.stream == stream) {
        forget(current);
        owned.remove(key);
        lost.add(partition);
      }
    }
    return lost;
  }

  /** A stream ended. Returns the partitions the app was holding from it, which are lost. */
  List<Partition> gone(long stream) {
    List<Partition> lost = new ArrayList<>();
    for (Iterator<Map.Entry<Partition, Owned>> all = owned.entrySet().iterator(); all.hasNext(); ) {
      Map.Entry<Partition, Owned> entry = all.next();
      if (entry.getValue().stream == stream) {
        forget(entry.getValue());
        all.remove();
        lost.add(entry.getKey());
      }
    }
    lost.sort(
        java.util.Comparator.comparing(Partition::topic).thenComparingInt(Partition::partition));
    return lost;
  }

  /** When the record the app has held longest was handed to it, or -1 when it holds none. */
  long oldestHandedNanos() {
    return handedNanos.isEmpty() ? -1 : handedNanos.firstEntry().getValue();
  }

  /** How many records the app holds. */
  int held() {
    return handedNanos.size();
  }

  private void forget(Owned partition) {
    partition.held.values().forEach(handedNanos::remove);
    partition.held.clear();
  }
}
