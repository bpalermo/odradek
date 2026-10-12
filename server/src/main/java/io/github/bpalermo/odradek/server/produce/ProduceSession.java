package io.github.bpalermo.odradek.server.produce;

import io.github.bpalermo.odradek.kafka.v1.ProduceResult;
import java.util.TreeMap;
import java.util.function.Consumer;

/**
 * The deduplication state of one app process: what happened to each sequence number it sent.
 *
 * <p>The table outlives a stream, which is what lets a reconnecting app resend without producing
 * twice. It is bounded by the in-flight window: results below the app's low watermark are dropped.
 */
final class ProduceSession {
  /** What the session knows about a sequence number. */
  enum Seen {
    /** Never seen: the caller must produce it. */
    NEW,
    /** Already handed to Kafka or already answered: nothing to do. */
    DUPLICATE,
    /** At or below a number seen before, with no record of it left. */
    STALE
  }

  private static final class Entry {
    final long bytes;
    ProduceResult result;

    Entry(long bytes) {
      this.bytes = bytes;
    }
  }

  private final TreeMap<Long, Entry> table = new TreeMap<>();
  private long highestSeq;
  private int inFlightRecords;
  private long inFlightBytes;
  private Consumer<ProduceResult> sink;
  private Runnable onDisplaced;
  private long detachedAtNanos = System.nanoTime();

  /**
   * Makes {@code sink} the stream that receives this session's results. A stream attached before is
   * told through its {@code onDisplaced} that it has been replaced.
   */
  void attach(Consumer<ProduceResult> sink, Runnable onDisplaced) {
    Runnable displaced;
    synchronized (this) {
      displaced = this.onDisplaced;
      this.sink = sink;
      this.onDisplaced = onDisplaced;
    }
    if (displaced != null) {
      displaced.run();
    }
  }

  /** Stops sending results to {@code sink}, if it is still the attached stream. */
  synchronized void detach(Consumer<ProduceResult> sink) {
    if (this.sink == sink) {
      this.sink = null;
      this.onDisplaced = null;
      this.detachedAtNanos = System.nanoTime();
    }
  }

  /** Whether no stream is attached, nothing is in flight, and it has been so for {@code nanos}. */
  synchronized boolean idleFor(long nanos) {
    return sink == null && inFlightRecords == 0 && System.nanoTime() - detachedAtNanos >= nanos;
  }

  /** Drops retained results below {@code lowWatermark}. Records still in flight are kept. */
  synchronized void prune(long lowWatermark) {
    table.headMap(lowWatermark).values().removeIf(entry -> entry.result != null);
  }

  /**
   * Looks a sequence number up. For a duplicate that already has a result, the result is sent to
   * the attached stream again; a duplicate still in flight will be answered when it completes.
   */
  Seen check(long seq) {
    ProduceResult resend;
    Consumer<ProduceResult> to;
    synchronized (this) {
      Entry entry = table.get(seq);
      if (entry == null) {
        return seq <= highestSeq ? Seen.STALE : Seen.NEW;
      }
      resend = entry.result;
      to = sink;
    }
    if (resend != null && to != null) {
      to.accept(resend);
    }
    return Seen.DUPLICATE;
  }

  /** Whether one more record of {@code bytes} fits in the window. */
  synchronized boolean fits(long bytes, int maxRecords, long maxBytes) {
    return inFlightRecords < maxRecords && inFlightBytes + bytes <= maxBytes;
  }

  /** Records that {@code seq} is being produced and occupies a slot of the window. */
  synchronized void begin(long seq, long bytes) {
    table.put(seq, new Entry(bytes));
    highestSeq = Math.max(highestSeq, seq);
    inFlightRecords++;
    inFlightBytes += bytes;
  }

  /**
   * Records the final outcome of a sequence number, frees its slot if it held one, and sends the
   * result to the attached stream. With no stream attached the result waits for a resend.
   */
  void complete(ProduceResult result) {
    Consumer<ProduceResult> to;
    synchronized (this) {
      Entry entry = table.get(result.getSeq());
      if (entry == null) {
        entry = new Entry(0);
        table.put(result.getSeq(), entry);
        highestSeq = Math.max(highestSeq, result.getSeq());
      } else if (entry.result == null) {
        inFlightRecords--;
        inFlightBytes -= entry.bytes;
      }
      entry.result = result;
      to = sink;
    }
    if (to != null) {
      to.accept(result);
    }
  }

  synchronized int inFlightRecords() {
    return inFlightRecords;
  }
}
