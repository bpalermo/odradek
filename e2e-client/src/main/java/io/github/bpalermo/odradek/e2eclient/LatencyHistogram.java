package io.github.bpalermo.odradek.e2eclient;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import org.HdrHistogram.AbstractHistogram;
import org.HdrHistogram.IntCountsHistogram;

/**
 * One latency, in microseconds: over the whole run, and over a rolling window.
 *
 * <p>The window is kept as {@value #SLOTS} slots of a twelfth of it each, so what it covers is the
 * current slot and the eleven before it: between eleven and twelve twelfths of the window.
 *
 * <p>Thread-safe.
 */
public final class LatencyHistogram {
  static final int SLOTS = 12;

  /** Three significant digits: 1 µs at 1 ms, 1 ms at 1 s. */
  private static final int DIGITS = 3;

  private final long slotNanos;
  private final AbstractHistogram cumulative = new IntCountsHistogram(DIGITS);
  private final AbstractHistogram[] slots = new AbstractHistogram[SLOTS];
  private final long[] slotIndex = new long[SLOTS];
  private long negative;

  public LatencyHistogram(Duration window) {
    this.slotNanos = Math.max(1, window.toNanos() / SLOTS);
    for (int i = 0; i < SLOTS; i++) {
      slots[i] = new IntCountsHistogram(DIGITS);
      slotIndex[i] = Long.MIN_VALUE;
    }
  }

  /**
   * Records one latency. A negative one, which only a comparison of two clocks can give, is counted
   * as such and recorded as 0.
   *
   * @param nowNanos {@link System#nanoTime()}, which places it in the window
   */
  public synchronized void record(long micros, long nowNanos) {
    if (micros < 0) {
      negative++;
      micros = 0;
    }
    cumulative.recordValue(micros);
    slot(nowNanos).recordValue(micros);
  }

  private AbstractHistogram slot(long nowNanos) {
    long index = Math.floorDiv(nowNanos, slotNanos);
    int at = (int) Math.floorMod(index, (long) SLOTS);
    if (slotIndex[at] != index) {
      slots[at].reset();
      slotIndex[at] = index;
    }
    return slots[at];
  }

  /** {@code cumulative} and {@code window}, each {@link #summary}, and {@code negative}. */
  public synchronized Map<String, Object> results(long nowNanos) {
    long current = Math.floorDiv(nowNanos, slotNanos);
    AbstractHistogram window = new IntCountsHistogram(DIGITS);
    for (int i = 0; i < SLOTS; i++) {
      if (slotIndex[i] != Long.MIN_VALUE && current - slotIndex[i] < SLOTS) {
        window.add(slots[i]);
      }
    }
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("cumulative", summary(cumulative));
    out.put("window", summary(window));
    out.put("negative", negative);
    return out;
  }

  /**
   * {@code count}, then {@code p50Us}, {@code p90Us}, {@code p99Us}, {@code p999Us}, {@code maxUs}.
   */
  static Map<String, Object> summary(AbstractHistogram histogram) {
    long count = histogram.getTotalCount();
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("count", count);
    out.put("p50Us", count == 0 ? 0 : histogram.getValueAtPercentile(50));
    out.put("p90Us", count == 0 ? 0 : histogram.getValueAtPercentile(90));
    out.put("p99Us", count == 0 ? 0 : histogram.getValueAtPercentile(99));
    out.put("p999Us", count == 0 ? 0 : histogram.getValueAtPercentile(99.9));
    out.put("maxUs", count == 0 ? 0 : histogram.getMaxValue());
    return out;
  }
}
