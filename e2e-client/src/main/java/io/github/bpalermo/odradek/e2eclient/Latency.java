package io.github.bpalermo.odradek.e2eclient;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * The latencies of a process, for {@code /results} and for {@link Metrics}:
 *
 * <ul>
 *   <li><b>produce</b>: from send to acknowledgement, on {@link System#nanoTime()}, every record
 *       acknowledged;
 *   <li><b>end to end, self</b>: from {@code producerNanos} to the start of the handler, for the
 *       records this process produced itself, so on one monotonic clock;
 *   <li><b>end to end, cross</b>: from the wall clock {@code producerTs} to the start of the
 *       handler, for every record: two clocks, which differ by what the nodes' clocks differ by.
 *       For reference only.
 * </ul>
 *
 * <p>End to end is recorded for first deliveries. Thread-safe.
 */
public final class Latency {
  private final Duration window;
  private final Metrics metrics;
  private final LongSupplier nanoClock;
  private final LatencyHistogram produce;
  private final LatencyHistogram self;
  private final LatencyHistogram cross;

  public Latency(Duration window, Metrics metrics) {
    this(window, metrics, System::nanoTime);
  }

  Latency(Duration window, Metrics metrics, LongSupplier nanoClock) {
    this.window = window;
    this.metrics = metrics;
    this.nanoClock = nanoClock;
    this.produce = new LatencyHistogram(window);
    this.self = new LatencyHistogram(window);
    this.cross = new LatencyHistogram(window);
  }

  public Metrics metrics() {
    return metrics;
  }

  /** Send to acknowledgement, in nanoseconds. */
  public void produce(long nanos) {
    produce.record(nanos / 1000, nanoClock.getAsLong());
    metrics.produceLatency(nanos / 1e6);
  }

  /** Send to handler, in nanoseconds of this process's monotonic clock. */
  public void endToEndSelf(long nanos) {
    self.record(nanos / 1000, nanoClock.getAsLong());
    metrics.endToEndLatency(nanos / 1e6, true);
  }

  /** Send to handler, in milliseconds of two wall clocks. */
  public void endToEndCross(long millis) {
    cross.record(millis * 1000, nanoClock.getAsLong());
    metrics.endToEndLatency(millis, false);
  }

  /** The {@code latency} object of {@code /results}. */
  public Map<String, Object> results() {
    long now = nanoClock.getAsLong();
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("unit", "us");
    out.put("windowMs", window.toMillis());
    out.put("produce", produce.results(now));
    Map<String, Object> endToEndSelf = new LinkedHashMap<>();
    endToEndSelf.put("clock", "self");
    endToEndSelf.putAll(self.results(now));
    out.put("endToEndSelf", endToEndSelf);
    Map<String, Object> endToEndCross = new LinkedHashMap<>();
    endToEndCross.put("clock", "cross");
    endToEndCross.put(
        "note", "producerTs on the producer's wall clock to this one's: for reference only");
    endToEndCross.putAll(cross.results(now));
    out.put("endToEndCross", endToEndCross);
    return out;
  }
}
