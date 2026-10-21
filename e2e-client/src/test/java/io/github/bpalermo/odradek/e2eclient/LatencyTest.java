package io.github.bpalermo.odradek.e2eclient;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class LatencyTest {
  private static final long SECOND = 1_000_000_000L;

  @SuppressWarnings("unchecked")
  private static Map<String, Object> part(Map<String, Object> results, String name) {
    return (Map<String, Object>) results.get(name);
  }

  @Test
  void percentilesAtMicrosecondResolution() {
    LatencyHistogram histogram = new LatencyHistogram(Duration.ofSeconds(60));
    for (long micros = 1; micros <= 1000; micros++) {
      histogram.record(micros, 0);
    }

    Map<String, Object> cumulative = part(histogram.results(0), "cumulative");
    assertThat(cumulative)
        .containsEntry("count", 1000L)
        .containsEntry("p50Us", 500L)
        .containsEntry("p90Us", 900L)
        .containsEntry("p99Us", 990L)
        .containsEntry("p999Us", 999L)
        .containsEntry("maxUs", 1000L);
  }

  @Test
  void theWindowForgetsWhatIsOlderThanIt() {
    LatencyHistogram histogram = new LatencyHistogram(Duration.ofSeconds(60));
    histogram.record(1_500, 0);
    histogram.record(100, 30 * SECOND);

    Map<String, Object> early = histogram.results(31 * SECOND);
    assertThat(part(early, "window")).containsEntry("count", 2L).containsEntry("maxUs", 1_500L);

    // A minute and a slot later the first is out of the window; the run still has it.
    Map<String, Object> later = histogram.results(66 * SECOND);
    assertThat(part(later, "window"))
        .containsEntry("count", 1L)
        .containsEntry("p50Us", 100L)
        .containsEntry("maxUs", 100L);
    assertThat(part(later, "cumulative")).containsEntry("count", 2L).containsEntry("maxUs", 1_500L);

    // And long after, the window is empty.
    Map<String, Object> idle = histogram.results(600 * SECOND);
    assertThat(part(idle, "window"))
        .containsEntry("count", 0L)
        .containsEntry("p99Us", 0L)
        .containsEntry("maxUs", 0L);
  }

  @Test
  void aSlotIsReusedOnceItsTimeHasPassed() {
    LatencyHistogram histogram = new LatencyHistogram(Duration.ofSeconds(12));
    histogram.record(7, 0);
    // Twelve slots of a second: the same slot twelve seconds on, which must start afresh.
    histogram.record(9, 12 * SECOND);

    assertThat(part(histogram.results(12 * SECOND), "window"))
        .containsEntry("count", 1L)
        .containsEntry("maxUs", 9L);
  }

  @Test
  void negativeLatenciesOfTwoClocksAreCountedAndRecordedAsZero() {
    LatencyHistogram histogram = new LatencyHistogram(Duration.ofSeconds(60));
    histogram.record(-3_000, 0);
    histogram.record(2_000, 0);

    Map<String, Object> results = histogram.results(0);
    assertThat(results).containsEntry("negative", 1L);
    assertThat(part(results, "cumulative"))
        .containsEntry("count", 2L)
        .containsEntry("p50Us", 0L)
        .containsEntry("maxUs", 2_000L);
  }

  @Test
  void theThreeLatenciesGoToTheResultsAndTheMetrics() {
    List<String> recorded = new ArrayList<>();
    Metrics metrics =
        new Metrics() {
          @Override
          public void produceLatency(double millis) {
            recorded.add("produce " + millis);
          }

          @Override
          public void endToEndLatency(double millis, boolean self) {
            recorded.add((self ? "self " : "cross ") + millis);
          }

          @Override
          public void sent() {}

          @Override
          public void acked() {}

          @Override
          public void processed() {}

          @Override
          public void close() {}
        };
    AtomicLong now = new AtomicLong(1_000 * SECOND);
    Latency latency = new Latency(Duration.ofSeconds(30), metrics, now::get);
    latency.produce(1_500_000);
    latency.endToEndSelf(1_750_000);
    latency.endToEndCross(1);

    assertThat(recorded).containsExactly("produce 1.5", "self 1.75", "cross 1.0");
    Map<String, Object> results = latency.results();
    assertThat(results).containsEntry("unit", "us").containsEntry("windowMs", 30_000L);
    assertThat(part(part(results, "produce"), "cumulative")).containsEntry("maxUs", 1_500L);
    assertThat(part(results, "endToEndSelf")).containsEntry("clock", "self");
    assertThat(part(part(results, "endToEndSelf"), "window")).containsEntry("maxUs", 1_750L);
    assertThat(part(results, "endToEndCross")).containsEntry("clock", "cross").containsKey("note");
    assertThat(part(part(results, "endToEndCross"), "cumulative")).containsEntry("p50Us", 1_000L);
  }
}
