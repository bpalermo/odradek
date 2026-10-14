package io.github.bpalermo.odradek.e2eclient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The consumer's bookkeeping, with no I/O: what was delivered, in which order, how often, and what
 * that says about the run.
 *
 * <p>Records are tracked per run and key by {@code seq}. Each delivery is classified when it
 * arrives:
 *
 * <ul>
 *   <li><b>First delivery</b> of a {@code seq}. In order when it is one above the highest seen of
 *       its key; a <b>gap</b> when it is further above (the ones in between were not seen); a
 *       <b>reorder</b> when it is below the highest seen (it arrived after a later one), whose
 *       depth is how far below.
 *   <li><b>Redelivery after a failure</b>: the record was delivered before and this process failed
 *       it on purpose (or never finished it). Expected; neither a duplicate nor a reorder.
 *   <li><b>Duplicate</b>: the record was delivered before and processed. Its first and its new
 *       delivery times are kept.
 * </ul>
 *
 * <p>Thread-safe.
 */
public final class ConsumerLedger {
  /** The largest {@code seq} tracked. A record above it is malformed. */
  public static final long MAX_SEQ = 50_000_000L;

  /** How many duplicates are listed in the results; all of them are counted. */
  static final int DETAIL_LIMIT = 1000;

  private static final byte UNSEEN = 0;
  private static final byte HANDLING = 1;
  private static final byte FAILED = 2;
  private static final byte PROCESSED = 3;

  /** How a delivery was classified. */
  public enum Kind {
    FIRST,
    REDELIVERY_AFTER_FAILURE,
    DUPLICATE
  }

  /**
   * A delivery as the ledger sees it.
   *
   * @param kind its classification
   * @param failuresSoFar how many times this process has failed the record already
   */
  public record Delivery(Kind kind, int failuresSoFar) {}

  /** What is known of one key of one run. */
  private static final class KeyState {
    final String runId;
    final String key;
    byte[] state = new byte[64];
    long[] firstDeliveredMs = new long[64];
    final Map<Long, Integer> failures = new HashMap<>();
    long highestSeen;
    long delivered;
    long processed;
    long duplicates;
    long gaps;
    long reorders;
    long longestReorder;
    long expectedRedeliveries;

    KeyState(String runId, String key) {
      this.runId = runId;
      this.key = key;
    }

    void ensure(long seq) {
      if (seq < state.length) {
        return;
      }
      int size = state.length;
      while (size <= seq) {
        size *= 2;
      }
      state = Arrays.copyOf(state, size);
      firstDeliveredMs = Arrays.copyOf(firstDeliveredMs, size);
    }
  }

  private final Events events;
  private final boolean soleConsumer;
  private final Map<String, KeyState> keys = new TreeMap<>();
  private final List<Map<String, Object>> duplicateDetails = new ArrayList<>();
  private final Map<String, Long> consumerFailures = new TreeMap<>();
  private long delivered;
  private long processed;
  private long duplicates;
  private long gaps;
  private long reorders;
  private long expectedRedeliveries;
  private long deliberateFailures;
  private long checksumFailures;
  private long malformed;
  private long foreignRun;
  private long consumerRestarts;
  private long latencyCount;
  private long latencySumMs;
  private long latencyMaxMs;

  /**
   * @param events where anomalies are reported, one event each
   * @param soleConsumer whether this process should see every record of the keys it sees; when not,
   *     gaps are counted and not reported
   */
  public ConsumerLedger(Events events, boolean soleConsumer) {
    this.events = events;
    this.soleConsumer = soleConsumer;
  }

  /**
   * A record was handed to the handler. Call before deciding what to do with it.
   *
   * @throws IllegalArgumentException if {@code seq} is not between 1 and {@link #MAX_SEQ}
   */
  public synchronized Delivery delivered(
      String runId, String key, long seq, long producerTs, long nowMs) {
    if (seq < 1 || seq > MAX_SEQ) {
      throw new IllegalArgumentException("seq " + seq + " is not between 1 and " + MAX_SEQ);
    }
    KeyState k = keys.computeIfAbsent(runId + "/" + key, _ -> new KeyState(runId, key));
    delivered++;
    k.delivered++;
    k.ensure(seq);
    int index = (int) seq;
    int failures = k.failures.getOrDefault(seq, 0);
    switch (k.state[index]) {
      case PROCESSED -> {
        duplicates++;
        k.duplicates++;
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("runId", runId);
        detail.put("key", key);
        detail.put("seq", seq);
        detail.put("firstDeliveredMs", k.firstDeliveredMs[index]);
        detail.put("deliveredAgainMs", nowMs);
        if (duplicateDetails.size() < DETAIL_LIMIT) {
          duplicateDetails.add(detail);
        }
        events.emit("anomaly", withType("duplicate", detail));
        return new Delivery(Kind.DUPLICATE, failures);
      }
      case FAILED, HANDLING -> {
        expectedRedeliveries++;
        k.expectedRedeliveries++;
        k.state[index] = HANDLING;
        return new Delivery(Kind.REDELIVERY_AFTER_FAILURE, failures);
      }
      default -> {
        k.firstDeliveredMs[index] = nowMs;
        long latency = Math.max(0, nowMs - producerTs);
        latencyCount++;
        latencySumMs += latency;
        latencyMaxMs = Math.max(latencyMaxMs, latency);
        if (seq > k.highestSeen + 1) {
          gaps++;
          k.gaps++;
          if (soleConsumer) {
            Map<String, Object> detail = record(runId, key, seq);
            detail.put("expected", k.highestSeen + 1);
            detail.put("missing", seq - k.highestSeen - 1);
            events.emit("anomaly", withType("gap", detail));
          }
        } else if (seq < k.highestSeen) {
          long depth = k.highestSeen - seq;
          reorders++;
          k.reorders++;
          k.longestReorder = Math.max(k.longestReorder, depth);
          Map<String, Object> detail = record(runId, key, seq);
          detail.put("highestSeen", k.highestSeen);
          detail.put("depth", depth);
          events.emit("anomaly", withType("reorder", detail));
        }
        k.highestSeen = Math.max(k.highestSeen, seq);
        k.state[index] = HANDLING;
        return new Delivery(Kind.FIRST, failures);
      }
    }
  }

  /** The handler failed the record on purpose: it is not acknowledged and will come back. */
  public synchronized void failed(String runId, String key, long seq) {
    KeyState k = known(runId, key, seq);
    k.state[(int) seq] = FAILED;
    k.failures.merge(seq, 1, Integer::sum);
    deliberateFailures++;
  }

  /** The handler processed the record: it is acknowledged. */
  public synchronized void processed(String runId, String key, long seq) {
    KeyState k = known(runId, key, seq);
    if (k.state[(int) seq] != PROCESSED) {
      k.processed++;
      processed++;
    }
    k.state[(int) seq] = PROCESSED;
  }

  private KeyState known(String runId, String key, long seq) {
    KeyState k = keys.get(runId + "/" + key);
    if (k == null || seq < 1 || seq >= k.state.length || k.state[(int) seq] == UNSEEN) {
      throw new IllegalStateException(runId + "/" + key + "/" + seq + " was not delivered");
    }
    return k;
  }

  /** A record whose checksum does not match its fields. */
  public synchronized void checksumFailure(Map<String, Object> where) {
    checksumFailures++;
    events.emit("anomaly", withType("checksum", where));
  }

  /** A record that is not one of this program's. */
  public synchronized void malformed(Map<String, Object> where) {
    malformed++;
    events.emit("anomaly", withType("malformed", where));
  }

  /** A record of another run than the one tracked. */
  public synchronized void foreignRun() {
    foreignRun++;
  }

  /** The consumer stopped because the handler failed a record on purpose, and is started again. */
  public synchronized void restarted() {
    consumerRestarts++;
  }

  /** The consumer stopped for another reason: the server refused or evicted it. */
  public synchronized void consumerFailed(String code, String message) {
    consumerFailures.merge(code, 1L, Long::sum);
    Map<String, Object> detail = new LinkedHashMap<>();
    detail.put("code", code);
    detail.put("message", message);
    events.emit("anomaly", withType("consumer-failed", detail));
  }

  /** Records processed at least once. */
  public synchronized long distinctProcessed() {
    return processed;
  }

  /** The results, as served on {@code /results}. */
  public synchronized Map<String, Object> results() {
    long missing = 0;
    long unresolved = 0;
    List<Map<String, Object>> perKey = new ArrayList<>();
    for (KeyState k : keys.values()) {
      List<long[]> missingRanges = ranges(k, UNSEEN);
      List<long[]> failedRanges = ranges(k, FAILED);
      long keyMissing = count(missingRanges);
      long keyUnresolved = count(failedRanges);
      missing += keyMissing;
      unresolved += keyUnresolved;
      Map<String, Object> out = new LinkedHashMap<>();
      out.put("runId", k.runId);
      out.put("key", k.key);
      out.put("highestSeen", k.highestSeen);
      out.put("delivered", k.delivered);
      out.put("processed", k.processed);
      out.put("duplicates", k.duplicates);
      out.put("gaps", k.gaps);
      out.put("missing", keyMissing);
      out.put("reorders", k.reorders);
      out.put("longestReorder", k.longestReorder);
      out.put("expectedRedeliveries", k.expectedRedeliveries);
      out.put("unresolvedFailures", keyUnresolved);
      out.put("processedRanges", json(ranges(k, PROCESSED)));
      out.put("missingRanges", json(missingRanges));
      out.put("unresolvedRanges", json(failedRanges));
      perKey.add(out);
    }
    boolean clean =
        duplicates == 0
            && reorders == 0
            && checksumFailures == 0
            && malformed == 0
            && (!soleConsumer || missing == 0);

    Map<String, Object> out = new LinkedHashMap<>();
    out.put("clean", clean);
    out.put("delivered", delivered);
    out.put("processed", processed);
    out.put("duplicates", duplicates);
    out.put("gaps", gaps);
    out.put("missing", missing);
    out.put("reorders", reorders);
    out.put(
        "longestReorder",
        perKey.stream().mapToLong(k -> (long) k.get("longestReorder")).max().orElse(0));
    out.put("expectedRedeliveries", expectedRedeliveries);
    out.put("deliberateFailures", deliberateFailures);
    out.put("unresolvedFailures", unresolved);
    out.put("checksumFailures", checksumFailures);
    out.put("malformed", malformed);
    out.put("foreignRun", foreignRun);
    out.put("consumerRestarts", consumerRestarts);
    out.put("consumerFailures", new TreeMap<>(consumerFailures));
    Map<String, Object> latency = new LinkedHashMap<>();
    latency.put("count", latencyCount);
    latency.put("meanMs", latencyCount == 0 ? 0 : latencySumMs / latencyCount);
    latency.put("maxMs", latencyMaxMs);
    out.put("latency", latency);
    out.put("duplicateDetails", new ArrayList<>(duplicateDetails));
    out.put("duplicateDetailsTruncated", duplicates > duplicateDetails.size());
    out.put("keys", perKey);
    return out;
  }

  /** The ranges [first, last] of seqs from 1 to the highest seen that are in {@code state}. */
  private static List<long[]> ranges(KeyState k, byte state) {
    List<long[]> out = new ArrayList<>();
    long start = -1;
    for (long seq = 1; seq <= k.highestSeen; seq++) {
      boolean in = k.state[(int) seq] == state;
      if (in && start < 0) {
        start = seq;
      } else if (!in && start >= 0) {
        out.add(new long[] {start, seq - 1});
        start = -1;
      }
    }
    if (start >= 0) {
      out.add(new long[] {start, k.highestSeen});
    }
    return out;
  }

  private static long count(List<long[]> ranges) {
    return ranges.stream().mapToLong(range -> range[1] - range[0] + 1).sum();
  }

  private static List<List<Long>> json(List<long[]> ranges) {
    return ranges.stream().map(range -> List.of(range[0], range[1])).toList();
  }

  private static Map<String, Object> record(String runId, String key, long seq) {
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("runId", runId);
    out.put("key", key);
    out.put("seq", seq);
    return out;
  }

  private static Map<String, Object> withType(String type, Map<String, Object> fields) {
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("type", type);
    out.putAll(fields);
    return out;
  }
}
