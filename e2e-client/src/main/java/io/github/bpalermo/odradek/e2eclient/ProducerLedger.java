package io.github.bpalermo.odradek.e2eclient;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The producer's bookkeeping, with no I/O: each record sent and its outcome. A record's outcome is
 * counted once, whichever reports it first; one with none when the run ends is unknown.
 *
 * <p>Records are identified by run, origin (this process), key and {@code seq}: the merge of
 * several pods' results joins each producer's ledger with every consumer's {@code processedRanges}
 * by them.
 *
 * <p>Thread-safe.
 */
public final class ProducerLedger {
  /** Outcome code of a record still unanswered when the producer was closed. */
  public static final String UNANSWERED = "UNANSWERED";

  /** How many failed and unknown records are listed per key; all of them are counted. */
  static final int DETAIL_LIMIT = 1000;

  private static final class KeyState {
    long sent;
    long acked;
    long lastSentSeq;
    long lastAckedSeq;
    final Map<Long, Long> outstanding = new TreeMap<>();
    final List<Map<String, Object>> failed = new ArrayList<>();
    final List<Map<String, Object>> unknown = new ArrayList<>();
    long failedCount;
    long unknownCount;
  }

  private final Events events;
  private final String runId;
  private final String origin;
  private final Map<String, KeyState> keys = new TreeMap<>();
  private final Map<String, Long> failedByCode = new TreeMap<>();
  private final Map<String, Long> unknownByCode = new TreeMap<>();
  private final Map<String, Long> notSentByReason = new TreeMap<>();
  private long sent;
  private long acked;
  private long failed;
  private long unknown;
  private long notSent;

  public ProducerLedger(Events events, String runId, String origin) {
    this.events = events;
    this.runId = runId;
    this.origin = origin;
  }

  /** A record was handed to the producer. */
  public synchronized void sent(String key, long seq, long nowMs) {
    KeyState k = keys.computeIfAbsent(key, _ -> new KeyState());
    sent++;
    k.sent++;
    k.lastSentSeq = seq;
    k.outstanding.put(seq, nowMs);
  }

  /**
   * The producer refused the record just reported {@link #sent}, which was therefore not sent: it
   * is taken back, and its seq is used again by the next record of the key.
   */
  public synchronized void notSent(String key, long seq, String reason) {
    KeyState k = keys.get(key);
    if (k == null || k.outstanding.remove(seq) == null) {
      return;
    }
    sent--;
    k.sent--;
    k.lastSentSeq = seq - 1;
    notSent++;
    notSentByReason.merge(reason, 1L, Long::sum);
  }

  /** Every cluster the topic requires acknowledged the record. */
  public synchronized void acked(String key, long seq) {
    KeyState k = keys.get(key);
    if (k == null || k.outstanding.remove(seq) == null) {
      return;
    }
    acked++;
    k.acked++;
    k.lastAckedSeq = Math.max(k.lastAckedSeq, seq);
  }

  /** The record failed and was not written: {@code code} is the server's error code. */
  public synchronized void failed(String key, long seq, String code, String message) {
    KeyState k = keys.get(key);
    if (k == null || k.outstanding.remove(seq) == null) {
      return;
    }
    failed++;
    k.failedCount++;
    failedByCode.merge(code, 1L, Long::sum);
    Map<String, Object> detail = detail(key, seq, code, message);
    if (k.failed.size() < DETAIL_LIMIT) {
      k.failed.add(detail);
    }
    events.emit("anomaly", withType("produce-failed", detail));
  }

  /** The record may or may not have been written. */
  public synchronized void unknown(String key, long seq, String code, String message) {
    KeyState k = keys.get(key);
    if (k == null || k.outstanding.remove(seq) == null) {
      return;
    }
    recordUnknown(k, key, seq, code, message);
  }

  private void recordUnknown(KeyState k, String key, long seq, String code, String message) {
    unknown++;
    k.unknownCount++;
    unknownByCode.merge(code, 1L, Long::sum);
    Map<String, Object> detail = detail(key, seq, code, message);
    if (k.unknown.size() < DETAIL_LIMIT) {
      k.unknown.add(detail);
    }
    events.emit("anomaly", withType("produce-unknown", detail));
  }

  /** The producer was closed: whatever has no outcome yet is unknown. */
  public synchronized void unanswered() {
    keys.forEach(
        (key, k) -> {
          for (Long seq : List.copyOf(k.outstanding.keySet())) {
            k.outstanding.remove(seq);
            recordUnknown(k, key, seq, UNANSWERED, "no result when the producer was closed");
          }
        });
  }

  public synchronized long sent() {
    return sent;
  }

  /** Records with no outcome yet. */
  public synchronized long outstanding() {
    return keys.values().stream().mapToLong(k -> k.outstanding.size()).sum();
  }

  /** The results, as served on {@code /results}. */
  public synchronized Map<String, Object> results() {
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("clean", failed == 0 && unknown == 0);
    out.put("sent", sent);
    out.put("acked", acked);
    out.put("failed", failed);
    out.put("unknown", unknown);
    out.put("outstanding", outstanding());
    out.put("notSent", notSent);
    out.put("failedByCode", new TreeMap<>(failedByCode));
    out.put("unknownByCode", new TreeMap<>(unknownByCode));
    out.put("notSentByReason", new TreeMap<>(notSentByReason));
    List<Map<String, Object>> perKey = new ArrayList<>();
    keys.forEach(
        (key, k) -> {
          Map<String, Object> entry = new LinkedHashMap<>();
          entry.put("runId", runId);
          entry.put("origin", origin);
          entry.put("key", key);
          entry.put("sent", k.sent);
          entry.put("acked", k.acked);
          entry.put("lastSentSeq", k.lastSentSeq);
          entry.put("lastAckedSeq", k.lastAckedSeq);
          entry.put("failed", k.failedCount);
          entry.put("unknown", k.unknownCount);
          entry.put("failedSeqs", new ArrayList<>(k.failed));
          entry.put("unknownSeqs", new ArrayList<>(k.unknown));
          perKey.add(entry);
        });
    out.put("keys", perKey);
    return out;
  }

  private Map<String, Object> detail(String key, long seq, String code, String message) {
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("runId", runId);
    out.put("origin", origin);
    out.put("key", key);
    out.put("seq", seq);
    out.put("code", code);
    out.put("message", message);
    return out;
  }

  private static Map<String, Object> withType(String type, Map<String, Object> fields) {
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("type", type);
    out.putAll(fields);
    return out;
  }
}
