package io.github.bpalermo.odradek.e2eclient;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.zip.CRC32C;

/**
 * The value of every record this program produces: a JSON object
 *
 * <pre>{@code
 * {"runId":"…","key":"key-3","seq":12,"producerTs":1760000000000,
 *  "origin":"pod-a-1f2e3d4c","producerNanos":81234567890123,"checksum":"1a2b3c4d"}
 * }</pre>
 *
 * <p>{@code checksum} is the CRC-32C, as 8 lowercase hex digits, of the UTF-8 bytes of {@code runId
 * + "\n" + key + "\n" + seq + "\n" + producerTs + "\n" + origin + "\n" + producerNanos}. Records
 * written before {@code origin} and {@code producerNanos} existed have neither, and their checksum
 * covers the first four fields only; they are read with an empty origin.
 *
 * @param runId the producer's run
 * @param key the record key, also the Kafka key
 * @param seq position of the record among those of its key and origin in its run, from 1
 * @param producerTs when the producer sent it, in milliseconds since the epoch
 * @param origin the producing process: its pod and a suffix of its own, so a restarted pod is a new
 *     origin. Empty for a record of an older producer
 * @param producerNanos {@link System#nanoTime()} when the producer sent it: comparable only within
 *     the process named by {@code origin}. 0 for a record of an older producer
 * @param checksum as carried by the record; {@link #valid()} says whether it matches
 */
public record RecordValue(
    String runId,
    String key,
    long seq,
    long producerTs,
    String origin,
    long producerNanos,
    String checksum) {
  /** Builds a value with the checksum of its fields. */
  public static RecordValue of(
      String runId, String key, long seq, long producerTs, String origin, long producerNanos) {
    return new RecordValue(
        runId,
        key,
        seq,
        producerTs,
        origin,
        producerNanos,
        checksum(runId, key, seq, producerTs, origin, producerNanos));
  }

  static String checksum(
      String runId, String key, long seq, long producerTs, String origin, long producerNanos) {
    String covered = runId + "\n" + key + "\n" + seq + "\n" + producerTs;
    if (!origin.isEmpty()) {
      covered += "\n" + origin + "\n" + producerNanos;
    }
    CRC32C crc = new CRC32C();
    crc.update(covered.getBytes(StandardCharsets.UTF_8));
    return String.format(Locale.ROOT, "%08x", crc.getValue());
  }

  /** Whether the checksum matches the other fields. */
  public boolean valid() {
    return checksum(runId, key, seq, producerTs, origin, producerNanos).equals(checksum);
  }

  public byte[] encode() {
    Map<String, Object> fields = new LinkedHashMap<>();
    fields.put("runId", runId);
    fields.put("key", key);
    fields.put("seq", seq);
    fields.put("producerTs", producerTs);
    if (!origin.isEmpty()) {
      fields.put("origin", origin);
      fields.put("producerNanos", producerNanos);
    }
    fields.put("checksum", checksum);
    return Json.write(fields).getBytes(StandardCharsets.UTF_8);
  }

  /**
   * Reads a value. The checksum is not verified here: see {@link #valid()}.
   *
   * @throws IllegalArgumentException if it is not such an object
   */
  public static RecordValue decode(byte[] bytes) {
    if (bytes == null) {
      throw new IllegalArgumentException("no value (a tombstone)");
    }
    if (!(Json.parse(new String(bytes, StandardCharsets.UTF_8)) instanceof Map<?, ?> fields)) {
      throw new IllegalArgumentException("not a JSON object");
    }
    String origin = "";
    long producerNanos = 0;
    if (fields.containsKey("origin")) {
      origin = text(fields, "origin");
      if (origin.isEmpty()) {
        throw new IllegalArgumentException("field origin is empty");
      }
      producerNanos = number(fields, "producerNanos");
    }
    return new RecordValue(
        text(fields, "runId"),
        text(fields, "key"),
        number(fields, "seq"),
        number(fields, "producerTs"),
        origin,
        producerNanos,
        text(fields, "checksum"));
  }

  private static String text(Map<?, ?> fields, String name) {
    if (fields.get(name) instanceof String text) {
      return text;
    }
    throw new IllegalArgumentException("field " + name + " is missing or not a string");
  }

  private static long number(Map<?, ?> fields, String name) {
    if (fields.get(name) instanceof Long number) {
      return number;
    }
    throw new IllegalArgumentException("field " + name + " is missing or not an integer");
  }
}
