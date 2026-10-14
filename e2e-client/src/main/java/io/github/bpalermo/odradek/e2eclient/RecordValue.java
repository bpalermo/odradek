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
 * {"runId":"…","key":"key-3","seq":12,"producerTs":1760000000000,"checksum":"1a2b3c4d"}
 * }</pre>
 *
 * <p>{@code checksum} is the CRC-32C, as 8 lowercase hex digits, of the UTF-8 bytes of {@code runId
 * + "\n" + key + "\n" + seq + "\n" + producerTs}.
 *
 * @param runId the producer's run
 * @param key the record key, also the Kafka key
 * @param seq position of the record among those of its key in its run, from 1
 * @param producerTs when the producer sent it, in milliseconds since the epoch
 * @param checksum as carried by the record; {@link #valid()} says whether it matches
 */
public record RecordValue(String runId, String key, long seq, long producerTs, String checksum) {
  /** Builds a value with the checksum of its fields. */
  public static RecordValue of(String runId, String key, long seq, long producerTs) {
    return new RecordValue(runId, key, seq, producerTs, checksum(runId, key, seq, producerTs));
  }

  static String checksum(String runId, String key, long seq, long producerTs) {
    CRC32C crc = new CRC32C();
    crc.update(
        (runId + "\n" + key + "\n" + seq + "\n" + producerTs).getBytes(StandardCharsets.UTF_8));
    return String.format(Locale.ROOT, "%08x", crc.getValue());
  }

  /** Whether the checksum matches the other fields. */
  public boolean valid() {
    return checksum(runId, key, seq, producerTs).equals(checksum);
  }

  public byte[] encode() {
    Map<String, Object> fields = new LinkedHashMap<>();
    fields.put("runId", runId);
    fields.put("key", key);
    fields.put("seq", seq);
    fields.put("producerTs", producerTs);
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
    return new RecordValue(
        text(fields, "runId"),
        text(fields, "key"),
        number(fields, "seq"),
        number(fields, "producerTs"),
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
