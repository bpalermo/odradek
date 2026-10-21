package io.github.bpalermo.odradek.e2eclient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.zip.CRC32C;
import org.junit.jupiter.api.Test;

class RecordValueTest {
  @Test
  void encodesAndDecodes() {
    RecordValue value = RecordValue.of("run", "key-1", 42, 1_760_000_000_000L, "pod-0-ab12", 987);
    String json = new String(value.encode(), StandardCharsets.UTF_8);

    assertThat(json)
        .startsWith(
            "{\"runId\":\"run\",\"key\":\"key-1\",\"seq\":42,\"producerTs\":1760000000000,"
                + "\"origin\":\"pod-0-ab12\",\"producerNanos\":987,\"checksum\":\"")
        .matches(".*\"checksum\":\"[0-9a-f]{8}\"}");
    RecordValue decoded = RecordValue.decode(value.encode());
    assertThat(decoded).isEqualTo(value);
    assertThat(decoded.valid()).isTrue();
  }

  @Test
  void aRecordOfAnOlderProducerHasNoOriginAndStillChecksOut() {
    // As the producer wrote it before origin and producerNanos existed.
    String old =
        "{\"runId\":\"run\",\"key\":\"key-1\",\"seq\":42,\"producerTs\":1000,\"checksum\":\""
            + crc32c("run\nkey-1\n42\n1000")
            + "\"}";

    RecordValue decoded = RecordValue.decode(old.getBytes(StandardCharsets.UTF_8));
    assertThat(decoded.origin()).isEmpty();
    assertThat(decoded.producerNanos()).isZero();
    assertThat(decoded.valid()).isTrue();
    // And it is written back the same way.
    assertThat(new String(decoded.encode(), StandardCharsets.UTF_8)).isEqualTo(old);
  }

  @Test
  void aChangedFieldNoLongerMatchesItsChecksum() {
    RecordValue value = RecordValue.of("run", "key-1", 42, 1000, "o", 7);
    String sum = value.checksum();

    assertThat(new RecordValue("run", "key-1", 43, 1000, "o", 7, sum).valid()).isFalse();
    assertThat(new RecordValue("run", "key-2", 42, 1000, "o", 7, sum).valid()).isFalse();
    assertThat(new RecordValue("run2", "key-1", 42, 1000, "o", 7, sum).valid()).isFalse();
    assertThat(new RecordValue("run", "key-1", 42, 1001, "o", 7, sum).valid()).isFalse();
    assertThat(new RecordValue("run", "key-1", 42, 1000, "p", 7, sum).valid()).isFalse();
    assertThat(new RecordValue("run", "key-1", 42, 1000, "o", 8, sum).valid()).isFalse();
    // Dropping the new fields does not make it an older record that checks out.
    assertThat(new RecordValue("run", "key-1", 42, 1000, "", 0, sum).valid()).isFalse();
  }

  @Test
  void theChecksumIsCrc32cOfTheFields() {
    assertThat(RecordValue.checksum("a", "b", 1, 2, "o", 3)).isEqualTo(crc32c("a\nb\n1\n2\no\n3"));
    assertThat(RecordValue.checksum("a", "b", 1, 2, "", 0)).isEqualTo(crc32c("a\nb\n1\n2"));
  }

  private static String crc32c(String text) {
    CRC32C crc = new CRC32C();
    crc.update(text.getBytes(StandardCharsets.UTF_8));
    return String.format(Locale.ROOT, "%08x", crc.getValue());
  }

  @Test
  void refusesWhatIsNotARecordOfThisProgram() {
    for (String text :
        new String[] {
          "not json",
          "[1]",
          "{\"runId\":\"r\",\"key\":\"k\",\"seq\":\"1\",\"producerTs\":1,\"checksum\":\"x\"}",
          "{\"runId\":\"r\",\"key\":\"k\",\"seq\":1,\"producerTs\":1}",
          "{\"runId\":\"r\",\"key\":\"k\",\"seq\":1.5,\"producerTs\":1,\"checksum\":\"x\"}",
          // An origin needs its producerNanos, and must not be empty.
          "{\"runId\":\"r\",\"key\":\"k\",\"seq\":1,\"producerTs\":1,\"origin\":\"o\","
              + "\"checksum\":\"x\"}",
          "{\"runId\":\"r\",\"key\":\"k\",\"seq\":1,\"producerTs\":1,\"origin\":\"\","
              + "\"producerNanos\":1,\"checksum\":\"x\"}"
        }) {
      assertThatThrownBy(() -> RecordValue.decode(text.getBytes(StandardCharsets.UTF_8)))
          .as(text)
          .isInstanceOf(IllegalArgumentException.class);
    }
    assertThatThrownBy(() -> RecordValue.decode(null)).isInstanceOf(IllegalArgumentException.class);
  }
}
