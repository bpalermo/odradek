package io.github.bpalermo.odradek.e2eclient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class RecordValueTest {
  @Test
  void encodesAndDecodes() {
    RecordValue value = RecordValue.of("run", "key-1", 42, 1_760_000_000_000L);
    String json = new String(value.encode(), StandardCharsets.UTF_8);

    assertThat(json)
        .startsWith(
            "{\"runId\":\"run\",\"key\":\"key-1\",\"seq\":42,\"producerTs\":1760000000000,"
                + "\"checksum\":\"")
        .matches(".*\"checksum\":\"[0-9a-f]{8}\"}");
    RecordValue decoded = RecordValue.decode(value.encode());
    assertThat(decoded).isEqualTo(value);
    assertThat(decoded.valid()).isTrue();
  }

  @Test
  void aChangedFieldNoLongerMatchesItsChecksum() {
    RecordValue value = RecordValue.of("run", "key-1", 42, 1000);

    assertThat(new RecordValue("run", "key-1", 43, 1000, value.checksum()).valid()).isFalse();
    assertThat(new RecordValue("run", "key-2", 42, 1000, value.checksum()).valid()).isFalse();
    assertThat(new RecordValue("run2", "key-1", 42, 1000, value.checksum()).valid()).isFalse();
    assertThat(new RecordValue("run", "key-1", 42, 1001, value.checksum()).valid()).isFalse();
  }

  @Test
  void theChecksumIsCrc32cOfTheFields() {
    // CRC-32C of "a\nb\n1\n2".
    assertThat(RecordValue.checksum("a", "b", 1, 2))
        .isEqualTo(crc32c("a\nb\n1\n2".getBytes(StandardCharsets.UTF_8)));
  }

  private static String crc32c(byte[] bytes) {
    java.util.zip.CRC32C crc = new java.util.zip.CRC32C();
    crc.update(bytes);
    return String.format(java.util.Locale.ROOT, "%08x", crc.getValue());
  }

  @Test
  void refusesWhatIsNotARecordOfThisProgram() {
    for (String text :
        new String[] {
          "not json",
          "[1]",
          "{\"runId\":\"r\",\"key\":\"k\",\"seq\":\"1\",\"producerTs\":1,\"checksum\":\"x\"}",
          "{\"runId\":\"r\",\"key\":\"k\",\"seq\":1,\"producerTs\":1}",
          "{\"runId\":\"r\",\"key\":\"k\",\"seq\":1.5,\"producerTs\":1,\"checksum\":\"x\"}"
        }) {
      assertThatThrownBy(() -> RecordValue.decode(text.getBytes(StandardCharsets.UTF_8)))
          .as(text)
          .isInstanceOf(IllegalArgumentException.class);
    }
    assertThatThrownBy(() -> RecordValue.decode(null)).isInstanceOf(IllegalArgumentException.class);
  }
}
