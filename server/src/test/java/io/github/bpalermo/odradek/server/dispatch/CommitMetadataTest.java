package io.github.bpalermo.odradek.server.dispatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class CommitMetadataTest {

  private static CommitMetadata acked(long committed, long... offsets) {
    CommitMetadata.Builder above = CommitMetadata.above(committed);
    for (long offset : offsets) {
      assertThat(above.add(offset)).isTrue();
    }
    return above.build();
  }

  @Test
  void theOffsetsSurviveTheRoundTrip() {
    CommitMetadata above = acked(1000, 1001, 1003, 1010, 1999);
    String metadata = above.encode();
    assertThat(metadata).startsWith("odradek.acked/1:");
    CommitMetadata decoded = CommitMetadata.decode(1000, metadata);
    assertThat(decoded).isEqualTo(above);
    assertThat(decoded.count()).isEqualTo(4);
    assertThat(decoded.acknowledged(1001)).isTrue();
    assertThat(decoded.acknowledged(1002)).isFalse();
    assertThat(decoded.acknowledged(1999)).isTrue();
    assertThat(decoded.acknowledged(1000)).isFalse();
    assertThat(decoded.acknowledged(999)).isFalse();
    assertThat(decoded.acknowledged(2000)).isFalse();
  }

  @Test
  void theFormatIsFixed() {
    // A contract with the next replica, of whatever version: offsets 11, 13 and 20 above 10.
    assertThat(acked(10, 11, 13, 20).encode()).isEqualTo("odradek.acked/1:BQI");
    assertThat(CommitMetadata.decode(10, "odradek.acked/1:BQI")).isEqualTo(acked(10, 11, 13, 20));
  }

  @Test
  void aWindowOfAThousandOffsetsFitsInLessThanTwoHundredCharacters() {
    CommitMetadata.Builder above = CommitMetadata.above(0);
    for (long offset = 1; offset < 1000; offset++) {
      above.add(offset);
    }
    assertThat(above.build().encode()).hasSizeLessThan(200);
  }

  @Test
  void nothingAcknowledgedIsTheEmptyString() {
    assertThat(CommitMetadata.above(5).build()).isEqualTo(CommitMetadata.NONE);
    assertThat(CommitMetadata.NONE.encode()).isEmpty();
    assertThat(CommitMetadata.decode(5, "")).isEqualTo(CommitMetadata.NONE);
    assertThat(CommitMetadata.decode(5, null)).isEqualTo(CommitMetadata.NONE);
  }

  @Test
  void metadataItDoesNotRecogniseSaysNothing() {
    assertThat(CommitMetadata.decode(5, "written by someone else")).isEqualTo(CommitMetadata.NONE);
    assertThat(CommitMetadata.decode(5, "odradek.acked/2:BQI")).isEqualTo(CommitMetadata.NONE);
    assertThat(CommitMetadata.decode(5, "odradek.acked/1:not base64!"))
        .isEqualTo(CommitMetadata.NONE);
    assertThat(CommitMetadata.decode(5, "odradek.acked/1:AAA")).isEqualTo(CommitMetadata.NONE);
  }

  @Test
  void whatLiesPastTheCutIsNotCarried() {
    long last = CommitMetadata.MAX_BYTES * 8L;
    CommitMetadata.Builder above = CommitMetadata.above(0);
    assertThat(above.add(last)).isTrue();
    assertThat(above.add(last + 1)).isFalse();
    CommitMetadata built = above.build();
    assertThat(built.acknowledged(last)).isTrue();
    assertThat(built.acknowledged(last + 1)).isFalse();
    assertThat(built.encode()).hasSizeLessThanOrEqualTo(2747);
    assertThat(CommitMetadata.decode(0, built.encode())).isEqualTo(built);
  }

  @Test
  void anOffsetAtOrBelowTheCommittedOneIsRefused() {
    assertThatThrownBy(() -> CommitMetadata.above(5).add(5))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rebasedDropsWhatTheNewCommittedOffsetPassed() {
    CommitMetadata above = acked(10, 11, 13, 20);
    assertThat(above.rebased(10)).isSameAs(above);
    assertThat(above.rebased(12)).isEqualTo(acked(12, 13, 20));
    assertThat(above.rebased(13)).isEqualTo(acked(13, 20));
    assertThat(above.rebased(20)).isEqualTo(CommitMetadata.NONE);
    assertThat(above.rebased(100)).isEqualTo(CommitMetadata.NONE);
    assertThat(CommitMetadata.NONE.rebased(3)).isEqualTo(CommitMetadata.NONE);
    assertThatThrownBy(() -> above.rebased(9)).isInstanceOf(IllegalArgumentException.class);
  }
}
