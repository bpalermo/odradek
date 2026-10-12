package io.github.bpalermo.odradek.server.consume;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class PartitionStateTest {
  private final PartitionState state = new PartitionState(new TopicPartition("t", 0), 1);

  private void deliver(long... offsets) {
    for (long offset : offsets) {
      state.delivered(offset, offset * 10);
    }
  }

  @Test
  void nothingToCommitBeforeAnAck() {
    deliver(0, 1, 2);
    assertThat(state.dirty()).isFalse();
    assertThat(state.commitOffset()).isEqualTo(-1);
    assertThat(state.oldestUnackedNanos()).isZero();
  }

  @Test
  void ackOfTheOldestRecordsAdvancesTheCommitOffset() {
    deliver(0, 1, 2);
    state.ack(0, 1);
    assertThat(state.dirty()).isTrue();
    assertThat(state.commitOffset()).isEqualTo(2);
    assertThat(state.oldestUnackedNanos()).isEqualTo(20);
  }

  @Test
  void ackAheadOfAnUnackedRecordDoesNotAdvance() {
    deliver(0, 1, 2, 3);
    state.ack(2, 3);
    assertThat(state.dirty()).isFalse();
    state.ack(1, 1);
    assertThat(state.dirty()).isFalse();
    state.ack(0, 0);
    assertThat(state.commitOffset()).isEqualTo(4);
    assertThat(state.oldestUnackedNanos()).isEqualTo(-1);
  }

  @Test
  void gapsBetweenDeliveredOffsetsAreNotWaitedFor() {
    // Offsets 1, 2 and 4 do not exist for the app: compacted away, or transaction markers.
    deliver(0, 3, 5);
    state.ack(0, 0);
    assertThat(state.commitOffset()).isEqualTo(1);
    state.ack(3, 3);
    assertThat(state.commitOffset()).isEqualTo(4);
    state.ack(5, 5);
    assertThat(state.commitOffset()).isEqualTo(6);
  }

  @Test
  void ackOfRecordsNeverDeliveredIsIgnored() {
    deliver(0, 1);
    state.ack(0, 100);
    assertThat(state.commitOffset()).isEqualTo(2);
    deliver(2, 3);
    // 2 and 3 were delivered after that ack, so it does not cover them.
    assertThat(state.oldestUnackedNanos()).isEqualTo(20);
    state.committed();
    assertThat(state.dirty()).isFalse();

    state.ack(50, 60);
    assertThat(state.dirty()).isFalse();
  }

  @Test
  void repeatedAndOverlappingAcksAreHarmless() {
    deliver(0, 1, 2, 3, 4);
    state.ack(3, 4);
    state.ack(3, 3);
    state.ack(1, 3);
    state.ack(0, 0);
    state.ack(0, 4);
    assertThat(state.commitOffset()).isEqualTo(5);
  }

  @Test
  void committedClearsDirtyUntilTheOffsetMovesAgain() {
    deliver(0, 1);
    state.ack(0, 0);
    state.committed();
    assertThat(state.dirty()).isFalse();
    assertThat(state.commitOffset()).isEqualTo(1);
    state.ack(1, 1);
    assertThat(state.dirty()).isTrue();
  }
}
