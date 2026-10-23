package io.github.bpalermo.odradek.server.dispatch;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import io.github.bpalermo.odradek.delivery.v1.DeliverResponse;
import io.github.bpalermo.odradek.delivery.v1.Record;
import io.github.bpalermo.odradek.server.dispatch.DispatchAction.Deliver;
import io.github.bpalermo.odradek.server.dispatch.DispatchAction.Drop;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class BatchesTest {
  /** What was sent, as "receiver p<partition>[offset/attempt ...]" or "receiver released[...]". */
  private final List<String> sent = new ArrayList<>();

  private Batches<Long> batches = batches(100, 1 << 20);

  /** The payload is the offset; a record's value is as many bytes as its offset says. */
  private Batches<Long> batches(int maxRecords, long maxBytes) {
    return new Batches<>(
        (offset, attempt) ->
            Record.newBuilder()
                .setOffset(offset)
                .setAttempt(attempt)
                .setValue(ByteString.copyFrom(new byte[(int) (offset % 1000)]))
                .build(),
        (receiver, response) -> sent.add(receiver + " " + describe(response)),
        maxRecords,
        maxBytes);
  }

  private static String describe(DeliverResponse response) {
    if (response.hasReleased()) {
      return "released" + response.getReleased().getPartitionsList();
    }
    List<String> records = new ArrayList<>();
    response
        .getRecords()
        .getRecordsList()
        .forEach(record -> records.add(record.getOffset() + "/" + record.getAttempt()));
    return "p"
        + response.getRecords().getPartition()
        + records
        + (response.getRecords().getMaxProcessingMs() == DEADLINE_MS
            ? ""
            : " " + response.getRecords().getMaxProcessingMs() + "ms");
  }

  private static final int DEADLINE_MS = 30_000;

  private void deliver(String receiver, int partition, long offset, int attempt) {
    deliver(receiver, partition, offset, attempt, DEADLINE_MS);
  }

  private void deliver(String receiver, int partition, long offset, int attempt, int deadlineMs) {
    batches.deliver(new Deliver<>(receiver, partition, offset, offset, attempt), deadlineMs);
  }

  private void deliver(String receiver, int partition, long offset) {
    deliver(receiver, partition, offset, 1);
  }

  @Test
  void nothingIsSentUntilFlushed() {
    deliver("a", 0, 1);
    deliver("a", 0, 2);
    assertThat(sent).isEmpty();
    batches.flush();
    assertThat(sent).containsExactly("a p0[1/1, 2/1]");
    batches.flush();
    assertThat(sent).hasSize(1);
  }

  @Test
  void aBatchCarriesTheDeadlineItsRecordsWereSentUnderAndAnotherOneStartsANewBatch() {
    deliver("a", 0, 1);
    deliver("a", 0, 2, 1, 2_000);
    deliver("a", 0, 3, 1, 2_000);
    batches.flush();
    assertThat(sent).containsExactly("a p0[1/1]", "a p0[2/1, 3/1] 2000ms");
  }

  @Test
  void increasingOffsetsOfOnePartitionShareABatchWhateverTheGaps() {
    deliver("a", 0, 1);
    deliver("a", 0, 5, 2);
    deliver("a", 0, 40);
    batches.flush();
    assertThat(sent).containsExactly("a p0[1/1, 5/2, 40/1]");
  }

  @Test
  void anOffsetThatDoesNotIncreaseStartsANewBatch() {
    // A record delivered again behind a newer one, and the same record twice.
    deliver("a", 0, 5);
    deliver("a", 0, 7);
    deliver("a", 0, 3, 2);
    deliver("a", 0, 3, 3);
    deliver("a", 0, 4);
    batches.flush();
    assertThat(sent).containsExactly("a p0[5/1, 7/1]", "a p0[3/2]", "a p0[3/3, 4/1]");
  }

  @Test
  void anotherPartitionStartsANewBatch() {
    deliver("a", 0, 1);
    deliver("a", 1, 2);
    deliver("a", 0, 3);
    batches.flush();
    assertThat(sent).containsExactly("a p0[1/1]", "a p1[2/1]", "a p0[3/1]");
  }

  @Test
  void receiversAreBatchedIndependently() {
    // What the core emits when it spreads one fetch over the least loaded receivers.
    deliver("a", 0, 1);
    deliver("b", 0, 2);
    deliver("a", 0, 3);
    deliver("b", 0, 4);
    deliver("c", 1, 9);
    batches.flush();
    assertThat(sent).containsExactly("a p0[1/1, 3/1]", "b p0[2/1, 4/1]", "c p1[9/1]");
  }

  @Test
  void aBatchIsClosedAtTheRecordLimit() {
    batches = batches(2, 1 << 20);
    for (long offset = 1; offset <= 5; offset++) {
      deliver("a", 0, offset);
    }
    batches.flush();
    assertThat(sent).containsExactly("a p0[1/1, 2/1]", "a p0[3/1, 4/1]", "a p0[5/1]");
  }

  @Test
  void aBatchIsClosedBeforeItPassesTheByteLimit() {
    batches = batches(100, 1000);
    deliver("a", 0, 300);
    deliver("a", 0, 301);
    deliver("a", 0, 302);
    deliver("a", 0, 500);
    deliver("a", 0, 501);
    batches.flush();
    assertThat(sent).containsExactly("a p0[300/1, 301/1, 302/1]", "a p0[500/1]", "a p0[501/1]");
  }

  @Test
  void aRecordLargerThanTheLimitGoesAlone() {
    batches = batches(100, 100);
    deliver("a", 0, 1);
    deliver("a", 0, 999);
    deliver("a", 0, 1002);
    batches.flush();
    assertThat(sent).containsExactly("a p0[1/1]", "a p0[999/1]", "a p0[1002/1]");
  }

  @Test
  void releasedFollowsWhatWasWaitingForItsReceiverOnly() {
    deliver("a", 0, 1);
    deliver("b", 0, 2);
    batches.released(new Drop<>("a", List.of(0, 3)));
    deliver("a", 1, 7);
    batches.flush();
    assertThat(sent).containsExactly("a p0[1/1]", "a released[0, 3]", "b p0[2/1]", "a p1[7/1]");
  }
}
