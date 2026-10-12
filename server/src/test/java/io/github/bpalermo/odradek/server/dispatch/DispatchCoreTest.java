package io.github.bpalermo.odradek.server.dispatch;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import io.github.bpalermo.odradek.server.dispatch.DispatchAction.Commit;
import io.github.bpalermo.odradek.server.dispatch.DispatchAction.DeadLetter;
import io.github.bpalermo.odradek.server.dispatch.DispatchAction.Deliver;
import io.github.bpalermo.odradek.server.dispatch.DispatchAction.Drop;
import io.github.bpalermo.odradek.server.dispatch.DispatchAction.PartitionBlocked;
import io.github.bpalermo.odradek.server.dispatch.DispatchAction.PartitionReleased;
import io.github.bpalermo.odradek.server.dispatch.DispatchAction.PartitionUnblocked;
import io.github.bpalermo.odradek.server.dispatch.DispatchAction.PauseFetching;
import io.github.bpalermo.odradek.server.dispatch.DispatchAction.ResumeFetching;
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.Acknowledged;
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.CreditsGranted;
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.DeadLetterFailed;
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.DeadLetterWritten;
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.Fetched;
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.PartitionAssigned;
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.PartitionsLost;
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.PartitionsRevoked;
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.ReceiverJoined;
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.ReceiverLeft;
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.RecordsFetched;
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.Rejected;
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.TimePassed;
import io.github.bpalermo.odradek.server.dispatch.DispatchSettings.OnExhausted;
import io.github.bpalermo.odradek.server.dispatch.DispatchSettings.Ordering;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

/** Scripted scenarios: events in, the exact actions out. */
class DispatchCoreTest {
  private static final long DEADLINE = 100;
  private static final long GRACE = 50;

  private Ordering ordering = Ordering.NONE;
  private int maxAttempts;
  private OnExhausted onExhausted = OnExhausted.BLOCK;
  private long window = 1000;
  private int maxBufferedRecords = 1000;
  private long maxBufferedBytes = 1_000_000;

  private DispatchCore<String> core;
  private long now;

  // -------------------------------------------------------------------------------------------
  // Script
  // -------------------------------------------------------------------------------------------

  private void start(Ordering mode) {
    ordering = mode;
    core =
        new DispatchCore<>(
            new DispatchSettings(
                ordering,
                Duration.ofNanos(DEADLINE),
                maxAttempts,
                onExhausted,
                window,
                maxBufferedRecords,
                maxBufferedBytes,
                Duration.ofNanos(GRACE)));
  }

  private List<DispatchAction<String>> on(DispatchEvent<String> event) {
    return core.handle(event, now);
  }

  private List<DispatchAction<String>> assign(int partition, long position) {
    return on(new PartitionAssigned<>(partition, position));
  }

  /** A receiver that has joined, with credit. */
  private void receiver(String id, int credit) {
    assertThat(on(new ReceiverJoined<>(id))).isEmpty();
    assertThat(on(new CreditsGranted<>(id, credit))).isEmpty();
  }

  private List<DispatchAction<String>> credits(String id, int records) {
    return on(new CreditsGranted<>(id, records));
  }

  /** Fetches records given as "offset" or "offset:key", each of one byte. */
  private List<DispatchAction<String>> fetch(int partition, String... records) {
    List<Fetched<String>> fetched = new ArrayList<>();
    for (String record : records) {
      String[] parts = record.split(":", -1);
      long offset = Long.parseLong(parts[0]);
      ByteString key = parts.length > 1 ? ByteString.copyFromUtf8(parts[1]) : null;
      fetched.add(new Fetched<>(offset, key, 1, payload(partition, offset)));
    }
    return on(new RecordsFetched<>(partition, fetched));
  }

  private List<DispatchAction<String>> ack(String receiver, int partition, long first, long last) {
    return on(new Acknowledged<>(receiver, partition, first, last));
  }

  private List<DispatchAction<String>> ack(String receiver, int partition, long offset) {
    return ack(receiver, partition, offset, offset);
  }

  private List<DispatchAction<String>> reject(String receiver, int partition, long offset) {
    return on(new Rejected<>(receiver, partition, offset));
  }

  private List<DispatchAction<String>> after(long nanos) {
    now += nanos;
    return on(new TimePassed<>());
  }

  private static String payload(int partition, long offset) {
    return partition + "/" + offset;
  }

  private static Deliver<String> deliver(String receiver, int partition, long offset) {
    return deliver(receiver, partition, offset, 1);
  }

  private static Deliver<String> deliver(String receiver, int partition, long offset, int attempt) {
    return new Deliver<>(receiver, partition, offset, payload(partition, offset), attempt);
  }

  private static Commit<String> commit(int partition, long offset) {
    return new Commit<>(partition, offset);
  }

  // -------------------------------------------------------------------------------------------
  // Sending
  // -------------------------------------------------------------------------------------------

  @Test
  void nothingIsSentWithoutAReceiverWithCredit() {
    start(Ordering.NONE);
    assign(0, 0);
    assertThat(fetch(0, "0", "1")).isEmpty();
    assertThat(on(new ReceiverJoined<>("a"))).isEmpty();
    assertThat(credits("a", 1)).containsExactly(deliver("a", 0, 0));
    assertThat(credits("a", 0)).isEmpty();
    assertThat(credits("a", 3)).containsExactly(deliver("a", 0, 1));
    // Two credits are left for what comes next.
    assertThat(fetch(0, "2", "3", "4")).containsExactly(deliver("a", 0, 2), deliver("a", 0, 3));
  }

  @Test
  void aRecordGoesToTheLeastLoadedReceiverWithCredit() {
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 10);
    receiver("b", 10);
    // Equal load: the one that joined first.
    assertThat(fetch(0, "0", "1", "2"))
        .containsExactly(deliver("a", 0, 0), deliver("b", 0, 1), deliver("a", 0, 2));
    assertThat(fetch(0, "3")).containsExactly(deliver("b", 0, 3));
    ack("a", 0, 0, 2);
    assertThat(fetch(0, "4", "5", "6"))
        .containsExactly(deliver("a", 0, 4), deliver("a", 0, 5), deliver("a", 0, 6));
  }

  @Test
  void aReceiverWithoutCreditIsPassedOverHoweverIdle() {
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 1);
    receiver("b", 5);
    assertThat(fetch(0, "0", "1", "2"))
        .containsExactly(deliver("a", 0, 0), deliver("b", 0, 1), deliver("b", 0, 2));
    // Acknowledging does not give credit back.
    ack("a", 0, 0);
    assertThat(fetch(0, "3")).containsExactly(deliver("b", 0, 3));
  }

  @Test
  void partitionsTakeTurns() {
    start(Ordering.NONE);
    assign(0, 0);
    assign(1, 0);
    fetch(0, "0", "1", "2");
    fetch(1, "0", "1");
    on(new ReceiverJoined<>("a"));
    assertThat(credits("a", 4))
        .containsExactly(
            deliver("a", 0, 0), deliver("a", 1, 0), deliver("a", 0, 1), deliver("a", 1, 1));
  }

  @Test
  void recordsOfAnUnknownPartitionAndOffsetsAlreadyFetchedAreDropped() {
    start(Ordering.NONE);
    receiver("a", 10);
    assertThat(fetch(3, "0")).isEmpty();
    assign(0, 5);
    assertThat(fetch(0, "5", "6")).containsExactly(deliver("a", 0, 5), deliver("a", 0, 6));
    assertThat(fetch(0, "6", "7")).containsExactly(deliver("a", 0, 7));
  }

  // -------------------------------------------------------------------------------------------
  // Ordering
  // -------------------------------------------------------------------------------------------

  @Test
  void noneSendsEverythingAtOnce() {
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 10);
    assertThat(fetch(0, "0:k", "1:k", "2:k"))
        .containsExactly(deliver("a", 0, 0), deliver("a", 0, 1), deliver("a", 0, 2));
  }

  @Test
  void keyHoldsARecordBackWhileAnEarlierOneOfItsKeyIsUnacknowledged() {
    start(Ordering.KEY);
    assign(0, 0);
    receiver("a", 10);
    receiver("b", 10);
    assertThat(fetch(0, "0:x", "1:x", "2:y", "3:x", "4:y"))
        .containsExactly(deliver("a", 0, 0), deliver("b", 0, 2));
    assertThat(ack("b", 0, 2)).containsExactly(deliver("b", 0, 4));
    assertThat(ack("a", 0, 0)).containsExactly(commit(0, 1), deliver("a", 0, 1));
    assertThat(ack("a", 0, 1)).containsExactly(commit(0, 3), deliver("a", 0, 3));
    assertThat(ack("a", 0, 3)).containsExactly(commit(0, 4));
    assertThat(ack("b", 0, 4)).containsExactly(commit(0, 5));
  }

  @Test
  void keyLeavesRecordsWithoutAKeyUnconstrained() {
    start(Ordering.KEY);
    assign(0, 0);
    receiver("a", 10);
    // An empty key is a key; no key is not.
    assertThat(fetch(0, "0", "1", "2:", "3:", "4"))
        .containsExactly(
            deliver("a", 0, 0), deliver("a", 0, 1), deliver("a", 0, 2), deliver("a", 0, 4));
    assertThat(ack("a", 0, 2)).containsExactly(deliver("a", 0, 3));
  }

  @Test
  void keyComparesKeysAsBytesAndPerPartition() {
    start(Ordering.KEY);
    assign(0, 0);
    assign(1, 0);
    receiver("a", 10);
    List<Fetched<String>> records =
        List.of(
            new Fetched<>(0, ByteString.copyFrom(new byte[] {1, 2}), 1, "0/0"),
            new Fetched<>(1, ByteString.copyFrom(new byte[] {1, 2}), 1, "0/1"),
            new Fetched<>(2, ByteString.copyFrom(new byte[] {1, 2, 0}), 1, "0/2"));
    assertThat(on(new RecordsFetched<>(0, records)))
        .containsExactly(deliver("a", 0, 0), deliver("a", 0, 2));
    // The same key in another partition is another sequence.
    assertThat(
            on(
                new RecordsFetched<>(
                    1,
                    List.of(new Fetched<>(0, ByteString.copyFrom(new byte[] {1, 2}), 1, "1/0")))))
        .containsExactly(deliver("a", 1, 0));
  }

  @Test
  void partitionKeepsOneRecordInFlightPerPartition() {
    start(Ordering.PARTITION);
    assign(0, 0);
    assign(1, 0);
    receiver("a", 10);
    assertThat(fetch(0, "0:x", "1:y", "2")).containsExactly(deliver("a", 0, 0));
    assertThat(fetch(1, "0", "1")).containsExactly(deliver("a", 1, 0));
    assertThat(ack("a", 0, 0)).containsExactly(commit(0, 1), deliver("a", 0, 1));
    assertThat(ack("a", 0, 1)).containsExactly(commit(0, 2), deliver("a", 0, 2));
    assertThat(ack("a", 0, 2)).containsExactly(commit(0, 3));
  }

  // -------------------------------------------------------------------------------------------
  // Acknowledgement and commit
  // -------------------------------------------------------------------------------------------

  @Test
  void theCommitPositionMovesOnlyOverAContiguousRun() {
    start(Ordering.NONE);
    assign(0, 10);
    receiver("a", 10);
    fetch(0, "10", "11", "12", "13");
    assertThat(core.committable(0)).isEqualTo(OptionalLong.of(10));
    assertThat(ack("a", 0, 12, 13)).isEmpty();
    assertThat(ack("a", 0, 11)).isEmpty();
    assertThat(core.committable(0)).isEqualTo(OptionalLong.of(10));
    assertThat(ack("a", 0, 10)).containsExactly(commit(0, 14));
    assertThat(core.committable(0)).isEqualTo(OptionalLong.of(14));
    assertThat(core.committable(1)).isEmpty();
  }

  @Test
  void theCommitPositionDoesNotPassARecordThatWasNeverSent() {
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 2);
    assertThat(fetch(0, "0", "1", "2")).hasSize(2);
    assertThat(ack("a", 0, 0, 5)).containsExactly(commit(0, 2));
    assertThat(core.committable(0)).isEqualTo(OptionalLong.of(2));
  }

  @Test
  void offsetsThatHoldNoRecordAreNotWaitedFor() {
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 10);
    fetch(0, "0", "3", "5");
    assertThat(ack("a", 0, 0)).containsExactly(commit(0, 3));
    assertThat(ack("a", 0, 5)).isEmpty();
    assertThat(ack("a", 0, 3)).containsExactly(commit(0, 6));
    // A fetch shows that 6 to 8 hold nothing; the next acknowledgement says so.
    fetch(0, "9", "10");
    assertThat(core.committable(0)).isEqualTo(OptionalLong.of(9));
    assertThat(ack("a", 0, 10)).containsExactly(commit(0, 9));
    assertThat(ack("a", 0, 9)).containsExactly(commit(0, 11));
  }

  @Test
  void anAcknowledgedRangeAppliesOnlyToWhatThatReceiverWasSent() {
    start(Ordering.NONE);
    assign(0, 0);
    assign(1, 0);
    receiver("a", 10);
    receiver("b", 10);
    assertThat(fetch(0, "0", "1", "2", "3"))
        .containsExactly(
            deliver("a", 0, 0), deliver("b", 0, 1), deliver("a", 0, 2), deliver("b", 0, 3));
    fetch(1, "0");
    // b's range spans a's records: they stay unacknowledged.
    assertThat(ack("b", 0, 0, 3)).isEmpty();
    assertThat(ack("a", 0, 0)).containsExactly(commit(0, 2));
    assertThat(ack("a", 0, 2)).containsExactly(commit(0, 4));
    // Nothing to apply to: a repeat, an empty range, another partition, an unknown receiver.
    assertThat(ack("a", 0, 0, 3)).isEmpty();
    assertThat(ack("a", 1, 3, 0)).isEmpty();
    assertThat(ack("b", 1, 0)).isEmpty();
    assertThat(ack("a", 7, 0)).isEmpty();
    assertThat(ack("z", 1, 0)).isEmpty();
    assertThat(core.committable(1)).isEqualTo(OptionalLong.of(0));
  }

  // -------------------------------------------------------------------------------------------
  // Failed attempts
  // -------------------------------------------------------------------------------------------

  @Test
  void aRejectedRecordIsDeliveredAgainToAnotherReceiver() {
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 10);
    receiver("b", 10);
    assertThat(fetch(0, "0")).containsExactly(deliver("a", 0, 0));
    assertThat(reject("b", 0, 0)).isEmpty();
    assertThat(reject("a", 0, 7)).isEmpty();
    assertThat(reject("a", 0, 0)).containsExactly(deliver("b", 0, 0, 2));
    // a gave it back: it can neither reject nor acknowledge it any more.
    assertThat(reject("a", 0, 0)).isEmpty();
    assertThat(ack("a", 0, 0)).isEmpty();
    assertThat(ack("b", 0, 0)).containsExactly(commit(0, 1));
  }

  @Test
  void aRejectedRecordGoesBackToItsReceiverWhenNoOtherHasCredit() {
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 10);
    on(new ReceiverJoined<>("b"));
    assertThat(fetch(0, "0")).containsExactly(deliver("a", 0, 0));
    assertThat(reject("a", 0, 0)).containsExactly(deliver("a", 0, 0, 2));
  }

  @Test
  void aRecordPastItsDeadlineIsDeliveredAgainToAnotherReceiver() {
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 10);
    assertThat(core.nextDeadlineNanos()).isEmpty();
    assertThat(fetch(0, "0")).containsExactly(deliver("a", 0, 0));
    assertThat(core.nextDeadlineNanos()).isEqualTo(OptionalLong.of(DEADLINE));
    receiver("b", 10);
    assertThat(fetch(0, "1")).containsExactly(deliver("b", 0, 1));
    assertThat(after(DEADLINE - 1)).isEmpty();
    // Both are due, and each goes to the receiver that did not have it. a now holds both, and b
    // only 0, so 0 is tried on the less loaded b first.
    assertThat(after(1)).containsExactly(deliver("b", 0, 0, 2), deliver("a", 0, 1, 2));
    assertThat(core.nextDeadlineNanos()).isEqualTo(OptionalLong.of(2 * DEADLINE));
    assertThat(core.inFlight()).isEqualTo(2);
  }

  @Test
  void aRecordPastItsDeadlineGoesBackToTheOnlyReceiver() {
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 10);
    assertThat(fetch(0, "0")).containsExactly(deliver("a", 0, 0));
    assertThat(after(DEADLINE)).containsExactly(deliver("a", 0, 0, 2));
    assertThat(after(DEADLINE - 1)).isEmpty();
    assertThat(after(1)).containsExactly(deliver("a", 0, 0, 3));
    assertThat(ack("a", 0, 0)).containsExactly(commit(0, 1));
    assertThat(core.nextDeadlineNanos()).isEmpty();
  }

  @Test
  void deadlinesAreCheckedOnlyWhenTimePasses() {
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 10);
    fetch(0, "0");
    now += 10 * DEADLINE;
    // Reported with the deadline long gone, but before anyone looked: it counts.
    assertThat(ack("a", 0, 0)).containsExactly(commit(0, 1));
    assertThat(on(new TimePassed<>())).isEmpty();
  }

  @Test
  void whatWasInFlightToAReceiverThatLeavesIsDeliveredAgain() {
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 10);
    receiver("b", 1);
    assertThat(fetch(0, "0", "1", "2"))
        .containsExactly(deliver("a", 0, 0), deliver("b", 0, 1), deliver("a", 0, 2));
    // b has no credit left: 0 and 2 wait for it.
    assertThat(on(new ReceiverLeft<>("a"))).isEmpty();
    assertThat(core.inFlight()).isEqualTo(1);
    assertThat(credits("b", 1)).containsExactly(deliver("b", 0, 0, 2));
    on(new ReceiverJoined<>("c"));
    assertThat(credits("c", 1)).containsExactly(deliver("c", 0, 2, 2));
    assertThat(on(new ReceiverLeft<>("a"))).isEmpty();
    assertThat(ack("a", 0, 0, 2)).isEmpty();
  }

  @Test
  void aRedeliveryGoesOutBeforeNewerRecords() {
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 2);
    assertThat(fetch(0, "0", "1", "2", "3"))
        .containsExactly(deliver("a", 0, 0), deliver("a", 0, 1));
    assertThat(reject("a", 0, 1)).isEmpty();
    assertThat(credits("a", 3))
        .containsExactly(deliver("a", 0, 1, 2), deliver("a", 0, 2), deliver("a", 0, 3));
  }

  @Test
  void keyKeepsLaterRecordsOfAKeyBehindItsRedelivery() {
    start(Ordering.KEY);
    assign(0, 0);
    receiver("a", 1);
    assertThat(fetch(0, "0:x", "1:x", "2:y")).containsExactly(deliver("a", 0, 0));
    assertThat(on(new ReceiverLeft<>("a"))).isEmpty();
    on(new ReceiverJoined<>("b"));
    // 1 still waits for 0, which is being tried again.
    assertThat(credits("b", 10)).containsExactly(deliver("b", 0, 0, 2), deliver("b", 0, 2));
    assertThat(after(DEADLINE)).containsExactly(deliver("b", 0, 0, 3), deliver("b", 0, 2, 2));
    assertThat(ack("b", 0, 0)).containsExactly(commit(0, 1), deliver("b", 0, 1));
  }

  @Test
  void partitionKeepsThePartitionBehindARedelivery() {
    start(Ordering.PARTITION);
    assign(0, 0);
    receiver("a", 10);
    assertThat(fetch(0, "0", "1")).containsExactly(deliver("a", 0, 0));
    assertThat(reject("a", 0, 0)).containsExactly(deliver("a", 0, 0, 2));
    assertThat(after(DEADLINE)).containsExactly(deliver("a", 0, 0, 3));
    assertThat(ack("a", 0, 0)).containsExactly(commit(0, 1), deliver("a", 0, 1));
  }

  // -------------------------------------------------------------------------------------------
  // Late acknowledgements
  // -------------------------------------------------------------------------------------------

  @Test
  void aLateAcknowledgementCountsWhileTheRecordWaitsToBeDeliveredAgain() {
    start(Ordering.KEY);
    assign(0, 0);
    receiver("a", 1);
    assertThat(fetch(0, "0:x", "1:x")).containsExactly(deliver("a", 0, 0));
    // Past its deadline, and nobody has credit to take it.
    assertThat(after(DEADLINE)).isEmpty();
    assertThat(core.inFlight()).isZero();
    assertThat(ack("a", 0, 0)).containsExactly(commit(0, 1));
    // It is not delivered a second time; the next record of the key is.
    assertThat(credits("a", 5)).containsExactly(deliver("a", 0, 1));
  }

  @Test
  void aLateAcknowledgementCountsWhileTheRecordIsInFlightElsewhere() {
    start(Ordering.KEY);
    assign(0, 0);
    receiver("a", 1);
    receiver("b", 5);
    assertThat(fetch(0, "0:x", "1:x")).containsExactly(deliver("a", 0, 0));
    assertThat(after(DEADLINE)).containsExactly(deliver("b", 0, 0, 2));
    assertThat(ack("a", 0, 0)).containsExactly(commit(0, 1), deliver("b", 0, 1));
    assertThat(core.inFlight()).isEqualTo(1);
    // The copy b has is a duplicate; its acknowledgement and its deadline mean nothing now.
    assertThat(ack("b", 0, 0)).isEmpty();
    assertThat(reject("b", 0, 0)).isEmpty();
    assertThat(ack("b", 0, 1)).containsExactly(commit(0, 2));
    assertThat(after(DEADLINE)).isEmpty();
  }

  @Test
  void aRecordHeldPastItsDeadlineStillCountsAsLoad() {
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 10);
    receiver("b", 10);
    assertThat(fetch(0, "0")).containsExactly(deliver("a", 0, 0));
    assertThat(after(DEADLINE)).containsExactly(deliver("b", 0, 0, 2));
    // One each, so the tie goes to a; then b, then a.
    assertThat(fetch(0, "1", "2", "3"))
        .containsExactly(deliver("a", 0, 1), deliver("b", 0, 2), deliver("a", 0, 3));
    // Giving the late record back frees a without failing the attempt b has.
    assertThat(reject("a", 0, 0)).isEmpty();
    assertThat(ack("b", 0, 0)).containsExactly(commit(0, 1));
  }

  // -------------------------------------------------------------------------------------------
  // Exhaustion
  // -------------------------------------------------------------------------------------------

  @Test
  void anExhaustedRecordIsDeadLetteredAndCountsOnceWritten() {
    maxAttempts = 2;
    onExhausted = OnExhausted.DEAD_LETTER;
    start(Ordering.KEY);
    assign(0, 0);
    receiver("a", 10);
    assertThat(fetch(0, "0:x", "1:x", "2")).containsExactly(deliver("a", 0, 0), deliver("a", 0, 2));
    assertThat(ack("a", 0, 2)).isEmpty();
    assertThat(reject("a", 0, 0)).containsExactly(deliver("a", 0, 0, 2));
    assertThat(reject("a", 0, 0)).containsExactly(new DeadLetter<>(0, 0, "0/0", 2));
    // Until it is written it is unacknowledged: nothing is committed, its key waits.
    assertThat(core.committable(0)).isEqualTo(OptionalLong.of(0));
    assertThat(on(new DeadLetterFailed<>(0, 0))).containsExactly(new DeadLetter<>(0, 0, "0/0", 2));
    assertThat(on(new DeadLetterWritten<>(0, 5))).isEmpty();
    assertThat(on(new DeadLetterWritten<>(0, 0))).containsExactly(commit(0, 1), deliver("a", 0, 1));
    assertThat(on(new DeadLetterWritten<>(0, 0))).isEmpty();
    assertThat(on(new DeadLetterFailed<>(0, 0))).isEmpty();
    assertThat(ack("a", 0, 1)).containsExactly(commit(0, 3));
  }

  @Test
  void everyKindOfFailureUsesAnAttempt() {
    maxAttempts = 3;
    onExhausted = OnExhausted.DEAD_LETTER;
    start(Ordering.PARTITION);
    assign(0, 0);
    receiver("a", 10);
    assertThat(fetch(0, "0", "1")).containsExactly(deliver("a", 0, 0));
    assertThat(reject("a", 0, 0)).containsExactly(deliver("a", 0, 0, 2));
    assertThat(after(DEADLINE)).containsExactly(deliver("a", 0, 0, 3));
    assertThat(on(new ReceiverLeft<>("a"))).containsExactly(new DeadLetter<>(0, 0, "0/0", 3));
    receiver("b", 10);
    assertThat(on(new DeadLetterWritten<>(0, 0))).containsExactly(commit(0, 1), deliver("b", 0, 1));
  }

  @Test
  void withoutALimitARecordIsDeliveredUntilItIsAcknowledged() {
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 100);
    fetch(0, "0");
    for (int attempt = 2; attempt < 50; attempt++) {
      assertThat(reject("a", 0, 0)).containsExactly(deliver("a", 0, 0, attempt));
    }
  }

  @Test
  void anExhaustedRecordBlocksWhatFollowsItInItsPartition() {
    maxAttempts = 1;
    start(Ordering.NONE);
    assign(0, 0);
    assign(1, 0);
    receiver("a", 4);
    assertThat(fetch(0, "0", "1", "2", "3", "4")).hasSize(4);
    assertThat(reject("a", 0, 1)).containsExactly(new PartitionBlocked<>(0, 1));
    assertThat(core.blockedOffset(0)).isEqualTo(OptionalLong.of(1));
    // Nothing after it is sent, for the first time or again; other partitions go on.
    assertThat(on(new ReceiverLeft<>("a")))
        .containsExactly(
            new PartitionBlocked<>(0, 0),
            new PartitionBlocked<>(0, 2),
            new PartitionBlocked<>(0, 3));
    assertThat(core.blockedOffset(0)).isEqualTo(OptionalLong.of(0));
    receiver("b", 10);
    assertThat(fetch(0, "5")).isEmpty();
    assertThat(fetch(1, "0")).containsExactly(deliver("b", 1, 0));
    assertThat(after(10 * DEADLINE)).containsExactly(new PartitionBlocked<>(1, 0));
    assertThat(core.committable(0)).isEqualTo(OptionalLong.of(0));
    assertThat(core.blockedOffset(2)).isEmpty();
  }

  @Test
  void recordsBeforeABlockedOneAreStillDeliveredAndCommitted() {
    maxAttempts = 2;
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 10);
    receiver("b", 10);
    assertThat(fetch(0, "0", "1", "2")).hasSize(3);
    // 1 uses both attempts; 0 and 2 have failed once.
    assertThat(reject("b", 0, 1)).containsExactly(deliver("a", 0, 1, 2));
    assertThat(reject("a", 0, 0)).containsExactly(deliver("b", 0, 0, 2));
    assertThat(reject("a", 0, 1)).containsExactly(new PartitionBlocked<>(0, 1));
    // 2 is after the blocked record: it is not delivered again, but can be acknowledged.
    assertThat(after(DEADLINE)).containsExactly(new PartitionBlocked<>(0, 0));
    assertThat(core.blockedOffset(0)).isEqualTo(OptionalLong.of(0));
    assertThat(ack("a", 0, 2)).isEmpty();
    // 0 was held past its deadline by b, which acknowledges after all.
    assertThat(ack("b", 0, 0)).containsExactly(commit(0, 1));
    assertThat(core.blockedOffset(0)).isEqualTo(OptionalLong.of(1));
    assertThat(fetch(0, "3")).isEmpty();
  }

  @Test
  void aRecordBeforeTheBlockedOneIsRedelivered() {
    maxAttempts = 2;
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 10);
    assertThat(fetch(0, "0", "1", "2")).hasSize(3);
    assertThat(reject("a", 0, 1)).containsExactly(deliver("a", 0, 1, 2));
    assertThat(reject("a", 0, 1)).containsExactly(new PartitionBlocked<>(0, 1));
    assertThat(reject("a", 0, 2)).isEmpty();
    assertThat(reject("a", 0, 0)).containsExactly(deliver("a", 0, 0, 2));
    assertThat(ack("a", 0, 0)).containsExactly(commit(0, 1));
    assertThat(core.committable(0)).isEqualTo(OptionalLong.of(1));
  }

  @Test
  void aLateAcknowledgementUnblocksAPartition() {
    maxAttempts = 1;
    start(Ordering.PARTITION);
    assign(0, 0);
    receiver("a", 10);
    assertThat(fetch(0, "0", "1")).containsExactly(deliver("a", 0, 0));
    assertThat(after(DEADLINE)).containsExactly(new PartitionBlocked<>(0, 0));
    assertThat(ack("a", 0, 0))
        .containsExactly(new PartitionUnblocked<>(0), commit(0, 1), deliver("a", 0, 1));
    assertThat(core.blockedOffset(0)).isEmpty();
  }

  @Test
  void aLateAcknowledgementReplacesADeadLetterBeingWritten() {
    maxAttempts = 1;
    onExhausted = OnExhausted.DEAD_LETTER;
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 10);
    fetch(0, "0");
    assertThat(after(DEADLINE)).containsExactly(new DeadLetter<>(0, 0, "0/0", 1));
    assertThat(ack("a", 0, 0)).containsExactly(commit(0, 1));
    assertThat(on(new DeadLetterFailed<>(0, 0))).isEmpty();
    assertThat(on(new DeadLetterWritten<>(0, 0))).isEmpty();
  }

  // -------------------------------------------------------------------------------------------
  // Window and fetching
  // -------------------------------------------------------------------------------------------

  @Test
  void deliveryStopsWhenTheWindowIsFullAndGoesOnWhenTheCommitPositionMoves() {
    window = 3;
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 100);
    assertThat(fetch(0, "0", "1", "2", "3", "4", "5"))
        .containsExactly(deliver("a", 0, 0), deliver("a", 0, 1), deliver("a", 0, 2));
    assertThat(ack("a", 0, 1, 2)).isEmpty();
    assertThat(ack("a", 0, 0))
        .containsExactly(commit(0, 3), deliver("a", 0, 3), deliver("a", 0, 4), deliver("a", 0, 5));
  }

  @Test
  void theWindowIsASpanOfOffsetsAndNeverHoldsBackTheOldestRecord() {
    window = 3;
    start(Ordering.KEY);
    assign(0, 0);
    receiver("a", 100);
    // 10 is the oldest: it goes whatever its distance from where the partition started. 12 is
    // within three offsets of it, 13 is not.
    assertThat(fetch(0, "10:x", "12:y", "13:z", "14:x"))
        .containsExactly(deliver("a", 0, 10), deliver("a", 0, 12));
    assertThat(ack("a", 0, 10))
        .containsExactly(commit(0, 12), deliver("a", 0, 13), deliver("a", 0, 14));
  }

  @Test
  void fetchingPausesWhenEnoughRecordsAreBufferedAndResumesAtHalf() {
    maxBufferedRecords = 4;
    start(Ordering.NONE);
    assign(0, 0);
    assign(1, 0);
    assertThat(fetch(0, "0", "1", "2")).isEmpty();
    assertThat(fetch(0, "3", "4")).containsExactly(new PauseFetching<>(0));
    assertThat(fetch(0, "5")).isEmpty();
    assertThat(fetch(1, "0")).isEmpty();
    on(new ReceiverJoined<>("a"));
    // Partitions take turns; partition 0 goes from six buffered to two.
    assertThat(credits("a", 5))
        .containsExactly(
            deliver("a", 0, 0),
            deliver("a", 1, 0),
            deliver("a", 0, 1),
            deliver("a", 0, 2),
            deliver("a", 0, 3),
            new ResumeFetching<>(0));
    // A record delivered again was never given back to the buffer.
    assertThat(reject("a", 0, 0)).isEmpty();
    assertThat(fetch(0, "6")).isEmpty();
    assertThat(fetch(0, "7")).containsExactly(new PauseFetching<>(0));
  }

  @Test
  void fetchingPausesOnBufferedBytes() {
    maxBufferedBytes = 100;
    start(Ordering.NONE);
    assign(0, 0);
    assertThat(on(new RecordsFetched<>(0, List.of(new Fetched<>(0, null, 60, "0/0"))))).isEmpty();
    assertThat(on(new RecordsFetched<>(0, List.of(new Fetched<>(1, null, 60, "0/1")))))
        .containsExactly(new PauseFetching<>(0));
    on(new ReceiverJoined<>("a"));
    assertThat(credits("a", 1)).containsExactly(deliver("a", 0, 0));
    assertThat(on(new RecordsFetched<>(0, List.of(new Fetched<>(2, null, 1, "0/2"))))).isEmpty();
    assertThat(credits("a", 1)).containsExactly(deliver("a", 0, 1), new ResumeFetching<>(0));
  }

  @Test
  void aFullWindowFillsTheBufferAndThatPausesFetching() {
    window = 2;
    maxBufferedRecords = 3;
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 100);
    assertThat(fetch(0, "0", "1", "2", "3"))
        .containsExactly(deliver("a", 0, 0), deliver("a", 0, 1));
    assertThat(fetch(0, "4")).containsExactly(new PauseFetching<>(0));
    assertThat(ack("a", 0, 0, 1))
        .containsExactly(
            commit(0, 2), deliver("a", 0, 2), deliver("a", 0, 3), new ResumeFetching<>(0));
  }

  // -------------------------------------------------------------------------------------------
  // Giving partitions up
  // -------------------------------------------------------------------------------------------

  @Test
  void aRevokedPartitionWithNothingInFlightIsReleasedAtOnce() {
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 2);
    assertThat(fetch(0, "0", "1", "2", "3")).hasSize(2);
    assertThat(ack("a", 0, 1)).isEmpty();
    assertThat(ack("a", 0, 0)).containsExactly(commit(0, 2));
    // 2 and 3 were never sent: nothing to wait for, nothing new to commit, nobody to tell.
    assertThat(on(new PartitionsRevoked<>(List.of(0, 9))))
        .containsExactly(new PartitionReleased<>(9), new PartitionReleased<>(0));
    assertThat(core.committable(0)).isEmpty();
    assertThat(credits("a", 5)).isEmpty();
  }

  @Test
  void aRevokedPartitionWaitsForWhatIsInFlight() {
    start(Ordering.NONE);
    assign(0, 0);
    assign(1, 0);
    receiver("a", 3);
    receiver("b", 3);
    assertThat(fetch(0, "0", "1", "2", "3", "4"))
        .containsExactly(
            deliver("a", 0, 0),
            deliver("b", 0, 1),
            deliver("a", 0, 2),
            deliver("b", 0, 3),
            deliver("a", 0, 4));
    assertThat(on(new PartitionsRevoked<>(List.of(0, 1))))
        .containsExactly(new PartitionReleased<>(1));
    assertThat(core.nextDeadlineNanos()).isEqualTo(OptionalLong.of(GRACE));
    // Sending has stopped: a rejected record is not tried again, new records are dropped.
    assertThat(reject("b", 0, 3)).isEmpty();
    assertThat(fetch(0, "5")).isEmpty();
    assertThat(credits("b", 5)).isEmpty();
    assertThat(ack("a", 0, 0)).containsExactly(commit(0, 1));
    assertThat(ack("b", 0, 1)).containsExactly(commit(0, 2));
    assertThat(ack("a", 0, 4)).isEmpty();
    // The last acknowledgement in flight ends the wait. 3 was given back, so the position stops.
    assertThat(ack("a", 0, 2)).containsExactly(commit(0, 3), new PartitionReleased<>(0));
    assertThat(core.nextDeadlineNanos()).isEmpty();
  }

  @Test
  void aRevokedPartitionIsReleasedWhenTheWaitIsOver() {
    start(Ordering.NONE);
    assign(0, 0);
    assign(1, 0);
    receiver("a", 10);
    receiver("b", 10);
    receiver("c", 10);
    assertThat(fetch(0, "0", "1", "2"))
        .containsExactly(deliver("a", 0, 0), deliver("b", 0, 1), deliver("c", 0, 2));
    assertThat(fetch(1, "0")).containsExactly(deliver("a", 1, 0));
    assertThat(on(new PartitionsRevoked<>(List.of(0)))).isEmpty();
    assertThat(on(new PartitionsRevoked<>(List.of(0)))).isEmpty();
    assertThat(ack("a", 0, 0)).containsExactly(commit(0, 1));
    assertThat(after(GRACE - 1)).isEmpty();
    // b and c still hold records of it; a holds only a record of partition 1.
    assertThat(after(1))
        .containsExactly(
            new Drop<>("b", List.of(0)), new Drop<>("c", List.of(0)), new PartitionReleased<>(0));
    assertThat(ack("b", 0, 1)).isEmpty();
    assertThat(core.inFlight()).isEqualTo(1);
    assertThat(after(DEADLINE)).containsExactly(deliver("b", 1, 0, 2));
  }

  @Test
  void aLostPartitionIsForgottenWithoutACommit() {
    start(Ordering.NONE);
    assign(0, 0);
    assign(1, 0);
    assign(2, 0);
    receiver("a", 10);
    receiver("b", 10);
    assertThat(fetch(0, "0", "1")).containsExactly(deliver("a", 0, 0), deliver("b", 0, 1));
    assertThat(fetch(1, "0")).containsExactly(deliver("a", 1, 0));
    assertThat(fetch(2, "0")).containsExactly(deliver("b", 2, 0));
    assertThat(ack("a", 0, 0)).containsExactly(commit(0, 1));
    assertThat(on(new PartitionsLost<>(List.of(0, 1, 5))))
        .containsExactly(new Drop<>("a", List.of(1)), new Drop<>("b", List.of(0)));
    assertThat(core.committable(0)).isEmpty();
    assertThat(ack("b", 0, 1)).isEmpty();
    assertThat(core.inFlight()).isEqualTo(1);
    assertThat(after(DEADLINE)).containsExactly(deliver("a", 2, 0, 2));
  }

  @Test
  void aRevokedPartitionThatIsThenLostIsNotReleased() {
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 10);
    fetch(0, "0");
    assertThat(on(new PartitionsRevoked<>(List.of(0)))).isEmpty();
    assertThat(on(new PartitionsLost<>(List.of(0)))).containsExactly(new Drop<>("a", List.of(0)));
    assertThat(after(GRACE)).isEmpty();
  }

  @Test
  void aPartitionAssignedAgainStartsAfresh() {
    maxAttempts = 2;
    start(Ordering.KEY);
    assign(0, 0);
    receiver("a", 10);
    assertThat(fetch(0, "0:x", "1:x")).containsExactly(deliver("a", 0, 0));
    assertThat(reject("a", 0, 0)).containsExactly(deliver("a", 0, 0, 2));
    assertThat(on(new PartitionsRevoked<>(List.of(0)))).isEmpty();
    assertThat(after(GRACE))
        .containsExactly(new Drop<>("a", List.of(0)), new PartitionReleased<>(0));
    // Its next owner, here the same replica, counts attempts from one.
    assign(0, 0);
    assertThat(ack("a", 0, 0)).isEmpty();
    assertThat(fetch(0, "0:x", "1:x")).containsExactly(deliver("a", 0, 0));
    assertThat(ack("a", 0, 0)).containsExactly(commit(0, 1), deliver("a", 0, 1));
  }

  @Test
  void aPartitionAssignedWhileHeldIsTreatedAsLostFirst() {
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 10);
    fetch(0, "0");
    assertThat(assign(0, 0)).containsExactly(new Drop<>("a", List.of(0)));
    assertThat(core.inFlight()).isZero();
    assertThat(fetch(0, "0")).containsExactly(deliver("a", 0, 0));
  }

  @Test
  void aDeadLetterBeingWrittenIsWaitedForOnHandOver() {
    maxAttempts = 1;
    onExhausted = OnExhausted.DEAD_LETTER;
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 10);
    fetch(0, "0", "1");
    assertThat(reject("a", 0, 0)).containsExactly(new DeadLetter<>(0, 0, "0/0", 1));
    assertThat(ack("a", 0, 1)).isEmpty();
    assertThat(on(new PartitionsRevoked<>(List.of(0)))).isEmpty();
    assertThat(on(new DeadLetterWritten<>(0, 0)))
        .containsExactly(commit(0, 2), new PartitionReleased<>(0));
  }

  @Test
  void aTimeEarlierThanOneAlreadySeenIsTakenAsThatOne() {
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 10);
    now = 500;
    on(new TimePassed<>());
    now = 100;
    fetch(0, "0");
    assertThat(core.nextDeadlineNanos()).isEqualTo(OptionalLong.of(500 + DEADLINE));
  }
}
