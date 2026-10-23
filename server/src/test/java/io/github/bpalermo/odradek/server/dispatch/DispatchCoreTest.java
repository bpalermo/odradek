package io.github.bpalermo.odradek.server.dispatch;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import io.github.bpalermo.odradek.server.dispatch.DispatchAction.Cause;
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
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.GivenBack;
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.PartitionAssigned;
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.PartitionsLost;
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.PartitionsRevoked;
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.ReceiverJoined;
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.ReceiverLeft;
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.RecordsFetched;
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.Rejected;
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.Returned;
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

  /** The waits before a redelivery and before a dead letter is written again; none by default. */
  private long backoff;

  private long backoffMax;
  private long retry;
  private long retryMax;

  private DispatchCore<String> core;
  private long now;

  /**
   * What the core timed, in order: "queued <nanos> <wait>", "acked <nanos>", "rejected <nanos>".
   */
  private final List<String> timed = new ArrayList<>();

  private final DispatchCore.Timings timings =
      new DispatchCore.Timings() {
        @Override
        public void queued(long nanos, DispatchCore.Wait waited) {
          timed.add("queued " + nanos + " " + waited);
        }

        @Override
        public void answered(long nanos, boolean acknowledged) {
          timed.add((acknowledged ? "acked " : "rejected ") + nanos);
        }
      };

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
                    Duration.ofNanos(GRACE))
                .withRedeliveryBackoff(Duration.ofNanos(backoff), Duration.ofNanos(backoffMax))
                .withDeadLetterRetry(Duration.ofNanos(retry), Duration.ofNanos(retryMax)),
            timings);
  }

  /**
   * What the core answers, with why a record is sent again left out: most scenarios are about what
   * is sent, to whom and under which attempt number. {@link #raw} keeps it.
   */
  private List<DispatchAction<String>> on(DispatchEvent<String> event) {
    List<DispatchAction<String>> actions = new ArrayList<>();
    for (DispatchAction<String> action : raw(event)) {
      actions.add(
          action instanceof Deliver<String> d
              ? new Deliver<>(d.receiver(), d.partition(), d.offset(), d.payload(), d.attempt())
              : action);
    }
    return actions;
  }

  private List<DispatchAction<String>> raw(DispatchEvent<String> event) {
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
    return on(new Rejected<>(receiver, partition, offset, "bad"));
  }

  /** The dead letter of a record that was rejected last, asked for the first time. */
  private static DeadLetter<String> letter(int partition, long offset, int attempts) {
    return new DeadLetter<>(
        partition, offset, payload(partition, offset), attempts, Cause.REJECTED, "bad", 0);
  }

  private static PartitionReleased<String> released(int partition) {
    return new PartitionReleased<>(partition, true, 0);
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

  /** Deliveries given back, each under the first attempt. */
  private static List<GivenBack> given(long... offsets) {
    List<GivenBack> given = new ArrayList<>();
    for (long offset : offsets) {
      given.add(new GivenBack(offset, 1));
    }
    return given;
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
    // Equal load: the one sent a record longest ago, never sent first, then the one that joined
    // first.
    assertThat(fetch(0, "0", "1", "2"))
        .containsExactly(deliver("a", 0, 0), deliver("b", 0, 1), deliver("a", 0, 2));
    assertThat(fetch(0, "3")).containsExactly(deliver("b", 0, 3));
    ack("a", 0, 0, 2);
    // a holds nothing and b two: a until they hold as many, then b, sent to longer ago.
    assertThat(fetch(0, "4", "5", "6"))
        .containsExactly(deliver("a", 0, 4), deliver("a", 0, 5), deliver("b", 0, 6));
  }

  @Test
  void idleReceiversTakeTurns() {
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 10);
    receiver("b", 10);
    receiver("c", 10);
    // Each record is acknowledged before the next arrives, so every receiver is idle each time:
    // the one that joined first must not get them all.
    List<String> to = new ArrayList<>();
    for (long offset = 0; offset < 6; offset++) {
      List<DispatchAction<String>> sent = fetch(0, Long.toString(offset));
      assertThat(sent).hasSize(1);
      Deliver<String> d = (Deliver<String>) sent.getFirst();
      to.add(d.receiver());
      ack(d.receiver(), 0, offset);
      credits(d.receiver(), 1);
    }
    assertThat(to).containsExactly("a", "b", "c", "a", "b", "c");
  }

  @Test
  void aReceiverThatJoinsLateIsIdleTheLongest() {
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 10);
    receiver("b", 10);
    assertThat(fetch(0, "0")).containsExactly(deliver("a", 0, 0));
    ack("a", 0, 0);
    assertThat(fetch(0, "1")).containsExactly(deliver("b", 0, 1));
    ack("b", 0, 1);
    receiver("c", 10);
    assertThat(fetch(0, "2")).containsExactly(deliver("c", 0, 2));
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
  void aNewProcessingDeadlineAppliesToWhatIsSentFromThenOn() {
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 10);
    assertThat(fetch(0, "0")).containsExactly(deliver("a", 0, 0));
    // Shorter: a record sent later is due before one sent earlier.
    core.maxProcessing(Duration.ofNanos(DEADLINE / 4));
    now += DEADLINE / 2;
    assertThat(fetch(0, "1")).containsExactly(deliver("a", 0, 1));
    assertThat(core.nextDeadlineNanos()).isEqualTo(OptionalLong.of(DEADLINE / 2 + DEADLINE / 4));
    assertThat(after(DEADLINE / 4)).containsExactly(deliver("a", 0, 1, 2));
    // 0 keeps the deadline it was sent under, and its redelivery gets the new one.
    assertThat(core.nextDeadlineNanos()).isEqualTo(OptionalLong.of(DEADLINE));
    assertThat(after(DEADLINE / 4)).containsExactly(deliver("a", 0, 0, 2), deliver("a", 0, 1, 3));
    assertThat(after(DEADLINE / 4)).containsExactly(deliver("a", 0, 0, 3), deliver("a", 0, 1, 4));
    // Longer again.
    core.maxProcessing(Duration.ofNanos(DEADLINE));
    core.maxProcessing(Duration.ofNanos(DEADLINE));
    assertThat(fetch(0, "2")).containsExactly(deliver("a", 0, 2));
    assertThat(after(DEADLINE / 4)).containsExactly(deliver("a", 0, 0, 4), deliver("a", 0, 1, 5));
    assertThat(ack("a", 0, 0, 2)).containsExactly(commit(0, 3));
    assertThat(core.nextDeadlineNanos()).isEmpty();
    assertThat(core.inFlight()).isZero();
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
    // With the same attempt number: a receiver that leaves does not use one up.
    assertThat(credits("b", 1)).containsExactly(deliver("b", 0, 0, 1));
    on(new ReceiverJoined<>("c"));
    assertThat(credits("c", 1)).containsExactly(deliver("c", 0, 2, 1));
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
    assertThat(credits("b", 10)).containsExactly(deliver("b", 0, 0, 1), deliver("b", 0, 2));
    assertThat(after(DEADLINE)).containsExactly(deliver("b", 0, 0, 2), deliver("b", 0, 2, 2));
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
  // Records given back unread (ADR 7)
  // -------------------------------------------------------------------------------------------

  @Test
  void aRecordGivenBackUnreadGoesToAnotherReceiverAtOnceUnderTheSameAttempt() {
    backoff = 50;
    backoffMax = 50;
    start(Ordering.KEY);
    assign(0, 0);
    receiver("a", 10);
    receiver("b", 10);
    assertThat(fetch(0, "0:x", "1:x")).containsExactly(deliver("a", 0, 0));
    // No wait, the same attempt, and the next record of its key still waits behind it.
    assertThat(raw(new Returned<>("a", 0, given(0))))
        .containsExactly(new Deliver<>("b", 0, 0, "0/0", 1, Cause.RETURNED));
    // a gave it back: it can neither give it back again nor acknowledge it any more.
    assertThat(on(new Returned<>("a", 0, given(0)))).isEmpty();
    assertThat(ack("a", 0, 0)).isEmpty();
    assertThat(ack("b", 0, 0)).containsExactly(commit(0, 1), deliver("a", 0, 1));
  }

  @Test
  void aRecordGivenBackDoesNotGiveItsCreditBack() {
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 1);
    assertThat(fetch(0, "0")).containsExactly(deliver("a", 0, 0));
    // a spent its one credit on the record: nothing goes back to it until it grants more.
    assertThat(on(new Returned<>("a", 0, given(0)))).isEmpty();
    assertThat(core.inFlight()).isZero();
    assertThat(credits("a", 1)).containsExactly(deliver("a", 0, 0, 1));
  }

  @Test
  void givingBackWhatIsNotHeldOrIsPastItsDeadlineChangesNothing() {
    maxAttempts = 2;
    onExhausted = OnExhausted.DEAD_LETTER;
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 10);
    receiver("b", 10);
    assertThat(fetch(0, "0")).containsExactly(deliver("a", 0, 0));
    assertThat(on(new Returned<>("b", 0, given(0)))).isEmpty();
    assertThat(on(new Returned<>("a", 0, given(7)))).isEmpty();
    assertThat(on(new Returned<>("a", 1, given(0)))).isEmpty();
    // The deadline failed the first attempt; giving the expired copy back does not take it back.
    assertThat(after(DEADLINE)).containsExactly(deliver("b", 0, 0, 2));
    assertThat(on(new Returned<>("a", 0, given(0)))).isEmpty();
    assertThat(after(DEADLINE))
        .containsExactly(new DeadLetter<>(0, 0, "0/0", 2, Cause.DEADLINE, null, 0));
  }

  @Test
  void aLateReturnOfACopyThatMissedItsDeadlineLeavesTheNewCopyOnTheSameStreamAlone() {
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 10);
    assertThat(fetch(0, "0")).containsExactly(deliver("a", 0, 0));
    // Only a has credit, so the record goes back to it, under the next attempt.
    assertThat(after(DEADLINE)).containsExactly(deliver("a", 0, 0, 2));
    // The return of the first copy arrives now: it names attempt 1, and a holds attempt 2.
    assertThat(on(new Returned<>("a", 0, List.of(new GivenBack(0, 1))))).isEmpty();
    assertThat(core.inFlight()).isEqualTo(1);
    assertThat(ack("a", 0, 0)).containsExactly(commit(0, 1));
  }

  @Test
  void aLateReturnForgetsTheExpiredCopyWithoutGivingTheAttemptBack() {
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 10);
    receiver("b", 10);
    assertThat(fetch(0, "0")).containsExactly(deliver("a", 0, 0));
    assertThat(after(DEADLINE)).containsExactly(deliver("b", 0, 0, 2));
    // a still holds the expired copy, and its acknowledgement would count. Given back, the copy is
    // forgotten: nothing is sent again, and b keeps attempt 2.
    assertThat(on(new Returned<>("a", 0, given(0)))).isEmpty();
    assertThat(ack("a", 0, 0)).isEmpty();
    assertThat(ack("b", 0, 0)).containsExactly(commit(0, 1));
  }

  @Test
  void recordsGivenBackNeverExhaustARecord() {
    maxAttempts = 1;
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 100);
    receiver("b", 100);
    assertThat(fetch(0, "0", "1")).containsExactly(deliver("a", 0, 0), deliver("b", 0, 1));
    for (int i = 0; i < 10; i++) {
      assertThat(on(new Returned<>("a", 0, given(0)))).containsExactly(deliver("b", 0, 0, 1));
      assertThat(on(new Returned<>("b", 0, given(0, 1))))
          .containsExactly(deliver("a", 0, 0, 1), deliver("a", 0, 1, 1));
      assertThat(on(new Returned<>("a", 0, given(1)))).containsExactly(deliver("b", 0, 1, 1));
    }
    assertThat(ack("a", 0, 0)).containsExactly(commit(0, 1));
    assertThat(ack("b", 0, 1)).containsExactly(commit(0, 2));
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
    assertThat(reject("a", 0, 0)).containsExactly(letter(0, 0, 2));
    // Until it is written it is unacknowledged: nothing is committed, its key waits.
    assertThat(core.committable(0)).isEqualTo(OptionalLong.of(0));
    assertThat(on(new DeadLetterFailed<>(0, 0)))
        .containsExactly(new DeadLetter<>(0, 0, "0/0", 2, Cause.REJECTED, "bad", 1));
    assertThat(on(new DeadLetterWritten<>(0, 5))).isEmpty();
    assertThat(on(new DeadLetterWritten<>(0, 0))).containsExactly(commit(0, 1), deliver("a", 0, 1));
    assertThat(on(new DeadLetterWritten<>(0, 0))).isEmpty();
    assertThat(on(new DeadLetterFailed<>(0, 0))).isEmpty();
    assertThat(ack("a", 0, 1)).containsExactly(commit(0, 3));
  }

  @Test
  void aRejectionAndADeadlineUseAnAttemptAndAReceiverLeavingDoesNot() {
    maxAttempts = 3;
    onExhausted = OnExhausted.DEAD_LETTER;
    start(Ordering.PARTITION);
    assign(0, 0);
    receiver("a", 10);
    assertThat(fetch(0, "0", "1")).containsExactly(deliver("a", 0, 0));
    assertThat(reject("a", 0, 0)).containsExactly(deliver("a", 0, 0, 2));
    // The second attempt is taken back, not failed: it is made again.
    assertThat(on(new ReceiverLeft<>("a"))).isEmpty();
    on(new ReceiverJoined<>("b"));
    assertThat(credits("b", 10)).containsExactly(deliver("b", 0, 0, 2));
    assertThat(after(DEADLINE)).containsExactly(deliver("b", 0, 0, 3));
    assertThat(reject("b", 0, 0)).containsExactly(letter(0, 0, 3));
    assertThat(on(new DeadLetterWritten<>(0, 0))).containsExactly(commit(0, 1), deliver("b", 0, 1));
  }

  @Test
  void receiversLeavingNeverExhaustARecord() {
    // A rollout of the application with a limit and BLOCK: every pod leaves holding records.
    maxAttempts = 1;
    start(Ordering.NONE);
    assign(0, 0);
    receiver("r0", 10);
    assertThat(fetch(0, "0", "1")).containsExactly(deliver("r0", 0, 0), deliver("r0", 0, 1));
    for (int i = 1; i < 20; i++) {
      assertThat(on(new ReceiverLeft<>("r" + (i - 1)))).isEmpty();
      assertThat(core.inFlight()).isZero();
      on(new ReceiverJoined<>("r" + i));
      assertThat(credits("r" + i, 10))
          .containsExactly(deliver("r" + i, 0, 0, 1), deliver("r" + i, 0, 1, 1));
    }
    assertThat(core.blockedOffset(0)).isEmpty();
    assertThat(ack("r19", 0, 0, 1)).containsExactly(commit(0, 2));
  }

  @Test
  void aRecordTakenBackIsNotCountedTwiceAgainstTheFetchBuffer() {
    maxBufferedRecords = 2;
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 1);
    assertThat(fetch(0, "0", "1", "2")).containsExactly(deliver("a", 0, 0), new PauseFetching<>(0));
    assertThat(on(new ReceiverLeft<>("a"))).isEmpty();
    on(new ReceiverJoined<>("b"));
    // 0 again, which frees nothing in the buffer; then 1, which brings it to half.
    assertThat(credits("b", 1)).containsExactly(deliver("b", 0, 0, 1));
    assertThat(credits("b", 1)).containsExactly(deliver("b", 0, 1), new ResumeFetching<>(0));
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
    // What a leaves holding is taken back without using an attempt: 0, before the blocked record,
    // goes out again; 2 and 3, after it, wait.
    assertThat(on(new ReceiverLeft<>("a"))).isEmpty();
    assertThat(core.blockedOffset(0)).isEqualTo(OptionalLong.of(1));
    on(new ReceiverJoined<>("b"));
    assertThat(credits("b", 10)).containsExactly(deliver("b", 0, 0, 1));
    assertThat(fetch(0, "5")).isEmpty();
    assertThat(ack("b", 0, 0)).containsExactly(commit(0, 1));
    assertThat(fetch(1, "0")).containsExactly(deliver("b", 1, 0));
    assertThat(after(10 * DEADLINE)).containsExactly(new PartitionBlocked<>(1, 0));
    assertThat(core.committable(0)).isEqualTo(OptionalLong.of(1));
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
    assertThat(after(DEADLINE))
        .containsExactly(new DeadLetter<>(0, 0, "0/0", 1, Cause.DEADLINE, null, 0));
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
        .containsExactly(released(9), released(0));
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
    assertThat(on(new PartitionsRevoked<>(List.of(0, 1)))).containsExactly(released(1));
    assertThat(core.nextDeadlineNanos()).isEqualTo(OptionalLong.of(GRACE / 2));
    // Nothing new is sent: what is fetched now is dropped.
    assertThat(fetch(0, "5")).isEmpty();
    assertThat(credits("b", 5)).isEmpty();
    assertThat(ack("a", 0, 0)).containsExactly(commit(0, 1));
    assertThat(ack("b", 0, 1)).containsExactly(commit(0, 2));
    assertThat(ack("a", 0, 4)).isEmpty();
    assertThat(ack("a", 0, 2)).containsExactly(commit(0, 3));
    // The last acknowledgement in flight ends the wait.
    assertThat(ack("b", 0, 3)).containsExactly(commit(0, 5), released(0));
    assertThat(core.nextDeadlineNanos()).isEmpty();
  }

  // What a hand-over finishes: a record held back below one that was sent. The commit position is
  // a contiguous run, so such a record would stop it below records that are acknowledged, and the
  // next owner would send those again.

  @Test
  void whatWasHeldBackBelowWhatWasSentIsFinishedInTheOrderOfItsKey() {
    start(Ordering.KEY);
    assign(0, 0);
    receiver("a", 3);
    assertThat(fetch(0, "0:x", "1:x", "2:y", "3:x", "4:y"))
        .containsExactly(deliver("a", 0, 0), deliver("a", 0, 2));
    assertThat(ack("a", 0, 2)).containsExactly(deliver("a", 0, 4));
    assertThat(ack("a", 0, 4)).isEmpty();
    // Fetched, and never sent for want of credit: above everything that was sent.
    assertThat(fetch(0, "5:z", "6:x")).isEmpty();
    // 2 and 4 are acknowledged above 0, which is in flight, and above 1 and 3, which wait for it.
    assertThat(core.committable(0)).isEqualTo(OptionalLong.of(0));

    assertThat(on(new PartitionsRevoked<>(List.of(0)))).isEmpty();
    assertThat(core.nextDeadlineNanos()).isEqualTo(OptionalLong.of(GRACE / 2));
    // Credit changes nothing: 1 and 3 wait for their key, and 5 and 6 are not sent at all.
    assertThat(credits("a", 10)).isEmpty();
    assertThat(ack("a", 0, 0)).containsExactly(commit(0, 1), deliver("a", 0, 1));
    assertThat(ack("a", 0, 1)).containsExactly(commit(0, 3), deliver("a", 0, 3));
    // Everything this replica sent is acknowledged and committed: its next owner starts at 5,
    // the first record that was never sent, and sends nothing a second time.
    assertThat(ack("a", 0, 3)).containsExactly(commit(0, 5), released(0));
    assertThat(core.nextDeadlineNanos()).isEmpty();
    assertThat(core.inFlight()).isZero();
  }

  @Test
  void nothingAtOrAboveTheNewestRecordSentIsSentDuringAHandOver() {
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 3);
    assertThat(fetch(0, "0", "1", "2", "3", "4"))
        .containsExactly(deliver("a", 0, 0), deliver("a", 0, 1), deliver("a", 0, 2));
    assertThat(ack("a", 0, 2)).isEmpty();
    // Given back, and not sent again yet for want of credit.
    assertThat(reject("a", 0, 1)).isEmpty();
    assertThat(on(new PartitionsRevoked<>(List.of(0)))).isEmpty();
    // 1 lies below 2, which is acknowledged: it is sent again. 3 and 4 were never sent.
    assertThat(credits("a", 5)).containsExactly(deliver("a", 0, 1, 2));
    assertThat(fetch(0, "5")).isEmpty();
    assertThat(ack("a", 0, 0, 1)).containsExactly(commit(0, 3), released(0));
  }

  @Test
  void theNewestRecordSentIsNotSentAgainDuringAHandOver() {
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 10);
    assertThat(fetch(0, "0", "1", "2")).hasSize(3);
    assertThat(on(new PartitionsRevoked<>(List.of(0)))).isEmpty();
    // Nothing is acknowledged above 2, so sending it again would spare the next owner nothing.
    assertThat(reject("a", 0, 2)).isEmpty();
    // 0 lies below 1, which may yet be acknowledged.
    assertThat(reject("a", 0, 0)).containsExactly(deliver("a", 0, 0, 2));
    assertThat(ack("a", 0, 0, 1))
        .containsExactly(commit(0, 2), new PartitionReleased<>(0, true, 1));
  }

  @Test
  void sendingEndsHalfwayThroughTheWaitAndWhatIsLeftIsTheNextOwners() {
    start(Ordering.KEY);
    assign(0, 0);
    receiver("a", 10);
    assertThat(fetch(0, "0:x", "1:x", "2:y"))
        .containsExactly(deliver("a", 0, 0), deliver("a", 0, 2));
    assertThat(ack("a", 0, 2)).isEmpty();
    assertThat(on(new PartitionsRevoked<>(List.of(0)))).isEmpty();
    assertThat(after(GRACE / 2 - 1)).isEmpty();
    assertThat(core.nextDeadlineNanos()).isEqualTo(OptionalLong.of(GRACE / 2));
    assertThat(after(1)).isEmpty();
    // From here on there is only the wait for what is in flight.
    assertThat(core.nextDeadlineNanos()).isEqualTo(OptionalLong.of(GRACE));
    // 1 is not sent any more: a record sent now would have little time to be answered in. It
    // was never sent, so nobody has it; 2, acknowledged above it, is sent again by the next owner.
    assertThat(ack("a", 0, 0)).containsExactly(commit(0, 1), new PartitionReleased<>(0, true, 1));
  }

  @Test
  void aRecordSentDuringAHandOverAndNotAnsweredIsDroppedWhenTheWaitIsOver() {
    start(Ordering.KEY);
    assign(0, 0);
    receiver("a", 10);
    assertThat(fetch(0, "0:x", "1:x", "2:y")).hasSize(2);
    assertThat(ack("a", 0, 2)).isEmpty();
    assertThat(on(new PartitionsRevoked<>(List.of(0)))).isEmpty();
    assertThat(after(GRACE / 2 - 1)).isEmpty();
    assertThat(ack("a", 0, 0)).containsExactly(commit(0, 1), deliver("a", 0, 1));
    assertThat(after(GRACE / 2)).isEmpty();
    assertThat(after(1))
        .containsExactly(new Drop<>("a", List.of(0)), new PartitionReleased<>(0, false, 2));
  }

  @Test
  void aRecordWaitingToBeSentAgainIsSentDuringAHandOverWhenItsWaitEndsInTime() {
    backoff = GRACE / 2 - 5;
    backoffMax = GRACE;
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 10);
    assertThat(fetch(0, "0", "1")).hasSize(2);
    assertThat(reject("a", 0, 0)).isEmpty();
    assertThat(ack("a", 0, 1)).isEmpty();
    assertThat(on(new PartitionsRevoked<>(List.of(0)))).isEmpty();
    assertThat(core.nextDeadlineNanos()).isEqualTo(OptionalLong.of(GRACE / 2 - 5));
    assertThat(after(GRACE / 2 - 5)).containsExactly(deliver("a", 0, 0, 2));
    assertThat(ack("a", 0, 0)).containsExactly(commit(0, 2), released(0));
  }

  @Test
  void aRecordWhoseWaitEndsAfterSendingDoesIsNotWaitedFor() {
    backoff = GRACE / 2;
    backoffMax = GRACE;
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 10);
    assertThat(fetch(0, "0", "1", "2", "3", "4")).hasSize(5);
    assertThat(reject("a", 0, 1)).isEmpty();
    assertThat(ack("a", 0, 2)).isEmpty();
    assertThat(ack("a", 0, 4)).isEmpty();
    // 1 cannot be sent again while sending lasts, and nothing is committed past it. The
    // hand-over does not wait for it; and 3, which lies below a record that is acknowledged, is
    // not sent again when it comes back: the next owner starts at 1 and sends 3 whatever is done.
    assertThat(on(new PartitionsRevoked<>(List.of(0)))).isEmpty();
    assertThat(reject("a", 0, 3)).isEmpty();
    assertThat(ack("a", 0, 0)).containsExactly(commit(0, 1), new PartitionReleased<>(0, true, 4));
    assertThat(after(GRACE)).isEmpty();
  }

  @Test
  void aRecordThatFailsDuringAHandOverAndCannotBeSentAgainInTimeEndsTheSending() {
    backoff = GRACE / 2;
    backoffMax = GRACE;
    start(Ordering.KEY);
    assign(0, 0);
    receiver("a", 10);
    assertThat(fetch(0, "0:x", "1:y", "2:y", "3:z")).hasSize(3);
    assertThat(ack("a", 0, 3)).isEmpty();
    assertThat(on(new PartitionsRevoked<>(List.of(0)))).isEmpty();
    assertThat(after(1)).isEmpty();
    // Its wait would end after sending does. Nothing is committed past it by this replica, so 2,
    // which waits behind 1, is not sent either: the next owner would send it again besides.
    assertThat(reject("a", 0, 0)).isEmpty();
    assertThat(ack("a", 0, 1)).containsExactly(new PartitionReleased<>(0, true, 3));
  }

  @Test
  void aBlockedRecordIsNotWaitedForAndNothingAboveItIsSentDuringAHandOver() {
    maxAttempts = 1;
    start(Ordering.KEY);
    assign(0, 0);
    receiver("a", 10);
    assertThat(fetch(0, "0", "1:x", "2:x", "3:y")).hasSize(3);
    assertThat(reject("a", 0, 0)).containsExactly(new PartitionBlocked<>(0, 0));
    assertThat(ack("a", 0, 3)).isEmpty();
    // 1 is in flight above the blocked record, and 2 waits behind 1.
    assertThat(on(new PartitionsRevoked<>(List.of(0)))).isEmpty();
    assertThat(core.blockedOffset(0)).isEmpty();
    assertThat(core.nextDeadlineNanos()).isEqualTo(OptionalLong.of(GRACE));
    // 2 is not sent: the position stays at the blocked record whatever is done above it.
    assertThat(ack("a", 0, 1)).containsExactly(new PartitionReleased<>(0, true, 3));
  }

  @Test
  void aRecordThatUsesItsAttemptsDuringAHandOverBlocksNothingAndEndsTheSending() {
    maxAttempts = 1;
    start(Ordering.KEY);
    assign(0, 0);
    receiver("a", 10);
    assertThat(fetch(0, "0:x", "1:x", "2:y")).hasSize(2);
    assertThat(ack("a", 0, 2)).isEmpty();
    assertThat(on(new PartitionsRevoked<>(List.of(0)))).isEmpty();
    // No PartitionBlocked: the partition is leaving, and its next owner counts the attempts anew.
    assertThat(reject("a", 0, 0)).containsExactly(new PartitionReleased<>(0, true, 2));
  }

  @Test
  void aDeadLetterBeingWrittenIsWaitedForAndWhatWaitedBehindItIsSent() {
    maxAttempts = 1;
    onExhausted = OnExhausted.DEAD_LETTER;
    start(Ordering.KEY);
    assign(0, 0);
    receiver("a", 10);
    assertThat(fetch(0, "0:x", "1:x", "2:y")).hasSize(2);
    assertThat(ack("a", 0, 2)).isEmpty();
    assertThat(reject("a", 0, 0)).containsExactly(letter(0, 0, 1));
    assertThat(on(new PartitionsRevoked<>(List.of(0)))).isEmpty();
    assertThat(on(new DeadLetterWritten<>(0, 0))).containsExactly(commit(0, 1), deliver("a", 0, 1));
    assertThat(ack("a", 0, 1)).containsExactly(commit(0, 3), released(0));
  }

  @Test
  void aRecordThatUsesItsAttemptsDuringAHandOverIsDeadLettered() {
    maxAttempts = 1;
    onExhausted = OnExhausted.DEAD_LETTER;
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 10);
    assertThat(fetch(0, "0", "1")).hasSize(2);
    assertThat(ack("a", 0, 1)).isEmpty();
    assertThat(on(new PartitionsRevoked<>(List.of(0)))).isEmpty();
    assertThat(reject("a", 0, 0)).containsExactly(letter(0, 0, 1));
    assertThat(on(new DeadLetterWritten<>(0, 0))).containsExactly(commit(0, 2), released(0));
  }

  @Test
  void aDeadLetterThatFailsDuringAHandOverEndsTheSending() {
    maxAttempts = 1;
    onExhausted = OnExhausted.DEAD_LETTER;
    start(Ordering.KEY);
    assign(0, 0);
    receiver("a", 10);
    assertThat(fetch(0, "0:x", "1:y", "2:y", "3:z")).hasSize(3);
    assertThat(ack("a", 0, 3)).isEmpty();
    assertThat(reject("a", 0, 0)).containsExactly(letter(0, 0, 1));
    assertThat(on(new PartitionsRevoked<>(List.of(0)))).isEmpty();
    assertThat(on(new DeadLetterFailed<>(0, 0))).isEmpty();
    // 2 waited behind 1, above the record that is now the next owner's: it is not sent.
    assertThat(ack("a", 0, 1)).containsExactly(new PartitionReleased<>(0, true, 3));
  }

  @Test
  void aRecordWhoseReceiverLeavesDuringAHandOverGoesToAnother() {
    start(Ordering.KEY);
    assign(0, 0);
    receiver("a", 10);
    receiver("b", 10);
    assertThat(fetch(0, "0:x", "1:x", "2:y"))
        .containsExactly(deliver("a", 0, 0), deliver("b", 0, 2));
    assertThat(ack("b", 0, 2)).isEmpty();
    assertThat(on(new PartitionsRevoked<>(List.of(0)))).isEmpty();
    assertThat(raw(new ReceiverLeft<>("a")))
        .containsExactly(new Deliver<>("b", 0, 0, "0/0", 1, Cause.RECEIVER_LEFT));
    assertThat(ack("b", 0, 0)).containsExactly(commit(0, 1), deliver("b", 0, 1));
    assertThat(ack("b", 0, 1)).containsExactly(commit(0, 3), released(0));
  }

  @Test
  void withNoReceiverLeftAHandOverWaitsForNothing() {
    start(Ordering.KEY);
    assign(0, 0);
    assign(1, 0);
    receiver("a", 10);
    assertThat(fetch(0, "0:x", "1:x", "2:y")).hasSize(2);
    assertThat(fetch(1, "0:x", "1:x", "2:y")).hasSize(2);
    assertThat(ack("a", 0, 2)).isEmpty();
    assertThat(ack("a", 1, 2)).isEmpty();
    assertThat(on(new PartitionsRevoked<>(List.of(0)))).isEmpty();
    // Nobody can be sent what was held back, in the partition being handed over...
    assertThat(on(new ReceiverLeft<>("a"))).containsExactly(new PartitionReleased<>(0, true, 2));
    // ... nor in one that is handed over with nobody there.
    assertThat(on(new PartitionsRevoked<>(List.of(1))))
        .containsExactly(new PartitionReleased<>(1, true, 2));
    assertThat(core.nextDeadlineNanos()).isEmpty();
  }

  @Test
  void underPartitionOrderingAHandOverSendsNothing() {
    backoff = 1;
    backoffMax = 1;
    start(Ordering.PARTITION);
    assign(0, 0);
    assign(1, 0);
    receiver("a", 10);
    assertThat(fetch(0, "0", "1", "2")).containsExactly(deliver("a", 0, 0));
    assertThat(fetch(1, "0", "1", "2")).containsExactly(deliver("a", 1, 0));
    assertThat(on(new PartitionsRevoked<>(List.of(0, 1)))).isEmpty();
    // One record of a partition is unacknowledged at a time: there is nothing below it.
    assertThat(core.nextDeadlineNanos()).isEqualTo(OptionalLong.of(GRACE));
    assertThat(ack("a", 0, 0)).containsExactly(commit(0, 1), released(0));
    // Given back: it is the newest record sent, and is the next owner's.
    assertThat(reject("a", 1, 0)).containsExactly(new PartitionReleased<>(1, true, 1));
    assertThat(after(GRACE)).isEmpty();
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
            new Drop<>("b", List.of(0)),
            new Drop<>("c", List.of(0)),
            // The wait ended with two records unanswered.
            new PartitionReleased<>(0, false, 2));
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
        .containsExactly(new Drop<>("a", List.of(0)), new PartitionReleased<>(0, false, 1));
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
    assertThat(reject("a", 0, 0)).containsExactly(letter(0, 0, 1));
    assertThat(ack("a", 0, 1)).isEmpty();
    assertThat(on(new PartitionsRevoked<>(List.of(0)))).isEmpty();
    assertThat(on(new DeadLetterWritten<>(0, 0))).containsExactly(commit(0, 2), released(0));
  }

  @Test
  void aDeadLetterWhoseWriteFailsDuringAHandOverIsLeftToTheNextOwner() {
    maxAttempts = 1;
    onExhausted = OnExhausted.DEAD_LETTER;
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 10);
    fetch(0, "0", "1");
    assertThat(reject("a", 0, 0)).containsExactly(letter(0, 0, 1));
    assertThat(ack("a", 0, 1)).isEmpty();
    assertThat(on(new PartitionsRevoked<>(List.of(0)))).isEmpty();
    // Not written again by a replica that is giving the partition up, and not committed past:
    // both records were sent, and both are the next owner's.
    assertThat(on(new DeadLetterFailed<>(0, 0)))
        .containsExactly(new PartitionReleased<>(0, true, 2));
    assertThat(core.committable(0)).isEmpty();
    assertThat(on(new DeadLetterWritten<>(0, 0))).isEmpty();
  }

  @Test
  void aDeadLetterWaitingToBeWrittenAgainIsNotWaitedForOnHandOver() {
    maxAttempts = 1;
    onExhausted = OnExhausted.DEAD_LETTER;
    retry = 30;
    retryMax = 100;
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 10);
    fetch(0, "0");
    assertThat(reject("a", 0, 0)).containsExactly(letter(0, 0, 1));
    assertThat(on(new DeadLetterFailed<>(0, 0))).isEmpty();
    assertThat(on(new PartitionsRevoked<>(List.of(0))))
        .containsExactly(new PartitionReleased<>(0, true, 1));
    assertThat(core.nextDeadlineNanos()).isEmpty();
    assertThat(after(1000)).isEmpty();
  }

  // -------------------------------------------------------------------------------------------
  // Waits
  // -------------------------------------------------------------------------------------------

  @Test
  void aFailedRecordWaitsLongerEachTimeBeforeItIsSentAgain() {
    backoff = 10;
    backoffMax = 35;
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 100);
    assertThat(fetch(0, "0")).containsExactly(deliver("a", 0, 0));
    long[] waits = {10, 20, 35, 35};
    for (int failed = 1; failed <= waits.length; failed++) {
      assertThat(reject("a", 0, 0)).as("after %s failed attempts", failed).isEmpty();
      assertThat(core.nextDeadlineNanos()).isEqualTo(OptionalLong.of(now + waits[failed - 1]));
      assertThat(core.inFlight()).isZero();
      assertThat(core.committable(0)).isEqualTo(OptionalLong.of(0));
      assertThat(after(waits[failed - 1] - 1)).isEmpty();
      assertThat(after(1)).containsExactly(deliver("a", 0, 0, failed + 1));
    }
    assertThat(ack("a", 0, 0)).containsExactly(commit(0, 1));
    assertThat(core.nextDeadlineNanos()).isEmpty();
  }

  @Test
  void aMissedDeadlineWaitsToo() {
    backoff = 10;
    backoffMax = 10;
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 10);
    receiver("b", 10);
    fetch(0, "0");
    assertThat(after(DEADLINE)).isEmpty();
    assertThat(after(9)).isEmpty();
    assertThat(raw(new TimePassed<>())).isEmpty();
    now += 1;
    assertThat(raw(new TimePassed<>()))
        .containsExactly(new Deliver<>("b", 0, 0, "0/0", 2, Cause.DEADLINE));
  }

  @Test
  void whileARecordWaitsItsKeyWaitsBehindItAndOtherKeysGoOn() {
    backoff = 50;
    backoffMax = 50;
    start(Ordering.KEY);
    assign(0, 0);
    receiver("a", 100);
    assertThat(fetch(0, "0:x", "1:x", "2:y", "3"))
        .containsExactly(deliver("a", 0, 0), deliver("a", 0, 2), deliver("a", 0, 3));
    assertThat(reject("a", 0, 0)).isEmpty();
    // The other keys are not held up, in what is fetched and in what follows an acknowledgement.
    assertThat(fetch(0, "4:y", "5:z", "6:x")).containsExactly(deliver("a", 0, 5));
    assertThat(ack("a", 0, 2)).containsExactly(deliver("a", 0, 4));
    assertThat(ack("a", 0, 3, 5)).isEmpty();
    // Nothing is committed past the record that waits, and its key waits behind it, in order.
    assertThat(core.committable(0)).isEqualTo(OptionalLong.of(0));
    assertThat(after(49)).isEmpty();
    assertThat(after(1)).containsExactly(deliver("a", 0, 0, 2));
    assertThat(ack("a", 0, 0)).containsExactly(commit(0, 1), deliver("a", 0, 1));
    assertThat(ack("a", 0, 1)).containsExactly(commit(0, 6), deliver("a", 0, 6));
  }

  @Test
  void whileARecordWaitsItsPartitionWaitsUnderPartitionOrdering() {
    backoff = 50;
    backoffMax = 50;
    start(Ordering.PARTITION);
    assign(0, 0);
    assign(1, 0);
    receiver("a", 100);
    assertThat(fetch(0, "0", "1")).containsExactly(deliver("a", 0, 0));
    assertThat(reject("a", 0, 0)).isEmpty();
    assertThat(fetch(0, "2")).isEmpty();
    // The other partition is not concerned.
    assertThat(fetch(1, "0")).containsExactly(deliver("a", 1, 0));
    assertThat(after(50)).containsExactly(deliver("a", 0, 0, 2));
    assertThat(ack("a", 0, 0)).containsExactly(commit(0, 1), deliver("a", 0, 1));
  }

  @Test
  void whileARecordWaitsTheWindowStillBoundsWhatIsSent() {
    backoff = 50;
    backoffMax = 50;
    window = 3;
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 100);
    assertThat(fetch(0, "0", "1", "2", "3", "4"))
        .containsExactly(deliver("a", 0, 0), deliver("a", 0, 1), deliver("a", 0, 2));
    assertThat(core.atWindow()).isEqualTo(1);
    assertThat(core.windowPauses()).isEqualTo(1);
    assertThat(reject("a", 0, 0)).isEmpty();
    assertThat(ack("a", 0, 1, 2)).isEmpty();
    assertThat(core.atWindow()).isEqualTo(1);
    assertThat(after(50)).containsExactly(deliver("a", 0, 0, 2));
    assertThat(ack("a", 0, 0))
        .containsExactly(commit(0, 3), deliver("a", 0, 3), deliver("a", 0, 4));
    assertThat(core.atWindow()).isZero();
    assertThat(core.windowPauses()).isEqualTo(1);
  }

  @Test
  void aRecordWhoseReceiverLeftDoesNotWait() {
    backoff = 50;
    backoffMax = 50;
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 10);
    receiver("b", 10);
    fetch(0, "0");
    assertThat(raw(new ReceiverLeft<>("a")))
        .containsExactly(new Deliver<>("b", 0, 0, "0/0", 1, Cause.RECEIVER_LEFT));
    assertThat(raw(new Rejected<>("b", 0, 0, ""))).isEmpty();
    now += 50;
    assertThat(raw(new TimePassed<>()))
        .containsExactly(new Deliver<>("b", 0, 0, "0/0", 2, Cause.REJECTED));
  }

  @Test
  void aLateAcknowledgementEndsTheWait() {
    backoff = 50;
    backoffMax = 50;
    start(Ordering.KEY);
    assign(0, 0);
    receiver("a", 10);
    assertThat(fetch(0, "0:x", "1:x")).containsExactly(deliver("a", 0, 0));
    assertThat(after(DEADLINE)).isEmpty();
    // The receiver still had it, and finished.
    assertThat(ack("a", 0, 0)).containsExactly(commit(0, 1), deliver("a", 0, 1));
    assertThat(core.nextDeadlineNanos()).isEqualTo(OptionalLong.of(now + DEADLINE));
    assertThat(ack("a", 0, 1)).containsExactly(commit(0, 2));
    assertThat(after(1000)).isEmpty();
  }

  @Test
  void aRecordThatWaitsIsDroppedByAHandOverAndByALoss() {
    backoff = 50;
    backoffMax = 50;
    start(Ordering.NONE);
    assign(0, 0);
    assign(1, 0);
    receiver("a", 10);
    fetch(0, "0");
    fetch(1, "0");
    assertThat(reject("a", 0, 0)).isEmpty();
    assertThat(reject("a", 1, 0)).isEmpty();
    assertThat(on(new PartitionsRevoked<>(List.of(0))))
        .containsExactly(new PartitionReleased<>(0, true, 1));
    assertThat(on(new PartitionsLost<>(List.of(1)))).isEmpty();
    assertThat(core.nextDeadlineNanos()).isEmpty();
    assertThat(after(1000)).isEmpty();
  }

  @Test
  void theLastAttemptDoesNotWaitBeforeItIsGivenUpOn() {
    backoff = 50;
    backoffMax = 50;
    maxAttempts = 2;
    onExhausted = OnExhausted.DEAD_LETTER;
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 10);
    fetch(0, "0");
    assertThat(reject("a", 0, 0)).isEmpty();
    assertThat(after(50)).containsExactly(deliver("a", 0, 0, 2));
    assertThat(reject("a", 0, 0)).containsExactly(letter(0, 0, 2));
  }

  @Test
  void aDeadLetterThatFailsIsWrittenAgainAfterAWaitThatGrowsAndNothingIsCommittedPastIt() {
    maxAttempts = 1;
    onExhausted = OnExhausted.DEAD_LETTER;
    retry = 10;
    retryMax = 25;
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 10);
    assertThat(fetch(0, "0", "1")).hasSize(2);
    assertThat(reject("a", 0, 0)).containsExactly(letter(0, 0, 1));
    assertThat(ack("a", 0, 1)).isEmpty();
    long[] waits = {10, 20, 25, 25};
    for (int failures = 1; failures <= waits.length; failures++) {
      assertThat(on(new DeadLetterFailed<>(0, 0))).isEmpty();
      // A second answer to the same write changes nothing.
      assertThat(on(new DeadLetterFailed<>(0, 0))).isEmpty();
      assertThat(core.committable(0)).isEqualTo(OptionalLong.of(0));
      assertThat(core.nextDeadlineNanos()).isEqualTo(OptionalLong.of(now + waits[failures - 1]));
      assertThat(after(waits[failures - 1] - 1)).isEmpty();
      assertThat(after(1))
          .containsExactly(new DeadLetter<>(0, 0, "0/0", 1, Cause.REJECTED, "bad", failures));
    }
    assertThat(on(new DeadLetterWritten<>(0, 0))).containsExactly(commit(0, 2));
    assertThat(core.nextDeadlineNanos()).isEmpty();
  }

  @Test
  void aDeadLetterSaysHowTheLastAttemptFailed() {
    maxAttempts = 2;
    onExhausted = OnExhausted.DEAD_LETTER;
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 10);
    fetch(0, "0", "1");
    assertThat(raw(new Rejected<>("a", 0, 0, "cannot decode")))
        .containsExactly(new Deliver<>("a", 0, 0, "0/0", 2, Cause.REJECTED));
    now += DEADLINE;
    // Rejected first and timed out last: the dead letter says the last, and not what the sidecar
    // said of the attempt before.
    assertThat(raw(new TimePassed<>()))
        .containsExactly(
            new DeadLetter<>(0, 0, "0/0", 2, Cause.DEADLINE, null, 0),
            new Deliver<>("a", 0, 1, "0/1", 2, Cause.DEADLINE));
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

  // -------------------------------------------------------------------------------------------
  // Timings
  // -------------------------------------------------------------------------------------------

  @Test
  void aRecordSentAtTheFirstOpportunityWaitedForNothing() {
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 10);
    now = 40;
    fetch(0, "0", "1");
    assertThat(timed).containsExactly("queued 0 NOTHING", "queued 0 NOTHING");
  }

  @Test
  void aRecordReadyWithNoReceiverWithCreditWaitedForCredit() {
    start(Ordering.KEY);
    assign(0, 0);
    on(new ReceiverJoined<>("a"));
    fetch(0, "0:k", "1:j");
    now = 7;
    assertThat(credits("a", 1)).containsExactly(deliver("a", 0, 0));
    now = 9;
    assertThat(credits("a", 1)).containsExactly(deliver("a", 0, 1));
    assertThat(timed).containsExactly("queued 7 CREDIT", "queued 9 CREDIT");
  }

  @Test
  void aRecordBehindItsKeyWaitedForTheOrdering() {
    start(Ordering.KEY);
    assign(0, 0);
    receiver("a", 10);
    fetch(0, "0:k", "1:k");
    now = 5;
    assertThat(ack("a", 0, 0)).containsExactly(commit(0, 1), deliver("a", 0, 1));
    assertThat(timed).containsExactly("queued 0 NOTHING", "acked 5", "queued 5 ORDERING");
  }

  @Test
  void aRecordBehindItsPartitionWaitedForTheOrdering() {
    start(Ordering.PARTITION);
    assign(0, 0);
    receiver("a", 10);
    fetch(0, "0", "1");
    now = 4;
    assertThat(ack("a", 0, 0)).containsExactly(commit(0, 1), deliver("a", 0, 1));
    assertThat(timed).containsExactly("queued 0 NOTHING", "acked 4", "queued 4 ORDERING");
  }

  @Test
  void aRecordHeldBackByTheWindowWaitedForTheWindow() {
    window = 1;
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 10);
    fetch(0, "0", "1");
    now = 6;
    assertThat(ack("a", 0, 0)).containsExactly(commit(0, 1), deliver("a", 0, 1));
    // Held back as soon as it is fetched.
    fetch(0, "2");
    now = 8;
    assertThat(ack("a", 0, 1)).containsExactly(commit(0, 2), deliver("a", 0, 2));
    assertThat(timed)
        .containsExactly(
            "queued 0 NOTHING", "acked 6", "queued 6 WINDOW", "acked 2", "queued 2 WINDOW");
  }

  @Test
  void aRecordThatWaitedForSeveralThingsIsPutDownToTheLongestAndTheTotalIsKept() {
    start(Ordering.KEY);
    assign(0, 0);
    receiver("a", 1);
    fetch(0, "0:k", "1:k");
    // Behind its key for 3, then without credit for 5.
    now = 3;
    assertThat(ack("a", 0, 0)).containsExactly(commit(0, 1));
    now = 8;
    assertThat(credits("a", 1)).containsExactly(deliver("a", 0, 1));
    // Behind its key for 4, then without credit for 1.
    fetch(0, "2:k");
    now = 12;
    assertThat(ack("a", 0, 1)).containsExactly(commit(0, 2));
    now = 13;
    assertThat(credits("a", 1)).containsExactly(deliver("a", 0, 2));
    assertThat(timed)
        .containsExactly(
            "queued 0 NOTHING", "acked 3", "queued 8 CREDIT", "acked 4", "queued 5 ORDERING");
  }

  @Test
  void onlyTheFirstDeliveryIsQueuedAndOnlyTheHoldersAnswerIsTimed() {
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 10);
    receiver("b", 10);
    fetch(0, "0", "1");
    now = 2;
    // Offset 0 goes again, to b.
    reject("a", 0, 0);
    now = 3 + DEADLINE;
    // Both offsets, with b, pass their deadline and go back to a.
    assertThat(after(0)).containsExactly(deliver("a", 0, 0, 3), deliver("a", 0, 1, 2));
    // b answers after its deadline: nothing is timed, since the records were sent again.
    ack("b", 0, 0, 1);
    assertThat(timed).containsExactly("queued 0 NOTHING", "queued 0 NOTHING", "rejected 2");
  }

  @Test
  void aReadyRecordBehindABlockedOneWaitedForTheOrderingNotForCredit() {
    maxAttempts = 1;
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 1);
    assertThat(fetch(0, "0", "1")).containsExactly(deliver("a", 0, 0));
    // 1 waits for credit until 0 blocks the partition at its deadline, then behind 0.
    now = DEADLINE;
    assertThat(on(new TimePassed<>())).containsExactly(new PartitionBlocked<>(0, 0));
    now = DEADLINE + 10;
    assertThat(credits("a", 5)).isEmpty();
    now = 300;
    assertThat(ack("a", 0, 0))
        .containsExactly(new PartitionUnblocked<>(0), commit(0, 1), deliver("a", 0, 1));
    // 100 without credit, 200 behind the blocked record: the total, put down to the ordering.
    assertThat(timed).containsExactly("queued 0 NOTHING", "queued 300 ORDERING");
  }

  @Test
  void blockedTimeIsOrderingTimeOnAClockWhoseValuesAreNegative() {
    // System.nanoTime() may be negative: the same scenario, every moment shifted below zero.
    long origin = -1_000_000;
    now = origin;
    maxAttempts = 1;
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 1);
    assertThat(fetch(0, "0", "1")).containsExactly(deliver("a", 0, 0));
    now = origin + DEADLINE;
    assertThat(on(new TimePassed<>())).containsExactly(new PartitionBlocked<>(0, 0));
    now = origin + DEADLINE + 10;
    assertThat(credits("a", 5)).isEmpty();
    now = origin + 300;
    assertThat(ack("a", 0, 0))
        .containsExactly(new PartitionUnblocked<>(0), commit(0, 1), deliver("a", 0, 1));
    assertThat(timed).containsExactly("queued 0 NOTHING", "queued 300 ORDERING");
  }

  @Test
  void answersAfterTheDeadlineAreAcceptedAndNotTimedEvenBeforeTheTimeIsPassed() {
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 10);
    fetch(0, "0", "1");
    // The deadline has passed, and the core has not been told yet.
    now = DEADLINE + 5;
    assertThat(ack("a", 0, 0)).containsExactly(commit(0, 1));
    assertThat(reject("a", 0, 1)).containsExactly(deliver("a", 0, 1, 2));
    now = DEADLINE + 10;
    assertThat(ack("a", 0, 1)).containsExactly(commit(0, 2));
    assertThat(timed).containsExactly("queued 0 NOTHING", "queued 0 NOTHING", "acked 5");
  }

  @Test
  void answersFromAReceiverSentAgainWhatItHeldExpiredAreAcceptedAndNotTimed() {
    start(Ordering.NONE);
    assign(0, 0);
    receiver("a", 10);
    fetch(0, "0", "1");
    now = DEADLINE;
    // The only receiver gets both again, and still holds the first copies.
    assertThat(on(new TimePassed<>()))
        .containsExactly(deliver("a", 0, 0, 2), deliver("a", 0, 1, 2));
    now = DEADLINE + 10;
    assertThat(ack("a", 0, 0)).containsExactly(commit(0, 1));
    assertThat(reject("a", 0, 1)).containsExactly(deliver("a", 0, 1, 3));
    // A third copy, while no expired one is held: its answer is timed again.
    now = DEADLINE + 15;
    assertThat(ack("a", 0, 1)).containsExactly(commit(0, 2));
    assertThat(timed).containsExactly("queued 0 NOTHING", "queued 0 NOTHING", "acked 5");
  }
}
