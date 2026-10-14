package io.github.bpalermo.odradek.e2eclient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ConsumerLedgerTest {
  private static final String RUN = "run-1";

  private final List<Map<String, Object>> anomalies = new ArrayList<>();
  private final ConsumerLedger ledger =
      new ConsumerLedger(
          (event, fields) -> {
            assertThat(event).isEqualTo("anomaly");
            anomalies.add(fields);
          },
          true);

  /** Delivers and processes a record, as the handler does when it does not fail it. */
  private ConsumerLedger.Delivery process(String key, long seq, long nowMs) {
    ConsumerLedger.Delivery delivery = ledger.delivered(RUN, key, seq, nowMs - 5, nowMs);
    ledger.processed(RUN, key, seq);
    return delivery;
  }

  private ConsumerLedger.Delivery fail(String key, long seq) {
    ConsumerLedger.Delivery delivery = ledger.delivered(RUN, key, seq, 0, 1);
    ledger.failed(RUN, key, seq);
    return delivery;
  }

  private Map<String, Object> results() {
    return ledger.results();
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> key(String key) {
    return ((List<Map<String, Object>>) results().get("keys"))
        .stream().filter(k -> k.get("key").equals(key)).findFirst().orElseThrow();
  }

  @Test
  void recordsInOrderAreClean() {
    for (long seq = 1; seq <= 5; seq++) {
      assertThat(process("a", seq, 100 + seq).kind()).isEqualTo(ConsumerLedger.Kind.FIRST);
      assertThat(process("b", seq, 100 + seq).kind()).isEqualTo(ConsumerLedger.Kind.FIRST);
    }

    Map<String, Object> results = results();
    assertThat(results)
        .containsEntry("clean", true)
        .containsEntry("delivered", 10L)
        .containsEntry("processed", 10L)
        .containsEntry("duplicates", 0L)
        .containsEntry("gaps", 0L)
        .containsEntry("missing", 0L)
        .containsEntry("reorders", 0L);
    assertThat(key("a"))
        .containsEntry("highestSeen", 5L)
        .containsEntry("processedRanges", List.of(List.of(1L, 5L)))
        .containsEntry("missingRanges", List.of());
    assertThat(anomalies).isEmpty();
  }

  @Test
  void aSeqAboveTheNextIsAGapAndWhatItSkippedIsMissing() {
    process("a", 1, 10);
    process("a", 4, 20);

    assertThat(anomalies)
        .singleElement()
        .satisfies(
            anomaly ->
                assertThat(anomaly)
                    .containsEntry("type", "gap")
                    .containsEntry("seq", 4L)
                    .containsEntry("expected", 2L)
                    .containsEntry("missing", 2L));
    assertThat(results())
        .containsEntry("clean", false)
        .containsEntry("gaps", 1L)
        .containsEntry("missing", 2L);
    assertThat(key("a")).containsEntry("missingRanges", List.of(List.of(2L, 3L)));
  }

  @Test
  void aKeyThatStartsAboveOneHasAGap() {
    process("a", 3, 10);

    assertThat(results()).containsEntry("gaps", 1L).containsEntry("missing", 2L);
  }

  @Test
  void aSeqBelowTheHighestSeenIsAReorderWithItsDepth() {
    process("a", 1, 10);
    process("a", 2, 10);
    process("a", 5, 10);
    process("a", 3, 10);
    process("a", 4, 10);

    assertThat(anomalies)
        .extracting(anomaly -> anomaly.get("type"))
        .containsExactly("gap", "reorder", "reorder");
    assertThat(anomalies.get(1)).containsEntry("depth", 2L).containsEntry("highestSeen", 5L);
    assertThat(results())
        .containsEntry("clean", false)
        .containsEntry("reorders", 2L)
        .containsEntry("longestReorder", 2L)
        .containsEntry("missing", 0L);
    assertThat(key("a"))
        .containsEntry("longestReorder", 2L)
        .containsEntry("processedRanges", List.of(List.of(1L, 5L)));
  }

  @Test
  void theLongestReorderIsKeptPerKey() {
    process("a", 1, 10);
    process("a", 10, 10);
    process("a", 2, 10);
    process("a", 9, 10);
    process("b", 1, 10);
    process("b", 3, 10);
    process("b", 2, 10);

    assertThat(key("a")).containsEntry("longestReorder", 8L).containsEntry("reorders", 2L);
    assertThat(key("b")).containsEntry("longestReorder", 1L).containsEntry("reorders", 1L);
    assertThat(results()).containsEntry("longestReorder", 8L).containsEntry("reorders", 3L);
  }

  @Test
  void aProcessedRecordDeliveredAgainIsADuplicateWithBothTimes() {
    process("a", 1, 1000);
    process("a", 2, 1001);
    ConsumerLedger.Delivery again = process("a", 1, 5000);

    assertThat(again.kind()).isEqualTo(ConsumerLedger.Kind.DUPLICATE);
    assertThat(anomalies)
        .singleElement()
        .satisfies(
            anomaly ->
                assertThat(anomaly)
                    .containsEntry("type", "duplicate")
                    .containsEntry("runId", RUN)
                    .containsEntry("key", "a")
                    .containsEntry("seq", 1L)
                    .containsEntry("firstDeliveredMs", 1000L)
                    .containsEntry("deliveredAgainMs", 5000L));
    Map<String, Object> results = results();
    assertThat(results)
        .containsEntry("clean", false)
        .containsEntry("duplicates", 1L)
        .containsEntry("reorders", 0L)
        .containsEntry("delivered", 3L)
        .containsEntry("processed", 2L)
        .containsEntry("duplicateDetailsTruncated", false);
    assertThat((List<?>) results.get("duplicateDetails")).hasSize(1);
  }

  @Test
  void duplicatesAreTrackedPerRunAndKey() {
    ledger.delivered("r1", "a", 1, 0, 1);
    ledger.processed("r1", "a", 1);
    ledger.delivered("r2", "a", 1, 0, 1);
    ledger.processed("r2", "a", 1);
    ledger.delivered("r1", "b", 1, 0, 1);
    ledger.processed("r1", "b", 1);

    assertThat(results()).containsEntry("duplicates", 0L).containsEntry("clean", true);
    assertThat((List<?>) results().get("keys")).hasSize(3);
  }

  @Test
  void aRecordFailedOnPurposeAndDeliveredAgainIsExpected() {
    process("a", 1, 10);
    assertThat(fail("a", 2).failuresSoFar()).isZero();
    // Delivered again: not a duplicate, and not a reorder although 2 is the highest seen.
    ConsumerLedger.Delivery second = fail("a", 2);
    assertThat(second.kind()).isEqualTo(ConsumerLedger.Kind.REDELIVERY_AFTER_FAILURE);
    assertThat(second.failuresSoFar()).isEqualTo(1);
    ConsumerLedger.Delivery third = process("a", 2, 20);
    assertThat(third.kind()).isEqualTo(ConsumerLedger.Kind.REDELIVERY_AFTER_FAILURE);
    assertThat(third.failuresSoFar()).isEqualTo(2);
    process("a", 3, 30);

    assertThat(anomalies).isEmpty();
    assertThat(results())
        .containsEntry("clean", true)
        .containsEntry("expectedRedeliveries", 2L)
        .containsEntry("deliberateFailures", 2L)
        .containsEntry("unresolvedFailures", 0L)
        .containsEntry("duplicates", 0L)
        .containsEntry("reorders", 0L)
        .containsEntry("processed", 3L)
        .containsEntry("delivered", 5L);
  }

  @Test
  void aFailedRecordRedeliveredAfterLaterOnesIsNotAReorder() {
    // ORDERING_NONE through a dispatcher: the key goes on while its failed record waits.
    process("a", 1, 10);
    fail("a", 2);
    process("a", 3, 10);
    process("a", 4, 10);
    ConsumerLedger.Delivery back = process("a", 2, 10);

    assertThat(back.kind()).isEqualTo(ConsumerLedger.Kind.REDELIVERY_AFTER_FAILURE);
    assertThat(results())
        .containsEntry("reorders", 0L)
        .containsEntry("duplicates", 0L)
        .containsEntry("clean", true);
  }

  @Test
  void aRedeliveryAfterTheFailedRecordWasProcessedIsADuplicate() {
    fail("a", 1);
    process("a", 1, 10);
    assertThat(process("a", 1, 20).kind()).isEqualTo(ConsumerLedger.Kind.DUPLICATE);

    assertThat(results()).containsEntry("duplicates", 1L).containsEntry("expectedRedeliveries", 1L);
  }

  @Test
  void aPoisonRecordThatIsNeverProcessedIsUnresolvedAndTheNextOneIsInOrder() {
    process("p", 1, 10);
    fail("p", 2);
    fail("p", 2);
    fail("p", 2);
    // Dead-lettered: the dispatcher goes on with the next record of the key.
    process("p", 3, 10);

    assertThat(anomalies).isEmpty();
    assertThat(results())
        .containsEntry("clean", true)
        .containsEntry("unresolvedFailures", 1L)
        .containsEntry("gaps", 0L)
        .containsEntry("missing", 0L);
    assertThat(key("p"))
        .containsEntry("unresolvedRanges", List.of(List.of(2L, 2L)))
        .containsEntry("processedRanges", List.of(List.of(1L, 1L), List.of(3L, 3L)));
  }

  @Test
  void aRecordWhoseHandlingNeverFinishedIsAnExpectedRedelivery() {
    ledger.delivered(RUN, "a", 1, 0, 1);
    // The consumer went away while the handler had it.
    assertThat(process("a", 1, 2).kind()).isEqualTo(ConsumerLedger.Kind.REDELIVERY_AFTER_FAILURE);
    assertThat(results()).containsEntry("duplicates", 0L).containsEntry("clean", true);
  }

  @Test
  void withoutBeingTheSoleConsumerGapsAreCountedButNotReportedOrHeldAgainstTheRun() {
    List<Map<String, Object>> reported = new ArrayList<>();
    ConsumerLedger shared = new ConsumerLedger((event, fields) -> reported.add(fields), false);
    shared.delivered(RUN, "a", 2, 0, 1);
    shared.processed(RUN, "a", 2);
    shared.delivered(RUN, "a", 5, 0, 1);
    shared.processed(RUN, "a", 5);

    assertThat(reported).isEmpty();
    assertThat(shared.results())
        .containsEntry("gaps", 2L)
        .containsEntry("missing", 3L)
        .containsEntry("clean", true);
  }

  @Test
  void checksumFailuresMalformedRecordsAndOtherRunsAreCounted() {
    ledger.checksumFailure(Map.of("offset", 1L));
    ledger.malformed(Map.of("offset", 2L));
    ledger.foreignRun();

    assertThat(anomalies)
        .extracting(anomaly -> anomaly.get("type"))
        .containsExactly("checksum", "malformed");
    assertThat(results())
        .containsEntry("clean", false)
        .containsEntry("checksumFailures", 1L)
        .containsEntry("malformed", 1L)
        .containsEntry("foreignRun", 1L);
  }

  @Test
  void restartsAndConsumerFailuresAreCountedButAreNotAnomaliesOfTheRecords() {
    ledger.restarted();
    ledger.consumerFailed("ABORTED", "held too long");
    ledger.consumerFailed("ABORTED", "held too long");

    assertThat(results())
        .containsEntry("clean", true)
        .containsEntry("consumerRestarts", 1L)
        .containsEntry("consumerFailures", Map.of("ABORTED", 2L));
    assertThat(anomalies).hasSize(2).allMatch(a -> a.get("type").equals("consumer-failed"));
  }

  @Test
  void latencyIsMeasuredOnFirstDeliveries() {
    ledger.delivered(RUN, "a", 1, 1000, 1010);
    ledger.processed(RUN, "a", 1);
    ledger.delivered(RUN, "a", 2, 1000, 1030);
    ledger.processed(RUN, "a", 2);
    ledger.delivered(RUN, "a", 1, 1000, 9000);

    assertThat(results().get("latency"))
        .isEqualTo(Map.of("count", 2L, "meanMs", 20L, "maxMs", 30L));
  }

  @Test
  void onlyAFewDuplicatesAreListedAndAllAreCounted() {
    int count = ConsumerLedger.DETAIL_LIMIT + 5;
    for (long seq = 1; seq <= count; seq++) {
      process("a", seq, 1);
      process("a", seq, 2);
    }

    Map<String, Object> results = results();
    assertThat(results)
        .containsEntry("duplicates", (long) count)
        .containsEntry("duplicateDetailsTruncated", true);
    assertThat((List<?>) results.get("duplicateDetails")).hasSize(ConsumerLedger.DETAIL_LIMIT);
  }

  @Test
  void aSeqOutOfRangeIsRefused() {
    assertThatThrownBy(() -> ledger.delivered(RUN, "a", 0, 0, 0))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> ledger.delivered(RUN, "a", ConsumerLedger.MAX_SEQ + 1, 0, 0))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> ledger.processed(RUN, "a", 1))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void largeSeqsGrowTheTables() {
    process("a", 1, 1);
    process("a", 100_000, 1);

    assertThat(key("a"))
        .containsEntry("missingRanges", List.of(List.of(2L, 99_999L)))
        .containsEntry("missing", 99_998L);
  }
}
