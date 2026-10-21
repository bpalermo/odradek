package io.github.bpalermo.odradek.e2eclient;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ProducerLedgerTest {
  private final List<Map<String, Object>> anomalies = new ArrayList<>();
  private final ProducerLedger ledger =
      new ProducerLedger((event, fields) -> anomalies.add(fields), "run-1", "pod-0-1a2b3c4d");

  @SuppressWarnings("unchecked")
  private Map<String, Object> key(String key) {
    return ((List<Map<String, Object>>) ledger.results().get("keys"))
        .stream().filter(k -> k.get("key").equals(key)).findFirst().orElseThrow();
  }

  @Test
  void acknowledgedRecordsAreClean() {
    ledger.sent("key-0", 1, 100);
    ledger.sent("key-1", 1, 100);
    ledger.acked("key-0", 1);
    ledger.acked("key-1", 1);
    ledger.sent("key-0", 2, 200);
    ledger.acked("key-0", 2);

    assertThat(ledger.results())
        .containsEntry("clean", true)
        .containsEntry("sent", 3L)
        .containsEntry("acked", 3L)
        .containsEntry("outstanding", 0L);
    assertThat(key("key-0"))
        .containsEntry("runId", "run-1")
        .containsEntry("origin", "pod-0-1a2b3c4d")
        .containsEntry("lastSentSeq", 2L)
        .containsEntry("lastAckedSeq", 2L);
    assertThat(anomalies).isEmpty();
  }

  @Test
  void failuresAreCountedByCodeAndListed() {
    ledger.sent("key-0", 1, 0);
    ledger.failed("key-0", 1, "ERROR_CODE_RECORD_TOO_LARGE", "too large");

    assertThat(ledger.results())
        .containsEntry("clean", false)
        .containsEntry("failed", 1L)
        .containsEntry("failedByCode", Map.of("ERROR_CODE_RECORD_TOO_LARGE", 1L));
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> failures = (List<Map<String, Object>>) key("key-0").get("failedSeqs");
    assertThat(failures)
        .singleElement()
        .satisfies(
            failure ->
                assertThat(failure)
                    .containsEntry("seq", 1L)
                    .containsEntry("code", "ERROR_CODE_RECORD_TOO_LARGE"));
    assertThat(anomalies)
        .singleElement()
        .satisfies(a -> assertThat(a).containsEntry("type", "produce-failed"));
  }

  @Test
  void anOutcomeIsCountedOnce() {
    ledger.sent("key-0", 1, 0);
    ledger.acked("key-0", 1);
    ledger.failed("key-0", 1, "X", "late");
    ledger.unknown("key-0", 1, "Y", "late");
    ledger.unanswered();

    assertThat(ledger.results())
        .containsEntry("acked", 1L)
        .containsEntry("failed", 0L)
        .containsEntry("unknown", 0L);
  }

  @Test
  void recordsWithNoOutcomeWhenClosedAreUnknown() {
    ledger.sent("key-0", 1, 0);
    ledger.acked("key-0", 1);
    ledger.sent("key-0", 2, 0);
    ledger.sent("key-1", 1, 0);
    ledger.unknown("key-1", 1, "ERROR_CODE_OUTCOME_UNKNOWN", "server restarted");
    assertThat(ledger.outstanding()).isEqualTo(1);
    ledger.unanswered();

    assertThat(ledger.results())
        .containsEntry("clean", false)
        .containsEntry("unknown", 2L)
        .containsEntry("outstanding", 0L)
        .containsEntry(
            "unknownByCode",
            Map.of("ERROR_CODE_OUTCOME_UNKNOWN", 1L, ProducerLedger.UNANSWERED, 1L));
    assertThat(anomalies).hasSize(2).allMatch(a -> a.get("type").equals("produce-unknown"));
  }

  @Test
  void aRecordTheProducerRefusedIsTakenBack() {
    ledger.sent("key-0", 1, 0);
    ledger.acked("key-0", 1);
    ledger.sent("key-0", 2, 0);
    ledger.notSent("key-0", 2, "BUFFER_FULL");

    assertThat(ledger.results())
        .containsEntry("clean", true)
        .containsEntry("sent", 1L)
        .containsEntry("notSent", 1L)
        .containsEntry("notSentByReason", Map.of("BUFFER_FULL", 1L));
    assertThat(key("key-0")).containsEntry("lastSentSeq", 1L).containsEntry("sent", 1L);
  }
}
