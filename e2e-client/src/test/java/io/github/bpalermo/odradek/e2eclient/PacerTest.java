package io.github.bpalermo.odradek.e2eclient;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class PacerTest {
  private static final long MS = 1_000_000L;

  /**
   * The producer's loop on simulated time, with one key whose every acknowledgement takes {@code
   * ackMs}: the milliseconds at which records are sent, until {@code limit}.
   */
  private static List<Long> sends(String schedule, long ackMs, int limit) {
    Pacer pacer = new Pacer(RateSchedule.parse(schedule));
    List<Long> sent = new ArrayList<>();
    long now = 0;
    long acked = 0;
    while (sent.size() < limit) {
      long due = pacer.due();
      if (due < 0) {
        break;
      }
      now = Math.max(now, due);
      // Waits for the previous record's outcome.
      now = Math.max(now, acked);
      if (!pacer.admit(now)) {
        break;
      }
      pacer.sent();
      sent.add(now / MS);
      acked = now + ackMs * MS;
    }
    return sent;
  }

  @Test
  void aSteadyRateSendsOneIntervalApart() {
    assertThat(sends("10:0", 0, 5)).containsExactly(0L, 100L, 200L, 300L, 400L);
  }

  @Test
  void aStepThatEndsWhileWaitingForAnAcknowledgementSendsNoMore() {
    // Slow acknowledgements: the second record would go at 1 s, when the step has ended.
    assertThat(sends("5:1s", 1000, 100)).containsExactly(0L);
    assertThat(sends("5:1s", 300, 100)).containsExactly(0L, 300L, 600L, 900L);
  }

  @Test
  void aStepShorterThanAnIntervalOfTheOneBeforeIsNotSkipped() {
    // At 1/s the next deadline would be 1 s, past the end of both steps.
    assertThat(sends("1:500ms,10:500ms", 0, 100)).containsExactly(0L, 500L, 600L, 700L, 800L, 900L);
  }

  @Test
  void deadlinesOfAStepThatEndedAreDropped() {
    // Slow acknowledgements in a 10/s step: the 20/s step starts afresh at its beginning instead
    // of working off the backlog of the first step's deadlines.
    List<Long> sent = sends("10:1s,20:0", 700, 4);
    assertThat(sent).containsExactly(0L, 700L, 1400L, 2100L);
    Pacer pacer = new Pacer(RateSchedule.parse("10:1s,20:0"));
    pacer.admit(0);
    pacer.sent();
    // Sent at 1.4 s: the next record is due 50 ms after the second step began, not at 0.2 s.
    assertThat(pacer.admit(1400 * MS)).isTrue();
    pacer.sent();
    assertThat(pacer.due()).isEqualTo(1050 * MS);
  }

  @Test
  void aStepBoundaryIsAlwaysADeadline() {
    // 3/s does not divide 1 s: the step's last interval is cut short at its end, where the next
    // step's first record is due.
    assertThat(sends("3:1s,1:0", 0, 6)).containsExactly(0L, 333L, 666L, 999L, 1000L, 2000L);
  }

  @Test
  void theScheduleEndsAtTheEndOfItsLastStep() {
    assertThat(sends("2:1s", 0, 100)).containsExactly(0L, 500L);
  }
}
