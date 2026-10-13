package io.github.bpalermo.odradek.server.receive;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class BackoffTest {
  @Test
  void theDelayDoublesUpToFiveSeconds() {
    // With the largest draw each time, the delay is the whole step.
    Backoff backoff = new Backoff(bound -> bound - 1);
    List<Long> delays = new ArrayList<>();
    for (int i = 0; i < 9; i++) {
      delays.add(backoff.nextMs());
    }
    assertThat(delays).containsExactly(100L, 200L, 400L, 800L, 1600L, 3200L, 5000L, 5000L, 5000L);
  }

  @Test
  void aDelayIsNeverLessThanHalfItsStep() {
    Backoff backoff = new Backoff(bound -> 0);
    assertThat(backoff.nextMs()).isEqualTo(50);
    assertThat(backoff.nextMs()).isEqualTo(100);
    assertThat(backoff.nextMs()).isEqualTo(200);
  }

  @Test
  void delaysAreSpreadOverTheUpperHalfOfTheStep() {
    Backoff backoff = new Backoff();
    long least = Long.MAX_VALUE;
    long most = 0;
    for (int i = 0; i < 2000; i++) {
      long delay = backoff.nextMs();
      backoff.reset();
      assertThat(delay).isBetween(50L, 100L);
      least = Math.min(least, delay);
      most = Math.max(most, delay);
    }
    // Two sidecars that lose the dispatcher together do not come back together.
    assertThat(most - least).isGreaterThan(25);
  }

  @Test
  void aStreamThatWasAcceptedStartsOver() {
    Backoff backoff = new Backoff(bound -> bound - 1);
    backoff.nextMs();
    backoff.nextMs();
    backoff.nextMs();
    backoff.reset();
    assertThat(backoff.nextMs()).isEqualTo(100);
  }
}
