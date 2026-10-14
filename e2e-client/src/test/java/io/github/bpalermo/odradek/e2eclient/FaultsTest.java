package io.github.bpalermo.odradek.e2eclient;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;
import org.junit.jupiter.api.Test;

class FaultsTest {
  @Test
  void noFaultsFailNothing() {
    Faults faults = new Faults(0, 1, Set.of());

    assertThat(faults.shouldFail("key-0", 10, 0)).isFalse();
  }

  @Test
  void everyNthRecordFailsMTimesThenSucceeds() {
    Faults faults = new Faults(5, 2, Set.of());

    assertThat(faults.shouldFail("key-0", 4, 0)).isFalse();
    assertThat(faults.shouldFail("key-0", 5, 0)).isTrue();
    assertThat(faults.shouldFail("key-0", 5, 1)).isTrue();
    assertThat(faults.shouldFail("key-0", 5, 2)).isFalse();
    assertThat(faults.shouldFail("key-1", 10, 0)).isTrue();
  }

  @Test
  void poisonKeysAlwaysFail() {
    Faults faults = new Faults(0, 1, Set.of("key-3"));

    assertThat(faults.shouldFail("key-3", 1, 0)).isTrue();
    assertThat(faults.shouldFail("key-3", 1, 1000)).isTrue();
    assertThat(faults.shouldFail("key-2", 1, 0)).isFalse();
  }
}
