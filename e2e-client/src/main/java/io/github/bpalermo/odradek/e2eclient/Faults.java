package io.github.bpalermo.odradek.e2eclient;

import java.util.Set;

/**
 * Which records the consumer fails on purpose.
 *
 * @param failEvery records whose {@code seq} is a multiple of this fail; 0 for none
 * @param failTimes how many times each of them fails before it succeeds
 * @param poisonKeys keys whose records always fail
 */
public record Faults(long failEvery, int failTimes, Set<String> poisonKeys) {
  public Faults {
    poisonKeys = Set.copyOf(poisonKeys);
  }

  public static Faults of(Settings settings) {
    return new Faults(settings.failEvery(), settings.failTimes(), settings.poisonKeys());
  }

  /**
   * Whether to fail this delivery of a record.
   *
   * @param failuresSoFar how many times this process has already failed the record
   */
  public boolean shouldFail(String key, long seq, int failuresSoFar) {
    if (poisonKeys.contains(key)) {
      return true;
    }
    return failEvery > 0 && seq % failEvery == 0 && failuresSoFar < failTimes;
  }
}
