package io.github.bpalermo.odradek.e2eclient;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * The producer's rate over time: steps of a rate held for a duration, one after the other. A last
 * step whose duration is zero holds for good; a schedule whose last step has a duration ends there,
 * and the producer stops sending.
 *
 * <p>Written {@code rate:duration,…}, e.g. {@code 20:10m,50:5m,100:5m,20:0}.
 *
 * @param steps at least one; only the last may have a zero duration
 */
public record RateSchedule(List<Step> steps) {
  /**
   * One step.
   *
   * @param rate records per second, over all keys
   * @param duration how long it holds; zero for good (last step only)
   */
  public record Step(double rate, Duration duration) {}

  public RateSchedule {
    steps = List.copyOf(steps);
    if (steps.isEmpty()) {
      throw new IllegalArgumentException("a rate schedule needs at least one step");
    }
    for (int i = 0; i < steps.size() - 1; i++) {
      if (steps.get(i).duration().isZero()) {
        throw new IllegalArgumentException(
            "only the last step of a rate schedule may hold for good (duration 0)");
      }
    }
  }

  /** One rate, for good. */
  public static RateSchedule constant(double rate) {
    return new RateSchedule(List.of(new Step(rate, Duration.ZERO)));
  }

  /**
   * Reads {@code rate:duration,…}.
   *
   * @throws IllegalArgumentException with a message for the user
   */
  public static RateSchedule parse(String text) {
    List<Step> steps = new ArrayList<>();
    for (String item : text.split(",", -1)) {
      String step = item.trim();
      int colon = step.indexOf(':');
      if (colon < 0) {
        throw new IllegalArgumentException(
            "a step is rate:duration, e.g. 50:5m or 20:0, not \"" + step + "\"");
      }
      double rate;
      try {
        rate = Double.parseDouble(step.substring(0, colon).trim());
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("not a rate: " + step.substring(0, colon), e);
      }
      if (!(rate > 0) || Double.isInfinite(rate)) {
        throw new IllegalArgumentException("a rate must be above 0, not " + rate);
      }
      steps.add(new Step(rate, Settings.parseDuration(step.substring(colon + 1).trim())));
    }
    return new RateSchedule(steps);
  }

  /** The rate at {@code elapsed} since the start; 0 once a schedule that ends has ended. */
  public double rateAt(Duration elapsed) {
    long at = elapsed.toNanos();
    for (Step step : steps) {
      long length = step.duration().toNanos();
      if (length == 0 || at < length) {
        return step.rate();
      }
      at -= length;
    }
    return 0;
  }

  /** As written. */
  @Override
  public String toString() {
    return steps.stream()
        .map(
            step -> String.format(Locale.ROOT, "%s:%s", number(step.rate()), text(step.duration())))
        .collect(Collectors.joining(","));
  }

  private static String number(double value) {
    return value == Math.rint(value) ? Long.toString((long) value) : Double.toString(value);
  }

  private static String text(Duration duration) {
    return duration.isZero() ? "0" : duration.toMillis() + "ms";
  }
}
