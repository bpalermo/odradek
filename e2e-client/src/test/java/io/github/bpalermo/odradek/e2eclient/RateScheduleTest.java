package io.github.bpalermo.odradek.e2eclient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

class RateScheduleTest {
  @Test
  void stepsFollowEachOtherAndALastStepOfZeroHoldsForGood() {
    RateSchedule schedule = RateSchedule.parse("20:10m, 50:5m,100:5m,200:5m,20:0");

    assertThat(schedule.steps())
        .containsExactly(
            new RateSchedule.Step(20, Duration.ofMinutes(10)),
            new RateSchedule.Step(50, Duration.ofMinutes(5)),
            new RateSchedule.Step(100, Duration.ofMinutes(5)),
            new RateSchedule.Step(200, Duration.ofMinutes(5)),
            new RateSchedule.Step(20, Duration.ZERO));
    assertThat(schedule.rateAt(Duration.ZERO)).isEqualTo(20);
    assertThat(schedule.rateAt(Duration.ofMinutes(10).minusNanos(1))).isEqualTo(20);
    assertThat(schedule.rateAt(Duration.ofMinutes(10))).isEqualTo(50);
    assertThat(schedule.rateAt(Duration.ofMinutes(16))).isEqualTo(100);
    assertThat(schedule.rateAt(Duration.ofMinutes(24))).isEqualTo(200);
    assertThat(schedule.rateAt(Duration.ofMinutes(25))).isEqualTo(20);
    assertThat(schedule.rateAt(Duration.ofDays(30))).isEqualTo(20);
  }

  @Test
  void aScheduleWhoseLastStepHasADurationEnds() {
    RateSchedule schedule = RateSchedule.parse("5:1s,2.5:500ms");

    assertThat(schedule.rateAt(Duration.ofMillis(999))).isEqualTo(5);
    assertThat(schedule.rateAt(Duration.ofMillis(1200))).isEqualTo(2.5);
    assertThat(schedule.rateAt(Duration.ofMillis(1500))).isZero();
  }

  @Test
  void aConstantRateIsOneStepForGood() {
    assertThat(RateSchedule.constant(10).steps())
        .containsExactly(new RateSchedule.Step(10, Duration.ZERO));
    assertThat(RateSchedule.constant(10).rateAt(Duration.ofHours(100))).isEqualTo(10);
  }

  @Test
  void writtenAsRead() {
    assertThat(RateSchedule.parse("20:10m,2.5:1.5s,20:0"))
        .hasToString("20:600000ms,2.5:1500ms,20:0");
    assertThat(RateSchedule.parse(RateSchedule.parse("20:10m,50:0").toString()))
        .isEqualTo(
            new RateSchedule(
                List.of(
                    new RateSchedule.Step(20, Duration.ofMinutes(10)),
                    new RateSchedule.Step(50, Duration.ZERO))));
  }

  @Test
  void mistakesAreRefused() {
    for (String text : List.of("", "20", "20:", "x:1m", "0:1m", "-1:1m", "20:0,50:1m", "20:1y")) {
      assertThatThrownBy(() -> RateSchedule.parse(text))
          .as(text)
          .isInstanceOf(IllegalArgumentException.class);
    }
  }
}
