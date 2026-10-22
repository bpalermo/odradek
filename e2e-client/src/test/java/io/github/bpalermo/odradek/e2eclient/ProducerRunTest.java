package io.github.bpalermo.odradek.e2eclient;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class ProducerRunTest {
  /** A transport whose sender acknowledges every record at once and counts them. */
  private static final class Counting implements Transport {
    final AtomicInteger sent = new AtomicInteger();

    @Override
    public String name() {
      return "test";
    }

    @Override
    public Sender sender() {
      return new Sender() {
        @Override
        public CompletableFuture<Outcome> send(String topic, String key, byte[] value) {
          sent.incrementAndGet();
          return CompletableFuture.completedFuture(Outcome.ACKED);
        }

        @Override
        public void close() {}
      };
    }

    @Override
    public Subscription subscribe(
        String topic,
        Duration maxProcessing,
        int maxUnacked,
        Handler handler,
        Consumer<Throwable> failed) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() {}
  }

  @Test
  void aDurationEndsTheRunWhileItStillWaitsForAScheduleStartBeyondIt() throws Exception {
    Settings settings =
        Settings.parse(
            List.of(
                "--mode",
                "producer",
                "--topic",
                "t",
                "--duration",
                "300ms",
                "--schedule-start",
                Instant.now().plus(Duration.ofHours(1)).toString()),
            Map.of());
    Counting transport = new Counting();
    long begun = System.nanoTime();
    new ProducerRun(settings, (event, fields) -> {}).run(transport);
    long tookMs = (System.nanoTime() - begun) / 1_000_000;
    assertThat(transport.sent).hasValue(0);
    // It ends at the end of its duration, not an hour later at the schedule's start.
    assertThat(tookMs).isBetween(250L, 10_000L);
  }

  @Test
  void eachOriginGoesRoundTheKeysInAnOrderOfItsOwn() {
    int[] one = ProducerRun.keyOrder("e2e-lane-c-5bfzr-1a2b3c4d", 64);
    int[] other = ProducerRun.keyOrder("e2e-lane-c-bpwdk-5e6f7a8b", 64);
    // Every key once a round, for each.
    assertThat(Arrays.stream(one).sorted().toArray()).isEqualTo(IntStream.range(0, 64).toArray());
    // The same origin, the same order: a run is reproducible.
    assertThat(ProducerRun.keyOrder("e2e-lane-c-5bfzr-1a2b3c4d", 64)).isEqualTo(one);
    // Two processes in step send different keys at the same instant, nearly always.
    int same = 0;
    for (int i = 0; i < 64; i++) {
      if (one[i] == other[i]) {
        same++;
      }
    }
    assertThat(same).isLessThan(8);
  }
}
