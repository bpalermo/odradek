package io.github.bpalermo.odradek.e2eclient;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Producer mode: records at a set rate, or a schedule of rates, over a set of keys, round robin. A
 * record is sent only once the previous record of its key has its outcome, which is what Odradek's
 * ordering covers: a key's records reach Kafka in order however the stream breaks and resumes.
 */
public final class ProducerRun extends Run {
  private final ProducerLedger ledger;

  public ProducerRun(Settings settings, Events events) {
    this(settings, events, new Latency(settings.latencyWindow(), Metrics.none()));
  }

  public ProducerRun(Settings settings, Events events, Latency latency) {
    super(settings, events, latency);
    this.ledger = new ProducerLedger(events, settings.runId(), settings.origin());
  }

  public ProducerLedger ledger() {
    return ledger;
  }

  @Override
  protected Map<String, Object> body() {
    return ledger.results();
  }

  @Override
  public void run(Transport transport) throws InterruptedException {
    int keyCount = settings.keys();
    String[] keys = new String[keyCount];
    long[] nextSeq = new long[keyCount];
    List<CompletableFuture<Transport.Outcome>> pending =
        new ArrayList<>(Collections.nCopies(keyCount, null));
    for (int k = 0; k < keyCount; k++) {
      keys[k] = "key-" + k;
      nextSeq[k] = 1;
    }
    RateSchedule schedule = settings.rateSchedule();
    long start = System.nanoTime();
    boolean timed = !settings.duration().isZero();
    long end = start + settings.duration().toNanos();
    state("running");
    try (Transport.Sender sender = transport.sender()) {
      // When the next record is due, on System.nanoTime().
      long next = start;
      for (long i = 0; !stopping(); i++) {
        if (settings.count() > 0 && ledger.sent() >= settings.count()) {
          break;
        }
        // The rate of the moment this record is due: the schedule steps without a restart.
        long due = next;
        double rate = schedule.rateAt(Duration.ofNanos(due - start));
        if (rate == 0) {
          break;
        }
        next = due + Math.max(1, Math.round(1e9 / rate));
        if (!waitUntil(due)) {
          break;
        }
        if (timed && System.nanoTime() - end >= 0) {
          break;
        }
        int k = (int) (i % keyCount);
        if (!awaitOutcome(pending.get(k))) {
          break;
        }
        String key = keys[k];
        long seq = nextSeq[k];
        long nowMs = System.currentTimeMillis();
        long sentNanos = System.nanoTime();
        byte[] value =
            RecordValue.of(settings.runId(), key, seq, nowMs, settings.origin(), sentNanos)
                .encode();
        ledger.sent(key, seq, nowMs);
        CompletableFuture<Transport.Outcome> future;
        try {
          future = sender.send(settings.topic(), key, value);
        } catch (Transport.NotSentException e) {
          ledger.notSent(key, seq, e.reason());
          continue;
        }
        latency.metrics().sent();
        nextSeq[k]++;
        pending.set(k, future.thenApply(outcome -> outcome(key, seq, sentNanos, outcome)));
      }
      state(stopping() ? "stopping" : "draining");
    }
    // close() waited for the results of what was sent; what has none now may or may not be written.
    ledger.unanswered();
    state(stopping() ? "stopped" : "done");
  }

  private Transport.Outcome outcome(
      String key, long seq, long sentNanos, Transport.Outcome outcome) {
    switch (outcome.kind()) {
      case ACKED -> {
        latency.produce(System.nanoTime() - sentNanos);
        latency.metrics().acked();
        ledger.acked(key, seq);
      }
      case FAILED -> ledger.failed(key, seq, outcome.code(), outcome.message());
      case UNKNOWN -> ledger.unknown(key, seq, outcome.code(), outcome.message());
    }
    return outcome;
  }

  /** Waits until {@code System.nanoTime()} reaches {@code due}; false if stopped first. */
  private boolean waitUntil(long due) throws InterruptedException {
    long wait = due - System.nanoTime();
    if (wait <= 0) {
      return !stopping();
    }
    return !pause(Duration.ofNanos(wait));
  }

  /** Waits for a record's outcome, whatever it is; false if stopped first. */
  private boolean awaitOutcome(CompletableFuture<Transport.Outcome> future)
      throws InterruptedException {
    while (future != null && !future.isDone()) {
      if (stopping()) {
        return false;
      }
      try {
        future.get(100, TimeUnit.MILLISECONDS);
      } catch (ExecutionException | TimeoutException e) {
        // The outcome is the ledger's business; this only waits for it.
      }
    }
    return !stopping();
  }
}
