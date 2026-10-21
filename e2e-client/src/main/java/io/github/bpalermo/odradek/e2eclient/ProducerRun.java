package io.github.bpalermo.odradek.e2eclient;

import java.time.Duration;
import java.time.Instant;
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
    // The schedule steps without a restart, on time elapsed since its start: now, or the instant
    // every process was given, placed on this process's monotonic clock. Before that instant the
    // first record waits for it; after it, this process joins at the step in force.
    long begun = System.nanoTime();
    long start = begun;
    long joinedAt = 0;
    if (settings.scheduleStart() != null) {
      long offset = Duration.between(settings.scheduleStart(), Instant.now()).toNanos();
      start = begun - offset;
      joinedAt = Math.max(0, offset);
    }
    Pacer pacer = new Pacer(settings.rateSchedule(), joinedAt);
    boolean timed = !settings.duration().isZero();
    long end = begun + settings.duration().toNanos();
    state("running");
    try (Transport.Sender sender = transport.sender()) {
      for (long i = 0; !stopping(); i++) {
        if (settings.count() > 0 && ledger.sent() >= settings.count()) {
          break;
        }
        long due = pacer.due();
        if (due < 0) {
          break;
        }
        // A record due after --duration ends is never sent, however far off it is: a shared
        // schedule start can lie past the end. The run then ends at its end, not at the record's.
        if (timed && start + due - end >= 0) {
          waitUntil(end);
          break;
        }
        if (!waitUntil(start + due)) {
          break;
        }
        int k = (int) (i % keyCount);
        if (!awaitOutcome(pending.get(k))) {
          break;
        }
        // The wait for the key's previous outcome may have outlasted the step, or the schedule.
        long now = System.nanoTime();
        if ((timed && now - end >= 0) || !pacer.admit(now - start)) {
          break;
        }
        pacer.sent();
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
