package io.github.bpalermo.odradek.e2eclient;

import io.github.bpalermo.odradek.client.BufferFullException;
import io.github.bpalermo.odradek.client.ClientClosedException;
import io.github.bpalermo.odradek.client.OdradekClient;
import io.github.bpalermo.odradek.client.OutgoingRecord;
import io.github.bpalermo.odradek.client.ProduceException;
import io.github.bpalermo.odradek.client.Producer;
import io.github.bpalermo.odradek.client.SendResult;
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
 * Producer mode: records at a set rate over a set of keys, round robin. A record is sent only once
 * the previous record of its key has its outcome, which is what Odradek's ordering covers: a key's
 * records reach Kafka in order however the stream breaks and resumes.
 */
public final class ProducerRun extends Run {
  private final ProducerLedger ledger;

  public ProducerRun(Settings settings, Events events) {
    super(settings, events);
    this.ledger = new ProducerLedger(events, settings.runId());
  }

  public ProducerLedger ledger() {
    return ledger;
  }

  @Override
  protected Map<String, Object> body() {
    return ledger.results();
  }

  @Override
  public void run(OdradekClient client) throws InterruptedException {
    int keyCount = settings.keys();
    String[] keys = new String[keyCount];
    long[] nextSeq = new long[keyCount];
    List<CompletableFuture<SendResult>> pending =
        new ArrayList<>(Collections.nCopies(keyCount, null));
    for (int k = 0; k < keyCount; k++) {
      keys[k] = "key-" + k;
      nextSeq[k] = 1;
    }
    long interval = Math.max(1, Math.round(1e9 / settings.rate()));
    long start = System.nanoTime();
    boolean timed = !settings.duration().isZero();
    long end = start + settings.duration().toNanos();
    state("running");
    try (Producer producer = client.producer()) {
      for (long i = 0; !stopping(); i++) {
        if (settings.count() > 0 && ledger.sent() >= settings.count()) {
          break;
        }
        if (!waitUntil(start + i * interval)) {
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
        long now = System.currentTimeMillis();
        byte[] value = RecordValue.of(settings.runId(), key, seq, now).encode();
        ledger.sent(key, seq, now);
        CompletableFuture<SendResult> future;
        try {
          future = producer.send(OutgoingRecord.to(settings.topic()).key(key).value(value));
        } catch (BufferFullException e) {
          ledger.notSent(key, seq, "BUFFER_FULL");
          continue;
        } catch (ClientClosedException e) {
          ledger.notSent(key, seq, "CLIENT_CLOSED");
          continue;
        }
        nextSeq[k]++;
        future.whenComplete((result, error) -> outcome(key, seq, error));
        pending.set(k, future);
      }
      state(stopping() ? "stopping" : "draining");
    }
    // close() waited for the results of what was sent; what has none now may or may not be written.
    ledger.unanswered();
    state(stopping() ? "stopped" : "done");
  }

  private void outcome(String key, long seq, Throwable error) {
    long now = System.currentTimeMillis();
    if (error == null) {
      ledger.acked(key, seq, now);
    } else if (error instanceof ProduceException produce) {
      if (produce.outcomeUnknown()) {
        ledger.unknown(key, seq, produce.code().name(), produce.getMessage());
      } else {
        ledger.failed(key, seq, produce.code().name(), produce.getMessage());
      }
    } else {
      ledger.unknown(
          key, seq, error.getClass().getSimpleName(), String.valueOf(error.getMessage()));
    }
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
  private boolean awaitOutcome(CompletableFuture<SendResult> future) throws InterruptedException {
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
