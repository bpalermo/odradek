package io.github.bpalermo.odradek.e2eclient;

import io.github.bpalermo.odradek.client.Consumer;
import io.github.bpalermo.odradek.client.ConsumerFailedException;
import io.github.bpalermo.odradek.client.ConsumerListener;
import io.github.bpalermo.odradek.client.ConsumerOptions;
import io.github.bpalermo.odradek.client.OdradekClient;
import io.github.bpalermo.odradek.client.ReceivedRecord;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Consumer mode: subscribes to the topic, checks every record against the ledger, and acknowledges
 * it once processed.
 *
 * <p>A record is failed on purpose by throwing from the handler, the only way the Java client has
 * to leave a record unacknowledged: the consumer then stops, the record and everything not yet
 * acknowledged are delivered again, and this run starts a new consumer after {@code restartDelay}.
 * Through a dispatcher, a failure counts towards {@code maxAttempts} only when the processing
 * deadline passed first, hence {@code failHold}; a record whose consumer merely went away is
 * delivered again without using an attempt.
 */
public final class ConsumerRun extends Run {
  /** What the handler throws for a record it fails on purpose. */
  static final class DeliberateFailure extends Exception {
    private static final long serialVersionUID = 1L;

    DeliberateFailure(String message) {
      super(message);
    }
  }

  private final ConsumerLedger ledger;
  private final Faults faults;

  public ConsumerRun(Settings settings, Events events) {
    super(settings, events);
    this.ledger = new ConsumerLedger(events, settings.soleConsumer());
    this.faults = Faults.of(settings);
  }

  public ConsumerLedger ledger() {
    return ledger;
  }

  @Override
  protected Map<String, Object> body() {
    return ledger.results();
  }

  @Override
  public void run(OdradekClient client) throws InterruptedException {
    ConsumerOptions options =
        new ConsumerOptions(
            List.of(settings.topic()), settings.maxProcessing(), settings.maxUnacked());
    try {
      while (!stopping()) {
        CompletableFuture<Throwable> failure = new CompletableFuture<>();
        state("running");
        Consumer consumer =
            client.consumer(
                options,
                this::handle,
                new ConsumerListener() {
                  @Override
                  public void failed(Throwable cause) {
                    failure.complete(cause);
                  }
                });
        try {
          CompletableFuture.anyOf(failure, stopped()).get();
        } catch (ExecutionException e) {
          throw new IllegalStateException("cannot happen: neither future fails", e);
        } finally {
          // Lets the record in progress finish, and the server commit what was acknowledged.
          state(stopping() ? "stopping" : "restarting");
          consumer.close();
        }
        if (stopping()) {
          break;
        }
        Throwable cause = failure.getNow(null);
        if (cause instanceof DeliberateFailure) {
          ledger.restarted();
        } else if (cause instanceof ConsumerFailedException refused) {
          ledger.consumerFailed(refused.statusCode(), refused.getMessage());
        } else if (cause != null) {
          ledger.consumerFailed(
              cause.getClass().getSimpleName(), String.valueOf(cause.getMessage()));
        }
        pause(settings.restartDelay());
      }
    } finally {
      state("stopped");
    }
  }

  /** The record handler, on the consumer's thread. Returning acknowledges the record. */
  private void handle(ReceivedRecord record) throws Exception {
    long now = System.currentTimeMillis();
    RecordValue value;
    try {
      value = RecordValue.decode(record.value());
      if (value.seq() < 1 || value.seq() > ConsumerLedger.MAX_SEQ) {
        throw new IllegalArgumentException("seq " + value.seq() + " is out of range");
      }
    } catch (IllegalArgumentException e) {
      ledger.malformed(where(record, e.getMessage()));
      return;
    }
    if (!value.valid()) {
      ledger.checksumFailure(where(record, "checksum " + value.checksum() + " does not match"));
      return;
    }
    String kafkaKey =
        record.key() == null ? null : new String(record.key(), StandardCharsets.UTF_8);
    if (!value.key().equals(kafkaKey)) {
      ledger.malformed(where(record, "the record key " + kafkaKey + " is not " + value.key()));
      return;
    }
    if (!settings.runId().isEmpty() && !settings.runId().equals(value.runId())) {
      ledger.foreignRun();
      return;
    }

    ConsumerLedger.Delivery delivery =
        ledger.delivered(value.runId(), value.key(), value.seq(), value.producerTs(), now);
    if (faults.shouldFail(value.key(), value.seq(), delivery.failuresSoFar())) {
      // Cut short by a stop: the record is failed all the same, and delivered again.
      pause(settings.failHold());
      ledger.failed(value.runId(), value.key(), value.seq());
      throw new DeliberateFailure(
          "failing " + value.runId() + "/" + value.key() + "/" + value.seq() + " on purpose");
    }
    pause(settings.handlerDelay());
    ledger.processed(value.runId(), value.key(), value.seq());
  }

  private static Map<String, Object> where(ReceivedRecord record, String problem) {
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("topic", record.topic());
    out.put("partition", record.partition());
    out.put("offset", record.offset());
    out.put("problem", problem);
    return out;
  }
}
