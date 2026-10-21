package io.github.bpalermo.odradek.e2eclient;

import io.github.bpalermo.odradek.client.ConsumerFailedException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
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
 * delivered again without using an attempt. The {@code kafka} transport does the same with a Kafka
 * consumer: it commits what was handled and leaves the group.
 *
 * <p>The handler times each first delivery from its producer's send: on this process's monotonic
 * clock when this process produced it, and on the two wall clocks for every record.
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
    this(settings, events, new Latency(settings.latencyWindow(), Metrics.none()));
  }

  public ConsumerRun(Settings settings, Events events, Latency latency) {
    super(settings, events, latency);
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
  public void run(Transport transport) throws InterruptedException {
    try {
      while (!stopping()) {
        CompletableFuture<Throwable> failure = new CompletableFuture<>();
        state("running");
        Transport.Subscription consumer =
            transport.subscribe(
                settings.topic(),
                settings.maxProcessing(),
                settings.maxUnacked(),
                this::handle,
                failure::complete);
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
  private void handle(Transport.Delivered record) throws Exception {
    long startNanos = System.nanoTime();
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
    if (!settings.trackedRunId().isEmpty() && !settings.trackedRunId().equals(value.runId())) {
      ledger.foreignRun();
      return;
    }

    ConsumerLedger.Delivery delivery =
        ledger.delivered(value.runId(), value.origin(), value.key(), value.seq(), now);
    if (delivery.kind() == ConsumerLedger.Kind.FIRST) {
      if (value.origin().equals(settings.origin())) {
        latency.endToEndSelf(startNanos - value.producerNanos());
      }
      latency.endToEndCross(now - value.producerTs());
    }
    if (faults.shouldFail(value.key(), value.seq(), delivery.failuresSoFar())) {
      // Cut short by a stop: the record is failed all the same, and delivered again.
      pause(settings.failHold());
      ledger.failed(value.runId(), value.origin(), value.key(), value.seq());
      throw new DeliberateFailure(
          "failing " + value.runId() + "/" + value.key() + "/" + value.seq() + " on purpose");
    }
    pause(settings.handlerDelay());
    if (ledger.processed(value.runId(), value.origin(), value.key(), value.seq())) {
      latency.metrics().processed();
    }
  }

  private static Map<String, Object> where(Transport.Delivered record, String problem) {
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("topic", record.topic());
    out.put("partition", record.partition());
    out.put("offset", record.offset());
    out.put("problem", problem);
    return out;
  }
}
