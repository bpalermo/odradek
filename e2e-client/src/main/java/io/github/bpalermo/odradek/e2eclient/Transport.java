package io.github.bpalermo.odradek.e2eclient;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * How records reach Kafka and come back: through the Odradek sidecar ({@link OdradekTransport}), or
 * with the Kafka clients in this process ({@link KafkaTransport}). The runs work the same over
 * either.
 */
public interface Transport extends AutoCloseable {
  /** {@code odradek} or {@code kafka}: what the results and the metrics call it. */
  String name();

  /** Opens a producer. */
  Sender sender();

  /**
   * Starts a consumer of {@code topic} that hands records to {@code handler}, one at a time.
   * Returning from the handler acknowledges the record; throwing stops the consumer, and {@code
   * failed} is told why. So is a consumer stopped for another reason.
   *
   * @param maxProcessing the processing deadline asked of the sidecar; zero for its default
   * @param maxUnacked records delivered ahead of the handler; 0 for the client's default
   */
  Subscription subscribe(
      String topic,
      Duration maxProcessing,
      int maxUnacked,
      Handler handler,
      Consumer<Throwable> failed);

  @Override
  void close();

  /** A producer. */
  interface Sender extends AutoCloseable {
    /**
     * Sends a record. The future always completes normally, with the record's outcome.
     *
     * @throws NotSentException if the producer refused to take it
     */
    CompletableFuture<Outcome> send(String topic, String key, byte[] value) throws NotSentException;

    /** Waits, up to a minute, for the outcomes of what was sent, then closes. */
    @Override
    void close();
  }

  /**
   * What became of a record sent.
   *
   * @param code why it failed or is unknown; empty when acknowledged
   */
  record Outcome(Kind kind, String code, String message) {
    /** Acknowledged, failed (not written), or unknown (perhaps written). */
    public enum Kind {
      ACKED,
      FAILED,
      UNKNOWN
    }

    static final Outcome ACKED = new Outcome(Kind.ACKED, "", "");
  }

  /** The producer would not take a record, which was therefore not sent. */
  final class NotSentException extends Exception {
    private static final long serialVersionUID = 1L;

    private final String reason;

    public NotSentException(String reason, Throwable cause) {
      super(reason, cause);
      this.reason = reason;
    }

    /** {@code BUFFER_FULL} or {@code CLIENT_CLOSED}. */
    public String reason() {
      return reason;
    }
  }

  /** A record handed to the handler. */
  record Delivered(String topic, int partition, long offset, byte[] key, byte[] value) {}

  /** Processes records; see {@link #subscribe}. */
  @FunctionalInterface
  interface Handler {
    void handle(Delivered record) throws Exception;
  }

  /** A consumer started by {@link #subscribe}. */
  interface Subscription extends AutoCloseable {
    /**
     * Lets the record in progress finish, commits what was acknowledged, and leaves the group.
     * Records delivered and not handled are not acknowledged.
     */
    @Override
    void close();
  }
}
