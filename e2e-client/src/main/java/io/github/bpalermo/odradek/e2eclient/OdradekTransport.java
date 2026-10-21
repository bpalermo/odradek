package io.github.bpalermo.odradek.e2eclient;

import io.github.bpalermo.odradek.client.BufferFullException;
import io.github.bpalermo.odradek.client.ClientClosedException;
import io.github.bpalermo.odradek.client.ConsumerListener;
import io.github.bpalermo.odradek.client.ConsumerOptions;
import io.github.bpalermo.odradek.client.OdradekClient;
import io.github.bpalermo.odradek.client.OutgoingRecord;
import io.github.bpalermo.odradek.client.ProduceException;
import io.github.bpalermo.odradek.client.Producer;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;

/** Records through the Odradek sidecar, on the Java client. */
public final class OdradekTransport implements Transport {
  private final OdradekClient client;
  private final boolean owned;

  private OdradekTransport(OdradekClient client, boolean owned) {
    this.client = client;
    this.owned = owned;
  }

  /** Over a client the caller made and will close. */
  public static OdradekTransport of(OdradekClient client) {
    return new OdradekTransport(client, false);
  }

  /** Over a client of its own, which {@link #close()} closes. */
  public static OdradekTransport connect(java.nio.file.Path socket) {
    return new OdradekTransport(OdradekClient.connect(socket), true);
  }

  @Override
  public String name() {
    return "odradek";
  }

  @Override
  public Sender sender() {
    Producer producer = client.producer();
    return new Sender() {
      @Override
      public CompletableFuture<Outcome> send(String topic, String key, byte[] value)
          throws NotSentException {
        try {
          return producer
              .send(OutgoingRecord.to(topic).key(key).value(value))
              .handle((result, error) -> error == null ? Outcome.ACKED : outcome(error));
        } catch (BufferFullException e) {
          throw new NotSentException("BUFFER_FULL", e);
        } catch (ClientClosedException e) {
          throw new NotSentException("CLIENT_CLOSED", e);
        }
      }

      @Override
      public void close() {
        producer.close();
      }
    };
  }

  static Outcome outcome(Throwable error) {
    if (error instanceof CompletionException && error.getCause() != null) {
      error = error.getCause();
    }
    if (error instanceof ProduceException produce) {
      return new Outcome(
          produce.outcomeUnknown() ? Outcome.Kind.UNKNOWN : Outcome.Kind.FAILED,
          produce.code().name(),
          produce.getMessage());
    }
    return new Outcome(
        Outcome.Kind.UNKNOWN, error.getClass().getSimpleName(), String.valueOf(error.getMessage()));
  }

  @Override
  public Subscription subscribe(
      String topic,
      Duration maxProcessing,
      int maxUnacked,
      Handler handler,
      Consumer<Throwable> failed) {
    io.github.bpalermo.odradek.client.Consumer consumer =
        client.consumer(
            new ConsumerOptions(List.of(topic), maxProcessing, maxUnacked),
            record ->
                handler.handle(
                    new Delivered(
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        record.key(),
                        record.value())),
            new ConsumerListener() {
              @Override
              public void failed(Throwable cause) {
                failed.accept(cause);
              }
            });
    return consumer::close;
  }

  @Override
  public void close() {
    if (owned) {
      client.close();
    }
  }
}
