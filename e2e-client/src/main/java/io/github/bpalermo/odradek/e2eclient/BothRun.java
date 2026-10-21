package io.github.bpalermo.odradek.e2eclient;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Both modes in one process, over one transport: the producer sends at its rate while the consumer
 * reads the topic in its group. With every pod of a lane doing both, each pod times the records it
 * produced itself end to end on its own monotonic clock.
 *
 * <p>Runs until stopped, also once the producer has sent everything: the consumer goes on.
 */
public final class BothRun extends Run {
  private final ProducerRun producer;
  private final ConsumerRun consumer;

  public BothRun(Settings settings, Events events, Latency latency) {
    super(settings, events, latency);
    this.producer = new ProducerRun(settings, events, latency);
    this.consumer = new ConsumerRun(settings, events, latency);
  }

  public ProducerRun producer() {
    return producer;
  }

  public ConsumerRun consumer() {
    return consumer;
  }

  @Override
  public void run(Transport transport) throws InterruptedException {
    AtomicReference<Throwable> consumerError = new AtomicReference<>();
    Thread consuming =
        Thread.ofPlatform()
            .name("e2e-client-consumer")
            .start(
                () -> {
                  try {
                    consumer.run(transport);
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  } catch (RuntimeException e) {
                    consumerError.set(e);
                  }
                });
    state("running");
    try {
      producer.run(transport);
      if (!stopping()) {
        state("produced");
        awaitStop();
      }
    } finally {
      state("stopping");
      producer.stop();
      consumer.stop();
      consuming.join(TimeUnit.MINUTES.toMillis(1));
      state("stopped");
    }
    if (consumerError.get() != null) {
      throw new IllegalStateException("the consumer failed", consumerError.get());
    }
  }

  @Override
  public void stop() {
    super.stop();
    producer.stop();
    consumer.stop();
  }

  @Override
  protected Map<String, Object> body() {
    Map<String, Object> produced = producer.body();
    Map<String, Object> consumed = consumer.body();
    Map<String, Object> out = new LinkedHashMap<>();
    out.put(
        "clean",
        Boolean.TRUE.equals(produced.get("clean")) && Boolean.TRUE.equals(consumed.get("clean")));
    out.put("producerState", producer.state());
    out.put("consumerState", consumer.state());
    out.put("producer", produced);
    out.put("consumer", consumed);
    return out;
  }
}
