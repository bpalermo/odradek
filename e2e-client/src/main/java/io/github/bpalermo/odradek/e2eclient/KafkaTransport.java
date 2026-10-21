package io.github.bpalermo.odradek.e2eclient;

import io.github.bpalermo.odradek.config.v1.ServerConfig;
import io.github.bpalermo.odradek.server.tls.CredentialException;
import io.github.bpalermo.odradek.server.tls.TlsSources;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Records with the Kafka clients in this process: a producer, and a consumer in a group, configured
 * as the server configures its own ({@link KafkaClients}).
 *
 * <p>The consumer works as the server's does for an app: records are handled one at a time on the
 * poll thread, which polls every 50 ms; what was handled is committed asynchronously every {@code
 * commitInterval}, synchronously for partitions being revoked and when the consumer closes. A
 * handler that throws closes the consumer after that commit, so the next consumer of the group
 * resumes at the record that failed.
 */
public final class KafkaTransport implements Transport {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaTransport.class);

  /** How long one poll waits for records: the server's. */
  static final Duration POLL = Duration.ofMillis(50);

  private final Settings.Kafka kafka;
  private final TlsSources tls;
  private final Map<String, Object> producerProperties;
  private final Map<String, Object> consumerProperties;

  private KafkaTransport(
      Settings.Kafka kafka,
      TlsSources tls,
      Map<String, Object> producerProperties,
      Map<String, Object> consumerProperties) {
    this.kafka = kafka;
    this.tls = tls;
    this.producerProperties = producerProperties;
    this.consumerProperties = consumerProperties;
  }

  /**
   * Reads the overrides and loads the TLS credentials. With a Workload API, waits (up to two
   * minutes, as the server) for the first SVID.
   *
   * @throws IOException if the overrides file cannot be read
   * @throws CredentialException if the credentials cannot be loaded
   */
  public static KafkaTransport create(Settings settings)
      throws IOException, CredentialException, InterruptedException {
    Settings.Kafka kafka = settings.kafka();
    Map<String, String> overrides = KafkaClients.overrides(kafka.propertiesFile());
    String clientId = "e2e-client-" + settings.origin();
    Map<String, Object> producer = KafkaClients.producer(kafka, clientId, overrides);
    Map<String, Object> consumer = KafkaClients.consumer(kafka, clientId, overrides);
    ServerConfig config = KafkaClients.tlsConfig(kafka);
    TlsSources tls = config == null ? TlsSources.none() : TlsSources.create(config);
    tls.apply(KafkaClients.CLUSTER, producer);
    tls.apply(KafkaClients.CLUSTER, consumer);
    return new KafkaTransport(kafka, tls, producer, consumer);
  }

  @Override
  public String name() {
    return "kafka";
  }

  /** The values the producer and the consumer end up with, for the results. */
  public Map<String, Object> describe() {
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("producer", KafkaClients.effective(producerProperties, true));
    out.put("consumer", KafkaClients.effective(consumerProperties, false));
    return out;
  }

  @Override
  public Sender sender() {
    KafkaProducer<byte[], byte[]> producer =
        new KafkaProducer<>(
            new HashMap<>(producerProperties),
            new ByteArraySerializer(),
            new ByteArraySerializer());
    return new Sender() {
      @Override
      public CompletableFuture<Outcome> send(String topic, String key, byte[] value)
          throws NotSentException {
        CompletableFuture<Outcome> result = new CompletableFuture<>();
        try {
          producer.send(
              new ProducerRecord<>(topic, key.getBytes(StandardCharsets.UTF_8), value),
              (metadata, error) -> result.complete(error == null ? Outcome.ACKED : outcome(error)));
        } catch (IllegalStateException e) {
          throw new NotSentException("CLIENT_CLOSED", e);
        } catch (KafkaException e) {
          result.complete(outcome(e));
        }
        return result;
      }

      @Override
      public void close() {
        producer.close(Duration.ofMinutes(1));
      }
    };
  }

  /**
   * A failed send: unknown when it timed out or could have been retried (the record may have been
   * written), failed otherwise. A full buffer means the record was never taken.
   */
  static Outcome outcome(Exception error) {
    Outcome.Kind kind =
        error instanceof BufferExhaustedException
            ? Outcome.Kind.FAILED
            : error instanceof TimeoutException || error instanceof RetriableException
                ? Outcome.Kind.UNKNOWN
                : Outcome.Kind.FAILED;
    return new Outcome(kind, error.getClass().getSimpleName(), String.valueOf(error.getMessage()));
  }

  @Override
  public Subscription subscribe(
      String topic,
      Duration maxProcessing,
      int maxUnacked,
      Handler handler,
      Consumer<Throwable> failed) {
    PollLoop loop = new PollLoop(topic, handler, failed);
    Thread thread = new Thread(loop, "kafka-consumer-" + topic);
    thread.setDaemon(true);
    thread.start();
    return () -> {
      loop.closing = true;
      try {
        thread.join(TimeUnit.SECONDS.toMillis(30));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };
  }

  /** One consumer, on its own thread, from subscription to close. */
  private final class PollLoop implements Runnable, ConsumerRebalanceListener {
    private final String topic;
    private final Handler handler;
    private final Consumer<Throwable> failed;

    /** The next offset to commit of each partition owned, once its records were handled. */
    private final Map<TopicPartition, Long> handled = new HashMap<>();

    /** What the last successful commit of each partition was. */
    private final Map<TopicPartition, Long> committed = new HashMap<>();

    private KafkaConsumer<byte[], byte[]> consumer;
    volatile boolean closing;

    PollLoop(String topic, Handler handler, Consumer<Throwable> failed) {
      this.topic = topic;
      this.handler = handler;
      this.failed = failed;
    }

    @Override
    public void run() {
      Throwable failure;
      try {
        consumer =
            new KafkaConsumer<>(
                new HashMap<>(consumerProperties),
                new ByteArrayDeserializer(),
                new ByteArrayDeserializer());
        consumer.subscribe(List.of(topic), this);
        failure = loop();
      } catch (RuntimeException e) {
        failure = e;
      } finally {
        if (consumer != null) {
          try {
            commitSync(handled.keySet());
          } catch (RuntimeException e) {
            LOG.warn("committing before closing the consumer of {} failed", topic, e);
          }
          consumer.close();
        }
      }
      if (failure != null) {
        failed.accept(failure);
      }
    }

    /** Polls and handles until closed; returns what the handler threw, if it did. */
    private Throwable loop() {
      long interval = kafka.commitInterval().toNanos();
      long lastCommit = System.nanoTime();
      while (!closing) {
        for (ConsumerRecord<byte[], byte[]> record : consumer.poll(POLL)) {
          if (closing) {
            // What was fetched and not handled is not committed: the next owner reads it.
            return null;
          }
          try {
            handler.handle(
                new Delivered(
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.key(),
                    record.value()));
          } catch (Exception e) {
            return e;
          }
          handled.put(new TopicPartition(record.topic(), record.partition()), record.offset() + 1);
        }
        if (System.nanoTime() - lastCommit >= interval) {
          lastCommit = System.nanoTime();
          commitAsync();
        }
      }
      return null;
    }

    private Map<TopicPartition, OffsetAndMetadata> moved(Collection<TopicPartition> partitions) {
      Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
      for (TopicPartition partition : partitions) {
        Long next = handled.get(partition);
        if (next != null && !next.equals(committed.get(partition))) {
          offsets.put(partition, new OffsetAndMetadata(next));
        }
      }
      return offsets;
    }

    /** Commits the partitions whose position moved. A failure is retried at the next interval. */
    private void commitAsync() {
      Map<TopicPartition, OffsetAndMetadata> offsets = moved(handled.keySet());
      if (offsets.isEmpty()) {
        return;
      }
      consumer.commitAsync(
          offsets,
          (done, error) -> {
            if (error == null) {
              done.forEach((partition, offset) -> committed.put(partition, offset.offset()));
            }
          });
    }

    private void commitSync(Collection<TopicPartition> partitions) {
      Map<TopicPartition, OffsetAndMetadata> offsets = moved(List.copyOf(partitions));
      if (!offsets.isEmpty()) {
        consumer.commitSync(offsets);
        offsets.forEach((partition, offset) -> committed.put(partition, offset.offset()));
      }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {}

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
      try {
        commitSync(partitions);
      } catch (RuntimeException e) {
        LOG.warn("committing revoked partitions {} failed", partitions, e);
      }
      forget(partitions);
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
      forget(partitions);
    }

    private void forget(Collection<TopicPartition> partitions) {
      partitions.forEach(
          partition -> {
            handled.remove(partition);
            committed.remove(partition);
          });
    }
  }

  @Override
  public void close() {
    tls.close();
  }
}
