package io.github.bpalermo.odradek.server.dispatch;

import io.github.bpalermo.odradek.config.v1.ServerConfig;
import io.github.bpalermo.odradek.server.dispatch.DispatchAction.Cause;
import io.github.bpalermo.odradek.server.produce.ClusterProducers;
import io.github.bpalermo.odradek.server.telemetry.ServerMetrics;
import io.github.bpalermo.odradek.server.tls.TlsSources;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes dead letters: the records a dispatcher gave up on, to the topic their consumers named
 * ({@code ON_EXHAUSTED_DEAD_LETTER}), on the cluster they were read from.
 *
 * <p><b>One producer per cluster, of the dispatcher's own, made when the first dead letter of that
 * cluster is written.</b> A dispatcher that never dead-letters holds no producer: no buffer, no
 * connection, and its principal needs no right to write anything. The producer is built as the
 * server's other producers are ({@link ClusterProducers#properties}: the cluster's properties,
 * {@code acks=all}, idempotence, the cluster's {@code tls}, the Kafka metrics reporter), and is not
 * the one a server that also produces for an app has for the same cluster: a dead-letter topic that
 * cannot be written must not hold up an app's records, nor the reverse.
 *
 * <p><b>A write never runs on the caller's thread.</b> {@code KafkaProducer.send} waits, for up to
 * {@code max.block.ms}, for the metadata of a topic it does not know and for room in its buffer,
 * and a dispatcher's group thread must not wait for anything. Each cluster has one thread that
 * calls {@code send}; the answer comes back on the producer's own thread. Writes to one cluster are
 * therefore sent in the order they were asked for, and a topic whose metadata cannot be had (it
 * does not exist and the brokers do not create it) holds the cluster's other dead letters back for
 * {@code max.block.ms} each time it is tried.
 *
 * <p><b>What a dead letter is.</b> The record as stored (the dispatcher decodes nothing): the same
 * key and value bytes and the same headers in the same order, followed by the headers below. It has
 * no partition of its own choosing: the producer's partitioner places it by its key, as it would
 * any record. Its timestamp is the time of the write, not the record's own, so that the retention
 * of the dead-letter topic counts from when the record was given up on; the record's own timestamp
 * is in a header.
 *
 * <p>The header names are a contract with whoever reads dead-letter topics. Values are UTF-8 text;
 * numbers are decimal.
 *
 * <table>
 *   <caption>Headers added to a dead letter</caption>
 *   <tr><th>Header</th><th>Value</th></tr>
 *   <tr><td>{@value #SOURCE_CLUSTER}</td><td>the cluster, as the dispatcher's configuration names
 *       it</td></tr>
 *   <tr><td>{@value #SOURCE_TOPIC}</td><td>the topic the record was consumed from</td></tr>
 *   <tr><td>{@value #SOURCE_PARTITION}</td><td>its partition there</td></tr>
 *   <tr><td>{@value #SOURCE_OFFSET}</td><td>its offset there</td></tr>
 *   <tr><td>{@value #SOURCE_TIMESTAMP}</td><td>its timestamp there, in milliseconds since the
 *       epoch; absent when the record has none</td></tr>
 *   <tr><td>{@value #SOURCE_GROUP}</td><td>the consumer group that gave it up</td></tr>
 *   <tr><td>{@value #ATTEMPTS}</td><td>how many attempts failed, as the replica that wrote the
 *       dead letter counted them</td></tr>
 *   <tr><td>{@value #FAILURE}</td><td>how the last attempt failed: {@code rejected} (a sidecar
 *       could not decode the record) or {@code deadline} (the processing deadline passed)</td></tr>
 *   <tr><td>{@value #FAILURE_DETAIL}</td><td>what the sidecar said when it rejected the record,
 *       cut at {@value #MAX_DETAIL} characters; absent when it said nothing, and after a missed
 *       deadline. Not stable: do not match on it</td></tr>
 * </table>
 *
 * <p>A record that already carries headers of these names (a dead letter that was consumed from its
 * dead-letter topic and given up on again) keeps them: the new ones follow, so the last header of
 * each name is the one of this write.
 */
public final class DeadLetters {
  private static final Logger LOG = LoggerFactory.getLogger(DeadLetters.class);

  public static final String SOURCE_CLUSTER = "odradek-source-cluster";
  public static final String SOURCE_TOPIC = "odradek-source-topic";
  public static final String SOURCE_PARTITION = "odradek-source-partition";
  public static final String SOURCE_OFFSET = "odradek-source-offset";
  public static final String SOURCE_TIMESTAMP = "odradek-source-timestamp";
  public static final String SOURCE_GROUP = "odradek-source-group";
  public static final String ATTEMPTS = "odradek-attempts";
  public static final String FAILURE = "odradek-failure";
  public static final String FAILURE_DETAIL = "odradek-failure-detail";

  /** The longest {@link #FAILURE_DETAIL}, in characters. */
  public static final int MAX_DETAIL = 1024;

  /** Creates the producer of one cluster. Tests substitute a mock. */
  @FunctionalInterface
  public interface Factory {
    Producer<byte[], byte[]> create(String cluster, Map<String, Object> properties);
  }

  /** The producer of one cluster and the thread that sends on it. */
  private static final class Writer {
    final ExecutorService sender;

    /** Made by the first write, on the sender's thread; read by {@link #close} after it ended. */
    volatile Producer<byte[], byte[]> producer;

    Writer(String cluster) {
      sender =
          Executors.newSingleThreadExecutor(
              task -> {
                Thread thread = new Thread(task, "odradek-dead-letter-" + cluster);
                thread.setDaemon(true);
                return thread;
              });
    }
  }

  private final ServerConfig config;
  private final TlsSources tls;
  private final ServerMetrics metrics;
  private final Factory factory;
  private final Map<String, Writer> writers = new HashMap<>();
  private boolean closed;

  public DeadLetters(ServerConfig config, TlsSources tls, ServerMetrics metrics) {
    this(
        config,
        tls,
        metrics,
        (cluster, properties) ->
            new KafkaProducer<>(properties, new ByteArraySerializer(), new ByteArraySerializer()));
  }

  DeadLetters(ServerConfig config, TlsSources tls, ServerMetrics metrics, Factory factory) {
    this.config = config;
    this.tls = tls;
    this.metrics = metrics;
    this.factory = factory;
  }

  /**
   * The dead letter of a record: see the class comment.
   *
   * @param topic the dead-letter topic
   * @param source the record as it was fetched
   * @param detail what the sidecar that rejected it last said, or null
   */
  static ProducerRecord<byte[], byte[]> record(
      String topic,
      String cluster,
      String group,
      ConsumerRecord<byte[], byte[]> source,
      int attempts,
      Cause cause,
      String detail) {
    RecordHeaders headers = new RecordHeaders();
    for (Header header : source.headers()) {
      headers.add(header.key(), header.value());
    }
    add(headers, SOURCE_CLUSTER, cluster);
    add(headers, SOURCE_TOPIC, source.topic());
    add(headers, SOURCE_PARTITION, Integer.toString(source.partition()));
    add(headers, SOURCE_OFFSET, Long.toString(source.offset()));
    if (source.timestamp() != ConsumerRecord.NO_TIMESTAMP) {
      add(headers, SOURCE_TIMESTAMP, Long.toString(source.timestamp()));
    }
    add(headers, SOURCE_GROUP, group);
    add(headers, ATTEMPTS, Integer.toString(attempts));
    add(headers, FAILURE, cause == Cause.REJECTED ? "rejected" : "deadline");
    if (detail != null && !detail.isEmpty()) {
      int end = Math.min(detail.length(), MAX_DETAIL);
      if (end < detail.length() && Character.isHighSurrogate(detail.charAt(end - 1))) {
        end--;
      }
      add(headers, FAILURE_DETAIL, detail.substring(0, end));
    }
    // No partition and no timestamp: the partitioner's and the time of the write.
    return new ProducerRecord<>(topic, null, null, source.key(), source.value(), headers);
  }

  private static void add(RecordHeaders headers, String name, String value) {
    headers.add(name, value.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Writes a dead letter to a cluster and tells {@code done}, once and from another thread, how it
   * went: null when the brokers acknowledged it, the failure otherwise. A failure is final for this
   * write (the producer has retried what can be retried, for {@code delivery.timeout.ms}); whether
   * to write again is the caller's business. Never waits.
   */
  void write(String cluster, ProducerRecord<byte[], byte[]> record, Consumer<Exception> done) {
    AtomicBoolean answered = new AtomicBoolean();
    Consumer<Exception> once =
        failure -> {
          if (answered.compareAndSet(false, true)) {
            done.accept(failure);
          }
        };
    Writer writer;
    synchronized (this) {
      if (closed) {
        writer = null;
      } else {
        writer = writers.computeIfAbsent(cluster, Writer::new);
      }
    }
    if (writer == null) {
      once.accept(new IllegalStateException("the dispatcher is stopping"));
      return;
    }
    try {
      writer.sender.execute(
          () -> {
            try {
              if (writer.producer == null) {
                writer.producer = create(cluster);
              }
              writer.producer.send(record, (metadata, failure) -> once.accept(failure));
            } catch (RuntimeException e) {
              once.accept(e);
            }
          });
    } catch (RejectedExecutionException e) {
      once.accept(new IllegalStateException("the dispatcher is stopping", e));
    }
  }

  private Producer<byte[], byte[]> create(String name) {
    Map<String, Object> properties =
        ClusterProducers.properties(name, config.getClustersOrThrow(name));
    // Not the id of the producer an app's records go through, when this server has one too.
    properties.put(
        ProducerConfig.CLIENT_ID_CONFIG,
        properties.get(ProducerConfig.CLIENT_ID_CONFIG) + "-dead-letter");
    tls.apply(name, properties);
    metrics.kafkaClients().applyToProducer(name, properties);
    LOG.info("creating the dead-letter producer of cluster {}", name);
    return factory.create(name, properties);
  }

  /**
   * Stops writing: what was asked for and not yet sent is sent, and each producer waits up to
   * {@code timeout} for the brokers' answers. A write asked for after this fails.
   */
  public void close(Duration timeout) {
    Map<String, Writer> open;
    synchronized (this) {
      closed = true;
      open = Map.copyOf(writers);
    }
    open.values().forEach(writer -> writer.sender.shutdown());
    open.forEach(
        (cluster, writer) -> {
          try {
            if (!writer.sender.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
              // Waiting in send(), for metadata that does not come.
              writer.sender.shutdownNow();
              writer.sender.awaitTermination(1, TimeUnit.SECONDS);
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          Producer<byte[], byte[]> producer = writer.producer;
          if (producer != null) {
            try {
              producer.close(timeout);
            } catch (RuntimeException e) {
              LOG.warn("closing the dead-letter producer of cluster {} failed", cluster, e);
            }
          }
        });
  }
}
