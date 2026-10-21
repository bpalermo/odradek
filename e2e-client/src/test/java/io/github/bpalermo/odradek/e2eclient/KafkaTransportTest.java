package io.github.bpalermo.odradek.e2eclient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

class KafkaTransportTest {
  private static final String TOPIC = "t";
  private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);
  private static final TopicPartition OTHER = new TopicPartition(TOPIC, 1);

  private static Settings.Kafka kafka(String commitInterval) {
    return Settings.parse(
            List.of(
                "--mode",
                "consumer",
                "--topic",
                TOPIC,
                "--transport",
                "kafka",
                "--bootstrap-servers",
                "unused:9092",
                "--commit-interval",
                commitInterval),
            Map.of())
        .kafka();
  }

  /** A mock consumer that remembers its last synchronous commit, and may fail to close. */
  private static final class Recording extends MockConsumer<byte[], byte[]> {
    final boolean failClose;
    volatile Map<TopicPartition, OffsetAndMetadata> lastCommitSync = Map.of();

    Recording(boolean failClose) {
      super("earliest");
      this.failClose = failClose;
    }

    @Override
    public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
      lastCommitSync = Map.copyOf(offsets);
      super.commitSync(offsets);
    }

    @Override
    public synchronized void close() {
      super.close();
      if (failClose) {
        throw new IllegalStateException("close failed");
      }
    }

    /** Assigns the partitions on the first poll, from these offsets, with records from there. */
    void fetches(Map<TopicPartition, Long> firstOffsets, int records) {
      schedulePollTask(
          () -> {
            rebalance(List.copyOf(firstOffsets.keySet()));
            updateBeginningOffsets(firstOffsets);
            firstOffsets.forEach(
                (partition, first) -> {
                  for (long offset = first; offset < first + records; offset++) {
                    addRecord(
                        new ConsumerRecord<>(
                            TOPIC, partition.partition(), offset, new byte[0], new byte[0]));
                  }
                });
          });
    }
  }

  @Test
  void aSlowHandlerCommitsWithinABatchOnceTheIntervalHasPassed() throws Exception {
    Recording consumer = new Recording(false);
    consumer.fetches(Map.of(PARTITION, 0L), 10);
    CompletableFuture<OffsetAndMetadata> committedAtLast = new CompletableFuture<>();
    Transport transport = KafkaTransport.withConsumer(kafka("100ms"), consumer);

    Transport.Subscription subscription =
        transport.subscribe(
            TOPIC,
            Duration.ZERO,
            0,
            record -> {
              // 40 ms a record: the batch takes 400 ms, four commit intervals.
              Thread.sleep(40);
              if (record.offset() == 9) {
                committedAtLast.complete(consumer.committed(Set.of(PARTITION)).get(PARTITION));
              }
            },
            _ -> {});
    try {
      OffsetAndMetadata committed = committedAtLast.get(10, TimeUnit.SECONDS);
      // Committed while the batch was still being handled, not only after it.
      assertThat(committed).isNotNull();
      assertThat(committed.offset()).isBetween(2L, 9L);
    } finally {
      subscription.close();
    }
    assertThat(consumer.closed()).isTrue();
  }

  @Test
  void aFailureOnTheFirstRecordOfAPartitionCommitsItsOffsetSoItIsReadAgain() throws Exception {
    Recording consumer = new Recording(false);
    // Neither partition was ever committed; one batch brings records of both.
    consumer.fetches(Map.of(PARTITION, 5L, OTHER, 7L), 2);
    CompletableFuture<Throwable> failed = new CompletableFuture<>();
    Exception failure = new Exception("failing on purpose");
    Transport transport = KafkaTransport.withConsumer(kafka("1h"), consumer);

    Transport.Subscription subscription =
        transport.subscribe(
            TOPIC,
            Duration.ZERO,
            0,
            record -> {
              throw failure;
            },
            failed::complete);
    try {
      assertThat(failed.get(10, TimeUnit.SECONDS)).isSameAs(failure);
    } finally {
      subscription.close();
    }
    // Pinned where the next consumer must resume, whatever its auto.offset.reset: at the failed
    // record, and at the first record of the partition whose records were never handled.
    assertThat(consumer.lastCommitSync)
        .isEqualTo(Map.of(PARTITION, new OffsetAndMetadata(5), OTHER, new OffsetAndMetadata(7)));
  }

  @Test
  void aConsumerThatFailsToCloseStillReportsWhyItStopped() throws Exception {
    Recording consumer = new Recording(true);
    consumer.fetches(Map.of(PARTITION, 0L), 1);
    CompletableFuture<Throwable> failed = new CompletableFuture<>();
    Exception failure = new Exception("failing on purpose");
    Transport transport = KafkaTransport.withConsumer(kafka("1h"), consumer);

    Transport.Subscription subscription =
        transport.subscribe(
            TOPIC,
            Duration.ZERO,
            0,
            record -> {
              throw failure;
            },
            failed::complete);
    try {
      // The handler's failure, not the one of the cleanup after it.
      assertThat(failed.get(10, TimeUnit.SECONDS)).isSameAs(failure);
    } finally {
      subscription.close();
    }
  }

  @Test
  void aCloseFailureWithNoFailureBeforeItIsReported() throws Exception {
    Recording consumer = new Recording(true);
    CompletableFuture<Throwable> failed = new CompletableFuture<>();
    Transport transport = KafkaTransport.withConsumer(kafka("1h"), consumer);

    Transport.Subscription subscription =
        transport.subscribe(TOPIC, Duration.ZERO, 0, _ -> {}, failed::complete);
    subscription.close();

    assertThat(failed.get(10, TimeUnit.SECONDS))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("close failed");
  }

  /**
   * A producer whose buffer stays full, which reports it as KafkaProducer does: to the callback,
   * within send(), once max.block.ms has passed. Or, with {@code late}, only after send() returned.
   */
  private static final class FullBuffer extends MockProducer<byte[], byte[]> {
    final boolean late;
    volatile Callback pending;

    FullBuffer(boolean late) {
      super(false, null, new ByteArraySerializer(), new ByteArraySerializer());
      this.late = late;
    }

    @Override
    public synchronized Future<RecordMetadata> send(
        ProducerRecord<byte[], byte[]> record, Callback callback) {
      BufferExhaustedException full = new BufferExhaustedException("buffer full");
      if (late) {
        pending = callback;
      } else {
        callback.onCompletion(null, full);
      }
      return CompletableFuture.failedFuture(full);
    }
  }

  @Test
  void aFullBufferIsNotSentLikeTheOdradekTransportsBufferFull() {
    Transport.Sender sender =
        KafkaTransport.withProducer(kafka("1h"), new FullBuffer(false)).sender();
    assertThatThrownBy(() -> sender.send(TOPIC, "k", new byte[0]))
        .isInstanceOfSatisfying(
            Transport.NotSentException.class, e -> assertThat(e.reason()).isEqualTo("BUFFER_FULL"));
  }

  @Test
  void aFullBufferReportedAfterSendReturnedStillCompletesTheSend() throws Exception {
    FullBuffer producer = new FullBuffer(true);
    Transport.Sender sender = KafkaTransport.withProducer(kafka("1h"), producer).sender();
    CompletableFuture<Transport.Outcome> outcome = sender.send(TOPIC, "k", new byte[0]);
    assertThat(outcome).isNotDone();
    producer.pending.onCompletion(null, new BufferExhaustedException("buffer full"));
    assertThat(outcome.get(1, TimeUnit.SECONDS).kind()).isEqualTo(Transport.Outcome.Kind.FAILED);
  }
}
