package io.github.bpalermo.odradek.e2eclient;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class KafkaTransportTest {
  private static final String TOPIC = "t";
  private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);

  @Test
  void aSlowHandlerCommitsWithinABatchOnceTheIntervalHasPassed() throws Exception {
    Settings settings =
        Settings.parse(
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
                "100ms"),
            Map.of());
    MockConsumer<byte[], byte[]> consumer = new MockConsumer<>("earliest");
    // One poll hands over a batch of ten records.
    consumer.schedulePollTask(
        () -> {
          consumer.rebalance(List.of(PARTITION));
          consumer.updateBeginningOffsets(Map.of(PARTITION, 0L));
          for (long offset = 0; offset < 10; offset++) {
            consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, offset, new byte[0], new byte[0]));
          }
        });
    CompletableFuture<OffsetAndMetadata> committedAtLast = new CompletableFuture<>();
    Transport transport = KafkaTransport.withConsumer(settings.kafka(), consumer);

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
}
