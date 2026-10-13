package io.github.bpalermo.odradek.server.dispatch;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.bpalermo.odradek.config.v1.Cluster;
import io.github.bpalermo.odradek.config.v1.ServerConfig;
import io.github.bpalermo.odradek.server.dispatch.DispatchAction.Cause;
import io.github.bpalermo.odradek.server.telemetry.ServerMetrics;
import io.github.bpalermo.odradek.server.tls.TlsSources;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

/** What a dead letter is made of, and how the writes reach a producer. */
class DeadLettersTest {
  private static final ServerConfig CONFIG =
      ServerConfig.newBuilder()
          .putClusters(
              "main",
              Cluster.newBuilder()
                  .putProperties("bootstrap.servers", "kafka:9092")
                  .putProducerProperties("client.id", "billing")
                  .build())
          .build();

  private static byte[] utf8(String text) {
    return text.getBytes(StandardCharsets.UTF_8);
  }

  private static ConsumerRecord<byte[], byte[]> source(
      long timestamp, byte[] key, byte[] value, RecordHeaders headers) {
    return new ConsumerRecord<>(
        "orders.created",
        3,
        42,
        timestamp,
        TimestampType.CREATE_TIME,
        key == null ? ConsumerRecord.NULL_SIZE : key.length,
        value == null ? ConsumerRecord.NULL_SIZE : value.length,
        key,
        value,
        headers,
        Optional.empty());
  }

  private static List<String> headers(ProducerRecord<byte[], byte[]> record) {
    List<String> headers = new ArrayList<>();
    for (Header header : record.headers()) {
      headers.add(
          header.key()
              + "="
              + (header.value() == null
                  ? null
                  : new String(header.value(), StandardCharsets.UTF_8)));
    }
    return headers;
  }

  @Test
  void aDeadLetterIsTheRecordAsStoredFollowedByWhereItCameFrom() {
    byte[] key = {0, 1, 2};
    byte[] value = {(byte) 0xff, 0, 7};
    RecordHeaders stored = new RecordHeaders();
    stored.add("trace", utf8("abc"));
    stored.add("empty", null);
    stored.add("trace", utf8("def"));

    ProducerRecord<byte[], byte[]> letter =
        DeadLetters.record(
            "billing.dlq",
            "main",
            "billing",
            source(1_700_000_000_123L, key, value, stored),
            5,
            Cause.REJECTED,
            "no codec for it");

    assertThat(letter.topic()).isEqualTo("billing.dlq");
    assertThat(letter.partition()).isNull();
    assertThat(letter.timestamp()).isNull();
    // The very bytes: nothing is decoded, and nothing copied.
    assertThat(letter.key()).isSameAs(key);
    assertThat(letter.value()).isSameAs(value);
    assertThat(headers(letter))
        .containsExactly(
            "trace=abc",
            "empty=null",
            "trace=def",
            "odradek-source-cluster=main",
            "odradek-source-topic=orders.created",
            "odradek-source-partition=3",
            "odradek-source-offset=42",
            "odradek-source-timestamp=1700000000123",
            "odradek-source-group=billing",
            "odradek-attempts=5",
            "odradek-failure=rejected",
            "odradek-failure-detail=no codec for it");
  }

  @Test
  void aMissedDeadlineHasNoDetailAndARecordWithoutTimestampNoTimestamp() {
    ProducerRecord<byte[], byte[]> letter =
        DeadLetters.record(
            "billing.dlq",
            "main",
            "billing",
            source(ConsumerRecord.NO_TIMESTAMP, null, null, new RecordHeaders()),
            1,
            Cause.DEADLINE,
            null);
    assertThat(letter.key()).isNull();
    assertThat(letter.value()).as("a tombstone stays one").isNull();
    assertThat(headers(letter))
        .containsExactly(
            "odradek-source-cluster=main",
            "odradek-source-topic=orders.created",
            "odradek-source-partition=3",
            "odradek-source-offset=42",
            "odradek-source-group=billing",
            "odradek-attempts=1",
            "odradek-failure=deadline");
  }

  @Test
  void aDeadLetterOfADeadLetterKeepsTheEarlierHeadersBeforeItsOwn() {
    RecordHeaders stored = new RecordHeaders();
    stored.add(DeadLetters.SOURCE_TOPIC, utf8("orders.first"));
    stored.add(DeadLetters.ATTEMPTS, utf8("9"));
    ProducerRecord<byte[], byte[]> letter =
        DeadLetters.record(
            "billing.dlq2",
            "main",
            "billing",
            source(1, null, null, stored),
            2,
            Cause.DEADLINE,
            "");
    assertThat(headers(letter))
        .startsWith("odradek-source-topic=orders.first", "odradek-attempts=9");
    assertThat(
            new String(
                letter.headers().lastHeader(DeadLetters.SOURCE_TOPIC).value(),
                StandardCharsets.UTF_8))
        .isEqualTo("orders.created");
    assertThat(
            new String(
                letter.headers().lastHeader(DeadLetters.ATTEMPTS).value(), StandardCharsets.UTF_8))
        .isEqualTo("2");
  }

  @Test
  void aLongDetailIsCutWithoutSplittingACharacter() {
    // A character outside the basic plane takes two chars, the first of which is at the limit.
    String detail = "x".repeat(DeadLetters.MAX_DETAIL - 1) + "😀" + "tail";
    ProducerRecord<byte[], byte[]> letter =
        DeadLetters.record(
            "dlq",
            "main",
            "g",
            source(1, null, null, new RecordHeaders()),
            1,
            Cause.REJECTED,
            detail);
    String cut =
        new String(
            letter.headers().lastHeader(DeadLetters.FAILURE_DETAIL).value(),
            StandardCharsets.UTF_8);
    assertThat(cut).isEqualTo("x".repeat(DeadLetters.MAX_DETAIL - 1));
  }

  @Test
  void writesGoToOneProducerPerClusterMadeWhenFirstNeededAndAreAnsweredOnce() throws Exception {
    List<MockProducer<byte[], byte[]>> producers = new CopyOnWriteArrayList<>();
    List<Object> clientIds = new CopyOnWriteArrayList<>();
    DeadLetters letters =
        new DeadLetters(
            CONFIG,
            TlsSources.none(),
            ServerMetrics.noop(),
            (cluster, properties) -> {
              clientIds.add(properties.get("client.id"));
              MockProducer<byte[], byte[]> producer =
                  new MockProducer<>(
                      false,
                      new RoundRobinPartitioner(),
                      new ByteArraySerializer(),
                      new ByteArraySerializer());
              producers.add(producer);
              return producer;
            });
    assertThat(producers).isEmpty();
    BlockingQueue<Optional<Exception>> answers = new LinkedBlockingQueue<>();
    for (int i = 0; i < 3; i++) {
      letters.write(
          "main",
          new ProducerRecord<>("dlq", utf8("k" + i), utf8("v")),
          failure -> answers.add(Optional.ofNullable(failure)));
    }
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while ((producers.isEmpty() || producers.get(0).history().size() < 3)
        && System.nanoTime() < deadline) {
      Thread.sleep(5);
    }
    assertThat(producers).hasSize(1);
    // The cluster's own client id, told apart from that of a producer for an app.
    assertThat(clientIds).containsExactly("billing-dead-letter");
    MockProducer<byte[], byte[]> producer = producers.get(0);
    assertThat(producer.history().stream().map(r -> new String(r.key(), StandardCharsets.UTF_8)))
        .as("in the order asked for")
        .containsExactly("k0", "k1", "k2");
    assertThat(answers).isEmpty();

    producer.completeNext();
    assertThat(answers.poll(10, TimeUnit.SECONDS)).isEmpty();
    KafkaException failure = new KafkaException("refused");
    producer.errorNext(failure);
    assertThat(answers.poll(10, TimeUnit.SECONDS)).containsSame(failure);

    letters.close(Duration.ofSeconds(1));
    assertThat(producer.closed()).isTrue();
    // A write asked for once it is closed fails, and says so.
    letters.write(
        "main",
        new ProducerRecord<>("dlq", utf8("late"), utf8("v")),
        late -> answers.add(Optional.ofNullable(late)));
    assertThat(answers.poll(10, TimeUnit.SECONDS).orElseThrow())
        .isInstanceOf(IllegalStateException.class);
    assertThat(producer.history()).hasSize(3);
  }

  @Test
  void aProducerThatCannotBeMadeFailsTheWriteAndIsMadeAgainForTheNext() throws Exception {
    List<String> asked = new CopyOnWriteArrayList<>();
    DeadLetters letters =
        new DeadLetters(
            CONFIG,
            TlsSources.none(),
            ServerMetrics.noop(),
            (cluster, properties) -> {
              asked.add(cluster);
              throw new KafkaException("no such setting");
            });
    BlockingQueue<Optional<Exception>> answers = new LinkedBlockingQueue<>();
    for (int i = 0; i < 2; i++) {
      letters.write(
          "main",
          new ProducerRecord<>("dlq", utf8("k"), utf8("v")),
          failure -> answers.add(Optional.ofNullable(failure)));
      assertThat(answers.poll(10, TimeUnit.SECONDS).orElseThrow())
          .isInstanceOf(KafkaException.class);
    }
    assertThat(asked).containsExactly("main", "main");
    letters.close(Duration.ofSeconds(1));
  }
}
