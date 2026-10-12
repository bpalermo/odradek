package io.github.bpalermo.odradek.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import io.github.bpalermo.odradek.kafka.v1.ErrorCode;
import io.github.bpalermo.odradek.kafka.v1.OdradekServiceGrpc;
import io.github.bpalermo.odradek.kafka.v1.ProduceOpen;
import io.github.bpalermo.odradek.kafka.v1.ProduceRecord;
import io.github.bpalermo.odradek.kafka.v1.ProduceRequest;
import io.github.bpalermo.odradek.kafka.v1.ProduceResponse;
import io.github.bpalermo.odradek.kafka.v1.ProduceResult;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The produce path end to end: an app-side gRPC stream over the Unix socket, the server as {@code
 * Main} assembles it, and two real Kafka clusters.
 */
class ProduceE2eTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(30);
  private static final int PARTITIONS = 3;

  private final String bootstrapA = ServerHarness.KAFKA_A;
  private final String bootstrapB = ServerHarness.KAFKA_B;
  private final String topic = "odradek-e2e-produce-" + UUID.randomUUID();
  private final String uneven = "odradek-e2e-uneven-" + UUID.randomUUID();
  private final BlockingQueue<ProduceResponse> responses = new LinkedBlockingQueue<>();

  private ServerHarness harness;
  private ManagedChannel channel;

  @BeforeEach
  void start() throws Exception {
    ServerHarness.createTopic(bootstrapA, topic, PARTITIONS);
    ServerHarness.createTopic(bootstrapB, topic, PARTITIONS);
    // Breaks the prerequisite of producing to several clusters: equal partition counts.
    ServerHarness.createTopic(bootstrapA, uneven, PARTITIONS);
    ServerHarness.createTopic(bootstrapB, uneven, 1);
    harness =
        ServerHarness.start(
            """
            listener:
              socketPath: %s/kafka.sock
            clusters:
              a:
                properties:
                  bootstrap.servers: %s
                producerProperties:
                  max.block.ms: 5000
                  delivery.timeout.ms: 10000
                  request.timeout.ms: 5000
              b:
                properties:
                  bootstrap.servers: %s
                producerProperties:
                  max.block.ms: 5000
                  delivery.timeout.ms: 10000
                  request.timeout.ms: 5000
            producers:
              %s:
                clusters:
                  - cluster: a
                  - cluster: b
              %s:
                clusters:
                  - cluster: a
                  - cluster: b
            """,
            bootstrapA, bootstrapB, topic, uneven);
    channel = harness.channel;
  }

  @AfterEach
  void stop() throws Exception {
    harness.stop();
    ServerHarness.deleteTopics(bootstrapA, List.of(topic, uneven));
    ServerHarness.deleteTopics(bootstrapB, List.of(topic, uneven));
  }

  private StreamObserver<ProduceRequest> open(String session) throws InterruptedException {
    StreamObserver<ProduceRequest> requests =
        OdradekServiceGrpc.newStub(channel)
            .produce(
                new StreamObserver<ProduceResponse>() {
                  @Override
                  public void onNext(ProduceResponse response) {
                    responses.add(response);
                  }

                  @Override
                  public void onError(Throwable t) {}

                  @Override
                  public void onCompleted() {}
                });
    requests.onNext(
        ProduceRequest.newBuilder()
            .setOpen(ProduceOpen.newBuilder().setSessionId(session))
            .build());
    assertThat(next().hasReady()).isTrue();
    return requests;
  }

  private ProduceResponse next() throws InterruptedException {
    ProduceResponse response = responses.poll(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
    assertThat(response).as("a response within %s", TIMEOUT).isNotNull();
    return response;
  }

  private static ProduceRequest record(long seq, String topic, String key, String value) {
    return ProduceRequest.newBuilder()
        .setRecord(
            ProduceRecord.newBuilder()
                .setSeq(seq)
                .setLowWatermark(1)
                .setTopic(topic)
                .setKey(ByteString.copyFromUtf8(key))
                .setValue(ByteString.copyFromUtf8(value)))
        .build();
  }

  /** Reads everything in a topic, as "partition/key=value@timestamp", in partition order. */
  private static List<String> read(String bootstrap, String topic, int expected) {
    Map<String, Object> properties = new HashMap<>();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    List<TopicPartition> partitions = new ArrayList<>();
    for (int p = 0; p < PARTITIONS; p++) {
      partitions.add(new TopicPartition(topic, p));
    }
    Map<Integer, List<String>> byPartition = new HashMap<>();
    int count = 0;
    try (KafkaConsumer<String, String> consumer =
        new KafkaConsumer<>(properties, new StringDeserializer(), new StringDeserializer())) {
      consumer.assign(partitions);
      consumer.seekToBeginning(partitions);
      long deadline = System.nanoTime() + TIMEOUT.toNanos();
      while (count < expected && System.nanoTime() < deadline) {
        for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(200))) {
          byPartition
              .computeIfAbsent(r.partition(), unused -> new ArrayList<>())
              .add(r.partition() + "/" + r.key() + "=" + r.value() + "@" + r.timestamp());
          count++;
        }
      }
      // One more poll: anything beyond `expected` is a duplicate and must be seen.
      for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofSeconds(1))) {
        byPartition
            .computeIfAbsent(r.partition(), unused -> new ArrayList<>())
            .add(r.partition() + "/" + r.key() + "=" + r.value() + "@" + r.timestamp());
      }
    }
    List<String> all = new ArrayList<>();
    for (int p = 0; p < PARTITIONS; p++) {
      all.addAll(byPartition.getOrDefault(p, List.of()));
    }
    return all;
  }

  @Test
  void recordsAreWrittenToBothClustersWithTheSamePartitionOrderAndTimestamp() throws Exception {
    int count = 200;
    StreamObserver<ProduceRequest> requests = open("session-" + UUID.randomUUID());
    for (int i = 1; i <= count; i++) {
      requests.onNext(record(i, topic, "key-" + (i % 10), "value-" + i));
    }
    for (int i = 0; i < count; i++) {
      ProduceResult result = next().getResult();
      assertThat(result.hasError()).as("seq %s: %s", result.getSeq(), result.getError()).isFalse();
      assertThat(result.getWritesList()).extracting(w -> w.getCluster()).containsExactly("a", "b");
      assertThat(result.getWrites(0).getPartition()).isEqualTo(result.getWrites(1).getPartition());
      assertThat(result.getWrites(0).getTimestampMs())
          .isEqualTo(result.getWrites(1).getTimestampMs());
    }
    requests.onCompleted();

    List<String> onA = read(bootstrapA, topic, count);
    List<String> onB = read(bootstrapB, topic, count);
    assertThat(onA).hasSize(count);
    // Same records, same partitions, same order within each partition, same timestamps.
    assertThat(onB).containsExactlyElementsOf(onA);
    // Per key, the order of sending is the order stored.
    for (int k = 0; k < 10; k++) {
      String key = "/key-" + k + "=";
      List<Integer> values =
          onA.stream()
              .filter(r -> r.contains(key))
              .map(r -> Integer.parseInt(r.substring(r.indexOf("=value-") + 7, r.indexOf('@'))))
              .toList();
      assertThat(values).hasSize(count / 10).isSorted();
    }
  }

  @Test
  void resendingAfterAReconnectDoesNotDuplicate() throws Exception {
    String session = "session-" + UUID.randomUUID();
    StreamObserver<ProduceRequest> first = open(session);
    for (int i = 1; i <= 20; i++) {
      first.onNext(record(i, topic, "k", "value-" + i));
    }
    for (int i = 0; i < 20; i++) {
      assertThat(next().getResult().hasError()).isFalse();
    }
    first.onError(io.grpc.Status.CANCELLED.asRuntimeException());

    // The app never saw the results (say): it reconnects and resends everything.
    StreamObserver<ProduceRequest> second = open(session);
    for (int i = 1; i <= 20; i++) {
      second.onNext(record(i, topic, "k", "value-" + i));
    }
    for (int i = 0; i < 20; i++) {
      ProduceResult replayed = next().getResult();
      assertThat(replayed.hasError()).isFalse();
      assertThat(replayed.getWritesList()).hasSize(2);
    }
    second.onCompleted();

    assertThat(read(bootstrapA, topic, 20)).hasSize(20);
    assertThat(read(bootstrapB, topic, 20)).hasSize(20);
  }

  @Test
  void partitionMissingOnOneClusterIsOutcomeUnknownAndSaysWhereTheRecordIs() throws Exception {
    StreamObserver<ProduceRequest> requests = open("session-" + UUID.randomUUID());
    ProduceRequest record = record(1, uneven, "k", "v");
    requests.onNext(
        record.toBuilder().setRecord(record.getRecord().toBuilder().setPartition(2)).build());
    ProduceResult result = next().getResult();
    assertThat(result.getError().getCode()).isEqualTo(ErrorCode.ERROR_CODE_OUTCOME_UNKNOWN);
    assertThat(result.getError().getMessage()).contains("cluster b");
    assertThat(result.getWritesList()).extracting(w -> w.getCluster()).containsExactly("a");
    assertThat(result.getWrites(0).getPartition()).isEqualTo(2);
    requests.onCompleted();

    assertThat(read(bootstrapA, uneven, 1)).singleElement().asString().startsWith("2/k=v@");
  }
}
