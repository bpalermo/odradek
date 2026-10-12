package io.github.bpalermo.odradek.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.bpalermo.odradek.client.Consumer;
import io.github.bpalermo.odradek.client.ConsumerListener;
import io.github.bpalermo.odradek.client.ConsumerOptions;
import io.github.bpalermo.odradek.client.OdradekClient;
import io.github.bpalermo.odradek.client.OutgoingRecord;
import io.github.bpalermo.odradek.client.Partition;
import io.github.bpalermo.odradek.client.ProduceException;
import io.github.bpalermo.odradek.client.Producer;
import io.github.bpalermo.odradek.client.SendResult;
import io.github.bpalermo.odradek.kafka.v1.ErrorCode;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The Java client end to end: its public API against the server and two real clusters. */
// The consumers in try-with-resources work in the background; closing them is the point.
@SuppressWarnings("try")
class ClientE2eTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(60);
  private static final int PARTITIONS = 4;

  private final String id = UUID.randomUUID().toString();
  private final String topic = "odradek-e2e-client-" + id;
  private final String unconfigured = "odradek-e2e-unconfigured-" + id;

  private ServerHarness harness;
  private OdradekClient client;

  @BeforeEach
  void start() throws Exception {
    ServerHarness.createTopic(ServerHarness.KAFKA_A, topic, PARTITIONS);
    ServerHarness.createTopic(ServerHarness.KAFKA_B, topic, PARTITIONS);
    harness =
        ServerHarness.start(
            """
            listener:
              socketPath: %s/kafka.sock
            clusters:
              a:
                properties:
                  bootstrap.servers: %s
                consumerProperties:
                  session.timeout.ms: 10000
                  heartbeat.interval.ms: 1000
              b:
                properties:
                  bootstrap.servers: %s
            consume:
              commitInterval: 0.2s
              revokeDeadline: 5s
            producers:
              %s:
                clusters: [{cluster: a}, {cluster: b}]
            consumers:
              %s:
                cluster: a
                groupId: client-%s
            """,
            ServerHarness.KAFKA_A, ServerHarness.KAFKA_B, topic, topic, id);
    client = OdradekClient.connect(harness.socket());
  }

  @AfterEach
  void stop() throws Exception {
    client.close();
    harness.stop();
    ServerHarness.deleteTopics(ServerHarness.KAFKA_A, List.of(topic));
    ServerHarness.deleteTopics(ServerHarness.KAFKA_B, List.of(topic));
  }

  private static void eventually(String what, BooleanSupplier condition)
      throws InterruptedException {
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertThat(condition.getAsBoolean()).as(what).isTrue();
  }

  private void send(Producer producer, String prefix, int from, int to) throws Exception {
    List<CompletableFuture<SendResult>> futures = new ArrayList<>();
    for (int i = from; i <= to; i++) {
      futures.add(producer.send(OutgoingRecord.to(topic).key("key-" + i % 7).value(prefix + i)));
    }
    for (CompletableFuture<SendResult> future : futures) {
      SendResult result = future.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
      assertThat(result.writes()).extracting(SendResult.Write::cluster).containsExactly("a", "b");
    }
  }

  private static Set<String> expected(String prefix, int from, int to) {
    Set<String> values = new TreeSet<>();
    for (int i = from; i <= to; i++) {
      values.add(prefix + i);
    }
    return values;
  }

  private static String text(byte[] bytes) {
    return new String(bytes, StandardCharsets.UTF_8);
  }

  @Test
  void producedRecordsAreConsumedOnceAndInOrderPerKey() throws Exception {
    List<String> seen = new CopyOnWriteArrayList<>();
    Map<String, List<Integer>> byKey = new ConcurrentHashMap<>();
    try (Producer producer = client.producer();
        Consumer consumer =
            client.consumer(
                ConsumerOptions.topics(topic),
                record -> {
                  String value = text(record.value());
                  seen.add(value);
                  byKey
                      .computeIfAbsent(text(record.key()), unused -> new CopyOnWriteArrayList<>())
                      .add(Integer.parseInt(value.substring(1)));
                })) {
      send(producer, "v", 1, 300);
      eventually("all records handled", () -> seen.size() >= 300);
    }
    assertThat(new TreeSet<>(seen)).isEqualTo(expected("v", 1, 300));
    assertThat(seen).as("no duplicates").hasSize(300);
    assertThat(byKey).hasSize(7);
    byKey.values().forEach(values -> assertThat(values).isSorted());

    // Everything was processed, so a new consumer of the group has nothing to read.
    List<String> again = new CopyOnWriteArrayList<>();
    try (Consumer consumer =
        client.consumer(ConsumerOptions.topics(topic), record -> again.add(text(record.value())))) {
      Thread.sleep(3000);
    }
    assertThat(again).isEmpty();
  }

  @Test
  void unconfiguredTopicFailsTheSendWithoutBreakingTheProducer() throws Exception {
    try (Producer producer = client.producer()) {
      CompletableFuture<SendResult> refused =
          producer.send(OutgoingRecord.to(unconfigured).value("v"));
      assertThatThrownBy(() -> refused.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS))
          .isInstanceOf(ExecutionException.class)
          .cause()
          .isInstanceOfSatisfying(
              ProduceException.class,
              e -> {
                assertThat(e.code()).isEqualTo(ErrorCode.ERROR_CODE_TOPIC_NOT_CONFIGURED);
                assertThat(e.outcomeUnknown()).isFalse();
              });
      send(producer, "v", 1, 5);
    }
  }

  @Test
  void handlerThatThrowsStopsTheConsumerAndTheRecordIsDeliveredAgain() throws Exception {
    try (Producer producer = client.producer()) {
      send(producer, "v", 1, 20);
    }
    List<String> first = new CopyOnWriteArrayList<>();
    CompletableFuture<Throwable> failure = new CompletableFuture<>();
    Consumer failing =
        client.consumer(
            ConsumerOptions.topics(topic),
            record -> {
              if (text(record.value()).equals("v13")) {
                throw new IllegalStateException("cannot process v13");
              }
              first.add(text(record.value()));
            },
            new ConsumerListener() {
              @Override
              public void failed(Throwable cause) {
                failure.complete(cause);
              }
            });
    assertThat(failure.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS)).hasMessage("cannot process v13");
    failing.close();
    assertThat(first).doesNotContain("v13");

    List<String> second = new CopyOnWriteArrayList<>();
    try (Consumer consumer =
        client.consumer(
            ConsumerOptions.topics(topic), record -> second.add(text(record.value())))) {
      eventually("v13 is delivered again", () -> second.contains("v13"));
      eventually(
          "everything is processed by one consumer or the other",
          () -> {
            Set<String> all = new TreeSet<>(first);
            all.addAll(second);
            return all.equals(expected("v", 1, 20));
          });
    }
  }

  @Test
  void secondConsumerSharesTheGroupAndEachRecordIsHandledOnce() throws Exception {
    List<String> one = new CopyOnWriteArrayList<>();
    List<String> two = new CopyOnWriteArrayList<>();
    Set<Partition> ownedByOne = ConcurrentHashMap.newKeySet();
    Set<Partition> ownedByTwo = ConcurrentHashMap.newKeySet();
    ConsumerListener trackOne = tracking(ownedByOne);
    ConsumerListener trackTwo = tracking(ownedByTwo);
    try (Producer producer = client.producer();
        Consumer first =
            client.consumer(
                ConsumerOptions.topics(topic), record -> one.add(text(record.value())), trackOne)) {
      eventually("the first consumer owns every partition", () -> ownedByOne.size() == PARTITIONS);
      try (Consumer second =
          client.consumer(
              ConsumerOptions.topics(topic), record -> two.add(text(record.value())), trackTwo)) {
        eventually(
            "the partitions are split",
            () ->
                !ownedByTwo.isEmpty()
                    && ownedByOne.size() + ownedByTwo.size() == PARTITIONS
                    && Collections.disjoint(ownedByOne, ownedByTwo));
        send(producer, "v", 1, 200);
        eventually("all records handled", () -> one.size() + two.size() >= 200);
      }
    }
    List<String> all = new ArrayList<>(one);
    all.addAll(two);
    assertThat(new TreeSet<>(all)).isEqualTo(expected("v", 1, 200));
    assertThat(all).as("each record was handled by exactly one consumer").hasSize(200);
    assertThat(one).isNotEmpty();
    assertThat(two).isNotEmpty();
  }

  private static ConsumerListener tracking(Set<Partition> owned) {
    return new ConsumerListener() {
      @Override
      public void assigned(List<Partition> partitions) {
        owned.addAll(partitions);
      }

      @Override
      public void revoked(List<Partition> partitions) {
        partitions.forEach(owned::remove);
      }

      @Override
      public void lost(List<Partition> partitions) {
        partitions.forEach(owned::remove);
      }
    };
  }

  @Test
  void producerAndConsumerSurviveAServerRestart() throws Exception {
    List<String> seen = new CopyOnWriteArrayList<>();
    Set<Partition> lost = ConcurrentHashMap.newKeySet();
    try (Producer producer = client.producer();
        Consumer consumer =
            client.consumer(
                ConsumerOptions.topics(topic),
                record -> seen.add(text(record.value())),
                new ConsumerListener() {
                  @Override
                  public void lost(List<Partition> partitions) {
                    lost.addAll(partitions);
                  }
                })) {
      send(producer, "before", 1, 50);
      eventually("the first records are handled", () -> seen.size() >= 50);

      harness.restart();

      // The same producer and consumer objects, against a server with a new epoch.
      send(producer, "after", 1, 50);
      eventually(
          "the records sent after the restart are handled",
          () -> new TreeSet<>(seen).containsAll(expected("after", 1, 50)));
    }
    assertThat(lost).as("the consumer was told its partitions were gone").hasSize(PARTITIONS);
    assertThat(new TreeSet<>(seen)).containsAll(expected("before", 1, 50));
    // Records acked just before the restart may not have been committed yet, so "before" records
    // can be handled twice. "after" records cannot: nothing interrupted them.
    assertThat(seen.stream().filter(v -> v.startsWith("after")).count()).isEqualTo(50);
  }
}
