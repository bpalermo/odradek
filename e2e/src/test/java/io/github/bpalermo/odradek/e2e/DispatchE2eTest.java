package io.github.bpalermo.odradek.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.bpalermo.odradek.delivery.v1.Accepted;
import io.github.bpalermo.odradek.delivery.v1.Ack;
import io.github.bpalermo.odradek.delivery.v1.Credits;
import io.github.bpalermo.odradek.delivery.v1.Declare;
import io.github.bpalermo.odradek.delivery.v1.DeliverRequest;
import io.github.bpalermo.odradek.delivery.v1.DeliverResponse;
import io.github.bpalermo.odradek.delivery.v1.DeliveryServiceGrpc;
import io.github.bpalermo.odradek.delivery.v1.OffsetReset;
import io.github.bpalermo.odradek.delivery.v1.Ordering;
import io.github.bpalermo.odradek.delivery.v1.Record;
import io.github.bpalermo.odradek.delivery.v1.RecordBatch;
import io.github.bpalermo.odradek.delivery.v1.Reject;
import io.github.bpalermo.odradek.delivery.v1.Settings;
import io.github.bpalermo.odradek.server.admin.HealthService;
import io.github.bpalermo.odradek.server.admin.HealthService.ServingStatus;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The dispatcher end to end (ADR 6, step 3): a server with a {@code dispatch} section, a real
 * broker, and scripted sidecars, which are gRPC clients of {@code DeliveryService} written here.
 * What the sidecars are sent, and what the group has committed on the broker, is all that is
 * asserted.
 */
class DispatchE2eTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(60);
  private static final String CONFIG =
      """
      clusters:
        a:
          required: true
          properties:
            bootstrap.servers: %s
          consumerProperties:
            session.timeout.ms: 10000
      consume:
        commitInterval: 0.2s
        revokeDeadline: 2s
      dispatch:
        port: 0
        idleGrace: 60s
      """;

  private final String id = UUID.randomUUID().toString();
  private final String topic = "odradek-e2e-dispatch-" + id;
  private final String second = "odradek-e2e-dispatch-second-" + id;
  private final String group = "odradek-e2e-dispatch-" + id;
  private final List<String> topics = new ArrayList<>();
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

  private DispatcherHarness harness;
  private KafkaProducer<String, String> producer;
  private Admin admin;

  /** One record as a sidecar received it. */
  private record Received(
      Sidecar sidecar, int partition, long offset, String key, String value, int attempt) {
    String at() {
      return partition + "/" + offset;
    }
  }

  /** A scripted sidecar: one Deliver stream on a connection of its own. */
  private final class Sidecar {
    final String name;
    final ManagedChannel channel;
    final List<Received> received = new CopyOnWriteArrayList<>();
    final List<Integer> released = new CopyOnWriteArrayList<>();
    final CompletableFuture<Accepted> accepted = new CompletableFuture<>();
    final CompletableFuture<Status> closed = new CompletableFuture<>();
    private final StreamObserver<DeliverRequest> requests;

    /** What it does with each record, on the stream's thread. By default: holds it. */
    volatile Consumer<Received> onRecord = record -> {};

    Sidecar(String name, Declare.Builder declare) {
      this.name = name;
      channel = harness.connect();
      requests =
          DeliveryServiceGrpc.newStub(channel)
              .deliver(
                  new StreamObserver<DeliverResponse>() {
                    @Override
                    public void onNext(DeliverResponse response) {
                      switch (response.getMsgCase()) {
                        case ACCEPTED -> accepted.complete(response.getAccepted());
                        case RECORDS -> records(response.getRecords());
                        case RELEASED ->
                            released.addAll(response.getReleased().getPartitionsList());
                        default -> {}
                      }
                    }

                    @Override
                    public void onError(Throwable t) {
                      closed.complete(Status.fromThrowable(t));
                      accepted.completeExceptionally(t);
                    }

                    @Override
                    public void onCompleted() {
                      closed.complete(Status.OK);
                    }
                  });
      send(DeliverRequest.newBuilder().setDeclare(declare));
    }

    private void records(RecordBatch batch) {
      long previous = -1;
      for (Record record : batch.getRecordsList()) {
        assertThat(record.getOffset())
            .as("offsets increase within a batch")
            .isGreaterThan(previous);
        previous = record.getOffset();
        Received one =
            new Received(
                this,
                batch.getPartition(),
                record.getOffset(),
                record.hasKey() ? record.getKey().toStringUtf8() : null,
                record.getValue().toStringUtf8(),
                record.getAttempt());
        received.add(one);
        onRecord.accept(one);
      }
    }

    /** Requests are written from the stream's thread and from the test's. */
    private synchronized void send(DeliverRequest.Builder request) {
      requests.onNext(request.build());
    }

    Sidecar awaitAccepted() throws Exception {
      accepted.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
      return this;
    }

    Status status() throws Exception {
      return closed.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
    }

    void credits(int records) {
      send(DeliverRequest.newBuilder().setCredits(Credits.newBuilder().setRecords(records)));
    }

    void ack(Received record) {
      ack(record.partition(), record.offset(), record.offset());
    }

    void ack(int partition, long first, long last) {
      send(
          DeliverRequest.newBuilder()
              .setAck(
                  Ack.newBuilder()
                      .setPartition(partition)
                      .setFirstOffset(first)
                      .setLastOffset(last)));
    }

    void reject(Received record) {
      send(
          DeliverRequest.newBuilder()
              .setReject(
                  Reject.newBuilder()
                      .setPartition(record.partition())
                      .setOffset(record.offset())
                      .setReason("the test rejects it")));
    }

    /** Acknowledges every record as it arrives and asks for one more. */
    Sidecar working() {
      onRecord =
          record -> {
            ack(record);
            credits(1);
          };
      return this;
    }

    /** The application stopped consuming: the stream is closed in good order. */
    void close() {
      requests.onCompleted();
    }

    /** The pod is gone: its connection drops with whatever it held. */
    void kill() {
      channel.shutdownNow();
    }

    /** The offsets it was sent, as "partition/offset", in the order they arrived. */
    List<String> offsets() {
      return received.stream().map(Received::at).toList();
    }
  }

  @BeforeEach
  void start() throws Exception {
    admin =
        Admin.create(
            Map.<String, Object>of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, ServerHarness.KAFKA_A));
    producer =
        new KafkaProducer<>(
            Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                ServerHarness.KAFKA_A,
                ProducerConfig.ACKS_CONFIG,
                "all"),
            new StringSerializer(),
            new StringSerializer());
    harness = DispatcherHarness.start(CONFIG, ServerHarness.KAFKA_A);
  }

  @AfterEach
  void stop() throws Exception {
    scheduler.shutdownNow();
    if (harness != null) {
      harness.stop();
    }
    if (producer != null) {
      producer.close(Duration.ofSeconds(5));
    }
    if (admin != null) {
      admin.close(Duration.ofSeconds(5));
    }
    if (!topics.isEmpty()) {
      ServerHarness.deleteTopics(ServerHarness.KAFKA_A, topics);
    }
  }

  // -------------------------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------------------------

  private void createTopic(String name, int partitions) throws Exception {
    ServerHarness.createTopic(ServerHarness.KAFKA_A, name, partitions);
    topics.add(name);
  }

  private Declare.Builder declare(String name) {
    return Declare.newBuilder()
        .setCluster("a")
        .setGroupId(group)
        .setTopic(name)
        .setSettings(Settings.newBuilder().setMaxProcessingMs(120_000));
  }

  private Declare.Builder declare(String name, Consumer<Settings.Builder> settings) {
    Declare.Builder declare = declare(name);
    settings.accept(declare.getSettingsBuilder());
    return declare;
  }

  private Sidecar sidecar(String name, Declare.Builder declare) throws Exception {
    return new Sidecar(name, declare).awaitAccepted();
  }

  /** Writes records to one partition and returns their offsets, in order. */
  private List<Long> produce(String name, int partition, List<String> keys, String prefix)
      throws Exception {
    return produce(name, partition, keys, prefix, null);
  }

  private List<Long> produce(
      String name, int partition, List<String> keys, String prefix, Long timestamp)
      throws Exception {
    List<Future<RecordMetadata>> sends = new ArrayList<>();
    for (int i = 0; i < keys.size(); i++) {
      sends.add(
          producer.send(
              new ProducerRecord<>(name, partition, timestamp, keys.get(i), prefix + "-" + i)));
    }
    List<Long> offsets = new ArrayList<>();
    for (Future<RecordMetadata> send : sends) {
      offsets.add(send.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS).offset());
    }
    return offsets;
  }

  /** {@code count} keys, all different. */
  private static List<String> keys(String prefix, int count) {
    List<String> keys = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      keys.add(prefix + i);
    }
    return keys;
  }

  private static void await(String what, BooleanSupplier condition) throws InterruptedException {
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (!condition.getAsBoolean()) {
      assertThat(System.nanoTime()).as("%s within %s", what, TIMEOUT).isLessThan(deadline);
      Thread.sleep(20);
    }
  }

  /** What the group has committed, by "topic/partition"; nothing for a partition without. */
  private Map<String, Long> committed() {
    try {
      Map<String, Long> offsets = new TreeMap<>();
      admin
          .listConsumerGroupOffsets(group)
          .partitionsToOffsetAndMetadata()
          .get(30, TimeUnit.SECONDS)
          .forEach(
              (partition, offset) -> {
                if (offset != null) {
                  offsets.put(partition.topic() + "/" + partition.partition(), offset.offset());
                }
              });
      return offsets;
    } catch (Exception e) {
      throw new AssertionError("could not read the offsets of group " + group, e);
    }
  }

  private long committed(String name, int partition) {
    return committed().getOrDefault(name + "/" + partition, -1L);
  }

  /** The end of every partition of a topic, by "topic/partition". */
  private Map<String, Long> ends(String name, int partitions) throws Exception {
    Map<TopicPartition, OffsetSpec> request = new HashMap<>();
    for (int partition = 0; partition < partitions; partition++) {
      request.put(new TopicPartition(name, partition), OffsetSpec.latest());
    }
    Map<String, Long> ends = new TreeMap<>();
    admin
        .listOffsets(request)
        .all()
        .get(30, TimeUnit.SECONDS)
        .forEach((partition, end) -> ends.put(name + "/" + partition.partition(), end.offset()));
    return ends;
  }

  private void awaitCommitted(String name, int partitions) throws Exception {
    Map<String, Long> ends = ends(name, partitions);
    await(
        "everything of " + name + " is committed (" + ends + ")",
        () -> {
          Map<String, Long> now = committed();
          return ends.entrySet().stream()
              .allMatch(end -> end.getValue().equals(now.get(end.getKey())));
        });
  }

  private static Set<String> distinct(List<Sidecar> sidecars) {
    Set<String> all = new TreeSet<>();
    sidecars.forEach(sidecar -> all.addAll(sidecar.offsets()));
    return all;
  }

  private static int total(List<Sidecar> sidecars) {
    return sidecars.stream().mapToInt(sidecar -> sidecar.received.size()).sum();
  }

  // -------------------------------------------------------------------------------------------
  // The handshake
  // -------------------------------------------------------------------------------------------

  @Test
  void acceptedListsThisReplicaAtTheAddressItServes() throws Exception {
    createTopic(topic, 1);
    Sidecar sidecar = sidecar("a", declare(topic));
    Accepted accepted = sidecar.accepted.get();
    assertThat(accepted.getSettings().getOrdering()).isEqualTo(Ordering.ORDERING_KEY);
    assertThat(accepted.getSettings().getOffsetReset())
        .isEqualTo(OffsetReset.OFFSET_RESET_EARLIEST);
    assertThat(accepted.getSettings().getMaxProcessingMs()).isEqualTo(120_000);
    assertThat(accepted.getReplicas().getGeneration()).isPositive();
    assertThat(accepted.getReplicas().getReplicasCount()).isEqualTo(1);
    String address = accepted.getReplicas().getReplicas(0).getAddress();
    assertThat(address).endsWith(":" + harness.port());
    // Reachable as given: a sidecar connects to it and nowhere else.
    ManagedChannel direct = NettyChannelBuilder.forTarget(address).usePlaintext().build();
    try {
      CompletableFuture<Status> refused = new CompletableFuture<>();
      DeliveryServiceGrpc.newStub(direct)
          .deliver(
              new StreamObserver<DeliverResponse>() {
                @Override
                public void onNext(DeliverResponse response) {}

                @Override
                public void onError(Throwable t) {
                  refused.complete(Status.fromThrowable(t));
                }

                @Override
                public void onCompleted() {
                  refused.complete(Status.OK);
                }
              })
          .onNext(
              DeliverRequest.newBuilder().setCredits(Credits.newBuilder().setRecords(1)).build());
      assertThat(refused.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS).getCode())
          .isEqualTo(Status.Code.INVALID_ARGUMENT);
    } finally {
      direct.shutdownNow();
    }
    assertThat(accepted.getDispatcher().getVersion()).isNotEmpty().isNotEqualTo("unknown");
  }

  @Test
  void aConflictingDeclarationAndAnUnknownClusterAreRefused() throws Exception {
    createTopic(topic, 1);
    sidecar("first", declare(topic));

    Sidecar conflicting =
        new Sidecar("other", declare(topic, s -> s.setOrdering(Ordering.ORDERING_NONE)));
    Status conflict = conflicting.status();
    assertThat(conflict.getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
    assertThat(conflict.getDescription())
        .contains("In force: ordering ORDERING_KEY")
        .contains("Declared: ordering ORDERING_NONE");

    Sidecar elsewhere = new Sidecar("elsewhere", declare(topic).setCluster("b"));
    assertThat(elsewhere.status().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);

    // Only the processing deadline may differ.
    Sidecar slower = sidecar("slower", declare(topic, s -> s.setMaxProcessingMs(240_000)));
    assertThat(slower.accepted.get().getSettings().getMaxProcessingMs()).isEqualTo(240_000);
  }

  // -------------------------------------------------------------------------------------------
  // Delivery
  // -------------------------------------------------------------------------------------------

  @Test
  void moreSidecarsThanPartitionsAllReceiveAndEverythingIsCommitted() throws Exception {
    int partitions = 2;
    int perPartition = 150;
    createTopic(topic, partitions);
    for (int partition = 0; partition < partitions; partition++) {
      produce(topic, partition, keys("k" + partition + "-", perPartition), "v");
    }

    List<Sidecar> sidecars = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      sidecars.add(sidecar("s" + i, declare(topic)));
    }
    // Each takes ten records and sits on them: with five times ten in flight and two partitions,
    // every sidecar holds records whoever asked first.
    sidecars.forEach(sidecar -> sidecar.credits(10));
    await("every sidecar holds ten records", () -> total(sidecars) == 50);
    for (Sidecar sidecar : sidecars) {
      assertThat(sidecar.received).as("what %s holds", sidecar.name).hasSize(10);
    }
    assertThat(distinct(sidecars)).as("no record went to two sidecars").hasSize(50);
    Thread.sleep(500);
    assertThat(total(sidecars)).as("nothing is sent without credit").isEqualTo(50);

    // Then they work.
    for (Sidecar sidecar : sidecars) {
      List<Received> held = List.copyOf(sidecar.received);
      sidecar.working();
      held.forEach(sidecar.onRecord);
    }
    await("everything is delivered", () -> distinct(sidecars).size() == partitions * perPartition);
    awaitCommitted(topic, partitions);
    assertThat(total(sidecars))
        .as("nothing was delivered twice")
        .isEqualTo(partitions * perPartition);
    for (Sidecar sidecar : sidecars) {
      assertThat(sidecar.received).allMatch(record -> record.attempt() == 1);
      assertThat(sidecar.received.stream().map(Received::value))
          .allMatch(value -> value.startsWith("v-"));
    }
  }

  @Test
  void recordsOfOneKeyAreReceivedInOrderAndNeverTwoAtOnce() throws Exception {
    int partitions = 3;
    int keys = 9;
    int perKey = 40;
    createTopic(topic, partitions);
    // Three keys in each partition, interleaved.
    Map<String, List<Long>> produced = new HashMap<>();
    for (int partition = 0; partition < partitions; partition++) {
      List<String> sequence = new ArrayList<>();
      for (int i = 0; i < perKey; i++) {
        for (int key = partition; key < keys; key += partitions) {
          sequence.add("key-" + key);
        }
      }
      List<Long> offsets = produce(topic, partition, sequence, "v");
      for (int i = 0; i < sequence.size(); i++) {
        produced.computeIfAbsent(sequence.get(i), key -> new ArrayList<>()).add(offsets.get(i));
      }
    }

    Map<String, AtomicInteger> inFlight = new ConcurrentHashMap<>();
    Map<String, List<Long>> order = new ConcurrentHashMap<>();
    List<String> overlaps = new CopyOnWriteArrayList<>();
    List<Sidecar> sidecars = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      Sidecar sidecar = sidecar("s" + i, declare(topic, s -> s.setOrdering(Ordering.ORDERING_KEY)));
      sidecar.onRecord =
          record -> {
            if (inFlight.computeIfAbsent(record.key(), key -> new AtomicInteger()).incrementAndGet()
                > 1) {
              overlaps.add(record.key() + " at " + record.at() + " on " + sidecar.name);
            }
            order
                .computeIfAbsent(record.key(), key -> new CopyOnWriteArrayList<>())
                .add(record.offset());
            // "Processing" takes a moment, on another thread, so that keys overlap in time.
            scheduler.schedule(
                () -> {
                  inFlight.get(record.key()).decrementAndGet();
                  sidecar.ack(record);
                  sidecar.credits(1);
                },
                ThreadLocalRandom.current().nextInt(4),
                TimeUnit.MILLISECONDS);
          };
      sidecars.add(sidecar);
    }
    sidecars.forEach(sidecar -> sidecar.credits(5));

    await("everything is delivered", () -> total(sidecars) >= keys * perKey);
    awaitCommitted(topic, partitions);
    assertThat(overlaps).as("two records of one key in flight at once").isEmpty();
    assertThat(order).as("each key's records, in the order received").isEqualTo(produced);
    assertThat(sidecars.stream().filter(sidecar -> !sidecar.received.isEmpty()).count())
        .as("sidecars that received records")
        .isGreaterThan(1);
  }

  @Test
  void whatAKilledSidecarHeldIsDeliveredToAnotherWithTheSameAttemptNumber() throws Exception {
    createTopic(topic, 1);
    produce(topic, 0, keys("k", 10), "v");

    Sidecar doomed = sidecar("doomed", declare(topic));
    doomed.credits(10);
    await("the first sidecar holds the batch", () -> doomed.received.size() == 10);
    Sidecar survivor = sidecar("survivor", declare(topic));
    survivor.credits(20);
    doomed.ack(0, 0, 2);
    await("what it acknowledged is committed", () -> committed(topic, 0) == 3);
    assertThat(survivor.received).isEmpty();

    doomed.kill();
    await("the rest is delivered to the other", () -> survivor.received.size() == 7);
    assertThat(survivor.offsets()).containsExactly("0/3", "0/4", "0/5", "0/6", "0/7", "0/8", "0/9");
    assertThat(survivor.received)
        .as("a sidecar that goes away uses up no attempt")
        .allMatch(record -> record.attempt() == 1);
    survivor.received.forEach(survivor::ack);
    awaitCommitted(topic, 1);
  }

  @Test
  void aRecordNotAcknowledgedWithinTheDeadlineIsDeliveredAgainWithAHigherAttempt()
      throws Exception {
    createTopic(topic, 1);
    produce(topic, 0, keys("k", 1), "v");
    Consumer<Settings.Builder> settings = s -> s.setMaxProcessingMs(1500);

    Sidecar slow = sidecar("slow", declare(topic, settings));
    long sent = System.nanoTime();
    slow.credits(1);
    await("the record is delivered", () -> slow.received.size() == 1);
    assertThat(slow.received.get(0).attempt()).isEqualTo(1);
    Sidecar other = sidecar("other", declare(topic, settings));
    other.credits(1);

    await("the record is delivered again", () -> other.received.size() == 1);
    long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - sent);
    assertThat(elapsed).as("not before the deadline").isGreaterThanOrEqualTo(1500);
    assertThat(other.received.get(0).at()).isEqualTo("0/0");
    assertThat(other.received.get(0).attempt()).isEqualTo(2);
    assertThat(committed(topic, 0)).isLessThanOrEqualTo(0);

    // The slow one finishes after all: its acknowledgement counts, the other's copy is a
    // duplicate.
    slow.ack(slow.received.get(0));
    awaitCommitted(topic, 1);
  }

  @Test
  void aRecordThatKeepsBeingRejectedBlocksItsPartitionAtItsOffset() throws Exception {
    createTopic(topic, 2);
    produce(topic, 0, keys("k", 6), "v");
    produce(topic, 1, keys("other", 3), "v");

    Sidecar sidecar =
        sidecar(
            "s", declare(topic, s -> s.setOrdering(Ordering.ORDERING_PARTITION).setMaxAttempts(3)));
    sidecar.onRecord =
        record -> {
          if (record.at().equals("0/2")) {
            sidecar.reject(record);
          } else {
            sidecar.ack(record);
          }
          sidecar.credits(1);
        };
    sidecar.credits(1);

    await(
        "the record used its three attempts",
        () -> sidecar.received.stream().filter(r -> r.at().equals("0/2")).count() == 3);
    assertThat(sidecar.received.stream().filter(r -> r.at().equals("0/2")).map(Received::attempt))
        .containsExactly(1, 2, 3);
    // What came before it is committed, and so is the other partition, to its end.
    await("the records before it are committed", () -> committed(topic, 0) == 2);
    await("the other partition is committed", () -> committed(topic, 1) == 3);

    // It stays there: nothing after it is delivered, whatever arrives.
    produce(topic, 0, keys("later", 3), "v");
    List<Long> elsewhere = produce(topic, 1, keys("later", 2), "v");
    await("the other partition goes on", () -> committed(topic, 1) == elsewhere.get(1) + 1);
    Thread.sleep(1500);
    assertThat(committed(topic, 0)).as("the commit position of the blocked partition").isEqualTo(2);
    assertThat(sidecar.offsets().stream().filter(at -> at.startsWith("0/")).distinct())
        .as("what was delivered of the blocked partition")
        .containsExactly("0/0", "0/1", "0/2");
  }

  @Test
  void aGroupWithoutCommittedOffsetsStartsWhereItsDeclarationSays() throws Exception {
    createTopic(topic, 2);
    createTopic(second, 2);
    long before = java.time.Instant.parse("2023-06-01T00:00:00Z").toEpochMilli();
    long after = java.time.Instant.parse("2024-06-01T00:00:00Z").toEpochMilli();
    for (int partition = 0; partition < 2; partition++) {
      produce(topic, partition, keys("k", 10), "old");
      produce(second, partition, keys("k", 10), "old", before);
      produce(second, partition, keys("k", 10), "new", after);
    }

    // At the end: the position found is committed, which is how the test knows it was found.
    Sidecar latest =
        sidecar("latest", declare(topic, s -> s.setOffsetReset(OffsetReset.OFFSET_RESET_LATEST)));
    latest.working().credits(100);
    awaitCommitted(topic, 2);
    assertThat(latest.received).isEmpty();
    for (int partition = 0; partition < 2; partition++) {
      produce(topic, partition, keys("k", 10), "new");
    }
    await("what arrived since is delivered", () -> latest.received.size() == 20);

    // At a timestamp.
    Sidecar timed =
        sidecar(
            "timed",
            declare(
                second,
                s ->
                    s.setStartTimestampMs(
                        java.time.Instant.parse("2024-01-01T00:00:00Z").toEpochMilli())));
    timed.working().credits(100);
    await("what is at or after the timestamp is delivered", () -> timed.received.size() == 20);
    awaitCommitted(second, 2);
    for (Sidecar sidecar : List.of(latest, timed)) {
      assertThat(sidecar.received.stream().map(Received::value))
          .as("what %s received", sidecar.name)
          .hasSize(20)
          .allMatch(value -> value.startsWith("new-"));
    }
  }

  // -------------------------------------------------------------------------------------------
  // Topics and restarts
  // -------------------------------------------------------------------------------------------

  @Test
  void aSecondTopicDeclaredWhileTheFirstIsConsumedStartsFlowingWithoutDisturbingIt()
      throws Exception {
    createTopic(topic, 2);
    createTopic(second, 2);
    Sidecar first = sidecar("first", declare(topic)).working();
    first.credits(20);

    // The first topic is written to all along.
    AtomicBoolean writing = new AtomicBoolean(true);
    AtomicInteger written = new AtomicInteger();
    List<Throwable> failures = new CopyOnWriteArrayList<>();
    Thread writer =
        new Thread(
            () -> {
              try {
                while (writing.get()) {
                  int n = written.get();
                  producer
                      .send(new ProducerRecord<>(topic, n % 2, "k" + n, "first-" + n))
                      .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
                  written.incrementAndGet();
                  Thread.sleep(2);
                }
              } catch (Exception e) {
                failures.add(e);
              }
            },
            "writer");
    writer.start();
    try {
      await("the first topic flows", () -> first.received.size() >= 100);

      // A new version of the application arrives with a second topic, in the same group.
      for (int partition = 0; partition < 2; partition++) {
        produce(second, partition, keys("k", 25), "second");
      }
      Sidecar newer =
          sidecar("newer", declare(second, s -> s.setOrdering(Ordering.ORDERING_NONE))).working();
      newer.credits(20);
      await("the second topic flows", () -> newer.received.size() == 50);
      assertThat(newer.received.stream().map(Received::value))
          .allMatch(value -> value.startsWith("second-"));

      // And the first went on through the rebalance.
      int afterwards = first.received.size() + 100;
      await("the first topic still flows", () -> first.received.size() >= afterwards);
    } finally {
      writing.set(false);
      writer.join(TIMEOUT.toMillis());
    }
    assertThat(failures).isEmpty();
    await("everything written is delivered", () -> first.received.size() >= written.get());
    awaitCommitted(topic, 2);
    awaitCommitted(second, 2);
    // The dispatcher is the group's only member and rebalances cooperatively: the partitions it
    // had were not taken from it, so nothing of the first topic was delivered twice.
    assertThat(first.received).as("records of the first topic").hasSize(written.get());
    assertThat(new HashSet<>(first.offsets())).hasSize(written.get());
    assertThat(first.received.stream().map(Received::value))
        .allMatch(value -> value.startsWith("first-"));
    assertThat(first.released).isEmpty();
  }

  @Test
  void aRestartedDispatcherResumesFromTheCommittedPosition() throws Exception {
    createTopic(topic, 1);
    produce(topic, 0, keys("k", 100), "v");

    Sidecar before = sidecar("before", declare(topic));
    before.credits(50);
    await("the sidecar holds fifty records", () -> before.received.size() == 50);
    before.ack(0, 0, 39);

    // It stops as a pod does: what was acknowledged is committed, the stream ends.
    harness.stopDispatcher();
    assertThat(before.status().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
    assertThat(committed(topic, 0)).as("committed when it stopped").isEqualTo(40);

    harness.startDispatcher();
    Sidecar after = sidecar("after", declare(topic)).working();
    after.credits(10);
    await("the rest is delivered", () -> after.received.size() >= 60);
    awaitCommitted(topic, 1);
    assertThat(after.received.get(0).at()).as("where delivery resumed").isEqualTo("0/40");
    assertThat(after.received.get(0).attempt()).as("attempts start again").isEqualTo(1);
    Set<String> all = new TreeSet<>(before.offsets().subList(0, 40));
    all.addAll(after.offsets());
    assertThat(all).as("nothing was lost").hasSize(100);
  }

  @Test
  void aTopicNobodyDeclaresAnyMoreIsGivenUpAfterTheGraceAndTakenUpAgain() throws Exception {
    harness.stop();
    harness =
        DispatcherHarness.start(
            CONFIG.replace("idleGrace: 60s", "idleGrace: 1s"), ServerHarness.KAFKA_A);
    createTopic(topic, 2);
    produce(topic, 0, keys("k", 10), "v");

    Sidecar first = sidecar("first", declare(topic)).working();
    first.credits(5);
    await("the first records are delivered", () -> first.received.size() == 10);
    awaitCommitted(topic, 2);
    first.close();
    assertThat(first.status().isOk()).isTrue();

    // With the grace over the dispatcher has left the group: nobody is a member of it.
    await(
        "the group is empty",
        () -> {
          try {
            return admin
                .describeConsumerGroups(List.of(group))
                .describedGroups()
                .get(group)
                .get(30, TimeUnit.SECONDS)
                .members()
                .isEmpty();
          } catch (Exception e) {
            return false;
          }
        });

    produce(topic, 1, keys("k", 10), "v");
    Sidecar next = sidecar("next", declare(topic)).working();
    next.credits(5);
    await("what arrived meanwhile is delivered", () -> next.received.size() == 10);
    assertThat(next.offsets()).allMatch(at -> at.startsWith("1/"));
    awaitCommitted(topic, 2);
  }

  // -------------------------------------------------------------------------------------------
  // The process
  // -------------------------------------------------------------------------------------------

  /** Needs the packaged server: the in-process harness runs neither Main nor the health service. */
  @Test
  void aServerThatOnlyDispatchesStartsServesAndSaysItsPolicyIsNotEnforced() throws Exception {
    Assumptions.assumeTrue(DispatcherHarness.packaged(), "needs the packaged server");
    ManagedChannel probe =
        NettyChannelBuilder.forAddress("localhost", harness.healthPort()).usePlaintext().build();
    try {
      await(
          "the health service answers SERVING",
          () -> {
            try {
              return ClientCalls.blockingUnaryCall(
                      probe,
                      HealthService.CHECK,
                      CallOptions.DEFAULT.withDeadlineAfter(5, TimeUnit.SECONDS),
                      "")
                  == ServingStatus.SERVING;
            } catch (RuntimeException e) {
              return false;
            }
          });
    } finally {
      probe.shutdownNow();
    }
    // Ready with no sidecar connected, and with no socket for an app that is not there, although
    // the configuration says where one would be.
    assertThat(Files.exists(harness.socket())).as("the app's socket exists").isFalse();
    assertThat(harness.log)
        .anyMatch(line -> line.contains("not listening on the app's Unix socket"))
        .anyMatch(line -> line.contains("WARN") && line.contains("POLICY IS NOT ENFORCED"));
  }
}
