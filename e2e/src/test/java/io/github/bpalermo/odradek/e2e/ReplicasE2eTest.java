package io.github.bpalermo.odradek.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.bpalermo.odradek.kafka.v1.Ack;
import io.github.bpalermo.odradek.kafka.v1.ConsumeRequest;
import io.github.bpalermo.odradek.kafka.v1.ConsumeResponse;
import io.github.bpalermo.odradek.kafka.v1.ConsumedRecord;
import io.github.bpalermo.odradek.kafka.v1.Credits;
import io.github.bpalermo.odradek.kafka.v1.OdradekServiceGrpc;
import io.github.bpalermo.odradek.kafka.v1.RecordBatch;
import io.github.bpalermo.odradek.kafka.v1.Subscribe;
import io.github.bpalermo.odradek.kafka.v1.Subscribed;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
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
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Several dispatcher replicas end to end (ADR 6, step 6): real brokers, real replicas that find
 * each other through their coordination group, and real sidecars that are configured with one
 * address, the relay that stands in for the dispatcher's Service, and learn of every replica from
 * whichever one answers there.
 *
 * <p>Everything is driven through the app's API on each sidecar's socket. What is asserted is what
 * the apps received, in what order, and what the brokers say of the two groups: who is a member of
 * the group being consumed, with which partitions, and what it has committed.
 */
class ReplicasE2eTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(90);
  private static final int PARTITIONS = 6;
  private static final int KEYS = 12;

  /**
   * Short timeouts, so that a replica that vanished is given up on in seconds, and a short
   * heartbeat, which is how fast a member hears that its group rebalances.
   */
  private static final String DISPATCHER =
      """
      clusters:
        a:
          required: true
          properties:
            bootstrap.servers: %s
          consumerProperties:
            session.timeout.ms: 6000
            heartbeat.interval.ms: 500
      consume:
        commitInterval: 0.2s
        revokeDeadline: 5s
      dispatch:
        port: 0
        idleGrace: 60s
        coordination:
          groupId: odradek-e2e-replicas-%s
      """;

  /** What a replica says when it has given partitions up: how many records it sent meanwhile. */
  private static final Pattern HELD_BACK_AND_SENT =
      Pattern.compile("having sent (\\d+) record\\(s\\) it had held back");

  private final String id = UUID.randomUUID().toString();
  private final String topic = "odradek-e2e-replicas-" + id;
  private final String group = "odradek-e2e-replicas-group-" + id;
  private final List<DispatcherHarness> replicas = new ArrayList<>();
  private final List<ServerHarness> sidecars = new ArrayList<>();

  /** Every record an app was handed, as "key:sequence", in the order the apps were handed them. */
  private final List<String> handled = new CopyOnWriteArrayList<>();

  /** What the group had committed for a partition when it was last looked at. */
  private final Map<String, Long> lastCommitted = new ConcurrentHashMap<>();

  private final List<String> commitsThatWentBack = new CopyOnWriteArrayList<>();

  /** Every value an app was handed, once. */
  private final Set<String> handedOnce = ConcurrentHashMap.newKeySet();

  /**
   * Records an app was handed again although the group had, to this test's knowledge at that
   * moment, already committed past them.
   */
  private final List<String> handedOutAfterCommit = new CopyOnWriteArrayList<>();

  /** Every record an app was handed, as "partition/offset". */
  private final Set<String> handedAt = ConcurrentHashMap.newKeySet();

  /** The highest offset of each partition that an app has acknowledged. */
  private final Map<Integer, Long> acknowledgedUpTo = new ConcurrentHashMap<>();

  private TcpRelay service;
  private KafkaProducer<String, String> producer;
  private Admin admin;
  private Flow flow;
  private Thread commitWatch;
  private volatile boolean watching;

  /** A scripted app: one raw Consume stream on a sidecar's socket. */
  private final class App {
    final ServerHarness sidecar;
    final List<String> received = new CopyOnWriteArrayList<>();
    final Set<Integer> partitions = ConcurrentHashMap.newKeySet();
    final CompletableFuture<Subscribed> subscribed = new CompletableFuture<>();
    final CompletableFuture<Status> closed = new CompletableFuture<>();
    private final StreamObserver<ConsumeRequest> requests;
    private volatile boolean working = true;

    /** Whether records of the slow keys are kept unacknowledged, and those that are. */
    private boolean holding;

    private final List<Ack> held = new ArrayList<>();

    /** Takes one record and never finishes it: it acknowledges nothing and asks for nothing. */
    private final boolean stuck;

    App(ServerHarness sidecar) {
      this(sidecar, 120_000, false);
    }

    App(ServerHarness sidecar, int maxProcessingMs, boolean stuck) {
      this.sidecar = sidecar;
      this.stuck = stuck;
      requests =
          OdradekServiceGrpc.newStub(sidecar.channel)
              .consume(
                  new StreamObserver<ConsumeResponse>() {
                    @Override
                    public void onNext(ConsumeResponse response) {
                      if (response.hasSubscribed()) {
                        subscribed.complete(response.getSubscribed());
                      } else if (response.hasRecords()) {
                        records(response.getRecords());
                      }
                    }

                    @Override
                    public void onError(Throwable t) {
                      closed.complete(Status.fromThrowable(t));
                      subscribed.completeExceptionally(t);
                    }

                    @Override
                    public void onCompleted() {
                      closed.complete(Status.OK);
                    }
                  });
      send(
          ConsumeRequest.newBuilder()
              .setSubscribe(
                  Subscribe.newBuilder().addTopics(topic).setMaxProcessingMs(maxProcessingMs)));
    }

    /** Handles each record as it arrives: notes it, acknowledges it, asks for one more. */
    private void records(RecordBatch batch) {
      for (ConsumedRecord record : batch.getRecordsList()) {
        if (!working) {
          return;
        }
        String value = record.getValue().toStringUtf8();
        if (stuck) {
          received.add(value);
          continue;
        }
        Long committed = lastCommitted.get("" + batch.getPartition());
        if (!handedOnce.add(value) && committed != null && committed > record.getOffset()) {
          handedOutAfterCommit.add(
              value
                  + " at "
                  + batch.getPartition()
                  + "/"
                  + record.getOffset()
                  + ", committed "
                  + committed);
        }
        // Noted before it is acknowledged: the next record of its key is sent only after.
        handled.add(value);
        received.add(value);
        partitions.add(batch.getPartition());
        handedAt.add(batch.getPartition() + "/" + record.getOffset());
        Ack ack =
            Ack.newBuilder()
                .setTopic(batch.getTopic())
                .setPartition(batch.getPartition())
                .setAssignmentEpoch(batch.getAssignmentEpoch())
                .setFirstOffset(record.getOffset())
                .setLastOffset(record.getOffset())
                .build();
        synchronized (this) {
          if (holding && slow(record.getKey().toStringUtf8())) {
            held.add(ack);
            continue;
          }
        }
        acknowledge(ack);
      }
    }

    /** Acknowledges one record and asks for one more. */
    private void acknowledge(Ack ack) {
      send(ConsumeRequest.newBuilder().setAck(ack));
      acknowledgedUpTo.merge(ack.getPartition(), ack.getLastOffset(), Math::max);
      send(ConsumeRequest.newBuilder().setCredits(Credits.newBuilder().setRecords(1)));
    }

    /**
     * From now on the app keeps the records of the slow keys it is handed without acknowledging
     * them: a handler that takes its time. The dispatcher sends it no further record of such a key
     * meanwhile, and goes on with the other keys.
     */
    synchronized void hold() {
      holding = true;
    }

    /** The slow handlers are done: what was kept is acknowledged, and nothing is kept any more. */
    void release() {
      List<Ack> kept;
      synchronized (this) {
        holding = false;
        kept = List.copyOf(held);
        held.clear();
      }
      kept.forEach(this::acknowledge);
    }

    /** Requests are written from the stream's thread and from the test's. */
    private synchronized void send(ConsumeRequest.Builder request) {
      requests.onNext(request.build());
    }

    App start() throws Exception {
      subscribed.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
      send(ConsumeRequest.newBuilder().setCredits(Credits.newBuilder().setRecords(stuck ? 1 : 16)));
      return this;
    }

    /** The app is done: its stream is closed in good order, and nothing more is handled. */
    void close() throws Exception {
      working = false;
      requests.onCompleted();
      assertThat(closed.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS)).isEqualTo(Status.OK);
    }
  }

  /**
   * Records that keep coming while a test does something to the dispatcher: for each of a few keys,
   * the values "key:0", "key:1", ... in that order, each key always to the same partition.
   */
  private final class Flow implements AutoCloseable {
    private final List<String> produced = new CopyOnWriteArrayList<>();

    /** Where each value was written, as "partition/offset". */
    private final Map<String, String> placed = new ConcurrentHashMap<>();

    private final AtomicBoolean stop = new AtomicBoolean();
    private final CompletableFuture<Void> done = new CompletableFuture<>();
    private final Thread thread;

    Flow() {
      thread =
          new Thread(
              () -> {
                try {
                  for (int sequence = 0; !stop.get(); sequence++) {
                    for (int key = 0; key < KEYS; key++) {
                      String value = "k" + key + ":" + sequence;
                      RecordMetadata written =
                          producer
                              .send(new ProducerRecord<>(topic, key % PARTITIONS, "k" + key, value))
                              .get(30, TimeUnit.SECONDS);
                      placed.put(value, written.partition() + "/" + written.offset());
                      produced.add(value);
                    }
                    // Paces the flow; nothing waits on it.
                    Thread.sleep(10);
                  }
                  done.complete(null);
                } catch (Exception e) {
                  done.completeExceptionally(e);
                }
              },
              "flow");
      thread.setDaemon(true);
      thread.start();
    }

    int count() {
      return produced.size();
    }

    /** Stops producing and returns everything that was written, in order. */
    List<String> finish() throws Exception {
      stop.set(true);
      done.get(60, TimeUnit.SECONDS);
      return List.copyOf(produced);
    }

    @Override
    public void close() {
      stop.set(true);
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
    service = new TcpRelay();
    ServerHarness.createTopic(ServerHarness.KAFKA_A, topic, PARTITIONS);
  }

  @AfterEach
  void stop() throws Exception {
    watching = false;
    if (commitWatch != null) {
      commitWatch.join(TimeUnit.SECONDS.toMillis(40));
    }
    if (flow != null) {
      flow.close();
    }
    for (ServerHarness sidecar : sidecars) {
      sidecar.stop();
    }
    for (DispatcherHarness replica : replicas) {
      replica.stop();
    }
    service.close();
    producer.close(Duration.ofSeconds(5));
    admin.close(Duration.ofSeconds(5));
    ServerHarness.deleteTopics(ServerHarness.KAFKA_A, List.of(topic));
  }

  // -------------------------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------------------------

  /** Starts the first replica, or another one of the same dispatcher. */
  private DispatcherHarness replica() throws Exception {
    DispatcherHarness replica =
        replicas.isEmpty()
            ? DispatcherHarness.start(DISPATCHER, ServerHarness.KAFKA_A, id)
            : replicas.get(0).replica();
    replicas.add(replica);
    return replica;
  }

  /** Starts this many replicas and puts them all behind the one address sidecars know. */
  private void replicas(int count) throws Exception {
    DispatcherHarness first = replica();
    // The others at the same time, as a Deployment starts its pods: a process takes a while.
    List<CompletableFuture<DispatcherHarness>> others = new ArrayList<>();
    for (int i = 1; i < count; i++) {
      others.add(
          CompletableFuture.supplyAsync(
              () -> {
                try {
                  return first.replica();
                } catch (Exception e) {
                  throw new CompletionException(e);
                }
              }));
    }
    Exception failure = null;
    for (CompletableFuture<DispatcherHarness> other : others) {
      try {
        // Each one that started is kept, so that it is stopped, whatever became of the others.
        replicas.add(other.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS));
      } catch (Exception e) {
        failure = e;
      }
    }
    if (failure != null) {
      throw failure;
    }
    service.target(replicas.stream().mapToInt(DispatcherHarness::port).toArray());
    await(
        "every replica knows of " + count + " replicas",
        () -> replicas.stream().allMatch(replica -> replica.replicaCount() == count));
  }

  /** Starts a sidecar that knows the dispatcher by the relay's address, and its app. */
  private App app() throws Exception {
    return app("ORDERING_KEY");
  }

  /** As above, for a topic consumed with this ordering. */
  private App app(String ordering) throws Exception {
    ServerHarness sidecar =
        ServerHarness.start(
            """
            listener:
              socketPath: %s/kafka.sock
            dispatcher:
              address: localhost:%s
            consumers:
              %s:
                source: CONSUMER_SOURCE_DISPATCHER
                cluster: a
                groupId: %s
                ordering: %s
            """,
            service.port(), topic, group, ordering);
    sidecars.add(sidecar);
    return new App(sidecar).start();
  }

  /**
   * Starts a sidecar whose entry gives a record up after its first failed attempt, to this
   * dead-letter topic, and its app.
   */
  private App deadLettering(String deadLetters, int maxProcessingMs, boolean stuck)
      throws Exception {
    ServerHarness sidecar =
        ServerHarness.start(
            """
            listener:
              socketPath: %s/kafka.sock
            dispatcher:
              address: localhost:%s
            consumers:
              %s:
                source: CONSUMER_SOURCE_DISPATCHER
                cluster: a
                groupId: %s
                ordering: ORDERING_KEY
                maxAttempts: 1
                onExhausted: ON_EXHAUSTED_DEAD_LETTER
                deadLetterTopic: %s
            """,
            service.port(), topic, group, deadLetters);
    sidecars.add(sidecar);
    return new App(sidecar, maxProcessingMs, stuck).start();
  }

  private static void await(String what, BooleanSupplier condition) throws InterruptedException {
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (!condition.getAsBoolean()) {
      assertThat(System.nanoTime()).as("%s within %s", what, TIMEOUT).isLessThan(deadline);
      Thread.sleep(25);
    }
  }

  /**
   * Writes one record for each key, {@code rounds} times over, and returns the values. They are
   * written at once, so that several are in flight together and every pod gets some.
   */
  private List<String> produce(String prefix, int rounds) throws Exception {
    List<String> values = new ArrayList<>();
    List<Future<RecordMetadata>> sends = new ArrayList<>();
    for (int round = 0; round < rounds; round++) {
      for (int key = 0; key < KEYS; key++) {
        String value = prefix + key + ":" + round;
        sends.add(
            producer.send(new ProducerRecord<>(topic, key % PARTITIONS, prefix + key, value)));
        values.add(value);
      }
    }
    for (Future<RecordMetadata> send : sends) {
      send.get(30, TimeUnit.SECONDS);
    }
    return values;
  }

  /**
   * Writes rounds of records until each of these apps has been handed one, and everything written
   * has been handled by some app: a sidecar opens its streams when its app subscribes, and nothing
   * says when that is done.
   */
  private List<String> untilHanded(String prefix, App... waited) throws Exception {
    List<String> produced = new ArrayList<>();
    for (int round = 0; List.of(waited).stream().anyMatch(app -> app.received.isEmpty()); round++) {
      assertThat(round).as("rounds until every new stream is handed a record").isLessThan(200);
      List<String> values = produce(prefix + round + "-", 4);
      produced.addAll(values);
      awaitHandled("the records of round " + round + " are handled", values);
    }
    return produced;
  }

  /**
   * The members of the group being consumed and the partitions each has, as the brokers know them;
   * null while the group is not stable.
   */
  private Map<String, Set<Integer>> members() {
    try {
      ConsumerGroupDescription description =
          admin
              .describeConsumerGroups(List.of(group))
              .describedGroups()
              .get(group)
              .get(30, TimeUnit.SECONDS);
      if (description.groupState() != GroupState.STABLE) {
        return null;
      }
      Map<String, Set<Integer>> members = new TreeMap<>();
      description
          .members()
          .forEach(
              member -> {
                Set<Integer> partitions = new TreeSet<>();
                member
                    .assignment()
                    .topicPartitions()
                    .forEach(partition -> partitions.add(partition.partition()));
                members.put(member.consumerId(), partitions);
              });
      return members;
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Waits until the group being consumed has this many members, which between them have every
   * partition, and returns them. Each member is the consumer of one replica: a replica joins the
   * group when a sidecar declares it to that replica, so this many members is this many replicas
   * that a sidecar has a stream to.
   */
  private Map<String, Set<Integer>> awaitMembers(int count) throws InterruptedException {
    List<Map<String, Set<Integer>>> seen = new ArrayList<>();
    await(
        count + " replicas consume the group and have its " + PARTITIONS + " partitions",
        () -> {
          Map<String, Set<Integer>> members = members();
          if (members == null
              || members.size() != count
              || members.values().stream().mapToInt(Set::size).sum() != PARTITIONS
              || members.values().stream().anyMatch(Set::isEmpty)) {
            return false;
          }
          seen.add(members);
          return true;
        });
    return seen.get(seen.size() - 1);
  }

  /** Waits until the group has committed the end of every partition. */
  private void awaitCommitted() throws Exception {
    Map<TopicPartition, OffsetSpec> request = new HashMap<>();
    for (int partition = 0; partition < PARTITIONS; partition++) {
      request.put(new TopicPartition(topic, partition), OffsetSpec.latest());
    }
    Map<String, Long> ends = new TreeMap<>();
    admin
        .listOffsets(request)
        .all()
        .get(30, TimeUnit.SECONDS)
        .forEach((partition, end) -> ends.put("" + partition.partition(), end.offset()));
    await(
        "everything is committed (" + ends + ")",
        () -> {
          Map<String, Long> now = committed();
          return now != null
              && ends.entrySet().stream()
                  .allMatch(end -> end.getValue().equals(now.get(end.getKey())));
        });
  }

  /** What the group has committed, by partition; null when the brokers could not be asked. */
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
                  offsets.put("" + partition.partition(), offset.offset());
                }
              });
      return offsets;
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Looks at what the group has committed, again and again, for as long as the test runs: whoever
   * owns a partition, its committed offset must never be lower than it was.
   */
  private void watchCommits() {
    watching = true;
    commitWatch =
        new Thread(
            () -> {
              while (watching) {
                Map<String, Long> now = committed();
                if (now != null) {
                  now.forEach(
                      (partition, offset) -> {
                        Long before = lastCommitted.put(partition, offset);
                        if (before != null && offset < before) {
                          commitsThatWentBack.add(partition + ": " + before + " -> " + offset);
                        }
                      });
                }
                try {
                  // The pace of the watch; nothing waits on it.
                  Thread.sleep(50);
                } catch (InterruptedException e) {
                  return;
                }
              }
            },
            "commit-watch");
    commitWatch.setDaemon(true);
    commitWatch.start();
  }

  /** Waits until the apps have between them handled every one of {@code values}. */
  private void awaitHandled(String what, List<String> values) throws InterruptedException {
    await(what, () -> new HashSet<>(handled).containsAll(values));
  }

  /**
   * The sequences of each key in the order the apps were handed them, a record handed out again
   * counted only the first time.
   */
  private Map<String, List<Integer>> firstByKey() {
    Map<String, List<Integer>> byKey = new TreeMap<>();
    Set<String> seen = new HashSet<>();
    for (String value : List.copyOf(handled)) {
      if (!value.startsWith("k") || !seen.add(value)) {
        continue;
      }
      int colon = value.indexOf(':');
      byKey
          .computeIfAbsent(value.substring(0, colon), key -> new ArrayList<>())
          .add(Integer.parseInt(value.substring(colon + 1)));
    }
    return byKey;
  }

  /**
   * Checks what holds across every hand-over, in good order or not: for every key, the records
   * reached the apps in the order they were written, when a record that was handed out a second
   * time is looked at only the first time; nothing the group had committed past was handed out
   * again; and what the group has committed never went back.
   */
  private void assertOrderPerKey(List<String> produced) {
    assertThat(handedOutAfterCommit).as("handed out again after it was committed").isEmpty();
    assertThat(commitsThatWentBack).as("committed offsets that went back").isEmpty();
    Map<String, List<Integer>> byKey = firstByKey();
    assertThat(byKey).as("every key was handled").hasSize(KEYS);
    int perKey = produced.size() / KEYS;
    byKey.forEach(
        (key, sequences) -> {
          List<Integer> expected = new ArrayList<>();
          for (int sequence = 0; sequence < perKey; sequence++) {
            expected.add(sequence);
          }
          assertThat(sequences)
              .as("the records of %s, in the order handled", key)
              .isEqualTo(expected);
        });
  }

  /**
   * Whether a key of the flow is one whose handler can be made slow: the first key of each
   * partition. The other key of the partition is never held up.
   */
  private static boolean slow(String key) {
    return key.startsWith("k") && Integer.parseInt(key.substring(1)) < PARTITIONS;
  }

  /**
   * The records of the flow that a dispatcher holds back below a record it was acknowledged, by
   * partition: written at a lower offset than one an app has acknowledged, and handed to no app
   * yet. The group cannot commit past the first of them, whatever is acknowledged above.
   */
  private Map<Integer, List<String>> heldBack() {
    Map<Integer, List<String>> byPartition = new TreeMap<>();
    for (int partition = 0; partition < PARTITIONS; partition++) {
      byPartition.put(partition, new ArrayList<>());
    }
    flow.placed.forEach(
        (value, place) -> {
          int slash = place.indexOf('/');
          int partition = Integer.parseInt(place.substring(0, slash));
          long offset = Long.parseLong(place.substring(slash + 1));
          Long acknowledged = acknowledgedUpTo.get(partition);
          if (acknowledged != null && offset < acknowledged && !handedAt.contains(place)) {
            byPartition.get(partition).add(value);
          }
        });
    return byPartition;
  }

  /** The values handed to an app more than once. */
  private List<String> handledTwice() {
    Set<String> seen = new HashSet<>();
    List<String> twice = new ArrayList<>();
    for (String value : List.copyOf(handled)) {
      if (!seen.add(value)) {
        twice.add(value);
      }
    }
    return twice;
  }

  /** The highest generation with which any replica joined the group being consumed. */
  private int generation() {
    return replicas.stream().mapToInt(replica -> replica.generation(group)).max().orElse(-1);
  }

  // -------------------------------------------------------------------------------------------
  // Finding the replicas
  // -------------------------------------------------------------------------------------------

  @Test
  void aSidecarThatKnowsOneAddressEndsWithAStreamToEachOfThreeReplicas() throws Exception {
    replicas(3);
    // Every replica holds the same list, under the same generation, in the same order.
    assertThat(replicas.stream().map(DispatcherHarness::replicas).distinct()).hasSize(1);

    // One sidecar, which reached one replica through the address it was given. All three join
    // the group all the same, because it declares to each; and it receives the partitions of each.
    App first = app();
    Map<String, Set<Integer>> members = awaitMembers(3);
    List<String> produced = new ArrayList<>(produce("a", 5));
    awaitHandled("the first app is handed every partition's records", produced);
    assertThat(first.partitions).hasSize(PARTITIONS);

    // A second Consume stream of the same pod has streams of its own to every replica, on the
    // connections the pod already has: nothing new reaches the Service's address.
    assertThat(service.accepted()).as("connections to the Service's address").isEqualTo(1);
    App sibling = new App(first.sidecar).start();
    produced.addAll(untilHanded("s", sibling));
    assertThat(service.accepted()).as("connections to the Service's address").isEqualTo(1);
    sibling.close();

    // A second pod, which the relay hands to another replica: the same.
    App second = app();
    first.close();
    List<String> more = produce("b", 5);
    await(
        "the second app is handed every partition's records",
        () -> second.received.containsAll(more));
    assertThat(second.partitions).hasSize(PARTITIONS);
    awaitCommitted();

    assertThat(awaitMembers(3)).as("the group, which no sidecar changed").isEqualTo(members);
    assertThat(handledTwice()).isEmpty();
    assertThat(second.closed).isNotDone();
  }

  @Test
  void theApplicationIsScaledUpAndDownWithoutARebalanceOfItsGroup() throws Exception {
    replicas(2);
    App first = app();
    Map<String, Set<Integer>> members = awaitMembers(2);
    List<String> produced = new ArrayList<>(produce("a", 5));
    awaitHandled("the first pod is handed the records", produced);
    assertThat(first.partitions).hasSize(PARTITIONS);
    String list = replicas.get(0).replicas();
    int generation = generation();

    // Up: three more pods, each through the one address, each with a stream to both replicas.
    List<App> more = List.of(app(), app(), app());
    produced.addAll(untilHanded("up", more.toArray(App[]::new)));

    // Down: the first pod and two of the new ones go, one of them without a word.
    first.close();
    more.get(0).close();
    more.get(1).sidecar.kill();
    List<String> after = produce("down", 5);
    produced.addAll(after);
    await(
        "the pod that is left is handed everything", () -> more.get(2).received.containsAll(after));
    assertThat(more.get(2).partitions).hasSize(PARTITIONS);
    awaitHandled("everything was handled", produced);
    awaitCommitted();

    // Pods came and went as streams. The group never heard of them: the same members hold the
    // same partitions, and no replica joined the group again.
    assertThat(awaitMembers(2))
        .as("the members of the group and their partitions")
        .isEqualTo(members);
    if (DispatcherHarness.packaged()) {
      assertThat(generation).as("the group's generation before").isPositive();
      assertThat(generation())
          .as("the group's generation, which a rebalance raises")
          .isEqualTo(generation);
    }
    assertThat(replicas.get(0).replicas()).as("the list of replicas").isEqualTo(list);
    assertThat(replicas.get(1).replicas()).isEqualTo(list);
  }

  // -------------------------------------------------------------------------------------------
  // Replicas that come and go while records flow
  // -------------------------------------------------------------------------------------------

  @Test
  void aReplicaStoppedInGoodOrderHandsItsPartitionsOverWithNothingLostOrHandledTwice()
      throws Exception {
    replicas(2);
    // One record of a partition in flight at a time: once the replica has been acknowledged what
    // it sent, nothing of the partition is acknowledged above something that is not. The offset
    // it commits is then exactly where it stopped, and the next owner repeats nothing.
    App one = app("ORDERING_PARTITION");
    App two = app("ORDERING_PARTITION");
    awaitMembers(2);
    watchCommits();
    flow = new Flow();
    await("records flow", () -> handled.size() >= 10 * KEYS);

    // It sends nothing new, waits for what its sidecars hold, commits, and only then leaves.
    replicas.get(1).stopDispatcher();
    int stoppedAt = flow.count();
    Map<String, Set<Integer>> left = awaitMembers(1);
    assertThat(left.values().iterator().next()).as("the replica that is left").hasSize(PARTITIONS);
    await("records flow after the hand-over", () -> flow.count() >= stoppedAt + 10 * KEYS);

    List<String> produced = flow.finish();
    awaitHandled("everything is handled", produced);
    awaitCommitted();
    assertThat(handledTwice()).as("handled twice across a hand-over in good order").isEmpty();
    assertOrderPerKey(produced);
    assertThat(one.closed).as("the first app's stream").isNotDone();
    assertThat(two.closed).as("the second app's stream").isNotDone();
    await("the replica that is left lists itself alone", () -> replicas.get(0).replicaCount() == 1);
  }

  @Test
  void aReplicaStoppedInGoodOrderFinishesWhatItHeldBackAndNothingAcknowledgedIsHandledTwice()
      throws Exception {
    replicas(2);
    // Key ordering, and two keys to a partition: while the handler of one key takes its time,
    // the later records of that key wait in the dispatcher, and those of the other key are sent
    // and acknowledged past them. The group can commit nothing above the first of them.
    App one = app();
    App two = app();
    awaitMembers(2);
    watchCommits();
    flow = new Flow();
    await("records flow", () -> handled.size() >= 10 * KEYS);

    one.hold();
    two.hold();
    await(
        "every partition has records held back below a record that is acknowledged",
        () -> heldBack().values().stream().allMatch(values -> values.size() >= 3));

    DispatcherHarness stopped = replicas.get(1);
    String givesUp = "group " + group + " on cluster a gives up";
    String gaveUp = "group " + group + " on cluster a gave up";
    int saidBefore = stopped.logged(givesUp).size();
    int doneBefore = stopped.logged(gaveUp).size();
    CompletableFuture<Void> stopping =
        CompletableFuture.runAsync(
            () -> {
              try {
                stopped.stopDispatcher();
              } catch (Exception e) {
                throw new CompletionException(e);
              }
            });
    // The replica says so on the thread that then hands the partitions over, before anything
    // else it is sent is looked at: whatever the apps do from here on meets a hand-over under way.
    await("the replica gives its partitions up", () -> stopped.logged(givesUp).size() > saidBefore);

    // This is what the test is about, so it is checked and not assumed: when the hand-over began,
    // each partition had records that no app had been handed, below records that were
    // acknowledged. Nothing could have been sent of them before: the first record of their key is
    // still with an app, unacknowledged.
    Map<Integer, List<String>> heldBack = heldBack();
    assertThat(heldBack.values())
        .as("records held back below acknowledged ones when the hand-over began: %s", heldBack)
        .allMatch(values -> values.size() >= 3);

    // The slow handlers finish. The replica that is leaving sends what waited behind them, in
    // the order of each key, is acknowledged it, and only then commits and goes.
    one.release();
    two.release();
    stopping.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
    int stoppedAt = flow.count();
    Map<String, Set<Integer>> left = awaitMembers(1);
    assertThat(left.values().iterator().next()).as("the replica that is left").hasSize(PARTITIONS);
    await("records flow after the hand-over", () -> flow.count() >= stoppedAt + 10 * KEYS);

    List<String> produced = flow.finish();
    awaitHandled("everything is handled", produced);
    awaitCommitted();
    assertThat(handledTwice())
        .as("handled twice across a hand-over in good order, under key ordering")
        .isEmpty();
    assertOrderPerKey(produced);
    // It had half of the group's partitions, each with three such records or more, and says how
    // many it sent while it gave them up.
    List<String> done = stopped.logged(gaveUp);
    long sent = 0;
    for (String line : done.subList(doneBefore, done.size())) {
      Matcher matcher = HELD_BACK_AND_SENT.matcher(line);
      assertThat(matcher.find()).as(line).isTrue();
      sent += Long.parseLong(matcher.group(1));
    }
    assertThat(sent)
        .as("records the replica sent while it gave its partitions up: %s", done)
        .isGreaterThanOrEqualTo(3 * PARTITIONS / 2);
    assertThat(one.closed).as("the first app's stream").isNotDone();
    assertThat(two.closed).as("the second app's stream").isNotDone();
    await("the replica that is left lists itself alone", () -> replicas.get(0).replicaCount() == 1);
  }

  @Test
  void aReplicaThatIsKilledLosesNothingAndKeepsTheOrderOfEachKey() throws Exception {
    replicas(2);
    App one = app();
    App two = app();
    awaitMembers(2);
    watchCommits();
    flow = new Flow();
    await("records flow", () -> handled.size() >= 10 * KEYS);

    // No commit, no word to its sidecars, and the brokers hear of it when its session runs out.
    replicas.get(1).kill();
    int killedAt = flow.count();
    Map<String, Set<Integer>> left = awaitMembers(1);
    assertThat(left.values().iterator().next()).as("the replica that is left").hasSize(PARTITIONS);
    await("records flow after the partitions moved", () -> flow.count() >= killedAt + 10 * KEYS);

    List<String> produced = flow.finish();
    awaitHandled("everything is handled", produced);
    awaitCommitted();
    // What the dead replica had sent and not committed is handed out again by the other: twice,
    // but never a later record of a key before an earlier one was handed out.
    assertOrderPerKey(produced);
    assertThat(one.closed).as("the first app's stream").isNotDone();
    assertThat(two.closed).as("the second app's stream").isNotDone();
    await("the replica that is left lists itself alone", () -> replicas.get(0).replicaCount() == 1);
  }

  @Test
  void aReplicaAddedWhileConsumingIsDeclaredToAndTakesItsShare() throws Exception {
    replicas(1);
    App one = app();
    App two = app();
    awaitMembers(1);
    watchCommits();
    flow = new Flow();
    await("records flow", () -> handled.size() >= 10 * KEYS);

    // Nothing is deployed to the sidecars and they do not look at the address they were given
    // again: the replica that was there tells them of the new one, they declare to it, and only
    // then does it join the group and take partitions from the first, which hands them over.
    int connections = service.accepted();
    replica();
    int addedAt = flow.count();
    Map<String, Set<Integer>> members = awaitMembers(2);
    assertThat(members.values()).allMatch(partitions -> partitions.size() == PARTITIONS / 2);
    await("records flow after the hand-over", () -> flow.count() >= addedAt + 10 * KEYS);

    List<String> produced = flow.finish();
    awaitHandled("everything is handled", produced);
    awaitCommitted();
    // Under key ordering a record can be acknowledged above one of another key that was not
    // sent yet. The first replica sends it before it gives the partition up, so that what it
    // commits is past everything it was acknowledged, and the second hands nothing out again.
    assertThat(handledTwice()).as("handled twice across a hand-over in good order").isEmpty();
    assertOrderPerKey(produced);
    assertThat(service.accepted())
        .as("connections to the address of the Service")
        .isEqualTo(connections);
    assertThat(one.closed).isNotDone();
    assertThat(two.closed).isNotDone();
  }

  @Test
  void anAppStuckOnARecordCostsAnAttemptOfThatRecordAndOfNoOther() throws Exception {
    // ADR 7. A sidecar holds a spare credit on each replica's stream, so the replicas that did not
    // send the stuck app its record see its pod as idle and send it theirs. With one attempt each,
    // a record that waited there past its deadline would be dead-lettered; given back unread, it
    // goes to the other app with its attempt intact.
    String deadLetters = topic + "-dead-letters";
    ServerHarness.createTopic(ServerHarness.KAFKA_A, deadLetters, 1);
    try {
      replicas(3);
      // One deadline for both: the replicas send under the one declared last, whichever app that
      // is. Long enough for the other app to answer on a slow machine.
      int deadline = 5_000;
      deadLettering(deadLetters, deadline, false);
      App stuck = deadLettering(deadLetters, deadline, true);
      awaitMembers(3);

      List<String> produced = new ArrayList<>();
      for (int round = 0; stuck.received.isEmpty(); round++) {
        assertThat(round).as("rounds until the stuck app is handed a record").isLessThan(400);
        produced.addAll(produce("a" + round + "-", 1));
        Thread.sleep(50);
      }
      // Records keep coming for well past the stuck app's deadline.
      for (int round = 0; round < 50; round++) {
        produced.addAll(produce("b" + round + "-", 1));
        Thread.sleep(200);
      }
      String held = stuck.received.get(0);
      List<String> others = produced.stream().filter(value -> !value.equals(held)).toList();
      awaitHandled("every record but the one the stuck app holds", others);

      // Its record missed its deadline, its one attempt: that one, and only that one, is given up.
      List<ConsumerRecord<String, String>> letters =
          ServerHarness.read(ServerHarness.KAFKA_A, deadLetters, 1, 1);
      Thread.sleep(3_000);
      letters = ServerHarness.read(ServerHarness.KAFKA_A, deadLetters, 1, letters.size());
      assertThat(letters.stream().map(ConsumerRecord::value)).containsExactly(held);
      assertThat(stuck.received).containsExactly(held);
    } finally {
      ServerHarness.deleteTopics(ServerHarness.KAFKA_A, List.of(deadLetters));
    }
  }

  @Test
  void aRestartedReplicaIsFoundThroughTheListAndNotThroughTheAddressOfTheService()
      throws Exception {
    replicas(2);
    App app = app();
    awaitMembers(2);
    List<String> produced = new ArrayList<>(produce("a", 5));
    awaitHandled("the app is handed the records", produced);
    assertThat(app.partitions).hasSize(PARTITIONS);

    // The replica the sidecar reached through the Service's address is replaced: another
    // process, at another address. Nothing answers at the Service's address meanwhile, so
    // whatever the sidecar learns, it learns from the replica it still has a stream to.
    int connections = service.accepted();
    service.target(-1);
    DispatcherHarness old = replicas.get(0);
    int oldPort = old.port();
    old.stopDispatcher();
    await("the other replica lists itself alone", () -> replicas.get(1).replicaCount() == 1);
    old.startDispatcher();
    assertThat(old.port()).as("the port of the new replica").isNotEqualTo(oldPort);

    awaitMembers(2);
    List<String> more = produce("b", 5);
    produced.addAll(more);
    await(
        "the app is handed every partition's records again", () -> app.received.containsAll(more));
    awaitCommitted();
    assertThat(service.accepted())
        .as("connections to the address of the Service")
        .isEqualTo(connections);
    assertThat(app.closed).isNotDone();
    assertThat(new HashSet<>(handled)).containsAll(produced);
  }
}
