package io.github.bpalermo.odradek.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.bpalermo.odradek.client.ConsumerListener;
import io.github.bpalermo.odradek.client.ConsumerOptions;
import io.github.bpalermo.odradek.client.OdradekClient;
import io.github.bpalermo.odradek.client.OutgoingRecord;
import io.github.bpalermo.odradek.client.Producer;
import io.github.bpalermo.odradek.client.SendResult;
import io.github.bpalermo.odradek.kafka.v1.Ack;
import io.github.bpalermo.odradek.kafka.v1.ConsumeRequest;
import io.github.bpalermo.odradek.kafka.v1.ConsumeResponse;
import io.github.bpalermo.odradek.kafka.v1.ConsumedRecord;
import io.github.bpalermo.odradek.kafka.v1.Credits;
import io.github.bpalermo.odradek.kafka.v1.OdradekServiceGrpc;
import io.github.bpalermo.odradek.kafka.v1.RecordBatch;
import io.github.bpalermo.odradek.kafka.v1.Subscribe;
import io.github.bpalermo.odradek.kafka.v1.Subscribed;
import io.github.bpalermo.odradek.server.admin.HealthService;
import io.github.bpalermo.odradek.server.admin.HealthService.ServingStatus;
import io.grpc.CallOptions;
import io.grpc.Status;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Consuming through a dispatcher end to end (ADR 6, step 4): a real broker, a real dispatcher, and
 * real sidecars, which are servers whose consumer entries have the dispatcher as their source.
 * Everything is driven through the app's API on each sidecar's Unix socket, with the Java client
 * library or with raw Consume streams where a test has to decide what the app does; what is
 * asserted is what the apps received and what the group has committed on the broker.
 */
// The clients and consumers in try-with-resources work in the background; closing them is the
// point.
@SuppressWarnings("try")
class ReceiveE2eTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(60);
  private static final String DISPATCHER =
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
  private final String topic = "odradek-e2e-receive-" + id;
  private final String second = "odradek-e2e-receive-second-" + id;
  private final String group = "odradek-e2e-receive-" + id;
  private final List<String> topics = new ArrayList<>();
  private final List<ServerHarness> sidecars = new ArrayList<>();
  private final List<AutoCloseable> closeables = new ArrayList<>();

  private DispatcherHarness dispatcher;
  private KafkaProducer<String, String> producer;
  private Admin admin;

  /** One record as an app received it. */
  private record Got(
      App app,
      String topic,
      int partition,
      long epoch,
      long offset,
      String key,
      String value,
      String decodeError) {}

  /** A scripted app: one raw Consume stream on a sidecar's socket. */
  private final class App {
    final List<Got> received = new CopyOnWriteArrayList<>();
    final List<String> lost = new CopyOnWriteArrayList<>();
    final CompletableFuture<Subscribed> subscribed = new CompletableFuture<>();
    final CompletableFuture<Status> closed = new CompletableFuture<>();
    private final StreamObserver<ConsumeRequest> requests;

    /** What it does with each record, on the stream's thread. By default: holds it. */
    volatile Consumer<Got> onRecord = record -> {};

    App(ServerHarness sidecar, int maxProcessingMs, String... names) {
      requests =
          OdradekServiceGrpc.newStub(sidecar.channel)
              .consume(
                  new StreamObserver<ConsumeResponse>() {
                    @Override
                    public void onNext(ConsumeResponse response) {
                      switch (response.getMsgCase()) {
                        case SUBSCRIBED -> subscribed.complete(response.getSubscribed());
                        case RECORDS -> records(response.getRecords());
                        case LOST ->
                            response
                                .getLost()
                                .getPartitionsList()
                                .forEach(p -> lost.add(p.getTopic() + "/" + p.getPartition()));
                        default -> {}
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
                  Subscribe.newBuilder()
                      .addAllTopics(List.of(names))
                      .setMaxProcessingMs(maxProcessingMs)));
    }

    private void records(RecordBatch batch) {
      for (ConsumedRecord record : batch.getRecordsList()) {
        Got one =
            new Got(
                this,
                batch.getTopic(),
                batch.getPartition(),
                batch.getAssignmentEpoch(),
                record.getOffset(),
                record.hasKey() ? record.getKey().toStringUtf8() : null,
                record.hasValue() ? record.getValue().toStringUtf8() : null,
                record.hasDecodeError() ? record.getDecodeError() : null);
        received.add(one);
        onRecord.accept(one);
      }
    }

    /** Requests are written from the stream's thread and from the test's. */
    private synchronized void send(ConsumeRequest.Builder request) {
      requests.onNext(request.build());
    }

    App awaitSubscribed() throws Exception {
      subscribed.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
      return this;
    }

    void credits(int records) {
      send(ConsumeRequest.newBuilder().setCredits(Credits.newBuilder().setRecords(records)));
    }

    void ack(Got record) {
      send(
          ConsumeRequest.newBuilder()
              .setAck(
                  Ack.newBuilder()
                      .setTopic(record.topic())
                      .setPartition(record.partition())
                      .setAssignmentEpoch(record.epoch())
                      .setFirstOffset(record.offset())
                      .setLastOffset(record.offset())));
    }

    /** Acknowledges every record as it arrives and asks for one more. */
    App working() {
      onRecord =
          record -> {
            ack(record);
            credits(1);
          };
      return this;
    }

    /** The app is done: the stream is closed in good order. */
    void close() {
      onRecord = record -> {};
      requests.onCompleted();
    }

    Status status() throws Exception {
      return closed.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
    }

    Set<String> values() {
      Set<String> values = new TreeSet<>();
      received.forEach(record -> values.add(String.valueOf(record.value())));
      return values;
    }
  }

  @BeforeEach
  void start() {
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
  }

  @AfterEach
  void stop() throws Exception {
    for (AutoCloseable closeable : closeables.reversed()) {
      try {
        closeable.close();
      } catch (Exception e) {
        // Its sidecar may be gone already.
      }
    }
    for (ServerHarness sidecar : sidecars) {
      sidecar.stop();
    }
    if (dispatcher != null) {
      dispatcher.stop();
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

  private DispatcherHarness startDispatcher() throws Exception {
    dispatcher = DispatcherHarness.start(DISPATCHER, ServerHarness.KAFKA_A);
    return dispatcher;
  }

  /**
   * The configuration of a sidecar that receives {@link #topic} from the dispatcher at a local
   * port.
   *
   * @param more further top-level sections, as YAML
   * @param entry further lines of the topic's consumer entry
   */
  private String sidecarYaml(int dispatcherPort, String more, String... entry) {
    StringBuilder yaml =
        new StringBuilder()
            .append("listener:\n  socketPath: %s/kafka.sock\n")
            .append("dispatcher:\n  address: localhost:")
            .append(dispatcherPort)
            .append('\n')
            .append(more)
            .append("consumers:\n  ")
            .append(topic)
            .append(":\n    source: CONSUMER_SOURCE_DISPATCHER\n    cluster: a\n    groupId: ")
            .append(group)
            .append('\n');
    for (String line : entry) {
      yaml.append("    ").append(line).append('\n');
    }
    return yaml.toString();
  }

  /** Starts a sidecar with no cluster of its own: it consumes through the dispatcher only. */
  private ServerHarness sidecar(int dispatcherPort, String... entry) throws Exception {
    return sidecarWith(sidecarYaml(dispatcherPort, "", entry));
  }

  private ServerHarness sidecarWith(String yaml) throws Exception {
    ServerHarness sidecar = ServerHarness.start(yaml);
    sidecars.add(sidecar);
    return sidecar;
  }

  private App app(ServerHarness sidecar) throws Exception {
    return new App(sidecar, 120_000, topic).awaitSubscribed();
  }

  private OdradekClient client(ServerHarness sidecar) {
    OdradekClient client = OdradekClient.connect(sidecar.socket());
    closeables.add(client);
    return client;
  }

  /** Writes one record and returns its offset. */
  private long produce(String name, int partition, String key, String value) throws Exception {
    return producer
        .send(new ProducerRecord<>(name, partition, key, value))
        .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS)
        .offset();
  }

  /** Writes {@code count} records with keys of their own, spread over the partitions. */
  private Set<String> produce(String prefix, int count, int partitions) throws Exception {
    List<Future<RecordMetadata>> sends = new ArrayList<>();
    Set<String> values = new TreeSet<>();
    for (int i = 0; i < count; i++) {
      String value = prefix + "-" + i;
      values.add(value);
      sends.add(producer.send(new ProducerRecord<>(topic, i % partitions, "key-" + value, value)));
    }
    for (Future<RecordMetadata> send : sends) {
      send.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
    }
    return values;
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

  /** Waits until the group has committed the end of every partition of a topic. */
  private void awaitCommitted(String name, int partitions) throws Exception {
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
    await(
        "everything of " + name + " is committed (" + ends + ")",
        () -> {
          Map<String, Long> now = committed();
          return ends.entrySet().stream()
              .allMatch(end -> end.getValue().equals(now.get(end.getKey())));
        });
  }

  private static String text(byte[] bytes) {
    return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
  }

  /**
   * Produces rounds of records until the app of every sidecar has handled one: a sidecar's streams
   * to the dispatcher are opened when its app subscribes, and nothing says when that is done.
   *
   * @param handled the values each app has handled, one set per app
   * @return everything produced
   */
  private Set<String> warmUp(List<Set<String>> handled, int partitions) throws Exception {
    Set<String> produced = new TreeSet<>();
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    for (int round = 0; handled.stream().anyMatch(Set::isEmpty); round++) {
      assertThat(System.nanoTime())
          .as("every app receives records within %s: %s", TIMEOUT, handled)
          .isLessThan(deadline);
      produced.addAll(produce("warm-" + round, 16, partitions));
      await(
          "the records of round " + round + " are handled",
          () -> {
            Set<String> all = new TreeSet<>();
            handled.forEach(all::addAll);
            return all.containsAll(produced);
          });
    }
    return produced;
  }

  // -------------------------------------------------------------------------------------------
  // The path, with the Java client library
  // -------------------------------------------------------------------------------------------

  @Test
  void moreSidecarsThanPartitionsAllReceiveAndEverythingIsCommitted() throws Exception {
    createTopic(topic, 2);
    int port = startDispatcher().port();
    List<Set<String>> handled = new ArrayList<>();
    List<String> all = new CopyOnWriteArrayList<>();
    List<Throwable> failures = new CopyOnWriteArrayList<>();
    for (int i = 0; i < 4; i++) {
      // No cluster in its configuration: the sidecar has no Kafka client at all.
      OdradekClient client = client(sidecar(port));
      Set<String> mine = ConcurrentHashMap.newKeySet();
      handled.add(mine);
      closeables.add(
          client.consumer(
              ConsumerOptions.topics(topic).withMaxUnackedRecords(4),
              record -> {
                // Long enough for work to be in flight on several pods at once.
                Thread.sleep(2);
                mine.add(text(record.value()));
                all.add(text(record.value()));
              },
              new ConsumerListener() {
                @Override
                public void failed(Throwable cause) {
                  failures.add(cause);
                }
              }));
    }
    Set<String> produced = warmUp(handled, 2);
    produced.addAll(produce("v", 200, 2));
    await("everything is handled", () -> new TreeSet<>(all).equals(produced));
    awaitCommitted(topic, 2);

    assertThat(handled).as("four pods on two partitions").allMatch(mine -> !mine.isEmpty());
    assertThat(all).as("nothing is handled twice").hasSize(produced.size());
    assertThat(failures).isEmpty();
  }

  @Test
  void recordsOfOneKeyAreHandledInOrderAcrossSidecars() throws Exception {
    createTopic(topic, 3);
    int port = startDispatcher().port();
    List<Set<String>> handled = new ArrayList<>();
    // In the order the handlers ran, whichever pod they ran on.
    List<String> log = new CopyOnWriteArrayList<>();
    Set<String> inFlight = ConcurrentHashMap.newKeySet();
    List<String> overlaps = new CopyOnWriteArrayList<>();
    for (int i = 0; i < 3; i++) {
      OdradekClient client = client(sidecar(port, "ordering: ORDERING_KEY"));
      Set<String> mine = ConcurrentHashMap.newKeySet();
      handled.add(mine);
      closeables.add(
          client.consumer(
              ConsumerOptions.topics(topic).withMaxUnackedRecords(2),
              record -> {
                String key = text(record.key());
                if (!inFlight.add(key)) {
                  overlaps.add(key);
                }
                Thread.sleep(1);
                log.add(text(record.value()));
                mine.add(text(record.value()));
                inFlight.remove(key);
              }));
    }
    warmUp(handled, 3);
    handled.forEach(Set::clear);

    // Eight keys, thirty records each, interleaved; a key stays in one partition.
    int keys = 8;
    int perKey = 30;
    List<Future<RecordMetadata>> sends = new ArrayList<>();
    for (int sequence = 0; sequence < perKey; sequence++) {
      for (int key = 0; key < keys; key++) {
        sends.add(
            producer.send(
                new ProducerRecord<>(topic, key % 3, "k" + key, "k" + key + ":" + sequence)));
      }
    }
    for (Future<RecordMetadata> send : sends) {
      send.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
    }
    await(
        "every record is handled",
        () -> log.stream().filter(value -> value.startsWith("k")).count() >= keys * perKey);
    awaitCommitted(topic, 3);

    Map<String, List<Integer>> byKey = new TreeMap<>();
    for (String value : log) {
      if (value.startsWith("k")) {
        String[] parts = value.split(":");
        byKey.computeIfAbsent(parts[0], key -> new ArrayList<>()).add(Integer.parseInt(parts[1]));
      }
    }
    List<Integer> inOrder = new ArrayList<>();
    for (int sequence = 0; sequence < perKey; sequence++) {
      inOrder.add(sequence);
    }
    assertThat(byKey).hasSize(keys);
    byKey.forEach(
        (key, sequences) ->
            assertThat(sequences).as("key %s, in order and once", key).isEqualTo(inOrder));
    assertThat(overlaps).as("two records of one key in flight at once").isEmpty();
    // Eight keys in flight and two records at most per pod: the work was spread.
    assertThat(handled.stream().filter(mine -> !mine.isEmpty()).count())
        .as("pods that took part")
        .isGreaterThan(1);
  }

  @Test
  void anAppProducesThroughItsSidecarAndConsumesThroughTheDispatcher() throws Exception {
    createTopic(topic, 2);
    int port = startDispatcher().port();
    ServerHarness sidecar =
        sidecarWith(
            sidecarYaml(
                port,
                "clusters:\n  a:\n    properties:\n      bootstrap.servers: "
                    + ServerHarness.KAFKA_A
                    + "\nproducers:\n  "
                    + topic
                    + ":\n    clusters: [{cluster: a}]\n"));
    OdradekClient client = client(sidecar);
    List<String> seen = new CopyOnWriteArrayList<>();
    Map<String, List<Integer>> byKey = new ConcurrentHashMap<>();
    try (Producer producer = client.producer();
        io.github.bpalermo.odradek.client.Consumer consumer =
            client.consumer(
                ConsumerOptions.topics(topic),
                record -> {
                  String value = text(record.value());
                  seen.add(value);
                  byKey
                      .computeIfAbsent(text(record.key()), key -> new CopyOnWriteArrayList<>())
                      .add(Integer.parseInt(value.substring(1)));
                })) {
      List<CompletableFuture<SendResult>> sends = new ArrayList<>();
      Set<String> produced = new TreeSet<>();
      for (int i = 0; i < 200; i++) {
        produced.add("v" + i);
        sends.add(producer.send(OutgoingRecord.to(topic).key("key-" + i % 7).value("v" + i)));
      }
      for (CompletableFuture<SendResult> send : sends) {
        send.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
      }
      await("everything produced is consumed", () -> new TreeSet<>(seen).equals(produced));
      assertThat(seen).as("once each").hasSize(200);
      byKey.values().forEach(values -> assertThat(values).isSorted());
    }
    awaitCommitted(topic, 2);
  }

  // -------------------------------------------------------------------------------------------
  // Pods and apps that fail
  // -------------------------------------------------------------------------------------------

  @Test
  void theRecordsOfASidecarKilledMidBatchReachAnotherApp() throws Exception {
    createTopic(topic, 2);
    int port = startDispatcher().port();
    ServerHarness doomed = sidecar(port);
    App first = app(doomed);
    App other = app(sidecar(port));

    // Only the first app has asked for anything: it gets five records and sits on them.
    first.credits(5);
    Set<String> produced = produce("v", 20, 2);
    await("the first app holds five records", () -> first.received.size() == 5);
    Set<String> held = first.values();

    other.working().credits(20);
    await("the other app has the rest", () -> other.received.size() == 15);
    assertThat(other.values()).doesNotContainAnyElementsOf(held);

    doomed.kill();
    await("the other app gets what the first held", () -> other.values().containsAll(held));
    assertThat(other.values()).as("nothing is lost").isEqualTo(produced);
    assertThat(other.received).as("and nothing comes twice").hasSize(20);
    awaitCommitted(topic, 2);
    assertThat(other.closed).isNotDone();
  }

  @Test
  void aRecordAcknowledgedLateIsGivenToAnotherAppAndNothingIsLost() throws Exception {
    createTopic(topic, 1);
    int port = startDispatcher().port();
    // A deadline of three seconds, which is the dispatcher's to enforce.
    App slow = new App(sidecar(port), 3000, topic).awaitSubscribed();
    App other = new App(sidecar(port), 3000, topic).awaitSubscribed();

    slow.credits(1);
    long offset = produce(topic, 0, "k", "late");
    await("the slow app has the record", () -> slow.received.size() == 1);
    other.working().credits(5);
    // The deadline passes: the dispatcher gives the record to the other app, which handles it.
    await("the other app is given the record", () -> other.received.size() == 1);
    assertThat(other.received.get(0).offset()).isEqualTo(offset);
    awaitCommitted(topic, 1);
    // The slow app is neither evicted nor cut off.
    assertThat(slow.closed).isNotDone();

    // It acknowledges at last, which changes nothing, and is served again: once the other app is
    // gone, the next record can only come to it.
    slow.ack(slow.received.get(0));
    other.close();
    assertThat(other.status()).isEqualTo(Status.OK);
    slow.working().credits(1);
    produce(topic, 0, "k", "next");
    await("the slow app is served again", () -> slow.values().contains("next"));
    awaitCommitted(topic, 1);
    assertThat(slow.closed).isNotDone();
  }

  // -------------------------------------------------------------------------------------------
  // A dispatcher that comes and goes
  // -------------------------------------------------------------------------------------------

  @Test
  void whenTheDispatcherRestartsTheAppsStreamsStayOpenAndConsumptionResumes() throws Exception {
    createTopic(topic, 2);
    TcpRelay service = new TcpRelay();
    closeables.add(service);
    service.target(startDispatcher().port());
    App one = app(sidecar(service.port())).working();
    App two = app(sidecar(service.port())).working();
    one.credits(10);
    two.credits(10);

    Set<String> produced = produce("before", 30, 2);
    await(
        "the first records are handled",
        () -> {
          Set<String> all = one.values();
          all.addAll(two.values());
          return all.containsAll(produced);
        });
    awaitCommitted(topic, 2);

    // A new dispatcher behind the same address: another process, another port, no memory.
    dispatcher.stopDispatcher();
    dispatcher.startDispatcher();
    service.target(dispatcher.port());

    produced.addAll(produce("after", 30, 2));
    await(
        "consumption resumes",
        () -> {
          Set<String> all = one.values();
          all.addAll(two.values());
          return all.containsAll(produced);
        });
    awaitCommitted(topic, 2);
    assertThat(one.closed).as("the first app's stream").isNotDone();
    assertThat(two.closed).as("the second app's stream").isNotDone();
  }

  @Test
  void aSidecarStartedBeforeTheDispatcherServesItsAppAndReceivesWhenItAppears() throws Exception {
    createTopic(topic, 1);
    // Nothing answers at the dispatcher's address yet.
    TcpRelay service = new TcpRelay();
    closeables.add(service);
    ServerHarness sidecar = sidecar(service.port());
    if (DispatcherHarness.packaged()) {
      // Ready without a dispatcher: an app that only produces must be able to start.
      assertThat(
              ClientCalls.blockingUnaryCall(
                  sidecar.channel,
                  HealthService.CHECK,
                  CallOptions.DEFAULT.withDeadlineAfter(30, TimeUnit.SECONDS),
                  ""))
          .isEqualTo(ServingStatus.SERVING);
    }
    App app = app(sidecar).working();
    app.credits(10);
    Set<String> produced = produce("v", 5, 1);
    // Several attempts at the dispatcher fail meanwhile; the app's stream just stays quiet.
    Thread.sleep(1500);
    assertThat(app.received).isEmpty();
    assertThat(app.closed).isNotDone();

    service.target(startDispatcher().port());
    await("the app receives once the dispatcher is there", () -> app.values().equals(produced));
    awaitCommitted(topic, 1);
    assertThat(app.closed).isNotDone();
  }

  @Test
  void aDeclarationTheDispatcherRefusesForGoodEndsTheAppsStreamSayingWhy() throws Exception {
    createTopic(topic, 1);
    int port = startDispatcher().port();
    App first = app(sidecar(port, "ordering: ORDERING_KEY"));
    first.credits(1);
    produce(topic, 0, "k", "v");
    await("the first sidecar holds the topic", () -> first.received.size() == 1);

    // The same group and topic, declared with another ordering by a pod configured otherwise.
    App conflicting = new App(sidecar(port, "ordering: ORDERING_PARTITION"), 120_000, topic);
    Status status = conflicting.status();
    assertThat(status.getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
    assertThat(status.getDescription())
        .contains("the dispatcher at localhost:" + port + " refused topic " + topic)
        .contains("already consumes topic " + topic)
        .contains("In force: ordering ORDERING_KEY")
        .contains("Declared: ordering ORDERING_PARTITION");

    // The app whose declaration is in force is not disturbed.
    first.ack(first.received.get(0));
    awaitCommitted(topic, 1);
    assertThat(first.closed).isNotDone();
  }

  @Test
  void aStreamThatMixesSourcesIsRefused() throws Exception {
    // One topic received from the dispatcher, one read from Kafka by the sidecar itself, in the
    // same group on the same cluster. No dispatcher is needed to say no.
    ServerHarness sidecar =
        sidecarWith(
            sidecarYaml(
                    1,
                    "clusters:\n  a:\n    properties:\n      bootstrap.servers: "
                        + ServerHarness.KAFKA_A
                        + "\n")
                + "  "
                + second
                + ":\n    cluster: a\n    groupId: "
                + group
                + "\n");
    App mixed = new App(sidecar, 120_000, topic, second);
    Status status = mixed.status();
    assertThat(status.getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
    assertThat(status.getDescription())
        .contains("all be read from Kafka or all be received from the dispatcher")
        .contains(topic)
        .contains(second);
  }

  // -------------------------------------------------------------------------------------------
  // Records the codec cannot decode
  // -------------------------------------------------------------------------------------------

  /**
   * A codec the packaged server has, native image included, that fails on some records: Kafka's
   * string deserializer returns a String where the server wants bytes or a protobuf message, so
   * every value fails, while a tombstone, which never enters a codec, passes.
   */
  private static final String FAILING_CODEC =
      """
      codecs:
        text:
          value:
            serializer: org.apache.kafka.common.serialization.StringSerializer
            deserializer: org.apache.kafka.common.serialization.StringDeserializer
      """;

  @Test
  void aRecordThatCannotBeDecodedIsDeliveredUndecodedWhenTheEntrySaysSo() throws Exception {
    createTopic(topic, 1);
    int port = startDispatcher().port();
    App app =
        app(sidecarWith(
                sidecarYaml(
                    port,
                    FAILING_CODEC,
                    "codec: text",
                    "onDecodeError: ON_DECODE_ERROR_DELIVER_UNDECODED")))
            .working();
    app.credits(10);
    produce(topic, 0, "good-1", null);
    produce(topic, 0, "bad", "cannot be decoded");
    produce(topic, 0, "good-2", null);

    await("all three records arrive", () -> app.received.size() == 3);
    assertThat(app.received).extracting(Got::key).containsExactly("good-1", "bad", "good-2");
    assertThat(app.received.get(0).decodeError()).isNull();
    assertThat(app.received.get(0).value()).isNull();
    Got undecoded = app.received.get(1);
    assertThat(undecoded.value()).as("as stored").isEqualTo("cannot be decoded");
    assertThat(undecoded.decodeError()).contains("codec text value");
    assertThat(app.received.get(2).decodeError()).isNull();
    // The app acknowledged it like the others.
    awaitCommitted(topic, 1);
  }

  @Test
  void aRecordThatCannotBeDecodedIsRejectedAndTheStreamStays() throws Exception {
    createTopic(topic, 2);
    int port = startDispatcher().port();
    // The default, ON_DECODE_ERROR_FAIL: a consumer that reads Kafka would end the stream. Two
    // attempts, then the record blocks its partition. One record at a time per partition, so that
    // what is behind the record is certain not to overtake it.
    App app =
        app(sidecarWith(
                sidecarYaml(
                    port,
                    FAILING_CODEC,
                    "codec: text",
                    "ordering: ORDERING_PARTITION",
                    "maxAttempts: 2",
                    "redeliveryBackoff: 0.2s")))
            .working();
    app.credits(10);
    produce(topic, 0, "good-1", null);
    long bad = produce(topic, 0, "bad", "cannot be decoded");
    produce(topic, 0, "behind", null);
    produce(topic, 1, "elsewhere-1", null);
    long last = produce(topic, 1, "elsewhere-2", null);

    await(
        "the partition is committed up to the record that cannot be decoded, the other fully",
        () -> {
          Map<String, Long> now = committed();
          return Long.valueOf(bad).equals(now.get(topic + "/0"))
              && Long.valueOf(last + 1).equals(now.get(topic + "/1"));
        });
    // Rejected twice and then held back by the dispatcher, with what is behind it in its
    // partition. The app never saw it, and its stream is open.
    Thread.sleep(1000);
    assertThat(app.received)
        .extracting(Got::key)
        .containsExactlyInAnyOrder("good-1", "elsewhere-1", "elsewhere-2");
    assertThat(app.received).allMatch(record -> record.decodeError() == null);
    assertThat(app.closed).isNotDone();
    assertThat(committed().get(topic + "/0")).isEqualTo(bad);

    // And it still works: a record for the other partition arrives.
    produce(topic, 1, "elsewhere-3", null);
    await("the stream still delivers", () -> app.received.size() == 4);
  }

  @Test
  void aRecordNoSidecarCanDecodeEndsInTheDeadLetterTopicAndItsPartitionGoesOn() throws Exception {
    String deadLetters = "odradek-e2e-receive-dlq-" + id;
    createTopic(topic, 1);
    createTopic(deadLetters, 1);
    int port = startDispatcher().port();
    // ON_DECODE_ERROR_FAIL, two attempts, and then the dead-letter topic: all of it written in
    // the application's own configuration, and none in the dispatcher's.
    App app =
        app(sidecarWith(
                sidecarYaml(
                    port,
                    FAILING_CODEC,
                    "codec: text",
                    "ordering: ORDERING_PARTITION",
                    "maxAttempts: 2",
                    "onExhausted: ON_EXHAUSTED_DEAD_LETTER",
                    "deadLetterTopic: " + deadLetters,
                    "redeliveryBackoff: 0.2s")))
            .working();
    app.credits(10);
    produce(topic, 0, "good-1", null);
    long bad = produce(topic, 0, "bad", "cannot be decoded");
    produce(topic, 0, "behind", null);

    // The partition goes on past the record, and the commit with it.
    awaitCommitted(topic, 1);
    assertThat(app.received).extracting(Got::key).containsExactly("good-1", "behind");
    assertThat(app.received).allMatch(record -> record.decodeError() == null);
    assertThat(app.closed).isNotDone();

    List<ConsumerRecord<String, String>> letters =
        ServerHarness.read(ServerHarness.KAFKA_A, deadLetters, 1, 1);
    assertThat(letters).as("dead letters").hasSize(1);
    ConsumerRecord<String, String> letter = letters.get(0);
    assertThat(letter.key()).isEqualTo("bad");
    assertThat(letter.value())
        .as("as stored: the dispatcher decodes nothing")
        .isEqualTo("cannot be decoded");
    assertThat(ServerHarness.header(letter, "odradek-source-topic")).isEqualTo(topic);
    assertThat(ServerHarness.header(letter, "odradek-source-partition")).isEqualTo("0");
    assertThat(ServerHarness.header(letter, "odradek-source-offset")).isEqualTo("" + bad);
    assertThat(ServerHarness.header(letter, "odradek-source-group")).isEqualTo(group);
    assertThat(ServerHarness.header(letter, "odradek-attempts")).isEqualTo("2");
    assertThat(ServerHarness.header(letter, "odradek-failure")).isEqualTo("rejected");
    // What the sidecar said of it: why its codec could not decode it.
    assertThat(ServerHarness.header(letter, "odradek-failure-detail")).contains("codec text value");
  }

  @Test
  void twoTopicsOnOneStreamAreBothReceived() throws Exception {
    createTopic(topic, 1);
    createTopic(second, 1);
    int port = startDispatcher().port();
    ServerHarness sidecar =
        sidecarWith(
            sidecarYaml(port, "")
                + "  "
                + second
                + ":\n    source: CONSUMER_SOURCE_DISPATCHER\n    cluster: a\n    groupId: "
                + group
                + "\n");
    // An app that takes one record at a time, from two topics of which one is busy.
    App app = new App(sidecar, 120_000, topic, second).awaitSubscribed().working();
    app.credits(1);
    for (int i = 1; i <= 40; i++) {
      produce(topic, 0, "k" + i, "busy-" + i);
    }
    produce(second, 0, "k", "quiet");
    await("both topics are received", () -> app.received.size() == 41);
    assertThat(app.values()).contains("quiet", "busy-1", "busy-40");
    awaitCommitted(topic, 1);
    awaitCommitted(second, 1);
  }
}
