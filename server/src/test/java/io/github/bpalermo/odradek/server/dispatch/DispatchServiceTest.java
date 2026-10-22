package io.github.bpalermo.odradek.server.dispatch;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.util.Durations;
import io.github.bpalermo.odradek.config.v1.Cluster;
import io.github.bpalermo.odradek.config.v1.Consume;
import io.github.bpalermo.odradek.config.v1.Dispatch;
import io.github.bpalermo.odradek.config.v1.ServerConfig;
import io.github.bpalermo.odradek.delivery.v1.Accepted;
import io.github.bpalermo.odradek.delivery.v1.Ack;
import io.github.bpalermo.odradek.delivery.v1.Credits;
import io.github.bpalermo.odradek.delivery.v1.Declare;
import io.github.bpalermo.odradek.delivery.v1.DeliverRequest;
import io.github.bpalermo.odradek.delivery.v1.DeliverResponse;
import io.github.bpalermo.odradek.delivery.v1.DeliveryServiceGrpc;
import io.github.bpalermo.odradek.delivery.v1.OffsetReset;
import io.github.bpalermo.odradek.delivery.v1.OnExhausted;
import io.github.bpalermo.odradek.delivery.v1.Ordering;
import io.github.bpalermo.odradek.delivery.v1.Record;
import io.github.bpalermo.odradek.delivery.v1.RecordBatch;
import io.github.bpalermo.odradek.delivery.v1.Reject;
import io.github.bpalermo.odradek.delivery.v1.Replica;
import io.github.bpalermo.odradek.delivery.v1.ReplicaList;
import io.github.bpalermo.odradek.delivery.v1.Settings;
import io.github.bpalermo.odradek.server.Version;
import io.github.bpalermo.odradek.server.telemetry.ServerMetrics;
import io.github.bpalermo.odradek.server.tls.TlsSources;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The Delivery service over a real gRPC stream, against Kafka's mock consumer: the handshake and
 * its refusals, and what the dispatcher does with the consumer. The broker's side of it (groups,
 * rebalances between replicas, committed offsets) is the end-to-end suite's.
 */
class DispatchServiceTest {
  private static final TopicPartition P0 = new TopicPartition("orders", 0);
  private static final TopicPartition P1 = new TopicPartition("orders", 1);
  private static final TopicPartition R0 = new TopicPartition("refunds", 0);

  /**
   * Remembers commits on the side: the mock cannot be asked from another thread or once closed.
   *
   * <p>Where the group's thread relies on it, it behaves as a real consumer does, which the mock
   * alone does not: a poll waits, up to its timeout, until it has something to return (a scheduled
   * task, an added record, records left from the last poll) or is woken; and a wakeup makes the
   * next blocking call throw {@link WakeupException}, a poll or a commit or a lookup of offsets,
   * until one has. A wakeup asked for while a scheduled task runs is checked by nothing in that
   * poll, and so is pending when it returns: what a wakeup that comes just as a poll returns does
   * to a real consumer.
   */
  private static final class RecordingConsumer extends MockConsumer<byte[], byte[]> {
    final Map<TopicPartition, Long> commits = new ConcurrentHashMap<>();
    final List<Set<String>> subscriptions = new CopyOnWriteArrayList<>();

    /** The timeouts of the last polls, and whether each had something to return: for failures. */
    final List<String> recentPolls = new CopyOnWriteArrayList<>();

    /** Wakeups asked for. */
    final AtomicInteger wakeups = new AtomicInteger();

    /** Polls that a wakeup ended. */
    final AtomicInteger wokenPolls = new AtomicInteger();

    /** Other calls that met a wakeup and threw. */
    final AtomicInteger wokenCalls = new AtomicInteger();

    /** The timeout of the poll that waits now; null when none does. */
    volatile Duration waiting;

    /** Run by {@link #committed}, before it answers. */
    volatile Runnable onCommitted = () -> {};

    /** Guards {@link #woken} and {@link #poked}; held while a poll waits, never with the mock's. */
    private final Object signal = new Object();

    private boolean woken;

    /** Whether a poll has something to return, and so does not wait. */
    private boolean poked;

    RecordingConsumer() {
      super("none");
    }

    /** Every poll, woken or not. */
    final AtomicInteger polls = new AtomicInteger();

    @Override
    public ConsumerRecords<byte[], byte[]> poll(Duration timeout) {
      polls.incrementAndGet();
      boolean wake;
      synchronized (signal) {
        recentPolls.add(timeout.toMillis() + (poked ? " poked" : "") + (woken ? " woken" : ""));
        if (recentPolls.size() > 20) {
          recentPolls.removeFirst();
        }
        long deadline = System.nanoTime() + timeout.toNanos();
        waiting = timeout;
        try {
          long left = timeout.toNanos();
          while (!woken && !poked && left > 0) {
            TimeUnit.NANOSECONDS.timedWait(signal, left);
            left = deadline - System.nanoTime();
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          waiting = null;
        }
        wake = woken;
        woken = false;
        // A poll that throws runs no scheduled task: what was pending stays so, and the next poll
        // must not wait for it.
        if (!wake) {
          poked = false;
        }
      }
      if (wake) {
        wokenPolls.incrementAndGet();
        throw new WakeupException();
      }
      ConsumerRecords<byte[], byte[]> records = super.poll(timeout);
      if (!records.isEmpty()) {
        // There may be more, which a real consumer would return without waiting.
        poke();
      }
      return records;
    }

    @Override
    public void wakeup() {
      wakeups.incrementAndGet();
      synchronized (signal) {
        woken = true;
        signal.notifyAll();
      }
    }

    private void poke() {
      synchronized (signal) {
        poked = true;
        signal.notifyAll();
      }
    }

    /** What a blocking call does first: throws if a wakeup is pending. */
    private void wakeupPoint() {
      synchronized (signal) {
        if (woken) {
          woken = false;
          wokenCalls.incrementAndGet();
          throw new WakeupException();
        }
      }
    }

    @Override
    public void schedulePollTask(Runnable task) {
      super.schedulePollTask(task);
      poke();
    }

    @Override
    public void addRecord(ConsumerRecord<byte[], byte[]> record) {
      super.addRecord(record);
      poke();
    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {
      super.resume(partitions);
      poke();
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions) {
      wakeupPoint();
      onCommitted.run();
      return super.committed(partitions);
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
      wakeupPoint();
      return super.beginningOffsets(partitions);
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
      wakeupPoint();
      return super.endOffsets(partitions);
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
        Map<TopicPartition, Long> timestampsToSearch) {
      wakeupPoint();
      return super.offsetsForTimes(timestampsToSearch);
    }

    @Override
    public synchronized void subscribe(
        java.util.Collection<String> topics,
        org.apache.kafka.clients.consumer.ConsumerRebalanceListener listener) {
      subscriptions.add(Set.copyOf(topics));
      super.subscribe(topics, listener);
    }

    @Override
    public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
      wakeupPoint();
      offsets.forEach((partition, offset) -> commits.put(partition, offset.offset()));
      super.commitSync(offsets);
    }

    @Override
    public synchronized void commitAsync(
        Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
      offsets.forEach((partition, offset) -> commits.put(partition, offset.offset()));
      super.commitAsync(offsets, callback);
    }
  }

  /** The consumers the service asked for, in order, with the properties it gave. */
  private final List<RecordingConsumer> consumers = new CopyOnWriteArrayList<>();

  private final List<Map<String, Object>> consumerProperties = new CopyOnWriteArrayList<>();

  /** The dead-letter producers the service asked for, with the properties it gave. */
  private final List<MockProducer<byte[], byte[]>> producers = new CopyOnWriteArrayList<>();

  private final List<Map<String, Object>> producerProperties = new CopyOnWriteArrayList<>();

  /** Metrics are read back from the SDK, so that what a dashboard would show is what is checked. */
  private final InMemoryMetricReader reader = InMemoryMetricReader.create();

  private final SdkMeterProvider provider =
      SdkMeterProvider.builder().registerMetricReader(reader).build();
  private final ServerMetrics metrics = new ServerMetrics(provider.get("odradek"));

  /**
   * The sum of the points of a metric whose attributes include these, given as name and value in
   * turn; 0 when it has none.
   */
  private long metric(String name, String... attributes) {
    long total = 0;
    for (MetricData data : reader.collectAllMetrics()) {
      if (!data.getName().equals(name)) {
        continue;
      }
      for (LongPointData point : data.getLongSumData().getPoints()) {
        boolean matches = true;
        for (int i = 0; i < attributes.length; i += 2) {
          matches &=
              attributes[i + 1].equals(
                  point.getAttributes().get(AttributeKey.stringKey(attributes[i])));
        }
        if (matches) {
          total += point.getValue();
        }
      }
    }
    return total;
  }

  /** The points of a histogram whose attributes include these, given as name and value in turn. */
  private List<HistogramPointData> histogram(String name, String... attributes) {
    List<HistogramPointData> points = new ArrayList<>();
    for (MetricData data : reader.collectAllMetrics()) {
      if (!data.getName().equals(name)) {
        continue;
      }
      for (HistogramPointData point : data.getHistogramData().getPoints()) {
        boolean matches = true;
        for (int i = 0; i < attributes.length; i += 2) {
          matches &=
              attributes[i + 1].equals(
                  point.getAttributes().get(AttributeKey.stringKey(attributes[i])));
        }
        if (matches) {
          points.add(point);
        }
      }
    }
    return points;
  }

  /** How many values a histogram recorded under these attributes. */
  private long recorded(String name, String... attributes) {
    return histogram(name, attributes).stream().mapToLong(HistogramPointData::getCount).sum();
  }

  private Server server;
  private ManagedChannel channel;
  private DispatchService service;

  /** A sidecar's end of one Deliver stream. */
  private static final class Sidecar {
    final BlockingQueue<DeliverResponse> responses = new LinkedBlockingQueue<>();
    final CompletableFuture<Status> closed = new CompletableFuture<>();
    StreamObserver<DeliverRequest> requests;

    void send(DeliverRequest.Builder request) {
      requests.onNext(request.build());
    }

    void declare(Declare.Builder declare) {
      send(DeliverRequest.newBuilder().setDeclare(declare));
    }

    void credits(int records) {
      send(DeliverRequest.newBuilder().setCredits(Credits.newBuilder().setRecords(records)));
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

    void reject(int partition, long offset) {
      send(
          DeliverRequest.newBuilder()
              .setReject(
                  Reject.newBuilder().setPartition(partition).setOffset(offset).setReason("test")));
    }

    /** The next record, as "partition/offset/attempt", and how long after {@code since}. */
    long millisUntil(String record, long since) throws InterruptedException {
      assertThat(records(1)).containsExactly(record);
      return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - since);
    }

    DeliverResponse next() throws InterruptedException {
      DeliverResponse response = responses.poll(10, TimeUnit.SECONDS);
      assertThat(response).as("a response within 10s").isNotNull();
      return response;
    }

    Accepted accepted() throws InterruptedException {
      DeliverResponse response = next();
      assertThat(response.hasAccepted()).as("%s", response).isTrue();
      return response.getAccepted();
    }

    RecordBatch batch() throws InterruptedException {
      DeliverResponse response = next();
      assertThat(response.hasRecords()).as("%s", response).isTrue();
      return response.getRecords();
    }

    /** Records until {@code count} have arrived, as "partition/offset/attempt". */
    List<String> records(int count) throws InterruptedException {
      List<String> records = new ArrayList<>();
      while (records.size() < count) {
        RecordBatch batch = batch();
        for (Record record : batch.getRecordsList()) {
          records.add(batch.getPartition() + "/" + record.getOffset() + "/" + record.getAttempt());
        }
      }
      return records;
    }

    void nothing() throws InterruptedException {
      assertThat(responses.poll(200, TimeUnit.MILLISECONDS)).isNull();
    }

    Status status() throws Exception {
      return closed.get(10, TimeUnit.SECONDS);
    }
  }

  private static ServerConfig.Builder config() {
    return ServerConfig.newBuilder()
        .putClusters("main", Cluster.getDefaultInstance())
        .setDispatch(Dispatch.newBuilder().setIdleGrace(Durations.fromSeconds(60)))
        .setConsume(
            Consume.newBuilder()
                .setCommitInterval(Durations.fromMillis(20))
                .setRevokeDeadline(Durations.fromMillis(300)));
  }

  private void start(ServerConfig.Builder config) throws Exception {
    start(config, true);
  }

  /**
   * @param joined whether the replica has been told its replicas, as it is once it has joined the
   *     coordination group
   */
  private void start(ServerConfig.Builder config, boolean joined) throws Exception {
    service =
        new DispatchService(
            config.build(),
            TlsSources.none(),
            metrics,
            (cluster, properties) -> {
              consumerProperties.add(properties);
              RecordingConsumer consumer = new RecordingConsumer();
              consumers.add(consumer);
              return consumer;
            },
            new DeadLetters(
                config.build(),
                TlsSources.none(),
                metrics,
                (cluster, properties) -> {
                  producerProperties.add(properties);
                  // Completed by the test: a write is acknowledged when the test says so.
                  MockProducer<byte[], byte[]> producer =
                      new MockProducer<>(
                          false,
                          new RoundRobinPartitioner(),
                          new ByteArraySerializer(),
                          new ByteArraySerializer());
                  producers.add(producer);
                  return producer;
                }),
            System::nanoTime);
    service.advertise("10.1.2.3:9465");
    if (joined) {
      // What the coordination group would have settled on with one replica.
      service.replicas(ReplicaAssignor.self("10.1.2.3:9465", 1));
    }
    String name = InProcessServerBuilder.generateName();
    server = InProcessServerBuilder.forName(name).addService(service).build().start();
    channel = InProcessChannelBuilder.forName(name).build();
  }

  @AfterEach
  void stop() throws InterruptedException {
    if (channel != null) {
      channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
      service.shutdown(Duration.ofSeconds(5));
      server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
    provider.close();
  }

  private Sidecar stream() {
    Sidecar sidecar = new Sidecar();
    sidecar.requests =
        DeliveryServiceGrpc.newStub(channel)
            .deliver(
                new StreamObserver<DeliverResponse>() {
                  @Override
                  public void onNext(DeliverResponse response) {
                    sidecar.responses.add(response);
                  }

                  @Override
                  public void onError(Throwable t) {
                    sidecar.closed.complete(Status.fromThrowable(t));
                  }

                  @Override
                  public void onCompleted() {
                    sidecar.closed.complete(Status.OK);
                  }
                });
    return sidecar;
  }

  /** Declares a topic with no wait before a redelivery, unless the test asks for one. */
  private static Declare.Builder declare(String topic) {
    return Declare.newBuilder()
        .setCluster("main")
        .setGroupId("billing")
        .setTopic(topic)
        .setSettings(Settings.newBuilder().setMaxProcessingMs(60_000).setRedeliveryBackoffMs(0));
  }

  private static Declare.Builder declare(String topic, Consumer<Settings.Builder> settings) {
    Declare.Builder declare = declare(topic);
    settings.accept(declare.getSettingsBuilder());
    return declare;
  }

  /** A sidecar whose declaration was accepted. */
  private Sidecar declared(Declare.Builder declare) throws InterruptedException {
    Sidecar sidecar = stream();
    sidecar.declare(declare);
    sidecar.accepted();
    return sidecar;
  }

  private RecordingConsumer kafka() throws InterruptedException {
    eventually("a consumer was created", () -> !consumers.isEmpty());
    return consumers.get(consumers.size() - 1);
  }

  /** Gives the consumer these partitions, each empty and starting at offset 0. */
  private void assign(TopicPartition... partitions) throws InterruptedException {
    RecordingConsumer kafka = kafka();
    kafka.schedulePollTask(
        () -> {
          for (TopicPartition partition : partitions) {
            kafka.updateBeginningOffsets(Map.of(partition, 0L));
            kafka.updateEndOffsets(Map.of(partition, 0L));
          }
          kafka.rebalance(List.of(partitions));
        });
  }

  private void add(TopicPartition partition, long offset, String key) throws InterruptedException {
    RecordingConsumer kafka = kafka();
    kafka.schedulePollTask(
        () ->
            kafka.addRecord(
                new ConsumerRecord<>(
                    partition.topic(),
                    partition.partition(),
                    offset,
                    key == null ? null : key.getBytes(StandardCharsets.UTF_8),
                    ("v" + offset).getBytes(StandardCharsets.UTF_8))));
  }

  private static void eventually(String what, BooleanSupplier condition)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
      Thread.sleep(5);
    }
    assertThat(condition.getAsBoolean()).as(what).isTrue();
  }

  private long committed(TopicPartition partition) throws InterruptedException {
    return kafka().commits.getOrDefault(partition, -1L);
  }

  // -------------------------------------------------------------------------------------------
  // The handshake
  // -------------------------------------------------------------------------------------------

  @Test
  void theFirstMessageMustBeDeclare() throws Exception {
    start(config());
    Sidecar sidecar = stream();
    sidecar.credits(1);
    Status status = sidecar.status();
    assertThat(status.getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    assertThat(status.getDescription()).isEqualTo("the first message must be Declare");
    assertThat(consumers).isEmpty();
  }

  @Test
  void aMalformedDeclarationIsRefused() throws Exception {
    start(config());
    Sidecar sidecar = stream();
    sidecar.declare(declare("orders").clearSettings());
    assertThat(sidecar.status().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    assertThat(consumers).isEmpty();
  }

  @Test
  void aClusterTheDispatcherLacksIsRefused() throws Exception {
    start(config());
    Sidecar sidecar = stream();
    sidecar.declare(declare("orders").setCluster("elsewhere"));
    assertThat(sidecar.status().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
    assertThat(consumers).isEmpty();
    // Counted by its status alone: what was declared is whatever a caller chose to say.
    assertThat(metric("odradek.dispatch.declarations.refused", "reason", "PERMISSION_DENIED"))
        .isEqualTo(1);
    assertThat(metric("odradek.dispatch.declarations.refused")).isEqualTo(1);
  }

  @Test
  void aDeadLetterTopicIsPartOfTheSettingsInForce() throws Exception {
    start(config());
    Consumer<Settings.Builder> deadLettering =
        s ->
            s.setMaxAttempts(3)
                .setOnExhausted(OnExhausted.ON_EXHAUSTED_DEAD_LETTER)
                .setDeadLetterTopic("orders.dlq");
    Sidecar sidecar = stream();
    sidecar.declare(declare("orders", deadLettering));
    Settings inForce = sidecar.accepted().getSettings();
    assertThat(inForce.getOnExhausted()).isEqualTo(OnExhausted.ON_EXHAUSTED_DEAD_LETTER);
    assertThat(inForce.getDeadLetterTopic()).isEqualTo("orders.dlq");
    // No producer until there is something to write.
    assertThat(producers).isEmpty();

    Sidecar elsewhere = stream();
    elsewhere.declare(
        declare("orders", deadLettering.andThen(s -> s.setDeadLetterTopic("orders.other"))));
    Status status = elsewhere.status();
    assertThat(status.getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
    assertThat(status.getDescription())
        .contains("dead_letter_topic orders.dlq")
        .contains("dead_letter_topic orders.other");
  }

  @Test
  void acceptedCarriesTheSettingsInForceAndThisReplicaAlone() throws Exception {
    start(config());
    Sidecar sidecar = stream();
    sidecar.declare(declare("orders"));
    Accepted accepted = sidecar.accepted();
    assertThat(accepted.getSettings())
        .isEqualTo(
            Settings.newBuilder()
                .setOrdering(Ordering.ORDERING_KEY)
                .setMaxProcessingMs(60_000)
                .setOffsetReset(OffsetReset.OFFSET_RESET_EARLIEST)
                .setMaxAttempts(0)
                .setOnExhausted(OnExhausted.ON_EXHAUSTED_BLOCK)
                .setRedeliveryBackoffMs(0)
                .setRedeliveryBackoffMaxMs(30_000)
                .build());
    assertThat(accepted.getReplicas().getGeneration()).isEqualTo(1);
    assertThat(accepted.getReplicas().getReplicasList().stream().map(Replica::getAddress).toList())
        .containsExactly("10.1.2.3:9465");
    assertThat(accepted.getAddress()).isEqualTo("10.1.2.3:9465");
    assertThat(accepted.getDispatcher().getVersion()).isEqualTo(Version.VERSION).isNotEmpty();
    eventually("subscribed", () -> consumers.get(0).subscriptions.contains(Set.of("orders")));
  }

  private static ReplicaList list(long generation, String... addresses) {
    ReplicaList.Builder list = ReplicaList.newBuilder().setGeneration(generation);
    for (String address : addresses) {
      list.addReplicas(Replica.newBuilder().setAddress(address));
    }
    return list.build();
  }

  @Test
  void aReplicaThatDoesNotKnowItsReplicasYetRefusesDeclarationsForTheTimeBeing() throws Exception {
    // It has a port and an address, and has not joined the coordination group: answering with
    // itself alone would have the sidecar miss the partitions of every other replica.
    start(config(), false);
    Sidecar early = stream();
    early.declare(declare("orders"));
    Status status = early.status();
    assertThat(status.getCode()).isEqualTo(Status.Code.UNAVAILABLE);
    assertThat(consumers).isEmpty();

    service.replicas(list(4, "10.1.2.3:9465", "10.1.2.4:9465"));
    Sidecar sidecar = stream();
    sidecar.declare(declare("orders"));
    assertThat(sidecar.accepted().getReplicas())
        .isEqualTo(list(4, "10.1.2.3:9465", "10.1.2.4:9465"));
  }

  @Test
  void aNewListOfReplicasIsSentOnEveryOpenStreamAndAnswersTheNextHandshake() throws Exception {
    start(config());
    // Two topics of one group and a second group: every stream there is, whoever owns it.
    Sidecar orders = declared(declare("orders"));
    Sidecar payments = declared(declare("payments"));
    Sidecar other = declared(declare("orders").setGroupId("audit"));

    ReplicaList two = list(2, "10.1.2.3:9465", "10.1.2.4:9465");
    service.replicas(two);
    for (Sidecar sidecar : List.of(orders, payments, other)) {
      DeliverResponse response = sidecar.next();
      assertThat(response.hasReplicas()).as("%s", response).isTrue();
      assertThat(response.getReplicas()).isEqualTo(two);
    }

    // A sidecar that connects now is answered with the same list, by a replica that still gives
    // its own address as the one it was reached at.
    Accepted accepted = declaredAccepted(declare("orders"));
    assertThat(accepted.getReplicas()).isEqualTo(two);
    assertThat(accepted.getAddress()).isEqualTo("10.1.2.3:9465");

    // A replica that left: the list shrinks under a higher generation.
    ReplicaList one = list(3, "10.1.2.3:9465");
    service.replicas(one);
    assertThat(orders.next().getReplicas()).isEqualTo(one);
    assertThat(payments.next().getReplicas()).isEqualTo(one);
    assertThat(other.next().getReplicas()).isEqualTo(one);
  }

  private Accepted declaredAccepted(Declare.Builder declare) throws InterruptedException {
    Sidecar sidecar = stream();
    sidecar.declare(declare);
    return sidecar.accepted();
  }

  @Test
  void theConsumerCarriesTheGuaranteesAndCreatesNoTopic() throws Exception {
    start(config());
    declared(declare("orders"));
    kafka();
    assertThat(consumerProperties.get(0))
        .containsEntry("group.id", "billing")
        .containsEntry("enable.auto.commit", "false")
        .containsEntry("auto.offset.reset", "none")
        .containsEntry("allow.auto.create.topics", "false")
        .containsEntry("client.id", "odradek-dispatch-billing-1")
        .containsEntry(
            "partition.assignment.strategy",
            "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
  }

  @Test
  void aDeclarationWithOtherSettingsIsRefusedWhileASidecarHoldsTheTopic() throws Exception {
    start(config());
    Sidecar first = declared(declare("orders"));
    Sidecar second = stream();
    second.declare(declare("orders", s -> s.setOrdering(Ordering.ORDERING_NONE).setMaxAttempts(3)));
    Status status = second.status();
    assertThat(status.getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
    assertThat(status.getDescription())
        .contains("group billing already consumes topic orders on cluster main")
        .contains(
            "In force: ordering ORDERING_KEY, offset_reset OFFSET_RESET_EARLIEST, no max_attempts")
        .contains("Declared: ordering ORDERING_NONE");

    // Another topic of the same group is nobody's business but its own.
    declared(declare("refunds", s -> s.setOrdering(Ordering.ORDERING_NONE)));

    // Once nobody holds the topic with the old settings, the new ones are accepted.
    first.requests.onCompleted();
    assertThat(first.status().isOk()).isTrue();
    Sidecar third = stream();
    third.declare(declare("orders", s -> s.setOrdering(Ordering.ORDERING_NONE).setMaxAttempts(3)));
    Settings settings = third.accepted().getSettings();
    assertThat(settings.getOrdering()).isEqualTo(Ordering.ORDERING_NONE);
    assertThat(settings.getMaxAttempts()).isEqualTo(3);
    assertThat(consumers).hasSize(1);
  }

  @Test
  void theProcessingDeadlineFollowsTheLatestDeclaration() throws Exception {
    start(config());
    declared(declare("orders"));
    Sidecar newer = stream();
    newer.declare(declare("orders", s -> s.setMaxProcessingMs(90_000)));
    assertThat(newer.accepted().getSettings().getMaxProcessingMs()).isEqualTo(90_000);
    // And an old pod that reconnects sets it back: the latest declaration, not the largest.
    Sidecar older = stream();
    older.declare(declare("orders"));
    assertThat(older.accepted().getSettings().getMaxProcessingMs()).isEqualTo(60_000);
  }

  @Test
  void aNewDeadlineAppliesToWhatIsSentFromThenOn() throws Exception {
    start(config());
    Sidecar slow = declared(declare("orders", s -> s.setOrdering(Ordering.ORDERING_NONE)));
    assign(P0);
    add(P0, 0, "a");
    slow.credits(1);
    assertThat(slow.records(1)).containsExactly("0/0/1");
    // A version with a deadline of 100 ms: what it is sent expires, what was sent before does not.
    Sidecar fast =
        declared(
            declare("orders", s -> s.setOrdering(Ordering.ORDERING_NONE).setMaxProcessingMs(100)));
    add(P0, 1, "b");
    fast.credits(1);
    assertThat(fast.records(1)).containsExactly("0/1/1");
    slow.credits(1);
    assertThat(slow.records(1)).containsExactly("0/1/2");
    slow.nothing();
  }

  @Test
  void aSecondDeclareBreaksTheProtocol() throws Exception {
    start(config());
    Sidecar sidecar = declared(declare("orders"));
    sidecar.declare(declare("orders"));
    Status status = sidecar.status();
    assertThat(status.getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    assertThat(status.getDescription()).isEqualTo("Declare was already sent");
  }

  @Test
  void anEmptyMessageBreaksTheProtocol() throws Exception {
    start(config());
    Sidecar sidecar = declared(declare("orders"));
    sidecar.send(DeliverRequest.newBuilder());
    assertThat(sidecar.status().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
  }

  // -------------------------------------------------------------------------------------------
  // Records
  // -------------------------------------------------------------------------------------------

  @Test
  void recordsAreDeliveredAgainstCreditAsTheyAreStored() throws Exception {
    start(config());
    Sidecar sidecar = declared(declare("orders"));
    assign(P0);
    RecordingConsumer kafka = kafka();
    kafka.schedulePollTask(
        () -> {
          ConsumerRecord<byte[], byte[]> record =
              new ConsumerRecord<>("orders", 0, 0, new byte[] {1, 2}, new byte[] {3});
          record.headers().add(new RecordHeader("h", new byte[] {9}));
          record.headers().add(new RecordHeader("none", null));
          kafka.addRecord(record);
          kafka.addRecord(new ConsumerRecord<>("orders", 0, 1, null, null));
        });
    sidecar.nothing();
    sidecar.credits(10);
    RecordBatch batch = sidecar.batch();
    assertThat(batch.getPartition()).isZero();
    assertThat(batch.getRecordsCount()).isEqualTo(2);
    Record first = batch.getRecords(0);
    assertThat(first.getOffset()).isZero();
    assertThat(first.getAttempt()).isEqualTo(1);
    assertThat(first.getKey().toByteArray()).containsExactly(1, 2);
    assertThat(first.getValue().toByteArray()).containsExactly(3);
    assertThat(first.getHeadersCount()).isEqualTo(2);
    assertThat(first.getHeaders(0).getKey()).isEqualTo("h");
    assertThat(first.getHeaders(0).getValue().toByteArray()).containsExactly(9);
    assertThat(first.getHeaders(1).hasValue()).isFalse();
    Record tombstone = batch.getRecords(1);
    assertThat(tombstone.getOffset()).isEqualTo(1);
    assertThat(tombstone.hasKey()).isFalse();
    assertThat(tombstone.hasValue()).isFalse();
  }

  @Test
  void eachStageOfARecordIsTimed() throws Exception {
    start(config());
    Sidecar sidecar = declared(declare("orders"));
    assign(P0);
    RecordingConsumer kafka = kafka();
    long written = System.currentTimeMillis() - 5_000;
    kafka.schedulePollTask(
        () -> {
          kafka.addRecord(stamped(0, written, TimestampType.CREATE_TIME));
          kafka.addRecord(stamped(1, written, TimestampType.LOG_APPEND_TIME));
          // A record with no timestamp has no lag to tell.
          kafka.addRecord(new ConsumerRecord<>("orders", 0, 2, null, null));
        });
    // Fetched, and waiting for credit.
    sidecar.nothing();
    sidecar.credits(10);
    assertThat(sidecar.records(3)).containsExactly("0/0/1", "0/1/1", "0/2/1");
    sidecar.ack(0, 0, 0);
    sidecar.reject(0, 1);
    assertThat(sidecar.records(1)).containsExactly("0/1/2");

    List<HistogramPointData> created =
        histogram("odradek.dispatch.fetch.lag", "topic", "orders", "timestamp_type", "create_time");
    assertThat(created).hasSize(1);
    assertThat(created.getFirst().getCount()).isEqualTo(1);
    assertThat(created.getFirst().getMin()).isGreaterThanOrEqualTo(5_000);
    assertThat(
            recorded(
                "odradek.dispatch.fetch.lag",
                "group",
                "billing",
                "timestamp_type",
                "log_append_time"))
        .isEqualTo(1);
    assertThat(recorded("odradek.dispatch.fetch.lag")).isEqualTo(2);

    // Each record once, at its first delivery, and all three waited for the sidecar's credit.
    List<HistogramPointData> queued =
        histogram("odradek.dispatch.queue.duration", "topic", "orders", "reason", "credit");
    assertThat(queued).hasSize(1);
    assertThat(queued.getFirst().getCount()).isEqualTo(3);
    assertThat(queued.getFirst().getMin()).isGreaterThanOrEqualTo(100);
    assertThat(recorded("odradek.dispatch.queue.duration")).isEqualTo(3);

    eventually("both answers are timed", () -> recorded("odradek.dispatch.ack.duration") == 2);
    assertThat(recorded("odradek.dispatch.ack.duration", "outcome", "acknowledged")).isEqualTo(1);
    assertThat(recorded("odradek.dispatch.ack.duration", "outcome", "rejected")).isEqualTo(1);

    // The group's thread waited in its poll in between.
    List<HistogramPointData> waits =
        histogram("odradek.dispatch.loop.wait", "cluster", "main", "group", "billing");
    assertThat(waits).hasSize(1);
    assertThat(waits.getFirst().getCount()).isPositive();
  }

  private static ConsumerRecord<byte[], byte[]> stamped(
      long offset, long timestamp, TimestampType type) {
    return new ConsumerRecord<>(
        "orders",
        0,
        offset,
        timestamp,
        type,
        1,
        1,
        // A key of its own: under key ordering, nothing waits behind another record.
        new byte[] {(byte) offset},
        new byte[] {2},
        new RecordHeaders(),
        Optional.empty());
  }

  @Test
  void acknowledgedRecordsAreCommittedAtTheInterval() throws Exception {
    start(config());
    Sidecar sidecar = declared(declare("orders"));
    assign(P0);
    // The position found for a partition without a committed offset is committed.
    eventually("the start position is committed", () -> committedNow(P0) == 0);
    for (int offset = 0; offset < 4; offset++) {
      add(P0, offset, "k" + offset);
    }
    sidecar.credits(10);
    assertThat(sidecar.records(4)).containsExactly("0/0/1", "0/1/1", "0/2/1", "0/3/1");
    sidecar.ack(0, 1, 2);
    sidecar.nothing();
    assertThat(committed(P0)).isZero();
    sidecar.ack(0, 0, 0);
    eventually("offsets 0 to 2 are committed", () -> committedNow(P0) == 3);
  }

  private long committedNow(TopicPartition partition) {
    return consumers.get(consumers.size() - 1).commits.getOrDefault(partition, -1L);
  }

  @Test
  void whatASidecarHeldWhenItWentIsDeliveredToAnotherUnderTheSameAttempt() throws Exception {
    start(config());
    Sidecar doomed = declared(declare("orders"));
    Sidecar survivor = declared(declare("orders"));
    assign(P0);
    add(P0, 0, "a");
    add(P0, 1, "b");
    doomed.credits(10);
    assertThat(doomed.records(2)).containsExactly("0/0/1", "0/1/1");
    survivor.credits(10);
    survivor.nothing();
    doomed.requests.onError(Status.CANCELLED.asRuntimeException());
    assertThat(survivor.records(2)).containsExactly("0/0/1", "0/1/1");
    assertThat(metric("odradek.dispatch.records", "topic", "orders")).isEqualTo(4);
    assertThat(metric("odradek.dispatch.redeliveries", "reason", "receiver_gone")).isEqualTo(2);
    assertThat(metric("odradek.dispatch.redeliveries")).isEqualTo(2);
    eventually("one stream is left", () -> metric("odradek.dispatch.receivers") == 1);
    assertThat(metric("odradek.consume.rebalances", "group", "billing", "event", "assigned"))
        .isEqualTo(1);
  }

  @Test
  void aRejectedRecordIsDeliveredAgainWithTheNextAttempt() throws Exception {
    start(config());
    Sidecar sidecar = declared(declare("orders"));
    assign(P0);
    add(P0, 0, "a");
    sidecar.credits(10);
    assertThat(sidecar.records(1)).containsExactly("0/0/1");
    sidecar.reject(0, 0);
    assertThat(sidecar.records(1)).containsExactly("0/0/2");
  }

  @Test
  void aRecordThatUsesItsAttemptsBlocksItsPartitionAndNothingElse() throws Exception {
    start(config());
    Sidecar sidecar =
        declared(
            declare("orders", s -> s.setOrdering(Ordering.ORDERING_PARTITION).setMaxAttempts(2)));
    assign(P0, P1);
    add(P0, 0, "a");
    add(P0, 1, "b");
    add(P0, 2, "c");
    sidecar.credits(10);
    assertThat(sidecar.records(1)).containsExactly("0/0/1");
    sidecar.ack(0, 0, 0);
    assertThat(sidecar.records(1)).containsExactly("0/1/1");
    sidecar.reject(0, 1);
    assertThat(sidecar.records(1)).containsExactly("0/1/2");
    sidecar.reject(0, 1);
    // Offset 2 is never sent; the other partition goes on.
    add(P1, 0, "z");
    assertThat(sidecar.records(1)).containsExactly("1/0/1");
    sidecar.ack(1, 0, 0);
    sidecar.nothing();
    eventually("the other partition is committed", () -> committedNow(P1) == 1);
    eventually("the blocked one is committed up to the record", () -> committedNow(P0) == 1);
    sidecar.nothing();
    assertThat(committed(P0)).isEqualTo(1);
    // And it shows: one partition of the topic is blocked, by one record given up on.
    assertThat(
            metric(
                "odradek.dispatch.partitions.blocked",
                "cluster",
                "main",
                "group",
                "billing",
                "topic",
                "orders"))
        .isEqualTo(1);
    assertThat(metric("odradek.dispatch.exhausted", "action", "block")).isEqualTo(1);
    assertThat(metric("odradek.dispatch.exhausted")).isEqualTo(1);
    assertThat(metric("odradek.dispatch.redeliveries", "reason", "rejected")).isEqualTo(1);
    eventually("nothing is in flight", () -> metric("odradek.dispatch.in_flight") == 0);
    // Until the partition changes owner.
    assign(P1);
    eventually("no longer blocked", () -> metric("odradek.dispatch.partitions.blocked") == 0);
  }

  @Test
  void aPartitionHeldBackByTheWindowIsCountedWhileItIs() throws Exception {
    start(config().setDispatch(config().getDispatchBuilder().setPartitionWindow(2)));
    Sidecar sidecar = declared(declare("orders", s -> s.setOrdering(Ordering.ORDERING_NONE)));
    assign(P0);
    for (int offset = 0; offset < 4; offset++) {
      add(P0, offset, "k" + offset);
    }
    sidecar.credits(10);
    // Offsets 0 and 1 are two offsets of unacknowledged records: 2 waits for the first of them.
    assertThat(sidecar.records(2)).containsExactly("0/0/1", "0/1/1");
    eventually("held back", () -> metric("odradek.dispatch.partitions.at_window") == 1);
    assertThat(metric("odradek.dispatch.window.pauses", "topic", "orders")).isEqualTo(1);
    sidecar.nothing();
    sidecar.ack(0, 0, 1);
    assertThat(sidecar.records(2)).containsExactly("0/2/1", "0/3/1");
    eventually("no longer held back", () -> metric("odradek.dispatch.partitions.at_window") == 0);
    assertThat(metric("odradek.dispatch.window.pauses")).isEqualTo(1);
  }

  @Test
  void aFailedRecordIsNotSentAgainBeforeItsWaitWhichDoubles() throws Exception {
    start(config());
    Sidecar sidecar =
        declared(
            declare(
                "orders", s -> s.setRedeliveryBackoffMs(150).setRedeliveryBackoffMaxMs(10_000)));
    assign(P0);
    add(P0, 0, "a");
    add(P0, 1, "a");
    add(P0, 2, "b");
    sidecar.credits(10);
    assertThat(sidecar.records(2)).containsExactly("0/0/1", "0/2/1");
    long rejected = System.nanoTime();
    sidecar.reject(0, 0);
    assertThat(sidecar.millisUntil("0/0/2", rejected)).isGreaterThanOrEqualTo(150);
    rejected = System.nanoTime();
    sidecar.reject(0, 0);
    // Meanwhile another key goes on, and the record's own waits behind it.
    add(P0, 3, "b");
    sidecar.ack(0, 2, 2);
    assertThat(sidecar.records(1)).containsExactly("0/3/1");
    assertThat(sidecar.millisUntil("0/0/3", rejected)).isGreaterThanOrEqualTo(300);
    sidecar.ack(0, 0, 0);
    assertThat(sidecar.records(1)).containsExactly("0/1/1");
  }

  // -------------------------------------------------------------------------------------------
  // Dead letters
  // -------------------------------------------------------------------------------------------

  private static Declare.Builder deadLettering(int maxAttempts) {
    return declare(
        "orders",
        s ->
            s.setOrdering(Ordering.ORDERING_PARTITION)
                .setMaxAttempts(maxAttempts)
                .setOnExhausted(OnExhausted.ON_EXHAUSTED_DEAD_LETTER)
                .setDeadLetterTopic("orders.dlq"));
  }

  private MockProducer<byte[], byte[]> producer() throws InterruptedException {
    eventually("a dead-letter producer was created", () -> !producers.isEmpty());
    assertThat(producers).hasSize(1);
    return producers.get(0);
  }

  private static String header(ProducerRecord<byte[], byte[]> record, String name) {
    Header header = record.headers().lastHeader(name);
    return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
  }

  @Test
  void anExhaustedRecordIsWrittenToTheDeadLetterTopicAndCountsOnlyOnceTheBrokerAcknowledged()
      throws Exception {
    start(config());
    Sidecar sidecar = declared(deadLettering(2));
    assign(P0);
    add(P0, 0, "a");
    add(P0, 1, "b");
    sidecar.credits(10);
    assertThat(sidecar.records(1)).containsExactly("0/0/1");
    sidecar.reject(0, 0);
    assertThat(sidecar.records(1)).containsExactly("0/0/2");
    assertThat(producers).isEmpty();
    sidecar.reject(0, 0);

    MockProducer<byte[], byte[]> producer = producer();
    eventually("the dead letter is sent", () -> producer.history().size() == 1);
    ProducerRecord<byte[], byte[]> letter = producer.history().get(0);
    assertThat(letter.topic()).isEqualTo("orders.dlq");
    assertThat(letter.partition()).as("left to the partitioner").isNull();
    assertThat(letter.timestamp()).as("the time of the write").isNull();
    assertThat(new String(letter.key(), StandardCharsets.UTF_8)).isEqualTo("a");
    assertThat(new String(letter.value(), StandardCharsets.UTF_8)).isEqualTo("v0");
    assertThat(header(letter, DeadLetters.SOURCE_CLUSTER)).isEqualTo("main");
    assertThat(header(letter, DeadLetters.SOURCE_TOPIC)).isEqualTo("orders");
    assertThat(header(letter, DeadLetters.SOURCE_PARTITION)).isEqualTo("0");
    assertThat(header(letter, DeadLetters.SOURCE_OFFSET)).isEqualTo("0");
    assertThat(header(letter, DeadLetters.SOURCE_GROUP)).isEqualTo("billing");
    assertThat(header(letter, DeadLetters.ATTEMPTS)).isEqualTo("2");
    assertThat(header(letter, DeadLetters.FAILURE)).isEqualTo("rejected");
    assertThat(header(letter, DeadLetters.FAILURE_DETAIL)).isEqualTo("test");

    // Not acknowledged yet: nothing is committed past it, and the partition waits for it.
    sidecar.nothing();
    assertThat(committed(P0)).isLessThanOrEqualTo(0);
    assertThat(metric("odradek.dispatch.dead_letter.writes")).isZero();
    assertThat(producer.completeNext()).isTrue();
    assertThat(sidecar.records(1)).containsExactly("0/1/1");
    eventually("the dead-lettered record is committed", () -> committedNow(P0) == 1);
    assertThat(producer.history()).hasSize(1);
    assertThat(metric("odradek.dispatch.exhausted", "action", "dead_letter")).isEqualTo(1);
    assertThat(metric("odradek.dispatch.dead_letter.writes", "outcome", "ok", "topic", "orders"))
        .isEqualTo(1);
    assertThat(metric("odradek.dispatch.dead_letter.writes")).isEqualTo(1);
    assertThat(metric("odradek.dispatch.partitions.blocked")).isZero();
  }

  @Test
  void aDeadLetterThatCannotBeWrittenIsWrittenAgainAndNothingIsCommittedPastIt() throws Exception {
    start(config());
    Sidecar sidecar = declared(deadLettering(1));
    assign(P0, P1);
    add(P0, 0, "a");
    add(P0, 1, "b");
    sidecar.credits(10);
    assertThat(sidecar.records(1)).containsExactly("0/0/1");
    long rejected = System.nanoTime();
    sidecar.reject(0, 0);
    MockProducer<byte[], byte[]> producer = producer();
    eventually("the dead letter is sent", () -> producer.history().size() == 1);
    assertThat(producer.errorNext(new RecordTooLargeException("too large for the test"))).isTrue();

    // The other partition is not held up by it.
    add(P1, 0, "z");
    assertThat(sidecar.records(1)).containsExactly("1/0/1");
    sidecar.ack(1, 0, 0);
    eventually("the other partition is committed", () -> committedNow(P1) == 1);

    // Written again, not at once, with the same content; the record still holds its partition.
    eventually("the dead letter is sent again", () -> producer.history().size() == 2);
    assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - rejected))
        .isGreaterThanOrEqualTo(DispatchService.DEAD_LETTER_RETRY.toMillis());
    assertThat(producer.history().get(1).headers()).isEqualTo(producer.history().get(0).headers());
    sidecar.nothing();
    assertThat(committedNow(P0)).isLessThanOrEqualTo(0);
    assertThat(metric("odradek.dispatch.dead_letter.writes", "outcome", "error")).isEqualTo(1);
    assertThat(metric("odradek.dispatch.dead_letter.writes", "outcome", "ok")).isZero();
    assertThat(producer.completeNext()).isTrue();
    assertThat(sidecar.records(1)).containsExactly("0/1/1");
    eventually("the dead-lettered record is committed", () -> committedNow(P0) == 1);
    assertThat(metric("odradek.dispatch.dead_letter.writes", "outcome", "ok")).isEqualTo(1);
    // Given up on once, however many times it had to be written.
    assertThat(metric("odradek.dispatch.exhausted")).isEqualTo(1);
  }

  @Test
  void theDeadLetterProducerCarriesTheGuaranteesAndIsNotAnAppsProducer() throws Exception {
    start(config());
    Sidecar sidecar = declared(deadLettering(1));
    assign(P0);
    add(P0, 0, "a");
    sidecar.credits(1);
    assertThat(sidecar.records(1)).containsExactly("0/0/1");
    sidecar.reject(0, 0);
    producer();
    Map<String, Object> properties = producerProperties.get(0);
    assertThat(properties)
        .containsEntry(ProducerConfig.ACKS_CONFIG, "all")
        .containsEntry(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
        .containsEntry(ProducerConfig.CLIENT_ID_CONFIG, "odradek-main-dead-letter");
  }

  @Test
  void aDeadLetterStillBeingWrittenWhenTheDispatcherStopsIsNotCommittedPast() throws Exception {
    start(config());
    Sidecar sidecar = declared(deadLettering(1));
    assign(P0);
    add(P0, 0, "a");
    add(P0, 1, "b");
    sidecar.credits(10);
    assertThat(sidecar.records(1)).containsExactly("0/0/1");
    sidecar.reject(0, 0);
    MockProducer<byte[], byte[]> producer = producer();
    eventually("the dead letter is sent", () -> producer.history().size() == 1);
    RecordingConsumer kafka = kafka();
    // Never acknowledged: the hand-over waits out its grace, and the record is the next owner's.
    service.shutdown(Duration.ofSeconds(5));
    assertThat(sidecar.status().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
    assertThat(kafka.commits.getOrDefault(P0, -1L)).isLessThanOrEqualTo(0);
    assertThat(producer.closed()).isTrue();
  }

  // -------------------------------------------------------------------------------------------
  // Partitions, topics and the end
  // -------------------------------------------------------------------------------------------

  @Test
  void aRevokedPartitionWaitsForWhatIsInFlightThenIsCommittedAndReleased() throws Exception {
    start(config().setConsume(Consume.newBuilder().setRevokeDeadline(Durations.fromSeconds(30))));
    Sidecar sidecar = declared(declare("orders"));
    assign(P0, P1);
    add(P0, 0, "a");
    add(P1, 0, "b");
    sidecar.credits(10);
    assertThat(sidecar.records(2)).containsExactlyInAnyOrder("0/0/1", "1/0/1");
    assign(P1);
    // The acknowledgement arrives while the partition is being handed over, and is committed.
    sidecar.nothing();
    assertThat(committed(P0)).isLessThanOrEqualTo(0);
    sidecar.ack(0, 0, 0);
    eventually("the revoked partition is committed", () -> committedNow(P0) == 1);
    // Nothing was left with the sidecar, so it is told nothing; the other partition goes on.
    add(P1, 1, "c");
    sidecar.ack(1, 0, 0);
    assertThat(sidecar.records(1)).containsExactly("1/1/1");
    assertThat(metric("odradek.dispatch.handovers", "outcome", "completed")).isEqualTo(1);
    assertThat(metric("odradek.dispatch.handovers")).isEqualTo(1);
    assertThat(metric("odradek.dispatch.handover.records")).isZero();
    assertThat(metric("odradek.consume.rebalances", "event", "revoked")).isEqualTo(1);
  }

  @Test
  void aRevokedPartitionFinishesWhatItHeldBackBelowWhatItSentAndCommitsPastAllOfIt()
      throws Exception {
    start(config().setConsume(Consume.newBuilder().setRevokeDeadline(Durations.fromSeconds(30))));
    Sidecar sidecar = declared(declare("orders"));
    assign(P0, P1);
    add(P0, 0, "a");
    add(P0, 1, "a");
    add(P0, 2, "b");
    sidecar.credits(10);
    // Offset 1 waits for 0, of its key; 2, of another key, is sent past it and acknowledged.
    assertThat(sidecar.records(2)).containsExactly("0/0/1", "0/2/1");
    sidecar.ack(0, 2, 2);
    assign(P1);
    sidecar.nothing();
    assertThat(committed(P0)).isLessThanOrEqualTo(0);
    // While the partition is handed over: 1 is sent once 0 is acknowledged, and the offset
    // committed is past everything this replica was acknowledged, 2 included.
    sidecar.ack(0, 0, 0);
    assertThat(sidecar.records(1)).containsExactly("0/1/1");
    sidecar.ack(0, 1, 1);
    eventually("the revoked partition is committed", () -> committedNow(P0) == 3);
    assertThat(metric("odradek.dispatch.handovers", "outcome", "completed")).isEqualTo(1);
    assertThat(metric("odradek.dispatch.handovers")).isEqualTo(1);
    // Nothing it sent is left for the next owner to send again.
    assertThat(metric("odradek.dispatch.handover.records")).isZero();
    assertThat(metric("odradek.dispatch.records")).isEqualTo(3);
  }

  @Test
  void whatIsStillHeldAfterTheGraceIsReleased() throws Exception {
    start(config());
    Sidecar sidecar = declared(declare("orders"));
    assign(P0, P1);
    add(P0, 0, "a");
    add(P0, 1, "b");
    sidecar.credits(10);
    assertThat(sidecar.records(2)).containsExactly("0/0/1", "0/1/1");
    sidecar.ack(0, 0, 0);
    assign(P1);
    DeliverResponse response = sidecar.next();
    assertThat(response.hasReleased()).as("%s", response).isTrue();
    assertThat(response.getReleased().getPartitionsList()).containsExactly(0);
    // The sidecar is told first and the commit follows, before the partition is given up.
    eventually("what was acknowledged is committed", () -> committedNow(P0) == 1);
    // The wait ran out, with one record sent that the next owner sends again.
    assertThat(metric("odradek.dispatch.handovers", "outcome", "deadline")).isEqualTo(1);
    assertThat(metric("odradek.dispatch.handovers")).isEqualTo(1);
    assertThat(metric("odradek.dispatch.handover.records", "topic", "orders")).isEqualTo(1);
  }

  @Test
  void aSecondTopicGrowsTheSubscriptionOfTheSameConsumer() throws Exception {
    start(config());
    Sidecar orders = declared(declare("orders"));
    assign(P0);
    add(P0, 0, "a");
    orders.credits(10);
    assertThat(orders.records(1)).containsExactly("0/0/1");

    Sidecar refunds = declared(declare("refunds", s -> s.setOrdering(Ordering.ORDERING_NONE)));
    eventually(
        "subscribed to both",
        () -> consumers.get(0).subscriptions.contains(Set.of("orders", "refunds")));
    assign(P0, R0);
    add(R0, 0, "r");
    add(P0, 1, "b");
    refunds.credits(10);
    assertThat(refunds.records(1)).containsExactly("0/0/1");
    // The first topic went on from where it was: nothing of it is delivered again.
    assertThat(orders.records(1)).containsExactly("0/1/1");
    orders.nothing();
    assertThat(consumers).hasSize(1);
  }

  @Test
  void aTopicWithoutSidecarsIsDroppedAfterTheGraceAndTheConsumerClosedWithTheLast()
      throws Exception {
    start(config().setDispatch(Dispatch.newBuilder().setIdleGrace(Durations.fromMillis(300))));
    Sidecar orders = declared(declare("orders"));
    Sidecar refunds = declared(declare("refunds"));
    RecordingConsumer kafka = kafka();
    eventually(
        "subscribed to both", () -> kafka.subscriptions.contains(Set.of("orders", "refunds")));
    assign(P0, R0);
    add(R0, 0, "r");
    refunds.credits(1);
    assertThat(refunds.records(1)).containsExactly("0/0/1");
    refunds.ack(0, 0, 0);

    refunds.requests.onCompleted();
    assertThat(refunds.status().isOk()).isTrue();
    eventually(
        "refunds is dropped",
        () ->
            kafka.subscriptions.size() >= 2
                && kafka
                    .subscriptions
                    .get(kafka.subscriptions.size() - 1)
                    .equals(Set.of("orders")));
    assertThat(kafka.commits).containsEntry(R0, 1L);
    assertThat(kafka.closed()).isFalse();

    orders.requests.onCompleted();
    assertThat(orders.status().isOk()).isTrue();
    eventually("the consumer is closed", kafka::closed);

    // The group is gone, not broken: the next declaration starts a new consumer.
    declared(declare("orders"));
    eventually("a second consumer", () -> consumers.size() == 2);
  }

  @Test
  void aSidecarThatComesBackWithinTheGraceFindsTheTopicStillConsumed() throws Exception {
    start(config());
    Sidecar first = declared(declare("orders"));
    assign(P0);
    add(P0, 0, "a");
    add(P0, 1, "b");
    first.credits(1);
    assertThat(first.records(1)).containsExactly("0/0/1");
    first.requests.onCompleted();
    assertThat(first.status().isOk()).isTrue();

    Sidecar second = declared(declare("orders"));
    second.credits(10);
    assertThat(second.records(2)).containsExactly("0/0/1", "0/1/1");
    assertThat(consumers).hasSize(1);
    assertThat(consumers.get(0).subscriptions).containsExactly(Set.of("orders"));
  }

  @Test
  void shutdownCommitsEndsTheStreamsAndRefusesDeclarations() throws Exception {
    start(config());
    Sidecar sidecar = declared(declare("orders"));
    assign(P0);
    add(P0, 0, "a");
    add(P0, 1, "b");
    sidecar.credits(10);
    assertThat(sidecar.records(2)).containsExactly("0/0/1", "0/1/1");
    sidecar.ack(0, 0, 0);
    RecordingConsumer kafka = kafka();

    service.shutdown(Duration.ofSeconds(10));
    Status status = sidecar.status();
    assertThat(status.getCode()).isEqualTo(Status.Code.UNAVAILABLE);
    assertThat(kafka.commits).containsEntry(P0, 1L);
    assertThat(kafka.closed()).isTrue();

    Sidecar late = stream();
    late.declare(declare("orders"));
    assertThat(late.status().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
    assertThat(consumers).hasSize(1);
  }

  @Test
  void aSidecarThatSitsOnItsRecordsDoesNotStopTheOthers() throws Exception {
    start(config());
    Sidecar silent = declared(declare("orders", s -> s.setOrdering(Ordering.ORDERING_NONE)));
    Sidecar working = declared(declare("orders", s -> s.setOrdering(Ordering.ORDERING_NONE)));
    assign(P0);
    silent.credits(2);
    add(P0, 0, "a");
    add(P0, 1, "b");
    assertThat(silent.records(2)).containsExactly("0/0/1", "0/1/1");
    // It holds them and says nothing; records keep flowing to the other.
    working.credits(100);
    for (int offset = 2; offset < 50; offset++) {
      add(P0, offset, "k");
    }
    assertThat(working.records(48)).hasSize(48);
  }

  // -------------------------------------------------------------------------------------------
  // The group's thread: it waits in its poll, and commands wake the consumer
  // -------------------------------------------------------------------------------------------

  /** Nothing due for long: no commit at an interval, and a long revoke deadline. */
  private static ServerConfig.Builder quiet() {
    return config()
        .setConsume(
            Consume.newBuilder()
                .setCommitInterval(Durations.fromSeconds(3600))
                .setRevokeDeadline(Durations.fromSeconds(30)));
  }

  @Test
  void aCommitIntervalBelowAMillisecondDoesNotMakeTheThreadSpin() throws Exception {
    // It reaches the group in whole milliseconds, as zero: a commit then always due would make
    // every poll return at once.
    start(
        config()
            .setConsume(
                Consume.newBuilder()
                    .setCommitInterval(Durations.fromNanos(500_000))
                    .setRevokeDeadline(Durations.fromSeconds(30))));
    declared(declare("orders"));
    assign(P0);
    RecordingConsumer kafka = kafka();
    Thread.sleep(100);
    int before = kafka.polls.get();
    Thread.sleep(500);
    // A poll at most every millisecond, about 500; a thread that spins makes tens of thousands.
    assertThat(kafka.polls.get() - before)
        .as("polls in 500 ms; the last polls: %s", kafka.recentPolls)
        .isLessThan(2_000);
  }

  /** Waits until the group's thread waits in its poll for as long as it may: it has nothing due. */
  private static void waitsInItsPoll(RecordingConsumer kafka) throws InterruptedException {
    Duration longest = Duration.ofMillis(DispatchGroup.MAX_POLL_WAIT_MS);
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    Duration seen = kafka.waiting;
    while (!longest.equals(seen) && System.nanoTime() < deadline) {
      Thread.sleep(5);
      seen = kafka.waiting;
    }
    assertThat(seen)
        .as("the group's thread waits in its poll; the last polls: %s", kafka.recentPolls)
        .isEqualTo(longest);
  }

  /** Waits, on the group's thread, for a wakeup beyond the {@code before} first. */
  private static void awaitWakeup(RecordingConsumer kafka, int before) {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (kafka.wakeups.get() <= before && System.nanoTime() < deadline) {
      Thread.onSpinWait();
    }
  }

  @Test
  void aCommandWakesTheConsumerWhereTheThreadWaits() throws Exception {
    start(quiet());
    Sidecar sidecar = declared(declare("orders"));
    assign(P0);
    add(P0, 0, "a");
    RecordingConsumer kafka = kafka();
    // Fetched, and waiting for credit, with nothing due.
    waitsInItsPoll(kafka);
    int woken = kafka.wokenPolls.get();
    sidecar.credits(1);
    assertThat(sidecar.records(1)).containsExactly("0/0/1");
    // The credit ended the wait: the poll was woken, and did not run to its timeout.
    assertThat(kafka.wokenPolls.get()).isGreaterThan(woken);
  }

  @Test
  void aRecordIsHandedOnAsSoonAsTheConsumerHasIt() throws Exception {
    start(quiet());
    Sidecar sidecar = declared(declare("orders"));
    assign(P0);
    sidecar.credits(10);
    RecordingConsumer kafka = kafka();
    waitsInItsPoll(kafka);
    long since = System.nanoTime();
    kafka.addRecord(stamped(0, System.currentTimeMillis(), TimestampType.CREATE_TIME));
    // The poll returns it at once, rather than at the end of its wait.
    assertThat(sidecar.millisUntil("0/0/1", since)).isLessThan(DispatchGroup.MAX_POLL_WAIT_MS / 2);
    List<HistogramPointData> lag = histogram("odradek.dispatch.fetch.lag", "topic", "orders");
    assertThat(lag).hasSize(1);
    assertThat(lag.getFirst().getMax()).isLessThan(DispatchGroup.MAX_POLL_WAIT_MS / 2);
  }

  @Test
  void aWakeupPendingAsThePollReturnsDoesNotCostTheNextCommit() throws Exception {
    start(quiet());
    Sidecar first = declared(declare("orders"));
    assign(P0);
    add(P0, 0, "a");
    add(P0, 1, "b");
    first.credits(10);
    assertThat(first.records(2)).containsExactly("0/0/1", "0/1/1");
    first.ack(0, 0, 1);
    first.requests.onCompleted();
    assertThat(first.status().isOk()).isTrue();
    RecordingConsumer kafka = kafka();
    waitsInItsPoll(kafka);

    // A declaration with other settings, which nobody holds the topic with now, arrives as the
    // poll returns: the wakeup it asks for is pending when the thread hands the partition over
    // and commits it synchronously.
    Sidecar second = stream();
    int before = kafka.wakeups.get();
    kafka.schedulePollTask(
        () -> {
          second.declare(declare("orders", s -> s.setOrdering(Ordering.ORDERING_NONE)));
          awaitWakeup(kafka, before);
        });
    second.accepted();
    eventually("the partition is committed", () -> committedNow(P0) == 2);
    // The commit met the wakeup, and was made again.
    assertThat(kafka.wokenCalls.get()).isPositive();
    // And the topic goes on with its new settings, from there.
    add(P0, 2, "c");
    second.credits(1);
    assertThat(second.records(1)).containsExactly("0/2/1");
  }

  @Test
  void aWakeupPendingAsTheHandOverBeginsDoesNotCostItsCommit() throws Exception {
    start(quiet());
    Sidecar sidecar = declared(declare("orders"));
    assign(P0, P1);
    add(P0, 0, "a");
    sidecar.credits(10);
    assertThat(sidecar.records(1)).containsExactly("0/0/1");
    sidecar.ack(0, 0, 0);
    RecordingConsumer kafka = kafka();
    waitsInItsPoll(kafka);
    // A wakeup asked for as the poll gave way to the rebalance: pending in onPartitionsRevoked.
    kafka.schedulePollTask(
        () -> {
          kafka.wakeup();
          kafka.rebalance(List.of(P1));
        });
    eventually("the revoked partition is committed", () -> committedNow(P0) == 1);
    assertThat(kafka.wokenCalls.get()).isPositive();
    assertThat(metric("odradek.dispatch.handovers", "outcome", "completed")).isEqualTo(1);
  }

  @Test
  void aCommandWhileTheHandOverRunsDoesNotWakeTheConsumer() throws Exception {
    start(quiet());
    Sidecar sidecar = declared(declare("orders"));
    assign(P0, P1);
    add(P0, 0, "a");
    sidecar.credits(10);
    assertThat(sidecar.records(1)).containsExactly("0/0/1");
    RecordingConsumer kafka = kafka();
    waitsInItsPoll(kafka);
    // Earlier calls may have met a wakeup a command asked for as a callback began.
    int woken = kafka.wokenCalls.get();
    AtomicInteger duringRebalance = new AtomicInteger(-1);
    kafka.schedulePollTask(
        () -> {
          int before = kafka.wakeups.get();
          kafka.rebalance(List.of(P1));
          duringRebalance.set(kafka.wakeups.get() - before);
        });
    // The hand-over waits for the acknowledgement, which arrives while it runs.
    sidecar.nothing();
    sidecar.ack(0, 0, 0);
    eventually("the revoked partition is committed", () -> committedNow(P0) == 1);
    eventually("the rebalance is over", () -> duringRebalance.get() >= 0);
    assertThat(duringRebalance.get()).isZero();
    assertThat(kafka.wokenCalls.get()).isEqualTo(woken);
    assertThat(metric("odradek.dispatch.handovers", "outcome", "completed")).isEqualTo(1);
  }

  @Test
  void aCommandQueuedDuringACallbackWakesThePollItReturnsInto() throws Exception {
    start(quiet());
    Sidecar sidecar = declared(declare("orders"));
    RecordingConsumer kafka = kafka();
    waitsInItsPoll(kafka);
    // A credit queued while onPartitionsAssigned looks up the committed offsets.
    AtomicInteger once = new AtomicInteger();
    kafka.onCommitted =
        () -> {
          if (once.getAndIncrement() == 0) {
            sidecar.credits(1);
            try {
              Thread.sleep(300);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }
        };
    AtomicInteger duringRebalance = new AtomicInteger(-1);
    kafka.schedulePollTask(
        () -> {
          kafka.updateBeginningOffsets(Map.of(P0, 0L));
          kafka.updateEndOffsets(Map.of(P0, 0L));
          int before = kafka.wakeups.get();
          kafka.rebalance(List.of(P0));
          duringRebalance.set(kafka.wakeups.get() - before);
        });
    eventually("the rebalance is over", () -> duringRebalance.get() >= 0);
    // Not while the callback ran, but as it returned, since the credit was waiting.
    assertThat(duringRebalance.get()).isEqualTo(1);
    add(P0, 0, "a");
    assertThat(sidecar.records(1)).containsExactly("0/0/1");
  }

  @Test
  void aWakeupPendingAsPartitionsAreAssignedDoesNotStopTheirAdoption() throws Exception {
    start(quiet());
    Sidecar sidecar = declared(declare("orders"));
    RecordingConsumer kafka = kafka();
    waitsInItsPoll(kafka);
    kafka.schedulePollTask(
        () -> {
          kafka.updateBeginningOffsets(Map.of(P0, 0L));
          kafka.updateEndOffsets(Map.of(P0, 0L));
          kafka.wakeup();
          kafka.rebalance(List.of(P0));
        });
    add(P0, 0, "a");
    sidecar.credits(1);
    assertThat(sidecar.records(1)).containsExactly("0/0/1");
    // The lookup of the committed offset met the wakeup, and was made again.
    assertThat(kafka.wokenCalls.get()).isPositive();
  }
}
