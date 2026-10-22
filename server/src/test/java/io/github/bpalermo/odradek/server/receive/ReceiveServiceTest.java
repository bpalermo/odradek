package io.github.bpalermo.odradek.server.receive;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import io.github.bpalermo.odradek.config.v1.Cluster;
import io.github.bpalermo.odradek.config.v1.Codec;
import io.github.bpalermo.odradek.config.v1.Consumer;
import io.github.bpalermo.odradek.config.v1.ConsumerSource;
import io.github.bpalermo.odradek.config.v1.Dispatcher;
import io.github.bpalermo.odradek.config.v1.OnDecodeError;
import io.github.bpalermo.odradek.config.v1.Pipeline;
import io.github.bpalermo.odradek.config.v1.ServerConfig;
import io.github.bpalermo.odradek.config.v1.Transform;
import io.github.bpalermo.odradek.delivery.v1.Accepted;
import io.github.bpalermo.odradek.delivery.v1.Declare;
import io.github.bpalermo.odradek.delivery.v1.DeliverRequest;
import io.github.bpalermo.odradek.delivery.v1.DeliverResponse;
import io.github.bpalermo.odradek.delivery.v1.DeliveryServiceGrpc;
import io.github.bpalermo.odradek.delivery.v1.OffsetReset;
import io.github.bpalermo.odradek.delivery.v1.Ordering;
import io.github.bpalermo.odradek.delivery.v1.PeerInfo;
import io.github.bpalermo.odradek.delivery.v1.Record;
import io.github.bpalermo.odradek.delivery.v1.Released;
import io.github.bpalermo.odradek.delivery.v1.Replica;
import io.github.bpalermo.odradek.delivery.v1.ReplicaList;
import io.github.bpalermo.odradek.kafka.v1.Ack;
import io.github.bpalermo.odradek.kafka.v1.ConsumeRequest;
import io.github.bpalermo.odradek.kafka.v1.ConsumeResponse;
import io.github.bpalermo.odradek.kafka.v1.ConsumedRecord;
import io.github.bpalermo.odradek.kafka.v1.Credits;
import io.github.bpalermo.odradek.kafka.v1.OdradekServiceGrpc;
import io.github.bpalermo.odradek.kafka.v1.PartitionAssignment;
import io.github.bpalermo.odradek.kafka.v1.RecordBatch;
import io.github.bpalermo.odradek.kafka.v1.Subscribe;
import io.github.bpalermo.odradek.kafka.v1.TopicPartition;
import io.github.bpalermo.odradek.server.codec.CodecRegistry;
import io.github.bpalermo.odradek.server.codec.ContractRegistry;
import io.github.bpalermo.odradek.server.consume.ConsumeService;
import io.github.bpalermo.odradek.server.produce.SuffixTransform;
import io.github.bpalermo.odradek.server.telemetry.ServerMetrics;
import io.github.bpalermo.odradek.server.tls.TlsSources;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The receiving side against scripted dispatcher replicas: in-process gRPC servers written here,
 * each under the name that stands for its address. The app's side is a real Consume stream through
 * {@link ConsumeService}. What a real dispatcher and a real broker add is the end-to-end suite's.
 */
class ReceiveServiceTest {
  /** The configured address: the dispatcher's Service, behind which is one of the replicas. */
  private final String service = "service-" + UUID.randomUUID();

  private final String r1 = "r1-" + UUID.randomUUID();
  private final String r2 = "r2-" + UUID.randomUUID();
  private final String r3 = "r3-" + UUID.randomUUID();

  private final List<Server> servers = new ArrayList<>();
  private final List<ManagedChannel> dispatcherChannels = new ArrayList<>();
  private CodecRegistry codecs;
  private ReceiveService receive;
  private ConsumeService consume;
  private Server appServer;
  private ManagedChannel appChannel;

  /** The list every replica answers a declaration with. */
  private volatile ReplicaList listed = ReplicaList.getDefaultInstance();

  // -------------------------------------------------------------------------------------------
  // A scripted replica
  // -------------------------------------------------------------------------------------------

  /** One Deliver stream as a replica sees it. */
  private static final class Stream {
    final BlockingQueue<DeliverRequest> requests = new LinkedBlockingQueue<>();

    /** OK when the sidecar half-closed, its status when the stream broke. */
    final CompletableFuture<Status> ended = new CompletableFuture<>();

    StreamObserver<DeliverResponse> responses;

    DeliverRequest next() throws InterruptedException {
      DeliverRequest request = requests.poll(10, TimeUnit.SECONDS);
      assertThat(request).as("a request within 10s").isNotNull();
      return request;
    }

    Declare declare() throws InterruptedException {
      DeliverRequest request = next();
      assertThat(request.hasDeclare()).as("%s", request).isTrue();
      return request.getDeclare();
    }

    long credits() throws InterruptedException {
      DeliverRequest request = next();
      assertThat(request.hasCredits()).as("%s", request).isTrue();
      return request.getCredits().getRecords();
    }

    io.github.bpalermo.odradek.delivery.v1.Ack ack() throws InterruptedException {
      DeliverRequest request = next();
      assertThat(request.hasAck()).as("%s", request).isTrue();
      return request.getAck();
    }

    void nothing() throws InterruptedException {
      assertThat(requests.poll(300, TimeUnit.MILLISECONDS)).isNull();
    }

    void send(DeliverResponse.Builder response) {
      responses.onNext(response.build());
    }

    /** Sends records of one partition, each a string value at an offset. */
    void records(int partition, Object... offsetsAndValues) {
      io.github.bpalermo.odradek.delivery.v1.RecordBatch.Builder batch =
          io.github.bpalermo.odradek.delivery.v1.RecordBatch.newBuilder().setPartition(partition);
      for (int i = 0; i < offsetsAndValues.length; i += 2) {
        batch.addRecords(
            Record.newBuilder()
                .setOffset(((Number) offsetsAndValues[i]).longValue())
                .setValue(ByteString.copyFromUtf8((String) offsetsAndValues[i + 1]))
                .setAttempt(1));
      }
      send(DeliverResponse.newBuilder().setRecords(batch));
    }

    void released(int... partitions) {
      Released.Builder released = Released.newBuilder();
      for (int partition : partitions) {
        released.addPartitions(partition);
      }
      send(DeliverResponse.newBuilder().setReleased(released));
    }

    void replicas(ReplicaList list) {
      send(DeliverResponse.newBuilder().setReplicas(list));
    }

    void end(Status status) {
      responses.onError(status.asRuntimeException());
    }

    Status ended() throws Exception {
      return ended.get(10, TimeUnit.SECONDS);
    }
  }

  /** A dispatcher replica: accepts every declaration with the current list, unless told not to. */
  private final class FakeReplica extends DeliveryServiceGrpc.DeliveryServiceImplBase {
    final String address;
    final BlockingQueue<Stream> streams = new LinkedBlockingQueue<>();

    /** When set, declarations are refused with it. */
    volatile Status refusal;

    /** What it says its address is; null says nothing. */
    volatile String says;

    FakeReplica(String address) {
      this.address = address;
      this.says = address;
    }

    @Override
    public StreamObserver<DeliverRequest> deliver(StreamObserver<DeliverResponse> responses) {
      Stream stream = new Stream();
      stream.responses = responses;
      return new StreamObserver<DeliverRequest>() {
        private boolean declared;

        @Override
        public void onNext(DeliverRequest request) {
          if (!declared) {
            declared = true;
            // Answered before the test can see the stream: it writes to it from its own thread.
            Status refuse = refusal;
            if (refuse != null) {
              responses.onError(refuse.asRuntimeException());
            } else {
              Accepted.Builder accepted =
                  Accepted.newBuilder()
                      .setSettings(request.getDeclare().getSettings())
                      .setReplicas(listed)
                      .setDispatcher(PeerInfo.newBuilder().setVersion("test"));
              if (says != null) {
                accepted.setAddress(says);
              }
              responses.onNext(DeliverResponse.newBuilder().setAccepted(accepted).build());
            }
            stream.requests.add(request);
            streams.add(stream);
            return;
          }
          stream.requests.add(request);
        }

        @Override
        public void onError(Throwable t) {
          stream.ended.complete(Status.fromThrowable(t));
        }

        @Override
        public void onCompleted() {
          stream.ended.complete(Status.OK);
          responses.onCompleted();
        }
      };
    }

    /** The next stream opened to this replica, its declaration still unread. */
    Stream stream() throws InterruptedException {
      Stream stream = streams.poll(10, TimeUnit.SECONDS);
      assertThat(stream).as("a stream to %s within 10s", address).isNotNull();
      return stream;
    }

    /** The next stream, past its declaration. */
    Stream accepted() throws InterruptedException {
      Stream stream = stream();
      stream.declare();
      return stream;
    }

    void noStream() throws InterruptedException {
      assertThat(streams.poll(300, TimeUnit.MILLISECONDS)).as("no stream to %s", address).isNull();
    }
  }

  /** Serves a replica under a name; a name nothing is served under refuses connections. */
  private Server serve(String name, FakeReplica replica) throws IOException {
    Server server = InProcessServerBuilder.forName(name).addService(replica).build().start();
    servers.add(server);
    return server;
  }

  private static ReplicaList list(long generation, String... addresses) {
    ReplicaList.Builder list = ReplicaList.newBuilder().setGeneration(generation);
    for (String address : addresses) {
      list.addReplicas(Replica.newBuilder().setAddress(address));
    }
    return list.build();
  }

  /** One replica, reached through the Service address as well as its own. */
  private FakeReplica single() throws IOException {
    FakeReplica replica = new FakeReplica(r1);
    serve(service, replica);
    serve(r1, replica);
    listed = list(1, r1);
    return replica;
  }

  // -------------------------------------------------------------------------------------------
  // The app
  // -------------------------------------------------------------------------------------------

  /** The app's end of one Consume stream. */
  private static final class App {
    final BlockingQueue<ConsumeResponse> responses = new LinkedBlockingQueue<>();
    final CompletableFuture<Status> closed = new CompletableFuture<>();
    StreamObserver<ConsumeRequest> requests;

    void subscribe(int maxProcessingMs, String... topics) {
      requests.onNext(
          ConsumeRequest.newBuilder()
              .setSubscribe(
                  Subscribe.newBuilder()
                      .addAllTopics(List.of(topics))
                      .setMaxProcessingMs(maxProcessingMs))
              .build());
    }

    void credits(int records) {
      requests.onNext(
          ConsumeRequest.newBuilder().setCredits(Credits.newBuilder().setRecords(records)).build());
    }

    void ack(String topic, int partition, long epoch, long first, long last) {
      requests.onNext(
          ConsumeRequest.newBuilder()
              .setAck(
                  Ack.newBuilder()
                      .setTopic(topic)
                      .setPartition(partition)
                      .setAssignmentEpoch(epoch)
                      .setFirstOffset(first)
                      .setLastOffset(last))
              .build());
    }

    ConsumeResponse next() throws InterruptedException {
      ConsumeResponse response = responses.poll(10, TimeUnit.SECONDS);
      assertThat(response).as("a response within 10s").isNotNull();
      return response;
    }

    PartitionAssignment assigned() throws InterruptedException {
      ConsumeResponse response = next();
      assertThat(response.hasAssigned()).as("%s", response).isTrue();
      assertThat(response.getAssigned().getPartitionsCount()).isEqualTo(1);
      return response.getAssigned().getPartitions(0);
    }

    RecordBatch batch() throws InterruptedException {
      ConsumeResponse response = next();
      assertThat(response.hasRecords()).as("%s", response).isTrue();
      return response.getRecords();
    }

    List<TopicPartition> lost() throws InterruptedException {
      ConsumeResponse response = next();
      assertThat(response.hasLost()).as("%s", response).isTrue();
      return response.getLost().getPartitionsList();
    }

    void nothing() throws InterruptedException {
      assertThat(responses.poll(300, TimeUnit.MILLISECONDS)).isNull();
    }

    Status status() throws Exception {
      return closed.get(10, TimeUnit.SECONDS);
    }
  }

  private static Consumer.Builder dispatched() {
    return Consumer.newBuilder()
        .setCluster("main")
        .setGroupId("billing")
        .setSource(ConsumerSource.CONSUMER_SOURCE_DISPATCHER);
  }

  private static ServerConfig.Builder config() {
    return ServerConfig.newBuilder()
        .setDispatcher(Dispatcher.newBuilder().setAddress("dispatcher:9465"))
        .putConsumers("orders", dispatched().build())
        .putConsumers("refunds", dispatched().build());
  }

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

  /** The single point of a histogram for one topic, which must have one. */
  private HistogramPointData histogram(String name, String topic) {
    List<HistogramPointData> points = new ArrayList<>();
    for (MetricData data : reader.collectAllMetrics()) {
      if (data.getName().equals(name)) {
        for (HistogramPointData point : data.getHistogramData().getPoints()) {
          if (topic.equals(point.getAttributes().get(AttributeKey.stringKey("topic")))) {
            points.add(point);
          }
        }
      }
    }
    assertThat(points).as("%s of %s", name, topic).hasSize(1);
    return points.getFirst();
  }

  /** When positive, the next write of records to the app takes this long, once. */
  private final AtomicLong slowWriteMs = new AtomicLong();

  /** The server's end of the app's stream, with a write of records that can be made slow. */
  private static final class SlowWrites extends ServerCallStreamObserver<ConsumeResponse> {
    private final ServerCallStreamObserver<ConsumeResponse> call;
    private final AtomicLong slowMs;

    SlowWrites(ServerCallStreamObserver<ConsumeResponse> call, AtomicLong slowMs) {
      this.call = call;
      this.slowMs = slowMs;
    }

    @Override
    public void onNext(ConsumeResponse response) {
      long slow = response.hasRecords() ? slowMs.getAndSet(0) : 0;
      if (slow > 0) {
        try {
          Thread.sleep(slow);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      call.onNext(response);
    }

    @Override
    public void onError(Throwable t) {
      call.onError(t);
    }

    @Override
    public void onCompleted() {
      call.onCompleted();
    }

    @Override
    public boolean isCancelled() {
      return call.isCancelled();
    }

    @Override
    public void setOnCancelHandler(Runnable handler) {
      call.setOnCancelHandler(handler);
    }

    @Override
    public void setOnCloseHandler(Runnable handler) {
      call.setOnCloseHandler(handler);
    }

    @Override
    public void setCompression(String compression) {
      call.setCompression(compression);
    }

    @Override
    public boolean isReady() {
      return call.isReady();
    }

    @Override
    public void setOnReadyHandler(Runnable handler) {
      call.setOnReadyHandler(handler);
    }

    @Override
    public void disableAutoInboundFlowControl() {
      call.disableAutoInboundFlowControl();
    }

    @Override
    public void request(int count) {
      call.request(count);
    }

    @Override
    public void setMessageCompression(boolean enable) {
      call.setMessageCompression(enable);
    }
  }

  private void start(ServerConfig.Builder builder) throws Exception {
    ServerConfig config = builder.build();
    codecs = CodecRegistry.create(config, ContractRegistry.empty(), getClass().getClassLoader());
    receive =
        ReceiveService.create(
            new ReceiveSession.Options(
                service,
                "1.2.3",
                Duration.ofSeconds(2),
                System::nanoTime,
                // The shortest delays: 50 ms, 100 ms, 200 ms and so on.
                () -> new Backoff(bound -> 0)),
            metrics,
            address -> {
              ManagedChannel channel = InProcessChannelBuilder.forName(address).build();
              synchronized (dispatcherChannels) {
                dispatcherChannels.add(channel);
              }
              return channel;
            });
    consume =
        new ConsumeService(
            config, codecs, TlsSources.none(), "epoch-1", ServerMetrics.noop(), receive);
    String name = InProcessServerBuilder.generateName();
    appServer =
        InProcessServerBuilder.forName(name)
            .addService(
                new OdradekServiceGrpc.OdradekServiceImplBase() {
                  @Override
                  public StreamObserver<ConsumeRequest> consume(
                      StreamObserver<ConsumeResponse> responses) {
                    return consume.consume(
                        new SlowWrites(
                            (ServerCallStreamObserver<ConsumeResponse>) responses, slowWriteMs));
                  }
                })
            .build()
            .start();
    appChannel = InProcessChannelBuilder.forName(name).build();
  }

  @AfterEach
  void stop() throws InterruptedException {
    if (appChannel != null) {
      appChannel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
      appServer.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
      consume.shutdown(Duration.ofSeconds(5));
      receive.close();
      codecs.close();
    }
    synchronized (dispatcherChannels) {
      for (ManagedChannel channel : dispatcherChannels) {
        channel.shutdownNow();
      }
    }
    for (Server server : servers) {
      server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
    provider.close();
  }

  private App app() {
    App app = new App();
    app.requests =
        OdradekServiceGrpc.newStub(appChannel)
            .consume(
                new StreamObserver<ConsumeResponse>() {
                  @Override
                  public void onNext(ConsumeResponse response) {
                    app.responses.add(response);
                  }

                  @Override
                  public void onError(Throwable t) {
                    app.closed.complete(Status.fromThrowable(t));
                  }

                  @Override
                  public void onCompleted() {
                    app.closed.complete(Status.OK);
                  }
                });
    return app;
  }

  /** Subscribes to "orders" with a deadline of a minute and consumes the handshake reply. */
  private App subscribed() throws InterruptedException {
    App app = app();
    app.subscribe(60_000, "orders");
    assertThat(app.next().hasSubscribed()).isTrue();
    return app;
  }

  private static List<Long> offsets(RecordBatch batch) {
    return batch.getRecordsList().stream().map(ConsumedRecord::getOffset).toList();
  }

  private static List<String> values(RecordBatch batch) {
    return batch.getRecordsList().stream().map(record -> record.getValue().toStringUtf8()).toList();
  }

  private static TopicPartition partition(String topic, int partition) {
    return TopicPartition.newBuilder().setTopic(topic).setPartition(partition).build();
  }

  // -------------------------------------------------------------------------------------------
  // The handshake
  // -------------------------------------------------------------------------------------------

  @Test
  void theDeclarationIsTheConsumerEntryWithTheAppsDeadline() throws Exception {
    FakeReplica replica = single();
    start(
        config()
            .putConsumers(
                "orders",
                dispatched()
                    .setOrdering(io.github.bpalermo.odradek.config.v1.Ordering.ORDERING_PARTITION)
                    .setOffsetReset(
                        io.github.bpalermo.odradek.config.v1.OffsetReset.OFFSET_RESET_LATEST)
                    .setStartTimestamp(Timestamps.fromMillis(1_700_000_000_000L))
                    .setMaxAttempts(3)
                    .setRedeliveryBackoff(Durations.fromMillis(250))
                    .setRedeliveryBackoffMax(Durations.fromSeconds(90))
                    .build()));
    App app = app();
    app.subscribe(45_000, "orders");
    ConsumeResponse subscribed = app.next();
    assertThat(subscribed.getSubscribed().getCluster()).isEqualTo("main");
    assertThat(subscribed.getSubscribed().getGroupId()).isEqualTo("billing");
    assertThat(subscribed.getSubscribed().getMaxProcessingMs()).isEqualTo(45_000);
    assertThat(subscribed.getSubscribed().getServerEpoch()).isEqualTo("epoch-1");

    Declare declare = replica.stream().declare();
    assertThat(declare.getCluster()).isEqualTo("main");
    assertThat(declare.getGroupId()).isEqualTo("billing");
    assertThat(declare.getTopic()).isEqualTo("orders");
    assertThat(declare.getSidecar().getVersion()).isEqualTo("1.2.3");
    assertThat(declare.getSettings().getOrdering()).isEqualTo(Ordering.ORDERING_PARTITION);
    assertThat(declare.getSettings().getOffsetReset()).isEqualTo(OffsetReset.OFFSET_RESET_LATEST);
    assertThat(declare.getSettings().getStartTimestampMs()).isEqualTo(1_700_000_000_000L);
    assertThat(declare.getSettings().getMaxAttempts()).isEqualTo(3);
    assertThat(declare.getSettings().getMaxProcessingMs()).isEqualTo(45_000);
    assertThat(declare.getSettings().hasDeadLetterTopic()).isFalse();
    assertThat(declare.getSettings().getRedeliveryBackoffMs()).isEqualTo(250);
    assertThat(declare.getSettings().getRedeliveryBackoffMaxMs()).isEqualTo(90_000);
  }

  @Test
  void whatTheEntryLeavesUnsetIsLeftToTheDispatchersDefaults() throws Exception {
    FakeReplica replica = single();
    start(config());
    subscribed();
    Declare declare = replica.stream().declare();
    assertThat(declare.getSettings().getOrdering()).isEqualTo(Ordering.ORDERING_UNSPECIFIED);
    assertThat(declare.getSettings().getOffsetReset())
        .isEqualTo(OffsetReset.OFFSET_RESET_UNSPECIFIED);
    assertThat(declare.getSettings().hasStartTimestampMs()).isFalse();
    assertThat(declare.getSettings().getMaxAttempts()).isZero();
    assertThat(declare.getSettings().hasRedeliveryBackoffMs()).isFalse();
    assertThat(declare.getSettings().hasRedeliveryBackoffMaxMs()).isFalse();
    // The server's default deadline, as for a stream that reads Kafka.
    assertThat(declare.getSettings().getMaxProcessingMs()).isEqualTo(60_000);
  }

  @Test
  void aStreamMayNotMixSources() throws Exception {
    single();
    start(
        config()
            .putClusters("main", Cluster.getDefaultInstance())
            .putConsumers(
                "audit", Consumer.newBuilder().setCluster("main").setGroupId("billing").build()));
    App app = app();
    app.subscribe(0, "orders", "audit");
    Status status = app.status();
    assertThat(status.getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
    assertThat(status.getDescription())
        .contains("all be read from Kafka or all be received from the dispatcher")
        .contains("audit")
        .contains("orders");
  }

  // -------------------------------------------------------------------------------------------
  // Credits
  // -------------------------------------------------------------------------------------------

  @Test
  void withOneStreamTheAppsCreditsArePassedThrough() throws Exception {
    FakeReplica replica = single();
    start(config());
    App app = subscribed();
    Stream stream = replica.accepted();
    stream.nothing();

    app.credits(7);
    assertThat(stream.credits()).isEqualTo(7);
    app.credits(3);
    assertThat(stream.credits()).isEqualTo(3);
    replica.noStream();
  }

  @Test
  void creditsGrantedBeforeTheDispatcherAnswersAreKept() throws Exception {
    start(config());
    App app = subscribed();
    app.credits(5);
    // The dispatcher appears afterwards.
    FakeReplica replica = single();
    Stream stream = replica.accepted();
    assertThat(stream.credits()).isEqualTo(5);
    stream.records(0, 0, "a");
    assertThat(app.assigned().getPartition()).isZero();
    assertThat(values(app.batch())).containsExactly("a");
  }

  @Test
  void twoTopicsShareTheAppsCredits() throws Exception {
    FakeReplica replica = single();
    start(config());
    App app = app();
    app.subscribe(60_000, "orders", "refunds");
    assertThat(app.next().hasSubscribed()).isTrue();
    Stream first = replica.stream();
    Stream second = replica.stream();
    String firstTopic = first.declare().getTopic();
    assertThat(List.of(firstTopic, second.declare().getTopic()))
        .containsExactlyInAnyOrder("orders", "refunds");
    Stream orders = firstTopic.equals("orders") ? first : second;
    Stream refunds = orders == first ? second : first;

    app.credits(100);
    assertThat(orders.credits()).isEqualTo(1);
    assertThat(refunds.credits()).isEqualTo(1);

    // Records arrive for one topic: that is where the credit goes, a wider window each time.
    orders.records(0, 0, "a");
    assertThat(app.assigned().getTopic()).isEqualTo("orders");
    assertThat(app.batch().getTopic()).isEqualTo("orders");
    assertThat(orders.credits()).isEqualTo(2);
    orders.records(0, 1, "b", 2, "c");
    assertThat(offsets(app.batch())).containsExactly(1L, 2L);
    assertThat(orders.credits()).isEqualTo(4);
    refunds.nothing();

    refunds.records(3, 9, "r");
    PartitionAssignment assigned = app.assigned();
    assertThat(assigned.getTopic()).isEqualTo("refunds");
    assertThat(assigned.getPartition()).isEqualTo(3);
    assertThat(assigned.getAssignmentEpoch()).isEqualTo(2);
    assertThat(app.batch().getTopic()).isEqualTo("refunds");
    assertThat(refunds.credits()).isEqualTo(2);
  }

  @Test
  void recordsBeyondTheAppsCreditWaitForIt() throws Exception {
    FakeReplica replica = single();
    start(config());
    App app = app();
    app.subscribe(60_000, "orders", "refunds");
    assertThat(app.next().hasSubscribed()).isTrue();
    Stream first = replica.stream();
    Stream second = replica.stream();
    first.declare();
    second.declare();

    // One credit from the app, and both topics have a record: one is handed, one waits.
    app.credits(1);
    assertThat(first.credits()).isEqualTo(1);
    assertThat(second.credits()).isEqualTo(1);
    first.records(0, 0, "a");
    assertThat(app.assigned().getAssignmentEpoch()).isEqualTo(1);
    assertThat(app.batch().getRecordsCount()).isEqualTo(1);
    second.records(0, 0, "b");
    app.nothing();
    first.nothing();
    second.nothing();

    app.credits(1);
    assertThat(app.assigned().getAssignmentEpoch()).isEqualTo(2);
    assertThat(values(app.batch())).containsExactly("b");
    first.nothing();
    second.nothing();

    app.credits(1);
    assertThat(first.credits()).isEqualTo(1);
    assertThat(second.credits()).isEqualTo(1);
  }

  @Test
  void theHandOffToTheAppAndTheAppsAcknowledgementAreTimed() throws Exception {
    FakeReplica replica = single();
    start(config());
    App app = app();
    app.subscribe(60_000, "orders", "refunds");
    assertThat(app.next().hasSubscribed()).isTrue();
    // The two streams are opened together: which topic's is accepted first is not known.
    Stream one = replica.stream();
    Stream two = replica.stream();
    boolean ordersFirst = one.declare().getTopic().equals("orders");
    two.declare();
    Stream first = ordersFirst ? one : two;
    Stream second = ordersFirst ? two : one;

    app.credits(1);
    assertThat(first.credits()).isEqualTo(1);
    assertThat(second.credits()).isEqualTo(1);
    first.records(0, 0, "a");
    long orders = app.assigned().getAssignmentEpoch();
    assertThat(values(app.batch())).containsExactly("a");
    // Beyond the app's credit: it waits for the next. Arrived (and stamped) before the wait starts.
    second.records(0, 0, "b");
    waitFor("b arrived", () -> metric("odradek.receive.records", "topic", "refunds") == 1);
    app.nothing();
    app.credits(1);
    long refunds = app.assigned().getAssignmentEpoch();
    assertThat(values(app.batch())).containsExactly("b");
    app.ack("orders", 0, orders, 0, 0);
    assertThat(range(first.ack())).isEqualTo(List.of(0, 0L, 0L));
    app.ack("refunds", 0, refunds, 0, 0);
    assertThat(range(second.ack())).isEqualTo(List.of(0, 0L, 0L));

    HistogramPointData handedOrders = histogram("odradek.receive.handoff.duration", "orders");
    assertThat(handedOrders.getCount()).isEqualTo(1);
    HistogramPointData handedRefunds = histogram("odradek.receive.handoff.duration", "refunds");
    assertThat(handedRefunds.getCount()).isEqualTo(1);
    assertThat(handedRefunds.getMin()).isGreaterThanOrEqualTo(300);
    // "a" was held by the app while it had no credit for "b".
    HistogramPointData ackedOrders = histogram("odradek.receive.ack.duration", "orders");
    assertThat(ackedOrders.getCount()).isEqualTo(1);
    assertThat(ackedOrders.getMin()).isGreaterThanOrEqualTo(300);
    assertThat(histogram("odradek.receive.ack.duration", "refunds").getCount()).isEqualTo(1);

    // An acknowledgement of nothing held is not timed.
    app.ack("orders", 0, orders, 0, 0);
    first.nothing();
    assertThat(histogram("odradek.receive.ack.duration", "orders").getCount()).isEqualTo(1);
  }

  @Test
  void eachBatchIsTimedFromItsOwnWriteToTheApp() throws Exception {
    FakeReplica replica = single();
    start(config());
    App app = subscribed();
    Stream stream = replica.accepted();
    // Two batches wait for the app's credit, then go in one pump; the first write is slow.
    stream.records(0, 0, "a");
    stream.records(1, 0, "b");
    waitFor("both arrived", () -> metric("odradek.receive.records") == 2);
    slowWriteMs.set(300);
    long granted = System.nanoTime();
    app.credits(10);
    long zero = app.assigned().getAssignmentEpoch();
    assertThat(values(app.batch())).containsExactly("a");
    long one = app.assigned().getAssignmentEpoch();
    assertThat(values(app.batch())).containsExactly("b");
    assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - granted))
        .isGreaterThanOrEqualTo(300);
    // What the app can still take, passed on after both writes.
    assertThat(stream.credits()).isEqualTo(8);
    app.ack("orders", 1, one, 0, 0);
    assertThat(range(stream.ack())).isEqualTo(List.of(1, 0L, 0L));
    app.ack("orders", 0, zero, 0, 0);
    assertThat(range(stream.ack())).isEqualTo(List.of(0, 0L, 0L));

    // "b" was written after the slow write of "a": its hand-off took that long, and the app's
    // acknowledgement of it, sent at once, took less.
    HistogramPointData handed = histogram("odradek.receive.handoff.duration", "orders");
    assertThat(handed.getCount()).isEqualTo(2);
    assertThat(handed.getMax()).isGreaterThanOrEqualTo(300);
    HistogramPointData acked = histogram("odradek.receive.ack.duration", "orders");
    assertThat(acked.getCount()).isEqualTo(2);
    assertThat(acked.getMin()).isLessThan(300);
  }

  // -------------------------------------------------------------------------------------------
  // Records and acknowledgements
  // -------------------------------------------------------------------------------------------

  @Test
  void aPartitionIsAnnouncedBeforeItsFirstRecords() throws Exception {
    FakeReplica replica = single();
    start(config());
    App app = subscribed();
    Stream stream = replica.accepted();
    app.credits(10);
    stream.credits();
    app.nothing();

    stream.send(
        DeliverResponse.newBuilder()
            .setRecords(
                io.github.bpalermo.odradek.delivery.v1.RecordBatch.newBuilder()
                    .setPartition(2)
                    .addRecords(
                        Record.newBuilder()
                            .setOffset(41)
                            .setTimestampMs(1234)
                            .setKey(ByteString.copyFromUtf8("k"))
                            .setValue(ByteString.copyFromUtf8("v"))
                            .addHeaders(
                                io.github.bpalermo.odradek.kafka.v1.Header.newBuilder()
                                    .setKey("h")
                                    .setValue(ByteString.copyFromUtf8("x")))
                            .setAttempt(1))
                    .addRecords(Record.newBuilder().setOffset(44).setAttempt(2))));
    PartitionAssignment assigned = app.assigned();
    assertThat(assigned.getTopic()).isEqualTo("orders");
    assertThat(assigned.getPartition()).isEqualTo(2);
    assertThat(assigned.getAssignmentEpoch()).isEqualTo(1);

    RecordBatch batch = app.batch();
    assertThat(batch.getTopic()).isEqualTo("orders");
    assertThat(batch.getPartition()).isEqualTo(2);
    assertThat(batch.getAssignmentEpoch()).isEqualTo(1);
    assertThat(offsets(batch)).containsExactly(41L, 44L);
    ConsumedRecord record = batch.getRecords(0);
    assertThat(record.getTimestampMs()).isEqualTo(1234);
    assertThat(record.getKey().toStringUtf8()).isEqualTo("k");
    assertThat(record.getValue().toStringUtf8()).isEqualTo("v");
    assertThat(record.getHeadersCount()).isEqualTo(1);
    assertThat(record.getHeaders(0).getKey()).isEqualTo("h");
    assertThat(record.getHeaders(0).getValue().toStringUtf8()).isEqualTo("x");
    assertThat(record.hasDecodeError()).isFalse();
    // A null key and a tombstone stay what they are.
    assertThat(batch.getRecords(1).hasKey()).isFalse();
    assertThat(batch.getRecords(1).hasValue()).isFalse();

    // More of the same partition: no second announcement. Another partition: its own.
    stream.records(2, 50, "w");
    assertThat(app.batch().getAssignmentEpoch()).isEqualTo(1);
    stream.records(0, 7, "z");
    assertThat(app.assigned().getAssignmentEpoch()).isEqualTo(2);
    assertThat(app.batch().getAssignmentEpoch()).isEqualTo(2);
  }

  @Test
  void anAcknowledgementOverGapsCoversOnlyWhatWasDeliveredHere() throws Exception {
    FakeReplica replica = single();
    start(config());
    App app = subscribed();
    Stream stream = replica.accepted();
    app.credits(10);
    stream.credits();

    stream.records(0, 3, "a", 7, "b", 8, "c");
    long epoch = app.assigned().getAssignmentEpoch();
    assertThat(offsets(app.batch())).containsExactly(3L, 7L, 8L);
    // A record delivered again behind newer ones: a batch of its own, with a lower offset.
    stream.records(0, 5, "d", 12, "e");
    assertThat(offsets(app.batch())).containsExactly(5L, 12L);

    app.ack("orders", 0, epoch, 0, 10);
    assertThat(range(stream.ack())).isEqualTo(List.of(0, 3L, 3L));
    assertThat(range(stream.ack())).isEqualTo(List.of(0, 5L, 5L));
    assertThat(range(stream.ack())).isEqualTo(List.of(0, 7L, 8L));

    // Nothing of these is held: not acknowledged twice, not for another epoch or partition.
    app.ack("orders", 0, epoch, 0, 10);
    app.ack("orders", 0, epoch + 1, 12, 12);
    app.ack("orders", 1, epoch, 12, 12);
    app.ack("refunds", 0, epoch, 12, 12);
    stream.nothing();
    app.ack("orders", 0, epoch, 12, 12);
    assertThat(range(stream.ack())).isEqualTo(List.of(0, 12L, 12L));
  }

  private static List<Object> range(io.github.bpalermo.odradek.delivery.v1.Ack ack) {
    return List.of(ack.getPartition(), ack.getFirstOffset(), ack.getLastOffset());
  }

  @Test
  void aBatchWhoseOffsetsGoBackIsSplit() throws Exception {
    FakeReplica replica = single();
    start(config());
    App app = subscribed();
    Stream stream = replica.accepted();
    // The records wait here until the app grants, so two batches of the dispatcher meet.
    stream.records(0, 8, "a", 9, "b");
    stream.records(0, 4, "c");
    app.credits(10);
    app.assigned();
    assertThat(offsets(app.batch())).containsExactly(8L, 9L);
    assertThat(offsets(app.batch())).containsExactly(4L);
  }

  @Test
  void aReleasedPartitionIsLostToTheAppAndComesBackUnderANewEpoch() throws Exception {
    FakeReplica replica = single();
    start(config());
    App app = subscribed();
    Stream stream = replica.accepted();
    app.credits(10);
    stream.credits();
    stream.records(0, 1, "a", 2, "b");
    stream.records(1, 6, "c");
    long first = app.assigned().getAssignmentEpoch();
    app.batch();
    long other = app.assigned().getAssignmentEpoch();
    app.batch();

    // A partition the app holds nothing of is released with it: only the one it knows is lost.
    stream.released(0, 5);
    assertThat(app.lost()).containsExactly(partition("orders", 0));
    app.ack("orders", 0, first, 1, 2);
    stream.nothing();

    // The dispatcher delivers the records again; here, as it happens.
    stream.records(0, 1, "a");
    PartitionAssignment again = app.assigned();
    assertThat(again.getPartition()).isZero();
    assertThat(again.getAssignmentEpoch()).isGreaterThan(other);
    assertThat(app.batch().getAssignmentEpoch()).isEqualTo(again.getAssignmentEpoch());
    app.ack("orders", 0, first, 1, 1);
    stream.nothing();
    app.ack("orders", 0, again.getAssignmentEpoch(), 1, 1);
    assertThat(range(stream.ack())).isEqualTo(List.of(0, 1L, 1L));
    // The partition that was not released is as it was.
    app.ack("orders", 1, other, 6, 6);
    assertThat(range(stream.ack())).isEqualTo(List.of(1, 6L, 6L));
  }

  @Test
  void recordsOfAReleasedPartitionThatStillWaitAreDropped() throws Exception {
    FakeReplica replica = single();
    start(config());
    App app = subscribed();
    Stream stream = replica.accepted();
    stream.records(0, 1, "a");
    stream.records(1, 1, "b");
    stream.released(0);
    // The app's credit must not overtake the release, which comes on another stream. A record
    // behind it on the same stream is counted only once the release has been handled.
    stream.records(1, 2, "c");
    waitFor("the release was handled", () -> metric("odradek.receive.records") == 3);
    app.credits(10);
    assertThat(app.assigned().getPartition()).isEqualTo(1);
    assertThat(values(app.batch())).containsExactly("b", "c");
    app.nothing();
    // The place of the dropped record is the app's again: all ten credits, less the two used.
    assertThat(stream.credits()).isEqualTo(8);
  }

  // -------------------------------------------------------------------------------------------
  // Decoding
  // -------------------------------------------------------------------------------------------

  private static ServerConfig.Builder withSuffixCodec(OnDecodeError onDecodeError) {
    return config()
        .putCodecs(
            "suffix",
            Codec.newBuilder()
                .setValue(
                    Pipeline.newBuilder()
                        .addTransforms(
                            Transform.newBuilder().setClassName(SuffixTransform.class.getName())))
                .build())
        .putConsumers(
            "orders", dispatched().setCodec("suffix").setOnDecodeError(onDecodeError).build());
  }

  @Test
  void recordsAreDecodedByTheTopicsCodec() throws Exception {
    FakeReplica replica = single();
    start(withSuffixCodec(OnDecodeError.ON_DECODE_ERROR_UNSPECIFIED));
    App app = subscribed();
    Stream stream = replica.accepted();
    app.credits(10);
    stream.credits();
    stream.send(
        DeliverResponse.newBuilder()
            .setRecords(
                io.github.bpalermo.odradek.delivery.v1.RecordBatch.newBuilder()
                    .setPartition(0)
                    .addRecords(
                        Record.newBuilder()
                            .setOffset(0)
                            .setValue(ByteString.copyFromUtf8("hello!"))
                            .addHeaders(
                                io.github.bpalermo.odradek.kafka.v1.Header.newBuilder()
                                    .setKey("suffixed")
                                    .setValue(ByteString.copyFrom(new byte[] {1})))
                            .addHeaders(
                                io.github.bpalermo.odradek.kafka.v1.Header.newBuilder()
                                    .setKey("kept")))));
    app.assigned();
    ConsumedRecord record = app.batch().getRecords(0);
    assertThat(record.getValue().toStringUtf8()).isEqualTo("hello");
    // The transform removes its own header; one without a value stays without.
    assertThat(record.getHeadersCount()).isEqualTo(1);
    assertThat(record.getHeaders(0).getKey()).isEqualTo("kept");
    assertThat(record.getHeaders(0).hasValue()).isFalse();
  }

  @Test
  void aRecordThatCannotBeDecodedIsRejectedAndTheStreamStays() throws Exception {
    FakeReplica replica = single();
    start(withSuffixCodec(OnDecodeError.ON_DECODE_ERROR_FAIL));
    App app = subscribed();
    Stream stream = replica.accepted();
    app.credits(10);
    assertThat(stream.credits()).isEqualTo(10);

    stream.records(0, 0, "a!", 1, "not suffixed", 2, "c!");
    app.assigned();
    RecordBatch batch = app.batch();
    assertThat(offsets(batch)).containsExactly(0L, 2L);
    assertThat(values(batch)).containsExactly("a", "c");

    // The rejection goes back at once, before anything else is said: how long the record waits
    // before it is delivered again is the dispatcher's business. The credits the three records
    // used are the app's again but for the two it holds.
    DeliverRequest reject = stream.next();
    assertThat(reject.hasReject()).as("%s", reject).isTrue();
    assertThat(reject.getReject().getPartition()).isZero();
    assertThat(reject.getReject().getOffset()).isEqualTo(1);
    assertThat(reject.getReject().getReason()).contains("codec suffix value");
    assertThat(stream.credits()).isEqualTo(1);

    // The stream to the app is open, and so is the one to the dispatcher.
    app.ack("orders", 0, batch.getAssignmentEpoch(), 0, 2);
    assertThat(range(stream.ack())).isEqualTo(List.of(0, 0L, 0L));
    assertThat(range(stream.ack())).isEqualTo(List.of(0, 2L, 2L));
    assertThat(app.closed).isNotDone();
    assertThat(stream.ended).isNotDone();
  }

  @Test
  void aRecordThatCannotBeDecodedIsDeliveredUndecodedWhenTheEntrySaysSo() throws Exception {
    FakeReplica replica = single();
    start(withSuffixCodec(OnDecodeError.ON_DECODE_ERROR_DELIVER_UNDECODED));
    App app = subscribed();
    Stream stream = replica.accepted();
    app.credits(10);
    stream.credits();

    stream.records(0, 0, "a!", 1, "not suffixed", 2, "c!");
    app.assigned();
    RecordBatch batch = app.batch();
    assertThat(offsets(batch)).containsExactly(0L, 1L, 2L);
    assertThat(batch.getRecords(0).hasDecodeError()).isFalse();
    ConsumedRecord undecoded = batch.getRecords(1);
    assertThat(undecoded.getValue().toStringUtf8()).isEqualTo("not suffixed");
    assertThat(undecoded.getDecodeError()).contains("codec suffix value");

    // The app acknowledges it like any other record.
    app.ack("orders", 0, batch.getAssignmentEpoch(), 0, 2);
    assertThat(range(stream.ack())).isEqualTo(List.of(0, 0L, 2L));
    stream.nothing();
  }

  // -------------------------------------------------------------------------------------------
  // Replicas
  // -------------------------------------------------------------------------------------------

  @Test
  void aStreamIsOpenedToEveryListedReplica() throws Exception {
    // The Service address reaches the first replica; the list names two.
    FakeReplica one = new FakeReplica(r1);
    FakeReplica two = new FakeReplica(r2);
    serve(service, one);
    serve(r1, one);
    serve(r2, two);
    listed = list(1, r1, r2);
    start(config());
    App app = subscribed();
    Stream first = one.accepted();
    Stream second = two.accepted();
    // The stream through the Service address is the stream to the first replica: no other.
    one.noStream();
    assertThat(second.requests).isEmpty();

    app.credits(100);
    assertThat(first.credits()).isEqualTo(1);
    assertThat(second.credits()).isEqualTo(1);

    // Each replica owns some partitions; an acknowledgement goes back where its record came from.
    first.records(0, 4, "a");
    long epochOfZero = app.assigned().getAssignmentEpoch();
    app.batch();
    assertThat(first.credits()).isEqualTo(2);
    second.records(1, 9, "b");
    long epochOfOne = app.assigned().getAssignmentEpoch();
    app.batch();
    assertThat(second.credits()).isEqualTo(2);
    app.ack("orders", 1, epochOfOne, 9, 9);
    assertThat(range(second.ack())).isEqualTo(List.of(1, 9L, 9L));
    app.ack("orders", 0, epochOfZero, 4, 4);
    assertThat(range(first.ack())).isEqualTo(List.of(0, 4L, 4L));
    first.nothing();
    second.nothing();
  }

  @Test
  void aListedReplicaThatIsNotReachedShowsAsAStreamWantedAndNotOpen() throws Exception {
    // Two replicas are listed; nothing answers at the address of the second.
    FakeReplica one = new FakeReplica(r1);
    serve(service, one);
    serve(r1, one);
    listed = list(1, r1, r2);
    start(config());
    App app = subscribed();
    one.accepted();
    waitFor("two streams are wanted", () -> metric("odradek.receive.streams.wanted") == 2);
    assertThat(
            metric(
                "odradek.receive.streams.wanted",
                "cluster",
                "main",
                "group",
                "billing",
                "topic",
                "orders"))
        .isEqualTo(2);
    waitFor("it is tried again", () -> metric("odradek.receive.reconnects") >= 2);
    assertThat(metric("odradek.receive.streams")).isEqualTo(1);

    // It appears: both are open, and wanted.
    FakeReplica two = new FakeReplica(r2);
    serve(r2, two);
    two.accepted();
    waitFor("both streams are open", () -> metric("odradek.receive.streams") == 2);
    assertThat(metric("odradek.receive.streams.wanted")).isEqualTo(2);

    // And nothing is wanted once the app has gone.
    app.requests.onCompleted();
    assertThat(app.status()).isEqualTo(Status.OK);
    waitFor("nothing is wanted", () -> metric("odradek.receive.streams.wanted") == 0);
    assertThat(metric("odradek.receive.streams")).isZero();
  }

  private static void waitFor(String what, java.util.function.BooleanSupplier condition)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
      Thread.sleep(5);
    }
    assertThat(condition.getAsBoolean()).as(what).isTrue();
  }

  @Test
  void aNewerListOpensAndClosesStreamsAndAnOlderOneIsIgnored() throws Exception {
    FakeReplica one = new FakeReplica(r1);
    FakeReplica two = new FakeReplica(r2);
    FakeReplica three = new FakeReplica(r3);
    serve(service, one);
    serve(r1, one);
    serve(r2, two);
    serve(r3, three);
    listed = list(5, r1, r2);
    start(config());
    App app = subscribed();
    Stream first = one.accepted();
    Stream second = two.accepted();
    app.credits(100);
    first.credits();
    second.credits();
    first.records(0, 4, "a");
    long epoch = app.assigned().getAssignmentEpoch();
    app.batch();
    first.credits();

    // The first replica is replaced by a third. Any replica may be the one to say so.
    listed = list(6, r2, r3);
    second.replicas(listed);
    Stream third = three.accepted();
    assertThat(first.ended())
        .as("the stream to the replica that left is closed")
        .isEqualTo(Status.OK);
    // What the app held from it will be delivered again by whoever owns the partition now.
    assertThat(app.lost()).containsExactly(partition("orders", 0));
    app.ack("orders", 0, epoch, 4, 4);
    first.nothing();
    assertThat(third.credits()).isEqualTo(1);

    // The same list from the other replica, and an older one still on its way: nothing changes.
    third.replicas(listed);
    second.replicas(list(5, r1, r2));
    one.noStream();
    two.noStream();
    three.noStream();
    assertThat(second.ended).isNotDone();
    assertThat(third.ended).isNotDone();

    // The new owner delivers the record again: announced again, under a new epoch.
    third.records(0, 4, "a");
    assertThat(app.assigned().getAssignmentEpoch()).isGreaterThan(epoch);
    assertThat(offsets(app.batch())).containsExactly(4L);
  }

  @Test
  void aPartitionThatMovesBeforeItsOldOwnerSaysSoIsAnnouncedAgain() throws Exception {
    FakeReplica one = new FakeReplica(r1);
    FakeReplica two = new FakeReplica(r2);
    serve(service, one);
    serve(r1, one);
    serve(r2, two);
    listed = list(1, r1, r2);
    start(config());
    App app = subscribed();
    Stream first = one.accepted();
    Stream second = two.accepted();
    app.credits(100);
    first.credits();
    second.credits();
    first.records(0, 4, "a", 5, "b");
    long epoch = app.assigned().getAssignmentEpoch();
    app.batch();

    // The second replica owns the partition now, and its records overtake the first's Released.
    second.records(0, 4, "a");
    assertThat(app.lost()).containsExactly(partition("orders", 0));
    PartitionAssignment again = app.assigned();
    assertThat(again.getAssignmentEpoch()).isGreaterThan(epoch);
    assertThat(app.batch().getAssignmentEpoch()).isEqualTo(again.getAssignmentEpoch());
    first.released(0);
    app.nothing();

    // Only the new owner hears of an acknowledgement, and only under the new epoch.
    first.credits();
    second.credits();
    app.ack("orders", 0, epoch, 4, 5);
    app.ack("orders", 0, again.getAssignmentEpoch(), 4, 5);
    assertThat(range(second.ack())).isEqualTo(List.of(0, 4L, 4L));
    first.nothing();
  }

  @Test
  void aReplicaThatDoesNotSayWhichOneItIsGetsAStreamOfItsOwn() throws Exception {
    // What answers at the Service address gives no address: an older dispatcher, say.
    FakeReplica one = new FakeReplica(r1);
    one.says = null;
    serve(service, one);
    serve(r1, one);
    listed = list(1, r1);
    start(config());
    App app = subscribed();
    Stream through = one.accepted();
    Stream direct = one.accepted();
    assertThat(through.ended())
        .as("the stream through the Service is given up")
        .isEqualTo(Status.OK);
    app.credits(4);
    assertThat(direct.credits()).isEqualTo(4);
    assertThat(through.requests).isEmpty();
  }

  @Test
  void aLostStreamIsOpenedAgainAtItsReplica() throws Exception {
    FakeReplica one = new FakeReplica(r1);
    FakeReplica two = new FakeReplica(r2);
    serve(service, one);
    serve(r1, one);
    serve(r2, two);
    listed = list(1, r1, r2);
    start(config());
    App app = subscribed();
    Stream first = one.accepted();
    Stream second = two.accepted();
    app.credits(100);
    first.credits();
    second.credits();
    second.records(1, 9, "b");
    long epoch = app.assigned().getAssignmentEpoch();
    app.batch();
    second.credits();

    second.end(Status.UNAVAILABLE.withDescription("the dispatcher is stopping"));
    // The app's stream stays open; what it held from that replica is lost to it.
    assertThat(app.lost()).containsExactly(partition("orders", 1));
    Stream again = two.accepted();
    assertThat(again.credits()).isEqualTo(1);
    one.noStream();
    assertThat(first.ended).isNotDone();
    assertThat(app.closed).isNotDone();

    // An acknowledgement for what the lost stream delivered has nowhere to go.
    app.ack("orders", 1, epoch, 9, 9);
    again.nothing();
    first.nothing();
  }

  @Test
  void withEveryStreamLostItStartsAgainFromTheConfiguredAddress() throws Exception {
    FakeReplica one = new FakeReplica(r1);
    Server throughService = serve(service, one);
    Server own = serve(r1, one);
    listed = list(8, r1);
    start(config());
    App app = subscribed();
    Stream first = one.accepted();
    app.credits(10);
    assertThat(first.credits()).isEqualTo(10);

    // The dispatcher is replaced as a whole: a new replica at a new address, counting generations
    // from where it likes. The old address answers nothing.
    throughService.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    own.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    FakeReplica two = new FakeReplica(r2);
    listed = list(1, r2);
    serve(service, two);
    serve(r2, two);

    Stream second = two.accepted();
    assertThat(second.credits()).as("the app's credits are granted again").isEqualTo(10);
    two.noStream();
    assertThat(app.closed).isNotDone();
    second.records(0, 0, "a");
    app.assigned();
    assertThat(values(app.batch())).containsExactly("a");
  }

  @Test
  void theAppsStreamOpensBeforeTheDispatcherCanBeReached() throws Exception {
    start(config());
    App app = subscribed();
    app.credits(3);
    app.nothing();
    assertThat(app.closed).isNotDone();

    FakeReplica replica = single();
    Stream stream = replica.accepted();
    assertThat(stream.credits()).isEqualTo(3);
  }

  @Test
  void aDispatcherThatIsStartingIsAskedAgain() throws Exception {
    FakeReplica replica = single();
    replica.refusal = Status.UNAVAILABLE.withDescription("the dispatcher is starting or stopping");
    start(config());
    App app = subscribed();
    replica.stream();
    replica.stream();
    replica.refusal = null;
    // Some attempt after this one is accepted, and the app has noticed nothing.
    app.credits(2);
    Stream accepted = null;
    for (int attempt = 0; attempt < 20 && accepted == null; attempt++) {
      Stream stream = replica.stream();
      stream.declare();
      DeliverRequest next = stream.requests.poll(2, TimeUnit.SECONDS);
      if (next != null) {
        assertThat(next.getCredits().getRecords()).isEqualTo(2);
        accepted = stream;
      }
    }
    assertThat(accepted).isNotNull();
    assertThat(app.closed).isNotDone();
  }

  // -------------------------------------------------------------------------------------------
  // Failures the sidecar cannot repair
  // -------------------------------------------------------------------------------------------

  @Test
  void aDeclarationRefusedForGoodEndsTheAppsStreamWithWhatTheDispatcherSaid() throws Exception {
    FakeReplica replica = single();
    replica.refusal =
        Status.FAILED_PRECONDITION.withDescription(
            "group billing already consumes topic orders with other settings");
    start(config());
    App app = app();
    app.subscribe(60_000, "orders");
    assertThat(app.next().hasSubscribed()).isTrue();
    Status status = app.status();
    assertThat(status.getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
    assertThat(status.getDescription())
        .contains("the dispatcher at " + service + " refused topic orders")
        .contains("group billing")
        .contains("cluster main")
        .contains("already consumes topic orders with other settings");
    replica.stream();
    replica.noStream();
  }

  @Test
  void whichStatusesAreForGood() {
    for (Status.Code code : Status.Code.values()) {
      boolean permanent =
          switch (code) {
            case FAILED_PRECONDITION,
                PERMISSION_DENIED,
                UNAUTHENTICATED,
                INVALID_ARGUMENT,
                UNIMPLEMENTED ->
                true;
            default -> false;
          };
      assertThat(ReceiveSession.permanent(code)).as("%s", code).isEqualTo(permanent);
    }
  }

  @Test
  void theReasonOfALostStreamIsOneLineAndNamesWhatHappenedUnderneath() {
    // What gRPC makes of a TLS handshake the sidecar refused.
    Status handshake =
        Status.UNAVAILABLE
            .withDescription("io exception\nChannel Pipeline: [SslHandler#0, ClientTlsHandler#0]")
            .withCause(
                new RuntimeException(
                    "decoding",
                    new javax.net.ssl.SSLHandshakeException(
                        "the peer's SPIFFE ID spiffe://example.org/other is not accepted")));
    assertThat(ReceiveSession.reason(handshake))
        .isEqualTo(
            "io exception: SSLHandshakeException: the peer's SPIFFE ID spiffe://example.org/other"
                + " is not accepted");
    // What a dispatcher said, and nothing at all.
    assertThat(ReceiveSession.reason(Status.UNAVAILABLE.withDescription("stopping")))
        .isEqualTo("stopping");
    assertThat(ReceiveSession.reason(Status.UNAVAILABLE)).isEmpty();
    assertThat(ReceiveSession.reason(Status.UNAVAILABLE.withCause(new java.io.IOException())))
        .isEqualTo("IOException");
  }

  @Test
  void aStreamBrokenForGoodAfterItWasAcceptedEndsTheAppsStream() throws Exception {
    FakeReplica replica = single();
    start(config());
    App app = subscribed();
    Stream orders = replica.accepted();
    app.credits(5);
    orders.credits();

    // Broken after it was accepted: the sidecar's own doing, and no retry would change it.
    orders.end(Status.INVALID_ARGUMENT.withDescription("Declare was already sent"));
    Status status = app.status();
    assertThat(status.getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    assertThat(status.getDescription()).contains("Declare was already sent");
  }

  // -------------------------------------------------------------------------------------------
  // An app that stops acknowledging
  // -------------------------------------------------------------------------------------------

  @Test
  void anAppThatHoldsARecordPastItsDeadlineIsGrantedNothingUntilItAcknowledges() throws Exception {
    FakeReplica replica = single();
    start(config());
    App app = app();
    app.subscribe(400, "orders");
    assertThat(app.next().hasSubscribed()).isTrue();
    Stream stream = replica.accepted();
    app.credits(1);
    assertThat(stream.credits()).isEqualTo(1);
    stream.records(0, 0, "a");
    long epoch = app.assigned().getAssignmentEpoch();
    app.batch();

    // Past the deadline: the dispatcher will give the record to another pod. More credit from
    // the app is not passed on, and the stream to the app is not ended.
    Thread.sleep(600);
    app.credits(5);
    stream.nothing();
    assertThat(app.closed).isNotDone();

    // The late acknowledgement is still passed on, and credit flows again.
    app.ack("orders", 0, epoch, 0, 0);
    assertThat(range(stream.ack())).isEqualTo(List.of(0, 0L, 0L));
    assertThat(stream.credits()).isEqualTo(5);
  }

  // -------------------------------------------------------------------------------------------
  // The end
  // -------------------------------------------------------------------------------------------

  @Test
  void whenTheAppEndsItsStreamTheDispatcherIsLeftInGoodOrder() throws Exception {
    FakeReplica replica = single();
    start(config());
    App app = subscribed();
    Stream stream = replica.accepted();
    app.credits(10);
    stream.credits();
    stream.records(0, 0, "a", 1, "b");
    long epoch = app.assigned().getAssignmentEpoch();
    app.batch();

    // The last acknowledgement, then the half-close: both reach the dispatcher, in that order.
    app.ack("orders", 0, epoch, 0, 0);
    app.requests.onCompleted();
    assertThat(range(stream.ack())).isEqualTo(List.of(0, 0L, 0L));
    assertThat(stream.ended()).isEqualTo(Status.OK);
    assertThat(app.status()).isEqualTo(Status.OK);
    stream.nothing();
  }

  @Test
  void whenTheAppVanishesTheDispatcherIsLeftInGoodOrder() throws Exception {
    FakeReplica replica = single();
    start(config());
    App app = subscribed();
    Stream stream = replica.accepted();
    app.credits(10);
    stream.credits();
    app.requests.onError(Status.CANCELLED.asRuntimeException());
    assertThat(stream.ended()).isEqualTo(Status.OK);
  }

  @Test
  void whenTheServerStopsTheStreamsEndOnBothSides() throws Exception {
    FakeReplica replica = single();
    start(config());
    App app = subscribed();
    Stream stream = replica.accepted();
    app.credits(10);
    stream.credits();
    stream.records(0, 0, "a");
    long epoch = app.assigned().getAssignmentEpoch();
    app.batch();
    app.ack("orders", 0, epoch, 0, 0);
    assertThat(range(stream.ack())).isEqualTo(List.of(0, 0L, 0L));

    consume.shutdown(Duration.ofSeconds(10));
    assertThat(stream.ended()).isEqualTo(Status.OK);
    Status status = app.status();
    assertThat(status.getCode()).isEqualTo(Status.Code.UNAVAILABLE);
    assertThat(status.getDescription()).contains("shutting down");

    // And no new stream is taken.
    App late = app();
    late.subscribe(0, "orders");
    assertThat(late.status().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
  }

  @Test
  void aServerWithoutADispatcherRefusesSuchAStream() throws Exception {
    ReceiveService none =
        ReceiveService.create(
            ServerConfig.getDefaultInstance(), TlsSources.none(), ServerMetrics.noop());
    assertThat(ReceiveService.receives(ServerConfig.getDefaultInstance())).isFalse();
    assertThat(ReceiveService.receives(config().build())).isTrue();
    org.assertj.core.api.Assertions.assertThatThrownBy(() -> none.open(null))
        .isInstanceOf(io.grpc.StatusRuntimeException.class)
        .hasMessageContaining("no dispatcher to receive from");
    none.close();
  }
}
