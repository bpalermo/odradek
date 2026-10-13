package io.github.bpalermo.odradek.server.dispatch;

import com.google.protobuf.util.Durations;
import io.github.bpalermo.odradek.config.v1.Cluster;
import io.github.bpalermo.odradek.config.v1.Dispatch;
import io.github.bpalermo.odradek.config.v1.ServerConfig;
import io.github.bpalermo.odradek.delivery.v1.Accepted;
import io.github.bpalermo.odradek.delivery.v1.DeliverRequest;
import io.github.bpalermo.odradek.delivery.v1.DeliverResponse;
import io.github.bpalermo.odradek.delivery.v1.DeliveryServiceGrpc;
import io.github.bpalermo.odradek.delivery.v1.PeerInfo;
import io.github.bpalermo.odradek.delivery.v1.Replica;
import io.github.bpalermo.odradek.delivery.v1.ReplicaList;
import io.github.bpalermo.odradek.server.Version;
import io.github.bpalermo.odradek.server.consume.ConsumeService;
import io.github.bpalermo.odradek.server.dispatch.Declarations.Declared;
import io.github.bpalermo.odradek.server.dispatch.Declarations.Refused;
import io.github.bpalermo.odradek.server.dispatch.DispatchGroup.Key;
import io.github.bpalermo.odradek.server.dispatch.DispatchGroup.Stream;
import io.github.bpalermo.odradek.server.telemetry.ServerMetrics;
import io.github.bpalermo.odradek.server.tls.TlsSources;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code DeliveryService}: what makes a server a dispatcher (ADR 6).
 *
 * <p>It checks each declaration and hands the stream to the {@link DispatchGroup} of its cluster
 * and consumer group, creating that group, and with it a Kafka consumer, when this is the first
 * declaration for it. Nothing here touches Kafka or a core: a gRPC thread validates and queues.
 *
 * <p><b>The policy is not enforced yet.</b> Without TLS on the delivery port a sidecar has no
 * identity to check, so every well-formed declaration that names a cluster the dispatcher has is
 * accepted. Until that changes (step 5 of the ADR's plan) a dispatcher must not be reachable by
 * anything that is not trusted with every topic its Kafka credentials can read.
 */
public final class DispatchService extends DeliveryServiceGrpc.DeliveryServiceImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(DispatchService.class);

  /** The port when the configuration does not name one. */
  public static final int DEFAULT_PORT = 9465;

  static final Duration DEFAULT_IDLE_GRACE = Duration.ofMinutes(5);
  static final long DEFAULT_PARTITION_WINDOW = 1000;
  static final Duration DEFAULT_REVOKE_GRACE = Duration.ofSeconds(10);
  static final Duration DEFAULT_COMMIT_INTERVAL = Duration.ofSeconds(5);

  /**
   * How much of a partition may be fetched and waiting for a first delivery before fetching it
   * pauses, in records and in bytes; it resumes at half. Not configuration: what bounds memory and
   * redelivery is {@code dispatch.partitionWindow}, and how much one fetch brings is Kafka's own
   * {@code max.partition.fetch.bytes} and {@code max.poll.records}, which a cluster's properties
   * set. This only has to keep a fetch or two ahead of the sidecars.
   */
  static final int MAX_BUFFERED_RECORDS = 1000;

  static final long MAX_BUFFERED_BYTES = 4L << 20;

  /** How far max.poll.interval.ms is kept above the longest the poll thread can stand still. */
  private static final long POLL_INTERVAL_MARGIN_MS = Duration.ofSeconds(30).toMillis();

  /** Creates the Kafka consumer of one group. Tests substitute a mock. */
  @FunctionalInterface
  public interface Factory {
    Consumer<byte[], byte[]> create(String cluster, Map<String, Object> properties);
  }

  private final ServerConfig config;
  private final TlsSources tls;
  private final ServerMetrics metrics;
  private final Factory factory;
  private final LongSupplier nanoClock;
  private final long window;
  private final Duration idleGrace;
  private final Duration revokeGrace;
  private final Duration commitInterval;
  private final AtomicLong streamCounter = new AtomicLong();
  private final AtomicLong groupCounter = new AtomicLong();

  /** Guards {@link #groups} and {@link #stopping}, and the queueing of a declaration. */
  private final Object lock = new Object();

  private final Map<Key, DispatchGroup> groups = new HashMap<>();

  /** Every group whose thread may still run, for shutdown to wait for. */
  private final Map<DispatchGroup, Boolean> running = new ConcurrentHashMap<>();

  private boolean stopping;

  /** What every handshake answers, but for the settings; null until {@link #advertise}. */
  private volatile Accepted accepted;

  public DispatchService(ServerConfig config, TlsSources tls, ServerMetrics metrics) {
    this(
        config,
        tls,
        metrics,
        (cluster, properties) ->
            new KafkaConsumer<>(
                properties, new ByteArrayDeserializer(), new ByteArrayDeserializer()),
        System::nanoTime);
  }

  DispatchService(
      ServerConfig config,
      TlsSources tls,
      ServerMetrics metrics,
      Factory factory,
      LongSupplier nanoClock) {
    this.config = config;
    this.tls = tls;
    this.metrics = metrics;
    this.factory = factory;
    this.nanoClock = nanoClock;
    Dispatch dispatch = config.getDispatch();
    this.window =
        dispatch.hasPartitionWindow() ? dispatch.getPartitionWindow() : DEFAULT_PARTITION_WINDOW;
    this.idleGrace =
        dispatch.hasIdleGrace()
            ? Duration.ofMillis(Durations.toMillis(dispatch.getIdleGrace()))
            : DEFAULT_IDLE_GRACE;
    var consume = config.getConsume();
    this.revokeGrace =
        consume.hasRevokeDeadline()
            ? Duration.ofMillis(Durations.toMillis(consume.getRevokeDeadline()))
            : DEFAULT_REVOKE_GRACE;
    this.commitInterval =
        consume.hasCommitInterval()
            ? Duration.ofMillis(Durations.toMillis(consume.getCommitInterval()))
            : DEFAULT_COMMIT_INTERVAL;
  }

  /**
   * Says where sidecars reach this replica, once its listener is bound: what {@code Accepted} tells
   * them. The list is this replica alone, under one generation that never changes, until the
   * replicas know of each other (step 6 of the ADR's plan). Declarations are refused with
   * UNAVAILABLE until this is called.
   */
  public void advertise(String address) {
    accepted =
        Accepted.newBuilder()
            .setReplicas(
                ReplicaList.newBuilder()
                    .setGeneration(1)
                    .addReplicas(Replica.newBuilder().setAddress(address)))
            .setDispatcher(PeerInfo.newBuilder().setVersion(Version.VERSION))
            .setAddress(address)
            .build();
  }

  @Override
  public StreamObserver<DeliverRequest> deliver(StreamObserver<DeliverResponse> responses) {
    return new Handshake((ServerCallStreamObserver<DeliverResponse>) responses);
  }

  /**
   * Stops dispatching: no declaration is accepted any more, every group hands its partitions over
   * (it waits for what is in flight, for at most the revoke grace, and commits), ends its streams
   * with UNAVAILABLE and leaves its consumer group. Waits up to {@code timeout} for that.
   */
  public void shutdown(Duration timeout) {
    List<DispatchGroup> open;
    synchronized (lock) {
      stopping = true;
      open = List.copyOf(running.keySet());
    }
    open.forEach(group -> group.submit(new DispatchGroup.Shutdown()));
    long deadline = System.nanoTime() + timeout.toNanos();
    for (DispatchGroup group : open) {
      try {
        group.join(Math.max(1, (deadline - System.nanoTime()) / 1_000_000));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  /** How long {@link #shutdown} needs at most: the hand-over wait and the commits after it. */
  public Duration shutdownTimeout() {
    return revokeGrace.plusSeconds(15);
  }

  /**
   * Queues a declaration for the group it names, creating the group if this is its first.
   *
   * @return the group, or null when the dispatcher is stopping or has not started
   */
  private DispatchGroup join(Stream stream) {
    Key key = new Key(stream.declared.cluster(), stream.declared.group());
    synchronized (lock) {
      if (stopping || accepted == null) {
        return null;
      }
      DispatchGroup group = groups.get(key);
      if (group == null) {
        group = create(key);
        groups.put(key, group);
        running.put(group, true);
        group.start();
      }
      // Under the lock: a group that finds itself without topics stops only if nothing is queued.
      group.submit(new DispatchGroup.Join(stream));
      return group;
    }
  }

  private DispatchGroup create(Key key) {
    Cluster cluster = config.getClustersOrThrow(key.cluster());
    long number = groupCounter.incrementAndGet();
    Map<String, String> topicsByMetricTag = new ConcurrentHashMap<>();
    DispatchGroup.Options options =
        new DispatchGroup.Options(
            window,
            MAX_BUFFERED_RECORDS,
            MAX_BUFFERED_BYTES,
            revokeGrace,
            idleGrace,
            commitInterval,
            () -> accepted,
            nanoClock);
    return new DispatchGroup(
        key,
        options,
        () -> {
          Map<String, Object> properties = properties(cluster, key.group(), number, revokeGrace);
          tls.apply(key.cluster(), properties);
          metrics
              .kafkaClients()
              .applyToConsumer(key.cluster(), key.group(), topicsByMetricTag, properties);
          return factory.create(key.cluster(), properties);
        },
        topicsByMetricTag,
        metrics,
        new DispatchGroup.Owner() {
          @Override
          public boolean retire(DispatchGroup group) {
            synchronized (lock) {
              if (!group.idle()) {
                return false;
              }
              groups.remove(group.key(), group);
              return true;
            }
          }

          @Override
          public void gone(DispatchGroup group) {
            synchronized (lock) {
              groups.remove(group.key(), group);
            }
          }

          @Override
          public void stopped(DispatchGroup group) {
            running.remove(group);
          }
        });
  }

  /**
   * The configuration of a group's consumer: that of every consumer the server creates, and no
   * topic created because a sidecar named it.
   */
  static Map<String, Object> properties(
      Cluster cluster, String group, long number, Duration revokeGrace) {
    // The poll thread stands still only while partitions are handed over.
    Map<String, Object> properties =
        ConsumeService.properties(
            cluster,
            group,
            "odradek-dispatch-" + group + "-" + number,
            revokeGrace.toMillis() + POLL_INTERVAL_MARGIN_MS);
    properties.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
    return properties;
  }

  /** One Deliver stream as gRPC sees it: checks the declaration, then only queues. */
  private final class Handshake implements StreamObserver<DeliverRequest> {
    private final ServerCallStreamObserver<DeliverResponse> call;
    private Stream stream;
    private DispatchGroup group;

    /** Nothing more is expected from the sidecar, or wanted. */
    private boolean done;

    Handshake(ServerCallStreamObserver<DeliverResponse> call) {
      this.call = call;
      call.setOnCancelHandler(this::cancelled);
    }

    @Override
    public synchronized void onNext(DeliverRequest request) {
      if (done) {
        return;
      }
      if (stream == null) {
        if (!request.hasDeclare()) {
          refuse(Status.INVALID_ARGUMENT.withDescription("the first message must be Declare"));
          return;
        }
        declare(request);
        return;
      }
      switch (request.getMsgCase()) {
        case CREDITS ->
            group.submit(new DispatchGroup.Credit(stream, request.getCredits().getRecords()));
        case ACK ->
            group.submit(
                new DispatchGroup.Acked(
                    stream,
                    request.getAck().getPartition(),
                    request.getAck().getFirstOffset(),
                    request.getAck().getLastOffset()));
        case REJECT ->
            group.submit(
                new DispatchGroup.Reject(
                    stream,
                    request.getReject().getPartition(),
                    request.getReject().getOffset(),
                    request.getReject().getReason()));
        case DECLARE -> fail(Status.INVALID_ARGUMENT.withDescription("Declare was already sent"));
        case MSG_NOT_SET -> fail(Status.INVALID_ARGUMENT.withDescription("empty DeliverRequest"));
      }
    }

    private void declare(DeliverRequest request) {
      Declared declared;
      try {
        declared = Declarations.check(request.getDeclare(), config.getClustersMap().keySet());
      } catch (Refused e) {
        refuse(e.status());
        return;
      }
      Stream opened =
          new Stream(
              "s" + streamCounter.incrementAndGet(),
              call,
              declared,
              request.getDeclare().getSidecar().getVersion());
      DispatchGroup joined = join(opened);
      if (joined == null) {
        refuse(Status.UNAVAILABLE.withDescription("the dispatcher is starting or stopping"));
        return;
      }
      // From here the group's thread is the only one that writes to the stream.
      stream = opened;
      group = joined;
    }

    @Override
    public synchronized void onCompleted() {
      if (done) {
        return;
      }
      done = true;
      if (stream == null) {
        call.onCompleted();
      } else {
        group.submit(new DispatchGroup.Leave(stream, Status.OK));
      }
    }

    @Override
    public void onError(Throwable t) {
      cancelled();
    }

    private synchronized void cancelled() {
      done = true;
      if (stream != null && !stream.cancelled) {
        stream.cancelled = true;
        group.submit(new DispatchGroup.Leave(stream, null));
      }
    }

    /** Ends a stream that never reached a group. */
    private void refuse(Status status) {
      done = true;
      LOG.info("declaration refused: {} {}", status.getCode(), status.getDescription());
      call.onError(status.asRuntimeException());
    }

    /** The sidecar broke the protocol after its declaration was accepted. */
    private void fail(Status status) {
      done = true;
      group.submit(new DispatchGroup.Leave(stream, status));
    }
  }
}
