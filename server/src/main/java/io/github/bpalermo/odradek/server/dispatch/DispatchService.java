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
import io.github.bpalermo.odradek.delivery.v1.ReplicaList;
import io.github.bpalermo.odradek.server.Version;
import io.github.bpalermo.odradek.server.consume.ConsumeService;
import io.github.bpalermo.odradek.server.dispatch.Declarations.Declared;
import io.github.bpalermo.odradek.server.dispatch.Declarations.Refused;
import io.github.bpalermo.odradek.server.dispatch.DispatchGroup.Key;
import io.github.bpalermo.odradek.server.dispatch.DispatchGroup.Stream;
import io.github.bpalermo.odradek.server.telemetry.ServerMetrics;
import io.github.bpalermo.odradek.server.tls.PeerPolicy;
import io.github.bpalermo.odradek.server.tls.TlsSources;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
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
 * <p>With {@code dispatch.tls}, a declaration is authorized against {@code dispatch.policy} by the
 * SPIFFE ID of the sidecar's client certificate, which {@link #identities()} takes from the
 * connection. <b>Without it the delivery port is plaintext and nobody has an identity</b>: the
 * policy is empty (the configuration is rejected otherwise) and every well-formed declaration that
 * names a cluster the dispatcher has is accepted. Such a dispatcher must not be reachable by
 * anything that is not trusted with every topic its Kafka credentials can read.
 */
public final class DispatchService extends DeliveryServiceGrpc.DeliveryServiceImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(DispatchService.class);

  /**
   * Who opened a stream, as far as its connection says.
   *
   * @param spiffeId the SPIFFE ID of the client certificate, or null when there is none to go by
   * @param problem why there is none, for the refusal
   */
  record Peer(String spiffeId, String problem) {}

  /** The peer of the call being started; unset when nothing looked at the connection. */
  static final Context.Key<Peer> PEER = Context.key("odradek-delivery-peer");

  private static final Peer PLAINTEXT = new Peer(null, "the connection has no TLS");

  /**
   * What the delivery listener puts in front of this service: it reads the client certificate the
   * TLS handshake verified and leaves its SPIFFE ID where {@link #deliver} finds it.
   */
  public static ServerInterceptor identities() {
    return new ServerInterceptor() {
      @Override
      public <RequestT, ResponseT> ServerCall.Listener<RequestT> interceptCall(
          ServerCall<RequestT, ResponseT> call,
          Metadata headers,
          ServerCallHandler<RequestT, ResponseT> next) {
        Peer peer = peer(call.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION));
        return Contexts.interceptCall(Context.current().withValue(PEER, peer), call, headers, next);
      }
    };
  }

  /** The identity a TLS session proves; {@code session} is null on a plaintext connection. */
  static Peer peer(SSLSession session) {
    if (session == null) {
      return PLAINTEXT;
    }
    try {
      if (session.getPeerCertificates()[0] instanceof X509Certificate leaf) {
        return new Peer(PeerPolicy.spiffeId(leaf), null);
      }
      return new Peer(null, "the client certificate is not an X.509 certificate");
    } catch (SSLPeerUnverifiedException e) {
      return new Peer(null, "the connection has no client certificate");
    } catch (CertificateException e) {
      return new Peer(null, e.getMessage());
    }
  }

  /** The port when the configuration does not name one. */
  public static final int DEFAULT_PORT = 9465;

  static final Duration DEFAULT_IDLE_GRACE = Duration.ofMinutes(5);
  static final long DEFAULT_PARTITION_WINDOW = 1000;
  static final Duration DEFAULT_REVOKE_GRACE = Duration.ofSeconds(10);
  static final Duration DEFAULT_COMMIT_INTERVAL = Duration.ofSeconds(5);

  /**
   * How long a dead letter whose write failed waits before it is written again: this the first
   * time, doubled for each further failure, up to the second. Not configuration: the producer has
   * already retried what a retry can mend, for its {@code delivery.timeout.ms}, before it reports a
   * failure, so what is left is a topic that is missing or may not be written, or a record it
   * refuses, and those are mended by an operator, not by a shorter wait.
   */
  static final Duration DEAD_LETTER_RETRY = Duration.ofSeconds(1);

  static final Duration DEAD_LETTER_RETRY_MAX = Duration.ofSeconds(30);

  /** How long {@link #shutdown} lets the dead-letter producers finish, after the groups have. */
  private static final Duration DEAD_LETTER_CLOSE = Duration.ofSeconds(5);

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
  private final DeadLetters deadLetters;
  private final LongSupplier nanoClock;
  private final long window;
  private final Duration idleGrace;
  private final Duration revokeGrace;
  private final Duration commitInterval;

  /** What sidecars are allowed; null when the delivery port has no TLS, and so no identities. */
  private final Policy policy;

  private final AtomicLong streamCounter = new AtomicLong();
  private final AtomicLong groupCounter = new AtomicLong();

  /** Guards {@link #groups} and {@link #stopping}, and the queueing of a declaration. */
  private final Object lock = new Object();

  private final Map<Key, DispatchGroup> groups = new HashMap<>();

  /** Every group whose thread may still run, for shutdown to wait for. */
  private final Map<DispatchGroup, Boolean> running = new ConcurrentHashMap<>();

  private boolean stopping;

  /** Where sidecars reach this replica; null until {@link #advertise}. Guarded by the lock. */
  private String advertised;

  /**
   * What every handshake answers, but for the settings: the newest list of replicas and this
   * replica's own address. Null until the first list is known ({@link #replicas}).
   */
  private volatile Accepted accepted;

  public DispatchService(ServerConfig config, TlsSources tls, ServerMetrics metrics) {
    this(
        config,
        tls,
        metrics,
        (cluster, properties) ->
            new KafkaConsumer<>(
                properties, new ByteArrayDeserializer(), new ByteArrayDeserializer()),
        new DeadLetters(config, tls, metrics),
        System::nanoTime);
  }

  DispatchService(
      ServerConfig config,
      TlsSources tls,
      ServerMetrics metrics,
      Factory factory,
      DeadLetters deadLetters,
      LongSupplier nanoClock) {
    this.config = config;
    this.tls = tls;
    this.metrics = metrics;
    this.factory = factory;
    this.deadLetters = deadLetters;
    this.nanoClock = nanoClock;
    Dispatch dispatch = config.getDispatch();
    if (!dispatch.hasTls() && dispatch.getPolicyCount() > 0) {
      // The configuration's own validation says the same; a policy must never be ignored.
      throw new IllegalArgumentException("dispatch.policy needs dispatch.tls");
    }
    this.policy = dispatch.hasTls() ? Policy.of(dispatch.getPolicyList()) : null;
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
   * Says where sidecars reach this replica, once its listener is bound: the address {@code
   * Accepted} gives as its own, which is also how the replica appears in the lists. Declarations
   * are still refused with UNAVAILABLE until the first list arrives ({@link #replicas}): a replica
   * that answered with itself alone would have its sidecars miss the partitions of the others.
   */
  public void advertise(String address) {
    synchronized (lock) {
      advertised = address;
    }
  }

  /**
   * The dispatcher's replicas changed, or are known for the first time: {@code list} is what the
   * coordination group settled on ({@link Membership}). From here on every handshake answers with
   * it, and it is sent on every open stream, so that each sidecar opens a stream to a replica that
   * started and closes those to one that is gone.
   */
  public void replicas(ReplicaList list) {
    List<DispatchGroup> open;
    synchronized (lock) {
      if (advertised == null) {
        throw new IllegalStateException("the replicas are known before this one's own address");
      }
      accepted =
          Accepted.newBuilder()
              .setReplicas(list)
              .setDispatcher(PeerInfo.newBuilder().setVersion(Version.VERSION))
              .setAddress(advertised)
              .setAcceptsReturn(true)
              .build();
      // After `accepted`, under the lock declarations are queued under: a stream that joins a
      // group before this is queued there is sent the list; one that joins after is answered
      // with it.
      open = List.copyOf(running.keySet());
      open.forEach(group -> group.submit(new DispatchGroup.Announce(list)));
    }
    LOG.info(
        "the dispatcher's replicas, generation {}: {}",
        Long.toUnsignedString(list.getGeneration()),
        ReplicaAssignor.addresses(list));
  }

  @Override
  public StreamObserver<DeliverRequest> deliver(StreamObserver<DeliverResponse> responses) {
    return new Handshake((ServerCallStreamObserver<DeliverResponse>) responses, PEER.get());
  }

  /** Whether declarations are authorized against the policy: the delivery port has TLS. */
  public boolean enforcesPolicy() {
    return policy != null;
  }

  /** Whether the policy, being enforced, has no rule: every declaration is then refused. */
  public boolean refusesEverything() {
    return policy != null && policy.isEmpty();
  }

  /**
   * Stops dispatching: no declaration is accepted any more, every group hands its partitions over
   * (it sends nothing new, finishes what it had held back below the newest record it sent, waits
   * for what is in flight and for the dead letters being written, for at most the revoke grace in
   * all, and commits), ends its streams with UNAVAILABLE and leaves its consumer group. Waits up to
   * {@code timeout} for that, and then closes the dead-letter producers: a dead letter that was not
   * acknowledged by then was not committed past, and its record is delivered again by the next
   * owner of its partition.
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
        break;
      }
    }
    deadLetters.close(DEAD_LETTER_CLOSE);
  }

  /**
   * How long {@link #shutdown} is to be given: the hand-over wait and the commits after it. The
   * dead-letter producers may take a few seconds more.
   */
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
            DEAD_LETTER_RETRY,
            DEAD_LETTER_RETRY_MAX,
            deadLetters,
            () -> accepted,
            nanoClock,
            System::currentTimeMillis);
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

    /** Who the connection says the sidecar is; null when nothing looked. */
    private final Peer peer;

    Handshake(ServerCallStreamObserver<DeliverResponse> call, Peer peer) {
      this.call = call;
      this.peer = peer;
      call.setOnCancelHandler(this::cancelled);
    }

    @Override
    public synchronized void onNext(DeliverRequest request) {
      if (done) {
        return;
      }
      if (stream == null) {
        // Before anything is said about the message: who cannot be told apart is told nothing.
        if (policy != null && (peer == null || peer.spiffeId() == null)) {
          refuse(
              Status.UNAUTHENTICATED.withDescription(
                  "no SPIFFE ID to authorize: "
                      + (peer == null ? PLAINTEXT.problem() : peer.problem())));
          return;
        }
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
        case RETURNED ->
            group.submit(
                new DispatchGroup.GiveBack(
                    stream,
                    request.getReturned().getPartition(),
                    request.getReturned().getOffsetsList()));
        case DECLARE -> fail(Status.INVALID_ARGUMENT.withDescription("Declare was already sent"));
        case MSG_NOT_SET -> fail(Status.INVALID_ARGUMENT.withDescription("empty DeliverRequest"));
      }
    }

    private void declare(DeliverRequest request) {
      Declared declared;
      try {
        declared =
            Declarations.check(
                request.getDeclare(),
                config.getClustersMap().keySet(),
                policy,
                policy == null ? null : peer.spiffeId());
      } catch (Refused e) {
        refuse(e.status());
        return;
      }
      Stream opened =
          new Stream(
              "s" + streamCounter.incrementAndGet(),
              call,
              declared,
              // What the group logs the stream with: who was let in is worth a line.
              request.getDeclare().getSidecar().getVersion()
                  + (policy == null ? "" : " as " + peer.spiffeId()));
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
      LOG.info(
          "declaration of {} refused: {} {}",
          peer == null || peer.spiffeId() == null ? "an unidentified sidecar" : peer.spiffeId(),
          status.getCode(),
          status.getDescription());
      metrics.declarationRefused(status.getCode().name());
      call.onError(status.asRuntimeException());
    }

    /** The sidecar broke the protocol after its declaration was accepted. */
    private void fail(Status status) {
      done = true;
      group.submit(new DispatchGroup.Leave(stream, status));
    }
  }
}
