package io.github.bpalermo.odradek.server.consume;

import com.google.protobuf.util.Durations;
import io.github.bpalermo.odradek.config.v1.Cluster;
import io.github.bpalermo.odradek.config.v1.Consumer;
import io.github.bpalermo.odradek.config.v1.ServerConfig;
import io.github.bpalermo.odradek.kafka.v1.ConsumeRequest;
import io.github.bpalermo.odradek.kafka.v1.ConsumeResponse;
import io.github.bpalermo.odradek.kafka.v1.Subscribe;
import io.github.bpalermo.odradek.kafka.v1.Subscribed;
import io.github.bpalermo.odradek.server.codec.CodecRegistry;
import io.github.bpalermo.odradek.server.codec.RecordDecoder;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Consume RPC: one Kafka consumer, on its own poll thread, per stream. */
public final class ConsumeService {
  private static final Logger LOG = LoggerFactory.getLogger(ConsumeService.class);

  static final long DEFAULT_MAX_PROCESSING_MS = Duration.ofMinutes(5).toMillis();
  static final long DEFAULT_MAX_MAX_PROCESSING_MS = Duration.ofMinutes(30).toMillis();
  static final long DEFAULT_REVOKE_DEADLINE_MS = Duration.ofSeconds(10).toMillis();
  static final long DEFAULT_COMMIT_INTERVAL_MS = Duration.ofSeconds(5).toMillis();

  /** How far max.poll.interval.ms is kept above the server's own hung-app deadline. */
  private static final long POLL_INTERVAL_MARGIN_MS = Duration.ofSeconds(30).toMillis();

  /** Creates the Kafka consumer of one stream. Tests substitute a mock. */
  @FunctionalInterface
  public interface Factory {
    org.apache.kafka.clients.consumer.Consumer<byte[], byte[]> create(
        String cluster, Map<String, Object> properties);
  }

  private final ServerConfig config;
  private final CodecRegistry codecs;
  private final Factory factory;
  private final String serverEpoch;
  private final LongSupplier nanoClock;
  private final Set<ConsumeSession> sessions = ConcurrentHashMap.newKeySet();
  private final AtomicLong streamCounter = new AtomicLong();
  private final long defaultMaxProcessingMs;
  private final long maxMaxProcessingMs;
  private final long revokeDeadlineMs;
  private final long commitIntervalMs;
  private volatile boolean shuttingDown;

  public ConsumeService(ServerConfig config, CodecRegistry codecs, String serverEpoch) {
    this(
        config,
        codecs,
        serverEpoch,
        (cluster, properties) ->
            new KafkaConsumer<>(
                properties, new ByteArrayDeserializer(), new ByteArrayDeserializer()),
        System::nanoTime);
  }

  ConsumeService(
      ServerConfig config,
      CodecRegistry codecs,
      String serverEpoch,
      Factory factory,
      LongSupplier nanoClock) {
    this.config = config;
    this.codecs = codecs;
    this.serverEpoch = serverEpoch;
    this.factory = factory;
    this.nanoClock = nanoClock;
    var consume = config.getConsume();
    this.defaultMaxProcessingMs =
        consume.hasDefaultMaxProcessing()
            ? Durations.toMillis(consume.getDefaultMaxProcessing())
            : DEFAULT_MAX_PROCESSING_MS;
    this.maxMaxProcessingMs =
        consume.hasMaxMaxProcessing()
            ? Durations.toMillis(consume.getMaxMaxProcessing())
            : DEFAULT_MAX_MAX_PROCESSING_MS;
    this.revokeDeadlineMs =
        consume.hasRevokeDeadline()
            ? Durations.toMillis(consume.getRevokeDeadline())
            : DEFAULT_REVOKE_DEADLINE_MS;
    this.commitIntervalMs =
        consume.hasCommitInterval()
            ? Durations.toMillis(consume.getCommitInterval())
            : DEFAULT_COMMIT_INTERVAL_MS;
  }

  /** Handles one Consume stream. */
  public StreamObserver<ConsumeRequest> consume(StreamObserver<ConsumeResponse> responses) {
    ServerCallStreamObserver<ConsumeResponse> call =
        (ServerCallStreamObserver<ConsumeResponse>) responses;
    return new Handshake(call);
  }

  /**
   * Ends every stream: acked offsets are committed, the consumers leave their groups, and the
   * streams close with UNAVAILABLE. Waits up to {@code timeout} for that to finish.
   */
  public void shutdown(Duration timeout) {
    shuttingDown = true;
    List<ConsumeSession> open = List.copyOf(sessions);
    open.forEach(session -> session.submit(new ConsumeSession.Shutdown()));
    long deadline = System.nanoTime() + timeout.toNanos();
    for (ConsumeSession session : open) {
      try {
        session.join(Math.max(1, (deadline - System.nanoTime()) / 1_000_000));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  /** The stream before its first message: validates Subscribe and starts the session. */
  private final class Handshake implements StreamObserver<ConsumeRequest> {
    private final ServerCallStreamObserver<ConsumeResponse> call;
    private ConsumeSession session;
    private boolean failed;

    Handshake(ServerCallStreamObserver<ConsumeResponse> call) {
      this.call = call;
      call.setOnCancelHandler(
          () -> {
            ConsumeSession current;
            synchronized (this) {
              failed = true;
              current = session;
            }
            if (current != null) {
              current.cancelled();
            }
          });
    }

    @Override
    public synchronized void onNext(ConsumeRequest request) {
      if (failed) {
        return;
      }
      if (session != null) {
        switch (request.getMsgCase()) {
          case CREDITS ->
              session.submit(new ConsumeSession.Credit(request.getCredits().getRecords()));
          case ACK -> session.submit(new ConsumeSession.Acked(request.getAck()));
          case REVOKE_DONE ->
              session.submit(new ConsumeSession.RevokeDone(request.getRevokeDone().getRevokeId()));
          case SUBSCRIBE ->
              session.submit(
                  new ConsumeSession.Fail(
                      Status.INVALID_ARGUMENT.withDescription("Subscribe was already sent")));
          case MSG_NOT_SET ->
              session.submit(
                  new ConsumeSession.Fail(
                      Status.INVALID_ARGUMENT.withDescription("empty ConsumeRequest")));
        }
        return;
      }
      if (!request.hasSubscribe()) {
        fail(Status.INVALID_ARGUMENT.withDescription("the first message must be Subscribe"));
        return;
      }
      try {
        subscribe(request.getSubscribe());
      } catch (RuntimeException e) {
        LOG.error("could not start a consume stream", e);
        fail(Status.INTERNAL.withDescription(e.toString()));
      }
    }

    private void subscribe(Subscribe subscribe) {
      if (shuttingDown) {
        fail(Status.UNAVAILABLE.withDescription("the server is shutting down"));
        return;
      }
      List<String> topics = subscribe.getTopicsList().stream().distinct().toList();
      if (topics.isEmpty()) {
        fail(Status.INVALID_ARGUMENT.withDescription("Subscribe.topics is empty"));
        return;
      }
      Map<String, Consumer> entries = new HashMap<>();
      Consumer first = null;
      for (String topic : topics) {
        Consumer entry = config.getConsumersMap().get(topic);
        if (entry == null) {
          fail(
              Status.NOT_FOUND.withDescription("topic " + topic + " has no entry under consumers"));
          return;
        }
        if (first == null) {
          first = entry;
        } else if (!first.getCluster().equals(entry.getCluster())
            || !first.getGroupId().equals(entry.getGroupId())) {
          fail(
              Status.FAILED_PRECONDITION.withDescription(
                  "the topics of one stream must share a cluster and a group; "
                      + topic
                      + " does not match "
                      + topics.get(0)));
          return;
        }
        entries.put(topic, entry);
      }
      String clusterName = first.getCluster();
      Cluster cluster = config.getClustersOrThrow(clusterName);
      if (cluster.hasTls()) {
        fail(
            Status.FAILED_PRECONDITION.withDescription(
                "clusters."
                    + clusterName
                    + ".tls is set, and mTLS to brokers is not implemented yet"));
        return;
      }
      long maxProcessingMs =
          Math.min(
              subscribe.getMaxProcessingMs() == 0
                  ? defaultMaxProcessingMs
                  : subscribe.getMaxProcessingMs(),
              maxMaxProcessingMs);

      Map<String, RecordDecoder> decoders = new HashMap<>();
      org.apache.kafka.clients.consumer.Consumer<byte[], byte[]> consumer;
      try {
        for (String topic : topics) {
          decoders.put(topic, codecs.newDecoder(topic));
        }
        consumer =
            factory.create(
                clusterName,
                properties(
                    cluster,
                    first.getGroupId(),
                    streamCounter.incrementAndGet(),
                    maxProcessingMs + revokeDeadlineMs + POLL_INTERVAL_MARGIN_MS));
      } catch (RuntimeException e) {
        decoders.values().forEach(RecordDecoder::close);
        throw e;
      }

      call.onNext(
          ConsumeResponse.newBuilder()
              .setSubscribed(
                  Subscribed.newBuilder()
                      .setServerEpoch(serverEpoch)
                      .setMaxProcessingMs((int) maxProcessingMs)
                      .setCluster(clusterName)
                      .setGroupId(first.getGroupId()))
              .build());
      ConsumeSession started =
          new ConsumeSession(
              call,
              consumer,
              topics,
              entries,
              decoders,
              Duration.ofMillis(maxProcessingMs),
              revokeDeadlineMs,
              Duration.ofMillis(commitIntervalMs),
              nanoClock,
              sessions::remove);
      LOG.info(
          "consume stream opened: topics {} group {} cluster {} client {} {}",
          topics,
          first.getGroupId(),
          clusterName,
          subscribe.getClient().getLanguage(),
          subscribe.getClient().getVersion());
      sessions.add(started);
      session = started;
      started.start("odradek-consume-" + first.getGroupId() + "-" + streamCounter.get());
    }

    @Override
    public void onCompleted() {
      ConsumeSession current;
      synchronized (this) {
        current = session;
        if (current == null && !failed) {
          failed = true;
          call.onCompleted();
        }
      }
      if (current != null) {
        current.submit(new ConsumeSession.HalfClose());
      }
    }

    @Override
    public void onError(Throwable t) {
      ConsumeSession current;
      synchronized (this) {
        failed = true;
        current = session;
      }
      if (current != null) {
        current.cancelled();
      }
    }

    private void fail(Status status) {
      failed = true;
      call.onError(status.asRuntimeException());
    }
  }

  /**
   * The consumer configuration of a stream: the cluster's shared and consumer properties, with the
   * settings the delivery guarantees rest on set last.
   */
  static Map<String, Object> properties(
      Cluster cluster, String groupId, long stream, long minPollIntervalMs) {
    Map<String, Object> properties = new HashMap<>();
    properties.putAll(cluster.getPropertiesMap());
    properties.putAll(cluster.getConsumerPropertiesMap());
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    // The server positions a partition without a committed offset itself. "none" makes a
    // partition it missed fail loudly instead of starting somewhere by default.
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
    properties.put(
        ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
        CooperativeStickyAssignor.class.getName());
    // Kafka's own eviction of a stuck consumer must never fire before the server's.
    long configured = 0;
    Object pollInterval = properties.get(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG);
    if (pollInterval != null) {
      configured = Long.parseLong(pollInterval.toString());
    }
    properties.put(
        ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,
        String.valueOf(Math.min(Integer.MAX_VALUE, Math.max(configured, minPollIntervalMs))));
    properties.putIfAbsent(ConsumerConfig.CLIENT_ID_CONFIG, "odradek-" + groupId + "-" + stream);
    return properties;
  }
}
