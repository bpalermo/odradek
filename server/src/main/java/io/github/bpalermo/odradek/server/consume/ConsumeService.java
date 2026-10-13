package io.github.bpalermo.odradek.server.consume;

import com.google.protobuf.util.Durations;
import io.github.bpalermo.odradek.config.v1.Cluster;
import io.github.bpalermo.odradek.config.v1.Consumer;
import io.github.bpalermo.odradek.config.v1.ConsumerSource;
import io.github.bpalermo.odradek.config.v1.ServerConfig;
import io.github.bpalermo.odradek.kafka.v1.ConsumeRequest;
import io.github.bpalermo.odradek.kafka.v1.ConsumeResponse;
import io.github.bpalermo.odradek.kafka.v1.Subscribe;
import io.github.bpalermo.odradek.kafka.v1.Subscribed;
import io.github.bpalermo.odradek.server.codec.CodecRegistry;
import io.github.bpalermo.odradek.server.codec.RecordDecoder;
import io.github.bpalermo.odradek.server.telemetry.ServerMetrics;
import io.github.bpalermo.odradek.server.tls.TlsSources;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
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

/**
 * The Consume RPC. A stream whose topics the server reads from Kafka is one Kafka consumer on its
 * own poll thread; one whose topics come from a dispatcher is handed to the {@link
 * DispatcherSource}. A stream is one or the other.
 */
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
  private final TlsSources tls;
  private final ServerMetrics metrics;
  private final Factory factory;
  private final DispatcherSource dispatcher;
  private final String serverEpoch;
  private final LongSupplier nanoClock;
  private final Set<AppStream> sessions = ConcurrentHashMap.newKeySet();
  private final AtomicLong streamCounter = new AtomicLong();
  private final long defaultMaxProcessingMs;
  private final long maxMaxProcessingMs;
  private final long revokeDeadlineMs;
  private final long commitIntervalMs;
  private volatile boolean shuttingDown;

  /**
   * @param dispatcher serves the streams whose topics are received from a dispatcher
   */
  public ConsumeService(
      ServerConfig config,
      CodecRegistry codecs,
      TlsSources tls,
      String serverEpoch,
      ServerMetrics metrics,
      DispatcherSource dispatcher) {
    this(
        config,
        codecs,
        tls,
        metrics,
        serverEpoch,
        (cluster, properties) ->
            new KafkaConsumer<>(
                properties, new ByteArrayDeserializer(), new ByteArrayDeserializer()),
        dispatcher,
        System::nanoTime);
  }

  ConsumeService(
      ServerConfig config,
      CodecRegistry codecs,
      String serverEpoch,
      Factory factory,
      LongSupplier nanoClock) {
    this(
        config,
        codecs,
        TlsSources.none(),
        ServerMetrics.noop(),
        serverEpoch,
        factory,
        DispatcherSource.NONE,
        nanoClock);
  }

  ConsumeService(
      ServerConfig config,
      CodecRegistry codecs,
      TlsSources tls,
      ServerMetrics metrics,
      String serverEpoch,
      Factory factory,
      DispatcherSource dispatcher,
      LongSupplier nanoClock) {
    this.config = config;
    this.codecs = codecs;
    this.tls = tls;
    this.metrics = metrics;
    this.serverEpoch = serverEpoch;
    this.factory = factory;
    this.dispatcher = dispatcher;
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
   * Ends every stream: acked offsets are committed and the consumers leave their groups, the
   * streams to a dispatcher are closed in good order, and the app's streams close with UNAVAILABLE.
   * Waits up to {@code timeout} for that to finish.
   */
  public void shutdown(Duration timeout) {
    shuttingDown = true;
    List<AppStream> open = List.copyOf(sessions);
    open.forEach(AppStream::shutdown);
    long deadline = System.nanoTime() + timeout.toNanos();
    for (AppStream session : open) {
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
    private AppStream session;
    private boolean failed;

    Handshake(ServerCallStreamObserver<ConsumeResponse> call) {
      this.call = call;
      call.setOnCancelHandler(
          () -> {
            AppStream current;
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
          case CREDITS -> session.credits(request.getCredits().getRecords());
          case ACK -> session.ack(request.getAck());
          case REVOKE_DONE -> session.revokeDone(request.getRevokeDone().getRevokeId());
          case SUBSCRIBE ->
              session.fail(Status.INVALID_ARGUMENT.withDescription("Subscribe was already sent"));
          case MSG_NOT_SET ->
              session.fail(Status.INVALID_ARGUMENT.withDescription("empty ConsumeRequest"));
        }
        return;
      }
      if (!request.hasSubscribe()) {
        fail(Status.INVALID_ARGUMENT.withDescription("the first message must be Subscribe"));
        return;
      }
      try {
        subscribe(request.getSubscribe());
      } catch (StatusRuntimeException e) {
        fail(e.getStatus());
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
        } else if (dispatched(first) != dispatched(entry)) {
          // One stream has one source: its credits, its assignment epochs and its deadline mean
          // one thing. An app that wants both opens a stream for each.
          fail(
              Status.FAILED_PRECONDITION.withDescription(
                  "the topics of one stream must all be read from Kafka or all be received from"
                      + " the dispatcher; the source of "
                      + topic
                      + " differs from that of "
                      + topics.get(0)
                      + ". Subscribe to them on separate streams"));
          return;
        }
        entries.put(topic, entry);
      }
      String clusterName = first.getCluster();
      long maxProcessingMs =
          Math.min(
              subscribe.getMaxProcessingMs() == 0
                  ? defaultMaxProcessingMs
                  : subscribe.getMaxProcessingMs(),
              maxMaxProcessingMs);
      if (dispatched(first)) {
        receive(subscribe, topics, entries, first, maxProcessingMs);
        return;
      }
      Cluster cluster = config.getClustersOrThrow(clusterName);

      Map<String, RecordDecoder> decoders = new HashMap<>();
      org.apache.kafka.clients.consumer.Consumer<byte[], byte[]> consumer;
      try {
        for (String topic : topics) {
          decoders.put(topic, codecs.newDecoder(topic));
        }
        Map<String, Object> properties =
            properties(
                cluster,
                first.getGroupId(),
                "odradek-" + first.getGroupId() + "-" + streamCounter.incrementAndGet(),
                maxProcessingMs + revokeDeadlineMs + POLL_INTERVAL_MARGIN_MS);
        tls.apply(clusterName, properties);
        metrics.kafkaClients().applyToConsumer(clusterName, first.getGroupId(), topics, properties);
        consumer = factory.create(clusterName, properties);
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
              "odradek-consume-" + first.getGroupId() + "-" + streamCounter.get(),
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
      String group = first.getGroupId();
      metrics.consumeStreamOpened(clusterName, group);
      started.events(
          new ConsumeSession.Events() {
            @Override
            public void delivered(String topic, long records, long undecodable) {
              metrics.delivered(topic, records, undecodable);
            }

            @Override
            public void rebalanced(String event) {
              metrics.rebalanced(clusterName, group, event);
            }

            @Override
            public void ended(String reason) {
              metrics.consumeStreamClosed(clusterName, group, reason);
            }
          });
      sessions.add(started);
      session = started;
      started.start();
    }

    /** Starts a stream whose topics a dispatcher delivers: no Kafka consumer, no cluster here. */
    private void receive(
        Subscribe subscribe,
        List<String> topics,
        Map<String, Consumer> entries,
        Consumer first,
        long maxProcessingMs) {
      String clusterName = first.getCluster();
      String group = first.getGroupId();
      Map<String, RecordDecoder> decoders = new HashMap<>();
      AppStream started;
      try {
        for (String topic : topics) {
          decoders.put(topic, codecs.newDecoder(topic));
        }
        started =
            dispatcher.open(
                new DispatcherSource.Request(
                    call,
                    topics,
                    entries,
                    decoders,
                    maxProcessingMs,
                    new DispatcherSource.Listener() {
                      @Override
                      public void delivered(String topic, long records, long undecodable) {
                        metrics.delivered(topic, records, undecodable);
                      }

                      @Override
                      public void ended(AppStream stream, String reason) {
                        sessions.remove(stream);
                        metrics.consumeStreamClosed(clusterName, group, reason);
                      }
                    }));
      } catch (RuntimeException e) {
        decoders.values().forEach(RecordDecoder::close);
        throw e;
      }
      // What the consumer entry says, as for a stream that reads Kafka: the app cannot tell.
      call.onNext(
          ConsumeResponse.newBuilder()
              .setSubscribed(
                  Subscribed.newBuilder()
                      .setServerEpoch(serverEpoch)
                      .setMaxProcessingMs((int) maxProcessingMs)
                      .setCluster(clusterName)
                      .setGroupId(group))
              .build());
      LOG.info(
          "consume stream opened: topics {} group {} cluster {} client {} {}, received from the"
              + " dispatcher",
          topics,
          group,
          clusterName,
          subscribe.getClient().getLanguage(),
          subscribe.getClient().getVersion());
      metrics.consumeStreamOpened(clusterName, group);
      sessions.add(started);
      session = started;
      started.start();
    }

    @Override
    public void onCompleted() {
      AppStream current;
      synchronized (this) {
        current = session;
        if (current == null && !failed) {
          failed = true;
          call.onCompleted();
        }
      }
      if (current != null) {
        current.halfClose();
      }
    }

    @Override
    public void onError(Throwable t) {
      AppStream current;
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

  private static boolean dispatched(Consumer entry) {
    return entry.getSource() == ConsumerSource.CONSUMER_SOURCE_DISPATCHER;
  }

  /**
   * The configuration of a consumer the server creates: the cluster's shared and consumer
   * properties, with the settings the delivery guarantees rest on set last.
   *
   * @param clientId the client id, unless the cluster's properties set one
   * @param minPollIntervalMs the least {@code max.poll.interval.ms} the consumer's owner can live
   *     with: the longest it may go without polling, with a margin
   */
  public static Map<String, Object> properties(
      Cluster cluster, String groupId, String clientId, long minPollIntervalMs) {
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
    properties.putIfAbsent(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
    return properties;
  }
}
