package io.github.bpalermo.odradek.server.dispatch;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import io.github.bpalermo.odradek.delivery.v1.Accepted;
import io.github.bpalermo.odradek.delivery.v1.DeliverResponse;
import io.github.bpalermo.odradek.delivery.v1.OffsetReset;
import io.github.bpalermo.odradek.delivery.v1.Record;
import io.github.bpalermo.odradek.delivery.v1.Settings;
import io.github.bpalermo.odradek.kafka.v1.Header;
import io.github.bpalermo.odradek.server.dispatch.Declarations.Declared;
import io.github.bpalermo.odradek.server.dispatch.DispatchAction.Commit;
import io.github.bpalermo.odradek.server.dispatch.DispatchAction.DeadLetter;
import io.github.bpalermo.odradek.server.dispatch.DispatchAction.Deliver;
import io.github.bpalermo.odradek.server.dispatch.DispatchAction.Drop;
import io.github.bpalermo.odradek.server.dispatch.DispatchAction.PartitionBlocked;
import io.github.bpalermo.odradek.server.dispatch.DispatchAction.PartitionReleased;
import io.github.bpalermo.odradek.server.dispatch.DispatchAction.PartitionUnblocked;
import io.github.bpalermo.odradek.server.dispatch.DispatchAction.PauseFetching;
import io.github.bpalermo.odradek.server.dispatch.DispatchAction.ResumeFetching;
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.Acknowledged;
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.CreditsGranted;
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.Fetched;
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.PartitionAssigned;
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.PartitionsLost;
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.PartitionsRevoked;
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.ReceiverJoined;
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.ReceiverLeft;
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.RecordsFetched;
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.TimePassed;
import io.github.bpalermo.odradek.server.telemetry.KafkaClientMetrics;
import io.github.bpalermo.odradek.server.telemetry.ServerMetrics;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.InvalidOffsetException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One consumer group on one cluster, as a dispatcher consumes it: a Kafka consumer, the thread that
 * owns it, one {@link DispatchCore} for each topic sidecars have declared, and the {@code Deliver}
 * streams of those sidecars.
 *
 * <p>Everything here but {@link #submit} runs on that thread: the consumer, the cores and every
 * message written to a stream after its handshake. gRPC threads only queue commands. The thread
 * never waits for a sidecar: it waits for a command, for at most a few milliseconds, and polls the
 * consumer without blocking, so the consumer is polled however slow the sidecars are.
 *
 * <p>The group exists while it has a topic. A topic is added by the first declaration for it, and
 * removed when it has had no stream for the idle grace; the consumer's subscription follows.
 */
final class DispatchGroup implements ConsumerRebalanceListener {
  private static final Logger LOG = LoggerFactory.getLogger(DispatchGroup.class);

  /** The longest the thread waits for a command when nothing is happening. */
  private static final long MAX_IDLE_WAIT_MS = 10;

  /** At most one line per topic in this time about records that sidecars reject. */
  private static final long REJECT_LOG_NANOS = TimeUnit.SECONDS.toNanos(10);

  /** The largest {@code RecordBatch}: a gRPC peer accepts 4 MiB by default. */
  static final int MAX_BATCH_RECORDS = 500;

  static final long MAX_BATCH_BYTES = 1 << 20;

  /** A group is one consumer group on one cluster. */
  record Key(String cluster, String group) {}

  /** What every group of a dispatcher shares. */
  record Options(
      long window,
      int maxBufferedRecords,
      long maxBufferedBytes,
      Duration revokeGrace,
      Duration idleGrace,
      Duration commitInterval,
      Supplier<Accepted> accepted,
      LongSupplier nanoClock) {}

  /** Who a group answers to: the service that created it. */
  interface Owner {
    /**
     * The group has no topic left. Returns whether it may stop: false when a declaration for it is
     * already queued, in which case it carries on.
     */
    boolean retire(DispatchGroup group);

    /** The group stops, whatever is queued for it; no declaration reaches it after this returns. */
    void gone(DispatchGroup group);

    /** The group has left its consumer group and its thread is about to end. */
    void stopped(DispatchGroup group);
  }

  /** One {@code Deliver} stream: a receiver of its topic's core once it is accepted. */
  static final class Stream {
    final String id;
    final ServerCallStreamObserver<DeliverResponse> call;
    final Declared declared;
    final String sidecar;

    /** Set by gRPC when the sidecar is gone: nothing more can be written. */
    volatile boolean cancelled;

    /** The topic it receives, between its acceptance and its end. Group thread only. */
    private Topic topic;

    private boolean ended;

    Stream(
        String id,
        ServerCallStreamObserver<DeliverResponse> call,
        Declared declared,
        String sidecar) {
      this.id = id;
      this.call = call;
      this.declared = declared;
      this.sidecar = sidecar;
    }
  }

  /** Something a stream, or the dispatcher, asks of the group's thread. */
  sealed interface Command permits Join, Credit, Acked, Reject, Leave, Shutdown {}

  /** A declaration that is well formed and names this group. */
  record Join(Stream stream) implements Command {}

  record Credit(Stream stream, long records) implements Command {}

  record Acked(Stream stream, int partition, long firstOffset, long lastOffset)
      implements Command {}

  record Reject(Stream stream, int partition, long offset, String reason) implements Command {}

  /**
   * The stream ends: with {@code status} when the dispatcher is to end it, and with null when the
   * sidecar already has.
   */
  record Leave(Stream stream, Status status) implements Command {}

  /** The dispatcher is stopping. */
  record Shutdown() implements Command {}

  /** A fetched record as its core carries it. */
  private static final class Held {
    final ConsumerRecord<byte[], byte[]> record;
    boolean sent;

    Held(ConsumerRecord<byte[], byte[]> record) {
      this.record = record;
    }
  }

  /** One declared topic. */
  private final class Topic {
    final String name;
    final ServerMetrics.DispatchTopic metrics;
    final Map<String, Stream> receivers = new LinkedHashMap<>();
    final Set<Integer> blocked = new HashSet<>();
    final Batches<Held> batches;

    /** The settings in force, with the processing deadline of the latest declaration. */
    Settings settings;

    DispatchCore<Held> core;

    /** When the last receiver left. */
    long idleSince;

    /** Partitions of it that the consumer has may not be the core's yet. */
    boolean adopt = true;

    int reportedInFlight;
    long rejects;
    long lastRejectLog;

    Topic(Declared declared) {
      name = declared.topic();
      metrics = DispatchGroup.this.metrics.dispatchTopic(key.cluster(), key.group(), name);
      batches =
          new Batches<>(
              DispatchGroup::toRecord,
              (receiver, response) -> {
                Stream stream = receivers.get(receiver);
                if (stream != null) {
                  send(stream, response);
                }
              },
              MAX_BATCH_RECORDS,
              MAX_BATCH_BYTES);
      start(declared);
      lastRejectLog = options.nanoClock().getAsLong() - REJECT_LOG_NANOS;
    }

    /** Starts consuming as {@code declared} says, with nothing known of any partition. */
    void start(Declared declared) {
      settings = declared.settings();
      core =
          new DispatchCore<>(
              declared.toCore(
                  options.window(),
                  options.maxBufferedRecords(),
                  options.maxBufferedBytes(),
                  options.revokeGrace()));
      adopt = true;
    }
  }

  /** A partition a core has. */
  private static final class Owned {
    /** The highest commit position known. */
    long toCommit;

    /** The last position handed to a commit; -1 when the group has none, or a commit failed. */
    long committed;

    Owned(long toCommit, long committed) {
      this.toCommit = toCommit;
      this.committed = committed;
    }
  }

  private final Key key;
  private final Options options;
  private final Supplier<Consumer<byte[], byte[]>> factory;
  private final Map<String, String> topicsByMetricTag;
  private final ServerMetrics metrics;
  private final Owner owner;
  private final LinkedBlockingQueue<Command> commands = new LinkedBlockingQueue<>();
  private final Thread thread;
  private final long revokeGraceNanos;
  private final long idleGraceNanos;
  private final long commitIntervalNanos;

  private final Map<String, Topic> topics = new LinkedHashMap<>();
  private final Map<TopicPartition, Owned> owned = new HashMap<>();

  /** The owned partitions whose cores asked for fetching to stop. */
  private final Set<TopicPartition> full = new HashSet<>();

  /** The partitions being handed over that their cores have not released yet. */
  private final Set<TopicPartition> releasing = new HashSet<>();

  private Consumer<byte[], byte[]> consumer;
  private boolean subscriptionChanged;
  private boolean pausesChanged;
  private boolean stopping;
  private long lastCommitNanos;

  /**
   * @param factory creates the group's Kafka consumer, on the group's thread
   * @param topicsByMetricTag given to the consumer's metrics: the group adds each topic it
   *     subscribes to
   */
  DispatchGroup(
      Key key,
      Options options,
      Supplier<Consumer<byte[], byte[]>> factory,
      Map<String, String> topicsByMetricTag,
      ServerMetrics metrics,
      Owner owner) {
    this.key = key;
    this.options = options;
    this.factory = factory;
    this.topicsByMetricTag = topicsByMetricTag;
    this.metrics = metrics;
    this.owner = owner;
    this.revokeGraceNanos = options.revokeGrace().toNanos();
    this.idleGraceNanos = options.idleGrace().toNanos();
    this.commitIntervalNanos = options.commitInterval().toNanos();
    this.thread = new Thread(this::run, "odradek-dispatch-" + key.cluster() + "-" + key.group());
    this.thread.setDaemon(true);
  }

  Key key() {
    return key;
  }

  void start() {
    thread.start();
  }

  /** Queues a command. The only method other threads call while the group runs. */
  void submit(Command command) {
    commands.add(command);
  }

  /** Whether nothing is queued; the owner asks under the lock it queues declarations under. */
  boolean idle() {
    return commands.isEmpty();
  }

  void join(long millis) throws InterruptedException {
    thread.join(millis);
  }

  // ---------------------------------------------------------------------------------------------
  // The thread
  // ---------------------------------------------------------------------------------------------

  private void run() {
    Status end = Status.UNAVAILABLE.withDescription("the dispatcher is stopping");
    try {
      consumer = factory.get();
      loop();
    } catch (RuntimeException e) {
      LOG.error(
          "dispatching group {} on cluster {} failed; its streams end and their sidecars reconnect",
          key.group(),
          key.cluster(),
          e);
      end =
          Status.UNAVAILABLE.withDescription(
              "the dispatcher cannot consume for group " + key.group() + ": " + e);
    }
    try {
      close(end);
    } finally {
      owner.stopped(this);
    }
  }

  /** Runs until the dispatcher stops or the group has no topic left. */
  private void loop() {
    lastCommitNanos = now();
    long wait = 0;
    while (true) {
      boolean busy = drain(wait);
      if (stopping) {
        return;
      }
      long now = now();
      expireIdleTopics(now);
      if (topics.isEmpty()) {
        if (owner.retire(this)) {
          LOG.info(
              "group {} on cluster {} has no topic left: leaving it", key.group(), key.cluster());
          return;
        }
        // A declaration is queued.
        wait = 0;
        continue;
      }
      if (subscriptionChanged) {
        subscriptionChanged = false;
        LOG.info(
            "group {} on cluster {} subscribes to {}", key.group(), key.cluster(), topics.keySet());
        consumer.subscribe(Set.copyOf(topics.keySet()), this);
      }
      adoptPending();
      if (pausesChanged) {
        pausesChanged = false;
        applyPauses();
      }

      ConsumerRecords<byte[], byte[]> records;
      try {
        // Never blocks: the wait is on the command queue, where an acknowledgement is acted on
        // at once and not at the end of a poll.
        records = consumer.poll(Duration.ZERO);
      } catch (InvalidOffsetException e) {
        // A committed offset that retention has since removed.
        e.partitions().forEach(this::reposition);
        continue;
      }
      for (TopicPartition partition : records.partitions()) {
        fetched(partition, records.records(partition));
      }
      now = now();
      passTime(now);
      if (now - lastCommitNanos >= commitIntervalNanos) {
        lastCommitNanos = now;
        commitAsync();
      }
      reportInFlight();
      busy |= !records.isEmpty();
      wait = busy ? 0 : Math.min(Math.max(1, wait * 2), MAX_IDLE_WAIT_MS);
    }
  }

  private long now() {
    return options.nanoClock().getAsLong();
  }

  /** Handles what is queued, waiting up to {@code waitMs} for the first; whether anything was. */
  private boolean drain(long waitMs) {
    Command command = waitMs > 0 ? await(TimeUnit.MILLISECONDS.toNanos(waitMs)) : commands.poll();
    boolean any = command != null;
    while (command != null) {
      handle(command);
      command = commands.poll();
    }
    return any;
  }

  private Command await(long nanos) {
    try {
      return commands.poll(nanos, TimeUnit.NANOSECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return new Shutdown();
    }
  }

  private void handle(Command command) {
    switch (command) {
      case Join join -> join(join.stream());
      case Credit credit -> {
        Topic topic = credit.stream().topic;
        if (topic != null && credit.records() > 0) {
          on(topic, new CreditsGranted<>(credit.stream().id, credit.records()));
        }
      }
      case Acked acked -> {
        Topic topic = acked.stream().topic;
        if (topic != null) {
          on(
              topic,
              new Acknowledged<>(
                  acked.stream().id, acked.partition(), acked.firstOffset(), acked.lastOffset()));
        }
      }
      case Reject reject -> rejected(reject);
      case Leave leave -> {
        left(leave.stream());
        end(leave.stream(), leave.status());
      }
      case Shutdown shutdown -> stopping = true;
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Streams
  // ---------------------------------------------------------------------------------------------

  private void join(Stream stream) {
    if (stream.cancelled) {
      return;
    }
    if (stopping) {
      end(stream, Status.UNAVAILABLE.withDescription("the dispatcher is stopping"));
      return;
    }
    Declared declared = stream.declared;
    Topic topic = topics.get(declared.topic());
    if (topic == null) {
      topic = new Topic(declared);
      topics.put(topic.name, topic);
      topicsByMetricTag.put(KafkaClientMetrics.metricTag(topic.name), topic.name);
      subscriptionChanged = true;
      LOG.info(
          "group {} on cluster {} starts consuming {}: {}",
          key.group(),
          key.cluster(),
          topic.name,
          Declarations.describe(topic.settings));
    } else if (!declared.agreesWith(topic.settings)) {
      if (!topic.receivers.isEmpty()) {
        end(
            stream,
            Status.FAILED_PRECONDITION.withDescription(
                "group "
                    + key.group()
                    + " already consumes topic "
                    + topic.name
                    + " on cluster "
                    + key.cluster()
                    + " with other settings, declared by a sidecar that is still connected. In"
                    + " force: "
                    + Declarations.describe(topic.settings)
                    + ". Declared: "
                    + Declarations.describe(declared.settings())));
        return;
      }
      // Nobody holds the topic with the old settings: the new ones take over, from what is
      // committed.
      LOG.info(
          "group {} on cluster {} consumes {} with new settings: {}",
          key.group(),
          key.cluster(),
          topic.name,
          Declarations.describe(declared.settings()));
      handOver(partitionsOf(topic));
      forgetBlocked(topic);
      topic.start(declared);
    }
    if (topic.settings.getMaxProcessingMs() != declared.settings().getMaxProcessingMs()) {
      // The one setting that follows the latest declaration; see Declared.agreesWith.
      topic.core.maxProcessing(declared.maxProcessing());
      topic.settings =
          topic.settings.toBuilder()
              .setMaxProcessingMs(declared.settings().getMaxProcessingMs())
              .build();
    }
    topic.receivers.put(stream.id, stream);
    stream.topic = topic;
    topic.metrics.receivers(1);
    LOG.info(
        "deliver stream {} opened: topic {} group {} cluster {} sidecar {}",
        stream.id,
        topic.name,
        key.group(),
        key.cluster(),
        stream.sidecar);
    send(
        stream,
        DeliverResponse.newBuilder()
            .setAccepted(options.accepted().get().toBuilder().setSettings(topic.settings))
            .build());
    on(topic, new ReceiverJoined<>(stream.id));
  }

  /** The stream is no longer a receiver. What it held is delivered again. */
  private void left(Stream stream) {
    Topic topic = stream.topic;
    if (topic == null) {
      return;
    }
    stream.topic = null;
    topic.receivers.remove(stream.id);
    topic.metrics.receivers(-1);
    on(topic, new ReceiverLeft<>(stream.id));
    if (topic.receivers.isEmpty()) {
      topic.idleSince = now();
    }
    LOG.info(
        "deliver stream {} ended: topic {} group {} cluster {}",
        stream.id,
        topic.name,
        key.group(),
        key.cluster());
  }

  private void rejected(Reject reject) {
    Topic topic = reject.stream().topic;
    if (topic == null) {
      return;
    }
    topic.rejects++;
    long now = now();
    if (now - topic.lastRejectLog >= REJECT_LOG_NANOS) {
      LOG.warn(
          "{} record(s) of topic {} rejected by sidecars of group {}; the last, {}/{} by {}: {}",
          topic.rejects,
          topic.name,
          key.group(),
          reject.partition(),
          reject.offset(),
          reject.stream().sidecar,
          reject.reason());
      topic.rejects = 0;
      topic.lastRejectLog = now;
    }
    on(
        topic,
        new DispatchEvent.Rejected<>(reject.stream().id, reject.partition(), reject.offset()));
  }

  private void send(Stream stream, DeliverResponse response) {
    if (!stream.cancelled && !stream.ended) {
      stream.call.onNext(response);
    }
  }

  /** Ends a stream from this side, unless the sidecar already has. */
  private void end(Stream stream, Status status) {
    if (stream.ended) {
      return;
    }
    stream.ended = true;
    if (stream.cancelled || status == null) {
      return;
    }
    try {
      if (status.isOk()) {
        stream.call.onCompleted();
      } else {
        stream.call.onError(status.asRuntimeException());
      }
    } catch (IllegalStateException e) {
      // Cancelled in between.
    }
  }

  // ---------------------------------------------------------------------------------------------
  // The cores
  // ---------------------------------------------------------------------------------------------

  private void on(Topic topic, DispatchEvent<Held> event) {
    apply(topic, topic.core.handle(event, now()));
  }

  /** Carries out what a core asked for, in order. */
  private void apply(Topic topic, List<DispatchAction<Held>> actions) {
    for (DispatchAction<Held> action : actions) {
      switch (action) {
        case Deliver<Held> deliver -> {
          topic.metrics.sent(deliver.payload().sent);
          deliver.payload().sent = true;
          topic.batches.deliver(deliver);
        }
        case Drop<Held> drop -> topic.batches.released(drop);
        case Commit<Held> commit -> {
          Owned partition = owned.get(new TopicPartition(topic.name, commit.partition()));
          if (partition != null) {
            partition.toCommit = Math.max(partition.toCommit, commit.offset());
          }
        }
        case PauseFetching<Held> pause -> {
          full.add(new TopicPartition(topic.name, pause.partition()));
          pausesChanged = true;
        }
        case ResumeFetching<Held> resume -> {
          full.remove(new TopicPartition(topic.name, resume.partition()));
          pausesChanged = true;
        }
        case PartitionBlocked<Held> blocked -> {
          if (topic.blocked.add(blocked.partition())) {
            topic.metrics.blocked(1);
          }
          LOG.warn(
              "{}-{} of group {} is blocked: the record at offset {} used all its {} attempts."
                  + " Nothing after it is delivered until the partition changes owner",
              topic.name,
              blocked.partition(),
              key.group(),
              blocked.offset(),
              topic.settings.getMaxAttempts());
        }
        case PartitionUnblocked<Held> unblocked -> {
          if (topic.blocked.remove(unblocked.partition())) {
            topic.metrics.blocked(-1);
          }
          LOG.info(
              "{}-{} of group {} is no longer blocked",
              topic.name,
              unblocked.partition(),
              key.group());
        }
        case PartitionReleased<Held> released ->
            releasing.remove(new TopicPartition(topic.name, released.partition()));
        case DeadLetter<Held> deadLetter ->
            // Declarations asking for it are refused, so no core is set up to.
            throw new IllegalStateException("dead-lettering is not implemented: " + deadLetter);
      }
    }
    topic.batches.flush();
  }

  /** A record as a sidecar receives it: the bytes stored in Kafka, untouched. */
  private static Record toRecord(Held held, int attempt) {
    ConsumerRecord<byte[], byte[]> record = held.record;
    Record.Builder out =
        Record.newBuilder()
            .setOffset(record.offset())
            .setTimestampMs(record.timestamp())
            .setAttempt(attempt);
    // Nothing writes to the arrays of a fetched record, so they need not be copied.
    if (record.key() != null) {
      out.setKey(UnsafeByteOperations.unsafeWrap(record.key()));
    }
    if (record.value() != null) {
      out.setValue(UnsafeByteOperations.unsafeWrap(record.value()));
    }
    record
        .headers()
        .forEach(
            header -> {
              Header.Builder h = Header.newBuilder().setKey(header.key());
              if (header.value() != null) {
                h.setValue(UnsafeByteOperations.unsafeWrap(header.value()));
              }
              out.addHeaders(h);
            });
    return out.build();
  }

  private void fetched(TopicPartition partition, List<ConsumerRecord<byte[], byte[]>> records) {
    Topic topic = topics.get(partition.topic());
    if (topic == null || !owned.containsKey(partition)) {
      // Of a topic nobody declares any more, until the consumer gives its partitions up. Whoever
      // takes the partition, this group included, starts from what is committed.
      return;
    }
    List<Fetched<Held>> fetched = new ArrayList<>(records.size());
    for (ConsumerRecord<byte[], byte[]> record : records) {
      ByteString key = record.key() == null ? null : UnsafeByteOperations.unsafeWrap(record.key());
      int size =
          Math.max(0, record.serializedKeySize()) + Math.max(0, record.serializedValueSize());
      fetched.add(new Fetched<>(record.offset(), key, size, new Held(record)));
    }
    on(topic, new RecordsFetched<>(partition.partition(), fetched));
  }

  /** Tells the cores that have a deadline due that time has passed. */
  private void passTime(long now) {
    for (Topic topic : topics.values()) {
      OptionalLong next = topic.core.nextDeadlineNanos();
      if (next.isPresent() && now - next.getAsLong() >= 0) {
        apply(topic, topic.core.handle(new TimePassed<>(), now));
      }
    }
  }

  private void reportInFlight() {
    for (Topic topic : topics.values()) {
      int inFlight = topic.core.inFlight();
      if (inFlight != topic.reportedInFlight) {
        topic.metrics.inFlight(inFlight - topic.reportedInFlight);
        topic.reportedInFlight = inFlight;
      }
    }
  }

  private void forgetBlocked(Topic topic) {
    topic.metrics.blocked(-topic.blocked.size());
    topic.blocked.clear();
  }

  // ---------------------------------------------------------------------------------------------
  // Topics
  // ---------------------------------------------------------------------------------------------

  /** Stops consuming the topics that have had no stream for the idle grace. */
  private void expireIdleTopics(long now) {
    List<Topic> expired = null;
    for (Topic topic : topics.values()) {
      if (topic.receivers.isEmpty() && now - topic.idleSince >= idleGraceNanos) {
        if (expired == null) {
          expired = new ArrayList<>();
        }
        expired.add(topic);
      }
    }
    if (expired == null) {
      return;
    }
    for (Topic topic : expired) {
      LOG.info(
          "group {} on cluster {} stops consuming {}: no sidecar has declared it for {}",
          key.group(),
          key.cluster(),
          topic.name,
          options.idleGrace());
      // Commits where the topic stands. The consumer gives the partitions up at its next
      // rebalance, which the new subscription asks for; until then they are fetched by nobody.
      handOver(partitionsOf(topic));
      forgetBlocked(topic);
      topic.metrics.inFlight(-topic.reportedInFlight);
      topics.remove(topic.name);
      subscriptionChanged = true;
      pausesChanged = true;
    }
  }

  private List<TopicPartition> partitionsOf(Topic topic) {
    List<TopicPartition> partitions = new ArrayList<>();
    for (TopicPartition partition : owned.keySet()) {
      if (partition.topic().equals(topic.name)) {
        partitions.add(partition);
      }
    }
    return partitions;
  }

  // ---------------------------------------------------------------------------------------------
  // Partitions
  // ---------------------------------------------------------------------------------------------

  /**
   * Gives the cores of topics that ask for it the partitions the consumer has and they have not: a
   * topic declared again before the consumer gave up the partitions it had, or declared with new
   * settings.
   */
  private void adoptPending() {
    List<TopicPartition> pending = null;
    for (Topic topic : topics.values()) {
      if (!topic.adopt) {
        continue;
      }
      topic.adopt = false;
      for (TopicPartition partition : consumer.assignment()) {
        if (partition.topic().equals(topic.name) && !owned.containsKey(partition)) {
          if (pending == null) {
            pending = new ArrayList<>();
          }
          pending.add(partition);
        }
      }
    }
    if (pending != null) {
      adopt(pending);
    }
  }

  /**
   * Starts dispatching partitions the consumer has: each from the group's committed offset, or,
   * when there is none, from where the topic's settings say. The consumer is positioned there
   * explicitly in every case, so that what it fetches next is what the core expects.
   */
  private void adopt(Collection<TopicPartition> partitions) {
    Set<TopicPartition> known = new HashSet<>();
    for (TopicPartition partition : partitions) {
      if (topics.containsKey(partition.topic())) {
        known.add(partition);
      }
    }
    pausesChanged = true;
    if (known.isEmpty()) {
      return;
    }
    Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(known);
    Map<TopicPartition, Long> positions = new HashMap<>();
    Set<TopicPartition> uncommitted = new HashSet<>();
    for (TopicPartition partition : known) {
      OffsetAndMetadata offset = committed.get(partition);
      if (offset == null) {
        uncommitted.add(partition);
      } else {
        positions.put(partition, offset.offset());
      }
    }
    positions.putAll(startPositions(uncommitted, true));
    for (TopicPartition partition : known) {
      long position = positions.get(partition);
      Topic topic = topics.get(partition.topic());
      consumer.seek(partition, position);
      full.remove(partition);
      if (topic.blocked.remove(partition.partition())) {
        topic.metrics.blocked(-1);
      }
      // Without a committed offset the position found is committed at the next interval, so
      // that a restart does not look for it again and, under LATEST, skip what arrived since.
      owned.put(partition, new Owned(position, uncommitted.contains(partition) ? -1 : position));
      on(topic, new PartitionAssigned<>(partition.partition(), position));
    }
    LOG.info("group {} on cluster {} dispatches {}", key.group(), key.cluster(), positions);
  }

  /**
   * Where to start partitions that have no usable committed offset: at the topic's start timestamp
   * when the group has never committed and one is declared, otherwise by its offset reset.
   */
  private Map<TopicPartition, Long> startPositions(
      Set<TopicPartition> partitions, boolean neverCommitted) {
    Map<TopicPartition, Long> positions = new HashMap<>();
    if (partitions.isEmpty()) {
      return positions;
    }
    Map<TopicPartition, Long> timestamps = new HashMap<>();
    if (neverCommitted) {
      for (TopicPartition partition : partitions) {
        Settings settings = topics.get(partition.topic()).settings;
        if (settings.hasStartTimestampMs()) {
          timestamps.put(partition, settings.getStartTimestampMs());
        }
      }
    }
    if (!timestamps.isEmpty()) {
      consumer
          .offsetsForTimes(timestamps)
          .forEach(
              (TopicPartition partition, OffsetAndTimestamp found) -> {
                if (found != null) {
                  positions.put(partition, found.offset());
                }
              });
    }
    Set<TopicPartition> earliest = new HashSet<>();
    Set<TopicPartition> latest = new HashSet<>();
    for (TopicPartition partition : partitions) {
      if (positions.containsKey(partition)) {
        continue;
      }
      boolean end =
          topics.get(partition.topic()).settings.getOffsetReset()
              == OffsetReset.OFFSET_RESET_LATEST;
      (end ? latest : earliest).add(partition);
    }
    if (!earliest.isEmpty()) {
      positions.putAll(consumer.beginningOffsets(earliest));
    }
    if (!latest.isEmpty()) {
      positions.putAll(consumer.endOffsets(latest));
    }
    return positions;
  }

  /** A partition whose position holds no record any more starts again by its offset reset. */
  private void reposition(TopicPartition partition) {
    Topic topic = topics.get(partition.topic());
    if (topic == null || !owned.containsKey(partition)) {
      // Not dispatched: only kept from failing the next poll.
      consumer.seekToBeginning(List.of(partition));
      return;
    }
    long position = startPositions(Set.of(partition), false).get(partition);
    LOG.warn(
        "{} of group {} has no record at its position any more; starting again at offset {}",
        partition,
        key.group(),
        position);
    consumer.seek(partition, position);
    full.remove(partition);
    pausesChanged = true;
    if (topic.blocked.remove(partition.partition())) {
      topic.metrics.blocked(-1);
    }
    owned.put(partition, new Owned(position, -1));
    // The core drops what it had of the partition and starts from here.
    on(topic, new PartitionAssigned<>(partition.partition(), position));
  }

  /** Pauses what a core finds full, and what no core has; resumes the rest. */
  private void applyPauses() {
    Set<TopicPartition> assigned = consumer.assignment();
    Set<TopicPartition> pause = new HashSet<>();
    for (TopicPartition partition : assigned) {
      if (full.contains(partition) || !owned.containsKey(partition)) {
        pause.add(partition);
      }
    }
    Set<TopicPartition> resume = new HashSet<>(consumer.paused());
    resume.removeAll(pause);
    consumer.pause(pause);
    consumer.resume(resume);
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    if (!partitions.isEmpty()) {
      adopt(partitions);
    }
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    // Runs inside poll(): nothing is fetched while the cores wait for what is in flight.
    handOver(partitions);
    pausesChanged = true;
  }

  @Override
  public void onPartitionsLost(Collection<TopicPartition> partitions) {
    Map<Topic, List<Integer>> byTopic = byTopic(partitions);
    byTopic.forEach((topic, numbers) -> on(topic, new PartitionsLost<>(numbers)));
    partitions.forEach(this::forget);
    pausesChanged = true;
    if (!byTopic.isEmpty()) {
      LOG.warn("group {} on cluster {} lost {}", key.group(), key.cluster(), partitions);
    }
  }

  /** The partitions among these that a core has, by topic. */
  private Map<Topic, List<Integer>> byTopic(Collection<TopicPartition> partitions) {
    Map<Topic, List<Integer>> byTopic = new LinkedHashMap<>();
    for (TopicPartition partition : partitions) {
      Topic topic = topics.get(partition.topic());
      if (topic != null && owned.containsKey(partition)) {
        byTopic.computeIfAbsent(topic, t -> new ArrayList<>()).add(partition.partition());
      }
    }
    return byTopic;
  }

  private void forget(TopicPartition partition) {
    owned.remove(partition);
    full.remove(partition);
    releasing.remove(partition);
    Topic topic = topics.get(partition.topic());
    if (topic != null && topic.blocked.remove(partition.partition())) {
      topic.metrics.blocked(-1);
    }
  }

  /**
   * Gives partitions up cleanly: their cores stop sending and wait, for at most the revoke grace,
   * for the acknowledgements of what is in flight; then their positions are committed and the cores
   * have forgotten them. Commands are handled while it waits, so those acknowledgements arrive, and
   * the other partitions go on being acknowledged; only fetching stands still.
   */
  private void handOver(Collection<TopicPartition> partitions) {
    Map<Topic, List<Integer>> byTopic = byTopic(partitions);
    if (byTopic.isEmpty()) {
      return;
    }
    List<TopicPartition> mine = new ArrayList<>();
    byTopic.forEach(
        (topic, numbers) -> numbers.forEach(n -> mine.add(new TopicPartition(topic.name, n))));
    releasing.addAll(mine);
    byTopic.forEach((topic, numbers) -> on(topic, new PartitionsRevoked<>(numbers)));

    // The cores release at the grace at the latest; a little more is only a guard.
    long giveUp = now() + revokeGraceNanos + TimeUnit.SECONDS.toNanos(1);
    while (waitingFor(mine)) {
      long now = now();
      if (now - giveUp >= 0) {
        LOG.warn("hand-over of {} did not complete in time; committing what is known", mine);
        break;
      }
      long wait = Math.min(giveUp - now, TimeUnit.MILLISECONDS.toNanos(MAX_IDLE_WAIT_MS));
      for (Topic topic : byTopic.keySet()) {
        OptionalLong next = topic.core.nextDeadlineNanos();
        if (next.isPresent()) {
          wait = Math.min(wait, next.getAsLong() - now);
        }
      }
      Command command = wait > 0 ? await(wait) : commands.poll();
      while (command != null) {
        handle(command);
        command = commands.poll();
      }
      passTime(now());
    }
    commitSync(mine);
    mine.forEach(this::forget);
    LOG.info("group {} on cluster {} gave up {}", key.group(), key.cluster(), mine);
  }

  private boolean waitingFor(List<TopicPartition> partitions) {
    for (TopicPartition partition : partitions) {
      if (releasing.contains(partition)) {
        return true;
      }
    }
    return false;
  }

  // ---------------------------------------------------------------------------------------------
  // Commits
  // ---------------------------------------------------------------------------------------------

  /** The commit position of an owned partition, as far as it is known now. */
  private long position(TopicPartition partition, Owned state) {
    Topic topic = topics.get(partition.topic());
    if (topic != null) {
      // Empty once the core has released the partition: its last Commit said where it stood.
      OptionalLong committable = topic.core.committable(partition.partition());
      if (committable.isPresent()) {
        state.toCommit = Math.max(state.toCommit, committable.getAsLong());
      }
    }
    return state.toCommit;
  }

  /** Commits the positions that moved since the last commit, without waiting for the answer. */
  private void commitAsync() {
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    owned.forEach(
        (partition, state) -> {
          long position = position(partition, state);
          if (position > state.committed) {
            offsets.put(partition, new OffsetAndMetadata(position));
            state.committed = position;
          }
        });
    if (offsets.isEmpty()) {
      return;
    }
    consumer.commitAsync(
        offsets,
        (committed, failure) -> {
          if (failure == null) {
            return;
          }
          // Expected while the group rebalances, as it does when a topic is declared.
          LOG.warn(
              "asynchronous commit of {} failed; it is tried again: {}",
              committed,
              failure.toString());
          committed.forEach(
              (partition, offset) -> {
                Owned state = owned.get(partition);
                if (state != null && state.committed == offset.offset()) {
                  state.committed = -1;
                }
              });
        });
  }

  /**
   * Commits where these partitions stand and waits for the answer: everything acknowledged so far,
   * not only what moved since the last asynchronous commit, which may have failed.
   */
  private void commitSync(Collection<TopicPartition> partitions) {
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    for (TopicPartition partition : partitions) {
      Owned state = owned.get(partition);
      if (state != null) {
        long position = position(partition, state);
        offsets.put(partition, new OffsetAndMetadata(position));
        state.committed = position;
      }
    }
    if (offsets.isEmpty()) {
      return;
    }
    try {
      consumer.commitSync(offsets);
    } catch (RuntimeException e) {
      LOG.warn(
          "commit of {} failed; what was acknowledged above the last commit is delivered again",
          offsets,
          e);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // The end
  // ---------------------------------------------------------------------------------------------

  /** Hands every partition over, ends every stream with {@code status} and leaves the group. */
  private void close(Status status) {
    stopping = true;
    // No declaration is queued after this.
    owner.gone(this);
    if (consumer != null) {
      try {
        handOver(List.copyOf(owned.keySet()));
      } catch (RuntimeException e) {
        LOG.warn("final hand-over of group {} failed", key.group(), e);
      }
    }
    for (Topic topic : topics.values()) {
      for (Stream stream : List.copyOf(topic.receivers.values())) {
        left(stream);
        end(stream, status);
      }
      forgetBlocked(topic);
      topic.metrics.inFlight(-topic.reportedInFlight);
    }
    topics.clear();
    if (consumer != null) {
      try {
        consumer.close(CloseOptions.timeout(Duration.ofSeconds(10)));
      } catch (RuntimeException e) {
        LOG.warn("closing the consumer of group {} failed", key.group(), e);
      }
    }
    // What was queued meanwhile: declarations are refused, the rest concerns ended streams.
    for (Command command = commands.poll(); command != null; command = commands.poll()) {
      if (command instanceof Join join) {
        end(join.stream(), status);
      } else if (command instanceof Leave leave) {
        end(leave.stream(), leave.status());
      }
    }
  }
}
