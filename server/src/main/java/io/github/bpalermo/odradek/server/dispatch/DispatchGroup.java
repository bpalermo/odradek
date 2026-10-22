package io.github.bpalermo.odradek.server.dispatch;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import io.github.bpalermo.odradek.delivery.v1.Accepted;
import io.github.bpalermo.odradek.delivery.v1.DeliverResponse;
import io.github.bpalermo.odradek.delivery.v1.OffsetReset;
import io.github.bpalermo.odradek.delivery.v1.Record;
import io.github.bpalermo.odradek.delivery.v1.ReplicaList;
import io.github.bpalermo.odradek.delivery.v1.Settings;
import io.github.bpalermo.odradek.kafka.v1.Header;
import io.github.bpalermo.odradek.server.dispatch.Declarations.Declared;
import io.github.bpalermo.odradek.server.dispatch.DispatchAction.Cause;
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
import java.util.concurrent.atomic.AtomicBoolean;
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
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.record.TimestampType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One consumer group on one cluster, as a dispatcher consumes it: a Kafka consumer, the thread that
 * owns it, one {@link DispatchCore} for each topic sidecars have declared, and the {@code Deliver}
 * streams of those sidecars.
 *
 * <p>Everything here but {@link #submit} runs on that thread: the consumer, the cores and every
 * message written to a stream after its handshake. gRPC threads only queue commands. The thread
 * never waits for a sidecar: when it has nothing to do it waits in the consumer's poll, until
 * records arrive, until the next thing it has to do at a time of its own, or until a command is
 * queued, which wakes the consumer ({@link #submit}). So records are handed to the cores as soon as
 * the consumer has them, a command is acted on as soon as it is queued, and the consumer is polled
 * however slow the sidecars are.
 *
 * <p>The group exists while it has a topic. A topic is added by the first declaration for it, and
 * removed when it has had no stream for the idle grace; the consumer's subscription follows.
 */
final class DispatchGroup implements ConsumerRebalanceListener {
  private static final Logger LOG = LoggerFactory.getLogger(DispatchGroup.class);

  /**
   * The longest the thread waits in a poll. Only a guard: what it waits for either has a time the
   * wait is cut to, or wakes the consumer.
   */
  static final long MAX_POLL_WAIT_MS = 100;

  /** While partitions are handed over, the longest the thread waits for a command at a time. */
  private static final long HAND_OVER_WAIT_MS = 10;

  /**
   * At most one line per topic in this time about each of: records that sidecars reject, records
   * that used all their attempts, and dead letters that could not be written.
   */
  private static final long REJECT_LOG_NANOS = TimeUnit.SECONDS.toNanos(10);

  /** The largest {@code RecordBatch}: a gRPC peer accepts 4 MiB by default. */
  static final int MAX_BATCH_RECORDS = 500;

  static final long MAX_BATCH_BYTES = 1 << 20;

  /** A group is one consumer group on one cluster. */
  record Key(String cluster, String group) {}

  /**
   * What every group of a dispatcher shares.
   *
   * @param nanoClock the monotonic clock the cores run on
   * @param wallClock milliseconds since the epoch, to measure how long after its timestamp a record
   *     was fetched
   */
  record Options(
      long window,
      int maxBufferedRecords,
      long maxBufferedBytes,
      Duration revokeGrace,
      Duration idleGrace,
      Duration commitInterval,
      Duration deadLetterRetry,
      Duration deadLetterRetryMax,
      DeadLetters deadLetters,
      Supplier<Accepted> accepted,
      LongSupplier nanoClock,
      LongSupplier wallClock) {}

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
  sealed interface Command
      permits Join, Credit, Acked, Reject, Leave, Announce, DeadLettered, Shutdown {}

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

  /** The dispatcher's replicas changed: every stream is sent the new list. */
  record Announce(ReplicaList replicas) implements Command {}

  /**
   * The write of a dead letter was answered: by the brokers, or by a failure, which is then not
   * null. {@code incarnation} says which run of the topic asked for it.
   */
  record DeadLettered(String topic, long incarnation, int partition, long offset, Exception failure)
      implements Command {}

  /** The dispatcher is stopping. */
  record Shutdown() implements Command {}

  /** A fetched record as its core carries it. */
  private record Held(ConsumerRecord<byte[], byte[]> record) {}

  /** Counts something that happens per record, for one log line every so often. */
  private static final class Throttled {
    long count;
    long lastLog;

    Throttled(long now) {
      lastLog = now - REJECT_LOG_NANOS;
    }

    /** Counts one; whether it is time for a line, which says {@link #count} and resets it. */
    boolean due(long now) {
      count++;
      if (now - lastLog < REJECT_LOG_NANOS) {
        return false;
      }
      lastLog = now;
      return true;
    }

    long taken() {
      long counted = count;
      count = 0;
      return counted;
    }
  }

  /** One declared topic. */
  private final class Topic implements DispatchCore.Timings {
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

    /**
     * Which run of the topic this is: it changes when the topic starts again with other settings,
     * so that the answer to a dead letter the earlier run asked for is told apart.
     */
    long incarnation;

    int reportedInFlight;
    int reportedAtWindow;
    long reportedWindowPauses;
    final Throttled rejects;
    final Throttled exhausted;
    final Throttled deadLetterFailures;

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
      long now = options.nanoClock().getAsLong();
      rejects = new Throttled(now);
      exhausted = new Throttled(now);
      deadLetterFailures = new Throttled(now);
    }

    /**
     * Starts consuming as {@code declared} says, with nothing known of any partition. What the core
     * before it counted must have been reported.
     */
    void start(Declared declared) {
      settings = declared.settings();
      core =
          new DispatchCore<>(
              declared.toCore(
                  options.window(),
                  options.maxBufferedRecords(),
                  options.maxBufferedBytes(),
                  options.revokeGrace(),
                  options.deadLetterRetry(),
                  options.deadLetterRetryMax()),
              this);
      adopt = true;
      incarnation = ++incarnations;
      reportedWindowPauses = 0;
    }

    @Override
    public void queued(long nanos, DispatchCore.Wait waited) {
      metrics.queued(
          nanos,
          switch (waited) {
            case NOTHING -> ServerMetrics.QueueWait.NONE;
            case CREDIT -> ServerMetrics.QueueWait.CREDIT;
            case ORDERING -> ServerMetrics.QueueWait.ORDERING;
            case WINDOW -> ServerMetrics.QueueWait.WINDOW;
          });
    }

    @Override
    public void answered(long nanos, boolean acknowledged) {
      metrics.answered(nanos, acknowledged);
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
  private final ServerMetrics.DispatchLoop loopMetrics;
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

  /** Records sent of partitions that were being handed over, since the group exists. */
  private long sentWhileGivingUp;

  /** Counts the runs of this group's topics. */
  private long incarnations;

  /**
   * Whether a command queued now has to wake the consumer: the thread is in a poll that may block,
   * and not in a rebalance callback. Set by the thread, and cleared by whoever wakes the consumer,
   * so that one poll is woken once. See {@link #submit}.
   */
  private final AtomicBoolean wakeable = new AtomicBoolean();

  /** The thread is in a poll that may block, callbacks included. Group thread only. */
  private boolean inPoll;

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
    this.loopMetrics = metrics.dispatchLoop(key.cluster(), key.group());
    this.owner = owner;
    this.revokeGraceNanos = options.revokeGrace().toNanos();
    this.idleGraceNanos = options.idleGrace().toNanos();
    // At least a millisecond: the interval reaches here in whole milliseconds, and one of zero
    // would make a commit always due, so that every poll returned at once and an idle group spun.
    this.commitIntervalNanos =
        Math.max(options.commitInterval().toNanos(), TimeUnit.MILLISECONDS.toNanos(1));
    this.thread = new Thread(this::run, "odradek-dispatch-" + key.cluster() + "-" + key.group());
    this.thread.setDaemon(true);
  }

  Key key() {
    return key;
  }

  void start() {
    thread.start();
  }

  /**
   * Queues a command, and wakes the consumer if the thread waits in a poll. The only method other
   * threads call while the group runs.
   *
   * <p>A wakeup makes the consumer's next blocking call throw {@link WakeupException}, whichever it
   * is. The thread allows one only while it is in its own poll ({@link #poll}), never while a
   * rebalance callback runs inside it ({@link #callback}); but one that was asked for just as that
   * poll returned, or as a callback began, is still pending afterwards. So every other blocking
   * call to the consumer goes through {@link #blocking}, which sees it through, and the poll treats
   * it as having returned nothing.
   */
  void submit(Command command) {
    commands.add(command);
    if (wakeable.compareAndSet(true, false)) {
      consumer.wakeup();
    }
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
    while (true) {
      drain();
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
        // Waits for records, or for the next thing to do at a time of its own; a command cuts the
        // wait short, so an acknowledgement is acted on at once and not at the end of the poll.
        records = poll(waitNanos(now()));
      } catch (InvalidOffsetException e) {
        // A committed offset that retention has since removed.
        e.partitions().forEach(this::reposition);
        continue;
      }
      if (!records.isEmpty()) {
        // Once per poll: how long after their timestamps the records were fetched.
        long fetchedAt = options.wallClock().getAsLong();
        for (TopicPartition partition : records.partitions()) {
          fetched(partition, records.records(partition), fetchedAt);
        }
      }
      now = now();
      passTime(now);
      if (now - lastCommitNanos >= commitIntervalNanos) {
        lastCommitNanos = now;
        commitAsync();
      }
      topics.values().forEach(this::report);
    }
  }

  /**
   * How long the thread may wait in its next poll: until a core's next deadline, the next commit or
   * the end of a topic's idle grace, whichever comes first, and at most {@link #MAX_POLL_WAIT_MS}.
   */
  private long waitNanos(long now) {
    long due = lastCommitNanos + commitIntervalNanos;
    for (Topic topic : topics.values()) {
      OptionalLong next = topic.core.nextDeadlineNanos();
      if (next.isPresent() && next.getAsLong() - due < 0) {
        due = next.getAsLong();
      }
      if (topic.receivers.isEmpty() && topic.idleSince + idleGraceNanos - due < 0) {
        due = topic.idleSince + idleGraceNanos;
      }
    }
    return Math.max(0, Math.min(due - now, TimeUnit.MILLISECONDS.toNanos(MAX_POLL_WAIT_MS)));
  }

  /**
   * Polls the consumer, waiting up to {@code waitNanos} for records unless a command is queued, and
   * letting {@link #submit} wake it meanwhile. A wakeup, that one or one still pending from before,
   * returns nothing.
   */
  private ConsumerRecords<byte[], byte[]> poll(long waitNanos) {
    // Rounded up: the consumer counts in milliseconds, and a wait cut short would only poll again.
    long waitMs = TimeUnit.NANOSECONDS.toMillis(waitNanos + TimeUnit.MILLISECONDS.toNanos(1) - 1);
    if (waitMs > 0) {
      // Allowed before the queue is looked at: a command queued after the look wakes the consumer.
      wakeable.set(true);
      if (!commands.isEmpty()) {
        wakeable.set(false);
        waitMs = 0;
      }
    }
    inPoll = waitMs > 0;
    long start = inPoll ? now() : 0;
    try {
      return consumer.poll(Duration.ofMillis(waitMs));
    } catch (WakeupException e) {
      return ConsumerRecords.empty();
    } finally {
      if (inPoll) {
        inPoll = false;
        wakeable.set(false);
        loopMetrics.waited(now() - start);
      }
    }
  }

  /**
   * Runs a rebalance callback, which the consumer calls inside a poll: no command wakes the
   * consumer while it runs, since the hand-over waits for acknowledgements by handling commands
   * itself, and a wakeup would only cut its synchronous commit short. When it returns into a poll
   * that may block, commands may wake the consumer again, at once if one was queued meanwhile.
   */
  private void callback(Runnable callback) {
    wakeable.set(false);
    try {
      callback.run();
    } finally {
      if (inPoll) {
        wakeable.set(true);
        if (!commands.isEmpty() && wakeable.compareAndSet(true, false)) {
          consumer.wakeup();
        }
      }
    }
  }

  /**
   * Makes a call to the consumer that may block: a synchronous commit, or a lookup of committed or
   * start offsets. A wakeup that {@link #submit} asked for just as a poll returned, or as a
   * callback began, may still be pending, and makes the call throw: it is made again, which for a
   * commit or a lookup only repeats it. Once that wakeup is spent no other is asked for while the
   * call runs, since the thread is out of its poll or in a callback, so this ends.
   */
  private <T> T blocking(Supplier<T> call) {
    while (true) {
      try {
        return call.get();
      } catch (WakeupException e) {
        LOG.debug("a call to the consumer of group {} met a wakeup meant for a poll", key.group());
      }
    }
  }

  private long now() {
    return options.nanoClock().getAsLong();
  }

  /** Handles what is queued, without waiting. */
  private void drain() {
    for (Command command = commands.poll(); command != null; command = commands.poll()) {
      handle(command);
    }
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
      case Announce announce -> {
        // Handled while partitions are handed over too: a sidecar learns of a replica that
        // started without waiting for the group it made rebalance.
        DeliverResponse response =
            DeliverResponse.newBuilder().setReplicas(announce.replicas()).build();
        for (Topic topic : topics.values()) {
          for (Stream stream : topic.receivers.values()) {
            send(stream, response);
          }
        }
      }
      case DeadLettered answer -> deadLettered(answer);
      case Shutdown _ -> stopping = true;
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
        metrics.declarationRefused(Status.Code.FAILED_PRECONDITION.name());
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
      report(topic);
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
    if (topic.rejects.due(now())) {
      LOG.warn(
          "{} record(s) of topic {} rejected by sidecars of group {}; the last, {}/{} by {}: {}",
          topic.rejects.taken(),
          topic.name,
          key.group(),
          reject.partition(),
          reject.offset(),
          reject.stream().sidecar,
          reject.reason());
    }
    on(
        topic,
        new DispatchEvent.Rejected<>(
            reject.stream().id, reject.partition(), reject.offset(), reject.reason()));
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
          topic.metrics.sent(
              switch (deliver.cause()) {
                case NONE -> null;
                case DEADLINE -> ServerMetrics.Redelivery.DEADLINE;
                case REJECTED -> ServerMetrics.Redelivery.REJECTED;
                case RECEIVER_LEFT -> ServerMetrics.Redelivery.RECEIVER_GONE;
              });
          if (!releasing.isEmpty()
              && releasing.contains(new TopicPartition(topic.name, deliver.partition()))) {
            sentWhileGivingUp++;
          }
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
          topic.metrics.exhausted(false);
          LOG.warn(
              "{}-{} of group {} is blocked: the record at offset {} used all its {} attempts."
                  + " Nothing after it is delivered, and nothing is committed past it, until the"
                  + " partition changes owner (another replica takes it, or this one restarts),"
                  + " which starts the attempts again, or a sidecar that still holds the record"
                  + " acknowledges it",
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
        case PartitionReleased<Held> released -> {
          if (releasing.remove(new TopicPartition(topic.name, released.partition()))) {
            topic.metrics.handedOver(released.inTime(), released.resent());
            if (!released.inTime()) {
              LOG.warn(
                  "{}-{} of group {} is given up with records still unanswered after {}: its next"
                      + " owner delivers them again while a sidecar may still be working on them",
                  topic.name,
                  released.partition(),
                  key.group(),
                  options.revokeGrace());
            }
          }
        }
        case DeadLetter<Held> letter -> deadLetter(topic, letter);
      }
    }
    topic.batches.flush();
  }

  /** Asks for an exhausted record to be written to the topic's dead-letter topic. */
  private void deadLetter(Topic topic, DeadLetter<Held> letter) {
    ConsumerRecord<byte[], byte[]> record = letter.payload().record();
    String target = topic.settings.getDeadLetterTopic();
    if (letter.retries() == 0) {
      topic.metrics.exhausted(true);
      if (topic.exhausted.due(now())) {
        LOG.warn(
            "{} record(s) of topic {} used all their {} attempts in group {} and are written to"
                + " dead-letter topic {}; the last, {}/{}, after {}",
            topic.exhausted.taken(),
            topic.name,
            topic.settings.getMaxAttempts(),
            key.group(),
            target,
            record.partition(),
            record.offset(),
            letter.cause() == Cause.REJECTED
                ? "a rejection: " + letter.detail()
                : "a missed processing deadline");
      }
    }
    String name = topic.name;
    long incarnation = topic.incarnation;
    int partition = letter.partition();
    long offset = letter.offset();
    options
        .deadLetters()
        .write(
            key.cluster(),
            DeadLetters.record(
                target,
                key.cluster(),
                key.group(),
                record,
                letter.attempts(),
                letter.cause(),
                letter.detail()),
            failure -> submit(new DeadLettered(name, incarnation, partition, offset, failure)));
  }

  /** The write of a dead letter was answered: only now does the record count as done. */
  private void deadLettered(DeadLettered answer) {
    Topic topic = topics.get(answer.topic());
    boolean ok = answer.failure() == null;
    (topic == null
            ? metrics.dispatchTopic(key.cluster(), key.group(), answer.topic())
            : topic.metrics)
        .deadLetter(ok);
    if (topic == null || topic.incarnation != answer.incarnation()) {
      // Asked for by a run of the topic that is over: whoever has the partition now starts from
      // what is committed, which is at or below this record.
      return;
    }
    if (ok) {
      on(topic, new DispatchEvent.DeadLetterWritten<>(answer.partition(), answer.offset()));
      return;
    }
    if (topic.deadLetterFailures.due(now())) {
      LOG.warn(
          "{} write(s) to dead-letter topic {} failed for topic {} of group {}; the last, of"
              + " {}/{}: {}. Each is made again, and nothing is committed past its record until"
              + " it is written",
          topic.deadLetterFailures.taken(),
          topic.settings.getDeadLetterTopic(),
          topic.name,
          key.group(),
          answer.partition(),
          answer.offset(),
          answer.failure().toString());
    }
    on(topic, new DispatchEvent.DeadLetterFailed<>(answer.partition(), answer.offset()));
  }

  /** A record as a sidecar receives it: the bytes stored in Kafka, untouched. */
  private static Record toRecord(Held held, int attempt) {
    ConsumerRecord<byte[], byte[]> record = held.record();
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

  /**
   * Hands a partition's fetched records to its core.
   *
   * @param fetchedAt when the poll that returned them did, in milliseconds since the epoch
   */
  private void fetched(
      TopicPartition partition, List<ConsumerRecord<byte[], byte[]>> records, long fetchedAt) {
    Topic topic = topics.get(partition.topic());
    if (topic == null || !owned.containsKey(partition)) {
      // Of a topic nobody declares any more, until the consumer gives its partitions up. Whoever
      // takes the partition, this group included, starts from what is committed.
      return;
    }
    List<Fetched<Held>> fetched = new ArrayList<>(records.size());
    for (ConsumerRecord<byte[], byte[]> record : records) {
      if (record.timestamp() >= 0 && record.timestampType() != TimestampType.NO_TIMESTAMP_TYPE) {
        topic.metrics.fetched(
            fetchedAt - record.timestamp(),
            record.timestampType() == TimestampType.LOG_APPEND_TIME);
      }
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

  /** Reports what the topic's core counts: records in flight, partitions at the window. */
  private void report(Topic topic) {
    int inFlight = topic.core.inFlight();
    if (inFlight != topic.reportedInFlight) {
      topic.metrics.inFlight(inFlight - topic.reportedInFlight);
      topic.reportedInFlight = inFlight;
    }
    int atWindow = topic.core.atWindow();
    if (atWindow != topic.reportedAtWindow) {
      topic.metrics.atWindow(atWindow - topic.reportedAtWindow);
      topic.reportedAtWindow = atWindow;
    }
    long pauses = topic.core.windowPauses();
    if (pauses != topic.reportedWindowPauses) {
      topic.metrics.windowPauses(pauses - topic.reportedWindowPauses);
      topic.reportedWindowPauses = pauses;
    }
  }

  /** The topic is no longer dispatched: its gauges go back to nothing. */
  private void unreport(Topic topic) {
    report(topic);
    topic.metrics.inFlight(-topic.reportedInFlight);
    topic.metrics.atWindow(-topic.reportedAtWindow);
    topic.reportedInFlight = 0;
    topic.reportedAtWindow = 0;
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
      unreport(topic);
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
    Map<TopicPartition, OffsetAndMetadata> committed = blocking(() -> consumer.committed(known));
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
      blocking(() -> consumer.offsetsForTimes(timestamps))
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
      positions.putAll(blocking(() -> consumer.beginningOffsets(earliest)));
    }
    if (!latest.isEmpty()) {
      positions.putAll(blocking(() -> consumer.endOffsets(latest)));
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
    callback(
        () -> {
          if (!partitions.isEmpty()) {
            metrics.rebalanced(key.cluster(), key.group(), "assigned");
            adopt(partitions);
          }
        });
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    callback(
        () -> {
          if (!partitions.isEmpty()) {
            metrics.rebalanced(key.cluster(), key.group(), "revoked");
          }
          // Runs inside poll(): nothing is fetched while the cores finish what they had sent.
          handOver(partitions);
          pausesChanged = true;
        });
  }

  @Override
  public void onPartitionsLost(Collection<TopicPartition> partitions) {
    callback(
        () -> {
          if (!partitions.isEmpty()) {
            metrics.rebalanced(key.cluster(), key.group(), "lost");
          }
          Map<Topic, List<Integer>> byTopic = byTopic(partitions);
          byTopic.forEach((topic, numbers) -> on(topic, new PartitionsLost<>(numbers)));
          partitions.forEach(this::forget);
          pausesChanged = true;
          if (!byTopic.isEmpty()) {
            LOG.warn("group {} on cluster {} lost {}", key.group(), key.cluster(), partitions);
          }
        });
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
   * Gives partitions up cleanly: their cores send nothing new, finish what lies below the newest
   * record they sent (a record held back there would otherwise stop the commit below records that
   * are acknowledged, which the next owner would deliver again) and wait for the acknowledgements
   * of what is in flight, all within the revoke grace; then their positions are committed and the
   * cores have forgotten them. Commands are handled while it waits, so those acknowledgements
   * arrive and what they let through is sent, and the other partitions go on being delivered and
   * acknowledged; only fetching stands still.
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
    long sentBefore = sentWhileGivingUp;
    // Said before the cores are told: whatever a sidecar sends after this line is handled with
    // the hand-over under way.
    LOG.info(
        "group {} on cluster {} gives up {}: nothing new of them is sent, and what lies below the"
            + " newest record sent is finished, within {}",
        key.group(),
        key.cluster(),
        mine,
        options.revokeGrace());
    byTopic.forEach((topic, numbers) -> on(topic, new PartitionsRevoked<>(numbers)));

    // The cores release at the grace at the latest; a little more is only a guard.
    long giveUp = now() + revokeGraceNanos + TimeUnit.SECONDS.toNanos(1);
    while (waitingFor(mine)) {
      long now = now();
      if (now - giveUp >= 0) {
        LOG.warn("hand-over of {} did not complete in time; committing what is known", mine);
        break;
      }
      long wait = Math.min(giveUp - now, TimeUnit.MILLISECONDS.toNanos(HAND_OVER_WAIT_MS));
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
    LOG.info(
        "group {} on cluster {} gave up {}, having sent {} record(s) it had held back",
        key.group(),
        key.cluster(),
        mine,
        sentWhileGivingUp - sentBefore);
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
      blocking(
          () -> {
            consumer.commitSync(offsets);
            return null;
          });
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
      unreport(topic);
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
