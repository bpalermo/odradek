package io.github.bpalermo.odradek.server.receive;

import com.google.protobuf.ByteString;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import io.github.bpalermo.odradek.config.v1.Consumer;
import io.github.bpalermo.odradek.config.v1.OnDecodeError;
import io.github.bpalermo.odradek.delivery.v1.Accepted;
import io.github.bpalermo.odradek.delivery.v1.Declare;
import io.github.bpalermo.odradek.delivery.v1.DeliverRequest;
import io.github.bpalermo.odradek.delivery.v1.DeliverResponse;
import io.github.bpalermo.odradek.delivery.v1.DeliveryServiceGrpc;
import io.github.bpalermo.odradek.delivery.v1.OffsetReset;
import io.github.bpalermo.odradek.delivery.v1.OnExhausted;
import io.github.bpalermo.odradek.delivery.v1.Ordering;
import io.github.bpalermo.odradek.delivery.v1.PeerInfo;
import io.github.bpalermo.odradek.delivery.v1.Record;
import io.github.bpalermo.odradek.delivery.v1.Reject;
import io.github.bpalermo.odradek.delivery.v1.Return;
import io.github.bpalermo.odradek.delivery.v1.ReturnedRecord;
import io.github.bpalermo.odradek.delivery.v1.Settings;
import io.github.bpalermo.odradek.kafka.v1.Ack;
import io.github.bpalermo.odradek.kafka.v1.Assigned;
import io.github.bpalermo.odradek.kafka.v1.ConsumeResponse;
import io.github.bpalermo.odradek.kafka.v1.ConsumedRecord;
import io.github.bpalermo.odradek.kafka.v1.Header;
import io.github.bpalermo.odradek.kafka.v1.Lost;
import io.github.bpalermo.odradek.kafka.v1.PartitionAssignment;
import io.github.bpalermo.odradek.kafka.v1.RecordBatch;
import io.github.bpalermo.odradek.kafka.v1.TopicPartition;
import io.github.bpalermo.odradek.server.codec.RecordDecodeException;
import io.github.bpalermo.odradek.server.codec.RecordDecoder;
import io.github.bpalermo.odradek.server.consume.AppStream;
import io.github.bpalermo.odradek.server.consume.DispatcherSource;
import io.github.bpalermo.odradek.server.telemetry.ServerMetrics;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One Consume stream whose topics a dispatcher delivers: the bridge between the app's stream and
 * the {@code Deliver} streams to the dispatcher's replicas, one per topic and replica (ADR 6).
 *
 * <p>It has no Kafka consumer and commits nothing. It declares each topic to each replica, divides
 * the app's credit among those streams ({@link CreditSplit}), decodes what arrives with the topic's
 * codec, hands it to the app in the words of the app's protocol ({@link Holdings}), and passes each
 * acknowledgement back on the stream its record came from. It follows the list of replicas the
 * dispatcher sends ({@link Replicas}) and opens a lost stream again with a backoff, while the app's
 * stream stays open and simply receives nothing.
 *
 * <p>Everything here runs on the session's own thread: the state, the writes to the app's stream
 * and the writes to the Deliver streams. gRPC threads, the app's and the dispatcher's, only put
 * events on a queue.
 */
final class ReceiveSession implements AppStream {
  private static final Logger LOG = LoggerFactory.getLogger(ReceiveSession.class);

  /** How long the thread waits for an event when no timer is due sooner. */
  private static final long IDLE_NANOS = TimeUnit.SECONDS.toNanos(1);

  /** Rejections are logged at most this often per topic. */
  private static final long REJECT_LOG_NANOS = TimeUnit.SECONDS.toNanos(10);

  /**
   * Where a session gets its connections, by address as host:port. A session asks once for each
   * address it uses and gives each connection back when it has no stream left for it. Tests
   * substitute in-process channels.
   */
  @FunctionalInterface
  interface Channels {
    /** A connection to this address for one session to open streams on. */
    ManagedChannel connect(String address);

    /**
     * The session has no more use for the connection it was given: every stream it opened on it has
     * ended or is half-closed. Here the connection was the session's own, and is closed once those
     * streams have ended.
     */
    default void release(String address, ManagedChannel channel) {
      channel.shutdown();
    }

    /**
     * The session ends and has released these connections: waits up to {@code timeout} for the
     * dispatcher to close the streams that were half-closed on them. Here they were the session's
     * own, and what is left after that is cut.
     */
    default void drained(Collection<ManagedChannel> released, Duration timeout) {
      long deadline = System.nanoTime() + timeout.toNanos();
      for (ManagedChannel channel : released) {
        try {
          if (!channel.awaitTermination(
              Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS)) {
            channel.shutdownNow();
          }
        } catch (InterruptedException e) {
          channel.shutdownNow();
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  /**
   * What a session needs besides its subscription.
   *
   * @param bootstrap {@code dispatcher.address}: where to connect while no replica is known
   * @param version this server's version, for the dispatcher's logs
   * @param drain how long to wait, at the end, for the dispatcher to close the streams
   * @param backoff makes the backoff of one stream
   */
  record Options(
      String bootstrap,
      String version,
      Duration drain,
      LongSupplier nanoClock,
      Supplier<Backoff> backoff) {}

  /** Something that happened, for the session's thread. */
  private sealed interface Event {}

  private record AppCredit(long records) implements Event {}

  /** The app acknowledged, at {@code arrivedNanos}. */
  private record AppAck(Ack ack, long arrivedNanos) implements Event {}

  private record AppHalfClose() implements Event {}

  private record AppCancel() implements Event {}

  private record AppFail(Status status) implements Event {}

  private record Stop() implements Event {}

  /** A message from a replica, which arrived at {@code arrivedNanos}. */
  private record FromDispatcher(Link link, DeliverResponse response, long arrivedNanos)
      implements Event {}

  private record LinkEnded(Link link, Status status) implements Event {}

  /** One Deliver stream: one attempt to hold a topic at one replica. */
  private final class Link {
    final long id;
    final Topic topic;

    /** Opened to the configured address, so which replica answers is not known beforehand. */
    final boolean bootstrap;

    /** The replica's address; for a bootstrap stream, what {@code Accepted} said, once it has. */
    String address;

    StreamObserver<DeliverRequest> requests;
    boolean accepted;

    /** The replica said in {@code Accepted} that it understands {@code Return} (ADR 7). */
    boolean acceptsReturn;

    /** Ended, by either side. Nothing is sent on it and nothing from it is acted on. */
    boolean closed;

    Link(long id, Topic topic, boolean bootstrap, String address) {
      this.id = id;
      this.topic = topic;
      this.bootstrap = bootstrap;
      this.address = address;
    }

    void send(DeliverRequest.Builder request) {
      if (!closed && requests != null) {
        requests.onNext(request.build());
      }
    }

    /** Where it connects, for logs. */
    String target() {
      return address == null || address.isEmpty() ? options.bootstrap() : address;
    }
  }

  /** A replica whose stream failed, and when to try it again. */
  private static final class Retry {
    final Backoff backoff;
    boolean waiting;
    long due;

    Retry(Backoff backoff) {
      this.backoff = backoff;
    }
  }

  /** One topic of the subscription and its streams. */
  private final class Topic {
    final String name;
    final RecordDecoder decoder;
    final boolean deliverUndecoded;
    final Declare declare;
    final ServerMetrics.ReceiveTopic metrics;
    final Backoff bootstrapBackoff = options.backoff().get();

    /** The stream to the configured address that has not been accepted yet. */
    Link bootstrap;

    boolean bootstrapWaiting;
    long bootstrapDue;

    /** The streams being opened or accepted, by replica address. */
    final Map<String, Link> links = new LinkedHashMap<>();

    /** The listed replicas whose stream failed, by address. */
    final Map<String, Retry> retries = new HashMap<>();

    long rejects;
    boolean rejectLogged;
    long lastRejectLog;

    long returns;
    boolean returnLogged;
    long lastReturnLog;

    /** The streams wanted for it, as last reported. */
    int reportedWanted;

    Topic(
        String name,
        RecordDecoder decoder,
        boolean deliverUndecoded,
        Declare declare,
        ServerMetrics.ReceiveTopic metrics) {
      this.name = name;
      this.decoder = decoder;
      this.deliverUndecoded = deliverUndecoded;
      this.declare = declare;
      this.metrics = metrics;
    }
  }

  /**
   * A record received and decoded, waiting for the app to have credit. {@code arrivedNanos} is when
   * its batch arrived; {@code attempt} is the one it was delivered with, and {@code giveBackNanos}
   * how long it may wait here before it is given back: half the deadline its batch was sent under
   * (ADR 7).
   */
  private record Pending(
      Link link,
      int partition,
      ConsumedRecord record,
      boolean undecodable,
      long arrivedNanos,
      int attempt,
      long giveBackNanos) {}

  private final ServerCallStreamObserver<ConsumeResponse> call;
  private final Map<String, Topic> topics = new LinkedHashMap<>();
  private final Collection<RecordDecoder> decoders;
  private final DispatcherSource.Listener listener;
  private final Options options;
  private final Channels connector;
  private final String cluster;
  private final String group;
  private final long maxProcessingNanos;
  private final LinkedBlockingQueue<Event> events = new LinkedBlockingQueue<>();
  private final Thread thread;

  private final CreditSplit split = new CreditSplit();
  private final Holdings holdings = new Holdings();
  private final Replicas replicas = new Replicas();
  private final Map<Long, Link> linksById = new HashMap<>();
  private final Map<String, ManagedChannel> channels = new HashMap<>();
  private final ArrayDeque<Pending> pending = new ArrayDeque<>();
  private volatile boolean cancelled;
  private long linkCounter;
  private boolean stalled;

  /** Set when the stream has to end, with how. */
  private Status exit;

  ReceiveSession(
      String name,
      DispatcherSource.Request request,
      Options options,
      Channels connector,
      ServerMetrics metrics) {
    this.call = request.call();
    this.decoders = request.decoders().values();
    this.listener = request.listener();
    this.options = options;
    this.connector = connector;
    this.maxProcessingNanos = TimeUnit.MILLISECONDS.toNanos(request.maxProcessingMs());
    Consumer first = request.entries().get(request.topics().get(0));
    this.cluster = first.getCluster();
    this.group = first.getGroupId();
    for (String topic : request.topics()) {
      Consumer entry = request.entries().get(topic);
      topics.put(
          topic,
          new Topic(
              topic,
              request.decoders().get(topic),
              entry.getOnDecodeError() == OnDecodeError.ON_DECODE_ERROR_DELIVER_UNDECODED,
              declare(topic, entry, request.maxProcessingMs(), options.version()),
              metrics.receiveTopic(cluster, group, topic)));
    }
    this.thread = new Thread(this::run, name);
    this.thread.setDaemon(true);
  }

  /** What the consumer entry of a topic, and the app's deadline, make the sidecar declare. */
  static Declare declare(String topic, Consumer entry, long maxProcessingMs, String version) {
    Settings.Builder settings =
        Settings.newBuilder()
            .setMaxProcessingMs((int) maxProcessingMs)
            .setOrdering(
                switch (entry.getOrdering()) {
                  case ORDERING_KEY -> Ordering.ORDERING_KEY;
                  case ORDERING_PARTITION -> Ordering.ORDERING_PARTITION;
                  case ORDERING_NONE -> Ordering.ORDERING_NONE;
                  default -> Ordering.ORDERING_UNSPECIFIED;
                })
            .setOffsetReset(
                switch (entry.getOffsetReset()) {
                  case OFFSET_RESET_EARLIEST -> OffsetReset.OFFSET_RESET_EARLIEST;
                  case OFFSET_RESET_LATEST -> OffsetReset.OFFSET_RESET_LATEST;
                  default -> OffsetReset.OFFSET_RESET_UNSPECIFIED;
                })
            .setOnExhausted(
                switch (entry.getOnExhausted()) {
                  case ON_EXHAUSTED_BLOCK -> OnExhausted.ON_EXHAUSTED_BLOCK;
                  case ON_EXHAUSTED_DEAD_LETTER -> OnExhausted.ON_EXHAUSTED_DEAD_LETTER;
                  default -> OnExhausted.ON_EXHAUSTED_UNSPECIFIED;
                });
    if (entry.hasStartTimestamp()) {
      settings.setStartTimestampMs(Timestamps.toMillis(entry.getStartTimestamp()));
    }
    if (entry.hasMaxAttempts()) {
      settings.setMaxAttempts(entry.getMaxAttempts());
    }
    if (entry.hasDeadLetterTopic()) {
      settings.setDeadLetterTopic(entry.getDeadLetterTopic());
    }
    if (entry.hasRedeliveryBackoff()) {
      settings.setRedeliveryBackoffMs((int) Durations.toMillis(entry.getRedeliveryBackoff()));
    }
    if (entry.hasRedeliveryBackoffMax()) {
      settings.setRedeliveryBackoffMaxMs((int) Durations.toMillis(entry.getRedeliveryBackoffMax()));
    }
    return Declare.newBuilder()
        .setCluster(entry.getCluster())
        .setGroupId(entry.getGroupId())
        .setTopic(topic)
        .setSettings(settings)
        .setSidecar(PeerInfo.newBuilder().setVersion(version))
        .build();
  }

  // ---------------------------------------------------------------------------------------------
  // The app's side: gRPC threads, which only queue
  // ---------------------------------------------------------------------------------------------

  @Override
  public void start() {
    thread.start();
  }

  @Override
  public void credits(long records) {
    events.add(new AppCredit(records));
  }

  @Override
  public void ack(Ack ack) {
    events.add(new AppAck(ack, now()));
  }

  @Override
  public void revokeDone(long revokeId) {
    // Nothing is ever revoked here, so there is nothing to answer; a stray RevokeDone is harmless.
  }

  @Override
  public void halfClose() {
    events.add(new AppHalfClose());
  }

  @Override
  public void cancelled() {
    cancelled = true;
    events.add(new AppCancel());
  }

  @Override
  public void fail(Status status) {
    events.add(new AppFail(status));
  }

  @Override
  public void shutdown() {
    events.add(new Stop());
  }

  @Override
  public void join(long millis) throws InterruptedException {
    thread.join(millis);
  }

  // ---------------------------------------------------------------------------------------------
  // The thread
  // ---------------------------------------------------------------------------------------------

  private void run() {
    Status end;
    try {
      end = loop();
    } catch (RuntimeException e) {
      LOG.error("consume stream failed", e);
      end = Status.INTERNAL.withDescription(e.toString());
    }
    finish(end);
  }

  private long now() {
    return options.nanoClock().getAsLong();
  }

  /** Runs until the stream has to end; returns how. */
  private Status loop() {
    topics.values().forEach(this::bootstrap);
    while (true) {
      Event event = await(untilNextTimer(now()));
      while (event != null) {
        handle(event);
        if (exit != null) {
          return exit;
        }
        event = events.poll();
      }
      long now = now();
      timers(now);
      pump(now);
      reportWanted();
      if (exit != null) {
        return exit;
      }
    }
  }

  /**
   * Reports how many streams each topic should have: one to each listed replica, and one while no
   * list is known. Beside the streams that are open, it is what shows a replica that is listed and
   * not reached.
   */
  private void reportWanted() {
    int wanted = Math.max(1, replicas.addresses().size());
    for (Topic topic : topics.values()) {
      if (topic.reportedWanted != wanted) {
        topic.metrics.wanted(wanted - topic.reportedWanted);
        topic.reportedWanted = wanted;
      }
    }
  }

  private Event await(long nanos) {
    try {
      return events.poll(nanos, TimeUnit.NANOSECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return new Stop();
    }
  }

  private void handle(Event event) {
    switch (event) {
      case AppCredit credit -> split.appGranted(credit.records());
      case AppAck acked -> acked(acked.ack(), acked.arrivedNanos());
      case AppHalfClose _ -> exit = Status.OK;
      case AppCancel _ -> exit = Status.CANCELLED;
      case AppFail fail -> exit = fail.status();
      case Stop _ -> exit = Status.UNAVAILABLE.withDescription("the server is shutting down");
      case FromDispatcher message ->
          received(message.link(), message.response(), message.arrivedNanos());
      case LinkEnded ended -> ended(ended.link(), ended.status());
    }
  }

  /** How long until something has to be done whatever arrives. */
  private long untilNextTimer(long now) {
    long wait = IDLE_NANOS;
    for (Pending waiting : pending) {
      if (waiting.link().acceptsReturn) {
        wait = Math.min(wait, waiting.arrivedNanos() + waiting.giveBackNanos() - now);
      }
    }
    for (Topic topic : topics.values()) {
      if (topic.bootstrapWaiting) {
        wait = Math.min(wait, topic.bootstrapDue - now);
      }
      for (Retry retry : topic.retries.values()) {
        if (retry.waiting) {
          wait = Math.min(wait, retry.due - now);
        }
      }
    }
    return Math.max(0, wait);
  }

  private void timers(long now) {
    for (Topic topic : topics.values()) {
      if (topic.bootstrapWaiting && now - topic.bootstrapDue >= 0) {
        topic.bootstrapWaiting = false;
        if (replicas.known()) {
          reconcile(topic);
        } else {
          topic.metrics.reconnected();
          bootstrap(topic);
        }
      }
      for (Map.Entry<String, Retry> entry : List.copyOf(topic.retries.entrySet())) {
        Retry retry = entry.getValue();
        if (retry.waiting && now - retry.due >= 0) {
          retry.waiting = false;
          topic.metrics.reconnected();
          open(topic, entry.getKey(), false);
        }
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Records, credit and acknowledgements
  // ---------------------------------------------------------------------------------------------

  /** Hands the app what its credit allows, then grants the dispatcher what the app can take. */
  private void pump(long now) {
    // First what waited too long to be handed now: the app would take it close to its deadline.
    giveBack(now, false);
    while (!pending.isEmpty() && split.appCredit() > 0) {
      Pending first = pending.peekFirst();
      Topic topic = first.link().topic;
      Holdings.Announcement announcement =
          holdings.arriving(topic.name, first.partition(), first.link().id);
      if (announcement.lost()) {
        send(lost(topic.name, List.of(first.partition())));
      }
      if (announcement.assigned()) {
        send(
            ConsumeResponse.newBuilder()
                .setAssigned(
                    Assigned.newBuilder()
                        .addPartitions(
                            PartitionAssignment.newBuilder()
                                .setTopic(topic.name)
                                .setPartition(first.partition())
                                .setAssignmentEpoch(announcement.epoch())))
                .build());
      }
      RecordBatch.Builder batch =
          RecordBatch.newBuilder()
              .setTopic(topic.name)
              .setPartition(first.partition())
              .setAssignmentEpoch(announcement.epoch());
      long credit = split.appCredit();
      long undecodable = 0;
      long last = Long.MIN_VALUE;
      // When this batch is written: the writes before it in this pump may have taken a while.
      long written = now();
      // One batch is records of one partition from one stream, in offset order: a record delivered
      // again behind a newer one starts the next batch.
      while (!pending.isEmpty() && batch.getRecordsCount() < credit) {
        Pending next = pending.peekFirst();
        if (next.link() != first.link()
            || next.partition() != first.partition()
            || next.record().getOffset() <= last) {
          break;
        }
        pending.pollFirst();
        last = next.record().getOffset();
        batch.addRecords(next.record());
        holdings.handed(topic.name, first.partition(), last, written);
        topic.metrics.handedOff(written - next.arrivedNanos());
        if (next.undecodable()) {
          undecodable++;
        }
      }
      split.handed(batch.getRecordsCount());
      send(ConsumeResponse.newBuilder().setRecords(batch).build());
      listener.delivered(topic.name, batch.getRecordsCount(), undecodable);
    }

    // The dispatcher is the one that delivers again a record held for too long. What is not done
    // here is to ask it for more on behalf of an app that has stopped acknowledging: each record
    // sent to such an app would wait out its deadline before another pod got it.
    boolean overdue =
        holdings.held() > 0 && now - holdings.oldestHandedNanos() > maxProcessingNanos;
    if (overdue != stalled) {
      stalled = overdue;
      if (stalled) {
        LOG.warn(
            "the app has held a record of group {} for longer than its processing deadline of {}"
                + " ms: the dispatcher is granted no more credit until the app acknowledges it."
                + " The dispatcher delivers the record again, to this pod or another",
            group,
            TimeUnit.NANOSECONDS.toMillis(maxProcessingNanos));
      } else {
        LOG.info("the app of group {} acknowledges again: granting credit again", group);
      }
    }
    if (stalled) {
      giveBack(now, true);
      return;
    }
    // One stream per topic and listed replica; while no list is known, one per topic.
    int wanted = topics.size() * Math.max(1, replicas.addresses().size());
    split
        .grants(wanted)
        .forEach(
            (stream, records) ->
                linksById
                    .get(stream)
                    .send(
                        DeliverRequest.newBuilder()
                            .setCredits(
                                io.github.bpalermo.odradek.delivery.v1.Credits.newBuilder()
                                    .setRecords((int) Math.min(Integer.MAX_VALUE, records)))));
  }

  /**
   * Gives back to their replicas, without using an attempt, the records waiting here that the app
   * cannot take in time (ADR 7): those that have waited half of the deadline they were sent under,
   * and with {@code everything}, when the app holds a record past its deadline, all of them. Their
   * deadline runs at the replica from when it sent them, so behind an app that is stuck they would
   * miss it, and use an attempt, without the app ever seeing them. Elsewhere they are delivered at
   * once. Only to replicas that said they understand it; the credit the records used is not given
   * back, so a stuck app is not sent more.
   */
  private void giveBack(long now, boolean everything) {
    if (pending.isEmpty()) {
      return;
    }
    // Per stream and partition, in the order they arrived.
    Map<Link, Map<Integer, List<ReturnedRecord>>> back = new LinkedHashMap<>();
    int count = 0;
    for (Iterator<Pending> all = pending.iterator(); all.hasNext(); ) {
      Pending waiting = all.next();
      Link link = waiting.link();
      if (!link.acceptsReturn
          || link.closed
          || (!everything && now - waiting.arrivedNanos() < waiting.giveBackNanos())) {
        continue;
      }
      all.remove();
      back.computeIfAbsent(link, l -> new LinkedHashMap<>())
          .computeIfAbsent(waiting.partition(), p -> new ArrayList<>())
          .add(
              ReturnedRecord.newBuilder()
                  .setOffset(waiting.record().getOffset())
                  .setAttempt(waiting.attempt())
                  .build());
      count++;
    }
    if (count == 0) {
      return;
    }
    split.dropped(count);
    back.forEach(
        (link, partitions) -> {
          int records = 0;
          for (Map.Entry<Integer, List<ReturnedRecord>> partition : partitions.entrySet()) {
            link.send(
                DeliverRequest.newBuilder()
                    .setReturned(
                        Return.newBuilder()
                            .setPartition(partition.getKey())
                            .addAllRecords(partition.getValue())));
            records += partition.getValue().size();
          }
          Topic topic = link.topic;
          topic.metrics.returned(records);
          topic.returns += records;
          if (!topic.returnLogged || now - topic.lastReturnLog >= REJECT_LOG_NANOS) {
            LOG.info(
                "{} record(s) of topic {} given back to the dispatcher unread: the app {}",
                topic.returns,
                topic.name,
                everything
                    ? "has held a record past its processing deadline"
                    : "had not taken them within half the deadline they were sent under");
            topic.returns = 0;
            topic.returnLogged = true;
            topic.lastReturnLog = now;
          }
        });
  }

  /**
   * Records arrived: decodes them and queues them for the app, or for rejection.
   *
   * @param arrived when the batch arrived
   */
  private void records(
      Link link, io.github.bpalermo.odradek.delivery.v1.RecordBatch batch, long arrived) {
    Topic topic = link.topic;
    split.received(link.id, batch.getRecordsCount());
    topic.metrics.received(batch.getRecordsCount());
    // The replica's deadline for these records, which is the app's unless another pod declared
    // another one since; a replica that does not say has no Return either.
    long giveBack =
        (batch.getMaxProcessingMs() > 0
                ? TimeUnit.MILLISECONDS.toNanos(batch.getMaxProcessingMs())
                : maxProcessingNanos)
            / 2;
    for (Record record : batch.getRecordsList()) {
      ConsumedRecord.Builder out =
          ConsumedRecord.newBuilder()
              .setOffset(record.getOffset())
              .setTimestampMs(record.getTimestampMs());
      boolean undecodable = false;
      try {
        // A copy: a transform may change the headers before a later step fails.
        Headers headers = new RecordHeaders();
        for (Header header : record.getHeadersList()) {
          headers.add(header.getKey(), header.hasValue() ? header.getValue().toByteArray() : null);
        }
        byte[] key =
            topic.decoder.decodeKey(
                headers, record.hasKey() ? record.getKey().toByteArray() : null);
        byte[] value =
            topic.decoder.decodeValue(
                headers, record.hasValue() ? record.getValue().toByteArray() : null);
        if (key != null) {
          out.setKey(ByteString.copyFrom(key));
        }
        if (value != null) {
          out.setValue(ByteString.copyFrom(value));
        }
        headers.forEach(
            header -> {
              Header.Builder h = Header.newBuilder().setKey(header.key());
              if (header.value() != null) {
                h.setValue(ByteString.copyFrom(header.value()));
              }
              out.addHeaders(h);
            });
      } catch (RecordDecodeException e) {
        if (!topic.deliverUndecoded) {
          reject(link, batch.getPartition(), record.getOffset(), e.getMessage(), now());
          continue;
        }
        // As stored, with the error: the app acknowledges it like any other record.
        if (record.hasKey()) {
          out.setKey(record.getKey());
        }
        if (record.hasValue()) {
          out.setValue(record.getValue());
        }
        out.addAllHeaders(record.getHeadersList());
        out.setDecodeError(e.getMessage());
        undecodable = true;
      }
      pending.addLast(
          new Pending(
              link,
              batch.getPartition(),
              out.build(),
              undecodable,
              arrived,
              record.getAttempt(),
              giveBack));
    }
  }

  /**
   * Where a consumer that reads Kafka would end the stream with DATA_LOSS, the record goes back to
   * the dispatcher, which counts a failed attempt, and the stream stays: other pods may decode what
   * this one cannot, and the dispatcher's limit on attempts is what stops a record nobody can. It
   * goes back at once: how long a failed record waits before it is delivered again is the
   * dispatcher's to decide ({@code redeliveryBackoff}), in one place and for every kind of failure.
   */
  private void reject(Link link, int partition, long offset, String reason, long now) {
    Topic topic = link.topic;
    split.dropped(1);
    topic.metrics.rejected();
    topic.rejects++;
    if (!topic.rejectLogged || now - topic.lastRejectLog >= REJECT_LOG_NANOS) {
      LOG.warn(
          "{} record(s) of topic {} could not be decoded and are rejected to the dispatcher; the"
              + " last, {}/{}: {}",
          topic.rejects,
          topic.name,
          partition,
          offset,
          reason);
      topic.rejects = 0;
      topic.rejectLogged = true;
      topic.lastRejectLog = now;
    }
    link.send(
        DeliverRequest.newBuilder()
            .setReject(
                Reject.newBuilder()
                    .setPartition(partition)
                    .setOffset(offset)
                    .setReason(reason == null ? "" : reason)));
  }

  /**
   * The app acknowledged: passes on what it held of the range, to the stream it came from.
   *
   * @param arrived when the acknowledgement arrived
   */
  private void acked(Ack ack, long arrived) {
    Topic topic = topics.get(ack.getTopic());
    Holdings.Relay relay =
        holdings.ack(
            ack.getTopic(),
            ack.getPartition(),
            ack.getAssignmentEpoch(),
            ack.getFirstOffset(),
            ack.getLastOffset(),
            topic == null
                ? handed -> {}
                : handed -> topic.metrics.appAcknowledged(arrived - handed));
    if (relay == null) {
      // Released, its stream gone, an earlier announcement of the partition, or never delivered
      // here: the record is delivered again by the dispatcher, or is not this pod's to speak for.
      return;
    }
    Link link = linksById.get(relay.stream());
    if (link == null) {
      return;
    }
    for (Holdings.Range range : relay.ranges()) {
      link.send(
          DeliverRequest.newBuilder()
              .setAck(
                  io.github.bpalermo.odradek.delivery.v1.Ack.newBuilder()
                      .setPartition(ack.getPartition())
                      .setFirstOffset(range.first())
                      .setLastOffset(range.last())));
    }
  }

  /** The replica gave partitions up: what was received of them is dropped, and the app told. */
  private void released(Link link, List<Integer> partitions) {
    Set<Integer> gone = new HashSet<>(partitions);
    int dropped = 0;
    for (Iterator<Pending> all = pending.iterator(); all.hasNext(); ) {
      Pending waiting = all.next();
      if (waiting.link() == link && gone.contains(waiting.partition())) {
        all.remove();
        dropped++;
      }
    }
    split.dropped(dropped);
    List<Integer> lost = holdings.released(link.id, link.topic.name, partitions);
    if (!lost.isEmpty()) {
      send(lost(link.topic.name, lost));
    }
  }

  private static ConsumeResponse lost(String topic, List<Integer> partitions) {
    Lost.Builder lost = Lost.newBuilder();
    for (int partition : partitions) {
      lost.addPartitions(TopicPartition.newBuilder().setTopic(topic).setPartition(partition));
    }
    return ConsumeResponse.newBuilder().setLost(lost).build();
  }

  private void send(ConsumeResponse response) {
    if (!cancelled) {
      call.onNext(response);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // The streams to the dispatcher
  // ---------------------------------------------------------------------------------------------

  private void received(Link link, DeliverResponse response, long arrived) {
    if (link.closed) {
      return;
    }
    if (response.hasAccepted()) {
      if (!link.accepted) {
        accepted(link, response.getAccepted());
      }
      return;
    }
    if (!link.accepted) {
      return;
    }
    switch (response.getMsgCase()) {
      case REPLICAS -> {
        if (replicas.offer(response.getReplicas())) {
          LOG.info("the dispatcher's replicas are now {}", replicas.addresses());
          reconcileAll();
        }
      }
      case RECORDS -> records(link, response.getRecords(), arrived);
      case RELEASED -> released(link, response.getReleased().getPartitionsList());
      default -> {}
    }
  }

  /**
   * Opens a stream to the configured address: whichever replica answers says who the others are.
   */
  private void bootstrap(Topic topic) {
    open(topic, null, true);
  }

  private void open(Topic topic, String address, boolean bootstrap) {
    Link link = new Link(++linkCounter, topic, bootstrap, address);
    linksById.put(link.id, link);
    if (bootstrap) {
      topic.bootstrap = link;
    } else {
      topic.links.put(address, link);
    }
    try {
      ManagedChannel channel =
          channels.computeIfAbsent(bootstrap ? options.bootstrap() : address, connector::connect);
      // gRPC keeps its own, much longer, backoff per connection; the pace is set here.
      channel.resetConnectBackoff();
      link.requests =
          DeliveryServiceGrpc.newStub(channel)
              .deliver(
                  new StreamObserver<DeliverResponse>() {
                    @Override
                    public void onNext(DeliverResponse response) {
                      events.add(new FromDispatcher(link, response, now()));
                    }

                    @Override
                    public void onError(Throwable t) {
                      events.add(new LinkEnded(link, Status.fromThrowable(t)));
                    }

                    @Override
                    public void onCompleted() {
                      events.add(new LinkEnded(link, Status.OK));
                    }
                  });
      link.send(DeliverRequest.newBuilder().setDeclare(topic.declare));
    } catch (RuntimeException e) {
      events.add(new LinkEnded(link, Status.UNAVAILABLE.withDescription("could not open: " + e)));
    }
  }

  private void accepted(Link link, Accepted accepted) {
    Topic topic = link.topic;
    link.accepted = true;
    link.acceptsReturn = accepted.getAcceptsReturn();
    split.opened(link.id);
    topic.metrics.streams(1);
    boolean changed = replicas.offer(accepted.getReplicas());
    if (link.bootstrap) {
      topic.bootstrap = null;
      link.address = accepted.getAddress();
      if (replicas.addresses().contains(link.address) && !topic.links.containsKey(link.address)) {
        // The replica that answered at the configured address is one of the list: this is the
        // stream to it.
        topic.links.put(link.address, link);
        topic.bootstrapBackoff.reset();
      } else {
        // It did not say which replica it is, it is not on the list, or a stream to it exists
        // already. Every listed replica gets a stream of its own, and this one is not needed.
        LOG.info(
            "the stream for topic {} through {} is not to a listed replica ({}); closing it",
            topic.name,
            options.bootstrap(),
            link.address.isEmpty() ? "it gave no address" : link.address);
        close(link);
      }
      changed = true;
    } else {
      topic.retries.remove(link.address);
      topic.bootstrapBackoff.reset();
    }
    if (!link.closed) {
      LOG.info(
          "deliver stream opened: topic {} group {} cluster {} replica {} dispatcher {}, {}",
          topic.name,
          group,
          cluster,
          link.address,
          accepted.getDispatcher().getVersion(),
          describe(accepted.getSettings()));
    }
    if (changed) {
      reconcileAll();
    }
  }

  private static String describe(Settings settings) {
    return "ordering "
        + settings.getOrdering()
        + ", max_processing_ms "
        + settings.getMaxProcessingMs()
        + (settings.getMaxAttempts() == 0 ? "" : ", max_attempts " + settings.getMaxAttempts());
  }

  private void reconcileAll() {
    topics.values().forEach(this::reconcile);
    lostEverything();
    releaseChannels();
  }

  /** Makes a topic's streams match the list: one to every replica on it, none to any other. */
  private void reconcile(Topic topic) {
    if (!replicas.known() || topic.bootstrap != null) {
      // Without a list there is only the configured address. And a stream to it that is still
      // being opened is to one of the replicas: which one is known when it is accepted.
      return;
    }
    topic.bootstrapWaiting = false;
    Set<String> listed = replicas.addresses();
    for (Link link : List.copyOf(topic.links.values())) {
      if (!listed.contains(link.address)) {
        LOG.info(
            "replica {} is no longer listed: closing the stream for topic {}",
            link.address,
            topic.name);
        topic.links.remove(link.address);
        close(link);
      }
    }
    topic.retries.keySet().retainAll(listed);
    for (String address : listed) {
      if (!topic.links.containsKey(address) && !topic.retries.containsKey(address)) {
        open(topic, address, false);
      }
    }
  }

  /** Ends a stream from this side, in good order. What the app held from it is lost to it. */
  private void close(Link link) {
    if (link.closed) {
      return;
    }
    forget(link);
    halfClose(link);
    link.closed = true;
  }

  private static void halfClose(Link link) {
    try {
      if (link.requests != null) {
        link.requests.onCompleted();
      }
    } catch (RuntimeException e) {
      // Already over.
    }
  }

  /** A stream is no longer one: what it delivered cannot be acknowledged any more. */
  private void forget(Link link) {
    int dropped = 0;
    for (Iterator<Pending> all = pending.iterator(); all.hasNext(); ) {
      if (all.next().link() == link) {
        all.remove();
        dropped++;
      }
    }
    split.dropped(dropped);
    split.closed(link.id);
    List<Holdings.Partition> lost = holdings.gone(link.id);
    if (!lost.isEmpty()) {
      // The dispatcher delivers them again, here or elsewhere; the app must drop what it holds.
      send(lost(link.topic.name, lost.stream().map(Holdings.Partition::partition).toList()));
    }
    if (link.accepted) {
      link.topic.metrics.streams(-1);
    }
    linksById.remove(link.id);
  }

  /** A stream ended on the dispatcher's side, or never got there. */
  private void ended(Link link, Status status) {
    if (link.closed) {
      return;
    }
    Topic topic = link.topic;
    boolean unanswered = link.bootstrap && !link.accepted;
    forget(link);
    link.closed = true;
    if (unanswered) {
      topic.bootstrap = null;
    } else {
      topic.links.remove(link.address, link);
    }
    if (permanent(status.getCode())) {
      // Nothing the sidecar can do changes the answer: the app's operator has to know.
      LOG.error(
          "the dispatcher at {} refused topic {} for group {} on cluster {}: {} {}",
          link.target(),
          topic.name,
          group,
          cluster,
          status.getCode(),
          status.getDescription());
      exit =
          Status.fromCode(status.getCode())
              .withDescription(
                  "the dispatcher at "
                      + link.target()
                      + " refused topic "
                      + topic.name
                      + " for group "
                      + group
                      + " on cluster "
                      + cluster
                      + ": "
                      + status.getDescription());
      return;
    }
    LOG.warn(
        "deliver stream for topic {} to {} ended: {} {}; opening it again",
        topic.name,
        link.target(),
        status.getCode(),
        reason(status));
    if (unanswered) {
      if (replicas.known()) {
        reconcile(topic);
      } else {
        scheduleBootstrap(topic);
      }
    } else if (replicas.addresses().contains(link.address)) {
      Retry retry =
          topic.retries.computeIfAbsent(link.address, a -> new Retry(options.backoff().get()));
      retry.waiting = true;
      retry.due = now() + TimeUnit.MILLISECONDS.toNanos(retry.backoff.nextMs());
    }
    lostEverything();
    releaseChannels();
  }

  /**
   * Why a stream ended, on one line. For a connection that was never made, gRPC's description is
   * "io exception" and a dump of the channel's handlers; what happened is the cause underneath,
   * which for a TLS handshake that failed is the one thing an operator needs: which end refused,
   * and what (an identity that is not accepted, an authority that is not trusted, no certificate).
   */
  static String reason(Status status) {
    String description = status.getDescription() == null ? "" : status.getDescription();
    int newline = description.indexOf('\n');
    StringBuilder reason =
        new StringBuilder(newline < 0 ? description : description.substring(0, newline));
    Throwable cause = status.getCause();
    for (int depth = 0; cause != null && cause.getCause() != null && depth < 8; depth++) {
      cause = cause.getCause();
    }
    if (cause != null) {
      reason
          .append(reason.isEmpty() ? "" : ": ")
          .append(cause.getClass().getSimpleName())
          .append(cause.getMessage() == null ? "" : ": " + cause.getMessage());
    }
    return reason.toString();
  }

  /**
   * Statuses that opening the stream again would meet again: a declaration the dispatcher refuses
   * for what it says or for who says it. Everything else, a lost connection included, is retried: a
   * TLS handshake that failed is among it, since gRPC reports that as UNAVAILABLE whichever end
   * refused, and never as one of these.
   */
  static boolean permanent(Status.Code code) {
    return switch (code) {
      case FAILED_PRECONDITION,
          PERMISSION_DENIED,
          UNAUTHENTICATED,
          INVALID_ARGUMENT,
          UNIMPLEMENTED ->
          true;
      default -> false;
    };
  }

  private void scheduleBootstrap(Topic topic) {
    topic.bootstrapWaiting = true;
    topic.bootstrapDue = now() + TimeUnit.MILLISECONDS.toNanos(topic.bootstrapBackoff.nextMs());
  }

  /**
   * With no stream left, open or being opened, the list is worth nothing: the replicas may all have
   * been replaced. Starts again from the configured address.
   */
  private void lostEverything() {
    if (!replicas.known()) {
      return;
    }
    for (Topic topic : topics.values()) {
      if (topic.bootstrap != null || !topic.links.isEmpty()) {
        return;
      }
    }
    LOG.warn("every stream to the dispatcher is lost: starting again from {}", options.bootstrap());
    replicas.forget();
    for (Topic topic : topics.values()) {
      topic.retries.clear();
      scheduleBootstrap(topic);
    }
  }

  /** Closes the connections to addresses that no stream uses and no list names. */
  private void releaseChannels() {
    Set<String> used = new HashSet<>(replicas.addresses());
    used.add(options.bootstrap());
    for (Link link : linksById.values()) {
      used.add(link.target());
    }
    for (Iterator<Map.Entry<String, ManagedChannel>> all = channels.entrySet().iterator();
        all.hasNext(); ) {
      Map.Entry<String, ManagedChannel> entry = all.next();
      if (!used.contains(entry.getKey())) {
        // Not cut: a stream just closed on it finishes in good order.
        connector.release(entry.getKey(), entry.getValue());
        all.remove();
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // The end
  // ---------------------------------------------------------------------------------------------

  /**
   * Ends everything. Nothing more is granted, and what the app acknowledged before this was passed
   * on in the order it arrived. Each Deliver stream is then half-closed, which is a clean leave:
   * the dispatcher delivers again what was held here, under the same attempt numbers.
   */
  private void finish(Status end) {
    for (Link link : List.copyOf(linksById.values())) {
      if (!link.closed) {
        halfClose(link);
        link.closed = true;
        if (link.accepted) {
          link.topic.metrics.streams(-1);
        }
      }
    }
    linksById.clear();
    for (Topic topic : topics.values()) {
      topic.metrics.wanted(-topic.reportedWanted);
      topic.reportedWanted = 0;
    }
    decoders.forEach(RecordDecoder::close);
    listener.ended(this, cancelled ? Status.Code.CANCELLED.name() : end.getCode().name());
    if (!cancelled) {
      if (end.isOk()) {
        call.onCompleted();
      } else {
        call.onError(end.asRuntimeException());
      }
    }
    channels.forEach(connector::release);
    connector.drained(List.copyOf(channels.values()), options.drain());
    channels.clear();
  }
}
