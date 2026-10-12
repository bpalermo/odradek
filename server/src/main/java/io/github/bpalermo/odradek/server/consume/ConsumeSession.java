package io.github.bpalermo.odradek.server.consume;

import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import io.github.bpalermo.odradek.config.v1.Consumer;
import io.github.bpalermo.odradek.config.v1.OffsetReset;
import io.github.bpalermo.odradek.config.v1.OnDecodeError;
import io.github.bpalermo.odradek.kafka.v1.Ack;
import io.github.bpalermo.odradek.kafka.v1.Assigned;
import io.github.bpalermo.odradek.kafka.v1.ConsumeResponse;
import io.github.bpalermo.odradek.kafka.v1.ConsumedRecord;
import io.github.bpalermo.odradek.kafka.v1.Header;
import io.github.bpalermo.odradek.kafka.v1.Lost;
import io.github.bpalermo.odradek.kafka.v1.PartitionAssignment;
import io.github.bpalermo.odradek.kafka.v1.RecordBatch;
import io.github.bpalermo.odradek.kafka.v1.Revoked;
import io.github.bpalermo.odradek.server.codec.RecordDecodeException;
import io.github.bpalermo.odradek.server.codec.RecordDecoder;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.InvalidOffsetException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One Consume stream after its handshake: a Kafka consumer and the thread that owns it.
 *
 * <p>Everything that touches the consumer, the partition states or the response stream runs on that
 * thread. The gRPC side only puts commands on a queue.
 */
final class ConsumeSession implements ConsumerRebalanceListener {
  private static final Logger LOG = LoggerFactory.getLogger(ConsumeSession.class);
  private static final Duration POLL = Duration.ofMillis(50);

  /** Something the app, or the server, asks of the poll thread. */
  sealed interface Command permits Credit, Acked, RevokeDone, HalfClose, Cancel, Fail, Shutdown {}

  record Credit(long records) implements Command {}

  record Acked(Ack ack) implements Command {}

  record RevokeDone(long revokeId) implements Command {}

  /** The app finished its side of the stream. */
  record HalfClose() implements Command {}

  /** The stream is gone; nothing more can be sent. */
  record Cancel() implements Command {}

  /** The app broke the protocol. */
  record Fail(Status status) implements Command {}

  /** The server is stopping. */
  record Shutdown() implements Command {}

  private final ServerCallStreamObserver<ConsumeResponse> call;
  private final org.apache.kafka.clients.consumer.Consumer<byte[], byte[]> consumer;
  private final List<String> topics;
  private final Map<String, Consumer> entries;
  private final Map<String, RecordDecoder> decoders;
  private final long maxProcessingNanos;
  private final long revokeDeadlineMs;
  private final long commitIntervalNanos;
  private final LongSupplier nanoClock;
  private final java.util.function.Consumer<ConsumeSession> onEnd;
  private final LinkedBlockingQueue<Command> commands = new LinkedBlockingQueue<>();
  private final Map<TopicPartition, PartitionState> states = new HashMap<>();
  private final Thread thread = new Thread(this::run);
  private volatile boolean cancelled;
  private long credits;
  private long epochCounter;
  private long revokeCounter;
  private long lastCommitNanos;
  private boolean closing;

  /** Set when a command that ends the stream arrives while a revocation is being handled. */
  private Status exit;

  ConsumeSession(
      ServerCallStreamObserver<ConsumeResponse> call,
      org.apache.kafka.clients.consumer.Consumer<byte[], byte[]> consumer,
      List<String> topics,
      Map<String, Consumer> entries,
      Map<String, RecordDecoder> decoders,
      Duration maxProcessing,
      long revokeDeadlineMs,
      Duration commitInterval,
      LongSupplier nanoClock,
      java.util.function.Consumer<ConsumeSession> onEnd) {
    this.call = call;
    this.consumer = consumer;
    this.topics = topics;
    this.entries = entries;
    this.decoders = decoders;
    this.maxProcessingNanos = maxProcessing.toNanos();
    this.revokeDeadlineMs = revokeDeadlineMs;
    this.commitIntervalNanos = commitInterval.toNanos();
    this.nanoClock = nanoClock;
    this.onEnd = onEnd;
  }

  void start(String name) {
    thread.setName(name);
    thread.setDaemon(true);
    thread.start();
  }

  void submit(Command command) {
    commands.add(command);
  }

  void cancelled() {
    cancelled = true;
    commands.add(new Cancel());
  }

  void join(long millis) throws InterruptedException {
    thread.join(millis);
  }

  private void run() {
    Status end;
    try {
      end = loop();
    } catch (RuntimeException e) {
      LOG.error("consume stream failed", e);
      end = Status.INTERNAL.withDescription(e.toString());
    }
    close(end);
  }

  /** Runs until the stream has to end; returns how. */
  private Status loop() {
    consumer.subscribe(topics, this);
    lastCommitNanos = nanoClock.getAsLong();
    boolean idle = false;
    while (true) {
      // A mock consumer returns from poll at once; waiting here keeps the loop from spinning.
      Command command = idle ? await(5) : commands.poll();
      while (command != null) {
        handle(command);
        if (exit != null) {
          return exit;
        }
        command = commands.poll();
      }
      if (credits > 0) {
        consumer.resume(consumer.paused());
      } else {
        consumer.pause(consumer.assignment());
      }

      long before = System.nanoTime();
      ConsumerRecords<byte[], byte[]> records;
      try {
        records = consumer.poll(POLL);
      } catch (InvalidOffsetException e) {
        // A committed offset that retention has since removed, or a partition the assignment
        // callback could not position.
        e.partitions().forEach(partition -> reset(partition, false));
        continue;
      }
      if (exit != null) {
        return exit;
      }
      Status failure = deliver(records);
      if (failure != null) {
        return failure;
      }
      long now = nanoClock.getAsLong();
      for (PartitionState state : states.values()) {
        long oldest = state.oldestUnackedNanos();
        if (oldest >= 0 && now - oldest > maxProcessingNanos) {
          return Status.ABORTED.withDescription(
              "a record of "
                  + state.partition
                  + " was not acked within max_processing_ms; the app is considered hung");
        }
      }
      if (now - lastCommitNanos >= commitIntervalNanos) {
        lastCommitNanos = now;
        commitAsync();
      }
      idle = records.isEmpty() && System.nanoTime() - before < 1_000_000;
    }
  }

  private Command await(long millis) {
    try {
      return commands.poll(millis, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return new Shutdown();
    }
  }

  private void handle(Command command) {
    switch (command) {
      case Credit credit -> credits += credit.records();
      case Acked acked -> ack(acked.ack());
      // Outside a revocation there is nothing to answer; a late RevokeDone is harmless.
      case RevokeDone done -> {}
      case HalfClose close -> exit = Status.OK;
      case Cancel cancel -> exit = Status.CANCELLED;
      case Fail fail -> exit = fail.status();
      case Shutdown shutdown ->
          exit = Status.UNAVAILABLE.withDescription("the server is shutting down");
    }
  }

  private void ack(Ack ack) {
    PartitionState state = states.get(new TopicPartition(ack.getTopic(), ack.getPartition()));
    // An ack for a partition no longer owned, or for an earlier ownership of it, says nothing
    // about the records delivered under the current one.
    if (state != null && state.epoch == ack.getAssignmentEpoch()) {
      state.ack(ack.getFirstOffset(), ack.getLastOffset());
    }
  }

  /** Sends what the credits allow and rewinds the consumer over the rest. */
  private Status deliver(ConsumerRecords<byte[], byte[]> records) {
    for (TopicPartition partition : records.partitions()) {
      PartitionState state = states.get(partition);
      List<ConsumerRecord<byte[], byte[]>> fetched = records.records(partition);
      if (state == null || credits == 0) {
        consumer.seek(partition, fetched.get(0).offset());
        continue;
      }
      RecordDecoder decoder = decoders.get(partition.topic());
      boolean deliverUndecoded =
          entries.get(partition.topic()).getOnDecodeError()
              == OnDecodeError.ON_DECODE_ERROR_DELIVER_UNDECODED;
      RecordBatch.Builder batch =
          RecordBatch.newBuilder()
              .setTopic(partition.topic())
              .setPartition(partition.partition())
              .setAssignmentEpoch(state.epoch);
      Status failure = null;
      long now = nanoClock.getAsLong();
      for (ConsumerRecord<byte[], byte[]> record : fetched) {
        if (credits == 0) {
          consumer.seek(partition, record.offset());
          break;
        }
        ConsumedRecord.Builder out =
            ConsumedRecord.newBuilder()
                .setOffset(record.offset())
                .setTimestampMs(record.timestamp());
        try {
          Headers headers = new RecordHeaders(record.headers().toArray());
          byte[] key = decoder.decodeKey(headers, record.key());
          byte[] value = decoder.decodeValue(headers, record.value());
          fill(out, key, value, headers);
        } catch (RecordDecodeException e) {
          if (!deliverUndecoded) {
            consumer.seek(partition, record.offset());
            failure =
                Status.DATA_LOSS.withDescription(
                    partition + " offset " + record.offset() + ": " + e.getMessage());
            break;
          }
          fill(out, record.key(), record.value(), record.headers());
          out.setDecodeError(e.getMessage());
        }
        batch.addRecords(out);
        state.delivered(record.offset(), now);
        credits--;
      }
      if (batch.getRecordsCount() > 0) {
        send(ConsumeResponse.newBuilder().setRecords(batch).build());
      }
      if (failure != null) {
        return failure;
      }
    }
    return null;
  }

  private static void fill(ConsumedRecord.Builder out, byte[] key, byte[] value, Headers headers) {
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
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    if (partitions.isEmpty()) {
      return;
    }
    Assigned.Builder assigned = Assigned.newBuilder();
    for (TopicPartition partition : partitions) {
      PartitionState state = new PartitionState(partition, ++epochCounter);
      states.put(partition, state);
      assigned.addPartitions(
          PartitionAssignment.newBuilder()
              .setTopic(partition.topic())
              .setPartition(partition.partition())
              .setAssignmentEpoch(state.epoch));
    }
    Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(Set.copyOf(partitions));
    for (TopicPartition partition : partitions) {
      if (committed.get(partition) == null) {
        reset(partition, true);
      }
    }
    if (credits == 0) {
      consumer.pause(partitions);
    }
    send(ConsumeResponse.newBuilder().setAssigned(assigned).build());
  }

  /**
   * Positions a partition that has no usable committed offset: at the topic's start timestamp when
   * the group has never committed and one is configured, otherwise by its offset reset.
   */
  private void reset(TopicPartition partition, boolean neverCommitted) {
    Consumer entry = entries.get(partition.topic());
    if (neverCommitted && entry.hasStartTimestamp()) {
      long millis = Timestamps.toMillis(entry.getStartTimestamp());
      OffsetAndTimestamp found = consumer.offsetsForTimes(Map.of(partition, millis)).get(partition);
      if (found != null) {
        consumer.seek(partition, found.offset());
        return;
      }
    }
    if (entry.getOffsetReset() == OffsetReset.OFFSET_RESET_LATEST) {
      consumer.seekToEnd(List.of(partition));
    } else {
      consumer.seekToBeginning(List.of(partition));
    }
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    List<PartitionState> owned = new ArrayList<>();
    for (TopicPartition partition : partitions) {
      PartitionState state = states.get(partition);
      if (state != null) {
        owned.add(state);
      }
    }
    if (owned.isEmpty()) {
      return;
    }
    if (!closing && !cancelled) {
      long revokeId = ++revokeCounter;
      Revoked.Builder revoked =
          Revoked.newBuilder().setRevokeId(revokeId).setDeadlineMs((int) revokeDeadlineMs);
      owned.forEach(state -> revoked.addPartitions(proto(state.partition)));
      send(ConsumeResponse.newBuilder().setRevoked(revoked).build());

      // The app finishes what it holds and acks it; those acks are applied here, so they are in
      // the commit below. Nothing new is delivered meanwhile: this runs inside poll().
      long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(revokeDeadlineMs);
      boolean done = false;
      while (!done && exit == null) {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
          LOG.warn("no RevokeDone for revocation {} within {} ms", revokeId, revokeDeadlineMs);
          break;
        }
        Command command = await(TimeUnit.NANOSECONDS.toMillis(remaining) + 1);
        if (command instanceof RevokeDone answer && answer.revokeId() == revokeId) {
          done = true;
        } else if (command != null) {
          handle(command);
        }
      }
    }
    commitSync(owned);
    owned.forEach(state -> states.remove(state.partition));
  }

  @Override
  public void onPartitionsLost(Collection<TopicPartition> partitions) {
    Lost.Builder lost = Lost.newBuilder();
    for (TopicPartition partition : partitions) {
      if (states.remove(partition) != null) {
        lost.addPartitions(proto(partition));
      }
    }
    if (lost.getPartitionsCount() > 0) {
      send(ConsumeResponse.newBuilder().setLost(lost).build());
    }
  }

  private static io.github.bpalermo.odradek.kafka.v1.TopicPartition proto(
      TopicPartition partition) {
    return io.github.bpalermo.odradek.kafka.v1.TopicPartition.newBuilder()
        .setTopic(partition.topic())
        .setPartition(partition.partition())
        .build();
  }

  private Map<TopicPartition, OffsetAndMetadata> dirtyOffsets(Collection<PartitionState> of) {
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    for (PartitionState state : of) {
      if (state.dirty()) {
        offsets.put(state.partition, new OffsetAndMetadata(state.commitOffset()));
        state.committed();
      }
    }
    return offsets;
  }

  private void commitAsync() {
    Map<TopicPartition, OffsetAndMetadata> offsets = dirtyOffsets(states.values());
    if (offsets.isEmpty()) {
      return;
    }
    consumer.commitAsync(
        offsets,
        (committed, failure) -> {
          if (failure != null) {
            // Not retried here: the next interval, a revocation or the close commits a position
            // at least as high.
            LOG.warn("asynchronous commit of {} failed", committed, failure);
          }
        });
  }

  private void commitSync(Collection<PartitionState> of) {
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    // Everything acked so far, not only what moved since the last asynchronous commit: that
    // commit may have failed, and this is the last chance for these partitions.
    for (PartitionState state : of) {
      if (state.commitOffset() >= 0) {
        offsets.put(state.partition, new OffsetAndMetadata(state.commitOffset()));
        state.committed();
      }
    }
    if (offsets.isEmpty()) {
      return;
    }
    try {
      consumer.commitSync(offsets);
    } catch (RuntimeException e) {
      LOG.warn(
          "commit of {} failed; unacked and uncommitted records will be redelivered", offsets, e);
    }
  }

  private void send(ConsumeResponse response) {
    if (!cancelled) {
      call.onNext(response);
    }
  }

  private void close(Status end) {
    closing = true;
    try {
      commitSync(List.copyOf(states.values()));
    } catch (RuntimeException e) {
      LOG.warn("final commit failed", e);
    }
    try {
      consumer.close(CloseOptions.timeout(Duration.ofSeconds(10)));
    } catch (RuntimeException e) {
      LOG.warn("closing the consumer failed", e);
    }
    decoders.values().forEach(RecordDecoder::close);
    onEnd.accept(this);
    if (cancelled) {
      return;
    }
    if (end.isOk()) {
      call.onCompleted();
    } else {
      call.onError(end.asRuntimeException());
    }
  }
}
