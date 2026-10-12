package io.github.bpalermo.odradek.client.core;

import io.github.bpalermo.odradek.client.v1.ConsumeAction;
import io.github.bpalermo.odradek.client.v1.ConsumeEvent;
import io.github.bpalermo.odradek.client.v1.ConsumeSetup;
import io.github.bpalermo.odradek.client.v1.Fatal;
import io.github.bpalermo.odradek.client.v1.HalfClose;
import io.github.bpalermo.odradek.client.v1.OpenStream;
import io.github.bpalermo.odradek.client.v1.StreamEnded;
import io.github.bpalermo.odradek.kafka.v1.Ack;
import io.github.bpalermo.odradek.kafka.v1.Assigned;
import io.github.bpalermo.odradek.kafka.v1.ConsumeRequest;
import io.github.bpalermo.odradek.kafka.v1.ConsumedRecord;
import io.github.bpalermo.odradek.kafka.v1.Credits;
import io.github.bpalermo.odradek.kafka.v1.Lost;
import io.github.bpalermo.odradek.kafka.v1.PartitionAssignment;
import io.github.bpalermo.odradek.kafka.v1.RecordBatch;
import io.github.bpalermo.odradek.kafka.v1.RevokeDone;
import io.github.bpalermo.odradek.kafka.v1.Revoked;
import io.github.bpalermo.odradek.kafka.v1.Subscribe;
import io.github.bpalermo.odradek.kafka.v1.TopicPartition;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * The consume side of the client protocol, with no I/O: events in, actions out.
 *
 * <p>It subscribes, grants the server credits so that the app never holds more unacked records than
 * it asked for, forwards an ack only while it still means something, and tracks which partitions
 * are owned so that it can tell the app what it lost when a stream ends.
 *
 * <p>Not thread-safe: the binding calls {@link #handle} from one thread at a time.
 */
public final class ConsumeCore {
  static final int DEFAULT_MAX_UNACKED_RECORDS = 500;

  /** Stream statuses that a new stream would meet again, or that the app must hear about. */
  private static final Set<String> FATAL =
      Set.of(
          "INVALID_ARGUMENT",
          "NOT_FOUND",
          "FAILED_PRECONDITION",
          "ABORTED",
          "DATA_LOSS",
          "PERMISSION_DENIED",
          "UNAUTHENTICATED",
          "UNIMPLEMENTED");

  private static final Comparator<TopicPartition> ORDER =
      Comparator.comparing(TopicPartition::getTopic).thenComparingInt(TopicPartition::getPartition);

  private enum State {
    DISCONNECTED,
    /** A stream is open and Subscribe was sent; Subscribed has not arrived. */
    SUBSCRIBING,
    ACTIVE,
    FAILED
  }

  private static final class Owned {
    final long epoch;
    final TreeSet<Long> unacked = new TreeSet<>();

    Owned(long epoch) {
      this.epoch = epoch;
    }
  }

  private final ConsumeSetup setup;
  private final int maxUnacked;
  private final TreeMap<TopicPartition, Owned> owned = new TreeMap<>(ORDER);
  private final Map<Long, List<TopicPartition>> revoking = new HashMap<>();
  private State state = State.DISCONNECTED;
  private boolean closed;
  private boolean halfClosed;

  /** Records the app holds: delivered, not yet acked, on partitions still owned. */
  private int unacked;

  /** Credits granted to the server and not yet used by a delivered record. */
  private long outstanding;

  public ConsumeCore(ConsumeSetup setup) {
    if (setup.getTopicsCount() == 0) {
      throw new IllegalArgumentException("ConsumeSetup.topics is empty");
    }
    this.setup = setup;
    this.maxUnacked =
        setup.hasMaxUnackedRecords() ? setup.getMaxUnackedRecords() : DEFAULT_MAX_UNACKED_RECORDS;
    if (maxUnacked < 1) {
      throw new IllegalArgumentException("ConsumeSetup.max_unacked_records must be positive");
    }
  }

  /** Applies one event and returns what the binding must do, in order. */
  public List<ConsumeAction> handle(ConsumeEvent event) {
    List<ConsumeAction> actions = new ArrayList<>();
    switch (event.getKindCase()) {
      case STREAM_OPENED -> streamOpened(actions);
      case RECEIVED -> {
        var message = event.getReceived();
        switch (message.getMsgCase()) {
          case SUBSCRIBED -> subscribed(actions);
          case ASSIGNED -> assigned(message.getAssigned(), actions);
          case RECORDS -> records(message.getRecords(), actions);
          case REVOKED -> revoked(message.getRevoked(), actions);
          case LOST -> lost(message.getLost(), actions);
          case MSG_NOT_SET -> {}
        }
      }
      case STREAM_ENDED -> streamEnded(event.getStreamEnded(), actions);
      case ACK -> ack(event.getAck(), actions);
      case REVOKE_DONE -> revokeDone(event.getRevokeDone(), actions);
      case CLOSE -> close(actions);
      case KIND_NOT_SET -> {}
    }
    return actions;
  }

  private void streamOpened(List<ConsumeAction> actions) {
    if (state == State.FAILED) {
      return;
    }
    state = State.SUBSCRIBING;
    halfClosed = false;
    Subscribe.Builder subscribe = Subscribe.newBuilder().addAllTopics(setup.getTopicsList());
    if (setup.hasMaxProcessingMs()) {
      subscribe.setMaxProcessingMs(setup.getMaxProcessingMs());
    }
    if (setup.hasClient()) {
      subscribe.setClient(setup.getClient());
    }
    actions.add(transmit(ConsumeRequest.newBuilder().setSubscribe(subscribe).build()));
    if (closed) {
      halfClose(actions);
    }
  }

  private void subscribed(List<ConsumeAction> actions) {
    if (state != State.SUBSCRIBING) {
      return;
    }
    state = State.ACTIVE;
    outstanding = 0;
    grant(actions);
  }

  private void assigned(Assigned assigned, List<ConsumeAction> actions) {
    for (PartitionAssignment partition : assigned.getPartitionsList()) {
      Owned previous =
          owned.put(
              key(partition.getTopic(), partition.getPartition()),
              new Owned(partition.getAssignmentEpoch()));
      if (previous != null) {
        unacked -= previous.unacked.size();
      }
    }
    actions.add(ConsumeAction.newBuilder().setAssigned(assigned).build());
    grant(actions);
  }

  private void records(RecordBatch batch, List<ConsumeAction> actions) {
    // Each record used one credit, whether or not it can still be handed to the app.
    outstanding = Math.max(0, outstanding - batch.getRecordsCount());
    Owned partition = owned.get(key(batch.getTopic(), batch.getPartition()));
    if (partition == null || partition.epoch != batch.getAssignmentEpoch()) {
      // Sent before the server learned the partition was given up. Its records will reach
      // whoever owns the partition next.
      grant(actions);
      return;
    }
    for (ConsumedRecord record : batch.getRecordsList()) {
      if (partition.unacked.add(record.getOffset())) {
        unacked++;
      }
    }
    actions.add(ConsumeAction.newBuilder().setDeliver(batch).build());
  }

  private void revoked(Revoked revoked, List<ConsumeAction> actions) {
    // The partitions stay owned until the app answers: its final acks must still go through.
    revoking.put(revoked.getRevokeId(), revoked.getPartitionsList());
    actions.add(ConsumeAction.newBuilder().setRevoking(revoked).build());
  }

  private void revokeDone(RevokeDone done, List<ConsumeAction> actions) {
    List<TopicPartition> partitions = revoking.remove(done.getRevokeId());
    if (partitions == null) {
      return;
    }
    partitions.forEach(this::release);
    actions.add(transmit(ConsumeRequest.newBuilder().setRevokeDone(done).build()));
    grant(actions);
  }

  private void lost(Lost lost, List<ConsumeAction> actions) {
    lost.getPartitionsList().forEach(this::release);
    actions.add(ConsumeAction.newBuilder().setLost(lost).build());
    grant(actions);
  }

  private void ack(Ack ack, List<ConsumeAction> actions) {
    if (state != State.ACTIVE) {
      return;
    }
    Owned partition = owned.get(key(ack.getTopic(), ack.getPartition()));
    if (partition == null || partition.epoch != ack.getAssignmentEpoch()) {
      // The partition was given up since these records were delivered. They will be delivered
      // again to its next owner; the server would ignore this ack anyway.
      return;
    }
    var range = partition.unacked.subSet(ack.getFirstOffset(), true, ack.getLastOffset(), true);
    int count = range.size();
    if (count == 0) {
      return;
    }
    range.clear();
    unacked -= count;
    actions.add(transmit(ConsumeRequest.newBuilder().setAck(ack).build()));
    grant(actions);
  }

  private void streamEnded(StreamEnded ended, List<ConsumeAction> actions) {
    if (state == State.FAILED) {
      return;
    }
    boolean wasOpen = state != State.DISCONNECTED;
    state = State.DISCONNECTED;
    if (!owned.isEmpty()) {
      // Whatever the app holds will be delivered again, here or elsewhere.
      actions.add(
          ConsumeAction.newBuilder()
              .setLost(Lost.newBuilder().addAllPartitions(owned.keySet()))
              .build());
    }
    owned.clear();
    revoking.clear();
    unacked = 0;
    outstanding = 0;
    if (FATAL.contains(ended.getStatusCode())) {
      state = State.FAILED;
      actions.add(
          ConsumeAction.newBuilder()
              .setFatal(
                  Fatal.newBuilder()
                      .setStatusCode(ended.getStatusCode())
                      .setMessage(ended.getMessage()))
              .build());
      return;
    }
    if (!closed && wasOpen) {
      actions.add(
          ConsumeAction.newBuilder().setOpenStream(OpenStream.getDefaultInstance()).build());
    }
  }

  private void close(List<ConsumeAction> actions) {
    closed = true;
    if (state == State.SUBSCRIBING || state == State.ACTIVE) {
      halfClose(actions);
    }
  }

  private void halfClose(List<ConsumeAction> actions) {
    if (!halfClosed) {
      halfClosed = true;
      actions.add(ConsumeAction.newBuilder().setHalfClose(HalfClose.getDefaultInstance()).build());
    }
  }

  private void release(TopicPartition partition) {
    Owned gone = owned.remove(partition);
    if (gone != null) {
      unacked -= gone.unacked.size();
    }
  }

  /**
   * Grants credits when at least half of the app's allowance is free. Granting in halves keeps the
   * stream from carrying one Credits message per acked record.
   */
  private void grant(List<ConsumeAction> actions) {
    if (state != State.ACTIVE || closed) {
      return;
    }
    long free = maxUnacked - unacked - outstanding;
    if (free * 2 >= maxUnacked && free > 0) {
      outstanding += free;
      actions.add(
          transmit(
              ConsumeRequest.newBuilder()
                  .setCredits(Credits.newBuilder().setRecords((int) free))
                  .build()));
    }
  }

  private static TopicPartition key(String topic, int partition) {
    return TopicPartition.newBuilder().setTopic(topic).setPartition(partition).build();
  }

  private static ConsumeAction transmit(ConsumeRequest request) {
    return ConsumeAction.newBuilder().setTransmit(request).build();
  }
}
