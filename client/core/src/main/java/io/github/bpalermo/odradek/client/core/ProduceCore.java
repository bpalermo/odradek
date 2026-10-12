package io.github.bpalermo.odradek.client.core;

import io.github.bpalermo.odradek.client.v1.Fatal;
import io.github.bpalermo.odradek.client.v1.HalfClose;
import io.github.bpalermo.odradek.client.v1.OpenStream;
import io.github.bpalermo.odradek.client.v1.ProduceAction;
import io.github.bpalermo.odradek.client.v1.ProduceEvent;
import io.github.bpalermo.odradek.client.v1.ProduceSetup;
import io.github.bpalermo.odradek.client.v1.SendAccepted;
import io.github.bpalermo.odradek.client.v1.SendCompleted;
import io.github.bpalermo.odradek.client.v1.SendRejected;
import io.github.bpalermo.odradek.client.v1.SendRejection;
import io.github.bpalermo.odradek.client.v1.StreamEnded;
import io.github.bpalermo.odradek.kafka.v1.Error;
import io.github.bpalermo.odradek.kafka.v1.ErrorCode;
import io.github.bpalermo.odradek.kafka.v1.Header;
import io.github.bpalermo.odradek.kafka.v1.ProduceOpen;
import io.github.bpalermo.odradek.kafka.v1.ProduceReady;
import io.github.bpalermo.odradek.kafka.v1.ProduceRecord;
import io.github.bpalermo.odradek.kafka.v1.ProduceRequest;
import io.github.bpalermo.odradek.kafka.v1.ProduceResult;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

/**
 * The produce side of the client protocol, with no I/O: events in, actions out.
 *
 * <p>It assigns sequence numbers, keeps every record until its result arrives, stays inside the
 * window the server announced, resends after a reconnect, and gives up on records the server can no
 * longer answer for. A binding opens the first stream on its own, reports what happens as events
 * and carries out the returned actions in order.
 *
 * <p>Not thread-safe: the binding calls {@link #handle} from one thread at a time.
 */
public final class ProduceCore {
  static final int DEFAULT_MAX_BUFFERED_RECORDS = 1000;
  static final long DEFAULT_MAX_BUFFERED_BYTES = 16L * 1024 * 1024;

  /** Stream statuses that a new stream would meet again. */
  private static final Set<String> FATAL =
      Set.of(
          "INVALID_ARGUMENT",
          "ABORTED",
          "PERMISSION_DENIED",
          "UNAUTHENTICATED",
          "UNIMPLEMENTED",
          "FAILED_PRECONDITION");

  private enum State {
    /** No stream. */
    DISCONNECTED,
    /** A stream is open and ProduceOpen was sent; ProduceReady has not arrived. */
    OPENING,
    /** Records may be transmitted. */
    READY,
    /** The server refused the producer for good. */
    FAILED
  }

  private static final class Pending {
    final ProduceRecord record;
    final long bytes;
    boolean transmitted;

    /** Whether the record was ever written to a stream; if so the server may have produced it. */
    boolean everTransmitted;

    Pending(ProduceRecord record, long bytes) {
      this.record = record;
      this.bytes = bytes;
    }
  }

  private final ProduceSetup setup;
  private final TreeMap<Long, Pending> pending = new TreeMap<>();
  private State state = State.DISCONNECTED;
  private long nextSeq = 1;
  private long pendingBytes;
  private int inFlightRecords;
  private long inFlightBytes;
  private long maxRecords;
  private long maxBytes;
  private String serverEpoch;
  private boolean closed;
  private boolean halfClosed;

  public ProduceCore(ProduceSetup setup) {
    if (setup.getSessionId().isEmpty()) {
      throw new IllegalArgumentException("ProduceSetup.session_id is required");
    }
    this.setup = setup;
    this.maxRecords =
        setup.hasMaxBufferedRecords()
            ? setup.getMaxBufferedRecords()
            : DEFAULT_MAX_BUFFERED_RECORDS;
    this.maxBytes =
        setup.hasMaxBufferedBytes() ? setup.getMaxBufferedBytes() : DEFAULT_MAX_BUFFERED_BYTES;
  }

  /** Applies one event and returns what the binding must do, in order. */
  public List<ProduceAction> handle(ProduceEvent event) {
    List<ProduceAction> actions = new ArrayList<>();
    switch (event.getKindCase()) {
      case STREAM_OPENED -> streamOpened(actions);
      case RECEIVED -> {
        switch (event.getReceived().getMsgCase()) {
          case READY -> ready(event.getReceived().getReady(), actions);
          case RESULT -> result(event.getReceived().getResult(), actions);
          case MSG_NOT_SET -> {}
        }
      }
      case STREAM_ENDED -> streamEnded(event.getStreamEnded(), actions);
      case SEND -> send(event.getSend(), actions);
      case CLOSE -> close(actions);
      case KIND_NOT_SET -> {}
    }
    return actions;
  }

  private void streamOpened(List<ProduceAction> actions) {
    if (state == State.FAILED) {
      return;
    }
    state = State.OPENING;
    halfClosed = false;
    ProduceOpen.Builder open = ProduceOpen.newBuilder().setSessionId(setup.getSessionId());
    if (setup.hasClient()) {
      open.setClient(setup.getClient());
    }
    actions.add(transmit(ProduceRequest.newBuilder().setOpen(open).build()));
  }

  private void ready(ProduceReady ready, List<ProduceAction> actions) {
    if (state != State.OPENING) {
      return;
    }
    if (serverEpoch != null && !serverEpoch.equals(ready.getServerEpoch())) {
      // The server restarted and forgot the session. A record it was sent may or may not have
      // been produced, and it can no longer say. Resending could duplicate, so that is left to
      // the app. Records never sent are safe to send now.
      for (var it = pending.values().iterator(); it.hasNext(); ) {
        Pending record = it.next();
        if (record.everTransmitted) {
          it.remove();
          pendingBytes -= record.bytes;
          actions.add(
              completed(
                  record.record.getSeq(),
                  "the server restarted before answering; the record may have been produced"));
        }
      }
    }
    serverEpoch = ready.getServerEpoch();
    maxRecords = ready.getMaxInFlightRecords();
    maxBytes = ready.getMaxInFlightBytes();
    state = State.READY;
    pump(actions);
    finishIfDrained(actions);
  }

  private void result(ProduceResult result, List<ProduceAction> actions) {
    Pending record = pending.remove(result.getSeq());
    if (record == null) {
      // A result repeated after a resend, or for a record already given up on.
      return;
    }
    pendingBytes -= record.bytes;
    if (record.transmitted) {
      inFlightRecords--;
      inFlightBytes -= record.bytes;
    }
    actions.add(
        ProduceAction.newBuilder()
            .setCompleted(SendCompleted.newBuilder().setResult(result))
            .build());
    pump(actions);
    finishIfDrained(actions);
  }

  private void streamEnded(StreamEnded ended, List<ProduceAction> actions) {
    if (state == State.FAILED) {
      return;
    }
    boolean wasOpen = state != State.DISCONNECTED;
    state = State.DISCONNECTED;
    inFlightRecords = 0;
    inFlightBytes = 0;
    pending.values().forEach(record -> record.transmitted = false);
    if (FATAL.contains(ended.getStatusCode())) {
      state = State.FAILED;
      for (Pending record : pending.values()) {
        actions.add(
            completed(
                record.record.getSeq(),
                record.everTransmitted
                    ? "the stream ended with "
                        + ended.getStatusCode()
                        + " before the record was answered; it may have been produced"
                    : "the stream ended with "
                        + ended.getStatusCode()
                        + "; the record was not sent"));
      }
      pending.clear();
      pendingBytes = 0;
      actions.add(
          ProduceAction.newBuilder()
              .setFatal(
                  Fatal.newBuilder()
                      .setStatusCode(ended.getStatusCode())
                      .setMessage(ended.getMessage()))
              .build());
      return;
    }
    if (closed && pending.isEmpty()) {
      return;
    }
    if (wasOpen) {
      actions.add(
          ProduceAction.newBuilder().setOpenStream(OpenStream.getDefaultInstance()).build());
    }
  }

  private void send(ProduceRecord record, List<ProduceAction> actions) {
    if (closed || state == State.FAILED) {
      actions.add(rejected(SendRejection.SEND_REJECTION_CLOSED));
      return;
    }
    long bytes = size(record);
    if (bytes > maxBytes) {
      actions.add(rejected(SendRejection.SEND_REJECTION_TOO_LARGE));
      return;
    }
    if (pending.size() >= maxRecords || pendingBytes + bytes > maxBytes) {
      actions.add(rejected(SendRejection.SEND_REJECTION_WINDOW_FULL));
      return;
    }
    long seq = nextSeq++;
    pending.put(seq, new Pending(record.toBuilder().setSeq(seq).build(), bytes));
    pendingBytes += bytes;
    actions.add(
        ProduceAction.newBuilder().setAccepted(SendAccepted.newBuilder().setSeq(seq)).build());
    pump(actions);
  }

  private void close(List<ProduceAction> actions) {
    closed = true;
    finishIfDrained(actions);
  }

  /** Transmits waiting records, oldest first, for as long as the server's window has room. */
  private void pump(List<ProduceAction> actions) {
    if (state != State.READY) {
      return;
    }
    long lowWatermark = pending.isEmpty() ? 0 : pending.firstKey();
    for (Pending record : pending.values()) {
      if (record.transmitted) {
        continue;
      }
      if (inFlightRecords >= maxRecords || inFlightBytes + record.bytes > maxBytes) {
        // In order, always: a later record never overtakes one that does not fit yet.
        return;
      }
      record.transmitted = true;
      record.everTransmitted = true;
      inFlightRecords++;
      inFlightBytes += record.bytes;
      actions.add(
          transmit(
              ProduceRequest.newBuilder()
                  .setRecord(record.record.toBuilder().setLowWatermark(lowWatermark))
                  .build()));
    }
  }

  private void finishIfDrained(List<ProduceAction> actions) {
    if (closed
        && !halfClosed
        && pending.isEmpty()
        && state != State.DISCONNECTED
        && state != State.FAILED) {
      halfClosed = true;
      actions.add(ProduceAction.newBuilder().setHalfClose(HalfClose.getDefaultInstance()).build());
    }
  }

  private static ProduceAction transmit(ProduceRequest request) {
    return ProduceAction.newBuilder().setTransmit(request).build();
  }

  private static ProduceAction rejected(SendRejection reason) {
    return ProduceAction.newBuilder()
        .setRejected(SendRejected.newBuilder().setReason(reason))
        .build();
  }

  private static ProduceAction completed(long seq, String message) {
    return ProduceAction.newBuilder()
        .setCompleted(
            SendCompleted.newBuilder()
                .setResult(
                    ProduceResult.newBuilder()
                        .setSeq(seq)
                        .setError(
                            Error.newBuilder()
                                .setCode(ErrorCode.ERROR_CODE_OUTCOME_UNKNOWN)
                                .setMessage(message))))
        .build();
  }

  /** The bytes a record counts for in the window: key, value, header keys and header values. */
  static long size(ProduceRecord record) {
    long size = (long) record.getKey().size() + record.getValue().size();
    for (Header header : record.getHeadersList()) {
      size += header.getKeyBytes().size() + header.getValue().size();
    }
    return size;
  }
}
