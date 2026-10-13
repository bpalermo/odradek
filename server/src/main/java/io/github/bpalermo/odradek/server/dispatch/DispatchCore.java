package io.github.bpalermo.odradek.server.dispatch;

import com.google.protobuf.ByteString;
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
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.DeadLetterFailed;
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.DeadLetterWritten;
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.Fetched;
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.PartitionAssigned;
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.PartitionsLost;
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.PartitionsRevoked;
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.ReceiverJoined;
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.ReceiverLeft;
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.RecordsFetched;
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.Rejected;
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.TimePassed;
import io.github.bpalermo.odradek.server.dispatch.DispatchSettings.OnExhausted;
import io.github.bpalermo.odradek.server.dispatch.DispatchSettings.Ordering;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * The dispatcher's decisions, with no I/O: events in, actions out (ADR 6).
 *
 * <p>One instance covers <b>one topic of one consumer group on one dispatcher replica</b>: the
 * partitions of that topic this replica owns, and the receivers (the {@code Deliver} streams of
 * sidecars) that declared that group and topic to this replica. That is the scope of one set of
 * {@link DispatchSettings} and of one {@code Deliver} stream, which is why partitions are plain
 * numbers here. It holds no Kafka client, no stream, no thread and no clock: the caller reports
 * what happened with {@link #handle}, passes the time, and carries out what comes back.
 *
 * <h2>What it decides</h2>
 *
 * <ul>
 *   <li><b>What to send, to whom.</b> A record goes to a connected receiver with credit, the one
 *       holding the fewest unacknowledged records (the one that joined first on a tie). Partitions
 *       with something to send take turns, one record each, and within a partition the lowest
 *       offset goes first, so a record to deliver again precedes newer ones.
 *   <li><b>Ordering.</b> Under {@link Ordering#KEY} a record waits while an earlier record of its
 *       partition with the same key bytes is unacknowledged; under {@link Ordering#PARTITION} while
 *       any earlier record of its partition is. "Unacknowledged" includes a record waiting to be
 *       delivered again or to be dead-lettered, so a redelivery always precedes what it holds back.
 *   <li><b>Failed attempts.</b> A rejection and a processing deadline that passes each fail the
 *       attempt, and the record is delivered again. It goes to a different receiver if one has
 *       credit at that moment, and back to the same one otherwise: waiting for another receiver
 *       could wait for ever.
 *   <li><b>The wait before a redelivery.</b> A record whose attempt failed is not sent again before
 *       {@link DispatchSettings#redeliveryBackoff}, doubled for each failed attempt after the first
 *       and at most {@link DispatchSettings#redeliveryBackoffMax}. While it waits it is
 *       unacknowledged like any other: under {@link Ordering#KEY} the later records of its key wait
 *       behind it, under {@link Ordering#PARTITION} the rest of its partition does, the commit
 *       position stays at or below it, and so the window still bounds what the other keys are sent
 *       meanwhile. A record whose receiver left does not wait.
 *   <li><b>A receiver that leaves</b> fails nothing: what was in flight to it is delivered again
 *       <em>with the same attempt number</em>. A stream that ends says something about the pod, not
 *       about the record, and rolling the application must not use up the attempts of whatever
 *       happened to be in flight (with a limit and {@link OnExhausted#BLOCK} it would block
 *       partitions).
 *   <li><b>Exhaustion.</b> A record whose attempts are used up is either dead-lettered (it stays
 *       unacknowledged until {@link DeadLetterWritten}; a failed write is asked for again, after
 *       {@link DispatchSettings#deadLetterRetry} doubled for each failure, for as long as the core
 *       has the partition) or blocks its partition, see below.
 *   <li><b>The commit position</b> of a partition: the offset of its oldest record that is not
 *       acknowledged, or one past the last fetched record when there is none. Offsets that hold no
 *       record (compacted away, transaction markers) are not waited for. {@link Commit} is emitted
 *       when an acknowledgement moves it; {@link #committable} reads it at any time.
 *   <li><b>The window.</b> A record is delivered for the first time only while its offset is less
 *       than {@code window} above the commit position, so the oldest unacknowledged record can
 *       always be delivered and at most {@code window} offsets are delivered again after a restart.
 *   <li><b>Fetching.</b> {@link PauseFetching} when the records of a partition that were never
 *       delivered reach {@link DispatchSettings#maxBufferedRecords} or {@link
 *       DispatchSettings#maxBufferedBytes}; {@link ResumeFetching} when they are back at half of
 *       both. The check follows a fetch and whatever that fetch let the core send, so the bound can
 *       be exceeded by one fetch.
 * </ul>
 *
 * <h2>A late acknowledgement</h2>
 *
 * <p>When a deadline passes, the receiver still has the record and its application may yet finish.
 * An acknowledgement from it <b>counts</b>, for as long as the record is unacknowledged, wherever
 * the record is by then: waiting, in flight to another receiver, being dead-lettered or blocking
 * its partition. The record was processed, so delivering it again would only add a duplicate; if
 * another receiver has it already, that copy is a duplicate nothing can recall, and its own
 * acknowledgement is then ignored. A receiver that rejected a record or left no longer holds it. A
 * record a receiver holds past its deadline still counts towards that receiver's load.
 *
 * <h2>A blocked partition</h2>
 *
 * <p>Under {@link OnExhausted#BLOCK} an exhausted record is not delivered again, the commit
 * position stays at it, and no record after it in its partition is sent: neither for the first time
 * nor again. Records before it that are still unacknowledged are delivered and redelivered as
 * usual, so the commit position reaches the blocked record. What is in flight after it can still be
 * acknowledged. Fetching goes on until the buffer bound pauses it. {@link PartitionBlocked} reports
 * it. The partition stays blocked until it is revoked or lost (the attempt counts go with it, so
 * its next owner starts again), or until a receiver that held the record past its deadline
 * acknowledges it after all ({@link PartitionUnblocked}).
 *
 * <h2>Giving partitions up</h2>
 *
 * <p>{@link PartitionsRevoked} stops sending for those partitions and drops what was not in flight,
 * a record waiting to be sent again and a dead letter waiting to be written again included: only a
 * dead letter whose write is under way is still waited for. Each partition is released when nothing
 * of it is in flight or being written any more, or when {@link DispatchSettings#revokeGrace} has
 * passed: the core emits the last {@link Commit} if the position moved, a {@link Drop} for every
 * receiver still holding its records, and {@link PartitionReleased}. {@link PartitionsLost} emits
 * the {@link Drop}s and nothing else.
 *
 * <h2>Time</h2>
 *
 * <p>Times are nanoseconds of a monotonic clock, compared as {@link System#nanoTime} values are. A
 * time earlier than one already seen is taken as that one. Deadlines and waits are checked only on
 * {@link TimePassed}; {@link #nextDeadlineNanos} says when the next one is due.
 *
 * <p>The processing deadline is the one setting that can change while the core runs ({@link
 * #maxProcessing}): a record keeps the deadline in force when it was sent.
 *
 * <p>The same events at the same times give the same actions. An event costs time in proportion to
 * the records it concerns, times a logarithm; giving a partition up also walks that partition's
 * records and the receivers. Not thread-safe.
 *
 * @param <R> what the caller attaches to a record: the core keeps it from the fetch until the
 *     record is acknowledged, and hands it back with every delivery
 */
public final class DispatchCore<R> {

  private enum State {
    /** Fetched; not yet let in by the window, or by the ordering under {@code PARTITION}. */
    PENDING,
    /** Behind an unacknowledged record with the same key. */
    WAITING,
    /** May be sent as soon as a receiver has credit. */
    READY,
    /** Its last attempt failed, and it is not to be sent again yet. */
    DELAYED,
    IN_FLIGHT,
    /** Exhausted; its dead letter is being written, or waits to be written again. */
    DEAD_LETTERING,
    /** Exhausted, and blocking its partition. */
    BLOCKED,
    /** Its partition is being handed over; it will not be sent again by this replica. */
    DROPPED,
    ACKED
  }

  private static final class Rec<R> {
    final Part<R> part;
    final long offset;
    final int size;
    ByteString key;
    R payload;
    State state = State.PENDING;

    /** Deliveries that count: those not taken back because their receiver left. */
    int attempts;

    /** Whether it was ever delivered, and so is no longer part of the fetch buffer. */
    boolean delivered;

    long sentAt;

    /** The deliveries it is in flight among: those made under the same processing deadline. */
    Flight<R> flight;

    /** Where the record is in flight. */
    Receiver<R> holder;

    /** Receivers that were sent the record, let its deadline pass, and may still acknowledge. */
    List<Receiver<R>> former;

    /** Where the last attempt failed. */
    Receiver<R> avoid;

    /** Why it is next sent again, or was given up on: how its last delivery ended. */
    Cause cause = Cause.NONE;

    /** What the receiver that rejected it last said, if it said anything. */
    String detail;

    /** While {@link State#DEAD_LETTERING}: whether the write was asked for and not answered. */
    boolean writing;

    /** Writes of its dead letter that failed. */
    int deadLetterFailures;

    /** While it is among the timers: when it is due, and what orders it among those due then. */
    long dueAt;

    long timerSeq;
    boolean timed;

    Rec(Part<R> part, long offset, int size, ByteString key, R payload) {
      this.part = part;
      this.offset = offset;
      this.size = size;
      this.key = key;
      this.payload = payload;
    }
  }

  /**
   * The records in flight that were sent under one processing deadline, oldest delivery first, so
   * that the first is the next to expire.
   */
  private static final class Flight<R> {
    final long deadlineNanos;
    final LinkedHashSet<Rec<R>> records = new LinkedHashSet<>();

    Flight(long deadlineNanos) {
      this.deadlineNanos = deadlineNanos;
    }
  }

  /** The records waiting behind the unacknowledged record of one key. */
  private static final class Lane<R> {
    ArrayDeque<Rec<R>> waiting;
  }

  private static final class Part<R> {
    final int id;

    /** Every fetched record at or above the commit position, in offset order. */
    final ArrayDeque<Rec<R>> log = new ArrayDeque<>();

    /** The tail of {@link #log} that is still {@link State#PENDING}. */
    final ArrayDeque<Rec<R>> pending = new ArrayDeque<>();

    final TreeMap<Long, Rec<R>> ready = new TreeMap<>();

    /** Under {@code KEY}: the keys that have an unacknowledged record past {@code PENDING}. */
    final Map<ByteString, Lane<R>> lanes = new HashMap<>();

    final Map<Long, Rec<R>> deadLettering = new HashMap<>();
    final TreeSet<Long> blocked = new TreeSet<>();

    /** The commit position when {@link #log} is empty. */
    long end;

    /** The last commit position emitted, or the position the partition was assigned at. */
    long emitted;

    int inFlight;
    int buffered;
    long bufferedBytes;
    boolean paused;
    boolean draining;
    long drainStart;

    /** Whether its oldest pending record is held back by the window. */
    boolean atWindow;

    /** The records this core sent that are still at or above the commit position. */
    int sent;

    Part(int id, long position) {
      this.id = id;
      this.end = position;
      this.emitted = position;
    }

    long commit() {
      Rec<R> oldest = log.peekFirst();
      return oldest == null ? end : oldest.offset;
    }

    boolean sendable() {
      return !draining
          && !ready.isEmpty()
          && (blocked.isEmpty() || ready.firstKey() < blocked.first());
    }
  }

  private static final class Receiver<R> {
    final String id;
    final long seq;
    long credit;

    /** Number of records in {@link #held}. */
    int load;

    /** What it was sent and has not acknowledged or given back, by partition and offset. */
    final TreeMap<Integer, TreeMap<Long, Rec<R>>> held = new TreeMap<>();

    Receiver(String id, long seq) {
      this.id = id;
      this.seq = seq;
    }
  }

  private final DispatchSettings settings;
  private final long revokeGraceNanos;
  private final long redeliveryBackoffNanos;
  private final long redeliveryBackoffMaxNanos;
  private final long deadLetterRetryNanos;
  private final long deadLetterRetryMaxNanos;

  /**
   * The records waiting for a time: {@link State#DELAYED} ones to be sent again, and {@link
   * State#DEAD_LETTERING} ones whose write failed to be written again. Earliest first.
   */
  private final TreeSet<Rec<R>> timers =
      new TreeSet<>(
          (a, b) -> {
            // Times of a monotonic clock, a few waits apart: compared as nanoTime values are.
            long difference = a.dueAt - b.dueAt;
            return difference != 0
                ? (difference < 0 ? -1 : 1)
                : Long.compare(a.timerSeq, b.timerSeq);
          });

  private long timerSeq;
  private int atWindow;
  private long windowPauses;

  private final TreeMap<Integer, Part<R>> partitions = new TreeMap<>();

  /** The partitions that have a record to send, in the order they take turns. */
  private final LinkedHashSet<Part<R>> sendable = new LinkedHashSet<>();

  private final LinkedHashSet<Part<R>> draining = new LinkedHashSet<>();

  /** In the order they joined. */
  private final LinkedHashMap<String, Receiver<R>> receivers = new LinkedHashMap<>();

  /** The receivers with credit, least loaded first. Removed before load changes, added after. */
  private final TreeSet<Receiver<R>> available =
      new TreeSet<>(
          Comparator.<Receiver<R>>comparingInt(r -> r.load).thenComparingLong(r -> r.seq));

  /**
   * The records in flight, by the processing deadline they were sent under. The last is the one in
   * force; one before it is kept only while it holds records.
   */
  private final ArrayDeque<Flight<R>> flights = new ArrayDeque<>();

  private int inFlight;

  /** The partition the event being handled added records to. */
  private Part<R> fetchedInto;

  private long receiverSeq;
  private long now;
  private boolean clockSet;

  public DispatchCore(DispatchSettings settings) {
    this.settings = settings;
    flights.add(new Flight<>(settings.maxProcessing().toNanos()));
    this.revokeGraceNanos = settings.revokeGrace().toNanos();
    this.redeliveryBackoffNanos = settings.redeliveryBackoff().toNanos();
    this.redeliveryBackoffMaxNanos = settings.redeliveryBackoffMax().toNanos();
    this.deadLetterRetryNanos = settings.deadLetterRetry().toNanos();
    this.deadLetterRetryMaxNanos = settings.deadLetterRetryMax().toNanos();
  }

  /** Applies one event at the given time and returns what the caller must do, in order. */
  public List<DispatchAction<R>> handle(DispatchEvent<R> event, long nowNanos) {
    if (!clockSet || nowNanos - now > 0) {
      now = nowNanos;
      clockSet = true;
    }
    List<DispatchAction<R>> out = new ArrayList<>();
    switch (event) {
      case PartitionAssigned<R> e -> assigned(e, out);
      case PartitionsRevoked<R> e -> revoked(e, out);
      case PartitionsLost<R> e -> lost(e, out);
      case RecordsFetched<R> e -> fetched(e, out);
      case ReceiverJoined<R> e -> joined(e);
      case ReceiverLeft<R> e -> left(e);
      case CreditsGranted<R> e -> credits(e);
      case Acknowledged<R> e -> acknowledged(e, out);
      case Rejected<R> e -> rejected(e, out);
      case DeadLetterWritten<R> e -> deadLetterWritten(e, out);
      case DeadLetterFailed<R> e -> deadLetterFailed(e, out);
      case TimePassed<R> _ -> expire(out);
    }
    finishHandOvers(out);
    dispatch(out);
    if (fetchedInto != null) {
      pauseIfFull(fetchedInto, out);
      fetchedInto = null;
    }
    return out;
  }

  /**
   * The commit position of a partition the core has: what a commit made now should carry. It is at
   * or above the last {@link Commit} emitted; it can be above when a fetch showed that the offsets
   * after the last acknowledged record hold nothing. Empty for a partition the core does not have.
   */
  public OptionalLong committable(int partition) {
    Part<R> p = partitions.get(partition);
    return p == null ? OptionalLong.empty() : OptionalLong.of(p.commit());
  }

  /** The lowest offset blocking a partition, if it is blocked. */
  public OptionalLong blockedOffset(int partition) {
    Part<R> p = partitions.get(partition);
    return p == null || p.blocked.isEmpty()
        ? OptionalLong.empty()
        : OptionalLong.of(p.blocked.first());
  }

  /** The number of records in flight, over all partitions. */
  public int inFlight() {
    return inFlight;
  }

  /**
   * The number of partitions held back by the window now: those with a fetched record that is not
   * sent because the partition's oldest unacknowledged record is {@link DispatchSettings#window}
   * offsets or more below it.
   */
  public int atWindow() {
    return atWindow;
  }

  /** How many times a partition came to be held back by the window, since the core exists. */
  public long windowPauses() {
    return windowPauses;
  }

  /**
   * Changes the processing deadline, for the records sent from now on: it comes from the
   * application, and so changes with its versions while the other settings stay. What is in flight
   * keeps the deadline it was sent under.
   */
  public void maxProcessing(Duration maxProcessing) {
    if (maxProcessing.isZero() || maxProcessing.isNegative()) {
      throw new IllegalArgumentException("maxProcessing must be positive");
    }
    long nanos = maxProcessing.toNanos();
    Flight<R> current = flights.getLast();
    if (current.deadlineNanos == nanos) {
      return;
    }
    if (current.records.isEmpty()) {
      flights.removeLast();
    }
    flights.addLast(new Flight<>(nanos));
  }

  /**
   * When {@link TimePassed} is next needed: the earliest processing deadline, end of a wait before
   * a redelivery or a dead letter, or end of a hand-over wait. Empty when nothing is in flight,
   * nothing waits and nothing is being handed over.
   */
  public OptionalLong nextDeadlineNanos() {
    boolean any = !timers.isEmpty();
    long earliest = any ? timers.first().dueAt : 0;
    for (Flight<R> flight : flights) {
      if (flight.records.isEmpty()) {
        continue;
      }
      long deadline = flight.records.getFirst().sentAt + flight.deadlineNanos;
      if (!any || deadline - earliest < 0) {
        earliest = deadline;
        any = true;
      }
    }
    for (Part<R> p : draining) {
      long deadline = p.drainStart + revokeGraceNanos;
      if (!any || deadline - earliest < 0) {
        earliest = deadline;
        any = true;
      }
    }
    return any ? OptionalLong.of(earliest) : OptionalLong.empty();
  }

  // ---------------------------------------------------------------------------------------------
  // Partitions
  // ---------------------------------------------------------------------------------------------

  private void assigned(PartitionAssigned<R> e, List<DispatchAction<R>> out) {
    Part<R> previous = partitions.get(e.partition());
    if (previous != null) {
      release(List.of(previous), false, out);
    }
    partitions.put(e.partition(), new Part<>(e.partition(), e.position()));
  }

  private void revoked(PartitionsRevoked<R> e, List<DispatchAction<R>> out) {
    for (int id : e.partitions()) {
      Part<R> p = partitions.get(id);
      if (p == null) {
        out.add(new PartitionReleased<>(id, true, 0));
      } else if (!p.draining) {
        startHandOver(p);
      }
    }
  }

  /** Stops sending for the partition and keeps only what an acknowledgement can still reach. */
  private void startHandOver(Part<R> p) {
    p.draining = true;
    p.drainStart = now;
    draining.add(p);
    sendable.remove(p);
    atWindow(p, false);
    p.pending.clear();
    p.ready.clear();
    p.lanes.clear();
    p.blocked.clear();
    p.buffered = 0;
    p.bufferedBytes = 0;
    // A dead letter that waits to be written again is not written by this replica any more. One
    // whose write is under way is still waited for.
    p.deadLettering.values().removeIf(rec -> !rec.writing);
    // Whatever follows the last record that can still be acknowledged is forgotten: the next
    // owner fetches it again.
    for (Rec<R> last = p.log.peekLast();
        last != null && !reachable(last);
        last = p.log.peekLast()) {
      p.log.pollLast();
      untime(last);
      p.end = last.offset;
    }
    for (Rec<R> rec : p.log) {
      if (rec.state != State.IN_FLIGHT && !writing(rec) && rec.state != State.ACKED) {
        untime(rec);
        rec.state = State.DROPPED;
        rec.payload = null;
        rec.key = null;
      }
    }
  }

  private static <R> boolean writing(Rec<R> rec) {
    return rec.state == State.DEAD_LETTERING && rec.writing;
  }

  private static <R> boolean reachable(Rec<R> rec) {
    return rec.state == State.IN_FLIGHT
        || writing(rec)
        || rec.state == State.ACKED
        || (rec.former != null && !rec.former.isEmpty());
  }

  private void lost(PartitionsLost<R> e, List<DispatchAction<R>> out) {
    List<Part<R>> gone = new ArrayList<>();
    for (int id : e.partitions()) {
      Part<R> p = partitions.get(id);
      if (p != null && !gone.contains(p)) {
        gone.add(p);
      }
    }
    release(gone, false, out);
  }

  /** Releases the revoked partitions that have nothing outstanding, or have waited long enough. */
  private void finishHandOvers(List<DispatchAction<R>> out) {
    if (draining.isEmpty()) {
      return;
    }
    List<Part<R>> done = new ArrayList<>();
    for (Part<R> p : draining) {
      if (answered(p) || now - p.drainStart >= revokeGraceNanos) {
        done.add(p);
      }
    }
    release(done, true, out);
  }

  /** Whether nothing of the partition is in flight or being written as a dead letter. */
  private static <R> boolean answered(Part<R> p) {
    return p.inFlight == 0 && p.deadLettering.isEmpty();
  }

  /** Forgets partitions, and tells the receivers that hold their records to drop them. */
  private void release(List<Part<R>> gone, boolean handOver, List<DispatchAction<R>> out) {
    if (gone.isEmpty()) {
      return;
    }
    List<PartitionReleased<R>> released = new ArrayList<>();
    for (Part<R> p : gone) {
      if (handOver) {
        commit(p, out);
        released.add(new PartitionReleased<>(p.id, answered(p), p.sent));
      }
      partitions.remove(p.id);
      sendable.remove(p);
      draining.remove(p);
      atWindow(p, false);
      for (Rec<R> rec : p.log) {
        untime(rec);
        if (rec.state == State.IN_FLIGHT) {
          land(rec);
        }
      }
    }
    for (Receiver<R> r : receivers.values()) {
      List<Integer> dropped = null;
      for (Part<R> p : gone) {
        TreeMap<Long, Rec<R>> records = r.held.remove(p.id);
        if (records != null) {
          reload(r, -records.size());
          if (dropped == null) {
            dropped = new ArrayList<>();
          }
          dropped.add(p.id);
        }
      }
      if (dropped != null) {
        out.add(new Drop<>(r.id, dropped));
      }
    }
    out.addAll(released);
  }

  // ---------------------------------------------------------------------------------------------
  // Records
  // ---------------------------------------------------------------------------------------------

  private void fetched(RecordsFetched<R> e, List<DispatchAction<R>> out) {
    Part<R> p = partitions.get(e.partition());
    if (p == null || p.draining) {
      return;
    }
    boolean keyed = settings.ordering() == Ordering.KEY;
    for (Fetched<R> fetched : e.records()) {
      if (fetched.offset() < p.end) {
        // Not after what was fetched before: the core has it, or is past it.
        continue;
      }
      Rec<R> rec =
          new Rec<>(
              p,
              fetched.offset(),
              fetched.sizeBytes(),
              keyed ? fetched.key() : null,
              fetched.payload());
      p.log.addLast(rec);
      p.pending.addLast(rec);
      p.end = rec.offset + 1;
      p.buffered++;
      p.bufferedBytes += rec.size;
    }
    admit(p);
    refresh(p);
    fetchedInto = p;
  }

  /** After a fetch, and after what could be sent at once was: is enough buffered? */
  private void pauseIfFull(Part<R> p, List<DispatchAction<R>> out) {
    if (!p.paused
        && (p.buffered >= settings.maxBufferedRecords()
            || p.bufferedBytes >= settings.maxBufferedBytes())) {
      p.paused = true;
      out.add(new PauseFetching<>(p.id));
    }
  }

  /** Lets pending records in, oldest first, as far as the window and the ordering allow. */
  private void admit(Part<R> p) {
    if (p.draining || !p.blocked.isEmpty()) {
      return;
    }
    long commit = p.commit();
    for (Rec<R> rec = p.pending.peekFirst(); rec != null; rec = p.pending.peekFirst()) {
      if (rec.offset - commit >= settings.window()) {
        atWindow(p, true);
        return;
      }
      atWindow(p, false);
      if (settings.ordering() == Ordering.PARTITION && p.log.peekFirst() != rec) {
        return;
      }
      p.pending.pollFirst();
      if (rec.key == null) {
        ready(rec);
        continue;
      }
      Lane<R> lane = p.lanes.get(rec.key);
      if (lane == null) {
        p.lanes.put(rec.key, new Lane<>());
        ready(rec);
      } else {
        if (lane.waiting == null) {
          lane.waiting = new ArrayDeque<>();
        }
        lane.waiting.addLast(rec);
        rec.state = State.WAITING;
      }
    }
    atWindow(p, false);
  }

  private void atWindow(Part<R> p, boolean held) {
    if (p.atWindow == held) {
      return;
    }
    p.atWindow = held;
    if (held) {
      atWindow++;
      windowPauses++;
    } else {
      atWindow--;
    }
  }

  /** Makes a record wait until {@code dueAt}. */
  private void time(Rec<R> rec, long dueAt) {
    rec.dueAt = dueAt;
    rec.timerSeq = timerSeq++;
    rec.timed = true;
    timers.add(rec);
  }

  private void untime(Rec<R> rec) {
    if (rec.timed) {
      timers.remove(rec);
      rec.timed = false;
    }
  }

  /** {@code first} doubled {@code doublings} times, and at most {@code longest}. */
  private static long doubled(long first, long longest, int doublings) {
    if (first <= 0) {
      return 0;
    }
    int shift = Math.min(Math.max(doublings, 0), 62);
    return first > (longest >> shift) ? longest : first << shift;
  }

  private void ready(Rec<R> rec) {
    rec.state = State.READY;
    rec.part.ready.put(rec.offset, rec);
  }

  /** Puts the partition among those that take turns sending, or takes it out. */
  private void refresh(Part<R> p) {
    if (p.sendable()) {
      sendable.add(p);
    } else {
      sendable.remove(p);
    }
  }

  private void dispatch(List<DispatchAction<R>> out) {
    while (!available.isEmpty() && !sendable.isEmpty()) {
      Iterator<Part<R>> turn = sendable.iterator();
      Part<R> p = turn.next();
      turn.remove();
      Rec<R> rec = p.ready.pollFirstEntry().getValue();
      send(rec, pick(rec), out);
      if (p.sendable()) {
        sendable.add(p);
      }
    }
  }

  /** The least loaded receiver with credit, other than the one that just failed if possible. */
  private Receiver<R> pick(Rec<R> rec) {
    Receiver<R> first = available.first();
    if (first == rec.avoid) {
      Receiver<R> next = available.higher(first);
      if (next != null) {
        return next;
      }
    }
    return first;
  }

  private void send(Rec<R> rec, Receiver<R> r, List<DispatchAction<R>> out) {
    Part<R> p = rec.part;
    available.remove(r);
    r.credit--;
    if (rec.former != null) {
      rec.former.remove(r);
    }
    if (r.held.computeIfAbsent(p.id, id -> new TreeMap<>()).put(rec.offset, rec) == null) {
      r.load++;
    }
    if (r.credit > 0) {
      available.add(r);
    }
    rec.state = State.IN_FLIGHT;
    rec.holder = r;
    rec.avoid = null;
    rec.sentAt = now;
    rec.attempts++;
    rec.flight = flights.getLast();
    rec.flight.records.add(rec);
    inFlight++;
    p.inFlight++;
    out.add(
        new Deliver<>(
            r.id,
            p.id,
            rec.offset,
            rec.payload,
            rec.attempts,
            rec.delivered ? rec.cause : Cause.NONE));
    if (!rec.delivered) {
      rec.delivered = true;
      p.sent++;
      p.buffered--;
      p.bufferedBytes -= rec.size;
      if (p.paused
          && p.buffered <= settings.maxBufferedRecords() / 2
          && p.bufferedBytes <= settings.maxBufferedBytes() / 2) {
        p.paused = false;
        out.add(new ResumeFetching<>(p.id));
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Receivers
  // ---------------------------------------------------------------------------------------------

  private void joined(ReceiverJoined<R> e) {
    receivers.computeIfAbsent(e.receiver(), id -> new Receiver<>(id, receiverSeq++));
  }

  private void left(ReceiverLeft<R> e) {
    Receiver<R> r = receivers.remove(e.receiver());
    if (r == null) {
      return;
    }
    available.remove(r);
    for (TreeMap<Long, Rec<R>> records : r.held.values()) {
      for (Rec<R> rec : records.values()) {
        if (rec.holder == r) {
          returned(rec);
        } else {
          rec.former.remove(r);
        }
      }
    }
    r.held.clear();
  }

  /**
   * The receiver a record was in flight to left. The delivery is taken back: the record is sent
   * again under the same attempt number.
   */
  private void returned(Rec<R> rec) {
    Part<R> p = rec.part;
    land(rec);
    p.inFlight--;
    rec.holder = null;
    if (p.draining) {
      rec.state = State.DROPPED;
      rec.payload = null;
      return;
    }
    rec.attempts--;
    rec.cause = Cause.RECEIVER_LEFT;
    ready(rec);
    refresh(p);
  }

  private void credits(CreditsGranted<R> e) {
    Receiver<R> r = receivers.get(e.receiver());
    if (r == null || e.records() <= 0) {
      return;
    }
    r.credit += e.records();
    available.add(r);
  }

  private void acknowledged(Acknowledged<R> e, List<DispatchAction<R>> out) {
    Receiver<R> r = receivers.get(e.receiver());
    Part<R> p = partitions.get(e.partition());
    if (r == null || p == null || e.firstOffset() > e.lastOffset()) {
      return;
    }
    TreeMap<Long, Rec<R>> held = r.held.get(p.id);
    if (held == null) {
      return;
    }
    // Only what this receiver was sent: the other offsets of the range are someone else's.
    List<Rec<R>> records =
        new ArrayList<>(held.subMap(e.firstOffset(), true, e.lastOffset(), true).values());
    if (records.isEmpty()) {
      return;
    }
    for (Rec<R> rec : records) {
      acknowledge(rec, out);
    }
    settle(p, out);
  }

  private void rejected(Rejected<R> e, List<DispatchAction<R>> out) {
    Receiver<R> r = receivers.get(e.receiver());
    TreeMap<Long, Rec<R>> held = r == null ? null : r.held.get(e.partition());
    Rec<R> rec = held == null ? null : held.get(e.offset());
    if (rec == null) {
      return;
    }
    unhold(r, rec);
    if (rec.holder == r) {
      rec.avoid = r;
      failed(
          rec, Cause.REJECTED, e.reason() == null || e.reason().isEmpty() ? null : e.reason(), out);
    } else {
      // Given back after its deadline passed: that attempt has failed already.
      rec.former.remove(r);
    }
  }

  private void expire(List<DispatchAction<R>> out) {
    // A copy: a flight that empties, and is not the one in force, is removed as it does.
    for (Flight<R> flight : List.copyOf(flights)) {
      for (Rec<R> rec = oldest(flight);
          rec != null && now - rec.sentAt >= flight.deadlineNanos;
          rec = oldest(flight)) {
        // The receiver keeps the record, and may still acknowledge it.
        if (rec.former == null) {
          rec.former = new ArrayList<>(1);
        }
        rec.former.add(rec.holder);
        rec.avoid = rec.holder;
        failed(rec, Cause.DEADLINE, null, out);
      }
    }
    // What has waited long enough: a record to send again, a dead letter to write again.
    for (Rec<R> rec = due(); rec != null; rec = due()) {
      untime(rec);
      if (rec.state == State.DELAYED) {
        ready(rec);
        refresh(rec.part);
      } else {
        writeDeadLetter(rec, out);
      }
    }
  }

  private Rec<R> due() {
    if (timers.isEmpty()) {
      return null;
    }
    Rec<R> first = timers.first();
    return now - first.dueAt >= 0 ? first : null;
  }

  private void writeDeadLetter(Rec<R> rec, List<DispatchAction<R>> out) {
    rec.writing = true;
    out.add(
        new DeadLetter<>(
            rec.part.id,
            rec.offset,
            rec.payload,
            rec.attempts,
            rec.cause,
            rec.detail,
            rec.deadLetterFailures));
  }

  private static <R> Rec<R> oldest(Flight<R> flight) {
    return flight.records.isEmpty() ? null : flight.records.getFirst();
  }

  /** The record is no longer in flight. */
  private void land(Rec<R> rec) {
    Flight<R> flight = rec.flight;
    flight.records.remove(rec);
    if (flight.records.isEmpty() && flight != flights.getLast()) {
      flights.remove(flight);
    }
    rec.flight = null;
    inFlight--;
  }

  private void deadLetterWritten(DeadLetterWritten<R> e, List<DispatchAction<R>> out) {
    Part<R> p = partitions.get(e.partition());
    Rec<R> rec = p == null ? null : p.deadLettering.get(e.offset());
    if (rec != null) {
      acknowledge(rec, out);
      settle(p, out);
    }
  }

  private void deadLetterFailed(DeadLetterFailed<R> e, List<DispatchAction<R>> out) {
    Part<R> p = partitions.get(e.partition());
    Rec<R> rec = p == null ? null : p.deadLettering.get(e.offset());
    if (rec == null || !rec.writing) {
      // Not being written: the answer to a write this core has given up on, or asked for again.
      return;
    }
    rec.writing = false;
    if (p.draining) {
      // The next owner delivers the record again, and dead-letters it if it must.
      p.deadLettering.remove(rec.offset);
      rec.state = State.DROPPED;
      rec.payload = null;
      return;
    }
    long wait = doubled(deadLetterRetryNanos, deadLetterRetryMaxNanos, rec.deadLetterFailures);
    rec.deadLetterFailures++;
    if (wait == 0) {
      writeDeadLetter(rec, out);
    } else {
      time(rec, now + wait);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Outcomes of an attempt
  // ---------------------------------------------------------------------------------------------

  /**
   * The attempt in flight failed: the record is to be delivered again, after a wait, or is
   * exhausted. The caller has dealt with the receiver's side of it.
   */
  private void failed(Rec<R> rec, Cause cause, String detail, List<DispatchAction<R>> out) {
    Part<R> p = rec.part;
    land(rec);
    p.inFlight--;
    rec.holder = null;
    rec.cause = cause;
    rec.detail = detail;
    if (p.draining) {
      rec.state = State.DROPPED;
      rec.payload = null;
      return;
    }
    if (settings.maxAttempts() > 0 && rec.attempts >= settings.maxAttempts()) {
      if (settings.onExhausted() == OnExhausted.DEAD_LETTER) {
        rec.state = State.DEAD_LETTERING;
        p.deadLettering.put(rec.offset, rec);
        writeDeadLetter(rec, out);
      } else {
        rec.state = State.BLOCKED;
        p.blocked.add(rec.offset);
        out.add(new PartitionBlocked<>(p.id, rec.offset));
        refresh(p);
      }
      return;
    }
    long wait = doubled(redeliveryBackoffNanos, redeliveryBackoffMaxNanos, rec.attempts - 1);
    if (wait == 0) {
      ready(rec);
      refresh(p);
    } else {
      rec.state = State.DELAYED;
      time(rec, now + wait);
    }
  }

  /** The record is done with. The caller settles the partition afterwards. */
  private void acknowledge(Rec<R> rec, List<DispatchAction<R>> out) {
    Part<R> p = rec.part;
    switch (rec.state) {
      case IN_FLIGHT -> {
        land(rec);
        p.inFlight--;
      }
      case DEAD_LETTERING -> {
        p.deadLettering.remove(rec.offset);
        untime(rec);
        rec.writing = false;
      }
      case DELAYED -> untime(rec);
      case BLOCKED -> {
        p.blocked.remove(rec.offset);
        if (p.blocked.isEmpty()) {
          out.add(new PartitionUnblocked<>(p.id));
        }
      }
      case READY -> p.ready.remove(rec.offset);
      case DROPPED -> {}
      case PENDING, WAITING, ACKED ->
          throw new IllegalStateException("acknowledged in state " + rec.state);
    }
    if (rec.holder != null) {
      unhold(rec.holder, rec);
    }
    if (rec.former != null) {
      for (Receiver<R> r : rec.former) {
        unhold(r, rec);
      }
    }
    rec.holder = null;
    rec.former = null;
    rec.avoid = null;
    rec.payload = null;
    rec.detail = null;
    rec.state = State.ACKED;
    if (rec.key != null && !p.draining) {
      // The next record of the key takes its turn.
      Lane<R> lane = p.lanes.get(rec.key);
      Rec<R> next = lane.waiting == null ? null : lane.waiting.pollFirst();
      if (next == null) {
        p.lanes.remove(rec.key);
      } else {
        ready(next);
      }
    }
    rec.key = null;
  }

  /** After acknowledgements: moves the commit position and lets in what that allows. */
  private void settle(Part<R> p, List<DispatchAction<R>> out) {
    for (Rec<R> oldest = p.log.peekFirst();
        oldest != null && oldest.state == State.ACKED;
        oldest = p.log.peekFirst()) {
      p.log.pollFirst();
      // Only a record that was sent is ever acknowledged.
      p.sent--;
    }
    admit(p);
    refresh(p);
    commit(p, out);
  }

  private void commit(Part<R> p, List<DispatchAction<R>> out) {
    long position = p.commit();
    if (position > p.emitted) {
      p.emitted = position;
      out.add(new Commit<>(p.id, position));
    }
  }

  private void unhold(Receiver<R> r, Rec<R> rec) {
    TreeMap<Long, Rec<R>> held = r.held.get(rec.part.id);
    if (held == null || held.remove(rec.offset) == null) {
      return;
    }
    if (held.isEmpty()) {
      r.held.remove(rec.part.id);
    }
    reload(r, -1);
  }

  /** Changes a receiver's load without losing its place among those with credit. */
  private void reload(Receiver<R> r, int delta) {
    boolean had = available.remove(r);
    r.load += delta;
    if (had) {
      available.add(r);
    }
  }
}
