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
import io.github.bpalermo.odradek.server.dispatch.DispatchEvent.Returned;
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
 *       holding the fewest unacknowledged records and, on a tie, the one sent a record longest ago,
 *       so that idle receivers take turns rather than the first to join getting all. Partitions
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
 *   <li><b>A record given back unread</b> ({@link Returned}, ADR 7) fails nothing either: its
 *       receiver's application had not taken it. It is delivered again at once, under the same
 *       attempt number, to a different receiver if one has credit. The credit it used is not given
 *       back, which is what keeps a receiver stuck on another record from being sent more.
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
 * <p>The commit position is a contiguous run, and under {@link Ordering#KEY} and {@link
 * Ordering#NONE} records are acknowledged out of offset order: a record held back (behind its key,
 * for credit, or for the wait before a redelivery) lies below records that receivers have
 * acknowledged. A partition given up as it stands would be committed at that record, and its next
 * owner would send everything acknowledged above it again. So {@link PartitionsRevoked} does not
 * stop a partition dead: the core <b>finishes what it began</b>.
 *
 * <ul>
 *   <li><b>Nothing new is sent.</b> The partition has a <em>limit</em>: the offset of the last
 *       record that is acknowledged, in flight, being written as a dead letter, or still held by a
 *       receiver past its deadline. No record at or above the limit is sent, for the first time or
 *       again, and what lies above the last such record is forgotten: the next owner fetches it.
 *   <li><b>What lies below the limit is sent as it would have been</b>: in the order of its key, to
 *       a receiver with credit, the least loaded, after the wait a failed attempt asks for, and
 *       dead-lettered when its attempts are used up. Each acknowledgement moves the commit position
 *       as usual.
 *   <li><b>Sending stops for good at the first record that cannot be finished</b>, which lowers the
 *       limit to it: nothing can be committed past it, so whatever was sent above it would be sent
 *       again by the next owner besides. Such a record is one that blocks the partition, or would;
 *       one whose dead letter failed to be written; one whose wait before a redelivery ends after
 *       the time left for sending; and every record not in flight once no receiver is connected.
 *   <li><b>Sending has half of {@link DispatchSettings#revokeGrace}.</b> After that the limit drops
 *       to the commit position, and the other half is left for the answers to what was sent: a
 *       record sent as the wait ended would be handed out again by the next owner while a receiver
 *       had barely begun on it.
 *   <li><b>The partition is released</b> when everything below the limit is acknowledged and
 *       nothing is in flight or being written, or when {@link DispatchSettings#revokeGrace} has
 *       passed: the core emits the last {@link Commit} if the position moved, a {@link Drop} for
 *       every receiver still holding its records, and {@link PartitionReleased}.
 * </ul>
 *
 * <p>Under {@link Ordering#PARTITION} one record of a partition is unacknowledged at a time, so
 * there is nothing below the limit: the partition waits for that record if it is in flight, and for
 * nothing otherwise.
 *
 * <p>{@link PartitionsLost} emits the {@link Drop}s and nothing else.
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
 * <h2>Timings</h2>
 *
 * <p>The core tells a {@link Timings}, if it is given one, how long a record waited between its
 * fetch and its first delivery, and what it waited for most, and how long a receiver took to answer
 * a delivery. The times are the core's, those that came with the events, so a wait is measured to
 * the precision of the events: a record fetched and sent while one event is handled waited nothing.
 *
 * <p>The same events at the same times give the same actions. An event costs time in proportion to
 * the records it concerns, times a logarithm; giving a partition up also walks that partition's
 * records and the receivers. Not thread-safe.
 *
 * @param <R> what the caller attaches to a record: the core keeps it from the fetch until the
 *     record is acknowledged, and hands it back with every delivery
 */
public final class DispatchCore<R> {

  /** What a record waited for most between its fetch and its first delivery. */
  public enum Wait {
    /** Nothing: it was sent while the event that let it be sent was handled. */
    NOTHING,
    /** It could be sent, and no receiver had credit. */
    CREDIT,
    /**
     * An earlier record of its key (under {@link Ordering#KEY}) or of its partition (under {@link
     * Ordering#PARTITION}) was unacknowledged, or a blocked record of its partition kept it from
     * being let in or, once ready, from being sent.
     */
    ORDERING,
    /** The window held its partition back while it waited to be let in. */
    WINDOW
  }

  /**
   * Told what the core times, as it happens, on the thread that calls {@link #handle}. The methods
   * do nothing unless overridden.
   */
  public interface Timings {
    /** Times nothing. */
    Timings NONE = new Timings() {};

    /**
     * A record is delivered for the first time by this core, {@code nanos} after it was fetched.
     *
     * @param waited what most of that time was spent waiting for; {@link Wait#NOTHING} when it is 0
     */
    default void queued(long nanos, Wait waited) {}

    /**
     * The receiver a record was in flight to answered, {@code nanos} after it was sent: it
     * acknowledged the record, or rejected it, within the delivery's deadline. Not told: an answer
     * after the deadline, whether or not the core has been told the time has passed (the record may
     * have been sent since); and an answer from a receiver that was sent the record again while it
     * held an expired copy, which may be for either.
     */
    default void answered(long nanos, boolean acknowledged) {}
  }

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
    /** Its partition is being handed over, and this replica will not send it (again). */
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

    /** When it was fetched, let in past {@code PENDING}, and made {@code READY}, last. */
    long fetchedAt;

    long admittedAt;
    long readyAt;

    /** The partition's count of window holds when it was fetched, less one if it was held then. */
    long windowMark;

    /** Whether the window held its partition back while it was {@code PENDING}. */
    boolean windowHeld;

    /**
     * While {@code READY} for its first delivery: whether a blocked record below it in its
     * partition keeps it from being sent now, since when, and how long that held it before then.
     * Whether is its own field: the clock's values may be negative, so no value of {@code
     * blockedSince} can mean "not blocked".
     */
    boolean blockedNow;

    long blockedSince;

    long blockedNanos;

    /**
     * Whether the receiver it is in flight to was sent it before and let that delivery's deadline
     * pass: an answer from it may be for either copy, and cannot be timed.
     */
    boolean resentToFormer;

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

    /**
     * While {@link #draining}: no record at or above this offset is sent. Every record below it is
     * acknowledged, in flight, being written as a dead letter, or still to be sent.
     */
    long limit = Long.MAX_VALUE;

    /** Whether its oldest pending record is held back by the window. */
    boolean atWindow;

    /** How many times it came to be held back by the window. */
    long windowHolds;

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

    /** Whether a record of it may be sent now. Handed over: {@link #ready} is what is below. */
    boolean sendable() {
      return !ready.isEmpty() && (blocked.isEmpty() || ready.firstKey() < blocked.first());
    }

    /** While {@link #draining}: whether everything below the limit is acknowledged. */
    boolean drained() {
      return commit() >= limit;
    }
  }

  private static final class Receiver<R> {
    final String id;
    final long seq;
    long credit;

    /** Number of records in {@link #held}. */
    int load;

    /**
     * When it was last sent a record, as a count of sends; 0 if never. Among receivers with the
     * same load, the one that waited longest goes first, so idle receivers take turns.
     */
    long lastSent;

    /** What it was sent and has not acknowledged or given back, by partition and offset. */
    final TreeMap<Integer, TreeMap<Long, Rec<R>>> held = new TreeMap<>();

    Receiver(String id, long seq) {
      this.id = id;
      this.seq = seq;
    }
  }

  private final DispatchSettings settings;
  private final Timings timings;
  private final long revokeGraceNanos;

  /** How long a partition being handed over still sends: half of the wait, see the class. */
  private final long handOverSendNanos;

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

  /**
   * The receivers with credit, least loaded first, then the one sent to longest ago. Removed before
   * load or {@code lastSent} changes, added after.
   */
  private final TreeSet<Receiver<R>> available =
      new TreeSet<>(
          Comparator.<Receiver<R>>comparingInt(r -> r.load)
              .thenComparingLong(r -> r.lastSent)
              .thenComparingLong(r -> r.seq));

  /** Sends so far, which orders {@link Receiver#lastSent}. */
  private long sends;

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
    this(settings, Timings.NONE);
  }

  /** A core that tells {@code timings} how long records wait. */
  public DispatchCore(DispatchSettings settings, Timings timings) {
    this.settings = settings;
    this.timings = timings;
    flights.add(new Flight<>(settings.maxProcessing().toNanos()));
    this.revokeGraceNanos = settings.revokeGrace().toNanos();
    this.handOverSendNanos = revokeGraceNanos / 2;
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
      case RecordsFetched<R> e -> fetched(e);
      case ReceiverJoined<R> e -> joined(e);
      case ReceiverLeft<R> e -> left(e);
      case CreditsGranted<R> e -> credits(e);
      case Acknowledged<R> e -> acknowledged(e, out);
      case Rejected<R> e -> rejected(e, out);
      case Returned<R> e -> givenBack(e);
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
   * a redelivery or a dead letter, or, for a partition being handed over, the end of its sending or
   * of its wait. Empty when nothing is in flight, nothing waits and nothing is being handed over.
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
      long deadline = p.drainStart + (p.drained() ? revokeGraceNanos : handOverSendNanos);
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

  /**
   * Begins the hand-over of a partition: nothing new is sent, and what lies below the last record
   * that was sent and can still be answered is finished, as far as it can be.
   */
  private void startHandOver(Part<R> p) {
    p.draining = true;
    p.drainStart = now;
    draining.add(p);
    atWindow(p, false);
    p.pending.clear();
    p.buffered = 0;
    p.bufferedBytes = 0;
    // Whatever follows the last record that can still be acknowledged is forgotten: the next
    // owner fetches it again.
    for (Rec<R> last = p.log.peekLast();
        last != null && !reachable(last);
        last = p.log.peekLast()) {
      p.log.pollLast();
      drop(last);
      p.end = last.offset;
    }
    // That record is the newest this replica sent that still counts. It is not sent again, and
    // neither is anything from the first record below it that cannot be finished in time.
    Rec<R> last = p.log.peekLast();
    long limit = last == null ? p.end : last.offset;
    long sendUntil = now + handOverSendNanos;
    for (Rec<R> rec : p.log) {
      if (rec.offset >= limit) {
        break;
      }
      if (!finishable(rec, sendUntil)) {
        limit = rec.offset;
        break;
      }
    }
    cut(p, limit);
  }

  /** Whether a record of a partition being handed over can be done with before sending ends. */
  private static <R> boolean finishable(Rec<R> rec, long sendUntil) {
    return switch (rec.state) {
      case ACKED, IN_FLIGHT, READY, WAITING -> true;
      case DELAYED -> rec.dueAt - sendUntil < 0;
      // A dead letter that failed is not written again by a replica that is leaving.
      case DEAD_LETTERING -> rec.writing;
      case BLOCKED, PENDING, DROPPED -> false;
    };
  }

  /**
   * Lowers the limit of a partition being handed over to {@code offset}: nothing at or above it is
   * sent any more. What is in flight there, or being written, can still be answered.
   */
  private void cut(Part<R> p, long offset) {
    if (offset >= p.limit) {
      return;
    }
    p.limit = offset;
    for (Iterator<Rec<R>> newest = p.log.descendingIterator(); newest.hasNext(); ) {
      Rec<R> rec = newest.next();
      if (rec.offset < offset) {
        break;
      }
      drop(rec);
    }
    refresh(p);
  }

  /**
   * This replica will not send the record (again): it is the next owner's. Nothing happens to a
   * record that is in flight, being written as a dead letter, or acknowledged.
   */
  private void drop(Rec<R> rec) {
    Part<R> p = rec.part;
    switch (rec.state) {
      case IN_FLIGHT, ACKED, DROPPED -> {
        return;
      }
      case DEAD_LETTERING -> {
        if (rec.writing) {
          return;
        }
        p.deadLettering.remove(rec.offset);
      }
      case READY -> p.ready.remove(rec.offset);
      case BLOCKED -> {
        p.blocked.remove(rec.offset);
        reblocked(p);
      }
      case PENDING, WAITING, DELAYED -> {}
    }
    untime(rec);
    rec.state = State.DROPPED;
    rec.payload = null;
    rec.key = null;
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

  /**
   * Ends the sending of the revoked partitions whose time for it is over, and releases those that
   * have nothing outstanding, or have waited long enough.
   */
  private void finishHandOvers(List<DispatchAction<R>> out) {
    if (draining.isEmpty()) {
      return;
    }
    List<Part<R>> done = new ArrayList<>();
    for (Part<R> p : draining) {
      long waited = now - p.drainStart;
      if (waited >= handOverSendNanos || receivers.isEmpty()) {
        // Nothing more is sent: the time for it is over, or there is nobody to send to.
        cut(p, p.commit());
      }
      if ((answered(p) && p.drained()) || waited >= revokeGraceNanos) {
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

  private void fetched(RecordsFetched<R> e) {
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
      rec.fetchedAt = now;
      rec.windowMark = p.atWindow ? p.windowHolds - 1 : p.windowHolds;
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
      rec.admittedAt = now;
      rec.windowHeld = rec.windowMark != p.windowHolds;
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
      p.windowHolds++;
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
    rec.readyAt = now;
    rec.blockedNow = false;
    rec.blockedNanos = 0;
    rec.part.ready.put(rec.offset, rec);
    if (!rec.delivered && behindBlock(rec)) {
      rec.blockedNow = true;
      rec.blockedSince = now;
    }
  }

  private static <R> boolean behindBlock(Rec<R> rec) {
    TreeSet<Long> blocked = rec.part.blocked;
    return !blocked.isEmpty() && rec.offset > blocked.first();
  }

  /**
   * The blocked records of a partition changed: starts or ends the time each record ready for its
   * first delivery spends behind one, which is ordering time and not credit time. Blocking is rare,
   * and a partition holds a bounded number of ready records.
   */
  private void reblocked(Part<R> p) {
    for (Rec<R> rec : p.ready.values()) {
      if (rec.delivered) {
        continue;
      }
      boolean behind = behindBlock(rec);
      if (behind && !rec.blockedNow) {
        rec.blockedNow = true;
        rec.blockedSince = now;
      } else if (!behind && rec.blockedNow) {
        rec.blockedNow = false;
        rec.blockedNanos += now - rec.blockedSince;
      }
    }
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
    r.lastSent = ++sends;
    // A receiver that still holds an expired copy: its answers are ambiguous from now on.
    rec.resentToFormer = rec.former != null && rec.former.remove(r);
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
      queued(rec);
      p.sent++;
      if (p.draining) {
        // Nothing is fetched for a partition being handed over, and nothing counted as buffered.
        return;
      }
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

  /**
   * Tells the timings how long a record waited for its first delivery, which happens now. Its time
   * before it was let in is the window's or the ordering's; its time between being let in and being
   * ready, behind its key, the ordering's; its time ready, the credit's.
   */
  private void queued(Rec<R> rec) {
    long pending = rec.admittedAt - rec.fetchedAt;
    // Ready, and behind a blocked record of its partition: credit would not have sent it.
    long blocked = rec.blockedNanos + (rec.blockedNow ? now - rec.blockedSince : 0);
    long ordering = rec.readyAt - rec.admittedAt + (rec.windowHeld ? 0 : pending) + blocked;
    long window = rec.windowHeld ? pending : 0;
    long credit = now - rec.readyAt - blocked;
    Wait waited = Wait.NOTHING;
    long most = 0;
    if (credit > most) {
      waited = Wait.CREDIT;
      most = credit;
    }
    if (ordering > most) {
      waited = Wait.ORDERING;
      most = ordering;
    }
    if (window > most) {
      waited = Wait.WINDOW;
    }
    timings.queued(now - rec.fetchedAt, waited);
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
          returned(rec, Cause.RECEIVER_LEFT);
        } else {
          rec.former.remove(r);
        }
      }
    }
    r.held.clear();
  }

  /**
   * A receiver gave records back unread. Each one it holds in flight is taken back as if it had
   * left, and sent elsewhere when another receiver has credit; one it holds after the deadline
   * passed has failed already and is only forgotten by it.
   */
  private void givenBack(Returned<R> e) {
    Receiver<R> r = receivers.get(e.receiver());
    TreeMap<Long, Rec<R>> held = r == null ? null : r.held.get(e.partition());
    if (held == null) {
      return;
    }
    for (long offset : e.offsets()) {
      Rec<R> rec = held.get(offset);
      if (rec == null) {
        continue;
      }
      unhold(r, rec);
      if (rec.holder == r) {
        rec.avoid = r;
        returned(rec, Cause.RETURNED);
      } else {
        rec.former.remove(r);
      }
    }
  }

  /**
   * The receiver a record was in flight to left, or gave it back unread. The delivery is taken
   * back: the record is sent again under the same attempt number.
   */
  private void returned(Rec<R> rec, Cause cause) {
    Part<R> p = rec.part;
    land(rec);
    p.inFlight--;
    rec.holder = null;
    if (p.draining && rec.offset >= p.limit) {
      // Not sent again by this replica: the next owner has it.
      rec.state = State.DROPPED;
      rec.payload = null;
      rec.key = null;
      return;
    }
    rec.attempts--;
    rec.cause = cause;
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
      if (rec.holder == r) {
        answered(rec, true);
      }
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
      answered(rec, false);
      rec.avoid = r;
      failed(
          rec, Cause.REJECTED, e.reason() == null || e.reason().isEmpty() ? null : e.reason(), out);
    } else {
      // Given back after its deadline passed: that attempt has failed already.
      rec.former.remove(r);
    }
  }

  /**
   * Tells the timings of an answer from the receiver a record is in flight to, if it can only be
   * for this delivery and came within its deadline. One that came after the deadline, handled
   * before the time was passed, is late; one from a receiver that holds an expired copy as well may
   * be for that copy. Whether the answer counts does not depend on this.
   */
  private void answered(Rec<R> rec, boolean acknowledged) {
    long took = now - rec.sentAt;
    if (!rec.resentToFormer && took < rec.flight.deadlineNanos) {
      timings.answered(took, acknowledged);
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
      // The next owner delivers the record again, and dead-letters it if it must. Nothing is
      // committed past it here, so nothing above it is sent any more either.
      giveUp(rec);
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
    if (p.draining && rec.offset >= p.limit) {
      // Not sent again by this replica: the next owner has it.
      rec.state = State.DROPPED;
      rec.payload = null;
      rec.key = null;
      return;
    }
    if (settings.maxAttempts() > 0 && rec.attempts >= settings.maxAttempts()) {
      if (settings.onExhausted() == OnExhausted.DEAD_LETTER) {
        rec.state = State.DEAD_LETTERING;
        p.deadLettering.put(rec.offset, rec);
        writeDeadLetter(rec, out);
      } else if (p.draining) {
        // It would block a partition that is leaving: its next owner counts its attempts anew.
        giveUp(rec);
      } else {
        rec.state = State.BLOCKED;
        p.blocked.add(rec.offset);
        reblocked(p);
        out.add(new PartitionBlocked<>(p.id, rec.offset));
        refresh(p);
      }
      return;
    }
    long wait = doubled(redeliveryBackoffNanos, redeliveryBackoffMaxNanos, rec.attempts - 1);
    if (wait == 0) {
      ready(rec);
      refresh(p);
    } else if (p.draining && now + wait - (p.drainStart + handOverSendNanos) >= 0) {
      // It could be sent again only after sending has ended.
      giveUp(rec);
    } else {
      rec.state = State.DELAYED;
      time(rec, now + wait);
    }
  }

  /**
   * A record of a partition being handed over cannot be finished by this replica: it is left to the
   * next owner, and so is everything above it that is not already with a receiver.
   */
  private void giveUp(Rec<R> rec) {
    if (rec.state == State.DEAD_LETTERING) {
      rec.part.deadLettering.remove(rec.offset);
    }
    untime(rec);
    rec.state = State.DROPPED;
    rec.payload = null;
    rec.key = null;
    cut(rec.part, rec.offset);
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
        reblocked(p);
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
    if (rec.key != null) {
      // The next record of the key takes its turn; while the partition is handed over, only if
      // it lies below the limit. The records that wait behind one that does not, do not either.
      Lane<R> lane = p.lanes.get(rec.key);
      Rec<R> next = lane.waiting == null ? null : lane.waiting.pollFirst();
      if (next == null || next.state == State.DROPPED) {
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
