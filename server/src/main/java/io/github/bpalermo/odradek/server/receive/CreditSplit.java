package io.github.bpalermo.odradek.server.receive;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Divides the credit an app grants on its Consume stream among the Deliver streams that feed it:
 * one per topic and dispatcher replica. No I/O.
 *
 * <p>A credit granted to a replica cannot be taken back, and a replica whose partitions are idle
 * never uses it. So no stream is given the app's credit to keep: each has a <b>window</b>, the most
 * it may have outstanding, and what the app granted beyond the windows stays here until a record
 * arrives somewhere, which is where it is then granted.
 *
 * <ul>
 *   <li><b>One stream</b> has no window: everything the app grants is passed on. Nothing can sit
 *       idle on credit that something else needs, because there is nothing else. That is one stream
 *       wanted, not one stream open: with two wanted and one being opened again, the one that is
 *       open keeps to its window, or it would hold everything when the other came back.
 *   <li><b>A window starts at 1</b> and doubles each time its stream uses all it had outstanding,
 *       so a stream that carries records gets a deep pipeline within a few round trips and one that
 *       carries none holds a single credit.
 *   <li><b>A window is at most the budget divided by twice the number of streams</b>, the budget
 *       being what the app can still take. All streams together can therefore sit on at most half
 *       of it, however busy they once were, and the other half follows the records.
 *   <li><b>A stream with nothing outstanding is granted one credit even when the budget is all
 *       promised</b> to other streams, as long as the app can take a record at all. Without that,
 *       an app that asks for one record at a time would give its one credit to one stream and never
 *       hear of the others. This is the only way the server comes to hold a record the app has not
 *       asked for, so it is also where that is bounded: such a credit is granted only while the
 *       credits outstanding exceed the budget by less than the number of streams less one. If every
 *       outstanding credit were used at once, that many records would wait here for the app.
 * </ul>
 *
 * <p>Not thread-safe: one instance belongs to one Consume stream's thread.
 */
final class CreditSplit {
  private static final long LARGEST_WINDOW = 1L << 30;

  private static final class Stream {
    final long id;

    /** Credits granted to the dispatcher on this stream and not used by a record yet. */
    long outstanding;

    long window = 1;

    Stream(long id) {
      this.id = id;
    }
  }

  private final Map<Long, Stream> streams = new LinkedHashMap<>();

  /** Credits the app granted that no record handed to it has used. */
  private long app;

  /** Records received from the dispatcher and not handed to the app yet. */
  private long buffered;

  /** A stream was accepted by its replica: it may be granted credit from now on. */
  void opened(long stream) {
    streams.put(stream, new Stream(stream));
  }

  /** A stream ended. The credit it had outstanding is gone with it. */
  void closed(long stream) {
    streams.remove(stream);
  }

  /** The app granted credits. */
  void appGranted(long records) {
    app += records;
  }

  /** Records arrived on a stream; each used one of its credits. They wait here for the app. */
  void received(long stream, int records) {
    buffered += records;
    Stream from = streams.get(stream);
    if (from == null) {
      return;
    }
    if (records >= from.outstanding) {
      from.window = Math.min(from.window * 2, LARGEST_WINDOW);
    }
    from.outstanding = Math.max(0, from.outstanding - records);
  }

  /** Records that were waiting were handed to the app; each used one of its credits. */
  void handed(int records) {
    app -= records;
    buffered -= records;
  }

  /** Records that were waiting will never reach the app: rejected, or their partition released. */
  void dropped(int records) {
    buffered -= records;
  }

  /** Credits of the app that no record handed to it has used. */
  long appCredit() {
    return app;
  }

  /** Records received and not handed to the app yet. */
  long buffered() {
    return buffered;
  }

  /** Credits granted on a stream and not used yet; 0 for a stream that is not open. */
  long outstanding(long stream) {
    Stream found = streams.get(stream);
    return found == null ? 0 : found.outstanding;
  }

  /**
   * What to grant now, by stream, and records it as granted. Empty while the app cannot take a
   * record beyond those already waiting here.
   *
   * @param wanted how many streams there are meant to be: one per topic and replica, whether open
   *     or being opened
   */
  Map<Long, Long> grants(int wanted) {
    Map<Long, Long> grants = new LinkedHashMap<>();
    long budget = app - buffered;
    if (budget <= 0 || streams.isEmpty()) {
      return grants;
    }
    int count = Math.max(wanted, streams.size());
    if (count == 1) {
      Stream only = streams.values().iterator().next();
      grant(only, budget - only.outstanding, grants);
      return grants;
    }
    long free = budget;
    for (Stream stream : streams.values()) {
      free -= stream.outstanding;
    }
    long largest = Math.max(1, budget / (2L * count));
    // The emptiest first: when there is less than everybody's window, it goes where the dispatcher
    // can send nothing at all.
    List<Stream> order = new ArrayList<>(streams.values());
    order.sort(Comparator.comparingLong(stream -> stream.outstanding));
    for (Stream stream : order) {
      long room = Math.min(stream.window, largest) - stream.outstanding;
      if (room <= 0) {
        continue;
      }
      long given = Math.min(room, Math.max(0, free));
      if (given == 0 && stream.outstanding == 0 && free > -(count - 1)) {
        given = 1;
      }
      free -= given;
      grant(stream, given, grants);
    }
    return grants;
  }

  private static void grant(Stream stream, long records, Map<Long, Long> grants) {
    if (records > 0) {
      stream.outstanding += records;
      grants.put(stream.id, records);
    }
  }
}
