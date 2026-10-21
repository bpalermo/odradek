package io.github.bpalermo.odradek.e2eclient;

/**
 * When the producer sends, by a {@link RateSchedule}, on elapsed monotonic time: no I/O and no
 * clock of its own, every moment is given in nanoseconds since the start.
 *
 * <p>Records are due one interval of their step apart. A step that ends before the next record is
 * due makes that record due at its end, so the next step starts on time however slow the current
 * one is. A record whose deadline passed while the producer waited (for the outcome of its key's
 * previous record) is checked against the step of the moment it is sent: once that step has ended,
 * deadlines of the steps before are dropped and the new step's count starts at its beginning; once
 * the schedule has ended, nothing more is sent.
 *
 * <p>Not thread-safe: the producer's loop owns it.
 */
final class Pacer {
  private final RateSchedule schedule;

  /** When the next record is due. */
  private long next;

  Pacer(RateSchedule schedule) {
    this(schedule, 0);
  }

  /**
   * A pacer that joins the schedule {@code joinedAt} nanoseconds after its start: the first record
   * is due then, at the rate of the step in force, and nothing is sent for the time before.
   */
  Pacer(RateSchedule schedule, long joinedAt) {
    this.schedule = schedule;
    this.next = joinedAt;
  }

  /** When the next record is due; -1 when the schedule ends before then. */
  long due() {
    return schedule.at(next) == null ? -1 : next;
  }

  /**
   * Whether a record may be sent at {@code now}: false once the schedule has ended. When a later
   * step than the one of the deadline is in force, the deadlines of the earlier ones are dropped.
   */
  boolean admit(long now) {
    RateSchedule.Active active = schedule.at(now);
    if (active == null) {
      return false;
    }
    next = Math.max(next, active.startNanos());
    return true;
  }

  /** The record admitted was sent, or refused: the next one is due an interval later. */
  void sent() {
    RateSchedule.Active active = schedule.at(next);
    if (active == null) {
      return;
    }
    long candidate = next + active.intervalNanos();
    next = Math.min(candidate, active.endNanos());
  }
}
