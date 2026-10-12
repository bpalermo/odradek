package io.github.bpalermo.odradek.server.dispatch;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;

/**
 * Drives the core with random events and checks, after every one, what must always hold. The model
 * here is deliberately naive: it knows the rules, not how the core applies them.
 */
class DispatchCoreRandomTest {
  private static final int SEEDS = 8;
  private static final int STEPS = 4000;
  private static final int PARTITIONS = 4;
  private static final int MAX_RECEIVERS = 5;
  private static final long DEADLINE = 400;
  private static final long GRACE = 150;
  private static final long WINDOW = 12;
  private static final int MAX_BUFFERED = 6;
  private static final int MAX_FETCH = 4;

  @Test
  void keyOrdering() {
    run(Ordering.KEY, 0, OnExhausted.BLOCK);
  }

  @Test
  void partitionOrdering() {
    run(Ordering.PARTITION, 0, OnExhausted.BLOCK);
  }

  @Test
  void noOrdering() {
    run(Ordering.NONE, 0, OnExhausted.BLOCK);
  }

  @Test
  void keyOrderingWithDeadLetters() {
    run(Ordering.KEY, 3, OnExhausted.DEAD_LETTER);
  }

  @Test
  void partitionOrderingWithDeadLetters() {
    run(Ordering.PARTITION, 2, OnExhausted.DEAD_LETTER);
  }

  @Test
  void noOrderingWithDeadLetters() {
    run(Ordering.NONE, 3, OnExhausted.DEAD_LETTER);
  }

  @Test
  void keyOrderingWithBlocking() {
    run(Ordering.KEY, 3, OnExhausted.BLOCK);
  }

  @Test
  void partitionOrderingWithBlocking() {
    run(Ordering.PARTITION, 3, OnExhausted.BLOCK);
  }

  @Test
  void noOrderingWithBlocking() {
    run(Ordering.NONE, 2, OnExhausted.BLOCK);
  }

  private static void run(Ordering ordering, int maxAttempts, OnExhausted onExhausted) {
    for (int seed = 0; seed < SEEDS; seed++) {
      Simulation simulation = new Simulation(ordering, maxAttempts, onExhausted, seed);
      try {
        simulation.chaos(STEPS);
        simulation.drain();
      } catch (AssertionError | RuntimeException e) {
        throw new AssertionError(
            "seed " + seed + ", step " + simulation.step + ": " + simulation.last, e);
      }
    }
  }

  /** One partition as the test sees it: the topic's log, and what this assignment has done. */
  private static final class Partition {
    final int id;

    /** The topic: offsets (with gaps) and keys, grown as it is read. */
    final List<Long> offsets = new ArrayList<>();

    final List<String> keys = new ArrayList<>();

    /** What the group has committed: where the next assignment starts. */
    long committed;

    boolean assigned;
    boolean draining;
    long drainStart;
    boolean paused;

    /** Index in {@link #offsets} of the next record to fetch. */
    int next;

    /** Fetched in this assignment and not acknowledged: offset to key ("" for none). */
    final TreeMap<Long, String> unacked = new TreeMap<>();

    final Map<String, TreeSet<Long>> unackedByKey = new HashMap<>();
    final Map<String, Long> lastDeliveredByKey = new HashMap<>();

    /** In flight: offset to receiver. */
    final Map<Long, String> inFlight = new HashMap<>();

    final Map<Long, Long> sentAt = new HashMap<>();
    final Map<Long, Integer> attempts = new HashMap<>();
    final Set<Long> deadLettering = new HashSet<>();
    final TreeSet<Long> blocked = new TreeSet<>();
    long lastCommit;
    long newestDelivered = -1;

    Partition(int id) {
      this.id = id;
    }

    void reset() {
      assigned = false;
      draining = false;
      paused = false;
      unacked.clear();
      unackedByKey.clear();
      lastDeliveredByKey.clear();
      inFlight.clear();
      sentAt.clear();
      attempts.clear();
      deadLettering.clear();
      blocked.clear();
      newestDelivered = -1;
    }

    int buffered() {
      int count = 0;
      for (long offset : unacked.keySet()) {
        if (!attempts.containsKey(offset)) {
          count++;
        }
      }
      return count;
    }
  }

  private static final class Receiver {
    long credit;

    /** What it was sent and has neither acknowledged nor given back. */
    final TreeMap<Integer, TreeSet<Long>> held = new TreeMap<>();
  }

  private static final class Simulation {
    final Ordering ordering;
    final int maxAttempts;
    final OnExhausted onExhausted;
    final Random random;
    final DispatchCore<String> core;
    final List<Partition> partitions = new ArrayList<>();
    final TreeMap<String, Receiver> receivers = new TreeMap<>();

    /** Dead letters asked for and not answered yet: {partition, offset}. */
    final List<long[]> deadLetters = new ArrayList<>();

    /** What the event of this step must produce. */
    final Set<String> expected = new HashSet<>();

    final Set<Integer> lostNow = new HashSet<>();
    final Set<Integer> unblockable = new HashSet<>();
    long now;
    int step;
    int receiverNames;
    String last = "";

    Simulation(Ordering ordering, int maxAttempts, OnExhausted onExhausted, long seed) {
      this.ordering = ordering;
      this.maxAttempts = maxAttempts;
      this.onExhausted = onExhausted;
      this.random = new Random(seed);
      this.core =
          new DispatchCore<>(
              new DispatchSettings(
                  ordering,
                  Duration.ofNanos(DEADLINE),
                  maxAttempts,
                  onExhausted,
                  WINDOW,
                  MAX_BUFFERED,
                  Long.MAX_VALUE,
                  Duration.ofNanos(GRACE)));
      for (int id = 0; id < PARTITIONS; id++) {
        partitions.add(new Partition(id));
      }
    }

    // -----------------------------------------------------------------------------------------
    // Phases
    // -----------------------------------------------------------------------------------------

    void chaos(int steps) {
      for (step = 0; step < steps; step++) {
        int dice = random.nextInt(100);
        if (dice < 22) {
          fetch(pick(partitions), 1 + random.nextInt(MAX_FETCH), Integer.MAX_VALUE);
        } else if (dice < 40) {
          credits(pick(receivers.keySet()), 1 + random.nextInt(4));
        } else if (dice < 68) {
          acknowledge();
        } else if (dice < 73) {
          reject();
        } else if (dice < 83) {
          now += 1 + random.nextInt((int) DEADLINE / 2);
          timePassed();
        } else if (dice < 87) {
          join();
        } else if (dice < 89) {
          leave(pick(receivers.keySet()));
        } else if (dice < 93) {
          assign(pick(partitions));
        } else if (dice < 95) {
          revoke(pick(partitions));
        } else if (dice < 96) {
          lose(pick(partitions));
        } else {
          answerDeadLetter();
        }
      }
    }

    /**
     * Ends the chaos: every partition is owned, every record already in the topic is fetched,
     * receivers have credit and acknowledge all they get, dead letters are written. Then everything
     * must be committed.
     */
    void drain() {
      for (Partition p : partitions) {
        if (!p.blocked.isEmpty() && !p.draining) {
          // Nothing ends a block but giving the partition up.
          lose(p);
        }
      }
      if (receivers.isEmpty()) {
        join();
      }
      for (int round = 0; round < 10_000 && !settled(); round++) {
        step++;
        for (Partition p : partitions) {
          assign(p);
          fetch(p, MAX_FETCH, p.offsets.size());
        }
        while (!deadLetters.isEmpty()) {
          long[] letter = deadLetters.removeFirst();
          Partition p = partitions.get((int) letter[0]);
          if (p.deadLettering.contains(letter[1])) {
            acknowledged(p, letter[1]);
          }
          on(new DeadLetterWritten<>(p.id, letter[1]));
        }
        for (String name : List.copyOf(receivers.keySet())) {
          Receiver r = receivers.get(name);
          for (int partition : List.copyOf(r.held.keySet())) {
            TreeSet<Long> held = r.held.get(partition);
            if (held != null) {
              ack(name, partition, held.first(), held.last());
            }
          }
          if (r.credit < 3) {
            credits(name, 3);
          }
        }
      }
      for (Partition p : partitions) {
        long end = p.offsets.isEmpty() ? 0 : p.offsets.getLast() + 1;
        assertThat(p.assigned && !p.draining).as("partition %s owned", p.id).isTrue();
        assertThat(p.unacked).as("unacknowledged in partition %s", p.id).isEmpty();
        assertThat(core.committable(p.id)).isEqualTo(OptionalLong.of(end));
        assertThat(p.committed).as("committed in partition %s", p.id).isEqualTo(end);
      }
      assertThat(core.inFlight()).isZero();
      assertThat(core.nextDeadlineNanos()).isEmpty();
    }

    boolean settled() {
      for (Partition p : partitions) {
        if (!p.assigned || p.draining || p.next < p.offsets.size() || !p.unacked.isEmpty()) {
          return false;
        }
      }
      return true;
    }

    // -----------------------------------------------------------------------------------------
    // Events: the model first, then the core
    // -----------------------------------------------------------------------------------------

    void fetch(Partition p, int count, int limit) {
      if (!p.assigned || p.draining || p.paused) {
        return;
      }
      List<Fetched<String>> records = new ArrayList<>();
      for (int i = 0; i < count && p.next < limit; i++) {
        if (p.next == p.offsets.size()) {
          long previous = p.offsets.isEmpty() ? -1 : p.offsets.getLast();
          p.offsets.add(previous + 1 + (random.nextInt(5) == 0 ? random.nextInt(4) : 0));
          p.keys.add(random.nextInt(4) == 0 ? "" : "k" + random.nextInt(4));
        }
        long offset = p.offsets.get(p.next);
        String key = p.keys.get(p.next);
        p.next++;
        p.unacked.put(offset, key);
        p.unackedByKey.computeIfAbsent(key, k -> new TreeSet<>()).add(offset);
        records.add(
            new Fetched<>(
                offset,
                key.isEmpty() ? null : ByteString.copyFromUtf8(key),
                1,
                p.id + "/" + offset));
      }
      on(new RecordsFetched<>(p.id, records));
    }

    void join() {
      if (receivers.size() < MAX_RECEIVERS) {
        String name = "r" + receiverNames++;
        receivers.put(name, new Receiver());
        on(new ReceiverJoined<>(name));
      }
    }

    void leave(String name) {
      if (name == null) {
        return;
      }
      Receiver r = receivers.remove(name);
      r.held.forEach(
          (partition, offsets) -> {
            for (long offset : offsets) {
              if (name.equals(partitions.get(partition).inFlight.get(offset))) {
                attemptFailed(partitions.get(partition), offset);
              }
            }
          });
      on(new ReceiverLeft<>(name));
    }

    void credits(String name, int records) {
      if (name != null) {
        receivers.get(name).credit += records;
        on(new CreditsGranted<>(name, records));
      }
    }

    void acknowledge() {
      String name = pick(receivers.keySet());
      if (name == null) {
        return;
      }
      Receiver r = receivers.get(name);
      if (!r.held.isEmpty() && random.nextInt(4) != 0) {
        // Something it holds, sometimes with its neighbours.
        int partition = pick(r.held.keySet());
        long offset = pick(r.held.get(partition));
        ack(name, partition, offset - random.nextInt(3), offset + random.nextInt(3));
      } else {
        Partition p = pick(partitions);
        long from = p.unacked.isEmpty() ? 0 : p.unacked.firstKey();
        long first = from + random.nextInt(10) - 2;
        ack(name, p.id, first, first + random.nextInt(8) - 1);
      }
    }

    void ack(String name, int partition, long first, long last) {
      Partition p = partitions.get(partition);
      TreeSet<Long> held = receivers.get(name).held.get(partition);
      if (held != null && first <= last) {
        for (long offset : List.copyOf(held.subSet(first, true, last, true))) {
          acknowledged(p, offset);
        }
      }
      on(new Acknowledged<>(name, partition, first, last));
    }

    void reject() {
      String name = pick(receivers.keySet());
      if (name == null || receivers.get(name).held.isEmpty()) {
        return;
      }
      Receiver r = receivers.get(name);
      int partition = pick(r.held.keySet());
      long offset = pick(r.held.get(partition));
      Partition p = partitions.get(partition);
      unhold(r, partition, offset);
      if (name.equals(p.inFlight.get(offset))) {
        attemptFailed(p, offset);
      }
      on(new Rejected<>(name, partition, offset));
    }

    void timePassed() {
      for (Partition p : partitions) {
        for (long offset : List.copyOf(p.inFlight.keySet())) {
          if (now - p.sentAt.get(offset) >= DEADLINE) {
            // The receiver keeps it, and may acknowledge it yet.
            attemptFailed(p, offset);
          }
        }
      }
      on(new TimePassed<>());
    }

    void assign(Partition p) {
      if (p.assigned) {
        return;
      }
      p.assigned = true;
      p.lastCommit = p.committed;
      p.next = 0;
      while (p.next < p.offsets.size() && p.offsets.get(p.next) < p.committed) {
        p.next++;
      }
      on(new PartitionAssigned<>(p.id, p.committed));
    }

    void revoke(Partition p) {
      if (p.assigned && !p.draining) {
        p.draining = true;
        p.drainStart = now;
        p.blocked.clear();
      }
      if (!p.assigned) {
        expected.add("released " + p.id);
      }
      on(new PartitionsRevoked<>(List.of(p.id)));
    }

    void lose(Partition p) {
      if (p.assigned) {
        lostNow.add(p.id);
        p.reset();
        deadLetters.removeIf(letter -> letter[0] == p.id);
      }
      on(new PartitionsLost<>(List.of(p.id)));
    }

    void answerDeadLetter() {
      if (deadLetters.isEmpty()) {
        return;
      }
      long[] letter = deadLetters.remove(random.nextInt(deadLetters.size()));
      Partition p = partitions.get((int) letter[0]);
      boolean pending = p.deadLettering.contains(letter[1]);
      if (random.nextInt(3) == 0) {
        if (pending) {
          expected.add("dead letter " + p.id + "/" + letter[1]);
        }
        on(new DeadLetterFailed<>(p.id, letter[1]));
      } else {
        if (pending) {
          acknowledged(p, letter[1]);
        }
        on(new DeadLetterWritten<>(p.id, letter[1]));
      }
    }

    // -----------------------------------------------------------------------------------------
    // The model
    // -----------------------------------------------------------------------------------------

    void attemptFailed(Partition p, long offset) {
      p.inFlight.remove(offset);
      p.sentAt.remove(offset);
      if (p.draining || maxAttempts == 0 || p.attempts.get(offset) < maxAttempts) {
        return;
      }
      if (onExhausted == OnExhausted.DEAD_LETTER) {
        p.deadLettering.add(offset);
        expected.add("dead letter " + p.id + "/" + offset);
      } else {
        p.blocked.add(offset);
        expected.add("blocked " + p.id + "/" + offset);
      }
    }

    void acknowledged(Partition p, long offset) {
      String key = p.unacked.remove(offset);
      assertThat(key).as("acknowledged twice: %s/%s", p.id, offset).isNotNull();
      TreeSet<Long> sameKey = p.unackedByKey.get(key);
      sameKey.remove(offset);
      p.inFlight.remove(offset);
      p.sentAt.remove(offset);
      p.deadLettering.remove(offset);
      if (p.blocked.remove(offset) && p.blocked.isEmpty()) {
        expected.add("unblocked " + p.id);
      }
      for (Receiver r : receivers.values()) {
        unhold(r, p.id, offset);
      }
    }

    static void unhold(Receiver r, int partition, long offset) {
      TreeSet<Long> held = r.held.get(partition);
      if (held != null && held.remove(offset) && held.isEmpty()) {
        r.held.remove(partition);
      }
    }

    /** Whether the rules allow the record to be sent now. */
    boolean sendable(Partition p, long offset, String key) {
      if (!p.assigned || p.draining) {
        return false;
      }
      if (p.inFlight.containsKey(offset)
          || p.deadLettering.contains(offset)
          || p.blocked.contains(offset)) {
        return false;
      }
      if (!p.blocked.isEmpty() && offset > p.blocked.first()) {
        return false;
      }
      if (offset - p.unacked.firstKey() >= WINDOW) {
        return false;
      }
      return switch (ordering) {
        case NONE -> true;
        case PARTITION -> p.unacked.firstKey() == offset;
        case KEY -> key.isEmpty() || p.unackedByKey.get(key).first() == offset;
      };
    }

    // -----------------------------------------------------------------------------------------
    // The core's answer
    // -----------------------------------------------------------------------------------------

    void on(DispatchEvent<String> event) {
      last = event.toString();
      List<DispatchAction<String>> actions = core.handle(event, now);
      Set<Integer> released = new HashSet<>();
      for (DispatchAction<String> action : actions) {
        switch (action) {
          case Deliver<String> a -> delivered(a);
          case Commit<String> a -> committed(a);
          case DeadLetter<String> a -> {
            assertThat(expected.remove("dead letter " + a.partition() + "/" + a.offset()))
                .as("%s", a)
                .isTrue();
            Partition p = partitions.get(a.partition());
            assertThat(a.payload()).isEqualTo(p.id + "/" + a.offset());
            assertThat(a.attempts()).isEqualTo(p.attempts.get(a.offset())).isEqualTo(maxAttempts);
            deadLetters.add(new long[] {a.partition(), a.offset()});
          }
          case PartitionBlocked<String> a ->
              assertThat(expected.remove("blocked " + a.partition() + "/" + a.offset()))
                  .as("%s", a)
                  .isTrue();
          case PartitionUnblocked<String> a ->
              assertThat(expected.remove("unblocked " + a.partition())).as("%s", a).isTrue();
          case PauseFetching<String> a -> {
            Partition p = partitions.get(a.partition());
            assertThat(p.paused).as("%s", a).isFalse();
            assertThat(p.assigned && !p.draining).as("%s", a).isTrue();
            p.paused = true;
          }
          case ResumeFetching<String> a -> {
            Partition p = partitions.get(a.partition());
            assertThat(p.paused).as("%s", a).isTrue();
            p.paused = false;
          }
          case Drop<String> a -> {
            Receiver r = receivers.get(a.receiver());
            assertThat(a.partitions()).isNotEmpty().doesNotHaveDuplicates();
            for (int partition : a.partitions()) {
              Partition p = partitions.get(partition);
              assertThat(p.draining || lostNow.contains(partition)).as("%s", a).isTrue();
              assertThat(r.held.remove(partition)).as("%s", a).isNotNull();
            }
          }
          case PartitionReleased<String> a -> {
            Partition p = partitions.get(a.partition());
            if (expected.remove("released " + p.id)) {
              continue;
            }
            assertThat(p.draining).as("%s", a).isTrue();
            assertThat(
                    (p.inFlight.isEmpty() && p.deadLettering.isEmpty())
                        || now - p.drainStart >= GRACE)
                .as("%s", a)
                .isTrue();
            released.add(p.id);
          }
        }
      }
      for (int id : released) {
        partitions.get(id).reset();
        deadLetters.removeIf(letter -> letter[0] == id);
      }
      assertThat(expected).as("expected and not seen").isEmpty();
      lostNow.clear();
      check();
    }

    void delivered(Deliver<String> a) {
      Partition p = partitions.get(a.partition());
      Receiver r = receivers.get(a.receiver());
      long offset = a.offset();
      assertThat(r).as("receiver of %s", a).isNotNull();
      assertThat(r.credit).as("credit for %s", a).isPositive();
      r.credit--;
      String key = p.unacked.get(offset);
      assertThat(key).as("%s is unacknowledged", a).isNotNull();
      assertThat(a.payload()).isEqualTo(p.id + "/" + offset);
      assertThat(sendable(p, offset, key)).as("%s is allowed", a).isTrue();
      int attempt = p.attempts.merge(offset, 1, Integer::sum);
      assertThat(a.attempt()).isEqualTo(attempt);
      if (maxAttempts > 0) {
        assertThat(attempt).isLessThanOrEqualTo(maxAttempts);
      }
      if (ordering == Ordering.KEY && !key.isEmpty()) {
        // Deliveries of one key are in offset order, but for a record delivered again.
        Long before = p.lastDeliveredByKey.put(key, offset);
        assertThat(before == null || before < offset || (before == offset && attempt > 1))
            .as("%s after %s of the same key", a, before)
            .isTrue();
        for (long inFlight : p.inFlight.keySet()) {
          assertThat(p.unacked.get(inFlight)).as("in flight with %s", a).isNotEqualTo(key);
        }
      }
      if (ordering == Ordering.PARTITION) {
        assertThat(p.inFlight).as("in flight with %s", a).isEmpty();
      }
      p.inFlight.put(offset, a.receiver());
      p.sentAt.put(offset, now);
      p.newestDelivered = Math.max(p.newestDelivered, offset);
      r.held.computeIfAbsent(p.id, id -> new TreeSet<>()).add(offset);
    }

    void committed(Commit<String> a) {
      Partition p = partitions.get(a.partition());
      assertThat(p.assigned).as("%s", a).isTrue();
      assertThat(a.offset()).as("%s goes forward", a).isGreaterThan(p.lastCommit);
      if (!p.unacked.isEmpty()) {
        assertThat(a.offset()).as("%s", a).isLessThanOrEqualTo(p.unacked.firstKey());
      }
      long fetchedUpTo = p.next == 0 ? p.committed : p.offsets.get(p.next - 1) + 1;
      assertThat(a.offset()).as("%s", a).isLessThanOrEqualTo(Math.max(fetchedUpTo, p.committed));
      p.lastCommit = a.offset();
      p.committed = a.offset();
    }

    /** What holds after every event. */
    void check() {
      int inFlight = 0;
      boolean credit = receivers.values().stream().anyMatch(r -> r.credit > 0);
      for (Partition p : partitions) {
        inFlight += p.inFlight.size();
        OptionalLong committable = core.committable(p.id);
        assertThat(committable.isPresent()).as("partition %s known", p.id).isEqualTo(p.assigned);
        if (!p.assigned) {
          for (Receiver r : receivers.values()) {
            assertThat(r.held).as("held of a partition given up").doesNotContainKey(p.id);
          }
          continue;
        }
        long position = committable.getAsLong();
        assertThat(position).isGreaterThanOrEqualTo(p.lastCommit);
        if (!p.unacked.isEmpty()) {
          assertThat(position).isLessThanOrEqualTo(p.unacked.firstKey());
        }
        assertThat(p.newestDelivered - position)
            .as("delivered span of partition %s", p.id)
            .isLessThan(WINDOW);
        assertThat(core.blockedOffset(p.id))
            .isEqualTo(
                p.blocked.isEmpty() ? OptionalLong.empty() : OptionalLong.of(p.blocked.first()));
        if (p.draining) {
          assertThat(now - p.drainStart).as("wait for partition %s", p.id).isLessThan(GRACE);
          assertThat(!p.inFlight.isEmpty() || !p.deadLettering.isEmpty())
              .as("partition %s is still waited for", p.id)
              .isTrue();
          continue;
        }
        // Fetching is paused exactly between the two bounds.
        int buffered = p.buffered();
        if (p.paused) {
          assertThat(buffered).as("buffered while paused").isGreaterThan(MAX_BUFFERED / 2);
        } else {
          assertThat(buffered).as("buffered while fetching").isLessThan(MAX_BUFFERED);
        }
        assertThat(buffered).isLessThan(MAX_BUFFERED + MAX_FETCH);
        if (credit) {
          // Nothing that could be sent is left waiting.
          p.unacked.forEach(
              (offset, key) ->
                  assertThat(sendable(p, offset, key))
                      .as("%s/%s left unsent with credit available", p.id, offset)
                      .isFalse());
        }
      }
      assertThat(core.inFlight()).isEqualTo(inFlight);
      assertThat(core.nextDeadlineNanos().isPresent())
          .isEqualTo(inFlight > 0 || partitions.stream().anyMatch(p -> p.draining));
    }

    // -----------------------------------------------------------------------------------------

    private <T> T pick(Collection<T> from) {
      if (from.isEmpty()) {
        return null;
      }
      int index = random.nextInt(from.size());
      for (T element : from) {
        if (index-- == 0) {
          return element;
        }
      }
      throw new IllegalStateException();
    }
  }
}
