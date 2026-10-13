package io.github.bpalermo.odradek.server.receive;

import io.github.bpalermo.odradek.delivery.v1.Replica;
import io.github.bpalermo.odradek.delivery.v1.ReplicaList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * The dispatcher's replicas as one Consume stream knows them: the newest list any of its Deliver
 * streams was sent. No I/O.
 *
 * <p>Until a first list arrives nothing is known, and the only address to try is the one in the
 * configuration. Every replica sends the same list under the same generation, so a list is taken
 * only when its generation is higher than the one held; the same list arriving on every stream
 * changes nothing. Losing every stream forgets the list: the replicas may all have been replaced.
 */
final class Replicas {
  private boolean known;
  private long generation;
  private Set<String> addresses = Set.of();

  /** Whether a list is held. */
  boolean known() {
    return known;
  }

  /** The replicas of the list held, as host:port; empty when none is. */
  Set<String> addresses() {
    return addresses;
  }

  /**
   * A list arrived. Returns whether it replaced the one held: it is the first, or its generation is
   * higher.
   */
  boolean offer(ReplicaList list) {
    if (known && Long.compareUnsigned(list.getGeneration(), generation) <= 0) {
      return false;
    }
    Set<String> listed = new LinkedHashSet<>();
    for (Replica replica : list.getReplicasList()) {
      if (!replica.getAddress().isEmpty()) {
        listed.add(replica.getAddress());
      }
    }
    known = true;
    generation = list.getGeneration();
    addresses = Collections.unmodifiableSet(listed);
    return true;
  }

  /** Every stream is lost: what was known may no longer be true. */
  void forget() {
    known = false;
    generation = 0;
    addresses = Set.of();
  }
}
