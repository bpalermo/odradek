package io.github.bpalermo.odradek.server.dispatch;

import com.google.protobuf.InvalidProtocolBufferException;
import io.github.bpalermo.odradek.delivery.v1.Replica;
import io.github.bpalermo.odradek.delivery.v1.ReplicaList;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The assignor of the coordination group (ADR 6): it assigns no partition to anyone and uses the
 * group's membership to tell every replica of a dispatcher where the others are, as Kafka Streams
 * does for its instances.
 *
 * <p>Both directions carry a {@link ReplicaList}, the message sidecars are sent:
 *
 * <ul>
 *   <li>a member joins with a list of itself alone, at the address it advertises, under the highest
 *       generation it has been told ({@code subscriptionUserData});
 *   <li>the member Kafka elects leader of the rebalance answers every member with the same list:
 *       every address, ordered, under a generation one above the highest any member joined with
 *       ({@link #merge}).
 * </ul>
 *
 * <p>The generation is made here and not taken from Kafka's own, which starts again at 1 when the
 * brokers have dropped a group that stayed empty, while a replica that could not reach them still
 * serves sidecars under a list with a higher one; a sidecar would ignore every list after that.
 * This one never goes back while one replica that knew the last list is still a member.
 *
 * <p>Kafka creates the assignor from its class name, so it finds the replica it belongs to in the
 * consumer's configuration, where {@link Membership} put it.
 */
public final class ReplicaAssignor implements ConsumerPartitionAssignor, Configurable {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicaAssignor.class);

  /** The protocol name the members of a coordination group agree on. */
  public static final String NAME = "odradek-replicas";

  /** The configuration key under which a consumer carries its {@link Binding}. */
  static final String BINDING_CONFIG = "odradek.membership.binding";

  /** The replica an assignor speaks for. */
  interface Binding {
    /** Where sidecars reach this replica, as host:port. */
    String address();

    /** The highest generation this replica has been told; 0 when it has been told none. */
    long generation();

    /** The group settled on this list. Called on the thread that polls the consumer. */
    void replicas(ReplicaList list);
  }

  private Binding binding;

  @Override
  public void configure(Map<String, ?> configs) {
    if (!(configs.get(BINDING_CONFIG) instanceof Binding found)) {
      throw new ConfigException(
          ReplicaAssignor.class.getName()
              + " is installed by the dispatcher; it cannot be named in"
              + " partition.assignment.strategy");
    }
    binding = found;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public ByteBuffer subscriptionUserData(Set<String> topics) {
    return ByteBuffer.wrap(self(binding.address(), binding.generation()).toByteArray());
  }

  @Override
  public GroupAssignment assign(Cluster metadata, GroupSubscription subscriptions) {
    Map<String, Subscription> members = subscriptions.groupSubscription();
    ReplicaList list = merge(members.values().stream().map(Subscription::userData).toList());
    if (list.getReplicasCount() != members.size()) {
      // Harmless when a replica restarted where it was and its old membership has not run out
      // yet. Otherwise two replicas at one address are one to every sidecar, and the partitions
      // of the other reach nobody.
      LOG.warn(
          "the coordination group has {} members and they advertise {} addresses: {}. Either a"
              + " replica restarted at the same address, and the brokers have not given up on its"
              + " old membership yet; or several replicas advertise one address, which they must"
              + " not: dispatch.advertisedAddress cannot be set in a configuration that several"
              + " replicas read",
          members.size(),
          list.getReplicasCount(),
          addresses(list));
    }
    byte[] data = list.toByteArray();
    Map<String, Assignment> assignments = new HashMap<>();
    for (String member : members.keySet()) {
      assignments.put(member, new Assignment(List.of(), ByteBuffer.wrap(data)));
    }
    return new GroupAssignment(assignments);
  }

  @Override
  public void onAssignment(Assignment assignment, ConsumerGroupMetadata metadata) {
    ReplicaList list = parse(assignment.userData());
    if (list == null) {
      LOG.error(
          "the coordination group {} answered with something that is not a list of replicas; the"
              + " list held is kept",
          metadata.groupId());
      return;
    }
    binding.replicas(list);
  }

  /** What a replica joins with: itself alone, under the highest generation it knows. */
  static ReplicaList self(String address, long generation) {
    return ReplicaList.newBuilder()
        .setGeneration(generation)
        .addReplicas(Replica.newBuilder().setAddress(address))
        .build();
  }

  /**
   * The list every member is answered with, from what each joined with: every address once, ordered
   * so that the list does not depend on who makes it, under a generation one above the highest any
   * of them knew. A member that said nothing usable is left out.
   */
  static ReplicaList merge(Collection<ByteBuffer> joined) {
    Set<String> addresses = new TreeSet<>();
    long highest = 0;
    for (ByteBuffer data : joined) {
      ReplicaList member = parse(data);
      if (member == null) {
        LOG.warn("a member of the coordination group joined with no list of itself; left out");
        continue;
      }
      if (Long.compareUnsigned(member.getGeneration(), highest) > 0) {
        highest = member.getGeneration();
      }
      for (Replica replica : member.getReplicasList()) {
        if (!replica.getAddress().isEmpty()) {
          addresses.add(replica.getAddress());
        }
      }
    }
    ReplicaList.Builder list = ReplicaList.newBuilder().setGeneration(highest + 1);
    addresses.forEach(address -> list.addReplicas(Replica.newBuilder().setAddress(address)));
    return list.build();
  }

  /** The addresses of a list, in its order. */
  static List<String> addresses(ReplicaList list) {
    return list.getReplicasList().stream().map(Replica::getAddress).toList();
  }

  private static ReplicaList parse(ByteBuffer data) {
    if (data == null) {
      return null;
    }
    try {
      // A view: Kafka may hand the same buffer to others.
      return ReplicaList.parseFrom(data.duplicate());
    } catch (InvalidProtocolBufferException e) {
      return null;
    }
  }
}
