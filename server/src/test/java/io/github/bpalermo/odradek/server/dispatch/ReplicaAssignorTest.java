package io.github.bpalermo.odradek.server.dispatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.bpalermo.odradek.delivery.v1.Replica;
import io.github.bpalermo.odradek.delivery.v1.ReplicaList;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupSubscription;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

/**
 * The assignor of the coordination group, without a broker: what a member joins with, what the
 * leader answers, and the generation the lists carry. The group itself is played by {@link
 * #rebalance}, which does what a broker and the consumers do between them: collects what every
 * member joins with, has one of them assign, and hands each its answer.
 */
class ReplicaAssignorTest {
  /** Kafka's own constructors are on their way out; its mock consumer still makes one. */
  private static final ConsumerGroupMetadata METADATA =
      new MockConsumer<byte[], byte[]>("none").groupMetadata();

  /** One replica as its assignor sees it. */
  private static final class Member implements ReplicaAssignor.Binding {
    final String address;
    final ReplicaAssignor assignor = new ReplicaAssignor();
    final List<ReplicaList> told = new ArrayList<>();
    long generation;

    Member(String address) {
      this.address = address;
      assignor.configure(Map.of(ReplicaAssignor.BINDING_CONFIG, this));
    }

    @Override
    public String address() {
      return address;
    }

    @Override
    public long generation() {
      return generation;
    }

    @Override
    public void replicas(ReplicaList list) {
      told.add(list);
      generation = list.getGeneration();
    }

    ReplicaList last() {
      return told.get(told.size() - 1);
    }
  }

  private static ReplicaList list(long generation, String... addresses) {
    ReplicaList.Builder list = ReplicaList.newBuilder().setGeneration(generation);
    for (String address : addresses) {
      list.addReplicas(Replica.newBuilder().setAddress(address));
    }
    return list.build();
  }

  /** One rebalance of a group with these members, the first of them its leader. */
  private static void rebalance(Member... members) {
    Map<String, Subscription> joined = new LinkedHashMap<>();
    for (Member member : members) {
      joined.put(
          "member-" + member.address,
          new Subscription(List.of(), member.assignor.subscriptionUserData(Set.of())));
    }
    Map<String, Assignment> answers =
        members[0]
            .assignor
            .assign(Cluster.empty(), new GroupSubscription(joined))
            .groupAssignment();
    assertThat(answers).containsOnlyKeys(joined.keySet());
    for (Member member : members) {
      member.assignor.onAssignment(answers.get("member-" + member.address), METADATA);
    }
  }

  @Test
  void isNamedAndMustBeInstalledByTheDispatcher() {
    assertThat(new ReplicaAssignor().name()).isEqualTo("odradek-replicas");
    // Named in a cluster's properties, it would have no replica to speak for.
    assertThatThrownBy(() -> new ReplicaAssignor().configure(Map.of("group.id", "g")))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("partition.assignment.strategy");
  }

  @Test
  void aMemberJoinsWithItselfAloneUnderTheGenerationItKnows() throws Exception {
    Member member = new Member("10.0.0.1:9465");
    assertThat(ReplicaList.parseFrom(member.assignor.subscriptionUserData(Set.of())))
        .isEqualTo(list(0, "10.0.0.1:9465"));
    member.generation = 12;
    assertThat(ReplicaList.parseFrom(member.assignor.subscriptionUserData(Set.of("ignored"))))
        .isEqualTo(list(12, "10.0.0.1:9465"));
  }

  @Test
  void everyMemberIsToldEveryAddressAndAssignedNothing() {
    Member a = new Member("10.0.0.1:9465");
    Member b = new Member("10.0.0.2:9465");
    Member c = new Member("[fd00::3]:9465");
    Map<String, Subscription> joined = new LinkedHashMap<>();
    for (Member member : List.of(a, b, c)) {
      joined.put(
          member.address, new Subscription(List.of(), member.assignor.subscriptionUserData(null)));
    }
    Map<String, Assignment> answers =
        b.assignor.assign(Cluster.empty(), new GroupSubscription(joined)).groupAssignment();

    assertThat(answers).containsOnlyKeys(a.address, b.address, c.address);
    for (Assignment answer : answers.values()) {
      assertThat(answer.partitions()).isEmpty();
      assertThat(ReplicaAssignor.merge(List.of(answer.userData())).getReplicasList())
          .extracting(Replica::getAddress)
          .containsExactly("10.0.0.1:9465", "10.0.0.2:9465", "[fd00::3]:9465");
    }
    // The same bytes for all: every replica hands its sidecars the same list.
    assertThat(answers.values().stream().map(Assignment::userData).distinct()).hasSize(1);
  }

  @Test
  void theListDoesNotDependOnWhoMakesItOrOnTheOrderOfTheMembers() {
    List<ByteBuffer> one =
        List.of(
            ByteBuffer.wrap(list(3, "b:1").toByteArray()),
            ByteBuffer.wrap(list(0, "c:1").toByteArray()),
            ByteBuffer.wrap(list(2, "a:1").toByteArray()));
    List<ByteBuffer> other = List.of(one.get(2), one.get(0), one.get(1));
    assertThat(ReplicaAssignor.merge(one)).isEqualTo(list(4, "a:1", "b:1", "c:1"));
    assertThat(ReplicaAssignor.merge(other)).isEqualTo(ReplicaAssignor.merge(one));
    // Read twice: Kafka hands the same buffers to more than one reader.
    assertThat(ReplicaAssignor.merge(one)).isEqualTo(list(4, "a:1", "b:1", "c:1"));
  }

  @Test
  void theGenerationIsOneAboveTheHighestAnyMemberKnew() {
    assertThat(
            ReplicaAssignor.merge(List.of(ByteBuffer.wrap(list(0, "a:1").toByteArray())))
                .getGeneration())
        .isEqualTo(1);
    assertThat(
            ReplicaAssignor.merge(
                    List.of(
                        ByteBuffer.wrap(list(7, "a:1").toByteArray()),
                        ByteBuffer.wrap(list(41, "b:1").toByteArray()),
                        ByteBuffer.wrap(list(0, "c:1").toByteArray())))
                .getGeneration())
        .isEqualTo(42);
    // Unsigned, as on the wire.
    assertThat(
            ReplicaAssignor.merge(
                    List.of(
                        ByteBuffer.wrap(list(Long.MIN_VALUE, "a:1").toByteArray()),
                        ByteBuffer.wrap(list(5, "b:1").toByteArray())))
                .getGeneration())
        .isEqualTo(Long.MIN_VALUE + 1);
  }

  @Test
  void aMemberThatSaysNothingUsableIsLeftOutAndAnAddressIsListedOnce() {
    List<ByteBuffer> joined = new ArrayList<>();
    joined.add(ByteBuffer.wrap(list(1, "a:1").toByteArray()));
    joined.add(null);
    joined.add(ByteBuffer.allocate(0));
    joined.add(ByteBuffer.wrap("not a list".getBytes(StandardCharsets.UTF_8)));
    joined.add(ByteBuffer.wrap(list(1, "").toByteArray()));
    // Two replicas that advertise the same address: one entry, which the leader complains of.
    joined.add(ByteBuffer.wrap(list(1, "a:1").toByteArray()));
    assertThat(ReplicaAssignor.merge(joined)).isEqualTo(list(2, "a:1"));
  }

  @Test
  void anAnswerThatIsNoListLeavesTheMemberWithWhatItHeld() {
    Member member = new Member("a:1");
    rebalance(member);
    member.assignor.onAssignment(new Assignment(List.of(), null), METADATA);
    member.assignor.onAssignment(
        new Assignment(List.of(), ByteBuffer.wrap(new byte[] {(byte) 0xff, (byte) 0xff})),
        METADATA);
    assertThat(member.told).containsExactly(list(1, "a:1"));
  }

  @Test
  void replicasThatComeAndGoAreAlwaysListedUnderAHigherGeneration() {
    Member a = new Member("a:1");
    Member b = new Member("b:1");
    Member c = new Member("c:1");

    rebalance(a);
    assertThat(a.last()).isEqualTo(list(1, "a:1"));

    // b starts, knowing nothing: the list is above what a knew, whoever leads.
    rebalance(b, a);
    assertThat(a.last()).isEqualTo(list(2, "a:1", "b:1"));
    assertThat(b.last()).isEqualTo(a.last());

    rebalance(a, b, c);
    assertThat(c.last()).isEqualTo(list(3, "a:1", "b:1", "c:1"));

    // a stops. The same members settling again, for whatever reason, make a new generation too.
    rebalance(c, b);
    rebalance(b, c);
    assertThat(b.last()).isEqualTo(list(5, "b:1", "c:1"));
    assertThat(c.last()).isEqualTo(b.last());

    // A rolling update down to replicas that all started after the first list: what was known
    // is carried by whoever is still there.
    Member d = new Member("d:1");
    rebalance(d, c);
    Member e = new Member("e:1");
    rebalance(e, d);
    assertThat(e.last()).isEqualTo(list(7, "d:1", "e:1"));
  }

  @Test
  void aReplicaThatWasCutOffBringsItsGenerationBackToAGroupThatStartedAgain() {
    // b served sidecars under generation 9 and lost the brokers; meanwhile the group emptied,
    // was dropped, and a new replica started it again from nothing.
    Member b = new Member("b:1");
    b.generation = 9;
    Member fresh = new Member("f:1");
    rebalance(fresh);
    assertThat(fresh.last()).isEqualTo(list(1, "f:1"));

    // When b is back, the list must be above 9, or b's sidecars would ignore it for good.
    rebalance(fresh, b);
    assertThat(b.last()).isEqualTo(list(10, "b:1", "f:1"));
    assertThat(fresh.last()).isEqualTo(b.last());
  }

  @Test
  void assignsAsAnyKafkaAssignorDoes() {
    ConsumerPartitionAssignor assignor = new ReplicaAssignor();
    assertThat(assignor.supportedProtocols())
        .containsExactly(ConsumerPartitionAssignor.RebalanceProtocol.EAGER);
  }
}
