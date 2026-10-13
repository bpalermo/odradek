package io.github.bpalermo.odradek.server.dispatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.bpalermo.odradek.config.v1.Cluster;
import io.github.bpalermo.odradek.config.v1.Coordination;
import io.github.bpalermo.odradek.config.v1.Dispatch;
import io.github.bpalermo.odradek.config.v1.ServerConfig;
import io.github.bpalermo.odradek.delivery.v1.Replica;
import io.github.bpalermo.odradek.delivery.v1.ReplicaList;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * A replica's membership of the coordination group, with Kafka's mock consumer: the thread that
 * keeps the consumer polled, what it does with the lists its assignor is answered, and what it does
 * when the consumer fails. The group itself, and the assignor in it, are in the end-to-end suite.
 */
class MembershipTest {
  /** A consumer that records what it was subscribed to and whether it was closed. */
  private static final class Recording extends MockConsumer<byte[], byte[]> {
    volatile Pattern pattern;
    volatile boolean polled;

    Recording() {
      super("none");
    }

    @Override
    public synchronized void subscribe(Pattern topics) {
      pattern = topics;
      super.subscribe(topics);
    }

    @Override
    public synchronized org.apache.kafka.clients.consumer.ConsumerRecords<byte[], byte[]> poll(
        Duration timeout) {
      polled = true;
      try {
        // The mock returns at once, and the thread under test does nothing but poll.
        wait(2);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return super.poll(Duration.ofMillis(1));
    }
  }

  private final List<Recording> consumers = new CopyOnWriteArrayList<>();
  private final List<ReplicaList> told = new CopyOnWriteArrayList<>();
  private Membership membership;

  @AfterEach
  void stop() {
    if (membership != null) {
      membership.close();
    }
  }

  private static ReplicaList list(long generation, String... addresses) {
    ReplicaList.Builder list = ReplicaList.newBuilder().setGeneration(generation);
    for (String address : addresses) {
      list.addReplicas(Replica.newBuilder().setAddress(address));
    }
    return list.build();
  }

  private static void eventually(String what, BooleanSupplier condition)
      throws InterruptedException {
    long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
    while (!condition.getAsBoolean()) {
      assertThat(System.nanoTime()).as("%s within 20s", what).isLessThan(deadline);
      Thread.sleep(5);
    }
  }

  private Map<String, Object> start() {
    Map<String, Object> properties = new HashMap<>();
    membership =
        Membership.start(
            "main",
            "replicas",
            "10.0.0.1:9465",
            properties,
            configured -> {
              Recording consumer = new Recording();
              consumers.add(consumer);
              return consumer;
            },
            told::add);
    return properties;
  }

  /** What the consumer's assignor is given to speak for this replica. */
  private static ReplicaAssignor.Binding binding(Map<String, Object> properties) {
    return (ReplicaAssignor.Binding) properties.get(ReplicaAssignor.BINDING_CONFIG);
  }

  @Test
  void subscribesToNoTopicAndIsPolled() throws Exception {
    start();
    eventually("a consumer is polled", () -> !consumers.isEmpty() && consumers.get(0).polled);
    Pattern pattern = consumers.get(0).pattern;
    assertThat(pattern).isNotNull();
    // Whatever a topic is called, the subscription does not name it.
    for (String topic : List.of("", "a", "orders.created", "__consumer_offsets", "(?!)")) {
      assertThat(pattern.matcher(topic).matches()).as(topic).isFalse();
    }
    assertThat(membership.awaitJoined(Duration.ofMillis(50))).as("joined with no list").isFalse();
    assertThat(membership.current()).isEqualTo(ReplicaList.getDefaultInstance());
  }

  @Test
  void theAssignorSpeaksForThisReplicaAndItsListsReachTheListener() throws Exception {
    ReplicaAssignor.Binding binding = binding(start());
    assertThat(binding.address()).isEqualTo("10.0.0.1:9465");
    assertThat(binding.generation()).isZero();

    binding.replicas(list(3, "10.0.0.1:9465", "10.0.0.2:9465"));
    assertThat(membership.awaitJoined(Duration.ofSeconds(5))).isTrue();
    assertThat(told).containsExactly(list(3, "10.0.0.1:9465", "10.0.0.2:9465"));
    assertThat(membership.current()).isEqualTo(told.get(0));
    // What it joins with the next time.
    assertThat(binding.generation()).isEqualTo(3);

    binding.replicas(list(4, "10.0.0.1:9465"));
    assertThat(told).hasSize(2);
    assertThat(binding.generation()).isEqualTo(4);
  }

  @Test
  void aListThatIsNotAboveWhatWasSeenIsTakenAndTheGenerationKept() {
    ReplicaAssignor.Binding binding = binding(start());
    binding.replicas(list(9, "10.0.0.1:9465", "10.0.0.2:9465"));
    // It should not happen: the assignor makes every list one above what any member knew. If
    // it does, the addresses are still the newest word on who the replicas are; what this
    // replica joins with stays the highest it has seen, so that the next list is above both.
    binding.replicas(list(2, "10.0.0.1:9465"));
    assertThat(told)
        .containsExactly(list(9, "10.0.0.1:9465", "10.0.0.2:9465"), list(2, "10.0.0.1:9465"));
    assertThat(membership.current()).isEqualTo(list(2, "10.0.0.1:9465"));
    assertThat(binding.generation()).isEqualTo(9);
  }

  @Test
  void aConsumerThatFailsIsReplacedAndTheListHeldIsKept() throws Exception {
    ReplicaAssignor.Binding binding = binding(start());
    eventually("a consumer is polled", () -> !consumers.isEmpty() && consumers.get(0).polled);
    binding.replicas(list(6, "10.0.0.1:9465", "10.0.0.2:9465"));

    // The group refuses this member, or the brokers say something no retry inside the client
    // gets past.
    consumers.get(0).setPollException(new KafkaException("not authorized to access group"));
    eventually("the consumer is replaced", () -> consumers.size() == 2 && consumers.get(1).polled);
    assertThat(consumers.get(0).closed()).as("the failed consumer is closed").isTrue();
    assertThat(consumers.get(1).pattern).isNotNull();

    // Nobody was told anything: the replica serves under the list it had, and the new consumer
    // joins with that list's generation.
    assertThat(told).hasSize(1);
    assertThat(membership.current()).isEqualTo(list(6, "10.0.0.1:9465", "10.0.0.2:9465"));
    assertThat(binding.generation()).isEqualTo(6);
  }

  @Test
  void closingLeavesTheGroupAtOnce() throws Exception {
    start();
    eventually("a consumer is polled", () -> !consumers.isEmpty() && consumers.get(0).polled);
    long before = System.nanoTime();
    membership.close();
    assertThat(Duration.ofNanos(System.nanoTime() - before)).isLessThan(Duration.ofSeconds(5));
    // Closing the consumer is what leaves the group.
    assertThat(consumers.get(0).closed()).isTrue();
    assertThat(consumers).hasSize(1);
  }

  @Test
  void theConsumerIsTheClustersWithWhatMembershipRestsOnSetLast() {
    Cluster cluster =
        Cluster.newBuilder()
            .putProperties("bootstrap.servers", "kafka:9092")
            .putProperties("client.rack", "a")
            // What the cluster's other consumers may use, and this one cannot.
            .putConsumerProperties("group.protocol", "consumer")
            .putConsumerProperties("group.remote.assignor", "uniform")
            .putConsumerProperties("group.instance.id", "shared-by-every-replica")
            .putConsumerProperties("partition.assignment.strategy", "range")
            .putConsumerProperties("allow.auto.create.topics", "true")
            .putConsumerProperties("session.timeout.ms", "6000")
            .build();
    Map<String, Object> properties = Membership.properties(cluster, "replicas", "client");
    assertThat(properties)
        .containsEntry("bootstrap.servers", "kafka:9092")
        .containsEntry("client.rack", "a")
        .containsEntry("session.timeout.ms", "6000")
        .containsEntry(ConsumerConfig.GROUP_ID_CONFIG, "replicas")
        .containsEntry(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "classic")
        .containsEntry(
            ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, ReplicaAssignor.class.getName())
        .containsEntry(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false")
        .containsEntry(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
        .containsEntry(ConsumerConfig.CLIENT_ID_CONFIG, "client")
        .doesNotContainKey(ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG)
        .doesNotContainKey(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG);
  }

  @Test
  void theGroupLivesOnTheClusterNamedOrOnTheOnlyOne() {
    ServerConfig.Builder one =
        ServerConfig.newBuilder()
            .putClusters("main", Cluster.getDefaultInstance())
            .setDispatch(
                Dispatch.newBuilder()
                    .setCoordination(Coordination.newBuilder().setGroupId("replicas")));
    assertThat(Membership.cluster(one.build())).isEqualTo("main");

    ServerConfig.Builder two = one.clone().putClusters("other", Cluster.getDefaultInstance());
    assertThatThrownBy(() -> Membership.cluster(two.build()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("dispatch.coordination.cluster");
    two.getDispatchBuilder().getCoordinationBuilder().setCluster("other");
    assertThat(Membership.cluster(two.build())).isEqualTo("other");
  }
}
