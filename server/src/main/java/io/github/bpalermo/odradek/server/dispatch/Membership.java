package io.github.bpalermo.odradek.server.dispatch;

import io.github.bpalermo.odradek.config.v1.Cluster;
import io.github.bpalermo.odradek.config.v1.Coordination;
import io.github.bpalermo.odradek.config.v1.ServerConfig;
import io.github.bpalermo.odradek.delivery.v1.ReplicaList;
import io.github.bpalermo.odradek.server.telemetry.ServerMetrics;
import io.github.bpalermo.odradek.server.tls.TlsSources;
import java.io.Closeable;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A dispatcher replica's membership of the coordination group (ADR 6): the one Kafka consumer group
 * every replica joins when it starts, whatever sidecars have declared, so that the group's members
 * are the list of replicas.
 *
 * <p>The consumer consumes nothing. It subscribes to a pattern that matches no topic, which is
 * enough to be a member: no topic has to exist, none is created, and the replica needs no right on
 * any topic for it. Its assignor, {@link ReplicaAssignor}, carries this replica's address to the
 * group and brings back the list of all, which is handed to the listener each time the group
 * settles: when a replica started, stopped, or was given up on by the brokers.
 *
 * <p>A member has to be polled to stay one, so the consumer has a thread of its own that does
 * nothing else. Whatever happens to it, the list last received is kept: a replica that cannot reach
 * the group goes on serving the streams it has under the list it knew. A consumer that fails is
 * replaced by a new one after a pause, for as long as it takes; the brokers' session timeout is
 * what removes a replica from the others' list.
 */
public final class Membership implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(Membership.class);

  /** Matches no topic name: an empty negative lookahead fails at every position. */
  static final Pattern NO_TOPIC = Pattern.compile("(?!)");

  /**
   * How long the group waits for every member to join again when one joins or leaves. The thread
   * does nothing but poll, so a member that takes longer than this is not coming.
   */
  private static final int REJOIN_TIMEOUT_MS = 30_000;

  private static final Duration POLL = Duration.ofMillis(500);
  private static final long FIRST_PAUSE_MS = 1_000;
  private static final long LONGEST_PAUSE_MS = 30_000;

  /** Told each list the group settles on, on the membership's thread. */
  @FunctionalInterface
  public interface Listener {
    void replicas(ReplicaList list);
  }

  private final String cluster;
  private final String group;
  private final String address;
  private final Map<String, Object> properties;
  private final Function<Map<String, Object>, Consumer<byte[], byte[]>> factory;
  private final Listener listener;
  private final Thread thread;
  private final CountDownLatch joined = new CountDownLatch(1);
  private final Object lock = new Object();

  /** The consumer in use, for {@link #close} to interrupt; guarded by {@link #lock}. */
  private Consumer<byte[], byte[]> consumer;

  private boolean closed;

  /** The newest list. */
  private volatile ReplicaList current = ReplicaList.getDefaultInstance();

  /** The highest generation of any list received: what this replica joins with. */
  private volatile long highest;

  private Membership(
      String cluster,
      String group,
      String address,
      Map<String, Object> properties,
      Function<Map<String, Object>, Consumer<byte[], byte[]>> factory,
      Listener listener) {
    this.cluster = cluster;
    this.group = group;
    this.address = address;
    this.properties = properties;
    this.factory = factory;
    this.listener = listener;
    properties.put(
        ReplicaAssignor.BINDING_CONFIG,
        new ReplicaAssignor.Binding() {
          @Override
          public String address() {
            return address;
          }

          @Override
          public long generation() {
            return highest;
          }

          @Override
          public void replicas(ReplicaList list) {
            settled(list);
          }
        });
    this.thread = new Thread(this::run, "odradek-membership");
    this.thread.setDaemon(true);
  }

  /**
   * Joins the coordination group of {@code dispatch.coordination} as the replica sidecars reach at
   * {@code address}, and keeps it a member until {@link #close}. Returns at once: {@link
   * #awaitJoined} says when the first list has arrived.
   */
  public static Membership start(
      ServerConfig config,
      TlsSources tls,
      ServerMetrics metrics,
      String address,
      Listener listener) {
    String clusterName = cluster(config);
    String group = config.getDispatch().getCoordination().getGroupId();
    Map<String, Object> properties =
        properties(config.getClustersOrThrow(clusterName), group, "odradek-membership-" + group);
    tls.apply(clusterName, properties);
    metrics.kafkaClients().applyToConsumer(clusterName, group, Map.of(), properties);
    Membership membership =
        start(
            clusterName,
            group,
            address,
            properties,
            configured ->
                new KafkaConsumer<>(
                    configured, new ByteArrayDeserializer(), new ByteArrayDeserializer()),
            listener);
    metrics.observeReplicas(clusterName, group, membership::current);
    return membership;
  }

  /** As above, with the consumer's properties and what makes a consumer of them; for tests. */
  static Membership start(
      String cluster,
      String group,
      String address,
      Map<String, Object> properties,
      Function<Map<String, Object>, Consumer<byte[], byte[]>> factory,
      Listener listener) {
    Membership membership = new Membership(cluster, group, address, properties, factory, listener);
    membership.thread.start();
    return membership;
  }

  /** The cluster the coordination group lives on: the one named, or the only one there is. */
  public static String cluster(ServerConfig config) {
    Coordination coordination = config.getDispatch().getCoordination();
    if (coordination.hasCluster() && !coordination.getCluster().isEmpty()) {
      return coordination.getCluster();
    }
    if (config.getClustersCount() != 1) {
      // The configuration's own validation says the same.
      throw new IllegalArgumentException(
          "dispatch.coordination.cluster must be set when there are several clusters");
    }
    return config.getClustersMap().keySet().iterator().next();
  }

  /**
   * The configuration of the coordination group's consumer: the cluster's, with what membership
   * rests on set last. The classic group protocol, because the addresses travel through a
   * client-side assignor, which the newer protocol does not have; a dynamic member, because a
   * static identity would be shared by every replica that reads the same configuration.
   */
  static Map<String, Object> properties(Cluster cluster, String group, String clientId) {
    Map<String, Object> properties = new HashMap<>();
    properties.putAll(cluster.getPropertiesMap());
    properties.putAll(cluster.getConsumerPropertiesMap());
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
    properties.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "classic");
    properties.remove(ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG);
    properties.remove(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG);
    properties.put(
        ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, ReplicaAssignor.class.getName());
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    // Nothing is subscribed to by name; this only says that the group creates no topic, ever.
    properties.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
    properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(REJOIN_TIMEOUT_MS));
    properties.putIfAbsent(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
    return properties;
  }

  /** The cluster the group lives on, by its name in the configuration. */
  public String cluster() {
    return cluster;
  }

  /** The coordination group. */
  public String group() {
    return group;
  }

  /** The newest list the group settled on; empty, under generation 0, before the first. */
  public ReplicaList current() {
    return current;
  }

  /**
   * Waits for the first list, which is when this replica is a member and knows the others.
   *
   * @return whether it arrived within {@code timeout}
   */
  public boolean awaitJoined(Duration timeout) throws InterruptedException {
    return joined.await(timeout.toNanos(), TimeUnit.NANOSECONDS);
  }

  private void settled(ReplicaList list) {
    if (Long.compareUnsigned(list.getGeneration(), highest) <= 0) {
      // The assignor makes every list one above what any member knew, this one included. What
      // this replica joins with stays the highest it has seen, so the next list is above it.
      LOG.error(
          "coordination group {} settled on generation {}, which is not above the {} this replica"
              + " has seen: sidecars that act on a newer list ignore this one",
          group,
          Long.toUnsignedString(list.getGeneration()),
          Long.toUnsignedString(highest));
    } else {
      highest = list.getGeneration();
    }
    if (!ReplicaAssignor.addresses(list).contains(address)) {
      LOG.error(
          "coordination group {} settled on a list without this replica ({}): {}",
          group,
          address,
          ReplicaAssignor.addresses(list));
    }
    current = list;
    listener.replicas(list);
    joined.countDown();
  }

  private void run() {
    long pause = FIRST_PAUSE_MS;
    while (true) {
      Consumer<byte[], byte[]> mine = null;
      try {
        synchronized (lock) {
          if (closed) {
            return;
          }
        }
        mine = factory.apply(properties);
        synchronized (lock) {
          consumer = mine;
          if (closed) {
            return;
          }
        }
        mine.subscribe(NO_TOPIC);
        while (true) {
          long before = highest;
          mine.poll(POLL);
          if (highest != before) {
            // A member again: the next failure starts from the shortest pause.
            pause = FIRST_PAUSE_MS;
          }
        }
      } catch (WakeupException e) {
        // close() asked.
        return;
      } catch (RuntimeException e) {
        LOG.error(
            "this replica's membership of coordination group {} on cluster {} failed; the list of"
                + " replicas held is kept, and the group is joined again in {} ms",
            group,
            cluster,
            pause,
            e);
      } finally {
        synchronized (lock) {
          consumer = null;
        }
        if (mine != null) {
          try {
            // Leaves the group, so that the others drop this replica now and not at the timeout.
            mine.close(CloseOptions.timeout(Duration.ofSeconds(5)));
          } catch (RuntimeException e) {
            LOG.warn("closing the consumer of coordination group {} failed", group, e);
          }
        }
      }
      synchronized (lock) {
        long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(pause);
        try {
          for (long left = pause; !closed && left > 0; ) {
            lock.wait(left);
            left = TimeUnit.NANOSECONDS.toMillis(until - System.nanoTime());
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
        if (closed) {
          return;
        }
      }
      pause = Math.min(pause * 2, LONGEST_PAUSE_MS);
    }
  }

  /**
   * Leaves the group, which tells the other replicas, and through them every sidecar, that this one
   * is gone. Called last when a dispatcher stops: while it still hands partitions over, its
   * sidecars must keep their streams to it, and a sidecar closes its streams to a replica that is
   * no longer listed.
   */
  @Override
  public void close() {
    synchronized (lock) {
      closed = true;
      if (consumer != null) {
        consumer.wakeup();
      }
      lock.notifyAll();
    }
    try {
      thread.join(TimeUnit.SECONDS.toMillis(10));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
