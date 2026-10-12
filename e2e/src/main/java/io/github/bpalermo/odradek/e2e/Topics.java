package io.github.bpalermo.odradek.e2e;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;

/**
 * Creates and deletes topics from the command line, for end-to-end suites not written in Java: they
 * run this instead of carrying a Kafka client of their own.
 *
 * <pre>
 * topics create BOOTSTRAP PARTITIONS TOPIC...
 * topics delete BOOTSTRAP TOPIC...
 * </pre>
 */
public final class Topics {
  private Topics() {}

  public static void main(String[] args) throws Exception {
    if (args.length >= 4 && args[0].equals("create")) {
      int partitions = Integer.parseInt(args[2]);
      List<NewTopic> topics =
          Arrays.stream(args, 3, args.length)
              .map(name -> new NewTopic(name, partitions, (short) 1))
              .toList();
      try (Admin admin = admin(args[1])) {
        admin.createTopics(topics).all().get();
        awaitLeaders(admin, topics, partitions);
      }
    } else if (args.length >= 3 && args[0].equals("delete")) {
      try (Admin admin = admin(args[1])) {
        admin.deleteTopics(Arrays.asList(args).subList(2, args.length)).all().get();
      }
    } else {
      System.err.println(
          "usage: topics create BOOTSTRAP PARTITIONS TOPIC... | topics delete BOOTSTRAP TOPIC...");
      System.exit(2);
    }
  }

  /**
   * Returns once every partition's leader answers. Producing in the gap between a topic's creation
   * and its broker taking up leadership can wedge Kafka's idempotent producer until its delivery
   * timeout; see ServerHarness.createTopic.
   */
  private static void awaitLeaders(Admin admin, List<NewTopic> topics, int partitions)
      throws Exception {
    Map<TopicPartition, OffsetSpec> ends = new HashMap<>();
    for (NewTopic topic : topics) {
      for (int partition = 0; partition < partitions; partition++) {
        ends.put(new TopicPartition(topic.name(), partition), OffsetSpec.latest());
      }
    }
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
    while (true) {
      try {
        admin.listOffsets(ends).all().get(30, TimeUnit.SECONDS);
        return;
      } catch (ExecutionException e) {
        if (!(e.getCause() instanceof RetriableException) || System.nanoTime() > deadline) {
          throw e;
        }
        Thread.sleep(50);
      }
    }
  }

  private static Admin admin(String bootstrap) {
    return Admin.create(
        Map.<String, Object>of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap));
  }
}
