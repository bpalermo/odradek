package io.github.bpalermo.odradek.e2e;

import static org.assertj.core.api.Assertions.fail;

import io.github.bpalermo.odradek.config.v1.ServerConfig;
import io.github.bpalermo.odradek.server.OdradekServer;
import io.github.bpalermo.odradek.server.OdradekService;
import io.github.bpalermo.odradek.server.codec.CodecRegistry;
import io.github.bpalermo.odradek.server.codec.ContractRegistry;
import io.github.bpalermo.odradek.server.config.ConfigLoader;
import io.github.bpalermo.odradek.server.consume.ConsumeService;
import io.github.bpalermo.odradek.server.produce.ClusterProducers;
import io.github.bpalermo.odradek.server.produce.ProduceRoutes;
import io.github.bpalermo.odradek.server.produce.ProduceService;
import io.github.bpalermo.odradek.server.receive.ReceiveService;
import io.github.bpalermo.odradek.server.telemetry.ServerMetrics;
import io.github.bpalermo.odradek.server.tls.TlsSources;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.epoll.EpollIoHandler;
import io.netty.channel.unix.DomainSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * A running server and a gRPC channel to its Unix socket, in a temporary directory.
 *
 * <p>By default the server is assembled in this JVM the way {@code Main} assembles it. When {@code
 * ODRADEK_E2E_SERVER} names an executable, that executable is started instead with {@code
 * --config}: this is how the suite runs against the packaged server, JVM or native.
 *
 * <p>The configuration is YAML with placeholders: first the socket directory, then whatever the
 * test passes.
 */
final class ServerHarness {
  static final String KAFKA_A = bootstrap("ODRADEK_E2E_KAFKA_A");
  static final String KAFKA_B = bootstrap("ODRADEK_E2E_KAFKA_B");

  private static final Duration START_TIMEOUT = Duration.ofSeconds(60);

  private final Path dir;
  private final String yaml;
  private final ServerConfig config;
  private final EventLoopGroup group;
  final ManagedChannel channel;

  // In-process server.
  private CodecRegistry codecs;
  private TlsSources tls;
  private ClusterProducers producers;
  private ReceiveService receive;
  private ConsumeService consume;
  private OdradekServer server;

  // External server.
  private Process process;

  private boolean stopped;

  private ServerHarness(String yamlTemplate, Object... arguments) throws Exception {
    dir = Files.createTempDirectory(Path.of("/tmp"), "odradek-e2e");
    Object[] all = new Object[arguments.length + 1];
    all[0] = dir;
    System.arraycopy(arguments, 0, all, 1, arguments.length);
    // A free admin port: several suites start a server on one machine at the same time.
    yaml = "admin: {port: 0}\n" + yamlTemplate.formatted(all);
    config = ConfigLoader.parse(yaml);
    startServer();
    group = new MultiThreadIoEventLoopGroup(1, EpollIoHandler.newFactory());
    channel =
        NettyChannelBuilder.forAddress(new DomainSocketAddress(socket().toString()))
            .eventLoopGroup(group)
            .channelType(EpollDomainSocketChannel.class)
            .usePlaintext()
            .build();
  }

  static ServerHarness start(String yamlTemplate, Object... arguments) throws Exception {
    return new ServerHarness(yamlTemplate, arguments);
  }

  /** The Unix socket the server listens on. */
  Path socket() {
    return Path.of(config.getListener().getSocketPath());
  }

  private void startServer() throws Exception {
    String executable = System.getenv("ODRADEK_E2E_SERVER");
    if (executable == null || executable.isBlank()) {
      codecs = CodecRegistry.create(config, ContractRegistry.empty(), getClass().getClassLoader());
      tls = TlsSources.create(config);
      producers = ClusterProducers.create(config, tls);
      ProduceService produce =
          new ProduceService(
              config, ProduceRoutes.create(config, producers, codecs), ServerMetrics.noop());
      receive = ReceiveService.create(config, tls, ServerMetrics.noop());
      consume =
          new ConsumeService(
              config, codecs, tls, produce.serverEpoch(), ServerMetrics.noop(), receive);
      server = OdradekServer.start(config, new OdradekService(produce, consume));
      return;
    }
    Path file = dir.resolve("odradek.yaml");
    Files.writeString(file, yaml);
    process =
        new ProcessBuilder(
                Path.of(executable).toAbsolutePath().toString(), "--config", file.toString())
            .inheritIO()
            .start();
    long deadline = System.nanoTime() + START_TIMEOUT.toNanos();
    while (!Files.exists(socket())) {
      if (!process.isAlive()) {
        fail("%s exited with status %s before listening", executable, process.exitValue());
      }
      if (System.nanoTime() > deadline) {
        process.destroyForcibly();
        fail("%s did not listen on %s within %s", executable, socket(), START_TIMEOUT);
      }
      Thread.sleep(50);
    }
  }

  /** Stops the server: in process the way its shutdown hook does, otherwise with SIGTERM. */
  private void stopServer() throws Exception {
    if (process != null) {
      process.destroy();
      if (!process.waitFor(60, TimeUnit.SECONDS)) {
        process.destroyForcibly();
        fail("the server did not stop within 60s of SIGTERM");
      }
      process = null;
    } else {
      server.stopAccepting();
      producers.flush();
      consume.shutdown(Duration.ofSeconds(15));
      receive.close();
      server.close();
      producers.close(Duration.ofSeconds(5));
      tls.close();
      codecs.close();
    }
  }

  /**
   * The pod is gone: the process is killed where it stands, with whatever it held. A server
   * assembled in this JVM cannot be killed, and is stopped in good order instead: seen from a
   * dispatcher both are a sidecar whose streams ended.
   */
  void kill() throws Exception {
    if (process != null) {
      process.destroyForcibly();
      process.waitFor(60, TimeUnit.SECONDS);
      process = null;
      stopped = true;
    } else {
      stopServer();
      stopped = true;
    }
  }

  /**
   * Stops the server and starts a new one with the same configuration and socket: a new server
   * epoch, with no memory of produce sessions.
   */
  void restart() throws Exception {
    stopServer();
    startServer();
  }

  void stop() throws Exception {
    if (!stopped) {
      stopServer();
    }
    channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    group.shutdownGracefully();
    try (var files = Files.walk(dir)) {
      for (Path file : files.sorted(java.util.Comparator.reverseOrder()).toList()) {
        Files.deleteIfExists(file);
      }
    }
  }

  /** A required environment variable of the suite. */
  static String bootstrap(String variable) {
    String value = System.getenv(variable);
    if (value == null || value.isBlank()) {
      return fail("%s is not set; see e2e/BUILD.bazel", variable);
    }
    return value.trim();
  }

  private static Admin admin(String bootstrap) {
    return Admin.create(
        Map.<String, Object>of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap));
  }

  /**
   * Creates a topic and waits until every partition's leader answers.
   *
   * <p>The wait matters. A topic exists in the metadata before its broker has taken up leadership.
   * An idempotent producer that writes in that gap can have its first batch refused and a later one
   * accepted; the retried first batch is then out of sequence for good, and the record fails only
   * when delivery.timeout.ms expires. That is Kafka's behaviour for any producer, and nothing an
   * app would normally meet, but a test that produces a few milliseconds after creating its topic
   * does.
   */
  static void createTopic(String bootstrap, String name, int partitions) throws Exception {
    createTopic(bootstrap, name, partitions, Map.of());
  }

  /** As above, for a topic with settings of its own. */
  static void createTopic(
      String bootstrap, String name, int partitions, Map<String, String> settings)
      throws Exception {
    try (Admin admin = admin(bootstrap)) {
      admin
          .createTopics(List.of(new NewTopic(name, partitions, (short) 1).configs(settings)))
          .all()
          .get();
      Map<TopicPartition, OffsetSpec> ends = new HashMap<>();
      for (int partition = 0; partition < partitions; partition++) {
        ends.put(new TopicPartition(name, partition), OffsetSpec.latest());
      }
      // Answered by each partition's leader. Until the topic has reached the broker's own
      // metadata, and the broker has taken up leadership, the request fails with a retriable
      // error.
      long deadline = System.nanoTime() + START_TIMEOUT.toNanos();
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
  }

  /**
   * Everything in a topic, from its beginning, in the order read: waits until it holds {@code
   * atLeast} records, then a moment longer for any that should not be there.
   */
  static List<ConsumerRecord<String, String>> read(
      String bootstrap, String name, int partitions, int atLeast) {
    List<ConsumerRecord<String, String>> all = new ArrayList<>();
    try (KafkaConsumer<String, String> consumer =
        new KafkaConsumer<>(
            Map.<String, Object>of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap),
            new StringDeserializer(),
            new StringDeserializer())) {
      List<TopicPartition> assignment = new ArrayList<>();
      for (int partition = 0; partition < partitions; partition++) {
        assignment.add(new TopicPartition(name, partition));
      }
      consumer.assign(assignment);
      consumer.seekToBeginning(assignment);
      long deadline = System.nanoTime() + START_TIMEOUT.toNanos();
      while (all.size() < atLeast) {
        if (System.nanoTime() > deadline) {
          fail("%s holds %s records after %s, not %s", name, all.size(), START_TIMEOUT, atLeast);
        }
        consumer.poll(Duration.ofMillis(200)).forEach(all::add);
      }
      consumer.poll(Duration.ofMillis(500)).forEach(all::add);
    }
    return all;
  }

  /** The headers of a record, as "name=value", in order. */
  static List<String> headers(ConsumerRecord<String, String> record) {
    List<String> headers = new ArrayList<>();
    for (Header header : record.headers()) {
      headers.add(
          header.key()
              + "="
              + (header.value() == null
                  ? null
                  : new String(header.value(), StandardCharsets.UTF_8)));
    }
    return headers;
  }

  /** The last header of a name, as text; null when the record has none. */
  static String header(ConsumerRecord<String, String> record, String name) {
    Header header = record.headers().lastHeader(name);
    return header == null || header.value() == null
        ? null
        : new String(header.value(), StandardCharsets.UTF_8);
  }

  static void deleteTopics(String bootstrap, List<String> names) throws Exception {
    try (Admin admin = admin(bootstrap)) {
      admin.deleteTopics(names).all().get();
    }
  }
}
