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
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.epoll.EpollIoHandler;
import io.netty.channel.unix.DomainSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

/**
 * A server assembled the way {@code Main} assembles it, listening on a Unix socket in a temporary
 * directory, with a gRPC channel to it. The configuration is YAML with two placeholders, in this
 * order: the socket directory, then whatever the test passes.
 */
final class ServerHarness {
  static final String KAFKA_A = bootstrap("ODRADEK_E2E_KAFKA_A");
  static final String KAFKA_B = bootstrap("ODRADEK_E2E_KAFKA_B");

  private final Path dir;
  private final CodecRegistry codecs;
  private final ClusterProducers producers;
  private final ConsumeService consume;
  private final OdradekServer server;
  private final EventLoopGroup group;
  final ManagedChannel channel;

  private ServerHarness(String yamlTemplate, Object... arguments) throws Exception {
    dir = Files.createTempDirectory(Path.of("/tmp"), "odradek-e2e");
    Object[] all = new Object[arguments.length + 1];
    all[0] = dir;
    System.arraycopy(arguments, 0, all, 1, arguments.length);
    ServerConfig config = ConfigLoader.parse(yamlTemplate.formatted(all));
    codecs = CodecRegistry.create(config, ContractRegistry.empty(), getClass().getClassLoader());
    producers = ClusterProducers.create(config);
    ProduceService produce =
        new ProduceService(config, ProduceRoutes.create(config, producers, codecs));
    consume = new ConsumeService(config, codecs, produce.serverEpoch());
    server = OdradekServer.start(config, new OdradekService(produce, consume));
    group = new MultiThreadIoEventLoopGroup(1, EpollIoHandler.newFactory());
    channel =
        NettyChannelBuilder.forAddress(new DomainSocketAddress(server.socket().toString()))
            .eventLoopGroup(group)
            .channelType(EpollDomainSocketChannel.class)
            .usePlaintext()
            .build();
  }

  static ServerHarness start(String yamlTemplate, Object... arguments) throws Exception {
    return new ServerHarness(yamlTemplate, arguments);
  }

  /** Stops the server the way its shutdown hook does. */
  void stop() throws Exception {
    server.stopAccepting();
    producers.flush();
    consume.shutdown(Duration.ofSeconds(15));
    channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    group.shutdownGracefully();
    server.close();
    producers.close(Duration.ofSeconds(5));
    codecs.close();
    Files.deleteIfExists(dir);
  }

  private static String bootstrap(String variable) {
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

  static void createTopic(String bootstrap, String name, int partitions) throws Exception {
    try (Admin admin = admin(bootstrap)) {
      admin.createTopics(List.of(new NewTopic(name, partitions, (short) 1))).all().get();
    }
  }

  static void deleteTopics(String bootstrap, List<String> names) throws Exception {
    try (Admin admin = admin(bootstrap)) {
      admin.deleteTopics(names).all().get();
    }
  }
}
