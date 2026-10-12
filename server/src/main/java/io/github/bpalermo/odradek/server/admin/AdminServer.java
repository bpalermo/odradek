package io.github.bpalermo.odradek.server.admin;

import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.epoll.EpollIoHandler;
import io.netty.channel.epoll.EpollServerSocketChannel;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The TCP port kubelet probes, serving the gRPC health service and nothing else. It exists because
 * a kubelet gRPC probe cannot target the Unix socket the app uses.
 */
public final class AdminServer implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(AdminServer.class);

  /** The port when the configuration does not name one. */
  public static final int DEFAULT_PORT = 9464;

  private final Server server;
  private final EventLoopGroup group;

  private AdminServer(Server server, EventLoopGroup group) {
    this.server = server;
    this.group = group;
  }

  /**
   * Starts serving on {@code port}; 0 picks a free one.
   *
   * @throws IOException if the port cannot be bound
   */
  public static AdminServer start(int port, HealthService health) throws IOException {
    EventLoopGroup group =
        new MultiThreadIoEventLoopGroup(
            1,
            runnable -> {
              Thread thread = new Thread(runnable, "admin");
              thread.setDaemon(true);
              return thread;
            },
            EpollIoHandler.newFactory());
    Server server =
        NettyServerBuilder.forPort(port)
            .channelType(EpollServerSocketChannel.class)
            .bossEventLoopGroup(group)
            .workerEventLoopGroup(group)
            .addService(health)
            .build();
    try {
      server.start();
    } catch (IOException | RuntimeException e) {
      group.shutdownGracefully(0, 1, TimeUnit.SECONDS);
      throw e;
    }
    AdminServer admin = new AdminServer(server, group);
    LOG.info("health service on port {}", admin.port());
    return admin;
  }

  /** The port being served, which is the one asked for unless that was 0. */
  public int port() {
    return server.getPort();
  }

  @Override
  public void close() {
    server.shutdownNow();
    group.shutdownGracefully(0, 1, TimeUnit.SECONDS);
  }
}
