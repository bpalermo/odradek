package io.github.bpalermo.odradek.server;

import io.github.bpalermo.odradek.config.v1.ServerConfig;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollIoHandler;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The app-facing gRPC server, listening on a Unix domain socket through Netty's epoll transport.
 */
public final class OdradekServer implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(OdradekServer.class);

  /** Where the socket is bound when the configuration does not say. */
  public static final String DEFAULT_SOCKET_PATH = "/var/run/kafka/kafka.sock";

  private final Path socket;
  private final Server server;
  private final EventLoopGroup acceptors;
  private final EventLoopGroup workers;

  private OdradekServer(
      Path socket, Server server, EventLoopGroup acceptors, EventLoopGroup workers) {
    this.socket = socket;
    this.server = server;
    this.acceptors = acceptors;
    this.workers = workers;
  }

  /**
   * Binds the socket and starts serving.
   *
   * <p>A socket file left by a previous run is replaced. Anything else at that path is left alone
   * and the bind fails. The socket's directory must exist: it is the volume shared with the app.
   * Once bound, the socket is made readable and writable by its owner and group only.
   *
   * @throws IOException if epoll is unavailable or the socket cannot be bound
   */
  public static OdradekServer start(ServerConfig config, BindableService... services)
      throws IOException {
    if (!Epoll.isAvailable()) {
      throw new IOException(
          "the epoll transport is unavailable; Unix domain sockets need it",
          Epoll.unavailabilityCause());
    }
    Path socket =
        Path.of(
            config.getListener().hasSocketPath()
                ? config.getListener().getSocketPath()
                : DEFAULT_SOCKET_PATH);
    if (Files.exists(socket, LinkOption.NOFOLLOW_LINKS)) {
      if (!Files.readAttributes(socket, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS)
          .isOther()) {
        throw new IOException(socket + " exists and is not a socket");
      }
      Files.delete(socket);
    }

    EventLoopGroup acceptors = new MultiThreadIoEventLoopGroup(1, EpollIoHandler.newFactory());
    EventLoopGroup workers = new MultiThreadIoEventLoopGroup(EpollIoHandler.newFactory());
    NettyServerBuilder builder =
        NettyServerBuilder.forAddress(new DomainSocketAddress(socket.toString()))
            .channelType(EpollServerDomainSocketChannel.class)
            .bossEventLoopGroup(acceptors)
            .workerEventLoopGroup(workers);
    for (BindableService service : services) {
      builder.addService(service);
    }
    Server server = builder.build();
    try {
      server.start();
      Files.setPosixFilePermissions(socket, PosixFilePermissions.fromString("rw-rw----"));
    } catch (IOException | RuntimeException e) {
      server.shutdownNow();
      acceptors.shutdownGracefully();
      workers.shutdownGracefully();
      throw e;
    }
    LOG.info("listening on {}", socket);
    return new OdradekServer(socket, server, acceptors, workers);
  }

  /** The socket this server is bound to. */
  public Path socket() {
    return socket;
  }

  /** Blocks until the server has stopped. */
  public void awaitTermination() throws InterruptedException {
    server.awaitTermination();
  }

  /** Stops accepting new streams. Open ones continue. */
  public void stopAccepting() {
    server.shutdown();
  }

  /**
   * Stops accepting streams, gives open ones a few seconds to finish, then cancels what is left and
   * removes the socket file.
   */
  @Override
  public void close() {
    server.shutdown();
    try {
      if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
        server.shutdownNow();
        server.awaitTermination(5, TimeUnit.SECONDS);
      }
    } catch (InterruptedException e) {
      server.shutdownNow();
      Thread.currentThread().interrupt();
    }
    acceptors.shutdownGracefully();
    workers.shutdownGracefully();
    try {
      Files.deleteIfExists(socket);
    } catch (IOException e) {
      LOG.warn("could not remove {}", socket, e);
    }
    LOG.info("stopped");
  }
}
