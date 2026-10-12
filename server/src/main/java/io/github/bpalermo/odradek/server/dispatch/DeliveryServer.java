package io.github.bpalermo.odradek.server.dispatch;

import io.github.bpalermo.odradek.config.v1.Dispatch;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.epoll.EpollIoHandler;
import io.netty.channel.epoll.EpollServerSocketChannel;
import java.io.Closeable;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The TCP port sidecars connect to, serving {@code DeliveryService} and nothing else.
 *
 * <p><b>Plaintext, and open to whoever reaches it</b>: mutual TLS and the policy come with step 5
 * of ADR 6's plan. Until then it is for tests.
 */
public final class DeliveryServer implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(DeliveryServer.class);

  private final Server server;
  private final EventLoopGroup acceptors;
  private final EventLoopGroup workers;
  private final String advertised;

  private DeliveryServer(
      Server server, EventLoopGroup acceptors, EventLoopGroup workers, String advertised) {
    this.server = server;
    this.acceptors = acceptors;
    this.workers = workers;
    this.advertised = advertised;
  }

  /**
   * Binds {@code dispatch.port} (0 picks a free one), starts serving and tells the service the
   * address sidecars reach this replica at.
   *
   * @throws IOException if the port cannot be bound
   */
  public static DeliveryServer start(Dispatch dispatch, DispatchService service)
      throws IOException {
    EventLoopGroup acceptors = new MultiThreadIoEventLoopGroup(1, EpollIoHandler.newFactory());
    EventLoopGroup workers = new MultiThreadIoEventLoopGroup(EpollIoHandler.newFactory());
    Server server =
        NettyServerBuilder.forPort(
                dispatch.hasPort() ? dispatch.getPort() : DispatchService.DEFAULT_PORT)
            .channelType(EpollServerSocketChannel.class)
            .bossEventLoopGroup(acceptors)
            .workerEventLoopGroup(workers)
            // A sidecar that vanished without closing its connection is noticed here, and what it
            // held redelivered, instead of at each record's processing deadline.
            .keepAliveTime(30, TimeUnit.SECONDS)
            .keepAliveTimeout(10, TimeUnit.SECONDS)
            // Sidecars hold streams that are silent while a topic is: let them probe too.
            .permitKeepAliveTime(10, TimeUnit.SECONDS)
            .permitKeepAliveWithoutCalls(true)
            .addService(service)
            .build();
    try {
      server.start();
    } catch (IOException | RuntimeException e) {
      acceptors.shutdownGracefully(0, 1, TimeUnit.SECONDS);
      workers.shutdownGracefully(0, 1, TimeUnit.SECONDS);
      throw e;
    }
    String advertised =
        dispatch.hasAdvertisedAddress()
            ? dispatch.getAdvertisedAddress()
            : hostAndPort(interfaceAddress(), server.getPort());
    service.advertise(advertised);
    LOG.info("delivery service on port {}, advertised as {}", server.getPort(), advertised);
    LOG.warn(
        "THE DISPATCH POLICY IS NOT ENFORCED YET: the delivery port is plaintext and accepts every"
            + " declaration that names one of this dispatcher's clusters, so whoever reaches port"
            + " {} can read every topic the dispatcher's Kafka credentials can. Do not run a"
            + " dispatcher outside tests",
        server.getPort());
    return new DeliveryServer(server, acceptors, workers, advertised);
  }

  /** The port being served, which is the one asked for unless that was 0. */
  public int port() {
    return server.getPort();
  }

  /** The address sidecars are told to reach this replica at, as host:port. */
  public String advertisedAddress() {
    return advertised;
  }

  /** Stops accepting new streams. Open ones continue. */
  public void stopAccepting() {
    server.shutdown();
  }

  /** Blocks until the server has stopped. */
  public void awaitTermination() throws InterruptedException {
    server.awaitTermination();
  }

  /** Stops accepting streams, gives open ones a moment to finish, then cancels what is left. */
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
    acceptors.shutdownGracefully(0, 1, TimeUnit.SECONDS);
    workers.shutdownGracefully(0, 1, TimeUnit.SECONDS);
    LOG.info("delivery service stopped");
  }

  static String hostAndPort(InetAddress address, int port) {
    String host = address.getHostAddress();
    if (address instanceof Inet6Address) {
      // Without the scope a link-local address carries, which means nothing to another host.
      int scope = host.indexOf('%');
      host = "[" + (scope < 0 ? host : host.substring(0, scope)) + "]";
    }
    return host + ":" + port;
  }

  /**
   * The address of this host's network interface: in a pod, the pod IP. The first address that is
   * not loopback or link-local, of an interface that is up, IPv4 before IPv6; the loopback address
   * when there is none, which only a sidecar on the same host can use.
   */
  static InetAddress interfaceAddress() {
    InetAddress fallback = null;
    try {
      List<NetworkInterface> interfaces = Collections.list(NetworkInterface.getNetworkInterfaces());
      for (NetworkInterface candidate : interfaces) {
        if (!candidate.isUp() || candidate.isLoopback()) {
          continue;
        }
        for (InetAddress address : Collections.list(candidate.getInetAddresses())) {
          if (address.isLoopbackAddress()
              || address.isLinkLocalAddress()
              || address.isAnyLocalAddress()
              || address.isMulticastAddress()) {
            continue;
          }
          if (address instanceof Inet4Address) {
            return address;
          }
          if (fallback == null) {
            fallback = address;
          }
        }
      }
    } catch (SocketException e) {
      LOG.warn("could not list the network interfaces", e);
    }
    return fallback != null ? fallback : InetAddress.getLoopbackAddress();
  }
}
