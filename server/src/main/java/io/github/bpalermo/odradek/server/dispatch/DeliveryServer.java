package io.github.bpalermo.odradek.server.dispatch;

import io.github.bpalermo.odradek.config.v1.Dispatch;
import io.github.bpalermo.odradek.server.tls.CredentialException;
import io.github.bpalermo.odradek.server.tls.CredentialSource;
import io.github.bpalermo.odradek.server.tls.ReloadingSslContext;
import io.github.bpalermo.odradek.server.tls.TlsSources;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.epoll.EpollIoHandler;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.handler.ssl.SslContext;
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
 * <p>With {@code dispatch.tls} the port speaks mutual TLS only: a connection without a client
 * certificate that verifies does not get past the handshake, and the certificate's SPIFFE ID is
 * what {@link DispatchService} authorizes each declaration by. The credentials are those of the
 * moment of each handshake ({@link ReloadingSslContext}), so a rotation restarts nothing and ends
 * no stream.
 *
 * <p><b>Without it the port is plaintext and open to whoever reaches it</b>, which is for tests.
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
   * @param tls where the listener's credentials are, when the configuration has {@code
   *     dispatch.tls}; the port is plaintext otherwise
   * @throws IOException if the port cannot be bound
   * @throws CredentialException if no TLS context can be made of the credentials
   */
  public static DeliveryServer start(Dispatch dispatch, DispatchService service, TlsSources tls)
      throws IOException, CredentialException {
    return start(dispatch, service, tls.listener());
  }

  /** As above, with the listener's credentials themselves; null for a plaintext port. */
  static DeliveryServer start(
      Dispatch dispatch, DispatchService service, CredentialSource credentials)
      throws IOException, CredentialException {
    if (dispatch.hasTls() != (credentials != null)
        || dispatch.hasTls() != service.enforcesPolicy()) {
      // One configuration made all three; a listener must never be less safe than its service.
      throw new IllegalStateException("dispatch.tls and the credentials loaded for it disagree");
    }
    SslContext context = credentials == null ? null : ReloadingSslContext.forListener(credentials);
    EventLoopGroup acceptors = new MultiThreadIoEventLoopGroup(1, EpollIoHandler.newFactory());
    EventLoopGroup workers = new MultiThreadIoEventLoopGroup(EpollIoHandler.newFactory());
    NettyServerBuilder builder =
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
            .addService(ServerInterceptors.intercept(service, DispatchService.identities()));
    if (context != null) {
      // Asked for an engine at every connection, so a rotation needs nothing done here.
      builder.sslContext(context);
    }
    Server server = builder.build();
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
    LOG.info(
        "delivery service on port {}, advertised as {}, {}",
        server.getPort(),
        advertised,
        context == null ? "PLAINTEXT" : "with mutual TLS");
    if (context == null) {
      LOG.warn(
          "THE DELIVERY PORT IS PLAINTEXT AND UNAUTHENTICATED (no dispatch.tls): it accepts every"
              + " declaration that names one of this dispatcher's clusters, so whoever reaches"
              + " port {} can read every topic the dispatcher's Kafka credentials can. Do not run"
              + " a dispatcher like this outside tests",
          server.getPort());
    } else if (service.refusesEverything()) {
      LOG.warn(
          "dispatch.policy has no rule: every declaration will be refused with PERMISSION_DENIED");
    }
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
