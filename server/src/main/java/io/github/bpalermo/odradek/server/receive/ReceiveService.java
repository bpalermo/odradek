package io.github.bpalermo.odradek.server.receive;

import io.github.bpalermo.odradek.config.v1.ConsumerSource;
import io.github.bpalermo.odradek.config.v1.ServerConfig;
import io.github.bpalermo.odradek.server.Version;
import io.github.bpalermo.odradek.server.consume.AppStream;
import io.github.bpalermo.odradek.server.consume.DispatcherSource;
import io.github.bpalermo.odradek.server.telemetry.ServerMetrics;
import io.github.bpalermo.odradek.server.tls.CredentialException;
import io.github.bpalermo.odradek.server.tls.PeerPolicy;
import io.github.bpalermo.odradek.server.tls.ReloadingSslContext;
import io.github.bpalermo.odradek.server.tls.TlsBinding;
import io.github.bpalermo.odradek.server.tls.TlsSources;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.epoll.EpollIoHandler;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.handler.ssl.SslContext;
import java.io.Closeable;
import java.time.Duration;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The receiving side of ADR 6: what serves a Consume stream whose topics have the dispatcher as
 * their {@code source}. It owns the connections' event loop and makes a {@link ReceiveSession} per
 * stream; nothing is connected until an app subscribes, so a server starts, and is ready, whether
 * or not the dispatcher can be reached.
 *
 * <p>With {@code dispatcher.tls} every connection is mutual TLS: this server presents its
 * certificate, whose SPIFFE ID is what the dispatcher's policy goes by, and accepts only a
 * dispatcher whose SPIFFE ID the configuration accepts. <b>Without it the connection is plaintext
 * and unauthenticated</b>: the dispatcher cannot tell this server from any other, and nothing
 * proves to this server that what answers at {@code dispatcher.address} is the dispatcher.
 */
public final class ReceiveService implements DispatcherSource, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(ReceiveService.class);

  /** How long an undecodable record is kept before it is rejected. */
  static final Duration REJECT_PAUSE = Duration.ofSeconds(1);

  /** How long a stream that ends waits for the dispatcher to close its Deliver streams. */
  static final Duration DRAIN = Duration.ofSeconds(5);

  /**
   * The largest message accepted from the dispatcher. A batch is about 1 MiB unless one record is
   * larger, and how large a record may be is the brokers' business, not something to cap here a
   * second time.
   */
  private static final int MAX_MESSAGE_BYTES = 256 << 20;

  private final ReceiveSession.Options options;
  private final ServerMetrics metrics;
  private final ReceiveSession.Channels connector;
  private final EventLoopGroup loops;
  private final AtomicLong counter = new AtomicLong();

  private ReceiveService(
      ReceiveSession.Options options,
      ServerMetrics metrics,
      ReceiveSession.Channels connector,
      EventLoopGroup loops) {
    this.options = options;
    this.metrics = metrics;
    this.connector = connector;
    this.loops = loops;
  }

  /** Whether any consumer of the configuration is received from a dispatcher. */
  public static boolean receives(ServerConfig config) {
    return config.getConsumersMap().values().stream()
        .anyMatch(consumer -> consumer.getSource() == ConsumerSource.CONSUMER_SOURCE_DISPATCHER);
  }

  /**
   * Makes the service for a configuration. It connects to nothing. A configuration without a
   * consumer received from a dispatcher gets a service that refuses every stream and holds no
   * thread.
   */
  public static ReceiveService create(ServerConfig config, TlsSources tls, ServerMetrics metrics)
      throws CredentialException {
    if (!receives(config)) {
      return new ReceiveService(null, metrics, null, null);
    }
    String address = config.getDispatcher().getAddress();
    TlsBinding binding = tls.dispatcher();
    if (config.getDispatcher().hasTls() != (binding != null)) {
      // One configuration made both; a connection must never be less safe than it asks for.
      throw new IllegalStateException("dispatcher.tls and the credentials loaded for it disagree");
    }
    SslContext context = binding == null ? null : ReloadingSslContext.forDispatcher(binding);
    if (context == null) {
      LOG.warn(
          "THE CONNECTION TO THE DISPATCHER AT {} IS PLAINTEXT AND UNAUTHENTICATED (no"
              + " dispatcher.tls): records and acknowledgements travel unencrypted, the dispatcher"
              + " cannot tell this server from any other, and nothing proves that what answers"
              + " there is the dispatcher. Do not receive from a dispatcher like this outside"
              + " tests",
          address);
    } else {
      LOG.info(
          "the dispatcher at {} is connected to with mutual TLS and must present {}",
          address,
          accepted(binding.policy()));
    }
    EventLoopGroup loops = new MultiThreadIoEventLoopGroup(1, EpollIoHandler.newFactory());
    ReceiveSession.Options options =
        new ReceiveSession.Options(
            address, Version.VERSION, REJECT_PAUSE, DRAIN, System::nanoTime, Backoff::new);
    return new ReceiveService(options, metrics, target -> connect(target, loops, context), loops);
  }

  private static String accepted(PeerPolicy policy) {
    if (policy.trustDomain().isEmpty()) {
      return "one of " + new TreeSet<>(policy.spiffeIds());
    }
    return "a SPIFFE ID of trust domain "
        + policy.trustDomain()
        + (policy.spiffeIds().isEmpty() ? "" : " or one of " + new TreeSet<>(policy.spiffeIds()));
  }

  /** A service with these timings whose connections are made by {@code connector}, for tests. */
  static ReceiveService create(
      ReceiveSession.Options options, ServerMetrics metrics, ReceiveSession.Channels connector) {
    return new ReceiveService(options, metrics, connector, null);
  }

  /**
   * The one place a connection to a dispatcher is made, bootstrap address and replicas alike.
   *
   * @param context the TLS context of {@code dispatcher.tls}, or null for plaintext. It is asked
   *     for an engine at every connection, with the credentials of that moment, and verifies the
   *     dispatcher by SPIFFE ID: {@code target} is an IP address as often as a name.
   */
  private static ManagedChannel connect(String target, EventLoopGroup loops, SslContext context) {
    // "dns" also takes an address literal, which is what a replica's address is; the configured
    // address is normally the name of a Service, resolved again whenever it is connected to.
    NettyChannelBuilder builder =
        NettyChannelBuilder.forTarget("dns:///" + target)
            .eventLoopGroup(loops)
            .channelType(EpollSocketChannel.class);
    if (context == null) {
      builder.usePlaintext();
    } else {
      builder.sslContext(context);
    }
    return builder
        // What arrives is only ever queued for the session's thread.
        .directExecutor()
        // A dispatcher that vanished without closing its connection is noticed, and the stream
        // opened again elsewhere. The dispatcher permits pings this often.
        .keepAliveTime(30, TimeUnit.SECONDS)
        .keepAliveTimeout(10, TimeUnit.SECONDS)
        .maxInboundMessageSize(MAX_MESSAGE_BYTES)
        .build();
  }

  @Override
  public AppStream open(Request request) {
    if (connector == null) {
      throw Status.FAILED_PRECONDITION
          .withDescription("this server has no dispatcher to receive from")
          .asRuntimeException();
    }
    String group = request.entries().get(request.topics().get(0)).getGroupId();
    return new ReceiveSession(
        "odradek-receive-" + group + "-" + counter.incrementAndGet(),
        request,
        options,
        connector,
        metrics);
  }

  /** Releases the event loop. The streams must have ended: see {@code ConsumeService.shutdown}. */
  @Override
  public void close() {
    if (loops != null) {
      loops.shutdownGracefully(0, 1, TimeUnit.SECONDS);
    }
  }
}
