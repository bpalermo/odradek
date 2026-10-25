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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
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

  /** How long a stream that ends waits for the dispatcher to close its Deliver streams. */
  static final Duration DRAIN = Duration.ofSeconds(5);

  /**
   * The largest message accepted from the dispatcher. A batch is about 1 MiB unless one record is
   * larger, and how large a record may be is the brokers' business, not something to cap here a
   * second time.
   */
  private static final int MAX_MESSAGE_BYTES = 256 << 20;

  /**
   * The connections to the dispatcher, shared by every Consume stream of this server: one to each
   * address, the Service's and each replica's, for as long as a stream uses it. A pod with several
   * Consume streams, or several topics on one, therefore makes one TLS handshake per replica and
   * not one per stream; each stream still has Deliver streams of its own on it.
   */
  static final class Pool implements ReceiveSession.Channels {
    /** One connection and how many sessions hold it. */
    private static final class Shared {
      final ManagedChannel channel;
      int users;

      Shared(ManagedChannel channel) {
        this.channel = channel;
      }
    }

    private final Function<String, ManagedChannel> connect;
    private final ScheduledExecutorService timer;
    private final Duration drain;
    private final Map<String, Shared> shared = new HashMap<>();

    /** Given back by their last session: closing in good order, and cut if that takes too long. */
    private final Set<ManagedChannel> closing = new HashSet<>();

    /**
     * @param connect makes the connection to an address
     * @param timer runs what cuts a connection that does not close
     * @param drain how long a connection nobody uses any more is given to close in good order
     */
    Pool(Function<String, ManagedChannel> connect, ScheduledExecutorService timer, Duration drain) {
      this.connect = connect;
      this.timer = timer;
      this.drain = drain;
    }

    @Override
    public synchronized ManagedChannel connect(String address) {
      Shared entry = shared.get(address);
      if (entry == null) {
        entry = new Shared(connect.apply(address));
        shared.put(address, entry);
      }
      entry.users++;
      return entry.channel;
    }

    @Override
    public synchronized void release(String address, ManagedChannel channel) {
      Shared entry = shared.get(address);
      if (entry == null || entry.channel != channel) {
        throw new IllegalStateException("a connection to " + address + " was given back twice");
      }
      if (--entry.users > 0) {
        return;
      }
      shared.remove(address);
      // The streams half-closed on it end when the dispatcher answers; one that never does must
      // not keep the connection for ever.
      channel.shutdown();
      closing.add(channel);
      try {
        timer.schedule(() -> cut(channel), drain.toMillis(), TimeUnit.MILLISECONDS);
      } catch (RejectedExecutionException e) {
        // The server is stopping and its event loop with it: nothing is left to wait for.
        cut(channel);
      }
    }

    private synchronized void cut(ManagedChannel channel) {
      if (closing.remove(channel)) {
        channel.shutdownNow();
      }
    }

    /** The connections close by themselves, in good order; {@link #close} waits for them. */
    @Override
    public void drained(Collection<ManagedChannel> released, Duration timeout) {}

    /** The server stops: waits a moment for the connections that are closing, then cuts all. */
    void close() {
      List<ManagedChannel> all = new ArrayList<>();
      synchronized (this) {
        shared.values().forEach(entry -> all.add(entry.channel));
        shared.clear();
        all.addAll(closing);
        closing.clear();
      }
      all.forEach(ManagedChannel::shutdown);
      long deadline = System.nanoTime() + drain.toNanos();
      for (ManagedChannel channel : all) {
        try {
          if (!channel.awaitTermination(
              Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS)) {
            channel.shutdownNow();
          }
        } catch (InterruptedException e) {
          channel.shutdownNow();
          Thread.currentThread().interrupt();
        }
      }
    }
  }

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
    SslContext context =
        binding == null
            ? null
            : ReloadingSslContext.forDispatcher(binding, tls.verifications(TlsSources.DISPATCHER));
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
        new ReceiveSession.Options(address, Version.VERSION, DRAIN, System::nanoTime, Backoff::new);
    return new ReceiveService(
        options, metrics, new Pool(target -> connect(target, loops, context), loops, DRAIN), loops);
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
   * The one place a connection to a dispatcher is made, bootstrap address and replicas alike, and
   * it is made once per address: see {@link Pool}.
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

  /**
   * Closes the connections to the dispatcher, giving it a moment to close the streams the Consume
   * streams half-closed, and releases the event loop. The streams must have ended: see {@code
   * ConsumeService.shutdown}.
   */
  @Override
  public void close() {
    if (connector instanceof Pool pool) {
      pool.close();
    }
    if (loops != null) {
      loops.shutdownGracefully(0, 1, TimeUnit.SECONDS);
    }
  }
}
