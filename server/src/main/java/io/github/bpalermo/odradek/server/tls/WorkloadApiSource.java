package io.github.bpalermo.odradek.server.tls;

import io.grpc.CallOptions;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.epoll.EpollIoHandler;
import io.netty.channel.unix.DomainSocketAddress;
import java.io.Closeable;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Credentials from the SPIFFE Workload API (typically a SPIRE agent): the workload's X.509-SVID and
 * the trust bundles, kept current by one long-lived stream. The private key never touches disk.
 *
 * <p>The agent sends the full set on every change. An update that is not usable is rejected and the
 * current set stays. When the stream ends it is opened again, with a growing pause; the current set
 * stays in use meanwhile, for as long as its certificate lasts.
 */
public final class WorkloadApiSource implements CredentialSource, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(WorkloadApiSource.class);

  private static final long FIRST_RETRY_MS = 500;
  private static final long MAX_RETRY_MS = 30_000;

  private final String socket;
  private final Clock clock;
  private final EventLoopGroup group;
  private final ManagedChannel channel;
  private final CountDownLatch first = new CountDownLatch(1);
  private final AtomicLong reloads = new AtomicLong();
  private final AtomicLong failures = new AtomicLong();

  private volatile Credentials current;
  private volatile String lastProblem = "no answer from the Workload API";
  private volatile boolean closed;
  private long retryMs = FIRST_RETRY_MS;

  /**
   * Opens the stream and waits for the first usable SVID.
   *
   * @param socket path of the Workload API's Unix socket
   * @throws CredentialException if none arrives within {@code timeout}
   */
  public WorkloadApiSource(String socket, Clock clock, Duration timeout)
      throws CredentialException, InterruptedException {
    this.socket = socket;
    this.clock = clock;
    this.group =
        new MultiThreadIoEventLoopGroup(
            1,
            runnable -> {
              Thread thread = new Thread(runnable, "workload-api");
              thread.setDaemon(true);
              return thread;
            },
            EpollIoHandler.newFactory());
    this.channel =
        NettyChannelBuilder.forAddress(new DomainSocketAddress(socket))
            .eventLoopGroup(group)
            .channelType(EpollDomainSocketChannel.class)
            .usePlaintext()
            .build();
    watch();
    if (!first.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
      String problem = lastProblem;
      close();
      throw new CredentialException(
          "no X.509-SVID from the Workload API at "
              + socket
              + " within "
              + timeout
              + ": "
              + problem);
    }
  }

  @Override
  public Credentials current() {
    return current;
  }

  /** How many times an update replaced the current set, the first one included. */
  public long reloads() {
    return reloads.get();
  }

  /** How many updates were rejected, plus how many times the stream ended. */
  public long failures() {
    return failures.get();
  }

  private void watch() {
    Metadata headers = new Metadata();
    headers.put(WorkloadApi.SECURITY_HEADER, "true");
    ClientCalls.asyncServerStreamingCall(
        ClientInterceptors.intercept(channel, MetadataUtils.newAttachHeadersInterceptor(headers))
            .newCall(WorkloadApi.FETCH_X509_SVID, CallOptions.DEFAULT),
        null,
        new StreamObserver<WorkloadApi.Response>() {
          @Override
          public void onNext(WorkloadApi.Response response) {
            apply(response);
          }

          @Override
          public void onError(Throwable t) {
            ended(t.toString());
          }

          @Override
          public void onCompleted() {
            ended("the Workload API closed the stream");
          }
        });
  }

  private synchronized void apply(WorkloadApi.Response response) {
    Credentials next;
    try {
      next = credentials(response);
    } catch (CredentialException e) {
      failures.incrementAndGet();
      lastProblem = e.getMessage();
      LOG.warn("Workload API {}: update rejected: {}", socket, e.getMessage());
      return;
    }
    current = next;
    retryMs = FIRST_RETRY_MS;
    reloads.incrementAndGet();
    LOG.info(
        "Workload API {}: X.509-SVID {} received, expires {}",
        socket,
        response.svids().get(0).spiffeId(),
        next.notAfter());
    first.countDown();
  }

  /** The first SVID of the update, with its bundle and the federated ones. */
  private Credentials credentials(WorkloadApi.Response response) throws CredentialException {
    if (response.svids().isEmpty()) {
      throw new CredentialException("the update holds no SVID");
    }
    WorkloadApi.Svid svid = response.svids().get(0);
    List<X509Certificate> chain =
        Pem.certificatesDer(svid.certificates().toByteArray(), "the SVID's certificates");
    PrivateKey key = Pem.privateKeyDer(svid.key().toByteArray(), "the SVID's key");
    List<X509Certificate> bundle =
        Pem.certificatesDer(svid.bundle().toByteArray(), "the trust bundle");
    if (!svid.spiffeId().startsWith("spiffe://")) {
      throw new CredentialException("the SVID's ID is not a SPIFFE ID: " + svid.spiffeId());
    }
    Map<String, List<X509Certificate>> bundles = new HashMap<>();
    for (var federated : response.federatedBundles().entrySet()) {
      // Keyed by trust domain ID; older agents send the bare name.
      String domain = federated.getKey();
      if (domain.startsWith("spiffe://")) {
        domain = PeerPolicy.trustDomain(domain);
      }
      bundles.put(
          domain,
          Pem.certificatesDer(
              federated.getValue().toByteArray(), "the bundle of " + federated.getKey()));
    }
    bundles.put(PeerPolicy.trustDomain(svid.spiffeId()), bundle);
    return Credentials.validated(key, chain, bundle, bundles, clock.instant());
  }

  private synchronized void ended(String why) {
    if (closed) {
      return;
    }
    failures.incrementAndGet();
    lastProblem = why;
    LOG.warn("Workload API {}: stream ended, retrying in {} ms: {}", socket, retryMs, why);
    group.schedule(this::rewatch, retryMs, TimeUnit.MILLISECONDS);
    retryMs = Math.min(MAX_RETRY_MS, retryMs * 2);
  }

  private void rewatch() {
    if (!closed) {
      watch();
    }
  }

  @Override
  public synchronized void close() {
    closed = true;
    channel.shutdownNow();
    group.shutdownGracefully(0, 1, TimeUnit.SECONDS);
  }
}
