package io.github.bpalermo.odradek.server.tls;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.bpalermo.kafka.spiffe.id.SpiffeId;
import io.github.bpalermo.kafka.spiffe.id.TrustDomain;
import io.github.bpalermo.kafka.spiffe.x509.BundleSet;
import io.github.bpalermo.kafka.spiffe.x509.PeerRejectedException;
import io.github.bpalermo.kafka.spiffe.x509.SpiffeTrustManager;
import io.github.bpalermo.kafka.spiffe.x509.X509Bundle;
import io.github.bpalermo.kafka.spiffe.x509.X509Context;
import io.github.bpalermo.kafka.spiffe.x509.X509Svid;
import io.grpc.CallOptions;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ServerCalls;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.SslContext;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.cert.Certificate;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * TLS session resumption on the delivery port with {@code spiffe} sources (ADR 9).
 *
 * <p>kafka-spiffe's contexts time sessions out after one second, and from 0.3.0 their engines
 * verify a resumed session's peer again, at the context's clock, since the JDK resumes a session
 * without checking either certificate. These tests reconnect within that second and show, on both
 * ends and through the engines Netty and grpc-netty make of those contexts, that a peer whose SVID
 * has expired since the first handshake is refused, that a valid resumption still verifies the peer
 * and negotiates h2, and that a rotated bundle allows no resumption at all.
 *
 * <p>A reconnection is a resumption only if the JDK resumes, which it does only within the second;
 * a test that finds it made a full handshake instead (a slow machine) tries again, a few times.
 */
class ResumptionTest {
  private static final String DISPATCHER_ID = "spiffe://example.org/ns/kafka/sa/dispatcher";
  private static final String SIDECAR_ID = "spiffe://example.org/ns/billing/sa/billing";
  private static final PeerPolicy DISPATCHER = new PeerPolicy(Set.of(DISPATCHER_ID), "");

  /** How many times a test reconnects for the JDK to resume a session within its second. */
  private static final int TRIES = 5;

  /** Past both SVIDs' end (they are valid for one day) and the authorities' (two). */
  private static final Duration LATER = Duration.ofDays(3);

  /** What kafka-spiffe's engines say when they refuse a resumed session's peer. */
  private static final String RESUMED = "the peer of a resumed session is refused";

  @TempDir static Path shared;

  private static String ca;
  private static String rotatedCa;
  private static TestPki.Issued dispatcher;
  private static TestPki.Issued sidecar;

  private final List<Server> servers = new ArrayList<>();
  private final List<ManagedChannel> channels = new ArrayList<>();

  @BeforeAll
  static void issue() throws Exception {
    TestPki pki = new TestPki(shared, "ca");
    ca = pki.caPem();
    rotatedCa = new TestPki(shared, "rotated").caPem();
    dispatcher = pki.issue("dispatcher", "uri:" + DISPATCHER_ID);
    sidecar = pki.issue("sidecar", "uri:" + SIDECAR_ID);
  }

  @AfterEach
  void stop() throws InterruptedException {
    for (ManagedChannel channel : channels) {
      channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
    for (Server server : servers) {
      server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  // -------------------------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------------------------

  /** What a context's verifications came to, as its observer heard them. */
  private static final class Recorder implements SpiffeTrustManager.Observer {
    final List<String> events = new CopyOnWriteArrayList<>();

    @Override
    public void accepted(boolean server, SpiffeId id) {
      events.add(id.toString());
    }

    @Override
    public void rejected(boolean server, PeerRejectedException rejection) {
      events.add(rejection.reason().name());
    }
  }

  /** A clock a test moves. */
  private static final class MovableClock extends Clock {
    private volatile Instant now = Instant.now();

    void advance(Duration duration) {
      now = now.plus(duration);
    }

    @Override
    public Instant instant() {
      return now;
    }

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return this;
    }
  }

  /** A snapshot as the library makes one: an SVID and example.org's bundle of {@code caPem}. */
  private static X509Context snapshot(TestPki.Issued issued, String caPem) throws Exception {
    return new X509Context(
        X509Svid.of(
            Pem.certificates(issued.certPem().getBytes(StandardCharsets.US_ASCII), "test"),
            Pem.privateKey(issued.keyPem().getBytes(StandardCharsets.US_ASCII), "test"),
            ""),
        BundleSet.of(
            List.of(
                new X509Bundle(
                    TrustDomain.parse("example.org"),
                    Pem.certificates(caPem.getBytes(StandardCharsets.US_ASCII), "test")))));
  }

  /** The dispatcher's listener, its peers verified at {@code clock}. */
  private static SslContext listener(
      AtomicReference<X509Context> snapshots, Recorder observer, Clock clock)
      throws CredentialException {
    return ReloadingSslContext.forListener(snapshots::get, observer, clock);
  }

  private static SslContext listener(Recorder observer, Clock clock) throws Exception {
    return listener(new AtomicReference<>(snapshot(dispatcher, ca)), observer, clock);
  }

  /** A sidecar's connections to the dispatcher, its peers verified at {@code clock}. */
  private static SslContext sidecar(Recorder observer, Clock clock) throws Exception {
    X509Context snapshot = snapshot(sidecar, ca);
    return ReloadingSslContext.forDispatcher(() -> snapshot, DISPATCHER, observer, clock);
  }

  /** Two ends of a connection, as Netty makes them of the contexts. */
  private record Connection(SSLEngine sidecar, SSLEngine dispatcher) {}

  /** Completes a handshake in memory, the sidecar's engine made for the dispatcher's address. */
  private static Connection connect(SslContext sidecarEnd, SslContext dispatcherEnd)
      throws SSLException {
    Connection connection =
        new Connection(
            sidecarEnd.newEngine(ByteBufAllocator.DEFAULT, "10.0.0.9", 9465),
            dispatcherEnd.newEngine(ByteBufAllocator.DEFAULT));
    TlsTest.handshake(connection.sidecar(), connection.dispatcher());
    return connection;
  }

  /**
   * Whether {@code again} resumed {@code first}'s session: a resumed session keeps the creation
   * time of the one it resumes and the peer's certificates as they were decoded then, where a full
   * handshake decodes them anew.
   */
  private static boolean resumed(SSLSession first, SSLSession again) throws SSLException {
    Certificate before = first.getPeerCertificates()[0];
    Certificate after = again.getPeerCertificates()[0];
    return first.getCreationTime() == again.getCreationTime() && before == after;
  }

  /** The exception, and whatever caused it, in order. */
  private static List<Throwable> causes(Throwable thrown) {
    List<Throwable> chain = new ArrayList<>();
    for (Throwable t = thrown; t != null && !chain.contains(t); t = t.getCause()) {
      chain.add(t);
    }
    return chain;
  }

  /** Whether {@code thrown} is a resumed session's peer refused for a chain that fails. */
  private static boolean refusedOnResumption(Throwable thrown) {
    List<Throwable> chain = causes(thrown);
    return chain.stream().anyMatch(t -> String.valueOf(t.getMessage()).contains(RESUMED))
        && chain.stream()
            .anyMatch(
                t ->
                    t instanceof PeerRejectedException rejected
                        && rejected.reason() == PeerRejectedException.Reason.CHAIN);
  }

  // -------------------------------------------------------------------------------------------
  // In memory, through the engines Netty makes of the contexts
  // -------------------------------------------------------------------------------------------

  @Test
  void aResumedSessionStillHasItsPeerVerifiedAndNegotiatesH2() throws Exception {
    for (int attempt = 0; attempt < TRIES; attempt++) {
      Recorder atListener = new Recorder();
      Recorder atSidecar = new Recorder();
      SslContext server = listener(atListener, Clock.systemUTC());
      SslContext client = sidecar(atSidecar, Clock.systemUTC());
      Connection first = connect(client, server);
      Connection again = connect(client, server);
      if (!resumed(first.sidecar().getSession(), again.sidecar().getSession())) {
        continue; // More than a second passed: a full handshake. Try again.
      }
      assertThat(resumed(first.dispatcher().getSession(), again.dispatcher().getSession()))
          .as("the dispatcher resumed too")
          .isTrue();
      assertThat(again.sidecar().getApplicationProtocol()).isEqualTo("h2");
      assertThat(again.dispatcher().getApplicationProtocol()).isEqualTo("h2");
      // Each end verified its peer for each connection, the resumed one included.
      assertThat(atListener.events).containsExactly(SIDECAR_ID, SIDECAR_ID);
      assertThat(atSidecar.events).containsExactly(DISPATCHER_ID, DISPATCHER_ID);
      return;
    }
    throw new AssertionError("the JDK never resumed a session in " + TRIES + " tries");
  }

  @Test
  void theListenerRefusesAResumedSessionOfASidecarWhoseSvidHasSinceExpired() throws Exception {
    for (int attempt = 0; attempt < TRIES; attempt++) {
      Recorder atListener = new Recorder();
      MovableClock clock = new MovableClock();
      SslContext server = listener(atListener, clock);
      SslContext client = sidecar(new Recorder(), Clock.systemUTC());
      connect(client, server);
      clock.advance(LATER);
      Throwable thrown = catchHandshake(client, server);
      if (thrown != null && !refusedOnResumption(thrown)) {
        continue; // A full handshake, refused by the trust manager. Try again.
      }
      assertThat(thrown).as("the reconnection with an expired SVID").isNotNull();
      assertThat(atListener.events).containsExactly(SIDECAR_ID, "CHAIN");
      return;
    }
    throw new AssertionError("the JDK never resumed a session in " + TRIES + " tries");
  }

  @Test
  void aSidecarRefusesAResumedSessionOfADispatcherWhoseSvidHasSinceExpired() throws Exception {
    for (int attempt = 0; attempt < TRIES; attempt++) {
      Recorder atSidecar = new Recorder();
      MovableClock clock = new MovableClock();
      SslContext server = listener(new Recorder(), Clock.systemUTC());
      SslContext client = sidecar(atSidecar, clock);
      connect(client, server);
      clock.advance(LATER);
      Throwable thrown = catchHandshake(client, server);
      if (thrown != null && !refusedOnResumption(thrown)) {
        continue;
      }
      assertThat(thrown).as("the reconnection to an expired SVID").isNotNull();
      assertThat(atSidecar.events).containsExactly(DISPATCHER_ID, "CHAIN");
      return;
    }
    throw new AssertionError("the JDK never resumed a session in " + TRIES + " tries");
  }

  /**
   * A rotated bundle is a new snapshot and so a new context, with a session cache and ticket keys
   * of its own: the reconnection within the second is a full handshake, and the sidecar, whose
   * chain the new bundle no longer verifies, is refused by the trust manager. True of 0.2.0 too.
   */
  @Test
  void aRotatedBundleAllowsNoResumptionOfASessionMadeBeforeIt() throws Exception {
    Recorder atListener = new Recorder();
    AtomicReference<X509Context> snapshots = new AtomicReference<>(snapshot(dispatcher, ca));
    SslContext server = listener(snapshots, atListener, Clock.systemUTC());
    SslContext client = sidecar(new Recorder(), Clock.systemUTC());
    connect(client, server);

    snapshots.set(snapshot(dispatcher, rotatedCa));
    Throwable thrown = catchHandshake(client, server);

    assertThat(thrown).isInstanceOf(SSLException.class);
    assertThat(causes(thrown)).noneMatch(t -> String.valueOf(t.getMessage()).contains(RESUMED));
    assertThat(atListener.events).containsExactly(SIDECAR_ID, "CHAIN");
  }

  /** The exception a handshake ends with, or null if it completes. */
  private static Throwable catchHandshake(SslContext client, SslContext server) {
    try {
      connect(client, server);
      return null;
    } catch (SSLException | RuntimeException e) {
      return e;
    }
  }

  // -------------------------------------------------------------------------------------------
  // Over real connections: grpc-netty's handlers, as the delivery port has them
  // -------------------------------------------------------------------------------------------

  private static final MethodDescriptor.Marshaller<byte[]> BYTES =
      new MethodDescriptor.Marshaller<>() {
        @Override
        public InputStream stream(byte[] value) {
          return new ByteArrayInputStream(value);
        }

        @Override
        public byte[] parse(InputStream stream) {
          try {
            return stream.readAllBytes();
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        }
      };

  private static final MethodDescriptor<byte[], byte[]> ECHO =
      MethodDescriptor.<byte[], byte[]>newBuilder()
          .setType(MethodDescriptor.MethodType.UNARY)
          .setFullMethodName("test.Echo/Echo")
          .setRequestMarshaller(BYTES)
          .setResponseMarshaller(BYTES)
          .build();

  /** A server on the listener's context that echoes, and records each call's TLS session. */
  private int serve(SslContext context, List<SSLSession> sessions) throws IOException {
    ServerServiceDefinition echo =
        ServerServiceDefinition.builder("test.Echo")
            .addMethod(
                ECHO,
                ServerCalls.asyncUnaryCall(
                    (request, response) -> {
                      response.onNext(request);
                      response.onCompleted();
                    }))
            .build();
    Server server =
        NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
            .sslContext(context)
            .addService(
                ServerInterceptors.intercept(
                    echo,
                    new ServerInterceptor() {
                      @Override
                      public <Q, R> ServerCall.Listener<Q> interceptCall(
                          ServerCall<Q, R> call, Metadata headers, ServerCallHandler<Q, R> next) {
                        sessions.add(call.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION));
                        return next.startCall(call, headers);
                      }
                    }))
            .build()
            .start();
    servers.add(server);
    return server.getPort();
  }

  /** One call on a channel of its own, so on a connection of its own. */
  private void call(SslContext context, int port) {
    ManagedChannel channel =
        NettyChannelBuilder.forAddress("127.0.0.1", port).sslContext(context).build();
    channels.add(channel);
    byte[] answer =
        ClientCalls.blockingUnaryCall(
            channel,
            ECHO,
            CallOptions.DEFAULT.withDeadlineAfter(10, TimeUnit.SECONDS),
            new byte[] {7});
    assertThat(answer).containsExactly(7);
    channel.shutdownNow();
  }

  /**
   * Through grpc-netty's handlers on both ends: a sidecar that reconnects within the second resumes
   * its session, the listener verifies it again, and once the sidecar's SVID has expired by the
   * listener's clock, the next reconnection within that second is refused.
   */
  @Test
  void overGrpcNettyAResumedConnectionIsVerifiedAndAnExpiredSidecarRefused() throws Exception {
    for (int attempt = 0; attempt < TRIES; attempt++) {
      Recorder atListener = new Recorder();
      MovableClock clock = new MovableClock();
      List<SSLSession> sessions = new CopyOnWriteArrayList<>();
      int port = serve(listener(atListener, clock), sessions);
      SslContext client = sidecar(new Recorder(), Clock.systemUTC());

      call(client, port);
      call(client, port);
      if (!resumed(sessions.get(0), sessions.get(1))) {
        continue; // More than a second passed: a full handshake. Try again.
      }

      clock.advance(LATER);
      // Within the session's second, or a refusal would prove nothing about resumption.
      if (System.currentTimeMillis() - sessions.get(0).getCreationTime() > 500) {
        continue;
      }
      assertThatThrownBy(() -> call(client, port))
          .isInstanceOfSatisfying(
              StatusRuntimeException.class,
              e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE));
      // The full handshake, the resumed one, and the refusal.
      assertThat(atListener.events).containsExactly(SIDECAR_ID, SIDECAR_ID, "CHAIN");
      return;
    }
    throw new AssertionError("the JDK never resumed a session in " + TRIES + " tries");
  }
}
