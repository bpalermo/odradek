package io.github.bpalermo.odradek.server.dispatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.util.Durations;
import io.github.bpalermo.odradek.config.v1.Cluster;
import io.github.bpalermo.odradek.config.v1.Consume;
import io.github.bpalermo.odradek.config.v1.Dispatch;
import io.github.bpalermo.odradek.config.v1.FileSource;
import io.github.bpalermo.odradek.config.v1.ListenerTls;
import io.github.bpalermo.odradek.config.v1.PolicyRule;
import io.github.bpalermo.odradek.config.v1.ServerConfig;
import io.github.bpalermo.odradek.delivery.v1.Credits;
import io.github.bpalermo.odradek.delivery.v1.Declare;
import io.github.bpalermo.odradek.delivery.v1.DeliverRequest;
import io.github.bpalermo.odradek.delivery.v1.DeliverResponse;
import io.github.bpalermo.odradek.delivery.v1.DeliveryServiceGrpc;
import io.github.bpalermo.odradek.delivery.v1.Settings;
import io.github.bpalermo.odradek.server.telemetry.ServerMetrics;
import io.github.bpalermo.odradek.server.tls.FileCredentialSource;
import io.github.bpalermo.odradek.server.tls.PeerPolicy;
import io.github.bpalermo.odradek.server.tls.ReloadingSslContext;
import io.github.bpalermo.odradek.server.tls.TestPki;
import io.github.bpalermo.odradek.server.tls.TlsBinding;
import io.github.bpalermo.odradek.server.tls.TlsSources;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The delivery port with mutual TLS, over real connections: the dispatcher's listener as {@code
 * Main} starts it, and clients that connect the way a sidecar's {@code ReceiveService} does. What
 * is asserted is what a sidecar gets to see: the answer to its declaration, or the status gRPC
 * makes of a handshake that failed. Kafka is the mock consumer.
 */
class DeliveryTlsTest {
  private static final String DISPATCHER_ID = "spiffe://example.org/ns/kafka/sa/dispatcher";
  private static final String NEXT_DISPATCHER_ID = "spiffe://example.org/ns/kafka/sa/dispatcher-2";
  private static final String BILLING = "spiffe://example.org/ns/billing/sa/billing";
  private static final String AUDIT = "spiffe://example.org/ns/audit/sa/audit";
  private static final TopicPartition ORDERS_0 = new TopicPartition("orders", 0);

  @TempDir static Path shared;

  private static String ca;
  private static String otherCa;
  private static TestPki.Issued dispatcher;
  private static TestPki.Issued nextDispatcher;
  private static TestPki.Issued namedDispatcher;
  private static TestPki.Issued strangeDispatcher;
  private static TestPki.Issued billing;
  private static TestPki.Issued audit;
  private static TestPki.Issued plain;
  private static TestPki.Issued stranger;

  @TempDir Path dir;

  private final List<MockConsumer<byte[], byte[]>> consumers = new CopyOnWriteArrayList<>();
  private final List<ManagedChannel> channels = new ArrayList<>();
  private final List<FileCredentialSource> sources = new ArrayList<>();
  private int directories;
  private FileCredentialSource listenerCredentials;
  private DispatchService service;
  private DeliveryServer delivery;

  @BeforeAll
  static void issue() throws Exception {
    TestPki pki = new TestPki(shared, "ca");
    TestPki other = new TestPki(shared, "other");
    ca = pki.caPem();
    otherCa = other.caPem();
    dispatcher = pki.issue("dispatcher", "uri:" + DISPATCHER_ID);
    nextDispatcher = pki.issue("dispatcher-2", "uri:" + NEXT_DISPATCHER_ID);
    namedDispatcher = pki.issue("dispatcher", "uri:" + DISPATCHER_ID + ",dns:localhost");
    strangeDispatcher = other.issue("strange-dispatcher", "uri:" + DISPATCHER_ID);
    billing = pki.issue("billing", "uri:" + BILLING);
    audit = pki.issue("audit", "uri:" + AUDIT);
    plain = pki.issue("plain", "dns:plain.example.org");
    stranger = other.issue("stranger", "uri:" + BILLING);
  }

  // -------------------------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------------------------

  /** Billing may consume orders in its group; audit may consume refunds in its own. */
  private static List<PolicyRule> policy() {
    return List.of(
        PolicyRule.newBuilder()
            .addSpiffeIds(BILLING)
            .setCluster("main")
            .addTopics("orders")
            .addGroups("billing")
            .build(),
        PolicyRule.newBuilder()
            .addSpiffeIds(AUDIT)
            .setCluster("main")
            .addTopics("refunds")
            .addGroups("audit")
            .build());
  }

  /** A files source of these credentials, in a directory of its own. */
  private FileCredentialSource source(TestPki.Issued issued, String caPem) throws Exception {
    Path where = dir.resolve("credentials-" + directories++);
    TestPki.write(where, issued, caPem);
    FileCredentialSource source =
        new FileCredentialSource(
            "test",
            where.resolve("tls.crt"),
            where.resolve("tls.key"),
            where.resolve("ca.crt"),
            Clock.systemUTC());
    sources.add(source);
    return source;
  }

  /** Replaces what a source reads, and has it read: a rotation, without the wait for a poll. */
  private void rotate(FileCredentialSource source, TestPki.Issued issued, String caPem)
      throws Exception {
    TestPki.write(dir.resolve("credentials-" + sources.indexOf(source)), issued, caPem);
    source.poll();
    assertThat(source.reloads()).isEqualTo(2);
  }

  private void start(TestPki.Issued certificate, String caPem, List<PolicyRule> rules)
      throws Exception {
    listenerCredentials = source(certificate, caPem);
    start(
        Dispatch.newBuilder()
            .setTls(ListenerTls.newBuilder().setFiles(FileSource.newBuilder().setDir("/unused")))
            .addAllPolicy(rules));
  }

  private void start(Dispatch.Builder dispatch) throws Exception {
    ServerConfig config =
        ServerConfig.newBuilder()
            .putClusters("main", Cluster.getDefaultInstance())
            .setDispatch(
                dispatch
                    .setPort(0)
                    .setAdvertisedAddress("127.0.0.1:9465")
                    .setIdleGrace(Durations.fromSeconds(60)))
            .setConsume(
                Consume.newBuilder()
                    .setCommitInterval(Durations.fromMillis(20))
                    .setRevokeDeadline(Durations.fromMillis(300)))
            .build();
    service =
        new DispatchService(
            config,
            TlsSources.none(),
            ServerMetrics.noop(),
            (cluster, properties) -> {
              MockConsumer<byte[], byte[]> consumer = new MockConsumer<>("none");
              consumers.add(consumer);
              return consumer;
            },
            new DeadLetters(config, TlsSources.none(), ServerMetrics.noop()),
            System::nanoTime);
    delivery = DeliveryServer.start(config.getDispatch(), service, listenerCredentials);
    service.replicas(ReplicaAssignor.self(delivery.advertisedAddress(), 1));
  }

  @AfterEach
  void stop() throws InterruptedException {
    for (ManagedChannel channel : channels) {
      channel.shutdownNow();
    }
    if (delivery != null) {
      delivery.stopAccepting();
      service.shutdown(Duration.ofSeconds(5));
      delivery.close();
    }
    for (ManagedChannel channel : channels) {
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private ManagedChannel channel(SslContext context) {
    // By address, as a sidecar connects to a replica: there is no host name to verify.
    NettyChannelBuilder builder = NettyChannelBuilder.forAddress("127.0.0.1", delivery.port());
    if (context == null) {
      builder.usePlaintext();
    } else {
      builder.sslContext(context);
    }
    ManagedChannel channel = builder.build();
    channels.add(channel);
    return channel;
  }

  /** A connection as a sidecar with these credentials makes it, accepting these dispatchers. */
  private ManagedChannel channel(FileCredentialSource credentials, String... dispatchers)
      throws Exception {
    return channel(
        ReloadingSslContext.forDispatcher(
            new TlsBinding(credentials, new PeerPolicy(Set.of(dispatchers), ""))));
  }

  private ManagedChannel channel(TestPki.Issued certificate) throws Exception {
    return channel(source(certificate, ca), DISPATCHER_ID);
  }

  /** A sidecar's end of one Deliver stream. */
  private static final class Sidecar {
    final BlockingQueue<DeliverResponse> responses = new LinkedBlockingQueue<>();
    final CompletableFuture<Status> closed = new CompletableFuture<>();
    StreamObserver<DeliverRequest> requests;

    DeliverResponse next() throws InterruptedException {
      DeliverResponse response = responses.poll(10, TimeUnit.SECONDS);
      assertThat(response).as("a response within 10s; the stream: %s", closed).isNotNull();
      return response;
    }

    void accepted() throws InterruptedException {
      DeliverResponse response = next();
      assertThat(response.hasAccepted()).as("%s", response).isTrue();
    }

    Status status() throws Exception {
      return closed.get(10, TimeUnit.SECONDS);
    }
  }

  private static Sidecar declare(ManagedChannel channel, String group, String topic) {
    Sidecar sidecar = new Sidecar();
    sidecar.requests =
        DeliveryServiceGrpc.newStub(channel)
            .deliver(
                new StreamObserver<DeliverResponse>() {
                  @Override
                  public void onNext(DeliverResponse response) {
                    sidecar.responses.add(response);
                  }

                  @Override
                  public void onError(Throwable t) {
                    sidecar.closed.complete(Status.fromThrowable(t));
                  }

                  @Override
                  public void onCompleted() {
                    sidecar.closed.complete(Status.OK);
                  }
                });
    sidecar.requests.onNext(
        DeliverRequest.newBuilder()
            .setDeclare(
                Declare.newBuilder()
                    .setCluster("main")
                    .setGroupId(group)
                    .setTopic(topic)
                    .setSettings(Settings.newBuilder().setMaxProcessingMs(60_000)))
            .build());
    return sidecar;
  }

  /**
   * A handshake that failed is no answer from the dispatcher: what gRPC reports is the connection,
   * as UNAVAILABLE, whichever end refused and for whatever reason. That is the status a sidecar
   * opens the stream again for, where UNAUTHENTICATED and PERMISSION_DENIED, which only a
   * dispatcher that was reached can answer, end the app's stream.
   */
  private static void assertUnavailable(Sidecar sidecar) throws Exception {
    Status status = sidecar.status();
    assertThat(status.getCode()).as("%s", status).isEqualTo(Status.Code.UNAVAILABLE);
    // Seen in the test's output, for whoever wonders what a sidecar's log will say.
    System.out.println("handshake refused: " + status + " cause " + status.getCause());
  }

  // -------------------------------------------------------------------------------------------
  // The policy
  // -------------------------------------------------------------------------------------------

  @Test
  void anIdentityThePolicyAllowsIsAccepted() throws Exception {
    start(dispatcher, ca, policy());
    declare(channel(billing), "billing", "orders").accepted();
  }

  @Test
  void whatThePolicyDoesNotAllowIsRefusedByNameAndInTheDeclarationsOwnWords() throws Exception {
    start(dispatcher, ca, policy());
    ManagedChannel channel = channel(billing);

    // Another identity's topic and group.
    Status status = declare(channel, "audit", "refunds").status();
    assertThat(status.getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
    assertThat(status.getDescription())
        .isEqualTo(
            BILLING + " is not allowed to consume topic refunds in group audit on cluster main");

    // Its own topic in another group, and a topic nobody has.
    assertThat(declare(channel, "reporting", "orders").status().getCode())
        .isEqualTo(Status.Code.PERMISSION_DENIED);
    status = declare(channel, "billing", "payments").status();
    assertThat(status.getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
    assertThat(status.getDescription()).doesNotContain(AUDIT, "refunds", "audit");
    assertThat(consumers).isEmpty();

    // On the same connection, what it may have.
    declare(channel, "billing", "orders").accepted();
  }

  @Test
  void theIdentityIsThatOfTheConnection() throws Exception {
    start(dispatcher, ca, policy());
    declare(channel(audit), "audit", "refunds").accepted();
    Status status = declare(channel(audit), "billing", "orders").status();
    assertThat(status.getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
    assertThat(status.getDescription()).startsWith(AUDIT + " is not allowed");
  }

  @Test
  void withTlsAndNoRuleEveryDeclarationIsRefused() throws Exception {
    start(dispatcher, ca, List.of());
    assertThat(service.refusesEverything()).isTrue();
    assertThat(declare(channel(billing), "billing", "orders").status().getCode())
        .isEqualTo(Status.Code.PERMISSION_DENIED);
  }

  @Test
  void aCertificateWithoutSpiffeIdIsUnauthenticated() throws Exception {
    // It verifies against the authority, so the handshake passes; it names nobody.
    start(dispatcher, ca, policy());
    Status status = declare(channel(plain), "billing", "orders").status();
    assertThat(status.getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
    assertThat(status.getDescription()).startsWith("no SPIFFE ID to authorize: ");
    assertThat(consumers).isEmpty();
  }

  // -------------------------------------------------------------------------------------------
  // Handshakes that fail
  // -------------------------------------------------------------------------------------------

  @Test
  void aSidecarOfAnotherAuthorityNeverReachesTheService() throws Exception {
    start(dispatcher, ca, policy());
    assertUnavailable(declare(channel(source(stranger, ca), DISPATCHER_ID), "billing", "orders"));
  }

  @Test
  void aSidecarWithoutCertificateNeverReachesTheService() throws Exception {
    // A dispatcher certificate that also names a host, so that this client, which verifies the
    // usual way, has nothing to object to: the refusal is the dispatcher's.
    start(namedDispatcher, ca, policy());
    SslContext anonymous =
        SslContextBuilder.forClient()
            .sslProvider(SslProvider.JDK)
            .trustManager(new ByteArrayInputStream(ca.getBytes(StandardCharsets.US_ASCII)))
            .protocols("TLSv1.3")
            .applicationProtocolConfig(
                new ApplicationProtocolConfig(
                    ApplicationProtocolConfig.Protocol.ALPN,
                    ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                    ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                    "h2"))
            .build();
    ManagedChannel channel =
        NettyChannelBuilder.forAddress("127.0.0.1", delivery.port())
            .sslContext(anonymous)
            .overrideAuthority("localhost")
            .build();
    channels.add(channel);
    assertUnavailable(declare(channel, "billing", "orders"));
    assertThat(consumers).isEmpty();
  }

  @Test
  void aPlaintextSidecarNeverReachesAListenerWithTls() throws Exception {
    start(dispatcher, ca, policy());
    assertUnavailable(declare(channel((SslContext) null), "billing", "orders"));
  }

  @Test
  void aDispatcherWithAnUnexpectedIdentityIsRefusedByTheSidecar() throws Exception {
    // Of the right authority: a workload that is not the dispatcher, answering at its address.
    start(billing, ca, policy());
    assertUnavailable(declare(channel(audit), "audit", "refunds"));
  }

  @Test
  void aDispatcherOfAnotherAuthorityIsRefusedByTheSidecar() throws Exception {
    // It accepts the sidecar's authority, so only the sidecar can object.
    start(strangeDispatcher, ca, policy());
    assertUnavailable(declare(channel(billing), "billing", "orders"));
  }

  // -------------------------------------------------------------------------------------------
  // Rotation
  // -------------------------------------------------------------------------------------------

  @Test
  void credentialsRotatedOnBothEndsLeaveOpenStreamsAndServeNewConnections() throws Exception {
    start(dispatcher, ca, policy());
    // The sidecar is billing, and becomes audit: two identities, so that the policy shows which
    // certificate a connection presented. The dispatcher changes its ID for the same reason.
    FileCredentialSource credentials = source(billing, ca);
    ManagedChannel first = channel(credentials, DISPATCHER_ID);
    Sidecar open = declare(first, "billing", "orders");
    open.accepted();
    assertThat(declare(first, "audit", "refunds").status().getCode())
        .isEqualTo(Status.Code.PERMISSION_DENIED);

    rotate(credentials, audit, ca);
    rotate(listenerCredentials, nextDispatcher, ca);

    // The stream that was open is open, and the connection under it is the one it had: a new
    // stream on it is still billing's, to a dispatcher that is still the first.
    assertThat(open.closed).isNotDone();
    assertThat(declare(first, "audit", "refunds").status().getCode())
        .isEqualTo(Status.Code.PERMISSION_DENIED);
    MockConsumer<byte[], byte[]> kafka = consumers.get(0);
    kafka.schedulePollTask(
        () -> {
          kafka.updateBeginningOffsets(Map.of(ORDERS_0, 0L));
          kafka.updateEndOffsets(Map.of(ORDERS_0, 0L));
          kafka.rebalance(List.of(ORDERS_0));
        });
    kafka.schedulePollTask(
        () ->
            kafka.addRecord(
                new ConsumerRecord<>("orders", 0, 0, null, "v".getBytes(StandardCharsets.UTF_8))));
    open.requests.onNext(
        DeliverRequest.newBuilder().setCredits(Credits.newBuilder().setRecords(1)).build());
    DeliverResponse delivered = open.next();
    assertThat(delivered.getRecords().getRecordsCount()).as("%s", delivered).isEqualTo(1);

    // A new connection with the same sources: the sidecar is audit now, and the dispatcher's new
    // identity is the one presented, which a sidecar that accepts only the old one refuses.
    assertUnavailable(declare(channel(credentials, DISPATCHER_ID), "audit", "refunds"));
    ManagedChannel second = channel(credentials, NEXT_DISPATCHER_ID);
    declare(second, "audit", "refunds").accepted();
    Status status = declare(second, "billing", "orders").status();
    assertThat(status.getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
    assertThat(status.getDescription()).startsWith(AUDIT + " is not allowed");
    assertThat(open.closed).isNotDone();
  }

  // -------------------------------------------------------------------------------------------
  // Without TLS
  // -------------------------------------------------------------------------------------------

  @Test
  void withoutTlsThePortIsPlaintextAndOpen() throws Exception {
    start(Dispatch.newBuilder());
    assertThat(service.enforcesPolicy()).isFalse();
    declare(channel((SslContext) null), "anyone", "orders").accepted();
    // A sidecar that insists on TLS does not talk to it.
    Status status = declare(channel(billing), "billing", "orders").status();
    assertThat(status.getCode()).isEqualTo(Status.Code.UNAVAILABLE);
  }

  @Test
  void aPolicyWithoutTlsIsNeverServed() {
    ServerConfig config =
        ServerConfig.newBuilder()
            .putClusters("main", Cluster.getDefaultInstance())
            .setDispatch(Dispatch.newBuilder().addAllPolicy(policy()))
            .build();
    assertThatThrownBy(() -> new DispatchService(config, TlsSources.none(), ServerMetrics.noop()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("dispatch.policy needs dispatch.tls");
  }

  @Test
  void aListenerNeverServesLessSecurelyThanItsConfigurationSays() throws Exception {
    // dispatch.tls is set and no credentials were loaded for it: not a plaintext port.
    listenerCredentials = null;
    assertThatThrownBy(
            () ->
                start(
                    Dispatch.newBuilder()
                        .setTls(
                            ListenerTls.newBuilder()
                                .setFiles(FileSource.newBuilder().setDir("/unused")))))
        .isInstanceOf(IllegalStateException.class);
    delivery = null;
  }
}
