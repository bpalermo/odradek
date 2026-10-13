package io.github.bpalermo.odradek.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.bpalermo.odradek.delivery.v1.Declare;
import io.github.bpalermo.odradek.delivery.v1.DeliverRequest;
import io.github.bpalermo.odradek.delivery.v1.DeliverResponse;
import io.github.bpalermo.odradek.delivery.v1.DeliveryServiceGrpc;
import io.github.bpalermo.odradek.delivery.v1.Settings;
import io.github.bpalermo.odradek.kafka.v1.Ack;
import io.github.bpalermo.odradek.kafka.v1.ConsumeRequest;
import io.github.bpalermo.odradek.kafka.v1.ConsumeResponse;
import io.github.bpalermo.odradek.kafka.v1.ConsumedRecord;
import io.github.bpalermo.odradek.kafka.v1.Credits;
import io.github.bpalermo.odradek.kafka.v1.OdradekServiceGrpc;
import io.github.bpalermo.odradek.kafka.v1.RecordBatch;
import io.github.bpalermo.odradek.kafka.v1.Subscribe;
import io.github.bpalermo.odradek.server.admin.HealthService;
import io.github.bpalermo.odradek.server.admin.HealthService.ServingStatus;
import io.github.bpalermo.odradek.server.tls.TestPki;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Mutual TLS and the policy on the delivery port, end to end (ADR 6, step 5): a real broker, a
 * dispatcher whose listener has credentials and a policy, and sidecars that connect to it with
 * credentials of their own. Everything is driven through the app's API on each sidecar's Unix
 * socket, except where the point is a client that is no sidecar. The credentials are files, made
 * here by a throwaway authority; the broker is a plaintext one, which has nothing to do with it.
 */
class DeliveryTlsE2eTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(60);

  /** Longer than the server's poll of a credentials directory (5 s). */
  private static final Duration ROTATION = Duration.ofSeconds(8);

  private static final String DISPATCHER_ID = "spiffe://e2e.test/ns/kafka/sa/dispatcher";
  private static final String NEXT_DISPATCHER_ID = "spiffe://e2e.test/ns/kafka/sa/dispatcher-2";
  private static final String BILLING = "spiffe://e2e.test/ns/billing/sa/billing";
  private static final String AUDIT = "spiffe://e2e.test/ns/audit/sa/audit";

  /**
   * Billing may consume {@code topic} and {@code third} in the group; audit may consume {@code
   * second} in it.
   */
  private static final String DISPATCHER =
      """
      clusters:
        a:
          required: true
          properties:
            bootstrap.servers: %s
          consumerProperties:
            session.timeout.ms: 10000
            heartbeat.interval.ms: 500
      consume:
        commitInterval: 0.2s
        revokeDeadline: 2s
      dispatch:
        port: 0
        idleGrace: 60s
        coordination:
          groupId: odradek-e2e-delivery-replicas-%s
        tls:
          files:
            dir: %s
        policy:
          - spiffeIds: ["spiffe://e2e.test/ns/billing/sa/billing"]
            cluster: a
            topics: ["%s", "%s"]
            groups: ["%s"]
            deadLetterTopics: ["odradek-e2e-delivery-dlq-*"]
          - spiffeIds: ["spiffe://e2e.test/ns/audit/sa/audit"]
            cluster: a
            topics: ["%s"]
            groups: ["%s"]
      """;

  private static Path pki;
  private static String ca;
  private static TestPki.Issued dispatcherCertificate;
  private static TestPki.Issued nextDispatcherCertificate;
  private static TestPki.Issued billing;
  private static TestPki.Issued audit;

  /** Of the right authority, with no SPIFFE ID. */
  private static TestPki.Issued plain;

  /** Billing's identity, signed by an authority the dispatcher does not trust. */
  private static TestPki.Issued stranger;

  private final String id = UUID.randomUUID().toString();
  private final String topic = "odradek-e2e-delivery-" + id;
  private final String second = "odradek-e2e-delivery-second-" + id;
  private final String third = "odradek-e2e-delivery-third-" + id;
  private final String group = "odradek-e2e-delivery-" + id;
  private final List<String> topics = new ArrayList<>();
  private final List<ServerHarness> sidecars = new ArrayList<>();
  private final List<ManagedChannel> channels = new ArrayList<>();
  private final List<DispatcherHarness> dispatchers = new ArrayList<>();
  private final TcpRelay service;

  DeliveryTlsE2eTest() throws Exception {
    service = new TcpRelay();
  }

  /** Further top-level sections of the sidecars started next, as YAML. */
  private String sidecarSections = "";

  /** Further lines of every consumer entry of the sidecars started next. */
  private List<String> entryLines = List.of();

  private Path credentials;
  private DispatcherHarness dispatcher;
  private KafkaProducer<String, String> producer;
  private Admin admin;

  @BeforeAll
  static void issue() throws Exception {
    pki = Files.createTempDirectory(Path.of("/tmp"), "odradek-e2e-delivery-pki");
    TestPki authority = new TestPki(pki, "ca");
    TestPki other = new TestPki(pki, "other");
    ca = authority.caPem();
    dispatcherCertificate = authority.issue("dispatcher", "uri:" + DISPATCHER_ID);
    nextDispatcherCertificate = authority.issue("dispatcher-2", "uri:" + NEXT_DISPATCHER_ID);
    billing = authority.issue("billing", "uri:" + BILLING);
    audit = authority.issue("audit", "uri:" + AUDIT);
    plain = authority.issue("plain", "dns:plain.e2e.test");
    stranger = other.issue("stranger", "uri:" + BILLING);
  }

  @AfterAll
  static void removePki() throws Exception {
    delete(pki);
  }

  private static void delete(Path dir) throws Exception {
    try (var files = Files.walk(dir)) {
      for (Path file : files.sorted(Comparator.reverseOrder()).toList()) {
        Files.deleteIfExists(file);
      }
    }
  }

  /** One record as an app received it. */
  private record Got(String topic, int partition, long epoch, long offset, String value) {}

  /** A scripted app: one raw Consume stream on a sidecar's socket. */
  private final class App {
    final List<Got> received = new CopyOnWriteArrayList<>();
    final List<String> lost = new CopyOnWriteArrayList<>();
    final CompletableFuture<Status> closed = new CompletableFuture<>();
    private final StreamObserver<ConsumeRequest> requests;

    /** What it does with each record, on the stream's thread. By default: holds it. */
    volatile Consumer<Got> onRecord = record -> {};

    App(ServerHarness sidecar, String name) {
      requests =
          OdradekServiceGrpc.newStub(sidecar.channel)
              .consume(
                  new StreamObserver<ConsumeResponse>() {
                    @Override
                    public void onNext(ConsumeResponse response) {
                      switch (response.getMsgCase()) {
                        case RECORDS -> records(response.getRecords());
                        case LOST ->
                            response
                                .getLost()
                                .getPartitionsList()
                                .forEach(p -> lost.add(p.getTopic() + "/" + p.getPartition()));
                        default -> {}
                      }
                    }

                    @Override
                    public void onError(Throwable t) {
                      closed.complete(Status.fromThrowable(t));
                    }

                    @Override
                    public void onCompleted() {
                      closed.complete(Status.OK);
                    }
                  });
      send(
          ConsumeRequest.newBuilder()
              .setSubscribe(Subscribe.newBuilder().addTopics(name).setMaxProcessingMs(120_000)));
    }

    private void records(RecordBatch batch) {
      for (ConsumedRecord record : batch.getRecordsList()) {
        Got one =
            new Got(
                batch.getTopic(),
                batch.getPartition(),
                batch.getAssignmentEpoch(),
                record.getOffset(),
                record.getValue().toStringUtf8());
        received.add(one);
        onRecord.accept(one);
      }
    }

    /** Requests are written from the stream's thread and from the test's. */
    private synchronized void send(ConsumeRequest.Builder request) {
      requests.onNext(request.build());
    }

    void credits(int records) {
      send(ConsumeRequest.newBuilder().setCredits(Credits.newBuilder().setRecords(records)));
    }

    void ack(Got record) {
      send(
          ConsumeRequest.newBuilder()
              .setAck(
                  Ack.newBuilder()
                      .setTopic(record.topic())
                      .setPartition(record.partition())
                      .setAssignmentEpoch(record.epoch())
                      .setFirstOffset(record.offset())
                      .setLastOffset(record.offset())));
    }

    /** Acknowledges every record as it arrives and asks for one more. */
    App working() {
      onRecord =
          record -> {
            ack(record);
            credits(1);
          };
      return this;
    }

    /** The app is done: the stream is closed in good order. */
    void close() {
      onRecord = record -> {};
      requests.onCompleted();
    }

    Status status() throws Exception {
      return closed.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
    }

    Set<String> values() {
      Set<String> values = new TreeSet<>();
      received.forEach(record -> values.add(record.value()));
      return values;
    }
  }

  @BeforeEach
  void start() throws Exception {
    credentials = Files.createTempDirectory(Path.of("/tmp"), "odradek-e2e-delivery");
    admin =
        Admin.create(
            Map.<String, Object>of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, ServerHarness.KAFKA_A));
    producer =
        new KafkaProducer<>(
            Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                ServerHarness.KAFKA_A,
                ProducerConfig.ACKS_CONFIG,
                "all"),
            new StringSerializer(),
            new StringSerializer());
  }

  @AfterEach
  void stop() throws Exception {
    for (ManagedChannel channel : channels) {
      channel.shutdownNow();
    }
    for (ServerHarness sidecar : sidecars) {
      sidecar.stop();
    }
    for (DispatcherHarness started : dispatchers) {
      started.stop();
    }
    service.close();
    if (producer != null) {
      producer.close(Duration.ofSeconds(5));
    }
    if (admin != null) {
      admin.close(Duration.ofSeconds(5));
    }
    if (!topics.isEmpty()) {
      ServerHarness.deleteTopics(ServerHarness.KAFKA_A, topics);
    }
    delete(credentials);
  }

  // -------------------------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------------------------

  private void createTopic(String name) throws Exception {
    createTopic(name, 1);
  }

  private void createTopic(String name, int partitions) throws Exception {
    ServerHarness.createTopic(ServerHarness.KAFKA_A, name, partitions);
    topics.add(name);
  }

  /** Writes credentials where a server reads them: the first time, or as a rotation. */
  private Path install(String name, TestPki.Issued certificate, String authorities)
      throws Exception {
    Path dir = credentials.resolve(name);
    TestPki.write(dir, certificate, authorities);
    return dir;
  }

  /** Starts the dispatcher with this certificate; it trusts the sidecars' authority. */
  private int startDispatcher(TestPki.Issued certificate) throws Exception {
    dispatcher = replica("dispatcher", certificate, topic);
    return dispatcher.port();
  }

  /**
   * Starts a server that joins the coordination group of this test's dispatcher: a replica of it,
   * or something that only says it is.
   *
   * @param name of its credentials directory
   * @param allowed the topic its policy allows billing, besides {@code third}
   */
  private DispatcherHarness replica(String name, TestPki.Issued certificate, String allowed)
      throws Exception {
    DispatcherHarness started =
        DispatcherHarness.start(
            DISPATCHER,
            ServerHarness.KAFKA_A,
            id,
            install(name, certificate, ca),
            allowed,
            third,
            group,
            second,
            group);
    dispatchers.add(started);
    return started;
  }

  /**
   * Starts a sidecar that receives these topics from the dispatcher.
   *
   * @param name of its credentials directory; null for a sidecar without {@code dispatcher.tls}
   * @param accepted the SPIFFE IDs it accepts of the dispatcher
   */
  private ServerHarness sidecar(
      String name, TestPki.Issued certificate, List<String> accepted, String... names)
      throws Exception {
    return sidecarAt(dispatcher.port(), name, certificate, accepted, names);
  }

  /** As above, for a sidecar that knows the dispatcher by the address at this local port. */
  private ServerHarness sidecarAt(
      int port, String name, TestPki.Issued certificate, List<String> accepted, String... names)
      throws Exception {
    StringBuilder yaml =
        new StringBuilder()
            .append("listener:\n  socketPath: %s/kafka.sock\n")
            .append("dispatcher:\n  address: localhost:")
            .append(port)
            .append('\n');
    if (name != null) {
      yaml.append("  tls:\n    files:\n      dir: ")
          .append(install(name, certificate, ca))
          .append("\n    acceptedSpiffeIds:\n");
      accepted.forEach(spiffeId -> yaml.append("      - \"").append(spiffeId).append("\"\n"));
    }
    yaml.append(sidecarSections).append("consumers:\n");
    for (String consumed : names) {
      yaml.append("  ")
          .append(consumed)
          .append(":\n    source: CONSUMER_SOURCE_DISPATCHER\n    cluster: a\n    groupId: ")
          .append(group)
          .append('\n');
      entryLines.forEach(line -> yaml.append("    ").append(line).append('\n'));
    }
    ServerHarness sidecar = ServerHarness.start(yaml.toString());
    sidecars.add(sidecar);
    return sidecar;
  }

  private Set<String> produce(String name, String prefix, int count) throws Exception {
    List<Future<RecordMetadata>> sends = new ArrayList<>();
    Set<String> values = new TreeSet<>();
    for (int i = 0; i < count; i++) {
      String value = prefix + "-" + i;
      values.add(value);
      sends.add(producer.send(new ProducerRecord<>(name, 0, "key-" + value, value)));
    }
    for (Future<RecordMetadata> send : sends) {
      send.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
    }
    return values;
  }

  private static void await(String what, BooleanSupplier condition) throws InterruptedException {
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (!condition.getAsBoolean()) {
      assertThat(System.nanoTime()).as("%s within %s", what, TIMEOUT).isLessThan(deadline);
      Thread.sleep(20);
    }
  }

  /** Writes {@code count} records with keys of their own, spread over a topic's partitions. */
  private Set<String> produce(String name, String prefix, int count, int partitions)
      throws Exception {
    List<Future<RecordMetadata>> sends = new ArrayList<>();
    Set<String> values = new TreeSet<>();
    for (int i = 0; i < count; i++) {
      String value = prefix + "-" + i;
      values.add(value);
      sends.add(producer.send(new ProducerRecord<>(name, i % partitions, "key-" + value, value)));
    }
    for (Future<RecordMetadata> send : sends) {
      send.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
    }
    return values;
  }

  /**
   * How many partitions each member of the consumed group has, as the brokers know it; empty while
   * the group is not stable. A member is the consumer of one replica, which joins the group only
   * when a sidecar it accepted declares the group to it.
   */
  private List<Integer> members() {
    try {
      ConsumerGroupDescription description =
          admin
              .describeConsumerGroups(List.of(group))
              .describedGroups()
              .get(group)
              .get(30, TimeUnit.SECONDS);
      if (description.groupState() != GroupState.STABLE) {
        return List.of();
      }
      return description.members().stream()
          .map(member -> member.assignment().topicPartitions().size())
          .toList();
    } catch (Exception e) {
      return List.of();
    }
  }

  /** Waits until this many replicas consume the group and have these partitions between them. */
  private void awaitMembers(int count, int partitions) throws InterruptedException {
    await(
        count + " replicas consume the group",
        () -> {
          List<Integer> members = members();
          return members.size() == count
              && members.stream().noneMatch(owned -> owned == 0)
              && members.stream().mapToInt(Integer::intValue).sum() == partitions;
        });
  }

  /** What the group has committed for the one partition of a topic; -1 for nothing. */
  private long committed(String name) {
    try {
      var offset =
          admin
              .listConsumerGroupOffsets(group)
              .partitionsToOffsetAndMetadata()
              .get(30, TimeUnit.SECONDS)
              .get(new TopicPartition(name, 0));
      return offset == null ? -1 : offset.offset();
    } catch (Exception e) {
      throw new AssertionError("could not read the offsets of group " + group, e);
    }
  }

  /** Waits until the group has committed the end of a topic. */
  private void awaitCommitted(String name) throws Exception {
    Map<TopicPartition, OffsetSpec> request = new HashMap<>();
    request.put(new TopicPartition(name, 0), OffsetSpec.latest());
    Map<String, Long> ends = new TreeMap<>();
    admin
        .listOffsets(request)
        .all()
        .get(30, TimeUnit.SECONDS)
        .forEach((partition, end) -> ends.put(name, end.offset()));
    await(
        "everything of " + name + " is committed (" + ends + ")",
        () -> ends.get(name).equals(committed(name)));
  }

  /** Lets time pass in which something must not happen: a refused sidecar's retries included. */
  private static void pause() throws InterruptedException {
    Thread.sleep(4_000);
  }

  /** The status a scripted sidecar's declaration ends with, on this connection. */
  private Status declare(ManagedChannel channel) throws Exception {
    channels.add(channel);
    CompletableFuture<Status> closed = new CompletableFuture<>();
    StreamObserver<DeliverRequest> requests =
        DeliveryServiceGrpc.newStub(channel)
            .deliver(
                new StreamObserver<DeliverResponse>() {
                  @Override
                  public void onNext(DeliverResponse response) {
                    closed.complete(Status.OK.withDescription("answered: " + response));
                  }

                  @Override
                  public void onError(Throwable t) {
                    closed.complete(Status.fromThrowable(t));
                  }

                  @Override
                  public void onCompleted() {
                    closed.complete(Status.OK);
                  }
                });
    requests.onNext(
        DeliverRequest.newBuilder()
            .setDeclare(
                Declare.newBuilder()
                    .setCluster("a")
                    .setGroupId(group)
                    .setTopic(topic)
                    .setSettings(Settings.newBuilder().setMaxProcessingMs(60_000)))
            .build());
    return closed.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
  }

  // -------------------------------------------------------------------------------------------
  // The policy
  // -------------------------------------------------------------------------------------------

  @Test
  void aSidecarThePolicyAllowsConsumesOverMutualTls() throws Exception {
    createTopic(topic);
    startDispatcher(dispatcherCertificate);
    App app = new App(sidecar("billing", billing, List.of(DISPATCHER_ID), topic), topic).working();
    app.credits(4);
    Set<String> produced = produce(topic, "v", 50);
    await("everything is received", () -> app.values().equals(produced));
    awaitCommitted(topic);
    assertThat(app.received).hasSize(50);
    assertThat(app.closed).isNotDone();
  }

  @Test
  void aTopicThePolicyDoesNotAllowEndsTheAppsStreamWithPermissionDenied() throws Exception {
    createTopic(topic);
    createTopic(second);
    startDispatcher(dispatcherCertificate);
    // Billing's sidecar asks for the topic that is audit's.
    ServerHarness sidecar = sidecar("billing", billing, List.of(DISPATCHER_ID), topic, second);
    produce(second, "secret", 5);
    App denied = new App(sidecar, second);
    denied.credits(10);
    Status status = denied.status();
    assertThat(status.getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
    assertThat(status.getDescription())
        .contains("refused topic " + second)
        .contains(
            BILLING
                + " is not allowed to consume topic "
                + second
                + " in group "
                + group
                + " on cluster a")
        // Nothing of who may: the rule that names audit is none of billing's business.
        .doesNotContain(AUDIT);
    assertThat(denied.received).isEmpty();
    assertThat(committed(second)).as("nothing of the topic was consumed").isEqualTo(-1);

    // What it may have, it gets, from the same sidecar.
    App allowed = new App(sidecar, topic).working();
    allowed.credits(4);
    Set<String> produced = produce(topic, "v", 10);
    await("the allowed topic is received", () -> allowed.values().equals(produced));
  }

  @Test
  void aDeadLetterTopicIsThePolicysToAllowAndAnAllowedOneIsWrittenTo() throws Exception {
    String deadLetters = "odradek-e2e-delivery-dlq-" + id;
    String elsewhere = "odradek-e2e-delivery-elsewhere-" + id;
    createTopic(topic);
    createTopic(deadLetters);
    startDispatcher(dispatcherCertificate);
    // A codec that fails on every value and lets a tombstone through: Kafka's string
    // deserializer gives the server a String where it wants bytes.
    sidecarSections =
        """
        codecs:
          text:
            value:
              serializer: org.apache.kafka.common.serialization.StringSerializer
              deserializer: org.apache.kafka.common.serialization.StringDeserializer
        """;

    // Billing may read the topic, but its rule does not list this dead-letter topic: the
    // dispatcher would write there with its own credentials on billing's word.
    entryLines =
        List.of(
            "codec: text",
            "maxAttempts: 1",
            "onExhausted: ON_EXHAUSTED_DEAD_LETTER",
            "deadLetterTopic: " + elsewhere);
    App denied = new App(sidecar("denied", billing, List.of(DISPATCHER_ID), topic), topic);
    Status status = denied.status();
    assertThat(status.getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
    assertThat(status.getDescription())
        .contains(
            BILLING
                + " is not allowed to consume topic "
                + topic
                + " in group "
                + group
                + " on cluster a with dead-letter topic "
                + elsewhere);

    // One its rule lists is written to.
    entryLines =
        List.of(
            "codec: text",
            "maxAttempts: 1",
            "onExhausted: ON_EXHAUSTED_DEAD_LETTER",
            "deadLetterTopic: " + deadLetters);
    App app = new App(sidecar("billing", billing, List.of(DISPATCHER_ID), topic), topic).working();
    app.credits(4);
    producer
        .send(new ProducerRecord<>(topic, 0, "poison", "cannot be decoded"))
        .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
    producer
        .send(new ProducerRecord<>(topic, 0, "fine", null))
        .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
    await("the record behind it arrives", () -> app.received.size() == 1);
    awaitCommitted(topic);
    var letters = ServerHarness.read(ServerHarness.KAFKA_A, deadLetters, 1, 1);
    assertThat(letters).hasSize(1);
    assertThat(letters.get(0).key()).isEqualTo("poison");
    assertThat(ServerHarness.header(letters.get(0), "odradek-source-group")).isEqualTo(group);
    assertThat(ServerHarness.header(letters.get(0), "odradek-failure")).isEqualTo("rejected");
    // And nothing was written where it was not allowed: the topic was never made.
    assertThat(admin.listTopics().names().get(30, TimeUnit.SECONDS)).doesNotContain(elsewhere);
    assertThat(app.closed).isNotDone();
  }

  @Test
  void aCertificateWithoutSpiffeIdEndsTheAppsStreamWithUnauthenticated() throws Exception {
    createTopic(topic);
    startDispatcher(dispatcherCertificate);
    App app = new App(sidecar("plain", plain, List.of(DISPATCHER_ID), topic), topic);
    Status status = app.status();
    assertThat(status.getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
    assertThat(status.getDescription()).contains("no SPIFFE ID to authorize");
  }

  // -------------------------------------------------------------------------------------------
  // Who is refused before a word is said
  // -------------------------------------------------------------------------------------------

  @Test
  void aClientWithoutCertificateIsRefused() throws Exception {
    createTopic(topic);
    int port = startDispatcher(dispatcherCertificate);
    // It accepts any server, so that only the dispatcher can object.
    SslContext anonymous =
        SslContextBuilder.forClient()
            .sslProvider(SslProvider.JDK)
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .protocols("TLSv1.3")
            .applicationProtocolConfig(
                new ApplicationProtocolConfig(
                    ApplicationProtocolConfig.Protocol.ALPN,
                    ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                    ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                    "h2"))
            .build();
    Status status =
        declare(NettyChannelBuilder.forAddress("localhost", port).sslContext(anonymous).build());
    assertThat(status.getCode()).as("%s", status).isEqualTo(Status.Code.UNAVAILABLE);

    // And one that does not speak TLS at all.
    status = declare(NettyChannelBuilder.forAddress("localhost", port).usePlaintext().build());
    assertThat(status.getCode()).as("%s", status).isEqualTo(Status.Code.UNAVAILABLE);
    assertThat(committed(topic)).isEqualTo(-1);
  }

  @Test
  void aSidecarWithoutTlsReceivesNothingFromADispatcherWithTls() throws Exception {
    createTopic(topic);
    startDispatcher(dispatcherCertificate);
    App app = new App(sidecar(null, null, List.of(), topic), topic).working();
    app.credits(10);
    produce(topic, "v", 5);
    pause();
    // Not an answer from the dispatcher, so nothing to end the app's stream with: it stays open
    // and the sidecar keeps trying, as it does while a dispatcher is away.
    assertThat(app.received).isEmpty();
    assertThat(app.closed).isNotDone();
    assertThat(committed(topic)).isEqualTo(-1);
  }

  @Test
  void aSidecarOfAnotherAuthorityReceivesNothing() throws Exception {
    createTopic(topic);
    startDispatcher(dispatcherCertificate);
    // Billing's very identity, vouched for by an authority the dispatcher does not know.
    App impostor =
        new App(sidecar("stranger", stranger, List.of(DISPATCHER_ID), topic), topic).working();
    impostor.credits(10);
    Set<String> produced = produce(topic, "v", 5);
    pause();
    assertThat(impostor.received).isEmpty();
    assertThat(impostor.closed).isNotDone();
    assertThat(committed(topic)).isEqualTo(-1);

    // The records are there for whoever is billing.
    App app = new App(sidecar("billing", billing, List.of(DISPATCHER_ID), topic), topic).working();
    app.credits(10);
    await("the real one receives them", () -> app.values().equals(produced));
    assertThat(impostor.received).isEmpty();
  }

  @Test
  void aDispatcherWithAnUnexpectedIdentityIsRefusedByTheSidecar() throws Exception {
    createTopic(topic);
    // What answers at the dispatcher's address has a certificate of the right authority, and is
    // not the dispatcher: here, it is one whose identity the sidecar was not told to accept.
    startDispatcher(nextDispatcherCertificate);
    App app = new App(sidecar("billing", billing, List.of(DISPATCHER_ID), topic), topic).working();
    app.credits(10);
    Set<String> produced = produce(topic, "v", 5);
    pause();
    assertThat(app.received).isEmpty();
    assertThat(app.closed).isNotDone();
    // It declared nothing to it: the dispatcher never learned what the sidecar consumes.
    assertThat(committed(topic)).isEqualTo(-1);

    // A sidecar told to accept that identity talks to it.
    App other =
        new App(sidecar("billing-2", billing, List.of(NEXT_DISPATCHER_ID), topic), topic).working();
    other.credits(10);
    await("the sidecar that accepts it receives", () -> other.values().equals(produced));
    assertThat(app.received).isEmpty();
  }

  // -------------------------------------------------------------------------------------------
  // Rotation
  // -------------------------------------------------------------------------------------------

  @Test
  void credentialsRotatedOnBothSidesKeepTheOpenStreamAndAreUsedByNewConnections() throws Exception {
    createTopic(topic);
    createTopic(second);
    createTopic(third);
    startDispatcher(dispatcherCertificate);
    // The sidecar is billing and becomes audit; the dispatcher changes its ID. Two identities on
    // each side, so that what a connection presented shows in what it is allowed and in who
    // accepts it. (A real rotation keeps the identity; nothing here depends on its changing.)
    ServerHarness sidecar =
        sidecar("sidecar", billing, List.of(DISPATCHER_ID, NEXT_DISPATCHER_ID), topic, second);
    // Told to accept only the identity the dispatcher does not have yet.
    ServerHarness waiting = sidecar("waiting", billing, List.of(NEXT_DISPATCHER_ID), third);

    // The app holds its first record through the rotation. If its Deliver stream were cut, the
    // record would be reported lost and delivered a second time.
    App app = new App(sidecar, topic);
    app.credits(1);
    produce(topic, "held", 1);
    await("the first record arrives", () -> app.received.size() == 1);
    Got held = app.received.get(0);

    // Before: as billing, the second topic is not this sidecar's; and the sidecar that waits for
    // the dispatcher's next identity does not talk to this one.
    assertThat(new App(sidecar, second).status().getCode())
        .isEqualTo(Status.Code.PERMISSION_DENIED);
    App late = new App(waiting, third).working();
    late.credits(10);
    Set<String> forLate = produce(third, "late", 5);
    pause();
    assertThat(late.received).isEmpty();
    assertThat(late.closed).isNotDone();

    install("sidecar", audit, ca);
    install("dispatcher", nextDispatcherCertificate, ca);
    Thread.sleep(ROTATION.toMillis());

    // The open stream: still there, still billing's, and the record it holds is still its own.
    assertThat(app.closed).isNotDone();
    assertThat(app.lost).isEmpty();
    app.working();
    app.ack(held);
    app.credits(1);
    Set<String> more = produce(topic, "after", 20);
    await("the stream goes on", () -> app.values().containsAll(more));
    awaitCommitted(topic);
    assertThat(app.received).as("nothing was delivered twice").hasSize(21);
    assertThat(app.lost).isEmpty();

    // The streams of one sidecar share its connection to the dispatcher, and a connection is
    // who it was when it was made: while the first stream holds it, the sidecar is still billing.
    assertThat(new App(sidecar, second).status().getCode())
        .isEqualTo(Status.Code.PERMISSION_DENIED);
    assertThat(app.closed).isNotDone();
    app.close();
    assertThat(app.status()).isEqualTo(Status.OK);

    // A new connection of the same sidecar presents the new certificate: it is audit now.
    App asAudit = new App(sidecar, second).working();
    asAudit.credits(10);
    Set<String> forAudit = produce(second, "audit", 5);
    await("the second topic is received as audit", () -> asAudit.values().equals(forAudit));

    // And the dispatcher presents its new one: the sidecar that waited for it is served.
    await("the waiting sidecar is served", () -> late.values().equals(forLate));
  }

  // -------------------------------------------------------------------------------------------
  // The process
  // -------------------------------------------------------------------------------------------

  // -------------------------------------------------------------------------------------------
  // Several replicas
  // -------------------------------------------------------------------------------------------

  @Test
  void replicasAreFoundAndVerifiedOverMutualTlsAndSoIsOneThatReplacesAnother() throws Exception {
    createTopic(topic, 4);
    startDispatcher(dispatcherCertificate);
    DispatcherHarness other = dispatcher.replica();
    dispatchers.add(other);
    service.target(dispatcher.port(), other.port());
    await(
        "both replicas know of each other",
        () -> dispatcher.replicaCount() == 2 && other.replicaCount() == 2);

    // One address, that of the Service. The replica that answers there is verified, and so is
    // the other, which the sidecar connects to at the address the first one gave it: each
    // authorizes the declaration by the sidecar's certificate, and joins the group for it.
    App app =
        new App(sidecarAt(service.port(), "billing", billing, List.of(DISPATCHER_ID), topic), topic)
            .working();
    app.credits(8);
    awaitMembers(2, 4);
    Set<String> produced = produce(topic, "a", 40, 4);
    await("everything is received", () -> app.values().equals(produced));

    // One replica goes and another comes, somewhere else. The sidecar hears of it from the
    // replica it still has, and does not go back to the Service's address.
    int connections = service.accepted();
    other.stopDispatcher();
    awaitMembers(1, 4);
    DispatcherHarness next = dispatcher.replica();
    dispatchers.add(next);
    awaitMembers(2, 4);
    produced.addAll(produce(topic, "b", 40, 4));
    await("everything is received after the change", () -> app.values().equals(produced));
    assertThat(service.accepted())
        .as("connections to the Service's address")
        .isEqualTo(connections);
    assertThat(app.closed).isNotDone();
  }

  @Test
  void anAddressOnTheListThatIsNotTheDispatchersIsConnectedToAndNeverDeclaredTo() throws Exception {
    createTopic(topic);
    startDispatcher(dispatcherCertificate);
    // Whoever can join the coordination group puts an address on the list every sidecar is
    // sent. This one chains to the same authority and is not the dispatcher: another workload
    // of the trust domain.
    DispatcherHarness other = replica("other", nextDispatcherCertificate, topic);
    await("the dispatcher lists the other", () -> dispatcher.replicaCount() == 2);

    // The sidecar accepts the dispatcher's SPIFFE ID and no other. It is sent both addresses,
    // and the handshake with the second fails before a word is said on it.
    App app = new App(sidecar("billing", billing, List.of(DISPATCHER_ID), topic), topic).working();
    app.credits(4);
    Set<String> produced = produce(topic, "v", 20);
    await("everything is received from the dispatcher", () -> app.values().equals(produced));
    awaitCommitted(topic);
    pause();

    // Nothing was declared to the other, so it never joined the group and never read a record.
    assertThat(members()).as("the members of the consumed group").containsExactly(1);
    assertThat(app.closed).as("the app's stream").isNotDone();
    if (DispatcherHarness.packaged()) {
      assertThat(other.log).noneMatch(line -> line.contains("deliver stream"));
      assertThat(dispatcher.log).anyMatch(line -> line.contains("deliver stream"));
    }
  }

  @Test
  void aReplicaWhosePolicyRefusesEndsTheAppsStreamAndTheStatusSaysWhichReplica() throws Exception {
    createTopic(topic);
    startDispatcher(dispatcherCertificate);
    // A replica whose policy does not allow billing the topic, as an old one during a rolling
    // update of the dispatcher that adds the rule.
    DispatcherHarness behind = replica("behind", dispatcherCertificate, second);
    await("the dispatcher lists both", () -> dispatcher.replicaCount() == 2);

    // The replica the sidecar reaches first accepts. The other refuses, and one refusal ends the
    // app's stream: a topic that only some replicas hand over would be consumed in part.
    App app = new App(sidecar("billing", billing, List.of(DISPATCHER_ID), topic), topic);
    app.credits(4);
    Status status = app.status();
    assertThat(status.getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
    assertThat(status.getDescription())
        .contains(":" + behind.port() + " refused topic " + topic)
        .contains(BILLING + " is not allowed to consume topic " + topic);
  }

  /** Needs the packaged server: the in-process harness runs neither Main nor the health service. */
  @Test
  void aDispatcherWithTlsIsReadyAndSaysHowItListens() throws Exception {
    Assumptions.assumeTrue(DispatcherHarness.packaged(), "needs the packaged server");
    createTopic(topic);
    startDispatcher(dispatcherCertificate);
    ManagedChannel probe =
        NettyChannelBuilder.forAddress("localhost", dispatcher.healthPort()).usePlaintext().build();
    channels.add(probe);
    await(
        "the health service answers SERVING",
        () -> {
          try {
            return ClientCalls.blockingUnaryCall(
                    probe,
                    HealthService.CHECK,
                    CallOptions.DEFAULT.withDeadlineAfter(5, TimeUnit.SECONDS),
                    "")
                == ServingStatus.SERVING;
          } catch (RuntimeException e) {
            return false;
          }
        });
    assertThat(dispatcher.log)
        .anyMatch(line -> line.contains("delivery service on port") && line.contains("mutual TLS"))
        .anyMatch(line -> line.contains("dispatch.tls: TLS credentials loaded"))
        .noneMatch(line -> line.contains("DELIVERY PORT IS PLAINTEXT"));
  }
}
