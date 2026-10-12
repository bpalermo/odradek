package io.github.bpalermo.odradek.server.config;

import static org.assertj.core.api.Assertions.assertThat;

import build.buf.protovalidate.ValidationResult;
import build.buf.protovalidate.Validator;
import build.buf.protovalidate.ValidatorFactory;
import build.buf.protovalidate.exceptions.ValidationException;
import com.google.protobuf.Duration;
import io.github.bpalermo.odradek.config.v1.Admin;
import io.github.bpalermo.odradek.config.v1.Cluster;
import io.github.bpalermo.odradek.config.v1.Codec;
import io.github.bpalermo.odradek.config.v1.Consume;
import io.github.bpalermo.odradek.config.v1.ConsumerSource;
import io.github.bpalermo.odradek.config.v1.Contracts;
import io.github.bpalermo.odradek.config.v1.Dispatch;
import io.github.bpalermo.odradek.config.v1.Dispatcher;
import io.github.bpalermo.odradek.config.v1.FileSource;
import io.github.bpalermo.odradek.config.v1.Listener;
import io.github.bpalermo.odradek.config.v1.OnDecodeError;
import io.github.bpalermo.odradek.config.v1.OnExhausted;
import io.github.bpalermo.odradek.config.v1.Ordering;
import io.github.bpalermo.odradek.config.v1.Pipeline;
import io.github.bpalermo.odradek.config.v1.PolicyRule;
import io.github.bpalermo.odradek.config.v1.ProduceTarget;
import io.github.bpalermo.odradek.config.v1.Producer;
import io.github.bpalermo.odradek.config.v1.ServerConfig;
import io.github.bpalermo.odradek.config.v1.SpiffeSource;
import io.github.bpalermo.odradek.config.v1.Tls;
import io.github.bpalermo.odradek.config.v1.Transform;
import java.util.List;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

/** Exercises the protovalidate rules of config.proto; asserts on rule ids, never on messages. */
class ServerConfigValidationTest {
  private static final Validator VALIDATOR = ValidatorFactory.newBuilder().build();

  private static Duration seconds(long s) {
    return Duration.newBuilder().setSeconds(s).build();
  }

  private static Cluster.Builder cluster() {
    return Cluster.newBuilder()
        .setRequired(true)
        .putProperties("bootstrap.servers", "kafka:9092")
        .putProperties("acks", "all")
        .putProducerProperties("enable.idempotence", "true")
        .putProducerProperties("max.in.flight.requests.per.connection", "5")
        .putConsumerProperties("enable.auto.commit", "false")
        .setTls(
            Tls.newBuilder()
                .setSpiffe(
                    SpiffeSource.newBuilder().setWorkloadApiSocket("unix:///run/spire/s.sock"))
                .addAcceptedSpiffeIds("spiffe://example.org/kafka")
                .setAcceptedTrustDomain("example.org"));
  }

  private static ServerConfig.Builder valid() {
    return ServerConfig.newBuilder()
        .setListener(
            Listener.newBuilder().setSocketPath("/var/run/kafka/kafka.sock").addAllowedUids(1000))
        .setAdmin(Admin.newBuilder().setPort(9464))
        .putProducers("orders", producer("main"))
        .putClusters("main", cluster().build())
        .putConsumers(
            "orders",
            io.github.bpalermo.odradek.config.v1.Consumer.newBuilder()
                .setCluster("main")
                .setGroupId("orders-group")
                .build())
        .setConsume(
            Consume.newBuilder()
                .setDefaultMaxProcessing(seconds(300))
                .setMaxMaxProcessing(seconds(1800))
                .setRevokeDeadline(seconds(10))
                .setCommitInterval(seconds(5))
                .setRebuildJitter(seconds(0)));
  }

  /** Applies {@code edit} to the main cluster of a valid config. */
  private static ServerConfig withCluster(Consumer<Cluster.Builder> edit) {
    Cluster.Builder c = cluster();
    edit.accept(c);
    return valid().putClusters("main", c.build()).build();
  }

  private static List<String> ruleIds(ServerConfig config) throws ValidationException {
    ValidationResult result = VALIDATOR.validate(config);
    return result.getViolations().stream().map(v -> v.toProto().getRuleId()).toList();
  }

  private static void assertViolates(ServerConfig config, String ruleId)
      throws ValidationException {
    assertThat(ruleIds(config)).containsExactly(ruleId);
  }

  @Test
  void validConfigPasses() throws ValidationException {
    assertThat(VALIDATOR.validate(valid().build()).isSuccess()).isTrue();
  }

  private static ProduceTarget.Builder target(String cluster) {
    return ProduceTarget.newBuilder().setCluster(cluster);
  }

  private static Producer producer(String... clusters) {
    Producer.Builder p = Producer.newBuilder();
    for (String c : clusters) {
      p.addClusters(target(c));
    }
    return p.build();
  }

  @Test
  void producingToSeveralClustersPasses() throws ValidationException {
    ServerConfig config =
        valid()
            .putClusters("next", cluster().build())
            .putProducers("orders", producer("main", "next"))
            .putProducers(
                "audit",
                Producer.newBuilder()
                    .addClusters(target("main"))
                    .addClusters(target("next").setBestEffort(true))
                    .build())
            .build();
    assertThat(VALIDATOR.validate(config).isSuccess()).isTrue();
  }

  @Test
  void producerClustersMustExist() throws ValidationException {
    assertViolates(
        valid().putProducers("orders", producer("main", "missing")).build(),
        "server_config.producer_clusters_exist");
  }

  @Test
  void producerClustersMustBeUnique() throws ValidationException {
    assertViolates(
        valid().putProducers("orders", producer("main", "main")).build(),
        "producer.clusters_unique");
  }

  @Test
  void producerNeedsOneRequiredCluster() throws ValidationException {
    ServerConfig config =
        valid()
            .putProducers(
                "orders",
                Producer.newBuilder().addClusters(target("main").setBestEffort(true)).build())
            .build();
    assertViolates(config, "producer.one_required_cluster");
  }

  @Test
  void producerNeedsACluster() throws ValidationException {
    assertThat(ruleIds(valid().putProducers("orders", Producer.getDefaultInstance()).build()))
        .contains("repeated.min_items");
  }

  @Test
  void produceTargetClusterUnsetIsRejected() throws ValidationException {
    ServerConfig config =
        valid()
            .putProducers(
                "orders",
                Producer.newBuilder().addClusters(ProduceTarget.getDefaultInstance()).build())
            .build();
    assertThat(ruleIds(config)).contains("required");
  }

  @Test
  void producerTopicNamesMustBeValid() throws ValidationException {
    assertViolates(valid().putProducers("bad topic!", producer("main")).build(), "string.pattern");
  }

  @Test
  void offsetResetIsOwnedByServer() throws ValidationException {
    assertViolates(
        withCluster(c -> c.putConsumerProperties("auto.offset.reset", "latest")),
        "cluster.offset_reset_owned_by_server");
  }

  @Test
  void consumerClusterMustExist() throws ValidationException {
    ServerConfig config =
        valid()
            .putConsumers(
                "orders",
                io.github.bpalermo.odradek.config.v1.Consumer.newBuilder()
                    .setCluster("missing")
                    .setGroupId("g")
                    .build())
            .build();
    assertViolates(config, "server_config.consumer_cluster_exists");
  }

  @Test
  void consumerClusterUnsetIsRejected() throws ValidationException {
    ServerConfig config =
        valid()
            .putConsumers(
                "orders",
                io.github.bpalermo.odradek.config.v1.Consumer.newBuilder().setGroupId("g").build())
            .build();
    // Unset violates `required`; an unset cluster is also no entry of clusters.
    assertThat(ruleIds(config))
        .containsExactlyInAnyOrder("required", "server_config.consumer_cluster_exists");
  }

  @Test
  void consumerClusterEmptyIsRejected() throws ValidationException {
    ServerConfig config =
        valid()
            .putConsumers(
                "orders",
                io.github.bpalermo.odradek.config.v1.Consumer.newBuilder()
                    .setCluster("")
                    .setGroupId("g")
                    .build())
            .build();
    assertThat(ruleIds(config))
        .containsExactlyInAnyOrder("string.min_len", "server_config.consumer_cluster_exists");
  }

  @Test
  void consumerGroupIdUnsetIsRejected() throws ValidationException {
    ServerConfig config =
        valid()
            .putConsumers(
                "orders",
                io.github.bpalermo.odradek.config.v1.Consumer.newBuilder()
                    .setCluster("main")
                    .build())
            .build();
    assertViolates(config, "required");
  }

  @Test
  void consumerGroupIdEmptyIsRejected() throws ValidationException {
    ServerConfig config =
        valid()
            .putConsumers(
                "orders",
                io.github.bpalermo.odradek.config.v1.Consumer.newBuilder()
                    .setCluster("main")
                    .setGroupId("")
                    .build())
            .build();
    assertViolates(config, "string.min_len");
  }

  @Test
  void fileSourceDirUnsetIsRejected() throws ValidationException {
    assertViolates(
        withCluster(c -> c.setTls(Tls.newBuilder().setFiles(FileSource.getDefaultInstance()))),
        "required");
  }

  @Test
  void fileSourceDirEmptyIsRejected() throws ValidationException {
    assertViolates(
        withCluster(c -> c.setTls(Tls.newBuilder().setFiles(FileSource.newBuilder().setDir("")))),
        "string.prefix");
  }

  @Test
  void emptyListenerSocketPathIsRejectedWhenSet() throws ValidationException {
    assertViolates(
        valid().setListener(Listener.newBuilder().setSocketPath("")).build(), "string.prefix");
  }

  @Test
  void unsetListenerSocketPathIsFine() throws ValidationException {
    assertThat(
            VALIDATOR
                .validate(valid().setListener(Listener.getDefaultInstance()).build())
                .isSuccess())
        .isTrue();
  }

  @Test
  void consumerTopicNameMustBeValid() throws ValidationException {
    ServerConfig config =
        valid()
            .putConsumers(
                "bad topic!",
                io.github.bpalermo.odradek.config.v1.Consumer.newBuilder()
                    .setCluster("main")
                    .setGroupId("g")
                    .build())
            .build();
    assertViolates(config, "string.pattern");
  }

  @Test
  void clusterNeedsBootstrapServers() throws ValidationException {
    assertViolates(
        withCluster(c -> c.removeProperties("bootstrap.servers")), "cluster.bootstrap_servers");
  }

  @Test
  void acksMustBeAll() throws ValidationException {
    assertViolates(withCluster(c -> c.putProperties("acks", "1")), "cluster.acks_all");
    assertViolates(withCluster(c -> c.putProducerProperties("acks", "0")), "cluster.acks_all");
  }

  @Test
  void idempotenceMustBeEnabled() throws ValidationException {
    assertViolates(
        withCluster(c -> c.putProducerProperties("enable.idempotence", "false")),
        "cluster.idempotence_enabled");
  }

  @Test
  void maxInFlightMustBeOneToFive() throws ValidationException {
    assertViolates(
        withCluster(c -> c.putProducerProperties("max.in.flight.requests.per.connection", "6")),
        "cluster.max_in_flight");
    assertViolates(
        withCluster(c -> c.putProperties("max.in.flight.requests.per.connection", "0")),
        "cluster.max_in_flight");
  }

  @Test
  void autoCommitMustBeDisabled() throws ValidationException {
    assertViolates(
        withCluster(c -> c.putConsumerProperties("enable.auto.commit", "true")),
        "cluster.auto_commit_disabled");
  }

  @Test
  void tlsMaterialIsOwnedByServer() throws ValidationException {
    assertViolates(
        withCluster(c -> c.putProperties("ssl.keystore.location", "/k")),
        "cluster.tls_owned_by_server");
    assertViolates(
        withCluster(c -> c.putConsumerProperties("ssl.truststore.type", "PKCS12")),
        "cluster.tls_owned_by_server");
    assertViolates(
        withCluster(c -> c.putProducerProperties("ssl.engine.factory.class", "x.Y")),
        "cluster.tls_owned_by_server");
  }

  @Test
  void spiffeSourceNeedsPeerPolicy() throws ValidationException {
    assertViolates(
        withCluster(
            c -> c.setTls(Tls.newBuilder().setSpiffe(SpiffeSource.getDefaultInstance()).build())),
        "tls.spiffe_peer_policy");
  }

  @Test
  void fileSourceNeedsNoPeerPolicy() throws ValidationException {
    ServerConfig config =
        withCluster(
            c -> c.setTls(Tls.newBuilder().setFiles(FileSource.newBuilder().setDir("/etc/tls"))));
    assertThat(VALIDATOR.validate(config).isSuccess()).isTrue();
  }

  @Test
  void tlsNeedsASource() throws ValidationException {
    assertViolates(
        withCluster(c -> c.setTls(Tls.newBuilder().setAcceptedTrustDomain("example.org"))),
        "required");
  }

  @Test
  void defaultProcessingMustNotExceedMax() throws ValidationException {
    ServerConfig config =
        valid()
            .setConsume(
                Consume.newBuilder()
                    .setDefaultMaxProcessing(seconds(3600))
                    .setMaxMaxProcessing(seconds(60)))
            .build();
    assertViolates(config, "consume.default_within_max");
  }

  @Test
  void clusterNamesMustBeDnsLabels() throws ValidationException {
    ServerConfig config =
        valid()
            .clearClusters()
            .putClusters("Bad_Name", cluster().build())
            .clearProducers()
            .clearConsumers()
            .build();
    assertViolates(config, "string.pattern");
  }

  private static Producer.Builder ordersProducer() {
    return Producer.newBuilder().addClusters(target("main"));
  }

  private static io.github.bpalermo.odradek.config.v1.Consumer.Builder ordersConsumer() {
    return io.github.bpalermo.odradek.config.v1.Consumer.newBuilder()
        .setCluster("main")
        .setGroupId("orders-group");
  }

  private static Codec textCodec() {
    return Codec.newBuilder()
        .setValue(
            Pipeline.newBuilder()
                .setSerializer("com.example.TextSerializer")
                .setDeserializer("com.example.TextDeserializer")
                .putProperties("schema.registry.url", "https://registry.example:8081")
                .addTransforms(
                    Transform.newBuilder()
                        .setClassName("com.example.Outer$AesGcmTransform")
                        .putProperties("key.uri", "kms://orders")))
        .build();
  }

  @Test
  void codecsWithDefaultOverrideAndContractsPass() throws ValidationException {
    ServerConfig config =
        valid()
            .setContracts(Contracts.newBuilder().addDescriptorSets("/etc/odradek/acme.binpb"))
            .setExtensionsDir("/opt/odradek/ext")
            .setDefaultCodec("text")
            .putCodecs("text", textCodec())
            .putProducers(
                "orders", ordersProducer().setValueType("acme.orders.v1.OrderCreated").build())
            .putConsumers(
                "orders",
                ordersConsumer()
                    .setValueType("acme.orders.v1.OrderCreated")
                    .setOnDecodeError(OnDecodeError.ON_DECODE_ERROR_DELIVER_UNDECODED)
                    .build())
            .putProducers("raw", ordersProducer().setCodec("passthrough").build())
            .build();
    assertThat(ruleIds(config)).isEmpty();
  }

  @Test
  void passthroughIsAValidDefaultCodec() throws ValidationException {
    assertThat(ruleIds(valid().setDefaultCodec("passthrough").build())).isEmpty();
  }

  @Test
  void defaultCodecMustExist() throws ValidationException {
    assertViolates(
        valid().setDefaultCodec("missing").build(), "server_config.default_codec_exists");
  }

  @Test
  void producerCodecMustExist() throws ValidationException {
    assertViolates(
        valid().putProducers("orders", ordersProducer().setCodec("missing").build()).build(),
        "server_config.producer_codec_exists");
  }

  @Test
  void consumerCodecMustExist() throws ValidationException {
    assertViolates(
        valid().putConsumers("orders", ordersConsumer().setCodec("missing").build()).build(),
        "server_config.consumer_codec_exists");
  }

  @Test
  void passthroughCannotBeRedefined() throws ValidationException {
    assertViolates(
        valid().putCodecs("passthrough", textCodec()).build(),
        "server_config.passthrough_reserved");
  }

  @Test
  void producedAndConsumedTopicMustAgreeOnContractTypes() throws ValidationException {
    ServerConfig config =
        valid()
            .setContracts(Contracts.newBuilder().addDescriptorSets("/etc/odradek/acme.binpb"))
            .putProducers("orders", ordersProducer().setValueType("acme.Order").build())
            .putConsumers("orders", ordersConsumer().setValueType("acme.Other").build())
            .build();
    assertViolates(config, "server_config.contract_types_match");
  }

  @Test
  void contractTypesNeedDescriptorSets() throws ValidationException {
    ServerConfig config =
        valid()
            .putProducers("events", ordersProducer().setKeyType("acme.EventKey").build())
            .build();
    assertViolates(config, "server_config.contracts_required");
  }

  @Test
  void serializerAndDeserializerComeTogether() throws ValidationException {
    Codec codec =
        Codec.newBuilder()
            .setKey(Pipeline.newBuilder().setSerializer("com.example.TextSerializer"))
            .build();
    assertViolates(valid().putCodecs("half", codec).build(), "pipeline.serializer_pair");
  }

  @Test
  void transformNeedsAClass() throws ValidationException {
    Codec codec =
        Codec.newBuilder()
            .setValue(Pipeline.newBuilder().addTransforms(Transform.getDefaultInstance()))
            .build();
    assertViolates(valid().putCodecs("bad", codec).build(), "required");
  }

  @Test
  void classNamesMustBeJavaNames() throws ValidationException {
    Codec codec =
        Codec.newBuilder()
            .setValue(
                Pipeline.newBuilder()
                    .addTransforms(Transform.newBuilder().setClassName("com.example.1Bad")))
            .build();
    assertViolates(valid().putCodecs("bad", codec).build(), "string.pattern");
  }

  @Test
  void contractTypeNamesMustBeProtobufNames() throws ValidationException {
    ServerConfig config =
        valid()
            .setContracts(Contracts.newBuilder().addDescriptorSets("/etc/odradek/acme.binpb"))
            .putProducers("events", ordersProducer().setValueType(".acme.Leading").build())
            .build();
    assertViolates(config, "string.pattern");
  }

  @Test
  void descriptorSetsMustBeAbsolutePaths() throws ValidationException {
    assertViolates(
        valid().setContracts(Contracts.newBuilder().addDescriptorSets("acme.binpb")).build(),
        "string.prefix");
  }

  @Test
  void codecNamesMustBeDnsLabels() throws ValidationException {
    assertViolates(valid().putCodecs("Bad_Name", textCodec()).build(), "string.pattern");
  }

  @Test
  void transformClassIsWrittenAsClassInTheFile() {
    assertThat(Transform.getDescriptor().findFieldByName("class_name").getJsonName())
        .isEqualTo("class");
  }

  // --- Dispatcher (ADR 6) ---

  private static final String SIDECAR_ID = "spiffe://example.org/ns/billing/sa/billing";

  private static PolicyRule.Builder rule() {
    return PolicyRule.newBuilder()
        .addSpiffeIds(SIDECAR_ID)
        .setCluster("main")
        .addTopics("orders.*")
        .addGroups("billing");
  }

  /** A valid config whose dispatch section has this one rule. */
  private static ServerConfig withRule(PolicyRule.Builder rule) {
    return valid().setDispatch(Dispatch.newBuilder().addPolicy(rule)).build();
  }

  private static io.github.bpalermo.odradek.config.v1.Consumer.Builder dispatched() {
    return io.github.bpalermo.odradek.config.v1.Consumer.newBuilder()
        .setCluster("orders")
        .setGroupId("billing")
        .setSource(ConsumerSource.CONSUMER_SOURCE_DISPATCHER);
  }

  /** A valid config that also receives {@code payments} from a dispatcher. */
  private static ServerConfig receiving(
      io.github.bpalermo.odradek.config.v1.Consumer.Builder consumer) {
    return valid()
        .setDispatcher(Dispatcher.newBuilder().setAddress("odradek-dispatcher.kafka.svc:9465"))
        .putConsumers("payments", consumer.build())
        .build();
  }

  @Test
  void dispatcherConfigurationPasses() throws ValidationException {
    ServerConfig config =
        valid()
            .setDispatch(
                Dispatch.newBuilder()
                    .setPort(9465)
                    .setAdvertisedAddress("10.0.0.7:9465")
                    .setIdleGrace(seconds(300))
                    .setPartitionWindow(1000)
                    .addPolicy(
                        rule()
                            .setTrustDomain("example.org")
                            .addTopics("payments")
                            .addGroups("billing-*")
                            .addDeadLetterTopics("orders.dlq"))
                    .addPolicy(
                        PolicyRule.newBuilder()
                            .setTrustDomain("example.org")
                            .setCluster("main")
                            .addTopics("*")
                            .addGroups("*")
                            .addDeadLetterTopics("*")))
            .build();
    assertThat(ruleIds(config)).isEmpty();
  }

  @Test
  void emptyDispatchSectionPasses() throws ValidationException {
    // Every field has a default, and no rule means that every declaration is refused.
    assertThat(ruleIds(valid().setDispatch(Dispatch.getDefaultInstance()).build())).isEmpty();
  }

  @Test
  void dispatchedConsumerPasses() throws ValidationException {
    // Its cluster, "orders", is a name in the dispatcher's configuration, not in this one.
    ServerConfig config =
        receiving(
            dispatched()
                .setOrdering(Ordering.ORDERING_PARTITION)
                .setMaxAttempts(5)
                .setOnExhausted(OnExhausted.ON_EXHAUSTED_DEAD_LETTER)
                .setDeadLetterTopic("payments.dlq")
                .setOnDecodeError(OnDecodeError.ON_DECODE_ERROR_DELIVER_UNDECODED));
    assertThat(ruleIds(config)).isEmpty();
  }

  @Test
  void dispatchedConsumerWithDefaultsPasses() throws ValidationException {
    assertThat(ruleIds(receiving(dispatched()))).isEmpty();
    assertThat(ruleIds(receiving(dispatched().setMaxAttempts(3)))).isEmpty();
    assertThat(
            ruleIds(
                receiving(
                    dispatched().setMaxAttempts(3).setOnExhausted(OnExhausted.ON_EXHAUSTED_BLOCK))))
        .isEmpty();
  }

  @Test
  void serverThatOnlyReceivesNeedsNoCluster() throws ValidationException {
    ServerConfig config =
        ServerConfig.newBuilder()
            .setDispatcher(Dispatcher.newBuilder().setAddress("odradek-dispatcher:9465"))
            .putConsumers("payments", dispatched().build())
            .build();
    assertThat(ruleIds(config)).isEmpty();
  }

  @Test
  void aClusterIsRequiredWithoutADispatchedConsumer() throws ValidationException {
    assertViolates(ServerConfig.getDefaultInstance(), "server_config.clusters_required");
    // Where the dispatcher is does not make the server receive anything.
    assertViolates(
        ServerConfig.newBuilder()
            .setDispatcher(Dispatcher.newBuilder().setAddress("odradek-dispatcher:9465"))
            .build(),
        "server_config.clusters_required");
  }

  @Test
  void consumerThatReadsKafkaItselfStillNeedsItsCluster() throws ValidationException {
    ServerConfig config =
        receiving(dispatched().setSource(ConsumerSource.CONSUMER_SOURCE_KAFKA).setCluster("gone"));
    assertViolates(config, "server_config.consumer_cluster_exists");
  }

  @Test
  void producerOfAServerThatReceivesStillNeedsItsCluster() throws ValidationException {
    ServerConfig config =
        ServerConfig.newBuilder()
            .setDispatcher(Dispatcher.newBuilder().setAddress("odradek-dispatcher:9465"))
            .putConsumers("payments", dispatched().build())
            .putProducers("payments.done", producer("orders"))
            .build();
    assertViolates(config, "server_config.producer_clusters_exist");
  }

  @Test
  void dispatchedConsumerNeedsTheDispatcher() throws ValidationException {
    assertViolates(
        valid().putConsumers("payments", dispatched().build()).build(),
        "server_config.dispatcher_required");
  }

  @Test
  void dispatcherAddressUnsetIsRejected() throws ValidationException {
    assertViolates(valid().setDispatcher(Dispatcher.getDefaultInstance()).build(), "required");
  }

  @Test
  void dispatcherAddressMustBeHostAndPort() throws ValidationException {
    for (String address :
        List.of("odradek-dispatcher", "http://odradek-dispatcher:9465", "dispatcher:99999")) {
      assertViolates(
          valid().setDispatcher(Dispatcher.newBuilder().setAddress(address)).build(),
          "string.host_and_port");
    }
    assertThat(
            ruleIds(
                valid()
                    .setDispatcher(Dispatcher.newBuilder().setAddress("[fd00::7]:9465"))
                    .build()))
        .isEmpty();
  }

  @Test
  void consumerSourceMustBeDefined() throws ValidationException {
    assertViolates(
        valid().putConsumers("orders", ordersConsumer().setSourceValue(99).build()).build(),
        "enum.defined_only");
  }

  @Test
  void dispatcherSettingsAreRejectedOnAConsumerThatReadsKafkaItself() throws ValidationException {
    List<Consumer<io.github.bpalermo.odradek.config.v1.Consumer.Builder>> settings =
        List.of(
            c -> c.setOrdering(Ordering.ORDERING_NONE),
            c -> c.setMaxAttempts(3),
            c -> c.setMaxAttempts(3).setOnExhausted(OnExhausted.ON_EXHAUSTED_BLOCK),
            c ->
                c.setMaxAttempts(3)
                    .setOnExhausted(OnExhausted.ON_EXHAUSTED_DEAD_LETTER)
                    .setDeadLetterTopic("orders.dlq"));
    for (var setting : settings) {
      var direct = ordersConsumer();
      setting.accept(direct);
      assertViolates(
          valid().putConsumers("orders", direct.build()).build(), "consumer.dispatcher_settings");
      // The same with the source spelled out.
      direct.setSource(ConsumerSource.CONSUMER_SOURCE_KAFKA);
      assertViolates(
          valid().putConsumers("orders", direct.build()).build(), "consumer.dispatcher_settings");
    }
  }

  @Test
  void orderingMustBeDefined() throws ValidationException {
    assertViolates(receiving(dispatched().setOrderingValue(99)), "enum.defined_only");
  }

  @Test
  void maxAttemptsMustBeAtLeastOne() throws ValidationException {
    assertViolates(receiving(dispatched().setMaxAttempts(0)), "uint32.gte");
  }

  @Test
  void onExhaustedNeedsMaxAttempts() throws ValidationException {
    assertViolates(
        receiving(dispatched().setOnExhausted(OnExhausted.ON_EXHAUSTED_BLOCK)),
        "consumer.on_exhausted_needs_max_attempts");
    assertViolates(
        receiving(
            dispatched()
                .setOnExhausted(OnExhausted.ON_EXHAUSTED_DEAD_LETTER)
                .setDeadLetterTopic("payments.dlq")),
        "consumer.on_exhausted_needs_max_attempts");
  }

  @Test
  void deadLetteringNeedsADeadLetterTopic() throws ValidationException {
    assertViolates(
        receiving(
            dispatched().setMaxAttempts(3).setOnExhausted(OnExhausted.ON_EXHAUSTED_DEAD_LETTER)),
        "consumer.dead_letter_topic");
  }

  @Test
  void deadLetterTopicIsRejectedUnlessDeadLettering() throws ValidationException {
    assertViolates(
        receiving(dispatched().setMaxAttempts(3).setDeadLetterTopic("payments.dlq")),
        "consumer.dead_letter_topic");
    assertViolates(
        receiving(
            dispatched()
                .setMaxAttempts(3)
                .setOnExhausted(OnExhausted.ON_EXHAUSTED_BLOCK)
                .setDeadLetterTopic("payments.dlq")),
        "consumer.dead_letter_topic");
  }

  @Test
  void deadLetterTopicMustNotBeTheTopicItself() throws ValidationException {
    assertViolates(
        receiving(
            dispatched()
                .setMaxAttempts(3)
                .setOnExhausted(OnExhausted.ON_EXHAUSTED_DEAD_LETTER)
                .setDeadLetterTopic("payments")),
        "server_config.dead_letter_topic_differs");
  }

  @Test
  void deadLetterTopicNameMustBeValid() throws ValidationException {
    assertViolates(
        receiving(
            dispatched()
                .setMaxAttempts(3)
                .setOnExhausted(OnExhausted.ON_EXHAUSTED_DEAD_LETTER)
                .setDeadLetterTopic("bad topic!")),
        "string.pattern");
  }

  @Test
  void onExhaustedMustBeDefined() throws ValidationException {
    assertViolates(
        receiving(dispatched().setMaxAttempts(3).setOnExhaustedValue(99)), "enum.defined_only");
  }

  @Test
  void dispatchPortMustBeAPort() throws ValidationException {
    assertViolates(valid().setDispatch(Dispatch.newBuilder().setPort(70000)).build(), "uint32.lte");
  }

  @Test
  void advertisedAddressMustBeHostAndPort() throws ValidationException {
    assertViolates(
        valid().setDispatch(Dispatch.newBuilder().setAdvertisedAddress("10.0.0.7")).build(),
        "string.host_and_port");
  }

  @Test
  void idleGraceMustNotBeNegative() throws ValidationException {
    assertViolates(
        valid().setDispatch(Dispatch.newBuilder().setIdleGrace(seconds(-1))).build(),
        "duration.gte");
    assertThat(ruleIds(valid().setDispatch(Dispatch.newBuilder().setIdleGrace(seconds(0))).build()))
        .isEmpty();
  }

  @Test
  void partitionWindowMustBeAtLeastOne() throws ValidationException {
    assertViolates(
        valid().setDispatch(Dispatch.newBuilder().setPartitionWindow(0)).build(), "uint32.gte");
  }

  @Test
  void policyRuleClusterMustExist() throws ValidationException {
    assertViolates(withRule(rule().setCluster("missing")), "server_config.policy_clusters_exist");
  }

  @Test
  void policyRuleClusterUnsetIsRejected() throws ValidationException {
    // Unset violates `required`; an unset cluster is also no entry of clusters.
    assertThat(ruleIds(withRule(rule().clearCluster())))
        .containsExactlyInAnyOrder("required", "server_config.policy_clusters_exist");
  }

  @Test
  void policyRuleNeedsAnIdentity() throws ValidationException {
    assertViolates(withRule(rule().clearSpiffeIds()), "policy_rule.identity_required");
    assertThat(ruleIds(withRule(rule().clearSpiffeIds().setTrustDomain("example.org")))).isEmpty();
  }

  @Test
  void policyRuleSpiffeIdsMustBeSpiffeIds() throws ValidationException {
    assertViolates(withRule(rule().addSpiffeIds("example.org/billing")), "string.prefix");
    assertViolates(withRule(rule().addSpiffeIds(SIDECAR_ID)), "repeated.unique");
  }

  @Test
  void policyRuleTrustDomainHasNoScheme() throws ValidationException {
    assertViolates(withRule(rule().setTrustDomain("spiffe://example.org")), "string.pattern");
  }

  @Test
  void policyRuleNeedsTopicsAndGroups() throws ValidationException {
    assertViolates(withRule(rule().clearTopics()), "repeated.min_items");
    assertViolates(withRule(rule().clearGroups()), "repeated.min_items");
  }

  @Test
  void policyRuleNeedsNoDeadLetterTopics() throws ValidationException {
    assertThat(ruleIds(withRule(rule().clearDeadLetterTopics()))).isEmpty();
  }

  @Test
  void policyPatternsAreANameOrAPrefixAndAStar() throws ValidationException {
    for (String pattern : List.of("orders.created", "orders.*", "orders*", "*", "a_b-c.D9")) {
      assertThat(ruleIds(withRule(rule().clearTopics().addTopics(pattern)))).isEmpty();
      assertThat(ruleIds(withRule(rule().addDeadLetterTopics(pattern)))).isEmpty();
    }
    for (String pattern : List.of("", "*.created", "orders.*.v1", "orders.**", "bad topic", "o?")) {
      assertViolates(withRule(rule().clearTopics().addTopics(pattern)), "string.pattern");
      assertViolates(withRule(rule().addDeadLetterTopics(pattern)), "string.pattern");
    }
    // A group id is any text, so only the star is constrained.
    for (String pattern : List.of("billing", "billing-*", "*", "a group, with: punctuation")) {
      assertThat(ruleIds(withRule(rule().clearGroups().addGroups(pattern)))).isEmpty();
    }
    for (String pattern : List.of("", "*-billing", "bill*ing", "billing**")) {
      assertViolates(withRule(rule().clearGroups().addGroups(pattern)), "string.pattern");
    }
  }

  @Test
  void policyPatternsAreListedOnce() throws ValidationException {
    assertViolates(withRule(rule().addTopics("orders.*")), "repeated.unique");
    assertViolates(withRule(rule().addGroups("billing")), "repeated.unique");
    assertViolates(
        withRule(rule().addDeadLetterTopics("dlq").addDeadLetterTopics("dlq")), "repeated.unique");
  }
}
