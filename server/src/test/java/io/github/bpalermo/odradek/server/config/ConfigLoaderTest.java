package io.github.bpalermo.odradek.server.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import io.github.bpalermo.odradek.config.v1.ConsumerSource;
import io.github.bpalermo.odradek.config.v1.Dispatch;
import io.github.bpalermo.odradek.config.v1.OffsetReset;
import io.github.bpalermo.odradek.config.v1.OnDecodeError;
import io.github.bpalermo.odradek.config.v1.OnExhausted;
import io.github.bpalermo.odradek.config.v1.Ordering;
import io.github.bpalermo.odradek.config.v1.PolicyRule;
import io.github.bpalermo.odradek.config.v1.ServerConfig;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class ConfigLoaderTest {
  private static final String FULL =
      """
      listener:
        socketPath: /var/run/kafka/kafka.sock
        allowedUids: [1000]
      admin:
        port: 9464
      extensionsDir: /opt/odradek/ext
      contracts:
        descriptorSets:
          - /etc/odradek/schemas/acme.binpb
      defaultCodec: encrypted
      codecs:
        encrypted:
          value:
            serializer: com.example.serde.ProtobufSerializer
            deserializer: com.example.serde.ProtobufDeserializer
            properties:
              schema.registry.url: https://registry.example:8081
              cache.size: 100          # a number in YAML, a string in the schema
            transforms:
              - class: com.example.crypto.AesGcmTransform
                properties:
                  key.uri: kms://default
      clusters:
        orders:
          required: true
          properties:
            bootstrap.servers: orders-kafka:9093
            acks: all
          producerProperties:
            enable.idempotence: true   # a boolean in YAML
            max.in.flight.requests.per.connection: 5
          tls:
            spiffe: {}
            acceptedSpiffeIds: ["spiffe://example.org/ns/kafka/sa/orders-broker"]
        analytics:
          properties:
            bootstrap.servers: analytics-kafka:9093
          tls:
            files:
              dir: /etc/odradek/tls/analytics
      producers:
        orders.created:
          valueType: acme.orders.v1.OrderCreated
          clusters:
            - cluster: orders
            - cluster: analytics
              bestEffort: true
        legacy.raw:
          codec: passthrough
          clusters:
            - cluster: orders
      consumers:
        orders.created:
          valueType: acme.orders.v1.OrderCreated
          cluster: orders
          groupId: billing
          startTimestamp: "2026-10-12T10:00:00Z"
          offsetReset: OFFSET_RESET_LATEST
          onDecodeError: ON_DECODE_ERROR_DELIVER_UNDECODED
      consume:
        defaultMaxProcessing: 300s
        maxMaxProcessing: 1800s
      """;

  /** A dispatcher: clusters and a policy, and no topic of its own. */
  private static final String DISPATCHER =
      """
      clusters:
        orders:
          required: true
          properties:
            bootstrap.servers: orders-kafka:9093
      dispatch:
        coordination:
          groupId: odradek-dispatcher
        port: 9465
        advertisedAddress: 10.0.0.7:9465
        idleGrace: 600s
        partitionWindow: 500
        tls:
          spiffe:
            workloadApiSocket: unix:///run/spire/agent/sockets/agent.sock
        policy:
          - spiffeIds: ["spiffe://example.org/ns/billing/sa/billing"]
            cluster: orders
            topics: ["orders.*", payments]
            groups: [billing]
            deadLetterTopics: ["billing.dlq.*"]
          - trustDomain: example.org
            cluster: orders
            topics: [audit]
            groups: ["*"]
      """;

  /** A sidecar that receives one topic from a dispatcher, reads another itself, and produces. */
  private static final String RECEIVING =
      """
      clusters:
        analytics:
          properties:
            bootstrap.servers: analytics-kafka:9093
      dispatcher:
        address: odradek-dispatcher.kafka.svc:9465
        tls:
          files:
            dir: /etc/odradek/delivery
          acceptedSpiffeIds: ["spiffe://example.org/ns/kafka/sa/odradek-dispatcher"]
      producers:
        clickstream:
          clusters:
            - cluster: analytics
      consumers:
        orders.created:
          source: CONSUMER_SOURCE_DISPATCHER
          cluster: orders              # a name in the dispatcher's configuration
          groupId: billing
          ordering: ORDERING_KEY
          maxAttempts: 5
          onExhausted: ON_EXHAUSTED_DEAD_LETTER
          deadLetterTopic: billing.dlq.orders
        clickstream:
          cluster: analytics
          groupId: billing
      """;

  private static ConfigException rejected(String yaml) {
    return catchThrowableOfType(ConfigException.class, () -> ConfigLoader.parse(yaml));
  }

  @Test
  void loadsTheMinimalFile() throws Exception {
    ServerConfig config = ConfigLoader.load(Path.of("server/testdata/minimal.yaml"));
    assertThat(config.getClustersMap().get("main").getPropertiesMap())
        .containsEntry("bootstrap.servers", "localhost:9092");
  }

  @Test
  void loadsEverySection() throws Exception {
    ServerConfig config = ConfigLoader.parse(FULL);
    assertThat(config.getListener().getAllowedUidsList()).containsExactly(1000);
    assertThat(config.getAdmin().getPort()).isEqualTo(9464);
    assertThat(config.getDefaultCodec()).isEqualTo("encrypted");
    assertThat(config.getCodecsMap().get("encrypted").getValue().getTransforms(0).getClassName())
        .isEqualTo("com.example.crypto.AesGcmTransform");
    assertThat(config.getProducersMap().get("orders.created").getClusters(1).getBestEffort())
        .isTrue();
    assertThat(config.getProducersMap().get("legacy.raw").getCodec()).isEqualTo("passthrough");
    var consumer = config.getConsumersMap().get("orders.created");
    assertThat(consumer.getStartTimestamp().getSeconds()).isEqualTo(1791799200L);
    assertThat(consumer.getOffsetReset()).isEqualTo(OffsetReset.OFFSET_RESET_LATEST);
    assertThat(consumer.getOnDecodeError())
        .isEqualTo(OnDecodeError.ON_DECODE_ERROR_DELIVER_UNDECODED);
    assertThat(config.getConsume().getDefaultMaxProcessing().getSeconds()).isEqualTo(300);
  }

  @Test
  void loadsADispatcher() throws Exception {
    Dispatch dispatch = ConfigLoader.parse(DISPATCHER).getDispatch();
    assertThat(dispatch.getPort()).isEqualTo(9465);
    assertThat(dispatch.getAdvertisedAddress()).isEqualTo("10.0.0.7:9465");
    assertThat(dispatch.getIdleGrace().getSeconds()).isEqualTo(600);
    assertThat(dispatch.getPartitionWindow()).isEqualTo(500);
    assertThat(dispatch.getPolicyList()).hasSize(2);
    PolicyRule rule = dispatch.getPolicy(0);
    assertThat(rule.getSpiffeIdsList())
        .containsExactly("spiffe://example.org/ns/billing/sa/billing");
    assertThat(rule.getCluster()).isEqualTo("orders");
    assertThat(rule.getTopicsList()).containsExactly("orders.*", "payments");
    assertThat(rule.getGroupsList()).containsExactly("billing");
    assertThat(rule.getDeadLetterTopicsList()).containsExactly("billing.dlq.*");
    assertThat(dispatch.getPolicy(1).getTrustDomain()).isEqualTo("example.org");
    assertThat(dispatch.getTls().getSpiffe().getWorkloadApiSocket())
        .isEqualTo("unix:///run/spire/agent/sockets/agent.sock");
  }

  @Test
  void aPolicyWithoutTlsIsRejected() {
    ConfigException e =
        rejected(
            DISPATCHER.replace(
                """
                  tls:
                    spiffe:
                      workloadApiSocket: unix:///run/spire/agent/sockets/agent.sock
                """,
                ""));
    assertThat(e.problems())
        .singleElement()
        .satisfies(
            p ->
                assertThat(p)
                    .startsWith("dispatch: a policy needs tls")
                    .endsWith("[dispatch.policy_requires_tls]"));
  }

  @Test
  void loadsAServerThatReceivesFromADispatcher() throws Exception {
    ServerConfig config = ConfigLoader.parse(RECEIVING);
    assertThat(config.getDispatcher().getAddress()).isEqualTo("odradek-dispatcher.kafka.svc:9465");
    assertThat(config.getDispatcher().getTls().getFiles().getDir())
        .isEqualTo("/etc/odradek/delivery");
    assertThat(config.getDispatcher().getTls().getAcceptedSpiffeIdsList())
        .containsExactly("spiffe://example.org/ns/kafka/sa/odradek-dispatcher");
    var dispatched = config.getConsumersMap().get("orders.created");
    assertThat(dispatched.getSource()).isEqualTo(ConsumerSource.CONSUMER_SOURCE_DISPATCHER);
    assertThat(dispatched.getCluster()).isEqualTo("orders");
    assertThat(dispatched.getOrdering()).isEqualTo(Ordering.ORDERING_KEY);
    assertThat(dispatched.getMaxAttempts()).isEqualTo(5);
    assertThat(dispatched.getOnExhausted()).isEqualTo(OnExhausted.ON_EXHAUSTED_DEAD_LETTER);
    assertThat(dispatched.getDeadLetterTopic()).isEqualTo("billing.dlq.orders");
    assertThat(config.getConsumersMap().get("clickstream").hasSource()).isFalse();
  }

  @Test
  void loadsAServerThatOnlyReceives() throws Exception {
    ServerConfig config =
        ConfigLoader.parse(
            """
            dispatcher:
              address: odradek-dispatcher.kafka.svc:9465
            consumers:
              orders.created:
                source: CONSUMER_SOURCE_DISPATCHER
                cluster: orders
                groupId: billing
            """);
    assertThat(config.getClustersMap()).isEmpty();
  }

  @Test
  void dispatcherViolationsNameTheirPath() {
    ConfigException e =
        rejected(
            """
            clusters:
              main:
                properties:
                  bootstrap.servers: k:9092
            dispatch:
              coordination:
                groupId: odradek-dispatcher
              tls:
                files:
                  dir: /etc/odradek/delivery
              policy:
                - trustDomain: example.org
                  cluster: main
                  topics: ["orders.*.v1"]
                  groups: [billing]
            consumers:
              orders:
                source: CONSUMER_SOURCE_DISPATCHER
                cluster: orders
                groupId: billing
                onExhausted: ON_EXHAUSTED_DEAD_LETTER
            """);
    assertThat(e.problems())
        .hasSize(4)
        .anySatisfy(
            p ->
                assertThat(p)
                    .startsWith("dispatch.policy[0].topics[0]: ")
                    .endsWith("[string.pattern]"))
        .anySatisfy(p -> assertThat(p).contains("[server_config.dispatcher_required]"))
        .anySatisfy(
            p ->
                assertThat(p)
                    .startsWith("consumers[\"orders\"]")
                    .contains("[consumer.dead_letter_topic]"))
        .anySatisfy(p -> assertThat(p).contains("[consumer.on_exhausted_needs_max_attempts]"));
  }

  @Test
  void unquotedNumbersAndBooleansBecomeStringProperties() throws Exception {
    ServerConfig config = ConfigLoader.parse(FULL);
    assertThat(config.getClustersMap().get("orders").getProducerPropertiesMap())
        .containsEntry("enable.idempotence", "true")
        .containsEntry("max.in.flight.requests.per.connection", "5");
    assertThat(config.getCodecsMap().get("encrypted").getValue().getPropertiesMap())
        .containsEntry("cache.size", "100");
  }

  @Test
  void jsonIsAccepted() throws Exception {
    ServerConfig config =
        ConfigLoader.parse(
            "{\"clusters\": {\"main\": {\"properties\": {\"bootstrap.servers\": \"k:9092\"}}}}");
    assertThat(config.getClustersMap()).containsKey("main");
  }

  @Test
  void unknownFieldIsRejected() {
    ConfigException e = rejected("clusters:\n  main:\n    propertiez: {}\n");
    assertThat(e.problems()).singleElement().asString().contains("propertiez");
  }

  @Test
  void wrongTypeIsRejected() {
    assertThat(rejected("admin:\n  port: not-a-number\n").problems()).hasSize(1);
  }

  @Test
  void everyViolationIsReportedWithItsPathAndRuleId() {
    ConfigException e =
        rejected(
            """
            clusters:
              main:
                properties:
                  bootstrap.servers: k:9092
                  acks: 1
            producers:
              orders:
                clusters:
                  - cluster: missing
            """);
    assertThat(e.problems())
        .hasSize(2)
        .anySatisfy(p -> assertThat(p).contains("[server_config.producer_clusters_exist]"))
        .anySatisfy(
            p -> assertThat(p).startsWith("clusters[\"main\"]").contains("[cluster.acks_all]"));
  }

  @Test
  void nestedViolationNamesTheListIndex() {
    ConfigException e =
        rejected(
            """
            clusters:
              main:
                properties:
                  bootstrap.servers: k:9092
            producers:
              orders:
                clusters:
                  - cluster: main
                  - cluster: main
                    bestEffort: true
                  - {}
            """);
    assertThat(e.problems())
        .anySatisfy(
            p ->
                assertThat(p)
                    .startsWith("producers[\"orders\"].clusters[2].cluster: ")
                    .endsWith("[required]"));
  }

  @Test
  void emptyConfigurationViolatesTheSchema() {
    assertThat(rejected("{}").problems()).isNotEmpty();
  }

  @Test
  void documentThatIsNotAMappingIsRejected() {
    assertThat(rejected("- a\n- b\n").problems()).singleElement().asString().contains("mapping");
    assertThat(rejected("").problems()).singleElement().asString().contains("mapping");
  }

  @Test
  void invalidYamlIsRejected() {
    assertThat(rejected("clusters: [unclosed").problems())
        .singleElement()
        .asString()
        .startsWith("not valid YAML");
  }

  @Test
  void duplicateKeysAreRejected() {
    assertThatThrownBy(() -> ConfigLoader.parse("admin:\n  port: 1\nadmin:\n  port: 2\n"))
        .isInstanceOf(ConfigException.class);
  }
}
