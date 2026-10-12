package io.github.bpalermo.odradek.server.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import io.github.bpalermo.odradek.config.v1.OffsetReset;
import io.github.bpalermo.odradek.config.v1.OnDecodeError;
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
