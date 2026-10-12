package io.github.bpalermo.odradek.sidecar.config;

import static org.assertj.core.api.Assertions.assertThat;

import build.buf.protovalidate.ValidationResult;
import build.buf.protovalidate.Validator;
import build.buf.protovalidate.ValidatorFactory;
import build.buf.protovalidate.exceptions.ValidationException;
import com.google.protobuf.Duration;
import io.github.bpalermo.odradek.config.v1.Admin;
import io.github.bpalermo.odradek.config.v1.Cluster;
import io.github.bpalermo.odradek.config.v1.Consume;
import io.github.bpalermo.odradek.config.v1.FileSource;
import io.github.bpalermo.odradek.config.v1.Listener;
import io.github.bpalermo.odradek.config.v1.ProduceTarget;
import io.github.bpalermo.odradek.config.v1.Producer;
import io.github.bpalermo.odradek.config.v1.SidecarConfig;
import io.github.bpalermo.odradek.config.v1.SpiffeSource;
import io.github.bpalermo.odradek.config.v1.Tls;
import java.util.List;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

/** Exercises the protovalidate rules of config.proto; asserts on rule ids, never on messages. */
class SidecarConfigValidationTest {
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

  private static SidecarConfig.Builder valid() {
    return SidecarConfig.newBuilder()
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
  private static SidecarConfig withCluster(Consumer<Cluster.Builder> edit) {
    Cluster.Builder c = cluster();
    edit.accept(c);
    return valid().putClusters("main", c.build()).build();
  }

  private static List<String> ruleIds(SidecarConfig config) throws ValidationException {
    ValidationResult result = VALIDATOR.validate(config);
    return result.getViolations().stream().map(v -> v.toProto().getRuleId()).toList();
  }

  private static void assertViolates(SidecarConfig config, String ruleId)
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
    SidecarConfig config =
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
        "sidecar_config.producer_clusters_exist");
  }

  @Test
  void producerClustersMustBeUnique() throws ValidationException {
    assertViolates(
        valid().putProducers("orders", producer("main", "main")).build(),
        "producer.clusters_unique");
  }

  @Test
  void producerNeedsOneRequiredCluster() throws ValidationException {
    SidecarConfig config =
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
    SidecarConfig config =
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
  void offsetResetIsOwnedBySidecar() throws ValidationException {
    assertViolates(
        withCluster(c -> c.putConsumerProperties("auto.offset.reset", "latest")),
        "cluster.offset_reset_owned_by_sidecar");
  }

  @Test
  void consumerClusterMustExist() throws ValidationException {
    SidecarConfig config =
        valid()
            .putConsumers(
                "orders",
                io.github.bpalermo.odradek.config.v1.Consumer.newBuilder()
                    .setCluster("missing")
                    .setGroupId("g")
                    .build())
            .build();
    assertViolates(config, "sidecar_config.consumer_cluster_exists");
  }

  @Test
  void consumerClusterUnsetIsRejected() throws ValidationException {
    SidecarConfig config =
        valid()
            .putConsumers(
                "orders",
                io.github.bpalermo.odradek.config.v1.Consumer.newBuilder().setGroupId("g").build())
            .build();
    // Unset violates `required`; an unset cluster is also no entry of clusters.
    assertThat(ruleIds(config))
        .containsExactlyInAnyOrder("required", "sidecar_config.consumer_cluster_exists");
  }

  @Test
  void consumerClusterEmptyIsRejected() throws ValidationException {
    SidecarConfig config =
        valid()
            .putConsumers(
                "orders",
                io.github.bpalermo.odradek.config.v1.Consumer.newBuilder()
                    .setCluster("")
                    .setGroupId("g")
                    .build())
            .build();
    assertThat(ruleIds(config))
        .containsExactlyInAnyOrder("string.min_len", "sidecar_config.consumer_cluster_exists");
  }

  @Test
  void consumerGroupIdUnsetIsRejected() throws ValidationException {
    SidecarConfig config =
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
    SidecarConfig config =
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
    SidecarConfig config =
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
  void tlsMaterialIsOwnedBySidecar() throws ValidationException {
    assertViolates(
        withCluster(c -> c.putProperties("ssl.keystore.location", "/k")),
        "cluster.tls_owned_by_sidecar");
    assertViolates(
        withCluster(c -> c.putConsumerProperties("ssl.truststore.type", "PKCS12")),
        "cluster.tls_owned_by_sidecar");
    assertViolates(
        withCluster(c -> c.putProducerProperties("ssl.engine.factory.class", "x.Y")),
        "cluster.tls_owned_by_sidecar");
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
    SidecarConfig config =
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
    SidecarConfig config =
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
    SidecarConfig config =
        valid()
            .clearClusters()
            .putClusters("Bad_Name", cluster().build())
            .clearProducers()
            .clearConsumers()
            .build();
    assertViolates(config, "string.pattern");
  }
}
