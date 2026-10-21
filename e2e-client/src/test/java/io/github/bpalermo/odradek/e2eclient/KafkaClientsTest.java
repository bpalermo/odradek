package io.github.bpalermo.odradek.e2eclient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.bpalermo.odradek.config.v1.ServerConfig;
import io.github.bpalermo.odradek.config.v1.Tls;
import io.github.bpalermo.odradek.server.tls.CredentialException;
import io.github.bpalermo.odradek.server.tls.TlsSources;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class KafkaClientsTest {
  private static Settings settings(String... flags) {
    List<String> args =
        new java.util.ArrayList<>(
            List.of(
                "--mode",
                "both",
                "--topic",
                "lane-a",
                "--transport",
                "kafka",
                "--bootstrap-servers",
                "broker:9093"));
    args.addAll(List.of(flags));
    return Settings.parse(args, Map.of());
  }

  @Test
  void theProducerHasWhatTheServerEnforcesAndKafkasDefaultsForTheRest() {
    Map<String, Object> properties =
        KafkaClients.producer(settings().kafka(), "e2e-client-pod", Map.of());

    assertThat(properties)
        .containsEntry("bootstrap.servers", "broker:9093")
        .containsEntry("acks", "all")
        .containsEntry("enable.idempotence", "true")
        .containsEntry("client.id", "e2e-client-pod")
        .doesNotContainKeys("linger.ms", "batch.size", "compression.type");
    // What the server's producers end up with too: the same kafka-clients, the same defaults.
    assertThat(KafkaClients.effective(properties, true))
        .containsEntry("acks", "-1")
        .containsEntry("enable.idempotence", "true")
        .containsEntry("max.in.flight.requests.per.connection", "5")
        .containsEntry("linger.ms", "5")
        .containsEntry("batch.size", "16384")
        .containsEntry("compression.type", "none")
        .containsEntry("security.protocol", "PLAINTEXT");
  }

  @Test
  void theConsumerCommitsByHandAndUsesTheCooperativeStickyAssignor() {
    Map<String, Object> properties =
        KafkaClients.consumer(settings().kafka(), "e2e-client-pod", Map.of());

    assertThat(KafkaClients.effective(properties, false))
        .containsEntry("group.id", "e2e-client-lane-a")
        .containsEntry("enable.auto.commit", "false")
        .containsEntry(
            "partition.assignment.strategy",
            "org.apache.kafka.clients.consumer.CooperativeStickyAssignor")
        .containsEntry("auto.offset.reset", "earliest")
        .containsEntry("group.protocol", "classic")
        .containsEntry("fetch.min.bytes", "1")
        .containsEntry("fetch.max.wait.ms", "500")
        .containsEntry("max.poll.records", "500");
  }

  @Test
  void overridesComeFromAPropertiesFileAndCannotWeakenWhatIsEnforced(@TempDir Path dir)
      throws Exception {
    Path file = dir.resolve("kafka.properties");
    Files.writeString(
        file,
        "linger.ms=0\nacks=1\nenable.auto.commit=true\nauto.offset.reset=latest\n"
            + "group.id=other\n",
        StandardCharsets.UTF_8);
    Map<String, String> overrides = KafkaClients.overrides(file.toString());
    Settings settings = settings("--group", "g");

    assertThat(KafkaClients.producer(settings.kafka(), "c", overrides))
        .containsEntry("linger.ms", "0")
        .containsEntry("acks", "all");
    assertThat(KafkaClients.consumer(settings.kafka(), "c", overrides))
        .containsEntry("auto.offset.reset", "latest")
        .containsEntry("enable.auto.commit", "false")
        .containsEntry("group.id", "g");
    assertThat(KafkaClients.overrides("")).isEmpty();
  }

  @Test
  void tlsIsTheTlsSectionOfAClusterOfTheServer() {
    assertThat(KafkaClients.tlsConfig(settings().kafka(), Map.of())).isNull();

    ServerConfig spiffe =
        KafkaClients.tlsConfig(
            settings(
                    "--tls",
                    "spiffe",
                    "--spiffe-socket",
                    "unix:///run/agent.sock",
                    "--broker-spiffe-ids",
                    "spiffe://td/kafka,spiffe://td/kafka-2")
                .kafka(),
            Map.of());
    Tls tls = spiffe.getClustersOrThrow(KafkaClients.CLUSTER).getTls();
    assertThat(tls.getSpiffe().getWorkloadApiSocket()).isEqualTo("unix:///run/agent.sock");
    assertThat(tls.getAcceptedSpiffeIdsList())
        .containsExactly("spiffe://td/kafka", "spiffe://td/kafka-2");

    ServerConfig files =
        KafkaClients.tlsConfig(settings("--tls", "files", "--tls-dir", "/certs").kafka(), Map.of());
    Tls fromFiles = files.getClustersOrThrow(KafkaClients.CLUSTER).getTls();
    assertThat(fromFiles.getFiles().getDir()).isEqualTo("/certs");
    assertThat(fromFiles.getAcceptedSpiffeIdsList()).isEmpty();
  }

  @Test
  void overridesThatTurnTlsOffAreRefusedAsTheServerRefusesThem() {
    for (String protocol : List.of("PLAINTEXT", "SASL_PLAINTEXT")) {
      ServerConfig config =
          KafkaClients.tlsConfig(
              settings("--tls", "files", "--tls-dir", "/nonexistent").kafka(),
              Map.of("security.protocol", protocol));
      // Refused before any credentials are looked for.
      assertThatThrownBy(() -> TlsSources.create(config))
          .isInstanceOf(CredentialException.class)
          .hasMessageContaining("security.protocol is " + protocol);
    }
    ServerConfig ssl =
        KafkaClients.tlsConfig(
            settings("--tls", "files", "--tls-dir", "/nonexistent").kafka(),
            Map.of("security.protocol", "SSL", "linger.ms", "0"));
    assertThat(ssl.getClustersOrThrow(KafkaClients.CLUSTER).getPropertiesMap())
        .containsEntry("security.protocol", "SSL")
        .containsEntry("linger.ms", "0")
        .containsEntry("bootstrap.servers", "broker:9093");
    // Accepted: what fails now is the missing directory.
    assertThatThrownBy(() -> TlsSources.create(ssl))
        .isInstanceOf(CredentialException.class)
        .hasMessageNotContaining("security.protocol");
  }

  @Test
  void aFailedSendIsUnknownWhenItMayHaveBeenWritten() {
    assertThat(KafkaTransport.outcome(new TimeoutException("expired")).kind())
        .isEqualTo(Transport.Outcome.Kind.UNKNOWN);
    assertThat(KafkaTransport.outcome(new NotLeaderOrFollowerException("moved")).kind())
        .isEqualTo(Transport.Outcome.Kind.UNKNOWN);
    assertThat(KafkaTransport.outcome(new BufferExhaustedException("full")))
        .isEqualTo(
            new Transport.Outcome(
                Transport.Outcome.Kind.FAILED, "BufferExhaustedException", "full"));
    assertThat(KafkaTransport.outcome(new RecordTooLargeException("big")).kind())
        .isEqualTo(Transport.Outcome.Kind.FAILED);
  }
}
