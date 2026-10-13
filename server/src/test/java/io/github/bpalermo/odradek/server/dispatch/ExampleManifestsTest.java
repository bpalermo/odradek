package io.github.bpalermo.odradek.server.dispatch;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.util.Durations;
import io.github.bpalermo.odradek.config.v1.ConsumerSource;
import io.github.bpalermo.odradek.config.v1.OnExhausted;
import io.github.bpalermo.odradek.config.v1.ServerConfig;
import io.github.bpalermo.odradek.server.admin.AdminServer;
import io.github.bpalermo.odradek.server.config.ConfigLoader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.snakeyaml.engine.v2.api.Load;
import org.snakeyaml.engine.v2.api.LoadSettings;

/**
 * The example manifests under {@code deploy/kubernetes} cannot be applied to a cluster by a test,
 * but what they say about the server can be checked: every configuration in a ConfigMap is loaded
 * by the server's own loader, and what the manifests around it assume of that configuration (ports,
 * the time a replica is given to stop, who is allowed what) is held against it. An example that
 * stops being true fails here.
 */
class ExampleManifestsTest {
  private static final Path ROOT = Path.of("deploy/kubernetes");
  private static final String TRUST_DOMAIN = "example.org";

  /** One document of one file. */
  private record Manifest(Path file, Map<?, ?> content) {
    String kind() {
      return String.valueOf(content.get("kind"));
    }

    String name() {
      return String.valueOf(at("metadata", "name"));
    }

    /** What is at this path of keys and list indices; fails when there is nothing. */
    Object at(Object... path) {
      Object node = content;
      for (Object step : path) {
        node = step instanceof Integer index ? ((List<?>) node).get(index) : map(node).get(step);
        assertThat(node).as("%s of %s in %s", List.of(path), kind(), file).isNotNull();
      }
      return node;
    }
  }

  private static Map<?, ?> map(Object node) {
    assertThat(node).isInstanceOf(Map.class);
    return (Map<?, ?>) node;
  }

  private static List<Manifest> manifests;

  /** The configurations found in ConfigMaps, by the ConfigMap's name. */
  private static Map<String, ServerConfig> configs;

  @BeforeAll
  static void read() throws Exception {
    manifests = new ArrayList<>();
    configs = new TreeMap<>();
    List<Path> files;
    try (var all = Files.walk(ROOT)) {
      files = all.filter(file -> file.toString().endsWith(".yaml")).sorted().toList();
    }
    for (Path file : files) {
      Load load = new Load(LoadSettings.builder().build());
      for (Object document : load.loadAllFromString(Files.readString(file))) {
        if (document instanceof Map<?, ?> content) {
          manifests.add(new Manifest(file, content));
        }
      }
    }
    for (Manifest manifest : manifests) {
      if (!manifest.kind().equals("ConfigMap")) {
        continue;
      }
      for (Map.Entry<?, ?> entry : map(manifest.at("data")).entrySet()) {
        // The server's own loader: an unknown field or a broken rule fails here.
        configs.put(manifest.name(), ConfigLoader.parse(String.valueOf(entry.getValue())));
      }
    }
  }

  private static Manifest manifest(String directory, String kind) {
    List<Manifest> found =
        manifests.stream()
            .filter(m -> m.kind().equals(kind) && m.file().getParent().endsWith(directory))
            .toList();
    assertThat(found).as("%s in %s", kind, directory).hasSize(1);
    return found.get(0);
  }

  private static ServerConfig dispatcher() {
    return configs.get(manifest("dispatcher", "ConfigMap").name());
  }

  private static ServerConfig application() {
    return configs.get(manifest("application", "ConfigMap").name());
  }

  /** The SPIFFE ID a workload has in the usual SPIRE registration for Kubernetes. */
  private static String spiffeId(String directory) {
    return "spiffe://"
        + TRUST_DOMAIN
        + "/ns/"
        + manifest(directory, "Kustomization").at("namespace")
        + "/sa/"
        + manifest(directory, "ServiceAccount").name();
  }

  @Test
  void everyManifestIsReadAndEveryConfigurationIsOneTheServerLoads() throws IOException {
    assertThat(configs).hasSize(2);
    // Every file is listed by its directory's kustomization, and the other way round.
    for (String directory : List.of("dispatcher", "application")) {
      List<String> listed = new ArrayList<>();
      for (Object resource : (List<?>) manifest(directory, "Kustomization").at("resources")) {
        listed.add(String.valueOf(resource));
      }
      List<String> present;
      try (var files = Files.list(ROOT.resolve(directory))) {
        present =
            files
                .map(file -> file.getFileName().toString())
                .filter(name -> !name.equals("kustomization.yaml"))
                .toList();
      }
      assertThat(listed)
          .as("resources of %s", directory)
          .containsExactlyInAnyOrderElementsOf(present);
    }
  }

  @Test
  void theDispatchersConfigurationIsThatOfAServerThatOnlyDispatches() {
    ServerConfig config = dispatcher();
    assertThat(config.hasDispatch()).isTrue();
    assertThat(config.getProducersCount() + config.getConsumersCount()).isZero();
    // Never plaintext in an example, and never an address every replica would share.
    assertThat(config.getDispatch().hasTls()).isTrue();
    assertThat(config.getDispatch().getPolicyCount()).isPositive();
    assertThat(config.getDispatch().hasAdvertisedAddress()).isFalse();
    assertThat(config.getDispatch().getCoordination().getGroupId()).isNotEmpty();
  }

  @Test
  void theDispatchersPodAgreesWithItsConfiguration() {
    ServerConfig config = dispatcher();
    Manifest deployment = manifest("dispatcher", "Deployment");
    Map<?, ?> pod = map(deployment.at("spec", "template", "spec"));
    Map<?, ?> container = map(deployment.at("spec", "template", "spec", "containers", 0));

    int delivery =
        config.getDispatch().hasPort()
            ? config.getDispatch().getPort()
            : DispatchService.DEFAULT_PORT;
    int health =
        config.getAdmin().hasPort() ? config.getAdmin().getPort() : AdminServer.DEFAULT_PORT;
    Map<String, Object> ports = new TreeMap<>();
    for (Object port : (List<?>) container.get("ports")) {
      ports.put(String.valueOf(map(port).get("name")), map(port).get("containerPort"));
    }
    assertThat(ports).containsEntry("delivery", delivery).containsEntry("health", health);
    for (String probe : List.of("startupProbe", "readinessProbe", "livenessProbe")) {
      assertThat(map(map(container.get(probe)).get("grpc")).get("port"))
          .as(probe)
          .isEqualTo(health);
    }
    assertThat(map(map(container.get("startupProbe")).get("grpc")).get("service")).isEqualTo("");
    assertThat(map(map(container.get("readinessProbe")).get("grpc")).get("service")).isEqualTo("");
    assertThat(map(map(container.get("livenessProbe")).get("grpc")).get("service"))
        .isEqualTo("liveness");

    // The replica's own address, from the pod; and the Service in front of the delivery port.
    boolean advertised = false;
    for (Object variable : (List<?>) container.get("env")) {
      if ("ODRADEK_ADVERTISED_HOST".equals(map(variable).get("name"))) {
        advertised = true;
        assertThat(map(map(map(variable).get("valueFrom")).get("fieldRef")).get("fieldPath"))
            .isEqualTo("status.podIP");
      }
    }
    assertThat(advertised).as("ODRADEK_ADVERTISED_HOST is set").isTrue();
    Manifest service = manifest("dispatcher", "Service");
    assertThat(service.at("spec", "ports", 0, "port")).isEqualTo(delivery);
    assertThat(service.at("spec", "ports", 0, "targetPort")).isEqualTo("delivery");
    assertThat(service.at("spec", "selector"))
        .isEqualTo(deployment.at("spec", "selector", "matchLabels"))
        .isEqualTo(manifest("dispatcher", "NetworkPolicy").at("spec", "podSelector", "matchLabels"))
        .isEqualTo(
            manifest("dispatcher", "PodDisruptionBudget").at("spec", "selector", "matchLabels"));
    Map<?, ?> labels = map(deployment.at("spec", "template", "metadata", "labels"));
    map(service.at("spec", "selector"))
        .forEach((label, value) -> assertThat(labels.get(label)).as("%s", label).isEqualTo(value));
    assertThat(manifest("dispatcher", "NetworkPolicy").at("spec", "ingress", 0, "ports", 0, "port"))
        .isEqualTo("delivery");

    // Time to stop in good order: the hand-over wait, what the server allows itself for the
    // commits (DispatchService.shutdownTimeout), and the dead-letter producers and the
    // coordination group after that.
    long revoke =
        config.getConsume().hasRevokeDeadline()
            ? Durations.toSeconds(config.getConsume().getRevokeDeadline())
            : DispatchService.DEFAULT_REVOKE_GRACE.toSeconds();
    assertThat(((Number) pod.get("terminationGracePeriodSeconds")).longValue())
        .as("terminationGracePeriodSeconds")
        .isGreaterThanOrEqualTo(revoke + 35);
    assertThat(deployment.kind()).as("not a StatefulSet").isEqualTo("Deployment");
  }

  @Test
  void theApplicationsConfigurationIsAllowedByTheDispatchersPolicy() {
    ServerConfig dispatcher = dispatcher();
    ServerConfig application = application();
    Policy policy = Policy.of(dispatcher.getDispatch().getPolicyList());
    String identity = spiffeId("application");

    assertThat(application.getConsumersMap()).isNotEmpty();
    application
        .getConsumersMap()
        .forEach(
            (topic, consumer) -> {
              assertThat(consumer.getSource()).isEqualTo(ConsumerSource.CONSUMER_SOURCE_DISPATCHER);
              assertThat(dispatcher.getClustersMap()).containsKey(consumer.getCluster());
              assertThat(
                      policy.allows(
                          identity,
                          consumer.getCluster(),
                          consumer.getGroupId(),
                          topic,
                          consumer.getDeadLetterTopic()))
                  .as("%s may consume %s as the example configures it", identity, topic)
                  .isTrue();
            });
    // The example dead-letters: the path the operations section describes.
    assertThat(application.getConsumersMap().values())
        .anyMatch(c -> c.getOnExhausted() == OnExhausted.ON_EXHAUSTED_DEAD_LETTER);

    // The sidecar reaches the dispatcher's Service, and accepts the dispatcher's identity.
    Manifest service = manifest("dispatcher", "Service");
    assertThat(application.getDispatcher().getAddress())
        .isEqualTo(
            service.name()
                + "."
                + manifest("dispatcher", "Kustomization").at("namespace")
                + ".svc:"
                + service.at("spec", "ports", 0, "port"));
    assertThat(application.getDispatcher().getTls().getAcceptedSpiffeIdsList())
        .containsExactly(spiffeId("dispatcher"));
  }
}
