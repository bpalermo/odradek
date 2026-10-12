package io.github.bpalermo.odradek.server.produce;

import io.github.bpalermo.odradek.config.v1.ProduceTarget;
import io.github.bpalermo.odradek.config.v1.ServerConfig;
import io.github.bpalermo.odradek.server.codec.CodecRegistry;
import io.github.bpalermo.odradek.server.codec.RecordEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.Producer;

/** Where each produced topic goes: its clusters, in configuration order, and its codec. */
public final class ProduceRoutes {
  /** One cluster a topic is written to. */
  record Target(String cluster, Producer<byte[], byte[]> producer, boolean bestEffort) {}

  /** Everything needed to write one topic. */
  record Route(List<Target> targets, RecordEncoder encoder) {}

  private final Map<String, Route> routes;

  private ProduceRoutes(Map<String, Route> routes) {
    this.routes = routes;
  }

  /** Resolves every entry under {@code producers}. */
  public static ProduceRoutes create(
      ServerConfig config, ClusterProducers producers, CodecRegistry codecs) {
    Map<String, Route> routes = new HashMap<>();
    config
        .getProducersMap()
        .forEach(
            (topic, producer) -> {
              List<Target> targets = new ArrayList<>();
              for (ProduceTarget target : producer.getClustersList()) {
                targets.add(
                    new Target(
                        target.getCluster(),
                        producers.get(target.getCluster()),
                        target.getBestEffort()));
              }
              routes.put(topic, new Route(List.copyOf(targets), codecs.encoder(topic)));
            });
    return new ProduceRoutes(Map.copyOf(routes));
  }

  /** Returns the route of a topic, or {@code null} if it has no entry under {@code producers}. */
  Route find(String topic) {
    return routes.get(topic);
  }
}
