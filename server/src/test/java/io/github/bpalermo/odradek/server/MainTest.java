package io.github.bpalermo.odradek.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.bpalermo.odradek.config.v1.Consumer;
import io.github.bpalermo.odradek.config.v1.ConsumerSource;
import io.github.bpalermo.odradek.config.v1.Dispatch;
import io.github.bpalermo.odradek.config.v1.ServerConfig;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class MainTest {
  @Test
  void servesTheOdradekService() {
    assertThat(Main.serviceName()).isEqualTo("odradek.kafka.v1.OdradekService");
  }

  @Test
  void configPathDefaultsToTheMountedVolume() {
    assertThat(Main.configPath(new String[0])).isEqualTo(Path.of("/etc/odradek/odradek.yaml"));
  }

  @Test
  void configPathComesFromTheFlag() {
    assertThat(Main.configPath(new String[] {"--config", "/tmp/o.yaml"}))
        .isEqualTo(Path.of("/tmp/o.yaml"));
  }

  @Test
  void otherArgumentsAreRejected() {
    assertThatThrownBy(() -> Main.configPath(new String[] {"--config"}))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> Main.configPath(new String[] {"/tmp/o.yaml"}))
        .isInstanceOf(IllegalArgumentException.class);
  }

  private static Consumer consumer(ConsumerSource source) {
    return Consumer.newBuilder().setCluster("main").setGroupId("g").setSource(source).build();
  }

  @Test
  void theAppsSocketIsOpenedUnlessTheServerOnlyDispatches() {
    ServerConfig.Builder plain = ServerConfig.newBuilder();
    assertThat(Main.servesApp(plain.build())).isTrue();
    ServerConfig.Builder dispatcher =
        ServerConfig.newBuilder().setDispatch(Dispatch.getDefaultInstance());
    assertThat(Main.servesApp(dispatcher.build())).isFalse();
    assertThat(
            Main.servesApp(
                dispatcher
                    .clone()
                    .putConsumers("orders", consumer(ConsumerSource.CONSUMER_SOURCE_KAFKA))
                    .build()))
        .isTrue();
    assertThat(
            Main.servesApp(
                dispatcher
                    .clone()
                    .putProducers(
                        "orders",
                        io.github.bpalermo.odradek.config.v1.Producer.getDefaultInstance())
                    .build()))
        .isTrue();
  }
}
