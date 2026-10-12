package io.github.bpalermo.odradek.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.bpalermo.odradek.config.v1.Consumer;
import io.github.bpalermo.odradek.config.v1.ConsumerSource;
import io.github.bpalermo.odradek.config.v1.Dispatch;
import io.github.bpalermo.odradek.config.v1.Dispatcher;
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
  void everythingImplementedIsNotReported() {
    ServerConfig config =
        ServerConfig.newBuilder()
            .putConsumers("direct", consumer(ConsumerSource.CONSUMER_SOURCE_KAFKA))
            .putConsumers("default", Consumer.newBuilder().setCluster("main").build())
            // Where a dispatcher is: unused, and harmless, without a consumer that names it.
            .setDispatcher(Dispatcher.newBuilder().setAddress("dispatcher:9465"))
            .build();
    assertThat(Main.notImplemented(config)).isEmpty();
  }

  @Test
  void actingAsADispatcherIsNotImplementedYet() {
    // Even empty: the section is what makes a server a dispatcher.
    ServerConfig config =
        ServerConfig.newBuilder().setDispatch(Dispatch.getDefaultInstance()).build();
    assertThat(Main.notImplemented(config))
        .containsExactly("dispatch is set, and acting as a dispatcher is not implemented yet");
  }

  @Test
  void receivingFromADispatcherIsNotImplementedYet() {
    ServerConfig config =
        ServerConfig.newBuilder()
            .setDispatcher(Dispatcher.newBuilder().setAddress("dispatcher:9465"))
            .putConsumers("payments", consumer(ConsumerSource.CONSUMER_SOURCE_DISPATCHER))
            .putConsumers("orders", consumer(ConsumerSource.CONSUMER_SOURCE_DISPATCHER))
            .putConsumers("audit", consumer(ConsumerSource.CONSUMER_SOURCE_KAFKA))
            .build();
    assertThat(Main.notImplemented(config))
        .containsExactly(
            "consumers.orders.source is CONSUMER_SOURCE_DISPATCHER, and receiving from a dispatcher"
                + " is not implemented yet",
            "consumers.payments.source is CONSUMER_SOURCE_DISPATCHER, and receiving from a"
                + " dispatcher is not implemented yet");
  }

  @Test
  void bothRolesAreReportedTogether() {
    ServerConfig config =
        ServerConfig.newBuilder()
            .setDispatch(Dispatch.getDefaultInstance())
            .putConsumers("orders", consumer(ConsumerSource.CONSUMER_SOURCE_DISPATCHER))
            .build();
    assertThat(Main.notImplemented(config)).hasSize(2);
  }
}
