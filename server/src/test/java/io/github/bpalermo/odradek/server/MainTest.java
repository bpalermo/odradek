package io.github.bpalermo.odradek.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
}
