package io.github.bpalermo.odradek.server;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class MainTest {
  @Test
  void servesTheGeneratedOdradekService() {
    assertThat(Main.serviceName()).isEqualTo("odradek.kafka.v1.OdradekService");
  }
}
