package io.github.bpalermo.odradek.client;

import io.github.bpalermo.odradek.kafka.v1.ClientInfo;

/** What this library tells the server about itself. */
final class Version {
  /** Replaced at release time; see docs/adr/0004. */
  static final String VERSION = "0.0.0-dev";

  static final ClientInfo CLIENT_INFO =
      ClientInfo.newBuilder().setLanguage("java").setVersion(VERSION).build();

  private Version() {}
}
