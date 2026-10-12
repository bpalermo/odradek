package io.github.bpalermo.odradek.client;

import io.github.bpalermo.odradek.kafka.v1.ClientInfo;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

/** What this library tells the server about itself. */
final class Version {
  /** The release this jar was built as; "0.0.0-dev" between releases. See docs/adr/0004. */
  static final String VERSION = read();

  static final ClientInfo CLIENT_INFO =
      ClientInfo.newBuilder().setLanguage("java").setVersion(VERSION).build();

  private Version() {}

  private static String read() {
    try (InputStream in = Version.class.getResourceAsStream("/odradek-client.version")) {
      return in == null ? "unknown" : new String(in.readAllBytes(), StandardCharsets.UTF_8).trim();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
