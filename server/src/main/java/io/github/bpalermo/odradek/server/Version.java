package io.github.bpalermo.odradek.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

/** The release this server was built as. */
public final class Version {
  /** "0.0.0-dev" between releases. See docs/adr/0004. */
  public static final String VERSION = read();

  private Version() {}

  private static String read() {
    try (InputStream in = Version.class.getResourceAsStream("/odradek-server.version")) {
      return in == null ? "unknown" : new String(in.readAllBytes(), StandardCharsets.UTF_8).trim();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
