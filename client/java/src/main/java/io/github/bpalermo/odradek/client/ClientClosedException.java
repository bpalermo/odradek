package io.github.bpalermo.odradek.client;

/** The producer or consumer was closed, or the server refused it for good. */
public final class ClientClosedException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  ClientClosedException(String message) {
    super(message);
  }
}
