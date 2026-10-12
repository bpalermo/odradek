package io.github.bpalermo.odradek.server.codec;

/** A stored record could not be turned back into the form the app expects. */
public final class RecordDecodeException extends Exception {
  private static final long serialVersionUID = 1L;

  public RecordDecodeException(String message, Throwable cause) {
    super(message, cause);
  }
}
