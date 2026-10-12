package io.github.bpalermo.odradek.server.codec;

/** A record could not be turned into its stored form. Nothing was sent to Kafka. */
public final class RecordEncodeException extends Exception {
  private static final long serialVersionUID = 1L;

  public RecordEncodeException(String message, Throwable cause) {
    super(message, cause);
  }
}
