package io.github.bpalermo.odradek.client;

/**
 * A record could not be taken: the window stayed full for the whole {@code maxBlock}, or the record
 * alone is larger than the window. The record was not sent.
 */
public final class BufferFullException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  BufferFullException(String message) {
    super(message);
  }

  BufferFullException(String message, Throwable cause) {
    super(message, cause);
  }
}
