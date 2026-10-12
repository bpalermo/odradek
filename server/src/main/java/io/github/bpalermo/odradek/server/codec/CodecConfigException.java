package io.github.bpalermo.odradek.server.codec;

/** The configuration names a codec, class or contract type that cannot be used. */
public final class CodecConfigException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public CodecConfigException(String message) {
    super(message);
  }

  public CodecConfigException(String message, Throwable cause) {
    super(message, cause);
  }
}
