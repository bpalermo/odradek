package io.github.bpalermo.odradek.server.tls;

/** TLS credentials could not be read, or do not form a usable set. */
public final class CredentialException extends Exception {
  private static final long serialVersionUID = 1L;

  public CredentialException(String message) {
    super(message);
  }

  public CredentialException(String message, Throwable cause) {
    super(message, cause);
  }
}
