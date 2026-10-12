package io.github.bpalermo.odradek.client;

/** The server refused or evicted a consumer; see the message for the gRPC status. */
public final class ConsumerFailedException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  private final String statusCode;

  ConsumerFailedException(String statusCode, String message) {
    super(statusCode + ": " + message);
    this.statusCode = statusCode;
  }

  /** Name of the gRPC status code, such as "ABORTED" when the app held a record too long. */
  public String statusCode() {
    return statusCode;
  }
}
