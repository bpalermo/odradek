package io.github.bpalermo.odradek.client;

import io.github.bpalermo.odradek.kafka.v1.ErrorCode;
import java.util.List;

/**
 * A record was not acknowledged by every cluster its topic requires.
 *
 * <p>{@link #code} says whether sending it again is safe, pointless, or may duplicate; {@link
 * #writes} lists the clusters that hold the record anyway.
 */
public final class ProduceException extends Exception {
  private static final long serialVersionUID = 1L;

  private final ErrorCode code;
  private final String kafkaException;
  private final transient List<SendResult.Write> writes;

  ProduceException(
      ErrorCode code, String message, String kafkaException, List<SendResult.Write> writes) {
    super(code + ": " + message);
    this.code = code;
    this.kafkaException = kafkaException;
    this.writes = List.copyOf(writes);
  }

  public ErrorCode code() {
    return code;
  }

  /** Simple class name of the Kafka exception behind this, or empty. For diagnostics. */
  public String kafkaException() {
    return kafkaException;
  }

  /** The clusters that acknowledged the record despite the failure. */
  public List<SendResult.Write> writes() {
    return writes;
  }

  /** Whether the record may have been written: sending it again could then duplicate it. */
  public boolean outcomeUnknown() {
    return code == ErrorCode.ERROR_CODE_OUTCOME_UNKNOWN || code == ErrorCode.ERROR_CODE_INTERNAL;
  }
}
