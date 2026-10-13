package io.github.bpalermo.odradek.server.produce;

import io.github.bpalermo.odradek.kafka.v1.Error;
import io.github.bpalermo.odradek.kafka.v1.ErrorCode;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

/** Classifies failures of a produce into the codes of the API. */
final class ProduceErrors {
  private ProduceErrors() {}

  /**
   * Maps a failure reported by a Kafka producer.
   *
   * <p>Only failures known to mean "not written" get a definite code. Everything else from the
   * Kafka client, timeouts included, is {@code OUTCOME_UNKNOWN}: a delivery timeout cannot tell a
   * record that never left from one whose acknowledgement was lost.
   */
  static ErrorCode classify(Throwable failure) {
    return switch (failure) {
      case RecordTooLargeException _ -> ErrorCode.ERROR_CODE_INVALID_RECORD;
      case InvalidTopicException _ -> ErrorCode.ERROR_CODE_INVALID_RECORD;
      case SerializationException _ -> ErrorCode.ERROR_CODE_INVALID_RECORD;
      case UnknownTopicOrPartitionException _ -> ErrorCode.ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION;
      case AuthorizationException _ -> ErrorCode.ERROR_CODE_AUTHORIZATION_FAILED;
      case ProducerFencedException _ -> ErrorCode.ERROR_CODE_FENCED;
      case InvalidProducerEpochException _ -> ErrorCode.ERROR_CODE_FENCED;
      case KafkaException _ -> ErrorCode.ERROR_CODE_OUTCOME_UNKNOWN;
      default -> ErrorCode.ERROR_CODE_INTERNAL;
    };
  }

  /** Whether a code says the record was certainly not written by the cluster that reported it. */
  static boolean notWritten(ErrorCode code) {
    return switch (code) {
      case ERROR_CODE_INVALID_RECORD,
          ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION,
          ERROR_CODE_AUTHORIZATION_FAILED,
          ERROR_CODE_FENCED,
          ERROR_CODE_PROTOCOL_VIOLATION,
          ERROR_CODE_TOPIC_NOT_CONFIGURED ->
          true;
      default -> false;
    };
  }

  static Error of(ErrorCode code, String message) {
    return Error.newBuilder().setCode(code).setMessage(message).build();
  }

  static Error of(String cluster, Throwable failure) {
    Error.Builder error =
        Error.newBuilder()
            .setCode(classify(failure))
            .setMessage("cluster " + cluster + ": " + failure.getMessage());
    if (failure instanceof KafkaException) {
      error.setKafkaException(failure.getClass().getSimpleName());
    }
    return error.build();
  }
}
