package io.github.bpalermo.odradek.client;

import java.util.List;

/**
 * A record handed to a {@link RecordHandler}.
 *
 * @param key the record key, or {@code null}
 * @param value the record value, or {@code null} for a tombstone. For a topic with a contract type,
 *     the protobuf encoding of that type.
 * @param headers the record headers, in order
 * @param decodeError {@code null} normally. Otherwise the server could not decode the record and
 *     the consumer is configured to receive it anyway: key, value and headers are then exactly as
 *     stored in Kafka, and this text says what went wrong.
 */
public record ReceivedRecord(
    String topic,
    int partition,
    long offset,
    long timestampMs,
    byte[] key,
    byte[] value,
    List<Header> headers,
    String decodeError) {

  /** A record header. The value may be {@code null}. */
  public record Header(String key, byte[] value) {}
}
