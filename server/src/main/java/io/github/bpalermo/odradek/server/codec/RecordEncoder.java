package io.github.bpalermo.odradek.server.codec;

import com.google.protobuf.Descriptors.Descriptor;
import org.apache.kafka.common.header.Headers;

/**
 * Turns the key and value an app sent for one topic into the bytes stored in Kafka.
 *
 * <p>Thread-safe as long as the configured serializer and transforms are, which the extension
 * contract requires. Owned by the {@link CodecRegistry} it came from; do not close.
 */
public final class RecordEncoder {
  private final String topic;
  private final SidePipeline key;
  private final Descriptor keyType;
  private final SidePipeline value;
  private final Descriptor valueType;

  RecordEncoder(
      String topic,
      SidePipeline key,
      Descriptor keyType,
      SidePipeline value,
      Descriptor valueType) {
    this.topic = topic;
    this.key = key;
    this.keyType = keyType;
    this.value = value;
    this.valueType = valueType;
  }

  /**
   * Encodes a record key. A {@code null} key stays {@code null}.
   *
   * @param headers the record's headers; transforms may add to them
   * @throws RecordEncodeException if the key is not a valid message of the contract type, or a
   *     serializer or transform fails
   */
  public byte[] encodeKey(Headers headers, byte[] data) throws RecordEncodeException {
    return key.encode(topic, headers, keyType, data);
  }

  /**
   * Encodes a record value. A {@code null} value (a tombstone) stays {@code null}.
   *
   * @param headers the record's headers; transforms may add to them
   * @throws RecordEncodeException if the value is not a valid message of the contract type, or a
   *     serializer or transform fails
   */
  public byte[] encodeValue(Headers headers, byte[] data) throws RecordEncodeException {
    return value.encode(topic, headers, valueType, data);
  }
}
