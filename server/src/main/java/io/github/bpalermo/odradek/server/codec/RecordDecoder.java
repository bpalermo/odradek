package io.github.bpalermo.odradek.server.codec;

import com.google.protobuf.Descriptors.Descriptor;
import java.io.Closeable;
import org.apache.kafka.common.header.Headers;

/**
 * Turns the key and value stored in Kafka for one topic back into what the app expects.
 *
 * <p>Not thread-safe: one instance belongs to one Consume stream, which closes it.
 */
public final class RecordDecoder implements Closeable {
  private final String topic;
  private final SidePipeline key;
  private final Descriptor keyType;
  private final SidePipeline value;
  private final Descriptor valueType;

  RecordDecoder(
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
   * Decodes a record key. A {@code null} key stays {@code null}.
   *
   * @param headers the record's headers. Transforms may change them even when decoding then fails,
   *     so pass a copy if the stored headers are needed afterwards.
   * @throws RecordDecodeException if a transform or the deserializer fails
   */
  public byte[] decodeKey(Headers headers, byte[] data) throws RecordDecodeException {
    return key.decode(topic, headers, keyType, data);
  }

  /**
   * Decodes a record value. A {@code null} value (a tombstone) stays {@code null}.
   *
   * @param headers see {@link #decodeKey}
   * @throws RecordDecodeException if a transform or the deserializer fails
   */
  public byte[] decodeValue(Headers headers, byte[] data) throws RecordDecodeException {
    return value.decode(topic, headers, valueType, data);
  }

  @Override
  public void close() {
    key.close();
    value.close();
  }
}
