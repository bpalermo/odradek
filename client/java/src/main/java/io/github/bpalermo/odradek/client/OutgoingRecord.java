package io.github.bpalermo.odradek.client;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import io.github.bpalermo.odradek.kafka.v1.Header;
import io.github.bpalermo.odradek.kafka.v1.ProduceRecord;
import java.nio.charset.StandardCharsets;

/**
 * A record to produce. Build one with {@link #to}:
 *
 * <pre>{@code
 * OutgoingRecord.to("orders.created").key(orderId).value(orderCreated)
 * }</pre>
 *
 * <p>Which cluster or clusters the topic is written to, and in which format it is stored, is the
 * server's configuration. For a topic with a contract type, pass the key and value as protobuf
 * messages of that type.
 */
public final class OutgoingRecord {
  private final ProduceRecord.Builder record;

  private OutgoingRecord(String topic) {
    record = ProduceRecord.newBuilder().setTopic(topic);
  }

  /** Starts a record for {@code topic}. Without a key or value it is a null-keyed tombstone. */
  public static OutgoingRecord to(String topic) {
    return new OutgoingRecord(topic);
  }

  public OutgoingRecord key(byte[] key) {
    record.setKey(ByteString.copyFrom(key));
    return this;
  }

  /** Sets the key to the UTF-8 bytes of {@code key}. */
  public OutgoingRecord key(String key) {
    return key(key.getBytes(StandardCharsets.UTF_8));
  }

  /** Sets the key to a protobuf message, for a topic whose contract declares a key type. */
  public OutgoingRecord key(Message key) {
    record.setKey(key.toByteString());
    return this;
  }

  public OutgoingRecord value(byte[] value) {
    record.setValue(ByteString.copyFrom(value));
    return this;
  }

  /** Sets the value to the UTF-8 bytes of {@code value}. */
  public OutgoingRecord value(String value) {
    return value(value.getBytes(StandardCharsets.UTF_8));
  }

  /** Sets the value to a protobuf message, for a topic whose contract declares a value type. */
  public OutgoingRecord value(Message value) {
    record.setValue(value.toByteString());
    return this;
  }

  /** Adds a header. Headers keep their order and a key may repeat. */
  public OutgoingRecord header(String key, byte[] value) {
    Header.Builder header = Header.newBuilder().setKey(key);
    if (value != null) {
      header.setValue(ByteString.copyFrom(value));
    }
    record.addHeaders(header);
    return this;
  }

  /** Chooses the partition. Without it the server's partitioner decides, by key when present. */
  public OutgoingRecord partition(int partition) {
    record.setPartition(partition);
    return this;
  }

  /** Sets the record timestamp. Without it the server uses its clock when it sends. */
  public OutgoingRecord timestampMs(long timestampMs) {
    record.setTimestampMs(timestampMs);
    return this;
  }

  ProduceRecord toProto() {
    return record.build();
  }
}
