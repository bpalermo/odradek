package io.github.bpalermo.odradek.extension;

import java.io.Closeable;
import java.util.Map;
import org.apache.kafka.common.header.Headers;

/**
 * A byte-to-byte step of a codec pipeline, such as encryption, applied to the serialized form of a
 * record key or value.
 *
 * <p>The lifecycle follows the Kafka client's serializers: the server creates an instance with the
 * public no-argument constructor, calls {@link #configure} once, then {@link #apply} or {@link
 * #reverse} once per record, and {@link #close} when the pipeline is torn down. Both directions
 * live in one class so that they cannot be configured apart.
 *
 * <p>An instance used for producing is shared by every Produce stream and must be thread-safe. An
 * instance used for consuming belongs to one Consume stream.
 *
 * <p>Neither method may block per record: fetch keys and other remote material in {@link
 * #configure}, or cache them. Neither method is called with {@code null} data, and neither may
 * return {@code null}. A transform of record keys must be deterministic, because Kafka partitions
 * and compacts by the stored key.
 */
public interface RecordTransform extends Closeable {

  /**
   * Configures this instance.
   *
   * @param configs the transform's {@code properties} from the server configuration
   * @param isKey whether this instance transforms record keys rather than values
   */
  default void configure(Map<String, ?> configs, boolean isKey) {}

  /**
   * Transforms data on its way to Kafka.
   *
   * @param topic the topic the record is produced to
   * @param headers the record's headers; the transform may add to them, for example a key id
   * @param data the serialized key or value, never {@code null}
   * @return the bytes to store, never {@code null}
   */
  byte[] apply(String topic, Headers headers, byte[] data);

  /**
   * Undoes {@link #apply} on data read from Kafka.
   *
   * @param topic the topic the record was read from
   * @param headers the record's headers; the transform may remove the ones it added
   * @param data the stored key or value, never {@code null}
   * @return the bytes {@link #apply} was given, never {@code null}
   */
  byte[] reverse(String topic, Headers headers, byte[] data);

  @Override
  default void close() {}
}
