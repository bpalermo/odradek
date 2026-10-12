package io.github.bpalermo.odradek.client;

import java.util.List;

/**
 * Hears about changes of a {@link Consumer}. All methods are called on the consumer's own thread,
 * between records, never while a {@link RecordHandler} is running.
 */
public interface ConsumerListener {
  /** These partitions now belong to this consumer. */
  default void assigned(List<Partition> partitions) {}

  /**
   * These partitions are being taken away. Records of them already handled are committed; records
   * not yet handled go to the next owner. Finish or drop any work kept outside the handler before
   * returning: the server waits for this method, up to its revoke deadline.
   */
  default void revoked(List<Partition> partitions) {}

  /** These partitions are gone without notice, for example because the stream broke. */
  default void lost(List<Partition> partitions) {}

  /**
   * The consumer stopped for good: the handler threw, or the server refused or evicted the
   * consumer. Nothing more is delivered. An app that reports readiness should report not ready.
   */
  default void failed(Throwable cause) {}
}
