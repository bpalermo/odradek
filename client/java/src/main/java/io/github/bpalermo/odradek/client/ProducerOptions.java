package io.github.bpalermo.odradek.client;

import java.time.Duration;

/**
 * Settings of a {@link Producer}.
 *
 * @param maxBlock how long {@link Producer#send} waits when the window is full before it fails
 * @param maxBufferedRecords how many records may await a result before the server has announced its
 *     window; afterwards the server's window applies. Zero uses the default (1000).
 * @param maxBufferedBytes the same limit in bytes. Zero uses the default (16 MiB).
 */
public record ProducerOptions(Duration maxBlock, int maxBufferedRecords, long maxBufferedBytes) {
  /** Waits up to a minute for room; default buffer limits. */
  public static ProducerOptions defaults() {
    return new ProducerOptions(Duration.ofSeconds(60), 0, 0);
  }

  public ProducerOptions withMaxBlock(Duration maxBlock) {
    return new ProducerOptions(maxBlock, maxBufferedRecords, maxBufferedBytes);
  }

  public ProducerOptions withMaxBufferedRecords(int records) {
    return new ProducerOptions(maxBlock, records, maxBufferedBytes);
  }
}
