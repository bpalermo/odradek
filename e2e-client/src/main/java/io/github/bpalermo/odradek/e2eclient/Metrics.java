package io.github.bpalermo.odradek.e2eclient;

/**
 * What the program reports as metrics, besides {@code /results}: see {@link OtlpMetrics}. Latencies
 * are in milliseconds.
 */
public interface Metrics extends AutoCloseable {
  /** From send to acknowledgement, on the producer's monotonic clock. */
  void produceLatency(double millis);

  /**
   * From send to the start of the handler.
   *
   * @param self whether it was timed on one monotonic clock (a record of this process); otherwise
   *     on two wall clocks, the producer's and this one
   */
  void endToEndLatency(double millis, boolean self);

  /** A record was handed to the producer. */
  void sent();

  /** A record was acknowledged. */
  void acked();

  /** A record was processed for the first time. */
  void processed();

  /** Pushes what was recorded, then stops. */
  @Override
  void close();

  /** Records nothing. */
  static Metrics none() {
    return new Metrics() {
      @Override
      public void produceLatency(double millis) {}

      @Override
      public void endToEndLatency(double millis, boolean self) {}

      @Override
      public void sent() {}

      @Override
      public void acked() {}

      @Override
      public void processed() {}

      @Override
      public void close() {}
    };
  }
}
