package io.github.bpalermo.odradek.client;

/** One partition of one topic. */
public record Partition(String topic, int partition) {
  @Override
  public String toString() {
    return topic + "-" + partition;
  }
}
