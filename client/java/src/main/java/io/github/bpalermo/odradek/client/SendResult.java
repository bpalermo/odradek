package io.github.bpalermo.odradek.client;

import java.util.List;

/** Where an acknowledged record was written: one entry per cluster, in configuration order. */
public record SendResult(List<Write> writes) {
  /** One cluster's acknowledgement. Partition and offset are specific to that cluster. */
  public record Write(String cluster, String topic, int partition, long offset, long timestampMs) {}
}
