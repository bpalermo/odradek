package io.github.bpalermo.odradek.client;

import java.time.Duration;
import java.util.List;

/**
 * Settings of a {@link Consumer}.
 *
 * @param topics the topics to read. Their cluster and consumer group are the server's
 *     configuration; all of them must resolve to the same cluster and group.
 * @param maxProcessing the longest a handler may take for one record before the server considers
 *     the app hung and evicts it. Zero uses the server default.
 * @param maxUnackedRecords how many records may be delivered ahead of the handler. Zero uses the
 *     default (500).
 */
public record ConsumerOptions(List<String> topics, Duration maxProcessing, int maxUnackedRecords) {
  public ConsumerOptions {
    topics = List.copyOf(topics);
  }

  public static ConsumerOptions topics(String... topics) {
    return new ConsumerOptions(List.of(topics), Duration.ZERO, 0);
  }

  public ConsumerOptions withMaxProcessing(Duration maxProcessing) {
    return new ConsumerOptions(topics, maxProcessing, maxUnackedRecords);
  }

  public ConsumerOptions withMaxUnackedRecords(int records) {
    return new ConsumerOptions(topics, maxProcessing, records);
  }
}
