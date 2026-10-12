package io.github.bpalermo.odradek.client;

/**
 * Processes consumed records.
 *
 * <p>Called for one record at a time, in offset order within a partition. Returning normally marks
 * the record as processed: its offset may then be committed. Throwing stops the consumer without
 * marking it; the record, and those after it, are delivered again to whoever reads the partition
 * next.
 */
@FunctionalInterface
public interface RecordHandler {
  void handle(ReceivedRecord record) throws Exception;
}
