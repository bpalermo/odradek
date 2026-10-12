package io.github.bpalermo.odradek.server.dispatch;

import io.github.bpalermo.odradek.delivery.v1.DeliverResponse;
import io.github.bpalermo.odradek.delivery.v1.Record;
import io.github.bpalermo.odradek.delivery.v1.RecordBatch;
import io.github.bpalermo.odradek.delivery.v1.Released;
import io.github.bpalermo.odradek.server.dispatch.DispatchAction.Deliver;
import io.github.bpalermo.odradek.server.dispatch.DispatchAction.Drop;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

/**
 * Turns what the core wants sent into the messages of the {@code Deliver} streams.
 *
 * <p>A {@code RecordBatch} carries records of one partition in offset order. The core's deliveries
 * to one receiver are therefore gathered while they stay in one partition and their offsets
 * increase; a delivery that does neither (another partition, or a record delivered again behind a
 * newer one) starts a new batch. What goes to one receiver keeps the order the core gave it, a
 * {@code Released} included; receivers are independent of each other.
 *
 * <p>A batch is also closed when it has {@code maxRecords} records, or when one more would take it
 * past {@code maxBytes}: a message must stay well under the size a gRPC peer accepts (4 MiB by
 * default). A record larger than that goes alone, in a message of its own size.
 *
 * @param <R> what the core carries for a record
 */
final class Batches<R> {
  private static final class Open {
    final RecordBatch.Builder batch;
    long lastOffset;
    long bytes;

    Open(int partition) {
      batch = RecordBatch.newBuilder().setPartition(partition);
    }
  }

  private final BiFunction<R, Integer, Record> toRecord;
  private final BiConsumer<String, DeliverResponse> sink;
  private final int maxRecords;
  private final long maxBytes;

  /** The batch being filled for each receiver, in the order they were started. */
  private final Map<String, Open> open = new LinkedHashMap<>();

  /**
   * @param toRecord makes the message of a record from what the core carries and the attempt number
   * @param sink is given each message with the receiver it is for
   */
  Batches(
      BiFunction<R, Integer, Record> toRecord,
      BiConsumer<String, DeliverResponse> sink,
      int maxRecords,
      long maxBytes) {
    this.toRecord = toRecord;
    this.sink = sink;
    this.maxRecords = maxRecords;
    this.maxBytes = maxBytes;
  }

  /** Adds one delivery to its receiver's batch, sending the batch before it if it cannot join. */
  void deliver(Deliver<R> delivery) {
    Record record = toRecord.apply(delivery.payload(), delivery.attempt());
    long size = record.getSerializedSize();
    Open batch = open.get(delivery.receiver());
    if (batch != null
        && (batch.batch.getPartition() != delivery.partition()
            || delivery.offset() <= batch.lastOffset
            || batch.batch.getRecordsCount() >= maxRecords
            || batch.bytes + size > maxBytes)) {
      flush(delivery.receiver());
      batch = null;
    }
    if (batch == null) {
      batch = new Open(delivery.partition());
      open.put(delivery.receiver(), batch);
    }
    batch.batch.addRecords(record);
    batch.lastOffset = delivery.offset();
    batch.bytes += size;
  }

  /** Tells a receiver to drop partitions, after whatever is waiting to be sent to it. */
  void released(Drop<R> drop) {
    flush(drop.receiver());
    sink.accept(
        drop.receiver(),
        DeliverResponse.newBuilder()
            .setReleased(Released.newBuilder().addAllPartitions(drop.partitions()))
            .build());
  }

  /** Sends every batch being filled. */
  void flush() {
    open.forEach(
        (receiver, batch) ->
            sink.accept(receiver, DeliverResponse.newBuilder().setRecords(batch.batch).build()));
    open.clear();
  }

  private void flush(String receiver) {
    Open batch = open.remove(receiver);
    if (batch != null) {
      sink.accept(receiver, DeliverResponse.newBuilder().setRecords(batch.batch).build());
    }
  }
}
