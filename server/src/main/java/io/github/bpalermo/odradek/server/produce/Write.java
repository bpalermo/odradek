package io.github.bpalermo.odradek.server.produce;

import io.github.bpalermo.odradek.kafka.v1.Error;
import io.github.bpalermo.odradek.kafka.v1.ErrorCode;
import io.github.bpalermo.odradek.kafka.v1.ProduceResult;
import io.github.bpalermo.odradek.kafka.v1.RecordMetadata;
import io.github.bpalermo.odradek.server.produce.ProduceRoutes.Target;
import java.util.List;
import java.util.function.Consumer;

/**
 * Collects the outcome of one record on every cluster it is written to and decides when, and as
 * what, the record is reported to the app.
 *
 * <p>Reported as a success as soon as every cluster that is not best effort acknowledged; a
 * best-effort cluster still pending is not waited for. Once a required cluster has failed, the
 * report waits for every cluster to answer, so that the error code and the list of writes say
 * truthfully where the record is.
 *
 * <p>Callbacks arrive on the I/O threads of different producers; every method is synchronized.
 */
final class Write {
  private final long seq;
  private final List<Target> targets;
  private final RecordMetadata[] acked;
  private final Error[] failed;
  private final Consumer<ProduceResult> onResult;
  private int pending;
  private int requiredPending;
  private boolean requiredFailed;
  private boolean reported;

  Write(long seq, List<Target> targets, Consumer<ProduceResult> onResult) {
    this.seq = seq;
    this.targets = targets;
    this.acked = new RecordMetadata[targets.size()];
    this.failed = new Error[targets.size()];
    this.onResult = onResult;
    this.pending = targets.size();
    this.requiredPending = (int) targets.stream().filter(t -> !t.bestEffort()).count();
  }

  /** Cluster {@code index} acknowledged the record. */
  void acked(int index, org.apache.kafka.clients.producer.RecordMetadata metadata) {
    RecordMetadata.Builder write =
        RecordMetadata.newBuilder()
            .setCluster(targets.get(index).cluster())
            .setTopic(metadata.topic())
            .setPartition(metadata.partition())
            .setOffset(metadata.offset());
    if (metadata.hasTimestamp()) {
      write.setTimestampMs(metadata.timestamp());
    }
    settle(index, write.build(), null);
  }

  /** Cluster {@code index} did not acknowledge the record. */
  void failed(int index, Throwable failure) {
    settle(index, null, ProduceErrors.of(targets.get(index).cluster(), failure));
  }

  private void settle(int index, RecordMetadata write, Error error) {
    ProduceResult result;
    synchronized (this) {
      if (acked[index] != null || failed[index] != null) {
        return;
      }
      acked[index] = write;
      failed[index] = error;
      pending--;
      if (!targets.get(index).bestEffort()) {
        requiredPending--;
        requiredFailed |= error != null;
      }
      if (reported || requiredPending > 0 || (requiredFailed && pending > 0)) {
        return;
      }
      reported = true;
      result = result();
    }
    onResult.accept(result);
  }

  private ProduceResult result() {
    ProduceResult.Builder result = ProduceResult.newBuilder().setSeq(seq);
    boolean anyAcked = false;
    for (RecordMetadata write : acked) {
      if (write != null) {
        result.addWrites(write);
        anyAcked = true;
      }
    }
    if (!requiredFailed) {
      return result.build();
    }
    Error first = null;
    boolean certainlyNowhere = !anyAcked;
    for (int i = 0; i < failed.length; i++) {
      if (failed[i] == null) {
        continue;
      }
      if (first == null && !targets.get(i).bestEffort()) {
        first = failed[i];
      }
      certainlyNowhere &= ProduceErrors.notWritten(failed[i].getCode());
    }
    // The first required failure names the cause. Its code stands only if the record is certainly
    // on no cluster; otherwise the honest answer is that the outcome is unknown.
    Error.Builder error = first.toBuilder();
    if (!certainlyNowhere && first.getCode() != ErrorCode.ERROR_CODE_INTERNAL) {
      error.setCode(ErrorCode.ERROR_CODE_OUTCOME_UNKNOWN);
    }
    return result.setError(error).build();
  }
}
