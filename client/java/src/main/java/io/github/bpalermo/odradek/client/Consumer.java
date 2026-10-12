package io.github.bpalermo.odradek.client;

import io.github.bpalermo.odradek.client.core.ConsumeCore;
import io.github.bpalermo.odradek.client.v1.Close;
import io.github.bpalermo.odradek.client.v1.ConsumeAction;
import io.github.bpalermo.odradek.client.v1.ConsumeEvent;
import io.github.bpalermo.odradek.client.v1.ConsumeSetup;
import io.github.bpalermo.odradek.client.v1.StreamEnded;
import io.github.bpalermo.odradek.client.v1.StreamOpened;
import io.github.bpalermo.odradek.kafka.v1.Ack;
import io.github.bpalermo.odradek.kafka.v1.ConsumeRequest;
import io.github.bpalermo.odradek.kafka.v1.ConsumeResponse;
import io.github.bpalermo.odradek.kafka.v1.ConsumedRecord;
import io.github.bpalermo.odradek.kafka.v1.OdradekServiceGrpc;
import io.github.bpalermo.odradek.kafka.v1.PartitionAssignment;
import io.github.bpalermo.odradek.kafka.v1.RecordBatch;
import io.github.bpalermo.odradek.kafka.v1.RevokeDone;
import io.github.bpalermo.odradek.kafka.v1.Revoked;
import io.github.bpalermo.odradek.kafka.v1.TopicPartition;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads records through the server and hands them to a {@link RecordHandler}.
 *
 * <p>A record is marked as processed when the handler returns for it, and only processed records
 * are committed, so a crash or a rebalance redelivers at most what was handed over and not
 * finished. A consumer survives the stream breaking: it reports its partitions lost, reconnects and
 * resumes from the committed offsets.
 *
 * <p>Records are handled on one thread owned by the consumer.
 */
public final class Consumer implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);

  /** Something for the handler thread to do, in order. */
  private sealed interface Work permits Records, Assignment, Revocation, Loss, Failure, Stop {}

  private record Records(RecordBatch batch) implements Work {}

  private record Assignment(List<Partition> partitions) implements Work {}

  private record Revocation(Revoked revoked) implements Work {}

  private record Loss(List<Partition> partitions) implements Work {}

  private record Failure(Throwable cause) implements Work {}

  private record Stop() implements Work {}

  private final Object lock = new Object();
  private final OdradekServiceGrpc.OdradekServiceStub stub;
  private final ScheduledExecutorService scheduler;
  private final RecordHandler handler;
  private final ConsumerListener listener;
  private final ConsumeCore core;
  private final Backoff backoff = new Backoff();
  private final LinkedBlockingDeque<Work> work = new LinkedBlockingDeque<>();

  /** Partitions whose records may be handled, with the epoch they are owned under. */
  private final Map<Partition, Long> live = new ConcurrentHashMap<>();

  /** Partitions being taken away: their records still queued are skipped. */
  private final Set<Partition> leaving = ConcurrentHashMap.newKeySet();

  private final CompletableFuture<Void> ended = new CompletableFuture<>();
  private final Thread thread;
  private StreamObserver<ConsumeRequest> requests;
  private long generation;
  private boolean shutDown;
  private volatile boolean stopped;

  Consumer(
      OdradekServiceGrpc.OdradekServiceStub stub,
      ScheduledExecutorService scheduler,
      ConsumerOptions options,
      RecordHandler handler,
      ConsumerListener listener) {
    this.stub = stub;
    this.scheduler = scheduler;
    this.handler = handler;
    this.listener = listener;
    ConsumeSetup.Builder setup =
        ConsumeSetup.newBuilder().addAllTopics(options.topics()).setClient(Version.CLIENT_INFO);
    if (!options.maxProcessing().isZero()) {
      setup.setMaxProcessingMs((int) options.maxProcessing().toMillis());
    }
    if (options.maxUnackedRecords() > 0) {
      setup.setMaxUnackedRecords(options.maxUnackedRecords());
    }
    this.core = new ConsumeCore(setup.build());
    this.thread = new Thread(this::run, "odradek-consumer-" + String.join(",", options.topics()));
    thread.setDaemon(true);
    thread.start();
    synchronized (lock) {
      openStream();
    }
  }

  /**
   * Stops handling records after the one in progress, lets the server commit what was processed,
   * and leaves the consumer group. Waits up to 30 seconds for that.
   */
  @Override
  public void close() {
    stopped = true;
    work.addFirst(new Stop());
    if (Thread.currentThread() != thread) {
      try {
        thread.join(TimeUnit.SECONDS.toMillis(30));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    apply(ConsumeEvent.newBuilder().setClose(Close.getDefaultInstance()).build(), -1);
    try {
      ended.get(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException e) {
      // Nothing more to wait for.
    }
    synchronized (lock) {
      shutDown = true;
      generation++;
      if (requests != null) {
        try {
          requests.onError(
              Status.CANCELLED.withDescription("consumer closed").asRuntimeException());
        } catch (RuntimeException e) {
          // The stream was already complete.
        }
        requests = null;
      }
    }
  }

  /** The handler thread. */
  private void run() {
    while (true) {
      Work next;
      try {
        next = work.take();
      } catch (InterruptedException e) {
        return;
      }
      try {
        switch (next) {
          case Records records -> handle(records.batch());
          case Assignment assignment -> listener.assigned(assignment.partitions());
          case Revocation revocation -> revoke(revocation.revoked());
          case Loss loss -> listener.lost(loss.partitions());
          case Failure failure -> {
            listener.failed(failure.cause());
            return;
          }
          case Stop stop -> {
            return;
          }
        }
      } catch (RuntimeException e) {
        LOG.error("a consumer listener threw", e);
      }
    }
  }

  private void handle(RecordBatch batch) {
    Partition partition = new Partition(batch.getTopic(), batch.getPartition());
    for (ConsumedRecord record : batch.getRecordsList()) {
      Long epoch = live.get(partition);
      if (stopped
          || epoch == null
          || epoch != batch.getAssignmentEpoch()
          || leaving.contains(partition)) {
        // Not ours to process any more. Unacked, so it is delivered again elsewhere.
        return;
      }
      try {
        handler.handle(received(batch, record));
      } catch (Exception e) {
        // Not acked, and nothing after it is processed: the order within the partition holds for
        // whoever reads it next.
        stopped = true;
        work.clear();
        work.addFirst(new Failure(e));
        scheduler.execute(this::close);
        return;
      }
      apply(
          ConsumeEvent.newBuilder()
              .setAck(
                  Ack.newBuilder()
                      .setTopic(batch.getTopic())
                      .setPartition(batch.getPartition())
                      .setAssignmentEpoch(batch.getAssignmentEpoch())
                      .setFirstOffset(record.getOffset())
                      .setLastOffset(record.getOffset()))
              .build(),
          -1);
    }
  }

  private void revoke(Revoked revoked) {
    List<Partition> partitions = partitions(revoked.getPartitionsList());
    try {
      listener.revoked(partitions);
    } finally {
      partitions.forEach(
          partition -> {
            live.remove(partition);
            leaving.remove(partition);
          });
      apply(
          ConsumeEvent.newBuilder()
              .setRevokeDone(RevokeDone.newBuilder().setRevokeId(revoked.getRevokeId()))
              .build(),
          -1);
    }
  }

  private static ReceivedRecord received(RecordBatch batch, ConsumedRecord record) {
    return new ReceivedRecord(
        batch.getTopic(),
        batch.getPartition(),
        record.getOffset(),
        record.getTimestampMs(),
        record.hasKey() ? record.getKey().toByteArray() : null,
        record.hasValue() ? record.getValue().toByteArray() : null,
        record.getHeadersList().stream()
            .map(
                h ->
                    new ReceivedRecord.Header(
                        h.getKey(), h.hasValue() ? h.getValue().toByteArray() : null))
            .toList(),
        record.hasDecodeError() ? record.getDecodeError() : null);
  }

  private static List<Partition> partitions(List<TopicPartition> partitions) {
    return partitions.stream().map(p -> new Partition(p.getTopic(), p.getPartition())).toList();
  }

  /** Applies an event that came from the stream of {@code fromGeneration}, or from the app (-1). */
  private void apply(ConsumeEvent event, long fromGeneration) {
    synchronized (lock) {
      if (fromGeneration >= 0 && fromGeneration != generation) {
        return;
      }
      if (event.hasStreamEnded()) {
        requests = null;
      }
      if (event.hasReceived() && event.getReceived().hasSubscribed()) {
        backoff.reset();
      }
      perform(core.handle(event));
      if (event.hasStreamEnded() && requests == null && (stopped || shutDown)) {
        ended.complete(null);
      }
    }
  }

  /** Carries out actions. Called with the lock held; app code runs later, on the handler thread. */
  private void perform(List<ConsumeAction> actions) {
    for (ConsumeAction action : actions) {
      switch (action.getKindCase()) {
        case TRANSMIT -> {
          if (requests != null) {
            requests.onNext(action.getTransmit());
          }
        }
        case ASSIGNED -> {
          for (PartitionAssignment p : action.getAssigned().getPartitionsList()) {
            Partition partition = new Partition(p.getTopic(), p.getPartition());
            leaving.remove(partition);
            live.put(partition, p.getAssignmentEpoch());
          }
          work.add(
              new Assignment(
                  action.getAssigned().getPartitionsList().stream()
                      .map(p -> new Partition(p.getTopic(), p.getPartition()))
                      .toList()));
        }
        case DELIVER -> work.add(new Records(action.getDeliver()));
        case REVOKING -> {
          List<Partition> partitions = partitions(action.getRevoking().getPartitionsList());
          leaving.addAll(partitions);
          // Ahead of the records already queued: the server is waiting for the answer.
          work.addFirst(new Revocation(action.getRevoking()));
        }
        case LOST -> {
          List<Partition> partitions = partitions(action.getLost().getPartitionsList());
          partitions.forEach(live::remove);
          work.addFirst(new Loss(partitions));
        }
        case OPEN_STREAM ->
            scheduler.schedule(
                () -> {
                  synchronized (lock) {
                    openStream();
                  }
                },
                backoff.next(),
                TimeUnit.MILLISECONDS);
        case HALF_CLOSE -> {
          if (requests != null) {
            requests.onCompleted();
          }
        }
        case FATAL -> {
          stopped = true;
          work.addFirst(
              new Failure(
                  new ConsumerFailedException(
                      action.getFatal().getStatusCode(), action.getFatal().getMessage())));
        }
        case KIND_NOT_SET -> {}
      }
    }
  }

  /** Opens a stream and tells the core. Called with the lock held. */
  private void openStream() {
    if (shutDown || stopped) {
      return;
    }
    long mine = ++generation;
    requests =
        stub.consume(
            new StreamObserver<ConsumeResponse>() {
              @Override
              public void onNext(ConsumeResponse response) {
                apply(ConsumeEvent.newBuilder().setReceived(response).build(), mine);
              }

              @Override
              public void onError(Throwable t) {
                Status status = Status.fromThrowable(t);
                apply(streamEnded(status.getCode().name(), status.getDescription()), mine);
              }

              @Override
              public void onCompleted() {
                apply(streamEnded("OK", null), mine);
              }
            });
    perform(
        core.handle(
            ConsumeEvent.newBuilder().setStreamOpened(StreamOpened.getDefaultInstance()).build()));
  }

  private static ConsumeEvent streamEnded(String code, String message) {
    StreamEnded.Builder ended = StreamEnded.newBuilder().setStatusCode(code);
    if (message != null) {
      ended.setMessage(message);
    }
    return ConsumeEvent.newBuilder().setStreamEnded(ended).build();
  }
}
