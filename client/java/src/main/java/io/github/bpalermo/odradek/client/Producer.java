package io.github.bpalermo.odradek.client;

import io.github.bpalermo.odradek.client.core.ProduceCore;
import io.github.bpalermo.odradek.client.v1.Close;
import io.github.bpalermo.odradek.client.v1.ProduceAction;
import io.github.bpalermo.odradek.client.v1.ProduceEvent;
import io.github.bpalermo.odradek.client.v1.ProduceSetup;
import io.github.bpalermo.odradek.client.v1.SendRejection;
import io.github.bpalermo.odradek.client.v1.StreamEnded;
import io.github.bpalermo.odradek.client.v1.StreamOpened;
import io.github.bpalermo.odradek.kafka.v1.OdradekServiceGrpc;
import io.github.bpalermo.odradek.kafka.v1.ProduceRequest;
import io.github.bpalermo.odradek.kafka.v1.ProduceResponse;
import io.github.bpalermo.odradek.kafka.v1.ProduceResult;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Sends records through the server.
 *
 * <p>{@link #send} returns a future that completes when every cluster the topic requires has
 * acknowledged the record, never earlier. Records sent from one thread reach each partition in the
 * order they were sent. A producer survives the stream breaking: it reconnects and resends without
 * duplicating, and fails a record as outcome unknown only when the server restarted before
 * answering for it.
 *
 * <p>Thread-safe.
 */
public final class Producer implements AutoCloseable {
  private final Object lock = new Object();
  private final OdradekServiceGrpc.OdradekServiceStub stub;
  private final ScheduledExecutorService scheduler;
  private final ProducerOptions options;
  private final ProduceCore core;
  private final Map<Long, CompletableFuture<SendResult>> futures = new HashMap<>();
  private final Backoff backoff = new Backoff();
  private final CompletableFuture<Void> drained = new CompletableFuture<>();
  private StreamObserver<ProduceRequest> requests;
  private long generation;
  private boolean closed;
  private boolean shutDown;
  private Status fatal;

  Producer(
      OdradekServiceGrpc.OdradekServiceStub stub,
      ScheduledExecutorService scheduler,
      ProducerOptions options) {
    this.stub = stub;
    this.scheduler = scheduler;
    this.options = options;
    ProduceSetup.Builder setup =
        ProduceSetup.newBuilder()
            .setSessionId(UUID.randomUUID().toString())
            .setClient(Version.CLIENT_INFO);
    if (options.maxBufferedRecords() > 0) {
      setup.setMaxBufferedRecords(options.maxBufferedRecords());
    }
    if (options.maxBufferedBytes() > 0) {
      setup.setMaxBufferedBytes(options.maxBufferedBytes());
    }
    this.core = new ProduceCore(setup.build());
    synchronized (lock) {
      openStream();
    }
  }

  /**
   * Sends a record.
   *
   * <p>Blocks while the window is full, up to {@code maxBlock}.
   *
   * @return a future that completes with where the record was written, or exceptionally with a
   *     {@link ProduceException} that says whether the record may have been written
   * @throws BufferFullException if the window stayed full, or the record can never fit
   * @throws ClientClosedException if the producer is closed or was refused by the server
   */
  public CompletableFuture<SendResult> send(OutgoingRecord record) {
    ProduceEvent event = ProduceEvent.newBuilder().setSend(record.toProto()).build();
    long deadline = System.nanoTime() + options.maxBlock().toNanos();
    CompletableFuture<SendResult> future = new CompletableFuture<>();
    List<Runnable> after = new ArrayList<>();
    try {
      synchronized (lock) {
        while (true) {
          List<ProduceAction> actions = core.handle(event);
          ProduceAction first = actions.get(0);
          if (first.hasAccepted()) {
            futures.put(first.getAccepted().getSeq(), future);
            perform(actions, after);
            return future;
          }
          SendRejection reason = first.getRejected().getReason();
          if (reason == SendRejection.SEND_REJECTION_TOO_LARGE) {
            throw new BufferFullException("the record is larger than the whole in-flight window");
          }
          if (reason == SendRejection.SEND_REJECTION_CLOSED) {
            throw new ClientClosedException(
                fatal == null
                    ? "the producer is closed"
                    : "the server refused the producer: " + fatal);
          }
          long remaining = deadline - System.nanoTime();
          if (remaining <= 0) {
            throw new BufferFullException(
                "the in-flight window stayed full for " + options.maxBlock());
          }
          try {
            TimeUnit.NANOSECONDS.timedWait(lock, remaining);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BufferFullException("interrupted while waiting for room in the window");
          }
        }
      }
    } finally {
      after.forEach(Runnable::run);
    }
  }

  /**
   * Stops taking records, waits up to a minute for the results of those already sent, and ends the
   * stream. Records still unanswered then stay with the server; their futures do not complete.
   */
  @Override
  public void close() {
    apply(ProduceEvent.newBuilder().setClose(Close.getDefaultInstance()).build(), -1);
    try {
      drained.get(60, TimeUnit.SECONDS);
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
              Status.CANCELLED.withDescription("producer closed").asRuntimeException());
        } catch (RuntimeException e) {
          // The stream was already complete.
        }
        requests = null;
      }
    }
  }

  /** Applies an event that came from the stream of {@code fromGeneration}, or from the app (-1). */
  private void apply(ProduceEvent event, long fromGeneration) {
    List<Runnable> after = new ArrayList<>();
    synchronized (lock) {
      if (fromGeneration >= 0 && fromGeneration != generation) {
        // An event of a stream that has since been replaced.
        return;
      }
      if (event.hasClose()) {
        closed = true;
      }
      if (event.hasStreamEnded()) {
        requests = null;
      }
      if (event.hasReceived() && event.getReceived().hasReady()) {
        backoff.reset();
      }
      perform(core.handle(event), after);
      if (closed && futures.isEmpty()) {
        drained.complete(null);
      }
      lock.notifyAll();
    }
    after.forEach(Runnable::run);
  }

  /** Carries out actions. Called with the lock held; futures are completed after it is released. */
  private void perform(List<ProduceAction> actions, List<Runnable> after) {
    for (ProduceAction action : actions) {
      switch (action.getKindCase()) {
        case TRANSMIT -> {
          if (requests != null) {
            requests.onNext(action.getTransmit());
          }
        }
        case COMPLETED -> {
          ProduceResult result = action.getCompleted().getResult();
          CompletableFuture<SendResult> future = futures.remove(result.getSeq());
          if (future != null) {
            after.add(() -> complete(future, result));
          }
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
        case FATAL ->
            fatal =
                Status.fromCode(Status.Code.valueOf(action.getFatal().getStatusCode()))
                    .withDescription(action.getFatal().getMessage());
        case ACCEPTED, REJECTED, KIND_NOT_SET -> {}
      }
    }
  }

  private static void complete(CompletableFuture<SendResult> future, ProduceResult result) {
    List<SendResult.Write> writes =
        result.getWritesList().stream()
            .map(
                w ->
                    new SendResult.Write(
                        w.getCluster(),
                        w.getTopic(),
                        w.getPartition(),
                        w.getOffset(),
                        w.getTimestampMs()))
            .toList();
    if (result.hasError()) {
      future.completeExceptionally(
          new ProduceException(
              result.getError().getCode(),
              result.getError().getMessage(),
              result.getError().getKafkaException(),
              writes));
    } else {
      future.complete(new SendResult(writes));
    }
  }

  /** Opens a stream and tells the core. Called with the lock held. */
  private void openStream() {
    if (shutDown) {
      return;
    }
    long mine = ++generation;
    requests =
        stub.produce(
            new StreamObserver<ProduceResponse>() {
              @Override
              public void onNext(ProduceResponse response) {
                apply(ProduceEvent.newBuilder().setReceived(response).build(), mine);
              }

              @Override
              public void onError(Throwable t) {
                Status status = Status.fromThrowable(t);
                apply(ended(status.getCode().name(), status.getDescription()), mine);
              }

              @Override
              public void onCompleted() {
                apply(ended("OK", null), mine);
              }
            });
    perform(
        core.handle(
            ProduceEvent.newBuilder().setStreamOpened(StreamOpened.getDefaultInstance()).build()),
        new ArrayList<>());
  }

  private static ProduceEvent ended(String code, String message) {
    StreamEnded.Builder ended = StreamEnded.newBuilder().setStatusCode(code);
    if (message != null) {
      ended.setMessage(message);
    }
    return ProduceEvent.newBuilder().setStreamEnded(ended).build();
  }
}
