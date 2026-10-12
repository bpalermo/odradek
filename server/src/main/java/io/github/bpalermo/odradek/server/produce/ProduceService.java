package io.github.bpalermo.odradek.server.produce;

import com.google.protobuf.ByteString;
import io.github.bpalermo.odradek.config.v1.ServerConfig;
import io.github.bpalermo.odradek.kafka.v1.ErrorCode;
import io.github.bpalermo.odradek.kafka.v1.Header;
import io.github.bpalermo.odradek.kafka.v1.ProduceReady;
import io.github.bpalermo.odradek.kafka.v1.ProduceRecord;
import io.github.bpalermo.odradek.kafka.v1.ProduceRequest;
import io.github.bpalermo.odradek.kafka.v1.ProduceResponse;
import io.github.bpalermo.odradek.kafka.v1.ProduceResult;
import io.github.bpalermo.odradek.server.codec.RecordEncodeException;
import io.github.bpalermo.odradek.server.produce.ProduceRoutes.Route;
import io.github.bpalermo.odradek.server.produce.ProduceRoutes.Target;
import io.github.bpalermo.odradek.server.telemetry.ServerMetrics;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Produce RPC: sessions, deduplication, the in-flight window, routing by topic and results
 * built only from broker acknowledgements.
 */
public final class ProduceService {
  private static final Logger LOG = LoggerFactory.getLogger(ProduceService.class);

  static final int DEFAULT_MAX_IN_FLIGHT_RECORDS = 1000;
  static final long DEFAULT_MAX_IN_FLIGHT_BYTES = 16L * 1024 * 1024;

  /** A session with no stream and nothing in flight is forgotten after this long. */
  private static final long SESSION_IDLE_NANOS = Duration.ofMinutes(10).toNanos();

  private final String serverEpoch = UUID.randomUUID().toString();
  private final ConcurrentHashMap<String, ProduceSession> sessions = new ConcurrentHashMap<>();
  private final ProduceRoutes routes;
  private final int maxInFlightRecords;
  private final long maxInFlightBytes;
  private final LongSupplier clock;
  private final ServerMetrics metrics;

  public ProduceService(ServerConfig config, ProduceRoutes routes, ServerMetrics metrics) {
    this(config, routes, System::currentTimeMillis, metrics);
  }

  ProduceService(ServerConfig config, ProduceRoutes routes, LongSupplier clock) {
    this(config, routes, clock, ServerMetrics.noop());
  }

  ProduceService(
      ServerConfig config, ProduceRoutes routes, LongSupplier clock, ServerMetrics metrics) {
    this.metrics = metrics;
    this.routes = routes;
    this.maxInFlightRecords =
        config.getProduce().hasMaxInFlightRecords()
            ? config.getProduce().getMaxInFlightRecords()
            : DEFAULT_MAX_IN_FLIGHT_RECORDS;
    this.maxInFlightBytes =
        config.getProduce().hasMaxInFlightBytes()
            ? config.getProduce().getMaxInFlightBytes()
            : DEFAULT_MAX_IN_FLIGHT_BYTES;
    this.clock = clock;
  }

  /** Identifies this server process; a different value tells an app its session state is gone. */
  public String serverEpoch() {
    return serverEpoch;
  }

  /** Handles one Produce stream. */
  public StreamObserver<ProduceRequest> produce(StreamObserver<ProduceResponse> responses) {
    ServerCallStreamObserver<ProduceResponse> call =
        (ServerCallStreamObserver<ProduceResponse>) responses;
    // One message at a time: the next is requested only after this one was handed to every
    // producer, so the order of arrival is the order of send().
    call.disableAutoRequest();
    ProduceStream stream = new ProduceStream(call);
    call.setOnCancelHandler(stream::cancelled);
    call.request(1);
    return stream;
  }

  private ProduceSession session(String id) {
    sessions.values().removeIf(session -> session.idleFor(SESSION_IDLE_NANOS));
    return sessions.computeIfAbsent(id, unused -> new ProduceSession());
  }

  private final class ProduceStream implements StreamObserver<ProduceRequest> {
    private final ServerCallStreamObserver<ProduceResponse> call;
    private final Consumer<ProduceResult> sink = this::send;
    private ProduceSession session;
    private boolean closed;
    private boolean halfClosed;
    private boolean counted;

    ProduceStream(ServerCallStreamObserver<ProduceResponse> call) {
      this.call = call;
    }

    @Override
    public void onNext(ProduceRequest request) {
      try {
        switch (request.getMsgCase()) {
          case OPEN -> open(request);
          case RECORD -> record(request.getRecord());
          case MSG_NOT_SET -> fail(Status.INVALID_ARGUMENT.withDescription("empty ProduceRequest"));
        }
      } catch (RuntimeException e) {
        LOG.error("produce stream failed", e);
        fail(Status.INTERNAL.withDescription(e.toString()));
        return;
      }
      synchronized (this) {
        if (!closed) {
          call.request(1);
        }
      }
    }

    private void open(ProduceRequest request) {
      if (session != null) {
        fail(Status.INVALID_ARGUMENT.withDescription("ProduceOpen was already sent"));
        return;
      }
      String id = request.getOpen().getSessionId();
      if (id.isEmpty()) {
        fail(Status.INVALID_ARGUMENT.withDescription("ProduceOpen.session_id is required"));
        return;
      }
      synchronized (this) {
        if (closed) {
          return;
        }
        call.onNext(
            ProduceResponse.newBuilder()
                .setReady(
                    ProduceReady.newBuilder()
                        .setServerEpoch(serverEpoch)
                        .setMaxInFlightRecords(maxInFlightRecords)
                        .setMaxInFlightBytes(maxInFlightBytes))
                .build());
      }
      // Ready goes out before the stream can receive a result of an earlier stream's record.
      LOG.info(
          "produce stream opened: session {} client {} {}",
          id,
          request.getOpen().getClient().getLanguage(),
          request.getOpen().getClient().getVersion());
      session = session(id);
      synchronized (this) {
        counted = true;
      }
      metrics.produceStreamOpened();
      session.attach(
          sink,
          () ->
              fail(
                  Status.ABORTED.withDescription(
                      "another stream opened session " + id + "; this one is closed")),
          this::finishIfDone);
    }

    private void record(ProduceRecord record) {
      if (session == null) {
        fail(Status.INVALID_ARGUMENT.withDescription("the first message must be ProduceOpen"));
        return;
      }
      long seq = record.getSeq();
      if (seq == 0) {
        fail(Status.INVALID_ARGUMENT.withDescription("ProduceRecord.seq starts at 1"));
        return;
      }
      if (record.hasLowWatermark()) {
        session.prune(
            Long.compareUnsigned(record.getLowWatermark(), seq) > 0
                ? seq
                : record.getLowWatermark());
      }
      switch (session.check(seq)) {
        case DUPLICATE -> {
          return;
        }
        case STALE -> {
          reject(
              record.getTopic(),
              seq,
              ErrorCode.ERROR_CODE_PROTOCOL_VIOLATION,
              "seq " + seq + " is not above the highest seen and has no retained result");
          return;
        }
        case NEW -> {}
      }
      Route route = routes.find(record.getTopic());
      if (route == null) {
        reject(
            record.getTopic(),
            seq,
            ErrorCode.ERROR_CODE_TOPIC_NOT_CONFIGURED,
            "topic " + record.getTopic() + " has no entry under producers");
        return;
      }
      long bytes = size(record);
      if (!session.fits(bytes, maxInFlightRecords, maxInFlightBytes)) {
        reject(
            record.getTopic(),
            seq,
            ErrorCode.ERROR_CODE_PROTOCOL_VIOLATION,
            "the in-flight window is exceeded: wait for results before sending more");
        return;
      }

      Headers headers = new RecordHeaders();
      for (Header header : record.getHeadersList()) {
        headers.add(header.getKey(), header.hasValue() ? header.getValue().toByteArray() : null);
      }
      byte[] key;
      byte[] value;
      try {
        key = route.encoder().encodeKey(headers, bytes(record.hasKey(), record.getKey()));
        value = route.encoder().encodeValue(headers, bytes(record.hasValue(), record.getValue()));
      } catch (RecordEncodeException e) {
        reject(record.getTopic(), seq, ErrorCode.ERROR_CODE_INVALID_RECORD, e.getMessage());
        return;
      }
      Integer partition = record.hasPartition() ? record.getPartition() : null;
      // One timestamp for every cluster, so a consumer can be positioned by time on any of them.
      long timestamp = record.hasTimestampMs() ? record.getTimestampMs() : clock.getAsLong();

      session.begin(seq, bytes);
      List<Target> targets = route.targets();
      String topic = record.getTopic();
      Write write =
          new Write(
              seq,
              targets,
              result -> {
                metrics.produced(
                    topic, result.hasError() ? result.getError().getCode().name() : null);
                session.complete(result);
              });
      // The session lock is not held here: send() may wait for metadata, and the callbacks that
      // need the lock run on the producers' I/O threads.
      for (int i = 0; i < targets.size(); i++) {
        int index = i;
        try {
          targets
              .get(i)
              .producer()
              .send(
                  new ProducerRecord<>(
                      record.getTopic(), partition, timestamp, key, value, copy(headers)),
                  (metadata, failure) -> {
                    metrics.clusterWrite(
                        topic,
                        targets.get(index).cluster(),
                        !targets.get(index).bestEffort(),
                        failure == null);
                    if (failure == null) {
                      write.acked(index, metadata);
                    } else {
                      write.failed(index, failure);
                    }
                  });
        } catch (RuntimeException e) {
          metrics.clusterWrite(
              topic, targets.get(i).cluster(), !targets.get(i).bestEffort(), false);
          write.failed(index, e);
        }
      }
    }

    /** Answers a record that is not handed to Kafka, and remembers the answer for resends. */
    private void reject(String topic, long seq, ErrorCode code, String message) {
      metrics.produced(topic, code.name());
      session.complete(
          ProduceResult.newBuilder().setSeq(seq).setError(ProduceErrors.of(code, message)).build());
    }

    private void send(ProduceResult result) {
      synchronized (this) {
        if (closed) {
          return;
        }
        call.onNext(ProduceResponse.newBuilder().setResult(result).build());
      }
    }

    /** Completes the stream once the app has half-closed and every result has been sent. */
    private void finishIfDone() {
      synchronized (this) {
        if (!halfClosed || closed || (session != null && !session.settled())) {
          return;
        }
        closed = true;
        call.onCompleted();
      }
      detach();
    }

    @Override
    public void onCompleted() {
      synchronized (this) {
        halfClosed = true;
      }
      finishIfDone();
    }

    @Override
    public void onError(Throwable t) {
      cancelled();
    }

    void cancelled() {
      synchronized (this) {
        closed = true;
      }
      detach();
    }

    private void fail(Status status) {
      synchronized (this) {
        if (closed) {
          return;
        }
        closed = true;
        call.onError(status.asRuntimeException());
      }
      detach();
    }

    private void detach() {
      boolean wasCounted;
      synchronized (this) {
        wasCounted = counted;
        counted = false;
      }
      if (wasCounted) {
        metrics.produceStreamClosed();
      }
      if (session != null) {
        session.detach(sink);
      }
    }
  }

  private static byte[] bytes(boolean set, ByteString value) {
    return set ? value.toByteArray() : null;
  }

  /** Each producer gets its own headers: a producer interceptor of one must not affect another. */
  private static Headers copy(Headers headers) {
    return new RecordHeaders(headers.toArray());
  }

  private static long size(ProduceRecord record) {
    long size = (long) record.getKey().size() + record.getValue().size();
    for (Header header : record.getHeadersList()) {
      size += header.getKeyBytes().size() + header.getValue().size();
    }
    return size;
  }
}
