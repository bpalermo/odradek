package io.github.bpalermo.odradek.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import io.github.bpalermo.odradek.kafka.v1.Ack;
import io.github.bpalermo.odradek.kafka.v1.ConsumeRequest;
import io.github.bpalermo.odradek.kafka.v1.ConsumeResponse;
import io.github.bpalermo.odradek.kafka.v1.ConsumedRecord;
import io.github.bpalermo.odradek.kafka.v1.Credits;
import io.github.bpalermo.odradek.kafka.v1.OdradekServiceGrpc;
import io.github.bpalermo.odradek.kafka.v1.PartitionAssignment;
import io.github.bpalermo.odradek.kafka.v1.ProduceOpen;
import io.github.bpalermo.odradek.kafka.v1.ProduceRecord;
import io.github.bpalermo.odradek.kafka.v1.ProduceRequest;
import io.github.bpalermo.odradek.kafka.v1.ProduceResponse;
import io.github.bpalermo.odradek.kafka.v1.RecordBatch;
import io.github.bpalermo.odradek.kafka.v1.RevokeDone;
import io.github.bpalermo.odradek.kafka.v1.Subscribe;
import io.github.bpalermo.odradek.kafka.v1.TopicPartition;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The consume path end to end: records produced through the server are read back through it, from a
 * real consumer group on a real cluster.
 */
class ConsumeE2eTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(60);
  private static final int PARTITIONS = 4;

  private final String id = UUID.randomUUID().toString();
  private final String topic = "odradek-e2e-consume-" + id;
  private final String timed = "odradek-e2e-timed-" + id;
  private final String latest = "odradek-e2e-latest-" + id;

  private ServerHarness harness;

  /** The app's end of one Consume stream, keeping track of what it owns and has seen. */
  private final class AppConsumer {
    final BlockingQueue<ConsumeResponse> responses = new LinkedBlockingQueue<>();
    final CompletableFuture<Status> closed = new CompletableFuture<>();
    final Map<String, Long> epochs = new HashMap<>();
    final List<String> values = new ArrayList<>();
    final List<ConsumeResponse> events = new ArrayList<>();
    StreamObserver<ConsumeRequest> requests;

    AppConsumer(String... topics) throws InterruptedException {
      requests =
          OdradekServiceGrpc.newStub(harness.channel)
              .consume(
                  new StreamObserver<ConsumeResponse>() {
                    @Override
                    public void onNext(ConsumeResponse response) {
                      responses.add(response);
                    }

                    @Override
                    public void onError(Throwable t) {
                      closed.complete(Status.fromThrowable(t));
                    }

                    @Override
                    public void onCompleted() {
                      closed.complete(Status.OK);
                    }
                  });
      requests.onNext(
          ConsumeRequest.newBuilder()
              .setSubscribe(Subscribe.newBuilder().addAllTopics(List.of(topics)))
              .build());
      ConsumeResponse first = responses.poll(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
      assertThat(first).isNotNull();
      assertThat(first.hasSubscribed()).as("%s", first).isTrue();
    }

    void credits(int records) {
      requests.onNext(
          ConsumeRequest.newBuilder().setCredits(Credits.newBuilder().setRecords(records)).build());
    }

    /**
     * Handles responses for up to {@code wait}, or until {@code until} values have been seen.
     * Records are acked when {@code ack} is set; revocations are always answered.
     */
    void pump(int until, boolean ack, Duration wait) throws InterruptedException {
      long deadline = System.nanoTime() + wait.toNanos();
      while (values.size() < until) {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
          return;
        }
        ConsumeResponse response = responses.poll(remaining, TimeUnit.NANOSECONDS);
        if (response == null) {
          return;
        }
        events.add(response);
        switch (response.getMsgCase()) {
          case ASSIGNED -> {
            for (PartitionAssignment p : response.getAssigned().getPartitionsList()) {
              epochs.put(p.getTopic() + "/" + p.getPartition(), p.getAssignmentEpoch());
            }
          }
          case REVOKED -> {
            for (TopicPartition p : response.getRevoked().getPartitionsList()) {
              epochs.remove(p.getTopic() + "/" + p.getPartition());
            }
            requests.onNext(
                ConsumeRequest.newBuilder()
                    .setRevokeDone(
                        RevokeDone.newBuilder().setRevokeId(response.getRevoked().getRevokeId()))
                    .build());
          }
          case LOST -> {
            for (TopicPartition p : response.getLost().getPartitionsList()) {
              epochs.remove(p.getTopic() + "/" + p.getPartition());
            }
          }
          case RECORDS -> {
            RecordBatch batch = response.getRecords();
            for (ConsumedRecord record : batch.getRecordsList()) {
              values.add(record.getValue().toStringUtf8());
            }
            if (ack) {
              requests.onNext(
                  ConsumeRequest.newBuilder()
                      .setAck(
                          Ack.newBuilder()
                              .setTopic(batch.getTopic())
                              .setPartition(batch.getPartition())
                              .setAssignmentEpoch(batch.getAssignmentEpoch())
                              .setFirstOffset(batch.getRecords(0).getOffset())
                              .setLastOffset(
                                  batch.getRecords(batch.getRecordsCount() - 1).getOffset()))
                      .build());
            }
          }
          default -> {}
        }
      }
    }

    void finish() throws Exception {
      requests.onCompleted();
      long deadline = System.nanoTime() + TIMEOUT.toNanos();
      while (!closed.isDone() && System.nanoTime() < deadline) {
        pump(Integer.MAX_VALUE, false, Duration.ofMillis(100));
      }
      assertThat(closed.get(1, TimeUnit.SECONDS).getCode()).isEqualTo(Status.Code.OK);
    }
  }

  @BeforeEach
  void start() throws Exception {
    for (String name : List.of(topic, latest)) {
      ServerHarness.createTopic(ServerHarness.KAFKA_A, name, PARTITIONS);
    }
    // Its records are written with timestamps years old: kept whatever their age, or the brokers'
    // retention check removes them when it happens to run before they are read.
    ServerHarness.createTopic(
        ServerHarness.KAFKA_A, timed, PARTITIONS, java.util.Map.of("retention.ms", "-1"));
    harness =
        ServerHarness.start(
            """
            listener:
              socketPath: %s/kafka.sock
            clusters:
              a:
                properties:
                  bootstrap.servers: %s
                consumerProperties:
                  session.timeout.ms: 10000
                  heartbeat.interval.ms: 1000
            consume:
              commitInterval: 0.2s
              revokeDeadline: 5s
            producers:
              %s:
                clusters: [{cluster: a}]
              %s:
                clusters: [{cluster: a}]
              %s:
                clusters: [{cluster: a}]
            consumers:
              %s:
                cluster: a
                groupId: group-%s
              %s:
                cluster: a
                groupId: timed-%s
                startTimestamp: "2024-01-01T00:00:00Z"
              %s:
                cluster: a
                groupId: latest-%s
                offsetReset: OFFSET_RESET_LATEST
            """,
            ServerHarness.KAFKA_A, topic, timed, latest, topic, id, timed, id, latest, id);
  }

  @AfterEach
  void stop() throws Exception {
    harness.stop();
    ServerHarness.deleteTopics(ServerHarness.KAFKA_A, List.of(topic, timed, latest));
  }

  /** Produces through the server and waits for every result. */
  private void produce(String to, String prefix, int count, long timestampMs) throws Exception {
    BlockingQueue<ProduceResponse> results = new LinkedBlockingQueue<>();
    StreamObserver<ProduceRequest> requests =
        OdradekServiceGrpc.newStub(harness.channel)
            .produce(
                new StreamObserver<ProduceResponse>() {
                  @Override
                  public void onNext(ProduceResponse response) {
                    results.add(response);
                  }

                  @Override
                  public void onError(Throwable t) {}

                  @Override
                  public void onCompleted() {}
                });
    requests.onNext(
        ProduceRequest.newBuilder()
            .setOpen(ProduceOpen.newBuilder().setSessionId(UUID.randomUUID().toString()))
            .build());
    for (int i = 1; i <= count; i++) {
      ProduceRecord.Builder record =
          ProduceRecord.newBuilder()
              .setSeq(i)
              .setLowWatermark(1)
              .setTopic(to)
              .setKey(ByteString.copyFromUtf8("key-" + i))
              .setValue(ByteString.copyFromUtf8(prefix + i));
      if (timestampMs > 0) {
        record.setTimestampMs(timestampMs);
      }
      requests.onNext(ProduceRequest.newBuilder().setRecord(record).build());
    }
    for (int i = 0; i <= count; i++) {
      ProduceResponse response = results.poll(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
      assertThat(response).isNotNull();
      assertThat(response.getResult().hasError()).as("%s", response).isFalse();
    }
    requests.onCompleted();
  }

  private static Set<String> expected(String prefix, int from, int to) {
    Set<String> values = new TreeSet<>();
    for (int i = from; i <= to; i++) {
      values.add(prefix + i);
    }
    return values;
  }

  @Test
  void ackedRecordsAreNotRedeliveredAndUnackedOnesAre() throws Exception {
    produce(topic, "v", 100, 0);

    AppConsumer first = new AppConsumer(topic);
    first.credits(1000);
    first.pump(100, true, TIMEOUT);
    assertThat(new TreeSet<>(first.values)).isEqualTo(expected("v", 1, 100));
    assertThat(first.values).as("no duplicates").hasSize(100);
    first.finish();

    produce(topic, "w", 50, 0);

    // Sees the new records, does not ack them, and goes away.
    AppConsumer second = new AppConsumer(topic);
    second.credits(1000);
    second.pump(50, false, TIMEOUT);
    assertThat(new TreeSet<>(second.values)).isEqualTo(expected("w", 1, 50));
    second.finish();

    // The same group again: only what nobody acked.
    AppConsumer third = new AppConsumer(topic);
    third.credits(1000);
    third.pump(50, true, TIMEOUT);
    third.pump(51, true, Duration.ofSeconds(2));
    assertThat(new TreeSet<>(third.values)).isEqualTo(expected("w", 1, 50));
    assertThat(third.values).hasSize(50);
    third.finish();
  }

  @Test
  void creditsBoundWhatIsDelivered() throws Exception {
    produce(topic, "v", 40, 0);
    AppConsumer app = new AppConsumer(topic);
    app.credits(7);
    app.pump(40, true, Duration.ofSeconds(8));
    assertThat(app.values).hasSize(7);

    app.credits(1000);
    app.pump(40, true, TIMEOUT);
    assertThat(new TreeSet<>(app.values)).isEqualTo(expected("v", 1, 40));
    assertThat(app.values).hasSize(40);
    app.finish();
  }

  @Test
  void secondMemberOfTheGroupTakesPartitionsThroughTheRevokeHandshake() throws Exception {
    AppConsumer first = new AppConsumer(topic);
    first.credits(10_000);
    first.pump(1, true, Duration.ofSeconds(10));
    assertThat(first.epochs).as("the first member owns every partition").hasSize(PARTITIONS);

    AppConsumer second = new AppConsumer(topic);
    second.credits(10_000);
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while ((second.epochs.isEmpty() || first.epochs.size() == PARTITIONS)
        && System.nanoTime() < deadline) {
      first.pump(1, true, Duration.ofMillis(200));
      second.pump(1, true, Duration.ofMillis(200));
    }
    assertThat(first.events)
        .as("the first member was asked to give partitions up")
        .anyMatch(ConsumeResponse::hasRevoked);
    Set<String> owned = new HashSet<>(first.epochs.keySet());
    assertThat(owned).doesNotContainAnyElementsOf(second.epochs.keySet());
    owned.addAll(second.epochs.keySet());
    assertThat(owned).hasSize(PARTITIONS);

    produce(topic, "v", 200, 0);
    deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (first.values.size() + second.values.size() < 200 && System.nanoTime() < deadline) {
      first.pump(Integer.MAX_VALUE, true, Duration.ofMillis(100));
      second.pump(Integer.MAX_VALUE, true, Duration.ofMillis(100));
    }
    List<String> all = new ArrayList<>(first.values);
    all.addAll(second.values);
    assertThat(new TreeSet<>(all)).isEqualTo(expected("v", 1, 200));
    assertThat(all).as("each record went to exactly one member").hasSize(200);
    assertThat(first.values).isNotEmpty();
    assertThat(second.values).isNotEmpty();
    first.finish();
    second.finish();
  }

  @Test
  void newGroupStartsAtTheConfiguredTimestamp() throws Exception {
    long before = java.time.Instant.parse("2023-06-01T00:00:00Z").toEpochMilli();
    long after = java.time.Instant.parse("2024-06-01T00:00:00Z").toEpochMilli();
    produce(timed, "old", 20, before);
    produce(timed, "new", 20, after);

    AppConsumer app = new AppConsumer(timed);
    app.credits(1000);
    app.pump(20, true, TIMEOUT);
    app.pump(21, true, Duration.ofSeconds(2));
    assertThat(new TreeSet<>(app.values)).isEqualTo(expected("new", 1, 20));
    app.finish();
  }

  @Test
  void offsetResetLatestSkipsWhatWasThereBeforeTheGroupJoined() throws Exception {
    produce(latest, "old", 20, 0);
    AppConsumer app = new AppConsumer(latest);
    app.credits(1000);
    app.pump(1, true, Duration.ofSeconds(10));
    assertThat(app.epochs).hasSize(PARTITIONS);
    assertThat(app.values).isEmpty();
    // Positions are resolved lazily; give the consumer a moment at the end of each partition.
    app.pump(1, true, Duration.ofSeconds(2));

    produce(latest, "new", 20, 0);
    app.pump(20, true, TIMEOUT);
    assertThat(new TreeSet<>(app.values)).isEqualTo(expected("new", 1, 20));
    app.finish();
  }
}
