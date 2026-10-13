package io.github.bpalermo.odradek.server.telemetry;

import io.github.bpalermo.odradek.delivery.v1.ReplicaList;
import io.github.bpalermo.odradek.server.admin.Readiness;
import io.github.bpalermo.odradek.server.tls.CredentialSource;
import io.github.bpalermo.odradek.server.tls.TlsSources;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.opentelemetry.api.metrics.Meter;
import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Every metric the server reports, in one place; the Kafka clients' own are listed in {@link
 * KafkaClientMetrics}.
 *
 * <p>Names and attributes are the contract with dashboards and alerts: a change here is a breaking
 * change for whoever reads them. All names start with {@code odradek.}.
 */
public final class ServerMetrics {
  static final AttributeKey<String> TOPIC = AttributeKey.stringKey("topic");
  static final AttributeKey<String> CLUSTER = AttributeKey.stringKey("cluster");
  static final AttributeKey<String> GROUP = AttributeKey.stringKey("group");
  static final AttributeKey<String> OUTCOME = AttributeKey.stringKey("outcome");
  static final AttributeKey<String> ERROR_CODE = AttributeKey.stringKey("error.code");
  static final AttributeKey<Boolean> REQUIRED = AttributeKey.booleanKey("required");
  static final AttributeKey<String> EVENT = AttributeKey.stringKey("event");
  static final AttributeKey<String> REASON = AttributeKey.stringKey("reason");
  static final AttributeKey<String> ACTION = AttributeKey.stringKey("action");

  /**
   * On the TLS metrics, in place of {@code cluster}: the credentials are those of the delivery
   * port, and the value is the configuration section that names them.
   */
  static final AttributeKey<String> DELIVERY = AttributeKey.stringKey("delivery");

  private static final String OK = "ok";
  private static final String ERROR = "error";

  private final Meter meter;
  private final LongCounter produceRecords;
  private final LongCounter produceClusterWrites;
  private final LongUpDownCounter produceStreams;
  private final LongCounter consumeRecords;
  private final LongCounter consumeDecodeErrors;
  private final LongUpDownCounter consumeStreams;
  private final LongCounter consumeRebalances;
  private final LongCounter consumeStreamEnds;
  private final LongUpDownCounter dispatchReceivers;
  private final LongCounter dispatchRecords;
  private final LongCounter dispatchRedeliveries;
  private final LongUpDownCounter dispatchInFlight;
  private final LongUpDownCounter dispatchBlocked;
  private final LongCounter dispatchExhausted;
  private final LongCounter dispatchDeadLetterWrites;
  private final LongCounter dispatchWindowPauses;
  private final LongUpDownCounter dispatchAtWindow;
  private final LongCounter dispatchHandOvers;
  private final LongCounter dispatchHandOverRecords;
  private final LongCounter dispatchDeclarationsRefused;
  private final LongUpDownCounter receiveStreams;
  private final LongUpDownCounter receiveStreamsWanted;
  private final LongCounter receiveRecords;
  private final LongCounter receiveRejects;
  private final LongCounter receiveReconnects;
  private final KafkaClientMetrics kafkaClients;

  public ServerMetrics(Meter meter) {
    this(meter, true);
  }

  private ServerMetrics(Meter meter, boolean exported) {
    this.meter = meter;
    kafkaClients = new KafkaClientMetrics(meter, exported);
    produceRecords =
        meter
            .counterBuilder("odradek.produce.records")
            .setDescription(
                "Records answered to the app, by outcome. An error carries the code the app got.")
            .setUnit("{record}")
            .build();
    produceClusterWrites =
        meter
            .counterBuilder("odradek.produce.cluster.writes")
            .setDescription(
                "Writes of a record to one cluster, by outcome. The only trace of a failure on a"
                    + " best-effort cluster (required = false).")
            .setUnit("{write}")
            .build();
    produceStreams =
        meter
            .upDownCounterBuilder("odradek.produce.streams")
            .setDescription("Open Produce streams.")
            .setUnit("{stream}")
            .build();
    consumeRecords =
        meter
            .counterBuilder("odradek.consume.records")
            .setDescription("Records delivered to the app.")
            .setUnit("{record}")
            .build();
    consumeDecodeErrors =
        meter
            .counterBuilder("odradek.consume.decode.errors")
            .setDescription("Records whose stored bytes the topic's codec could not decode.")
            .setUnit("{record}")
            .build();
    consumeStreams =
        meter
            .upDownCounterBuilder("odradek.consume.streams")
            .setDescription("Open Consume streams. Each one is a Kafka consumer in its group.")
            .setUnit("{stream}")
            .build();
    consumeRebalances =
        meter
            .counterBuilder("odradek.consume.rebalances")
            .setDescription("Rebalance callbacks, by event: assigned, revoked or lost.")
            .setUnit("{event}")
            .build();
    consumeStreamEnds =
        meter
            .counterBuilder("odradek.consume.stream.ends")
            .setDescription(
                "Consume streams ended, by gRPC status. ABORTED is an app that stopped"
                    + " acknowledging.")
            .setUnit("{stream}")
            .build();
    dispatchReceivers =
        meter
            .upDownCounterBuilder("odradek.dispatch.receivers")
            .setDescription(
                "Open Deliver streams: the sidecars a dispatcher hands a topic's records to.")
            .setUnit("{stream}")
            .build();
    dispatchRecords =
        meter
            .counterBuilder("odradek.dispatch.records")
            .setDescription(
                "Records a dispatcher sent to sidecars; a record sent again is counted again.")
            .setUnit("{record}")
            .build();
    dispatchRedeliveries =
        meter
            .counterBuilder("odradek.dispatch.redeliveries")
            .setDescription(
                "Of the records sent, those this replica had sent before, by reason: deadline (the"
                    + " processing deadline passed), rejected (a sidecar could not decode it) or"
                    + " receiver_gone (the sidecar's stream ended).")
            .setUnit("{record}")
            .build();
    dispatchInFlight =
        meter
            .upDownCounterBuilder("odradek.dispatch.in_flight")
            .setDescription("Records sent to a sidecar and not acknowledged, rejected or expired.")
            .setUnit("{record}")
            .build();
    dispatchBlocked =
        meter
            .upDownCounterBuilder("odradek.dispatch.partitions.blocked")
            .setDescription(
                "Partitions stopped at a record that used all its attempts (ON_EXHAUSTED_BLOCK).")
            .setUnit("{partition}")
            .build();
    dispatchExhausted =
        meter
            .counterBuilder("odradek.dispatch.exhausted")
            .setDescription(
                "Records that used all their attempts, by what became of them: block (the"
                    + " partition stops) or dead_letter.")
            .setUnit("{record}")
            .build();
    dispatchDeadLetterWrites =
        meter
            .counterBuilder("odradek.dispatch.dead_letter.writes")
            .setDescription(
                "Writes of a record to its dead-letter topic, by outcome. A write that failed is"
                    + " made again, and nothing is committed past the record meanwhile.")
            .setUnit("{write}")
            .build();
    dispatchWindowPauses =
        meter
            .counterBuilder("odradek.dispatch.window.pauses")
            .setDescription(
                "Times a partition stopped handing out new records because its oldest"
                    + " unacknowledged record is dispatch.partitionWindow offsets behind.")
            .setUnit("{pause}")
            .build();
    dispatchAtWindow =
        meter
            .upDownCounterBuilder("odradek.dispatch.partitions.at_window")
            .setDescription(
                "Partitions that hand out no new record now because of dispatch.partitionWindow.")
            .setUnit("{partition}")
            .build();
    dispatchHandOvers =
        meter
            .counterBuilder("odradek.dispatch.handovers")
            .setDescription(
                "Partitions this replica gave up in good order, by outcome: completed (all it had"
                    + " sent was answered first) or deadline (consume.revokeDeadline ended the"
                    + " wait).")
            .setUnit("{partition}")
            .build();
    dispatchHandOverRecords =
        meter
            .counterBuilder("odradek.dispatch.handover.records")
            .setDescription(
                "Records this replica had sent and left above the committed offset of a partition"
                    + " it gave up: what the next owner sends again.")
            .setUnit("{record}")
            .build();
    dispatchDeclarationsRefused =
        meter
            .counterBuilder("odradek.dispatch.declarations.refused")
            .setDescription("Declarations a dispatcher refused, by gRPC status.")
            .setUnit("{declaration}")
            .build();
    receiveStreams =
        meter
            .upDownCounterBuilder("odradek.receive.streams")
            .setDescription(
                "Deliver streams this server holds to dispatcher replicas, declaration accepted.")
            .setUnit("{stream}")
            .build();
    receiveStreamsWanted =
        meter
            .upDownCounterBuilder("odradek.receive.streams.wanted")
            .setDescription(
                "Deliver streams this server should hold: one per listed dispatcher replica, one"
                    + " while no list is known. More than odradek.receive.streams means a replica"
                    + " that is not reached.")
            .setUnit("{stream}")
            .build();
    receiveRecords =
        meter
            .counterBuilder("odradek.receive.records")
            .setDescription(
                "Records received from a dispatcher; one received again is counted again.")
            .setUnit("{record}")
            .build();
    receiveRejects =
        meter
            .counterBuilder("odradek.receive.rejects")
            .setDescription(
                "Records returned to the dispatcher because the topic's codec could not decode"
                    + " them.")
            .setUnit("{record}")
            .build();
    receiveReconnects =
        meter
            .counterBuilder("odradek.receive.reconnects")
            .setDescription(
                "Deliver streams opened again after one was lost or refused for the time being.")
            .setUnit("{stream}")
            .build();
  }

  /** Metrics that go nowhere, for a server without telemetry and for tests. */
  public static ServerMetrics noop() {
    return new ServerMetrics(OpenTelemetry.noop().getMeter("odradek"), false);
  }

  /**
   * The Kafka clients' own metrics. Every client the server creates is passed through it; when
   * nothing is exported it leaves the client's properties alone.
   */
  public KafkaClientMetrics kafkaClients() {
    return kafkaClients;
  }

  /** A record was answered to the app; {@code errorCode} is null when it was written. */
  public void produced(String topic, String errorCode) {
    if (errorCode == null) {
      produceRecords.add(1, Attributes.of(TOPIC, topic, OUTCOME, OK));
    } else {
      produceRecords.add(1, Attributes.of(TOPIC, topic, OUTCOME, ERROR, ERROR_CODE, errorCode));
    }
  }

  /** One cluster acknowledged, or failed, the write of a record. */
  public void clusterWrite(String topic, String cluster, boolean required, boolean ok) {
    produceClusterWrites.add(
        1,
        Attributes.of(
            TOPIC, topic, CLUSTER, cluster, REQUIRED, required, OUTCOME, ok ? OK : ERROR));
  }

  public void produceStreamOpened() {
    produceStreams.add(1);
  }

  public void produceStreamClosed() {
    produceStreams.add(-1);
  }

  /** Records were delivered to the app, {@code undecodable} of them with a decode error. */
  public void delivered(String topic, long records, long undecodable) {
    consumeRecords.add(records, Attributes.of(TOPIC, topic));
    if (undecodable > 0) {
      consumeDecodeErrors.add(undecodable, Attributes.of(TOPIC, topic));
    }
  }

  public void consumeStreamOpened(String cluster, String group) {
    consumeStreams.add(1, Attributes.of(CLUSTER, cluster, GROUP, group));
  }

  /** A Consume stream ended with the gRPC status code {@code reason}. */
  public void consumeStreamClosed(String cluster, String group, String reason) {
    consumeStreams.add(-1, Attributes.of(CLUSTER, cluster, GROUP, group));
    consumeStreamEnds.add(1, Attributes.of(CLUSTER, cluster, GROUP, group, REASON, reason));
  }

  /** A rebalance callback ran: {@code event} is assigned, revoked or lost. */
  public void rebalanced(String cluster, String group, String event) {
    consumeRebalances.add(1, Attributes.of(CLUSTER, cluster, GROUP, group, EVENT, event));
  }

  /** A declaration was refused with the gRPC status code {@code reason}. */
  public void declarationRefused(String reason) {
    // Nothing of what was declared: whoever reaches the port chooses those names.
    dispatchDeclarationsRefused.add(1, Attributes.of(REASON, reason));
  }

  /** Why a dispatcher sends a record again. The names are the values of {@code reason}. */
  public enum Redelivery {
    DEADLINE("deadline"),
    REJECTED("rejected"),
    RECEIVER_GONE("receiver_gone");

    private final String reason;

    Redelivery(String reason) {
      this.reason = reason;
    }
  }

  /**
   * What a dispatcher reports about one topic of one group. A record sent is on the dispatcher's
   * hot path, so the attributes are built once here.
   */
  public final class DispatchTopic {
    private final Attributes attributes;
    private final Attributes[] redelivered = new Attributes[Redelivery.values().length];
    private final Attributes blocks;
    private final Attributes deadLetters;
    private final Attributes written;
    private final Attributes notWritten;
    private final Attributes completed;
    private final Attributes deadline;

    private DispatchTopic(String cluster, String group, String topic) {
      attributes = Attributes.of(CLUSTER, cluster, GROUP, group, TOPIC, topic);
      for (Redelivery redelivery : Redelivery.values()) {
        redelivered[redelivery.ordinal()] =
            attributes.toBuilder().put(REASON, redelivery.reason).build();
      }
      blocks = attributes.toBuilder().put(ACTION, "block").build();
      deadLetters = attributes.toBuilder().put(ACTION, "dead_letter").build();
      written = attributes.toBuilder().put(OUTCOME, OK).build();
      notWritten = attributes.toBuilder().put(OUTCOME, ERROR).build();
      completed = attributes.toBuilder().put(OUTCOME, "completed").build();
      deadline = attributes.toBuilder().put(OUTCOME, "deadline").build();
    }

    /** Deliver streams opened (positive) or ended (negative). */
    public void receivers(long delta) {
      dispatchReceivers.add(delta, attributes);
    }

    /** A record was sent to a sidecar; {@code again} says why when it had been sent before. */
    public void sent(Redelivery again) {
      dispatchRecords.add(1, attributes);
      if (again != null) {
        dispatchRedeliveries.add(1, redelivered[again.ordinal()]);
      }
    }

    /** The number of records in flight changed by {@code delta}. */
    public void inFlight(long delta) {
      dispatchInFlight.add(delta, attributes);
    }

    /** The number of blocked partitions changed by {@code delta}. */
    public void blocked(long delta) {
      dispatchBlocked.add(delta, attributes);
    }

    /** A record used all its attempts: it is dead-lettered, or blocks its partition. */
    public void exhausted(boolean deadLettered) {
      dispatchExhausted.add(1, deadLettered ? deadLetters : blocks);
    }

    /** The write of a record to its dead-letter topic was acknowledged, or failed. */
    public void deadLetter(boolean ok) {
      dispatchDeadLetterWrites.add(1, ok ? written : notWritten);
    }

    /** Partitions came to be held back by the window, {@code pauses} times. */
    public void windowPauses(long pauses) {
      dispatchWindowPauses.add(pauses, attributes);
    }

    /** The number of partitions held back by the window changed by {@code delta}. */
    public void atWindow(long delta) {
      dispatchAtWindow.add(delta, attributes);
    }

    /**
     * A partition was given up in good order.
     *
     * @param inTime whether everything sent was answered before the revoke deadline
     * @param resent the records sent that the next owner sends again
     */
    public void handedOver(boolean inTime, long resent) {
      dispatchHandOvers.add(1, inTime ? completed : deadline);
      if (resent > 0) {
        dispatchHandOverRecords.add(resent, attributes);
      }
    }
  }

  /** The dispatcher metrics of one topic of one group on one cluster. */
  public DispatchTopic dispatchTopic(String cluster, String group, String topic) {
    return new DispatchTopic(cluster, group, topic);
  }

  /**
   * What a server that receives a topic from a dispatcher reports about it. The cluster is the
   * dispatcher's name for it.
   */
  public final class ReceiveTopic {
    private final Attributes attributes;

    private ReceiveTopic(String cluster, String group, String topic) {
      attributes = Attributes.of(CLUSTER, cluster, GROUP, group, TOPIC, topic);
    }

    /** Deliver streams accepted by a replica (positive) or ended after that (negative). */
    public void streams(long delta) {
      receiveStreams.add(delta, attributes);
    }

    /** The number of Deliver streams the server should hold for the topic changed. */
    public void wanted(long delta) {
      receiveStreamsWanted.add(delta, attributes);
    }

    /** Records arrived from a dispatcher. */
    public void received(long records) {
      receiveRecords.add(records, attributes);
    }

    /** A record could not be decoded and goes back to the dispatcher. */
    public void rejected() {
      receiveRejects.add(1, attributes);
    }

    /** A stream is opened again after one was lost or refused for the time being. */
    public void reconnected() {
      receiveReconnects.add(1, attributes);
    }
  }

  /** The receiving-side metrics of one topic of one group on one cluster. */
  public ReceiveTopic receiveTopic(String cluster, String group, String topic) {
    return new ReceiveTopic(cluster, group, topic);
  }

  /**
   * Reports, on every collection, the list of replicas a dispatcher replica holds: how many they
   * are and the list's generation. The replicas of one dispatcher should agree on both; one that
   * reports fewer replicas, or an older generation, than the others for long has fallen out of the
   * coordination group named by the attributes.
   */
  public void observeReplicas(String cluster, String group, Supplier<ReplicaList> current) {
    Attributes attributes = Attributes.of(CLUSTER, cluster, GROUP, group);
    meter
        .gaugeBuilder("odradek.dispatch.replicas")
        .ofLongs()
        .setDescription(
            "Dispatcher replicas in the list this replica holds and sends to sidecars; 0 until"
                + " it has joined the coordination group.")
        .setUnit("{replica}")
        .buildWithCallback(
            measurement -> measurement.record(current.get().getReplicasCount(), attributes));
    meter
        .gaugeBuilder("odradek.dispatch.replicas.generation")
        .ofLongs()
        .setDescription(
            "Generation of the list of replicas this replica holds. It grows with every change of"
                + " the dispatcher's replicas.")
        .buildWithCallback(
            measurement -> measurement.record(current.get().getGeneration(), attributes));
  }

  /**
   * Reports the state of the TLS credentials and of readiness on every collection.
   *
   * <p>Alert on {@code odradek.tls.certificate.not_after} minus now, not on reload events: a
   * certificate close to expiry is the fact that matters, whatever the reason.
   *
   * <p>One series per set of credentials: those of a cluster under {@code cluster}, those of the
   * delivery port under {@code delivery}.
   */
  public void observe(TlsSources tls, Readiness readiness, Clock clock) {
    Map<Attributes, CredentialSource> sources = new LinkedHashMap<>();
    tls.sources()
        .forEach((cluster, source) -> sources.put(Attributes.of(CLUSTER, cluster), source));
    tls.deliverySources()
        .forEach((section, source) -> sources.put(Attributes.of(DELIVERY, section), source));
    meter
        .gaugeBuilder("odradek.tls.certificate.not_after")
        .ofLongs()
        .setDescription("When the certificate in use expires, as seconds since the epoch.")
        .setUnit("s")
        .buildWithCallback(
            measurement ->
                sources.forEach(
                    (attributes, source) ->
                        measurement.record(
                            source.current().notAfter().getEpochSecond(), attributes)));
    meter
        .counterBuilder("odradek.tls.credential.reloads")
        .setDescription("Times new credentials replaced the ones in use, the first load included.")
        .setUnit("{reload}")
        .buildWithCallback(
            measurement ->
                sources.forEach(
                    (attributes, source) -> measurement.record(source.reloads(), attributes)));
    meter
        .counterBuilder("odradek.tls.credential.reload.failures")
        .setDescription(
            "Times new credentials were rejected or could not be fetched; the ones in use stayed.")
        .setUnit("{failure}")
        .buildWithCallback(
            measurement ->
                sources.forEach(
                    (attributes, source) -> measurement.record(source.failures(), attributes)));
    meter
        .gaugeBuilder("odradek.ready")
        .ofLongs()
        .setDescription("1 when the health service answers SERVING, 0 otherwise.")
        .buildWithCallback(
            measurement ->
                measurement.record(readiness.problems(clock.instant()).isEmpty() ? 1 : 0));
  }
}
