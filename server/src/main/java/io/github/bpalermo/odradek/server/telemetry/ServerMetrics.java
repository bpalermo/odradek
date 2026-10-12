package io.github.bpalermo.odradek.server.telemetry;

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
import java.util.Map;

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
                "Of the records sent, those that had been sent before: after a missed deadline, a"
                    + " rejection, or a sidecar that went away.")
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

  /**
   * What a dispatcher reports about one topic of one group. A record sent is on the dispatcher's
   * hot path, so the attributes are built once here.
   */
  public final class DispatchTopic {
    private final Attributes attributes;

    private DispatchTopic(String cluster, String group, String topic) {
      attributes = Attributes.of(CLUSTER, cluster, GROUP, group, TOPIC, topic);
    }

    /** Deliver streams opened (positive) or ended (negative). */
    public void receivers(long delta) {
      dispatchReceivers.add(delta, attributes);
    }

    /** A record was sent to a sidecar; {@code again} when it had been sent before. */
    public void sent(boolean again) {
      dispatchRecords.add(1, attributes);
      if (again) {
        dispatchRedeliveries.add(1, attributes);
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
  }

  /** The dispatcher metrics of one topic of one group on one cluster. */
  public DispatchTopic dispatchTopic(String cluster, String group, String topic) {
    return new DispatchTopic(cluster, group, topic);
  }

  /**
   * Reports the state of the TLS credentials and of readiness on every collection.
   *
   * <p>Alert on {@code odradek.tls.certificate.not_after} minus now, not on reload events: a
   * certificate close to expiry is the fact that matters, whatever the reason.
   */
  public void observe(TlsSources tls, Readiness readiness, Clock clock) {
    Map<String, CredentialSource> sources = tls.sources();
    meter
        .gaugeBuilder("odradek.tls.certificate.not_after")
        .ofLongs()
        .setDescription("When the client certificate in use expires, as seconds since the epoch.")
        .setUnit("s")
        .buildWithCallback(
            measurement ->
                sources.forEach(
                    (cluster, source) ->
                        measurement.record(
                            source.current().notAfter().getEpochSecond(),
                            Attributes.of(CLUSTER, cluster))));
    meter
        .counterBuilder("odradek.tls.credential.reloads")
        .setDescription("Times new credentials replaced the ones in use, the first load included.")
        .setUnit("{reload}")
        .buildWithCallback(
            measurement ->
                sources.forEach(
                    (cluster, source) ->
                        measurement.record(source.reloads(), Attributes.of(CLUSTER, cluster))));
    meter
        .counterBuilder("odradek.tls.credential.reload.failures")
        .setDescription(
            "Times new credentials were rejected or could not be fetched; the ones in use stayed.")
        .setUnit("{failure}")
        .buildWithCallback(
            measurement ->
                sources.forEach(
                    (cluster, source) ->
                        measurement.record(source.failures(), Attributes.of(CLUSTER, cluster))));
    meter
        .gaugeBuilder("odradek.ready")
        .ofLongs()
        .setDescription("1 when the health service answers SERVING, 0 otherwise.")
        .buildWithCallback(
            measurement ->
                measurement.record(readiness.problems(clock.instant()).isEmpty() ? 1 : 0));
  }
}
