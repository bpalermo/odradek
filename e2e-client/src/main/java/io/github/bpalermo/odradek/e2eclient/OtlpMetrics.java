package io.github.bpalermo.odradek.e2eclient;

import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.epoll.EpollIoHandler;
import io.netty.channel.epoll.EpollSocketChannel;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Metrics pushed over OTLP/gRPC, configured the way OpenTelemetry SDKs are: only when {@value
 * #ENDPOINT} is set, with the resource attributes of {@code OTEL_RESOURCE_ATTRIBUTES} (the pod's
 * name, from the downward API). As the server's, on the gRPC stack the program already has.
 *
 * <ul>
 *   <li>{@code e2e.produce.latency}, histogram, ms: send to acknowledgement. Attributes {@code
 *       lane}, {@code transport}.
 *   <li>{@code e2e.end_to_end.latency}, histogram, ms: send to the start of the handler. Attributes
 *       {@code lane}, {@code transport} and {@code clock}: {@code self} for records of this
 *       process, timed on one monotonic clock; {@code cross} for every record, timed on two wall
 *       clocks, for reference only.
 *   <li>{@code e2e.records.sent}, {@code e2e.records.acked}, {@code e2e.records.processed},
 *       counters. Attributes {@code lane}, {@code transport}.
 * </ul>
 */
public final class OtlpMetrics implements Metrics {
  static final String ENDPOINT = "OTEL_EXPORTER_OTLP_ENDPOINT";
  static final String METRICS_ENDPOINT = "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT";
  static final String PROTOCOL = "OTEL_EXPORTER_OTLP_PROTOCOL";
  static final String INTERVAL = "OTEL_METRIC_EXPORT_INTERVAL";
  static final String SERVICE_NAME = "OTEL_SERVICE_NAME";
  static final String RESOURCE_ATTRIBUTES = "OTEL_RESOURCE_ATTRIBUTES";

  /** How often metrics are pushed unless {@value #INTERVAL} says: as the server. */
  static final Duration DEFAULT_INTERVAL = Duration.ofSeconds(30);

  /**
   * Bucket boundaries of the latency histograms, in ms: fine below a millisecond, where a local
   * broker answers, and up to the seconds of a rebalance.
   */
  static final List<Double> BOUNDARIES =
      List.of(
          0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.25, 1.5, 1.75, 2.0, 2.5, 3.0, 3.5,
          4.0, 5.0, 6.0, 7.0, 8.0, 10.0, 12.5, 15.0, 20.0, 25.0, 30.0, 40.0, 50.0, 75.0, 100.0,
          150.0, 200.0, 300.0, 500.0, 750.0, 1000.0, 1500.0, 2000.0, 3000.0, 5000.0, 10000.0,
          30000.0);

  private static final AttributeKey<String> LANE = AttributeKey.stringKey("lane");
  private static final AttributeKey<String> TRANSPORT = AttributeKey.stringKey("transport");
  private static final AttributeKey<String> CLOCK = AttributeKey.stringKey("clock");

  private final SdkMeterProvider provider;
  private final ManagedChannel channel;
  private final EventLoopGroup group;
  private final Attributes attributes;
  private final Attributes self;
  private final Attributes cross;
  private final DoubleHistogram produce;
  private final DoubleHistogram endToEnd;
  private final LongCounter sent;
  private final LongCounter acked;
  private final LongCounter processed;

  private OtlpMetrics(
      SdkMeterProvider provider,
      ManagedChannel channel,
      EventLoopGroup group,
      String lane,
      String transport) {
    this.provider = provider;
    this.channel = channel;
    this.group = group;
    this.attributes = Attributes.of(LANE, lane, TRANSPORT, transport);
    this.self = attributes.toBuilder().put(CLOCK, "self").build();
    this.cross = attributes.toBuilder().put(CLOCK, "cross").build();
    Meter meter = provider.get("odradek-e2e-client");
    this.produce =
        meter
            .histogramBuilder("e2e.produce.latency")
            .setUnit("ms")
            .setDescription("From send to acknowledgement, on the producer's monotonic clock")
            .setExplicitBucketBoundariesAdvice(BOUNDARIES)
            .build();
    this.endToEnd =
        meter
            .histogramBuilder("e2e.end_to_end.latency")
            .setUnit("ms")
            .setDescription(
                "From send to the start of the handler: clock=self on one monotonic clock,"
                    + " clock=cross on two wall clocks (for reference only)")
            .setExplicitBucketBoundariesAdvice(BOUNDARIES)
            .build();
    this.sent =
        meter
            .counterBuilder("e2e.records.sent")
            .setUnit("{record}")
            .setDescription("Records handed to the producer")
            .build();
    this.acked =
        meter
            .counterBuilder("e2e.records.acked")
            .setUnit("{record}")
            .setDescription("Records acknowledged")
            .build();
    this.processed =
        meter
            .counterBuilder("e2e.records.processed")
            .setUnit("{record}")
            .setDescription("Records processed for the first time")
            .build();
  }

  /**
   * Starts pushing if the environment names an endpoint; otherwise records nothing.
   *
   * @throws IllegalArgumentException if a variable is malformed, or asks for a protocol other than
   *     gRPC
   */
  public static Metrics create(Map<String, String> env, String lane, String transport) {
    String address = env.getOrDefault(METRICS_ENDPOINT, env.getOrDefault(ENDPOINT, "")).trim();
    if (address.isEmpty()) {
      return Metrics.none();
    }
    String protocol = env.getOrDefault(PROTOCOL, "grpc").trim();
    if (!protocol.equals("grpc")) {
      throw new IllegalArgumentException(PROTOCOL + " must be grpc, not " + protocol);
    }
    Map<String, String> fromEnvironment = resourceAttributes(env.get(RESOURCE_ATTRIBUTES));
    Duration interval = interval(env.get(INTERVAL));
    URI endpoint;
    try {
      endpoint = new URI(address);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(ENDPOINT + " is not a URL: " + e.getMessage(), e);
    }
    boolean tls = "https".equals(endpoint.getScheme());
    if ((!tls && !"http".equals(endpoint.getScheme())) || endpoint.getHost() == null) {
      throw new IllegalArgumentException(
          ENDPOINT + " must be http://host:port or https://host:port, not " + address);
    }
    int port = endpoint.getPort() < 0 ? 4317 : endpoint.getPort();

    EventLoopGroup group =
        new MultiThreadIoEventLoopGroup(
            1,
            runnable -> {
              Thread thread = new Thread(runnable, "telemetry");
              thread.setDaemon(true);
              return thread;
            },
            EpollIoHandler.newFactory());
    NettyChannelBuilder builder =
        NettyChannelBuilder.forAddress(endpoint.getHost(), port)
            .eventLoopGroup(group)
            .channelType(EpollSocketChannel.class);
    if (tls) {
      builder.useTransportSecurity();
    } else {
      builder.usePlaintext();
    }
    ManagedChannel channel = builder.build();

    AttributesBuilder resource =
        Attributes.builder()
            .put(
                AttributeKey.stringKey("service.name"),
                env.getOrDefault(SERVICE_NAME, "odradek-e2e-client"));
    fromEnvironment.forEach((k, v) -> resource.put(AttributeKey.stringKey(k), v));
    @SuppressWarnings("deprecation") // setChannel is how the exporter is given our own channel.
    OtlpGrpcMetricExporter exporter = OtlpGrpcMetricExporter.builder().setChannel(channel).build();
    SdkMeterProvider provider =
        SdkMeterProvider.builder()
            .setResource(Resource.getDefault().merge(Resource.create(resource.build())))
            .registerMetricReader(
                PeriodicMetricReader.builder(exporter).setInterval(interval).build())
            .build();
    return new OtlpMetrics(provider, channel, group, lane, transport);
  }

  /** Records into a provider the caller made, for tests. */
  static OtlpMetrics of(SdkMeterProvider provider, String lane, String transport) {
    return new OtlpMetrics(provider, null, null, lane, transport);
  }

  private static Duration interval(String millis) {
    if (millis == null || millis.isBlank()) {
      return DEFAULT_INTERVAL;
    }
    try {
      long value = Long.parseLong(millis.trim());
      if (value > 0) {
        return Duration.ofMillis(value);
      }
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(INTERVAL + " must be milliseconds, not " + millis, e);
    }
    throw new IllegalArgumentException(INTERVAL + " must be above 0, not " + millis);
  }

  /**
   * Parses {@code OTEL_RESOURCE_ATTRIBUTES}: {@code key=value} pairs separated by commas, values
   * percent-encoded. Unset or blank is no attributes.
   *
   * @throws IllegalArgumentException if an entry has no key, or a value is not percent-encoded
   */
  static Map<String, String> resourceAttributes(String value) {
    Map<String, String> attributes = new LinkedHashMap<>();
    if (value == null || value.isBlank()) {
      return attributes;
    }
    for (String entry : value.split(",", -1)) {
      if (entry.isBlank()) {
        continue;
      }
      int equals = entry.indexOf('=');
      String key = equals < 0 ? "" : entry.substring(0, equals).strip();
      if (key.isEmpty()) {
        throw new IllegalArgumentException(
            RESOURCE_ATTRIBUTES + " must be key=value pairs, not \"" + entry.strip() + "\"");
      }
      try {
        // Percent-encoding, not form encoding: a '+' is a plus sign.
        attributes.put(
            key,
            URLDecoder.decode(
                entry.substring(equals + 1).strip().replace("+", "%2B"), StandardCharsets.UTF_8));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            RESOURCE_ATTRIBUTES + ": the value of " + key + " is not percent-encoded", e);
      }
    }
    return attributes;
  }

  @Override
  public void produceLatency(double millis) {
    produce.record(millis, attributes);
  }

  @Override
  public void endToEndLatency(double millis, boolean self) {
    // Two clocks can give a negative latency, which an OpenTelemetry histogram drops: recorded as
    // 0, as in /results, which also counts them.
    endToEnd.record(Math.max(0, millis), self ? this.self : cross);
  }

  @Override
  public void sent() {
    sent.add(1, attributes);
  }

  @Override
  public void acked() {
    acked.add(1, attributes);
  }

  @Override
  public void processed() {
    processed.add(1, attributes);
  }

  @Override
  public void close() {
    provider.shutdown().join(5, TimeUnit.SECONDS);
    if (channel != null) {
      channel.shutdownNow();
      group.shutdownGracefully(0, 1, TimeUnit.SECONDS);
    }
  }
}
