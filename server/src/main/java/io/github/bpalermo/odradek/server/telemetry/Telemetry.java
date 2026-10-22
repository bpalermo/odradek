package io.github.bpalermo.odradek.server.telemetry;

import io.github.bpalermo.odradek.config.v1.ServerConfig;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.epoll.EpollIoHandler;
import io.netty.channel.epoll.EpollSocketChannel;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import java.io.Closeable;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The server's metrics and where they go: pushed over OTLP/gRPC to the endpoint the configuration
 * names, on the gRPC stack the server already has. Without an endpoint the metrics are recorded
 * nowhere.
 */
public final class Telemetry implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(Telemetry.class);

  /**
   * The environment variable whose resource attributes are added to the configuration's, as the
   * OpenTelemetry SDKs read it: {@code key=value} pairs separated by commas, values
   * percent-encoded.
   */
  public static final String RESOURCE_ATTRIBUTES_VARIABLE = "OTEL_RESOURCE_ATTRIBUTES";

  /** How often metrics are pushed when the configuration does not say. */
  public static final Duration DEFAULT_INTERVAL = Duration.ofSeconds(30);

  private final ServerMetrics metrics;
  private final SdkMeterProvider provider;
  private final ManagedChannel channel;
  private final EventLoopGroup group;

  private Telemetry(
      ServerMetrics metrics,
      SdkMeterProvider provider,
      ManagedChannel channel,
      EventLoopGroup group) {
    this.metrics = metrics;
    this.provider = provider;
    this.channel = channel;
    this.group = group;
  }

  /**
   * Starts exporting if the configuration names an endpoint.
   *
   * @throws IllegalArgumentException if the endpoint is not {@code http://host:port} or {@code
   *     https://host:port}, or {@value #RESOURCE_ATTRIBUTES_VARIABLE} is malformed
   */
  public static Telemetry create(ServerConfig config) {
    return create(config, System.getenv(RESOURCE_ATTRIBUTES_VARIABLE));
  }

  /**
   * As above, with the value of {@value #RESOURCE_ATTRIBUTES_VARIABLE} given; {@code null} when it
   * is not set.
   */
  static Telemetry create(ServerConfig config, String environmentAttributes) {
    var telemetry = config.getTelemetry();
    if (!telemetry.hasEndpoint() || telemetry.getEndpoint().isEmpty()) {
      return new Telemetry(ServerMetrics.noop(), null, null, null);
    }
    // Before anything is started, so that a malformed variable leaves nothing to close.
    Map<String, String> fromEnvironment = resourceAttributes(environmentAttributes);
    URI endpoint;
    try {
      endpoint = new URI(telemetry.getEndpoint());
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("telemetry.endpoint is not a URL: " + e.getMessage(), e);
    }
    boolean tls = "https".equals(endpoint.getScheme());
    if ((!tls && !"http".equals(endpoint.getScheme()))
        || endpoint.getHost() == null
        || endpoint.getPort() < 0) {
      throw new IllegalArgumentException(
          "telemetry.endpoint must be http://host:port or https://host:port, not "
              + telemetry.getEndpoint());
    }

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
        NettyChannelBuilder.forAddress(endpoint.getHost(), endpoint.getPort())
            .eventLoopGroup(group)
            .channelType(EpollSocketChannel.class);
    if (tls) {
      builder.useTransportSecurity();
    } else {
      builder.usePlaintext();
    }
    ManagedChannel channel = builder.build();

    AttributesBuilder attributes =
        io.opentelemetry.api.common.Attributes.builder()
            .put(AttributeKey.stringKey("service.name"), "odradek");
    telemetry
        .getResourceAttributesMap()
        .forEach((k, v) -> attributes.put(AttributeKey.stringKey(k), v));
    // The environment is the pod's own (its name, from the downward API), where the configuration
    // is shared by every replica: on a key that both set, the environment wins.
    fromEnvironment.forEach((k, v) -> attributes.put(AttributeKey.stringKey(k), v));
    attributes.put(
        SERVICE_INSTANCE_ID,
        serviceInstanceId(
            telemetry.getResourceAttributesMap(), fromEnvironment, UUID.randomUUID()));
    Duration interval =
        telemetry.hasExportInterval()
            ? Duration.ofSeconds(
                telemetry.getExportInterval().getSeconds(),
                telemetry.getExportInterval().getNanos())
            : DEFAULT_INTERVAL;
    @SuppressWarnings("deprecation") // setChannel is how the exporter is given our own channel.
    OtlpGrpcMetricExporter exporter = OtlpGrpcMetricExporter.builder().setChannel(channel).build();
    SdkMeterProvider provider =
        SdkMeterProvider.builder()
            .setResource(Resource.getDefault().merge(Resource.create(attributes.build())))
            .registerMetricReader(
                PeriodicMetricReader.builder(exporter).setInterval(interval).build())
            .build();
    LOG.info("metrics are pushed to {} every {}", telemetry.getEndpoint(), interval);
    return new Telemetry(new ServerMetrics(provider.get("odradek")), provider, channel, group);
  }

  static final AttributeKey<String> SERVICE_INSTANCE_ID =
      AttributeKey.stringKey("service.instance.id");

  /**
   * The {@code service.instance.id} of this process: as the environment or the configuration set
   * it, else the pod's name ({@code k8s.pod.name}), else {@code random}. Every replica must have
   * its own: the translation to Prometheus makes it the {@code instance} label, and replicas
   * without one export the same series, whose cumulative values then overwrite each other.
   */
  static String serviceInstanceId(
      Map<String, String> fromConfiguration, Map<String, String> fromEnvironment, UUID random) {
    for (Map<String, String> attributes : List.of(fromEnvironment, fromConfiguration)) {
      String id = attributes.get(SERVICE_INSTANCE_ID.getKey());
      if (id != null && !id.isBlank()) {
        return id;
      }
    }
    for (Map<String, String> attributes : List.of(fromEnvironment, fromConfiguration)) {
      String pod = attributes.get("k8s.pod.name");
      if (pod != null && !pod.isBlank()) {
        return pod;
      }
    }
    return random.toString();
  }

  /**
   * Parses the value of {@value #RESOURCE_ATTRIBUTES_VARIABLE}. Unset or blank is no attributes;
   * blank entries are skipped.
   *
   * @throws IllegalArgumentException if an entry has no {@code =}, an empty key, or a value that is
   *     not valid percent-encoding. The whole variable is refused rather than read in part: a pod
   *     that reported under half its attributes would be told apart from its siblings by the wrong
   *     ones.
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
            RESOURCE_ATTRIBUTES_VARIABLE
                + " must be key=value pairs separated by commas, not \""
                + entry.strip()
                + "\"");
      }
      String encoded = entry.substring(equals + 1).strip();
      try {
        // Percent-encoding, not form encoding: a '+' is a plus sign.
        attributes.put(key, URLDecoder.decode(encoded.replace("+", "%2B"), StandardCharsets.UTF_8));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            RESOURCE_ATTRIBUTES_VARIABLE + ": the value of " + key + " is not percent-encoded", e);
      }
    }
    return attributes;
  }

  public ServerMetrics metrics() {
    return metrics;
  }

  /** Pushes what was recorded since the last export, then stops. */
  @Override
  public void close() {
    if (provider == null) {
      return;
    }
    provider.shutdown().join(5, TimeUnit.SECONDS);
    channel.shutdownNow();
    group.shutdownGracefully(0, 1, TimeUnit.SECONDS);
  }
}
