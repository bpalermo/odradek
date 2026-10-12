package io.github.bpalermo.odradek.server.tls;

import io.github.bpalermo.odradek.config.v1.Cluster;
import io.github.bpalermo.odradek.config.v1.FileSource;
import io.github.bpalermo.odradek.config.v1.ServerConfig;
import io.github.bpalermo.odradek.config.v1.Tls;
import java.io.Closeable;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The TLS credentials of every cluster that configures {@code tls}, kept current for the life of
 * the server, and the Kafka client settings that make a client use them.
 */
public final class TlsSources implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(TlsSources.class);

  /** How often credential files are re-read. */
  public static final Duration POLL_INTERVAL = Duration.ofSeconds(5);

  private static final Set<String> TLS_PROTOCOLS = Set.of("SSL", "SASL_SSL");

  /** How long the server waits at startup for the first SVID of a Workload API. */
  public static final Duration FIRST_SVID_TIMEOUT = Duration.ofMinutes(2);

  /** Names the Workload API socket when a cluster's {@code tls.spiffe} does not. */
  static final String SOCKET_VARIABLE = "SPIFFE_ENDPOINT_SOCKET";

  private static final String UNIX = "unix://";

  private final Map<String, TlsBinding> bindings;
  private final List<FileCredentialSource> files;
  private final List<WorkloadApiSource> watches;
  private final ScheduledExecutorService poller;

  private TlsSources(
      Map<String, TlsBinding> bindings,
      List<FileCredentialSource> files,
      List<WorkloadApiSource> watches) {
    this.bindings = bindings;
    this.files = files;
    this.watches = watches;
    if (files.isEmpty()) {
      this.poller = null;
    } else {
      this.poller =
          Executors.newSingleThreadScheduledExecutor(
              runnable -> {
                Thread thread = new Thread(runnable, "tls-credentials");
                thread.setDaemon(true);
                return thread;
              });
      long millis = POLL_INTERVAL.toMillis();
      poller.scheduleWithFixedDelay(this::poll, millis, millis, TimeUnit.MILLISECONDS);
    }
  }

  /** No cluster uses TLS credentials. */
  public static TlsSources none() {
    return new TlsSources(Map.of(), List.of(), List.of());
  }

  /**
   * Loads the credentials of every cluster with {@code tls}.
   *
   * <p>Clusters that name the same Workload API socket share one watch and so one SVID.
   *
   * @throws CredentialException if a cluster's credentials cannot be loaded or its properties ask
   *     for a connection without TLS
   */
  public static TlsSources create(ServerConfig config)
      throws CredentialException, InterruptedException {
    return create(config, System.getenv(SOCKET_VARIABLE), FIRST_SVID_TIMEOUT);
  }

  static TlsSources create(ServerConfig config, String defaultSocket, Duration firstSvidTimeout)
      throws CredentialException, InterruptedException {
    Map<String, TlsBinding> bindings = new HashMap<>();
    List<FileCredentialSource> files = new ArrayList<>();
    Map<String, WorkloadApiSource> watches = new TreeMap<>();
    try {
      load(config, defaultSocket, firstSvidTimeout, bindings, files, watches);
    } catch (CredentialException | InterruptedException | RuntimeException e) {
      watches.values().forEach(WorkloadApiSource::close);
      throw e;
    }
    return new TlsSources(Map.copyOf(bindings), List.copyOf(files), List.copyOf(watches.values()));
  }

  private static void load(
      ServerConfig config,
      String defaultSocket,
      Duration firstSvidTimeout,
      Map<String, TlsBinding> bindings,
      List<FileCredentialSource> files,
      Map<String, WorkloadApiSource> watches)
      throws CredentialException, InterruptedException {
    for (var entry : new TreeMap<>(config.getClustersMap()).entrySet()) {
      String name = entry.getKey();
      Cluster cluster = entry.getValue();
      if (!cluster.hasTls()) {
        continue;
      }
      Tls tls = cluster.getTls();
      for (var properties :
          List.of(
              cluster.getPropertiesMap(),
              cluster.getProducerPropertiesMap(),
              cluster.getConsumerPropertiesMap())) {
        String protocol = properties.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG);
        if (protocol != null && !TLS_PROTOCOLS.contains(protocol)) {
          throw new CredentialException(
              "clusters."
                  + name
                  + ": security.protocol is "
                  + protocol
                  + " but tls is set; use SSL or SASL_SSL, or leave it out");
        }
      }
      if (tls.hasSpiffe()) {
        String address =
            tls.getSpiffe().getWorkloadApiSocket().isEmpty()
                ? defaultSocket
                : tls.getSpiffe().getWorkloadApiSocket();
        if (address == null || !address.startsWith(UNIX)) {
          throw new CredentialException(
              "clusters."
                  + name
                  + ".tls.spiffe: no Workload API socket; set workloadApiSocket or "
                  + SOCKET_VARIABLE
                  + " to unix:///path");
        }
        String socket = address.substring(UNIX.length());
        WorkloadApiSource watch = watches.get(socket);
        if (watch == null) {
          try {
            watch = new WorkloadApiSource(socket, Clock.systemUTC(), firstSvidTimeout);
          } catch (CredentialException e) {
            throw new CredentialException("clusters." + name + ".tls: " + e.getMessage(), e);
          }
          watches.put(socket, watch);
        }
        bindings.put(name, new TlsBinding(watch, PeerPolicy.of(tls)));
        continue;
      }
      FileSource source = tls.getFiles();
      Path dir = Path.of(source.getDir());
      FileCredentialSource credentials;
      try {
        credentials =
            new FileCredentialSource(
                name,
                dir.resolve(source.getCertFile().isEmpty() ? "tls.crt" : source.getCertFile()),
                dir.resolve(source.getKeyFile().isEmpty() ? "tls.key" : source.getKeyFile()),
                dir.resolve(source.getCaFile().isEmpty() ? "ca.crt" : source.getCaFile()),
                Clock.systemUTC());
      } catch (CredentialException e) {
        throw new CredentialException("clusters." + name + ".tls: " + e.getMessage(), e);
      }
      files.add(credentials);
      bindings.put(name, new TlsBinding(credentials, PeerPolicy.of(tls)));
    }
  }

  /**
   * Adds to a Kafka client's properties what makes it use the cluster's credentials. Does nothing
   * for a cluster without {@code tls}.
   */
  public void apply(String cluster, Map<String, Object> properties) {
    TlsBinding binding = bindings.get(cluster);
    if (binding == null) {
      return;
    }
    properties.putIfAbsent(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
    properties.put(
        SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, ReloadingSslEngineFactory.class.getName());
    properties.put(ReloadingSslEngineFactory.BINDING_CONFIG, binding);
  }

  /**
   * Why the credentials of some cluster cannot be used at {@code now}; empty when all can. A
   * certificate expires when whatever renews it stopped doing so: the files were not replaced, or
   * the Workload API has been unreachable for the lifetime of an SVID.
   */
  public List<String> problems(Instant now) {
    List<String> problems = new ArrayList<>();
    for (var entry : new TreeMap<>(bindings).entrySet()) {
      Instant notAfter = entry.getValue().source().current().notAfter();
      if (!now.isBefore(notAfter)) {
        problems.add(
            "clusters." + entry.getKey() + ".tls: the client certificate expired at " + notAfter);
      }
    }
    return problems;
  }

  private void poll() {
    for (FileCredentialSource source : files) {
      try {
        source.poll();
      } catch (RuntimeException e) {
        LOG.warn("polling TLS credentials failed", e);
      }
    }
  }

  @Override
  public void close() {
    if (poller != null) {
      poller.shutdownNow();
    }
    watches.forEach(WorkloadApiSource::close);
  }
}
