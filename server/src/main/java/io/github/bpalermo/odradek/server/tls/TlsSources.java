package io.github.bpalermo.odradek.server.tls;

import io.github.bpalermo.kafka.spiffe.kafka.SpiffeConfig;
import io.github.bpalermo.kafka.spiffe.kafka.SpiffeSslEngineFactory;
import io.github.bpalermo.odradek.config.v1.Cluster;
import io.github.bpalermo.odradek.config.v1.FileSource;
import io.github.bpalermo.odradek.config.v1.ListenerTls;
import io.github.bpalermo.odradek.config.v1.ServerConfig;
import io.github.bpalermo.odradek.config.v1.SpiffeSource;
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
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The TLS credentials of every cluster that configures {@code tls}, and those of the delivery port
 * (ADR 6) in either role, kept current for the life of the server; and the Kafka client settings
 * that make a client use a cluster's.
 */
public final class TlsSources implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(TlsSources.class);

  /** How often credential files are re-read. */
  public static final Duration POLL_INTERVAL = Duration.ofSeconds(5);

  private static final Set<String> TLS_PROTOCOLS = Set.of("SSL", "SASL_SSL");

  /** How long the server waits at startup for the first SVID of a Workload API. */
  public static final Duration FIRST_SVID_TIMEOUT = Duration.ofMinutes(2);

  /** Names the Workload API socket when a {@code tls.spiffe} does not. */
  static final String SOCKET_VARIABLE = "SPIFFE_ENDPOINT_SOCKET";

  /** {@code dispatch.tls}: what the delivery metrics and the readiness problems call it. */
  public static final String LISTENER = "dispatch.tls";

  /** {@code dispatcher.tls}, likewise. */
  public static final String DISPATCHER = "dispatcher.tls";

  private static final String UNIX = "unix://";

  private final Map<String, TlsBinding> bindings;
  private final CredentialSource listener;
  private final TlsBinding dispatcher;
  private final List<FileCredentialSource> files;
  private final List<SpiffeCredentialSource> watches;
  private final ScheduledExecutorService poller;

  /** What {@link #load} fills in. */
  private static final class Loading {
    final Map<String, TlsBinding> bindings = new HashMap<>();
    final List<FileCredentialSource> files = new ArrayList<>();
    final Map<String, SpiffeCredentialSource> watches = new TreeMap<>();
    CredentialSource listener;
    TlsBinding dispatcher;
  }

  private TlsSources(Loading loaded) {
    this.bindings = Map.copyOf(loaded.bindings);
    this.listener = loaded.listener;
    this.dispatcher = loaded.dispatcher;
    this.files = List.copyOf(loaded.files);
    this.watches = List.copyOf(loaded.watches.values());
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

  /** Nothing uses TLS credentials. */
  public static TlsSources none() {
    return new TlsSources(new Loading());
  }

  /**
   * Loads the credentials of every cluster with {@code tls}, of {@code dispatch.tls} and of {@code
   * dispatcher.tls}.
   *
   * <p>Whatever names the same Workload API socket shares one source and so one SVID: the library's
   * shared source of the socket, which the clusters' Kafka clients read as well (ADR 9).
   *
   * @throws CredentialException if some credentials cannot be loaded or a cluster's properties ask
   *     for a connection without TLS
   */
  public static TlsSources create(ServerConfig config)
      throws CredentialException, InterruptedException {
    return create(config, System.getenv(SOCKET_VARIABLE), FIRST_SVID_TIMEOUT);
  }

  static TlsSources create(ServerConfig config, String defaultSocket, Duration firstSvidTimeout)
      throws CredentialException, InterruptedException {
    Loading loading = new Loading();
    try {
      load(config, defaultSocket, firstSvidTimeout, loading);
    } catch (CredentialException | InterruptedException | RuntimeException e) {
      loading.watches.values().forEach(SpiffeCredentialSource::close);
      throw e;
    }
    return new TlsSources(loading);
  }

  private static void load(
      ServerConfig config, String defaultSocket, Duration firstSvidTimeout, Loading loading)
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
        if (tls.hasSpiffe()) {
          for (String key : properties.keySet()) {
            if (key.startsWith(SpiffeConfig.PREFIX)) {
              // The library would combine them with what tls says, and so widen it.
              throw new CredentialException(
                  "clusters."
                      + name
                      + ": "
                      + key
                      + " is set, but the server sets the spiffe.* keys of a cluster whose tls has"
                      + " a spiffe source from tls itself; remove it");
            }
          }
        }
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
      CredentialSource source =
          source(
              "clusters." + name + ".tls",
              "cluster " + name,
              tls.hasSpiffe() ? tls.getSpiffe() : null,
              tls.getFiles(),
              defaultSocket,
              firstSvidTimeout,
              loading);
      loading.bindings.put(name, new TlsBinding(source, PeerPolicy.of(tls)));
    }
    if (config.getDispatch().hasTls()) {
      ListenerTls tls = config.getDispatch().getTls();
      loading.listener =
          source(
              LISTENER,
              LISTENER,
              tls.hasSpiffe() ? tls.getSpiffe() : null,
              tls.getFiles(),
              defaultSocket,
              firstSvidTimeout,
              loading);
    }
    if (config.getDispatcher().hasTls()) {
      Tls tls = config.getDispatcher().getTls();
      CredentialSource source =
          source(
              DISPATCHER,
              DISPATCHER,
              tls.hasSpiffe() ? tls.getSpiffe() : null,
              tls.getFiles(),
              defaultSocket,
              firstSvidTimeout,
              loading);
      loading.dispatcher = new TlsBinding(source, PeerPolicy.of(tls));
    }
  }

  /**
   * The source one {@code tls} section names, loaded.
   *
   * @param path the section, for an error
   * @param owner what the credentials are for, for the log
   * @param spiffe its Workload API source, or null when it names {@code files}
   */
  private static CredentialSource source(
      String path,
      String owner,
      SpiffeSource spiffe,
      FileSource files,
      String defaultSocket,
      Duration firstSvidTimeout,
      Loading loading)
      throws CredentialException, InterruptedException {
    if (spiffe != null) {
      String address =
          spiffe.getWorkloadApiSocket().isEmpty() ? defaultSocket : spiffe.getWorkloadApiSocket();
      if (address == null || !address.startsWith(UNIX)) {
        throw new CredentialException(
            path
                + ".spiffe: no Workload API socket; set workloadApiSocket or "
                + SOCKET_VARIABLE
                + " to unix:///path");
      }
      String socket = address.substring(UNIX.length());
      SpiffeCredentialSource watch = loading.watches.get(socket);
      if (watch == null) {
        try {
          watch = SpiffeCredentialSource.open(socket, firstSvidTimeout);
        } catch (CredentialException e) {
          throw new CredentialException(path + ": " + e.getMessage(), e);
        }
        loading.watches.put(socket, watch);
      }
      return watch;
    }
    Path dir = Path.of(files.getDir());
    FileCredentialSource credentials;
    try {
      credentials =
          new FileCredentialSource(
              owner,
              dir.resolve(files.getCertFile().isEmpty() ? "tls.crt" : files.getCertFile()),
              dir.resolve(files.getKeyFile().isEmpty() ? "tls.key" : files.getKeyFile()),
              dir.resolve(files.getCaFile().isEmpty() ? "ca.crt" : files.getCaFile()),
              Clock.systemUTC());
    } catch (CredentialException e) {
      throw new CredentialException(path + ": " + e.getMessage(), e);
    }
    loading.files.add(credentials);
    return credentials;
  }

  /**
   * Adds to a Kafka client's properties what makes it use the cluster's credentials. Does nothing
   * for a cluster without {@code tls}.
   *
   * <p>A {@code spiffe} source gets kafka-spiffe's engine factory (ADR 9), with the keys under
   * which it reads the same shared source as the server: the socket, no SVID selection, the
   * library's MBeans off. A {@code files} source gets the server's own {@link
   * ReloadingSslEngineFactory}.
   */
  public void apply(String cluster, Map<String, Object> properties) {
    TlsBinding binding = bindings.get(cluster);
    if (binding == null) {
      return;
    }
    properties.putIfAbsent(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
    if (binding.source() instanceof SpiffeCredentialSource spiffe) {
      properties.put(
          SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, SpiffeSslEngineFactory.class.getName());
      // Only the server's: tls is what the library is told, whatever else the map holds.
      properties.keySet().removeIf(key -> key.startsWith(SpiffeConfig.PREFIX));
      properties.putAll(spiffeKeys(spiffe.socket(), binding.policy()));
      return;
    }
    properties.put(
        SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, ReloadingSslEngineFactory.class.getName());
    properties.put(ReloadingSslEngineFactory.BINDING_CONFIG, binding);
  }

  /** The library's keys for a socket's shared source and the identities accepted of brokers. */
  static Map<String, Object> spiffeKeys(String socket, PeerPolicy policy) {
    Map<String, Object> keys = new HashMap<>();
    keys.put(SpiffeConfig.ENDPOINT_SOCKET, UNIX + socket);
    keys.put(SpiffeConfig.METRICS_JMX_ENABLED, false);
    if (!policy.spiffeIds().isEmpty()) {
      keys.put(SpiffeConfig.PEER_ALLOWED_IDS, List.copyOf(new TreeSet<>(policy.spiffeIds())));
    }
    if (!policy.trustDomain().isEmpty()) {
      keys.put(SpiffeConfig.PEER_ALLOWED_TRUST_DOMAINS, List.of(policy.trustDomain()));
    }
    return keys;
  }

  /** The credential source of every cluster with {@code tls}, by cluster name. */
  public Map<String, CredentialSource> sources() {
    Map<String, CredentialSource> sources = new TreeMap<>();
    bindings.forEach((cluster, binding) -> sources.put(cluster, binding.source()));
    return sources;
  }

  /**
   * The credentials the dispatcher's listener presents to sidecars ({@code dispatch.tls}); null
   * when the delivery port is plaintext or the server is no dispatcher.
   */
  public CredentialSource listener() {
    return listener;
  }

  /**
   * The credentials presented to the dispatcher and the identities accepted of it ({@code
   * dispatcher.tls}); null when the connection is plaintext or the server receives from none.
   */
  public TlsBinding dispatcher() {
    return dispatcher;
  }

  /**
   * The credential sources of the delivery port, by the configuration section that names them:
   * {@link #LISTENER}, {@link #DISPATCHER}, both or neither.
   */
  public Map<String, CredentialSource> deliverySources() {
    Map<String, CredentialSource> sources = new TreeMap<>();
    if (listener != null) {
      sources.put(LISTENER, listener);
    }
    if (dispatcher != null) {
      sources.put(DISPATCHER, dispatcher.source());
    }
    return sources;
  }

  /**
   * Why some credentials cannot be used at {@code now}; empty when all can. A certificate expires
   * when whatever renews it stopped doing so: the files were not replaced, or the Workload API has
   * been unreachable for the lifetime of an SVID.
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
    deliverySources()
        .forEach(
            (section, source) -> {
              Instant notAfter = source.current().notAfter();
              if (!now.isBefore(notAfter)) {
                problems.add(section + ": the certificate expired at " + notAfter);
              }
            });
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
    watches.forEach(SpiffeCredentialSource::close);
  }
}
