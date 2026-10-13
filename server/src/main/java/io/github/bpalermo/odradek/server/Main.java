package io.github.bpalermo.odradek.server;

import io.github.bpalermo.odradek.config.v1.ServerConfig;
import io.github.bpalermo.odradek.kafka.v1.OdradekServiceGrpc;
import io.github.bpalermo.odradek.server.admin.AdminServer;
import io.github.bpalermo.odradek.server.admin.HealthService;
import io.github.bpalermo.odradek.server.admin.Readiness;
import io.github.bpalermo.odradek.server.admin.RequiredClusters;
import io.github.bpalermo.odradek.server.codec.CodecConfigException;
import io.github.bpalermo.odradek.server.codec.CodecRegistry;
import io.github.bpalermo.odradek.server.codec.ContractRegistry;
import io.github.bpalermo.odradek.server.codec.Extensions;
import io.github.bpalermo.odradek.server.config.ConfigException;
import io.github.bpalermo.odradek.server.config.ConfigLoader;
import io.github.bpalermo.odradek.server.consume.ConsumeService;
import io.github.bpalermo.odradek.server.dispatch.DeliveryServer;
import io.github.bpalermo.odradek.server.dispatch.DispatchService;
import io.github.bpalermo.odradek.server.dispatch.Membership;
import io.github.bpalermo.odradek.server.produce.ClusterProducers;
import io.github.bpalermo.odradek.server.produce.ProduceRoutes;
import io.github.bpalermo.odradek.server.produce.ProduceService;
import io.github.bpalermo.odradek.server.receive.ReceiveService;
import io.github.bpalermo.odradek.server.telemetry.ServerMetrics;
import io.github.bpalermo.odradek.server.telemetry.Telemetry;
import io.github.bpalermo.odradek.server.tls.CredentialException;
import io.github.bpalermo.odradek.server.tls.TlsSources;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point of the odradek server.
 *
 * <p>Usage: {@code server [--config <file>]}. Exit status 2 means the configuration was rejected, 1
 * that the server could not start.
 */
public final class Main {
  private static final Logger LOG = LoggerFactory.getLogger(Main.class);

  /** Read when {@code --config} is not given. */
  static final String DEFAULT_CONFIG = "/etc/odradek/odradek.yaml";

  private Main() {}

  /** Returns the fully qualified gRPC service name this server serves. */
  static String serviceName() {
    return OdradekServiceGrpc.SERVICE_NAME;
  }

  /** Returns the configuration file named by the arguments, or the default. */
  static Path configPath(String[] args) {
    if (args.length == 0) {
      return Path.of(DEFAULT_CONFIG);
    }
    if (args.length == 2 && args[0].equals("--config")) {
      return Path.of(args[1]);
    }
    throw new IllegalArgumentException("usage: server [--config <file>]");
  }

  public static void main(String[] args) throws InterruptedException {
    LOG.info("odradek server starting; will serve {}", serviceName());
    Path file;
    try {
      file = configPath(args);
    } catch (IllegalArgumentException e) {
      LOG.error(e.getMessage());
      System.exit(2);
      return;
    }

    ServerConfig config;
    try {
      config = ConfigLoader.load(file);
    } catch (ConfigException e) {
      LOG.error("configuration {} rejected:", file);
      e.problems().forEach(problem -> LOG.error("  {}", problem));
      System.exit(2);
      return;
    } catch (IOException e) {
      LOG.error("configuration {} cannot be read: {}", file, e.toString());
      System.exit(2);
      return;
    }

    CodecRegistry codecs;
    try {
      codecs = codecs(config);
    } catch (CodecConfigException | IOException e) {
      LOG.error("configuration {} rejected: {}", file, e.getMessage());
      System.exit(2);
      return;
    }

    Telemetry telemetry;
    try {
      telemetry = Telemetry.create(config);
    } catch (IllegalArgumentException e) {
      LOG.error("configuration {} rejected: {}", file, e.getMessage());
      codecs.close();
      System.exit(2);
      return;
    }
    ServerMetrics metrics = telemetry.metrics();

    // Up before anything that can take time, so a probe sees "starting" rather than nothing.
    Readiness readiness = new Readiness();
    HealthService health = new HealthService(readiness, Clock.systemUTC());
    AdminServer admin;
    try {
      admin =
          AdminServer.start(
              config.getAdmin().hasPort() ? config.getAdmin().getPort() : AdminServer.DEFAULT_PORT,
              health);
    } catch (IOException e) {
      LOG.error("could not start the health service: {}", e.toString());
      codecs.close();
      System.exit(1);
      return;
    }

    TlsSources tls;
    try {
      // With a SPIFFE source this waits for the first SVID.
      readiness.starting("loading TLS credentials");
      tls = TlsSources.create(config);
    } catch (CredentialException e) {
      LOG.error("TLS credentials unavailable: {}", e.getMessage());
      codecs.close();
      System.exit(1);
      return;
    }

    ClusterProducers producers;
    try {
      producers = ClusterProducers.create(config, tls, metrics);
    } catch (RuntimeException e) {
      LOG.error("configuration {} rejected: {}", file, e.getMessage());
      tls.close();
      codecs.close();
      System.exit(2);
      return;
    }
    ProduceService produce =
        new ProduceService(config, ProduceRoutes.create(config, producers, codecs), metrics);

    // Connects to nothing until an app subscribes: readiness does not depend on the dispatcher,
    // and an app that only produces starts without one.
    ReceiveService receive;
    try {
      receive = ReceiveService.create(config, tls, metrics);
    } catch (CredentialException e) {
      LOG.error("TLS credentials unusable: {}", e.getMessage());
      producers.close(Duration.ZERO);
      tls.close();
      codecs.close();
      System.exit(1);
      return;
    }
    ConsumeService consume =
        new ConsumeService(config, codecs, tls, produce.serverEpoch(), metrics, receive);

    // Before the socket exists: an app never talks to a server that has not reached the clusters
    // it must have. Waits as long as it takes; the startup probe's failure threshold bounds it.
    RequiredClusters.await(config, tls, readiness);

    // A dispatcher that serves no app has nobody on the Unix socket, and no volume to bind it in.
    OdradekServer server = null;
    DispatchService dispatch = null;
    DeliveryServer delivery = null;
    Membership membership = null;
    try {
      readiness.starting("listening");
      if (servesApp(config)) {
        server = OdradekServer.start(config, new OdradekService(produce, consume), health);
      } else {
        LOG.info("no producers and no consumers: not listening on the app's Unix socket");
      }
      if (config.hasDispatch()) {
        dispatch = new DispatchService(config, tls, metrics);
        delivery = DeliveryServer.start(config.getDispatch(), dispatch, tls);
        // The replicas find each other through a consumer group. Until this one is a member and
        // knows the others it refuses every declaration, and it is not ready: the Service must
        // not send it a sidecar it would answer with less than every replica.
        membership =
            Membership.start(
                config, tls, metrics, delivery.advertisedAddress(), dispatch::replicas);
      }
    } catch (IOException | CredentialException e) {
      LOG.error("could not start", e);
      if (server != null) {
        server.close();
      }
      producers.close(Duration.ZERO);
      tls.close();
      codecs.close();
      System.exit(1);
      return;
    }
    OdradekServer app = server;
    DispatchService dispatcher = dispatch;
    DeliveryServer sidecars = delivery;
    Membership replicas = membership;
    if (replicas != null) {
      readiness.starting(
          "joining coordination group " + replicas.group() + " on cluster " + replicas.cluster());
    } else {
      readiness.started(tls);
    }
    metrics.observe(tls, readiness, Clock.systemUTC());
    LOG.info("server epoch {}", produce.serverEpoch());
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  // No new streams; then every record already accepted gets its broker answer,
                  // and with it its result, before the open streams are closed.
                  // Consumers commit what was acked and leave their groups, and the streams to
                  // a dispatcher are closed in good order; a dispatcher's own consumers first
                  // wait for what their sidecars hold.
                  readiness.stopping();
                  if (app != null) {
                    app.stopAccepting();
                  }
                  if (sidecars != null) {
                    sidecars.stopAccepting();
                  }
                  producers.flush();
                  consume.shutdown(Duration.ofSeconds(15));
                  receive.close();
                  if (dispatcher != null) {
                    dispatcher.shutdown(dispatcher.shutdownTimeout());
                    // Only now: a sidecar closes its streams to a replica that is no longer
                    // listed, and until here they carried the acknowledgements waited for.
                    replicas.close();
                    sidecars.close();
                  }
                  if (app != null) {
                    app.close();
                  }
                  producers.close();
                  tls.close();
                  codecs.close();
                  admin.close();
                  // Last, so that what the shutdown itself counted is pushed too.
                  telemetry.close();
                },
                "shutdown"));
    if (replicas != null) {
      // After the shutdown hook, so that a replica told to stop while it waits still stops in
      // good order. It waits as long as it takes, as for a required cluster: the startup probe's
      // failure threshold is what bounds it.
      LOG.info("joining coordination group {} on cluster {}", replicas.group(), replicas.cluster());
      while (!replicas.awaitJoined(Duration.ofSeconds(30))) {
        LOG.warn(
            "still joining coordination group {} on cluster {}: every declaration is refused"
                + " until then",
            replicas.group(),
            replicas.cluster());
      }
      readiness.started(tls);
    }
    if (app != null) {
      app.awaitTermination();
    } else {
      sidecars.awaitTermination();
    }
  }

  /**
   * Whether the server listens on the app's Unix socket: always, unless it is a dispatcher with
   * neither producers nor consumers. Such a server has no app next to it, and usually no volume for
   * the socket's directory.
   */
  static boolean servesApp(ServerConfig config) {
    return !config.hasDispatch()
        || config.getProducersCount() > 0
        || config.getConsumersCount() > 0;
  }

  /** Loads the contracts and builds the codecs the configuration names. */
  static CodecRegistry codecs(ServerConfig config) throws IOException {
    ContractRegistry contracts =
        ContractRegistry.load(
            config.getContracts().getDescriptorSetsList().stream().map(Path::of).toList());
    ClassLoader loader = Main.class.getClassLoader();
    if (config.hasExtensionsDir()) {
      // Deliberately never closed: its classes are in use for the life of the process.
      loader = Extensions.classLoader(Path.of(config.getExtensionsDir()), loader);
    }
    return CodecRegistry.create(config, contracts, loader);
  }
}
