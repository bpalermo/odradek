package io.github.bpalermo.odradek.server;

import io.github.bpalermo.odradek.config.v1.ServerConfig;
import io.github.bpalermo.odradek.kafka.v1.OdradekServiceGrpc;
import io.github.bpalermo.odradek.server.codec.CodecConfigException;
import io.github.bpalermo.odradek.server.codec.CodecRegistry;
import io.github.bpalermo.odradek.server.codec.ContractRegistry;
import io.github.bpalermo.odradek.server.codec.Extensions;
import io.github.bpalermo.odradek.server.config.ConfigException;
import io.github.bpalermo.odradek.server.config.ConfigLoader;
import java.io.IOException;
import java.nio.file.Path;
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

    OdradekServer server;
    try {
      // Produce and Consume are not implemented yet: the generated base answers UNIMPLEMENTED.
      server = OdradekServer.start(config, new OdradekServiceGrpc.OdradekServiceImplBase() {});
    } catch (IOException e) {
      LOG.error("could not start", e);
      codecs.close();
      System.exit(1);
      return;
    }
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  server.close();
                  codecs.close();
                },
                "shutdown"));
    server.awaitTermination();
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
