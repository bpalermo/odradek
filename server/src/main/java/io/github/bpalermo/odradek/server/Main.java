package io.github.bpalermo.odradek.server;

import io.github.bpalermo.odradek.kafka.v1.OdradekServiceGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Entry point of the odradek server. Placeholder: logs a startup line and exits. */
public final class Main {
  private static final Logger LOG = LoggerFactory.getLogger(Main.class);

  private Main() {}

  /** Returns the fully qualified gRPC service name this server will serve. */
  static String serviceName() {
    return OdradekServiceGrpc.SERVICE_NAME;
  }

  public static void main(String[] args) {
    LOG.info("odradek server starting; will serve {}", serviceName());
  }
}
