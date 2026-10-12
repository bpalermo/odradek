package io.github.bpalermo.odradek.sidecar;

import io.github.bpalermo.odradek.kafka.v1.OdradekServiceGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Entry point of the odradek sidecar. Placeholder: logs a startup line and exits. */
public final class Main {
  private static final Logger LOG = LoggerFactory.getLogger(Main.class);

  private Main() {}

  /** Returns the fully qualified gRPC service name this sidecar will serve. */
  static String serviceName() {
    return OdradekServiceGrpc.SERVICE_NAME;
  }

  public static void main(String[] args) {
    LOG.info("odradek sidecar starting; will serve {}", serviceName());
  }
}
