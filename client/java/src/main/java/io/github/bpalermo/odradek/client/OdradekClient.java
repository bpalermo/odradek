package io.github.bpalermo.odradek.client;

import io.github.bpalermo.odradek.kafka.v1.OdradekServiceGrpc;

/** Placeholder for the application-side client of the odradek server. */
public final class OdradekClient {
  private OdradekClient() {}

  /** Returns the fully qualified gRPC service name of the server API. */
  public static String serviceName() {
    return OdradekServiceGrpc.SERVICE_NAME;
  }
}
