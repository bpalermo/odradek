package io.github.bpalermo.odradek.server;

import io.github.bpalermo.odradek.kafka.v1.OdradekServiceGrpc;
import io.github.bpalermo.odradek.kafka.v1.ProduceRequest;
import io.github.bpalermo.odradek.kafka.v1.ProduceResponse;
import io.github.bpalermo.odradek.server.produce.ProduceService;
import io.grpc.stub.StreamObserver;

/** The gRPC service. Consume is not implemented yet and answers UNIMPLEMENTED. */
public final class OdradekService extends OdradekServiceGrpc.OdradekServiceImplBase {
  private final ProduceService produce;

  public OdradekService(ProduceService produce) {
    this.produce = produce;
  }

  @Override
  public StreamObserver<ProduceRequest> produce(StreamObserver<ProduceResponse> responses) {
    return produce.produce(responses);
  }
}
