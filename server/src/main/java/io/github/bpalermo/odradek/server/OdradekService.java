package io.github.bpalermo.odradek.server;

import io.github.bpalermo.odradek.kafka.v1.ConsumeRequest;
import io.github.bpalermo.odradek.kafka.v1.ConsumeResponse;
import io.github.bpalermo.odradek.kafka.v1.OdradekServiceGrpc;
import io.github.bpalermo.odradek.kafka.v1.ProduceRequest;
import io.github.bpalermo.odradek.kafka.v1.ProduceResponse;
import io.github.bpalermo.odradek.server.consume.ConsumeService;
import io.github.bpalermo.odradek.server.produce.ProduceService;
import io.grpc.stub.StreamObserver;

/** The gRPC service: binds the two RPCs to their implementations. */
public final class OdradekService extends OdradekServiceGrpc.OdradekServiceImplBase {
  private final ProduceService produce;
  private final ConsumeService consume;

  public OdradekService(ProduceService produce, ConsumeService consume) {
    this.produce = produce;
    this.consume = consume;
  }

  @Override
  public StreamObserver<ProduceRequest> produce(StreamObserver<ProduceResponse> responses) {
    return produce.produce(responses);
  }

  @Override
  public StreamObserver<ConsumeRequest> consume(StreamObserver<ConsumeResponse> responses) {
    return consume.consume(responses);
  }
}
