package io.github.bpalermo.odradek.client;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.bpalermo.odradek.kafka.v1.OdradekServiceGrpc;
import io.github.bpalermo.odradek.kafka.v1.ProduceReady;
import io.github.bpalermo.odradek.kafka.v1.ProduceRequest;
import io.github.bpalermo.odradek.kafka.v1.ProduceResponse;
import io.github.bpalermo.odradek.kafka.v1.ProduceResult;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ProducerTest {
  /** A server that answers every record once {@link #answer} is counted down. */
  private static final class Answering extends OdradekServiceGrpc.OdradekServiceImplBase {
    final CountDownLatch answer = new CountDownLatch(1);

    @Override
    public StreamObserver<ProduceRequest> produce(StreamObserver<ProduceResponse> responses) {
      return new StreamObserver<>() {
        @Override
        public void onNext(ProduceRequest request) {
          if (request.hasOpen()) {
            responses.onNext(
                ProduceResponse.newBuilder()
                    .setReady(
                        ProduceReady.newBuilder()
                            .setServerEpoch("epoch")
                            .setMaxInFlightRecords(16)
                            .setMaxInFlightBytes(1 << 20))
                    .build());
            return;
          }
          try {
            answer.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          }
          responses.onNext(
              ProduceResponse.newBuilder()
                  .setResult(ProduceResult.newBuilder().setSeq(request.getRecord().getSeq()))
                  .build());
        }

        @Override
        public void onError(Throwable t) {}

        @Override
        public void onCompleted() {
          responses.onCompleted();
        }
      };
    }
  }

  private final Answering service = new Answering();
  private Server server;
  private ManagedChannel channel;
  private OdradekClient client;

  @BeforeEach
  void start() throws Exception {
    String name = InProcessServerBuilder.generateName();
    server = InProcessServerBuilder.forName(name).addService(service).build().start();
    channel = InProcessChannelBuilder.forName(name).build();
    client = OdradekClient.of(channel);
  }

  @AfterEach
  void stop() throws Exception {
    client.close();
    channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
  }

  @Test
  void closeReturnsOnlyOnceTheLastResultHasReachedWhatWasChainedOnIt() throws Exception {
    Producer producer = client.producer();
    AtomicBoolean answered = new AtomicBoolean();
    producer
        .send(OutgoingRecord.to("t").key("k").value(new byte[] {1}))
        .whenComplete(
            (result, error) -> {
              // Slow bookkeeping on the result: close() must wait for it, not only for the
              // record to leave the producer's books.
              try {
                Thread.sleep(300);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              answered.set(error == null);
            });
    service.answer.countDown();
    producer.close();
    assertThat(answered).isTrue();
  }
}
