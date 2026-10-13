package io.github.bpalermo.odradek.server.receive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.bpalermo.odradek.delivery.v1.DeliverRequest;
import io.github.bpalermo.odradek.delivery.v1.DeliverResponse;
import io.github.bpalermo.odradek.delivery.v1.DeliveryServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The connections to the dispatcher that the Consume streams of one server share: one per address,
 * for as long as a stream uses it.
 */
class PoolTest {
  private static final Duration DRAIN = Duration.ofMillis(300);

  private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
  private final List<String> connected = new CopyOnWriteArrayList<>();
  private final List<ManagedChannel> made = new ArrayList<>();

  /** The Deliver streams the dispatcher was opened, which it never answers nor closes. */
  private final List<StreamObserver<DeliverResponse>> streams = new CopyOnWriteArrayList<>();

  private Server dispatcher;
  private ReceiveService.Pool pool;

  @BeforeEach
  void start() throws Exception {
    String name = InProcessServerBuilder.generateName();
    dispatcher =
        InProcessServerBuilder.forName(name)
            .addService(
                new DeliveryServiceGrpc.DeliveryServiceImplBase() {
                  @Override
                  public StreamObserver<DeliverRequest> deliver(
                      StreamObserver<DeliverResponse> responses) {
                    streams.add(responses);
                    return new StreamObserver<>() {
                      @Override
                      public void onNext(DeliverRequest request) {}

                      @Override
                      public void onError(Throwable t) {}

                      @Override
                      public void onCompleted() {}
                    };
                  }
                })
            .build()
            .start();
    pool =
        new ReceiveService.Pool(
            address -> {
              connected.add(address);
              ManagedChannel channel = InProcessChannelBuilder.forName(name).build();
              synchronized (made) {
                made.add(channel);
              }
              return channel;
            },
            timer,
            DRAIN);
  }

  @AfterEach
  void stop() throws Exception {
    pool.close();
    synchronized (made) {
      made.forEach(ManagedChannel::shutdownNow);
    }
    dispatcher.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    timer.shutdownNow();
  }

  /** Opens a Deliver stream that the dispatcher leaves open. */
  private StreamObserver<DeliverRequest> open(ManagedChannel channel) throws InterruptedException {
    int before = streams.size();
    StreamObserver<DeliverRequest> requests =
        DeliveryServiceGrpc.newStub(channel)
            .deliver(
                new StreamObserver<>() {
                  @Override
                  public void onNext(DeliverResponse response) {}

                  @Override
                  public void onError(Throwable t) {}

                  @Override
                  public void onCompleted() {}
                });
    requests.onNext(DeliverRequest.getDefaultInstance());
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (streams.size() == before) {
      assertThat(System.nanoTime()).as("the stream reaches the dispatcher").isLessThan(deadline);
      Thread.sleep(5);
    }
    return requests;
  }

  @Test
  void theStreamsOfOneServerShareOneConnectionToAnAddress() {
    ManagedChannel first = pool.connect("10.0.0.1:9465");
    ManagedChannel second = pool.connect("10.0.0.1:9465");
    ManagedChannel other = pool.connect("10.0.0.2:9465");
    assertThat(second).isSameAs(first);
    assertThat(other).isNotSameAs(first);
    assertThat(connected).containsExactly("10.0.0.1:9465", "10.0.0.2:9465");
  }

  @Test
  void aConnectionIsClosedWhenTheLastStreamThatUsedItGivesItBack() {
    ManagedChannel channel = pool.connect("10.0.0.1:9465");
    pool.connect("10.0.0.1:9465");

    pool.release("10.0.0.1:9465", channel);
    assertThat(channel.isShutdown()).as("closed while a stream uses it").isFalse();
    pool.release("10.0.0.1:9465", channel);
    assertThat(channel.isShutdown()).isTrue();

    // Whoever comes next gets a new one.
    ManagedChannel next = pool.connect("10.0.0.1:9465");
    assertThat(next).isNotSameAs(channel);
    assertThat(next.isShutdown()).isFalse();
    assertThat(connected).hasSize(2);
  }

  @Test
  void aConnectionGivenBackMoreOftenThanItWasTakenIsAMistake() {
    ManagedChannel channel = pool.connect("10.0.0.1:9465");
    pool.release("10.0.0.1:9465", channel);
    assertThatThrownBy(() -> pool.release("10.0.0.1:9465", channel))
        .isInstanceOf(IllegalStateException.class);
    ManagedChannel next = pool.connect("10.0.0.1:9465");
    // Nor is the connection of whoever came after closed by a late return of the old one.
    assertThatThrownBy(() -> pool.release("10.0.0.1:9465", channel))
        .isInstanceOf(IllegalStateException.class);
    assertThat(next.isShutdown()).isFalse();
  }

  @Test
  void aStreamHalfClosedOnAConnectionGivenBackEndsInGoodOrderIfTheDispatcherAnswers()
      throws Exception {
    ManagedChannel channel = pool.connect("10.0.0.1:9465");
    StreamObserver<DeliverRequest> requests = open(channel);
    requests.onCompleted();
    pool.release("10.0.0.1:9465", channel);
    assertThat(channel.isTerminated()).as("cut with a stream still open").isFalse();

    streams.get(0).onCompleted();
    assertThat(channel.awaitTermination(DRAIN.toMillis() / 2, TimeUnit.MILLISECONDS))
        .as("closed as soon as its stream ended")
        .isTrue();
  }

  @Test
  void aConnectionGivenBackIsCutWhenTheDispatcherNeverClosesItsStreams() throws Exception {
    ManagedChannel channel = pool.connect("10.0.0.1:9465");
    open(channel).onCompleted();
    pool.release("10.0.0.1:9465", channel);
    assertThat(channel.isTerminated()).isFalse();
    assertThat(channel.awaitTermination(10, TimeUnit.SECONDS)).as("cut after the drain").isTrue();
  }

  @Test
  void closingThePoolClosesEveryConnection() {
    ManagedChannel held = pool.connect("10.0.0.1:9465");
    ManagedChannel released = pool.connect("10.0.0.2:9465");
    pool.release("10.0.0.2:9465", released);
    pool.close();
    assertThat(held.isTerminated()).isTrue();
    assertThat(released.isTerminated()).isTrue();
    // The timer that would have cut the released one finds nothing left to do.
    timer.shutdownNow();
    pool.release("10.0.0.1:9465", pool.connect("10.0.0.1:9465"));
  }
}
