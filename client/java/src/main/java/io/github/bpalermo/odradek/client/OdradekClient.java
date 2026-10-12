package io.github.bpalermo.odradek.client;

import io.github.bpalermo.odradek.kafka.v1.OdradekServiceGrpc;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.epoll.EpollIoHandler;
import io.netty.channel.unix.DomainSocketAddress;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The entry point of the Odradek client: a connection to the server in the same pod, from which
 * producers and consumers are made.
 *
 * <pre>{@code
 * try (OdradekClient client = OdradekClient.connect(Path.of("/var/run/kafka/kafka.sock"));
 *     Producer producer = client.producer()) {
 *   producer.send(OutgoingRecord.to("orders.created").key(id).value(order)).get();
 * }
 * }</pre>
 *
 * <p>The app names topics only. Clusters, consumer groups, the stored format and encryption are the
 * server's configuration.
 */
public final class OdradekClient implements AutoCloseable {
  private final OdradekServiceGrpc.OdradekServiceStub stub;
  private final ManagedChannel owned;
  private final EventLoopGroup group;
  private final ScheduledExecutorService scheduler =
      Executors.newSingleThreadScheduledExecutor(
          runnable -> {
            Thread thread = new Thread(runnable, "odradek-client");
            thread.setDaemon(true);
            return thread;
          });

  private OdradekClient(Channel channel, ManagedChannel owned, EventLoopGroup group) {
    this.stub = OdradekServiceGrpc.newStub(channel);
    this.owned = owned;
    this.group = group;
  }

  /**
   * Connects to the server's Unix domain socket. Needs Netty's epoll transport, so Linux, and the
   * {@code netty-transport-native-epoll} jar for the machine's architecture on the class path.
   */
  public static OdradekClient connect(Path socket) {
    EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, EpollIoHandler.newFactory());
    ManagedChannel channel =
        NettyChannelBuilder.forAddress(new DomainSocketAddress(socket.toString()))
            .eventLoopGroup(group)
            .channelType(EpollDomainSocketChannel.class)
            .usePlaintext()
            .build();
    return new OdradekClient(channel, channel, group);
  }

  /** Uses a channel the caller made and will close. */
  public static OdradekClient of(Channel channel) {
    return new OdradekClient(channel, null, null);
  }

  public Producer producer() {
    return producer(ProducerOptions.defaults());
  }

  public Producer producer(ProducerOptions options) {
    return new Producer(stub, scheduler, options);
  }

  /** Starts a consumer that hands records to {@code handler}. */
  public Consumer consumer(ConsumerOptions options, RecordHandler handler) {
    return consumer(options, handler, new ConsumerListener() {});
  }

  /**
   * Starts a consumer that hands records to {@code handler} and reports changes to {@code
   * listener}.
   */
  public Consumer consumer(
      ConsumerOptions options, RecordHandler handler, ConsumerListener listener) {
    return new Consumer(stub, scheduler, options, handler, listener);
  }

  /** Closes the connection. Close producers and consumers first. */
  @Override
  public void close() {
    scheduler.shutdownNow();
    if (owned != null) {
      owned.shutdownNow();
      try {
        owned.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      group.shutdownGracefully();
    }
  }
}
