package io.github.bpalermo.odradek.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.bpalermo.odradek.config.v1.Listener;
import io.github.bpalermo.odradek.config.v1.ServerConfig;
import io.github.bpalermo.odradek.kafka.v1.OdradekServiceGrpc;
import io.github.bpalermo.odradek.kafka.v1.ProduceRequest;
import io.github.bpalermo.odradek.kafka.v1.ProduceResponse;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.epoll.EpollIoHandler;
import io.netty.channel.unix.DomainSocketAddress;
import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Comparator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OdradekServerTest {
  private Path dir;
  private Path socket;

  @BeforeEach
  void createDirectory() throws IOException {
    // Not @TempDir: a socket path is limited to about 100 bytes, and Bazel's test directory is
    // longer than that.
    dir = Files.createTempDirectory(Path.of("/tmp"), "odradek");
    socket = dir.resolve("kafka.sock");
  }

  @AfterEach
  void removeDirectory() throws IOException {
    try (Stream<Path> files = Files.walk(dir)) {
      for (Path file : files.sorted(Comparator.reverseOrder()).toList()) {
        Files.delete(file);
      }
    }
  }

  private ServerConfig config() {
    return ServerConfig.newBuilder()
        .setListener(Listener.newBuilder().setSocketPath(socket.toString()))
        .build();
  }

  private static OdradekServiceGrpc.OdradekServiceImplBase unimplemented() {
    return new OdradekServiceGrpc.OdradekServiceImplBase() {};
  }

  @Test
  void servesGrpcOverTheUnixSocketWithOwnerAndGroupAccessOnly() throws Exception {
    EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, EpollIoHandler.newFactory());
    try (OdradekServer server = OdradekServer.start(config(), unimplemented())) {
      assertThat(server.socket()).isEqualTo(socket);
      assertThat(PosixFilePermissions.toString(Files.getPosixFilePermissions(socket)))
          .isEqualTo("rw-rw----");

      ManagedChannel channel =
          NettyChannelBuilder.forAddress(new DomainSocketAddress(socket.toString()))
              .eventLoopGroup(group)
              .channelType(EpollDomainSocketChannel.class)
              .usePlaintext()
              .build();
      try {
        CompletableFuture<Status> outcome = new CompletableFuture<>();
        StreamObserver<ProduceRequest> requests =
            OdradekServiceGrpc.newStub(channel)
                .produce(
                    new StreamObserver<ProduceResponse>() {
                      @Override
                      public void onNext(ProduceResponse response) {}

                      @Override
                      public void onError(Throwable t) {
                        outcome.complete(Status.fromThrowable(t));
                      }

                      @Override
                      public void onCompleted() {
                        outcome.complete(Status.OK);
                      }
                    });
        requests.onCompleted();
        // The call reached the service: the placeholder answers UNIMPLEMENTED.
        assertThat(outcome.get(10, TimeUnit.SECONDS).getCode())
            .isEqualTo(Status.Code.UNIMPLEMENTED);
      } finally {
        channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
      }
    } finally {
      group.shutdownGracefully();
    }
    assertThat(socket).doesNotExist();
  }

  @Test
  void replacesASocketLeftByAPreviousRun() throws Exception {
    // Closing the channel without deleting the file is what a killed server leaves behind.
    try (ServerSocketChannel stale = ServerSocketChannel.open(StandardProtocolFamily.UNIX)) {
      stale.bind(UnixDomainSocketAddress.of(socket));
    }
    assertThat(socket).exists();

    try (OdradekServer server = OdradekServer.start(config(), unimplemented())) {
      assertThat(server.socket()).exists();
    }
  }

  @Test
  void refusesToReplaceSomethingThatIsNotASocket() throws Exception {
    Files.writeString(socket, "not a socket");
    assertThatThrownBy(() -> OdradekServer.start(config(), unimplemented()))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("not a socket");
    assertThat(Files.readString(socket)).isEqualTo("not a socket");
  }

  @Test
  void failsWhenTheSocketDirectoryIsMissing() {
    ServerConfig config =
        ServerConfig.newBuilder()
            .setListener(
                Listener.newBuilder().setSocketPath(dir.resolve("missing/kafka.sock").toString()))
            .build();
    assertThatThrownBy(() -> OdradekServer.start(config, unimplemented()))
        .isInstanceOf(IOException.class);
  }
}
