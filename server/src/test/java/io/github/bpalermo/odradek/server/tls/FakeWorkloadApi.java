package io.github.bpalermo.odradek.server.tls;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.epoll.EpollIoHandler;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A stand-in for a SPIRE agent: the SPIFFE Workload API's {@code FetchX509SVID} on a Unix socket,
 * answering with whatever the test published last and pushing every later publication to the open
 * streams. Like the real one, it refuses a request without the security header.
 */
public final class FakeWorkloadApi {
  /** Every request must carry this header, as SPIRE agents require. */
  private static final Metadata.Key<String> SECURITY_HEADER =
      Metadata.Key.of("workload.spiffe.io", Metadata.ASCII_STRING_MARSHALLER);

  private static final MethodDescriptor<byte[], byte[]> METHOD =
      MethodDescriptor.<byte[], byte[]>newBuilder()
          .setType(MethodDescriptor.MethodType.SERVER_STREAMING)
          .setFullMethodName("SpiffeWorkloadAPI/FetchX509SVID")
          .setRequestMarshaller(new Bytes())
          .setResponseMarshaller(new Bytes())
          .build();

  private final Server server;
  private final EventLoopGroup group;
  private final List<ServerCall<byte[], byte[]>> calls = new ArrayList<>();
  private final AtomicInteger streamsOpened = new AtomicInteger();
  private byte[] latest;

  private FakeWorkloadApi(Path socket) throws IOException {
    group = new MultiThreadIoEventLoopGroup(1, EpollIoHandler.newFactory());
    ServerCallHandler<byte[], byte[]> handler =
        (call, headers) -> {
          call.sendHeaders(new Metadata());
          if (!"true".equals(headers.get(SECURITY_HEADER))) {
            call.close(
                Status.INVALID_ARGUMENT.withDescription("security header missing from request"),
                new Metadata());
          } else {
            open(call);
          }
          call.request(1);
          return new ServerCall.Listener<>() {
            @Override
            public void onCancel() {
              closed(call);
            }
          };
        };
    server =
        NettyServerBuilder.forAddress(new DomainSocketAddress(socket.toString()))
            .channelType(EpollServerDomainSocketChannel.class)
            .bossEventLoopGroup(group)
            .workerEventLoopGroup(group)
            .addService(
                ServerServiceDefinition.builder("SpiffeWorkloadAPI")
                    .addMethod(METHOD, handler)
                    .build())
            .build()
            .start();
  }

  /** Starts listening on {@code socket}. Streams wait until something is published. */
  public static FakeWorkloadApi start(Path socket) throws IOException {
    return new FakeWorkloadApi(socket);
  }

  /** How many streams were opened so far. */
  public int streamsOpened() {
    return streamsOpened.get();
  }

  /**
   * Publishes an SVID to every open stream and to every later one.
   *
   * @param federated PEM bundles of other trust domains, by trust domain name
   */
  public synchronized void publish(
      String spiffeId,
      String certPem,
      String keyPem,
      String bundlePem,
      Map<String, String> federated)
      throws Exception {
    ByteArrayOutputStream svid = new ByteArrayOutputStream();
    CodedOutputStream svidOut = CodedOutputStream.newInstance(svid);
    svidOut.writeString(1, spiffeId);
    svidOut.writeBytes(2, ByteString.copyFrom(certificatesDer(certPem)));
    svidOut.writeBytes(3, ByteString.copyFrom(keyDer(keyPem)));
    svidOut.writeBytes(4, ByteString.copyFrom(certificatesDer(bundlePem)));
    svidOut.flush();

    ByteArrayOutputStream response = new ByteArrayOutputStream();
    CodedOutputStream out = CodedOutputStream.newInstance(response);
    out.writeBytes(1, ByteString.copyFrom(svid.toByteArray()));
    for (var bundle : federated.entrySet()) {
      ByteArrayOutputStream entry = new ByteArrayOutputStream();
      CodedOutputStream entryOut = CodedOutputStream.newInstance(entry);
      entryOut.writeString(1, bundle.getKey());
      entryOut.writeBytes(2, ByteString.copyFrom(certificatesDer(bundle.getValue())));
      entryOut.flush();
      out.writeBytes(3, ByteString.copyFrom(entry.toByteArray()));
    }
    out.flush();
    publishRaw(response.toByteArray());
  }

  /** Publishes an already encoded X509SVIDResponse, valid or not. */
  public synchronized void publishRaw(byte[] response) {
    latest = response;
    for (ServerCall<byte[], byte[]> call : calls) {
      call.sendMessage(response);
    }
  }

  /** Ends every open stream with an error, as an agent that restarts does. */
  public synchronized void dropStreams() {
    for (ServerCall<byte[], byte[]> call : List.copyOf(calls)) {
      call.close(Status.UNAVAILABLE.withDescription("agent restarting"), new Metadata());
    }
    calls.clear();
  }

  private synchronized void open(ServerCall<byte[], byte[]> call) {
    streamsOpened.incrementAndGet();
    calls.add(call);
    if (latest != null) {
      call.sendMessage(latest);
    }
  }

  private synchronized void closed(ServerCall<byte[], byte[]> call) {
    calls.remove(call);
  }

  /** Stops listening and ends every stream. */
  public void close() throws InterruptedException {
    server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    group.shutdownGracefully(0, 1, TimeUnit.SECONDS);
  }

  private static byte[] certificatesDer(String pem) throws Exception {
    ByteArrayOutputStream der = new ByteArrayOutputStream();
    for (X509Certificate certificate :
        Pem.certificates(pem.getBytes(StandardCharsets.US_ASCII), "test certificates")) {
      try {
        der.write(certificate.getEncoded());
      } catch (CertificateEncodingException e) {
        throw new IOException(e);
      }
    }
    return der.toByteArray();
  }

  private static byte[] keyDer(String pem) throws Exception {
    return Pem.privateKey(pem.getBytes(StandardCharsets.US_ASCII), "test key").getEncoded();
  }

  private static final class Bytes implements MethodDescriptor.Marshaller<byte[]> {
    @Override
    public InputStream stream(byte[] value) {
      return new ByteArrayInputStream(value);
    }

    @Override
    public byte[] parse(InputStream stream) {
      try {
        return stream.readAllBytes();
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }
  }
}
