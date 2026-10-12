package io.github.bpalermo.odradek.server.telemetry;

import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ServerCalls;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 * A stand-in for an OpenTelemetry collector: the OTLP/gRPC metrics service on a TCP port, keeping
 * every export it is sent.
 *
 * <p>The exports are kept as bytes. A metric name, an attribute or a string value is in there as
 * plain UTF-8, which is all the tests look for; the values themselves are checked where the SDK's
 * own in-memory reader can be used.
 */
public final class FakeCollector {
  private static final MethodDescriptor<byte[], byte[]> EXPORT =
      MethodDescriptor.<byte[], byte[]>newBuilder()
          .setType(MethodDescriptor.MethodType.UNARY)
          .setFullMethodName("opentelemetry.proto.collector.metrics.v1.MetricsService/Export")
          .setRequestMarshaller(new Bytes())
          .setResponseMarshaller(new Bytes())
          .build();

  private final Server server;
  private final List<byte[]> exports = new CopyOnWriteArrayList<>();

  private FakeCollector() throws IOException {
    server =
        NettyServerBuilder.forPort(0)
            .addService(
                ServerServiceDefinition.builder(
                        "opentelemetry.proto.collector.metrics.v1.MetricsService")
                    .addMethod(
                        EXPORT,
                        ServerCalls.asyncUnaryCall(
                            (request, responses) -> {
                              exports.add(request);
                              // ExportMetricsServiceResponse with nothing rejected.
                              responses.onNext(new byte[0]);
                              responses.onCompleted();
                            }))
                    .build())
            .build()
            .start();
  }

  public static FakeCollector start() throws IOException {
    return new FakeCollector();
  }

  /** The endpoint to configure, {@code http://localhost:<port>}. */
  public String endpoint() {
    return "http://localhost:" + server.getPort();
  }

  /** Whether some export so far contains every one of {@code texts}. */
  public boolean received(String... texts) {
    for (byte[] export : exports) {
      String content = new String(export, StandardCharsets.ISO_8859_1);
      boolean all = true;
      for (String text : texts) {
        all &= content.contains(text);
      }
      if (all) {
        return true;
      }
    }
    return false;
  }

  /** Waits until {@link #received} is true, or fails after {@code timeout}. */
  public void await(Duration timeout, String... texts) throws InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (!received(texts)) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError(
            "no export containing "
                + List.of(texts)
                + " within "
                + timeout
                + "; "
                + exports.size()
                + " exports received");
      }
      Thread.sleep(100);
    }
  }

  public void close() throws InterruptedException {
    server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
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
