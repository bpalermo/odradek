package io.github.bpalermo.odradek.server.consume;

import io.github.bpalermo.odradek.config.v1.Consumer;
import io.github.bpalermo.odradek.kafka.v1.ConsumeResponse;
import io.github.bpalermo.odradek.server.codec.RecordDecoder;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import java.util.List;
import java.util.Map;

/**
 * Where the records of a Consume stream come from when its topics' {@code source} is the
 * dispatcher: something that receives them from a dispatcher instead of reading Kafka (ADR 6).
 */
public interface DispatcherSource {
  /** A server with no way to reach a dispatcher: every such stream is refused. */
  DispatcherSource NONE =
      request -> {
        throw Status.FAILED_PRECONDITION
            .withDescription("this server has no dispatcher to receive from")
            .asRuntimeException();
      };

  /**
   * Makes the stream that serves one subscription. {@code Subscribed} has been sent; the stream is
   * not started.
   *
   * @throws io.grpc.StatusRuntimeException if the stream cannot be served; it ends with that status
   */
  AppStream open(Request request);

  /**
   * One accepted subscription.
   *
   * @param call the app's stream; the returned stream is its only writer from here on
   * @param topics the topics subscribed to, which share a cluster and a group
   * @param entries their consumer entries, by topic
   * @param decoders their decoders, by topic; the stream closes them when it ends
   * @param maxProcessingMs the processing deadline resolved for the app
   * @param listener is told what happens, on the stream's thread
   */
  record Request(
      ServerCallStreamObserver<ConsumeResponse> call,
      List<String> topics,
      Map<String, Consumer> entries,
      Map<String, RecordDecoder> decoders,
      long maxProcessingMs,
      Listener listener) {}

  /** What a stream reports as it happens. */
  interface Listener {
    /** Records of a topic were sent to the app, {@code undecodable} of them with a decode error. */
    void delivered(String topic, long records, long undecodable);

    /** The stream ended with this gRPC status code. Called once, last. */
    void ended(AppStream stream, String reason);
  }
}
