package io.github.bpalermo.odradek.server.consume;

import io.github.bpalermo.odradek.kafka.v1.Ack;
import io.grpc.Status;

/**
 * One Consume stream after its handshake, as the gRPC side sees it: where the app's messages go,
 * whoever supplies the records. Every method only queues; the stream's own thread does the work.
 */
public interface AppStream {
  /** Starts the stream's thread. Called once, after the stream is registered for shutdown. */
  void start();

  /** The app granted credits. */
  void credits(long records);

  /** The app acknowledged records. */
  void ack(Ack ack);

  /** The app answered a Revoked event. */
  void revokeDone(long revokeId);

  /** The app finished its side of the stream. */
  void halfClose();

  /** The stream is gone; nothing more can be sent to the app. */
  void cancelled();

  /** The app broke the protocol: the stream ends with {@code status}. */
  void fail(Status status);

  /** The server is stopping. */
  void shutdown();

  /** Waits for the stream's thread to finish, for at most {@code millis}. */
  void join(long millis) throws InterruptedException;
}
