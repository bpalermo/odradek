package io.github.bpalermo.odradek.e2e;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A TCP port that stays where it is while what is behind it comes and goes: what the address of a
 * Kubernetes Service is to the pods behind it.
 *
 * <p>A dispatcher under test listens on a port the system picks, so a restarted one is somewhere
 * else, and one that has not started is nowhere. Sidecars are configured with this relay's port
 * instead. Connections are passed on, byte for byte, to one of the ports set with {@link #target},
 * each new connection to the next of them, as a Service spreads connections over its pods; a port
 * where nothing listens is passed over. While there is none, or nothing listens at any, a
 * connection is closed at once.
 */
final class TcpRelay implements Closeable {
  private final ServerSocket server;
  private final Set<Socket> open = ConcurrentHashMap.newKeySet();
  private volatile int[] targets = {};
  private int next;
  private final AtomicInteger accepted = new AtomicInteger();

  TcpRelay() throws IOException {
    server = new ServerSocket(0);
    Thread acceptor = new Thread(this::accept, "relay-" + server.getLocalPort());
    acceptor.setDaemon(true);
    acceptor.start();
  }

  /** The port to connect to. */
  int port() {
    return server.getLocalPort();
  }

  /** Passes new connections on to these local ports, in turn; none, or -1, refuses them. */
  void target(int... ports) {
    targets = ports.length == 1 && ports[0] < 0 ? new int[0] : ports.clone();
  }

  /**
   * How many connections were made to this port so far, passed on or not: whether anything came
   * back to the address of the Service.
   */
  int accepted() {
    return accepted.get();
  }

  private void accept() {
    while (!server.isClosed()) {
      Socket in;
      try {
        in = server.accept();
      } catch (IOException e) {
        return;
      }
      accepted.incrementAndGet();
      int[] ports = targets;
      Socket out = null;
      // The next backend in turn, and after it the others, until one answers.
      for (int attempt = 0; attempt < ports.length && out == null; attempt++) {
        try {
          out = new Socket("localhost", ports[Math.floorMod(next++, ports.length)]);
        } catch (IOException e) {
          // Nothing listens there, as behind a Service whose pod has just gone.
        }
      }
      if (out == null) {
        closeQuietly(in);
        continue;
      }
      try {
        in.setTcpNoDelay(true);
        out.setTcpNoDelay(true);
      } catch (IOException e) {
        closeQuietly(in);
        closeQuietly(out);
        continue;
      }
      open.add(in);
      open.add(out);
      pump(in, out);
      pump(out, in);
    }
  }

  private void pump(Socket from, Socket to) {
    Thread thread =
        new Thread(
            () -> {
              byte[] buffer = new byte[64 * 1024];
              try {
                InputStream source = from.getInputStream();
                OutputStream sink = to.getOutputStream();
                for (int read = source.read(buffer); read >= 0; read = source.read(buffer)) {
                  sink.write(buffer, 0, read);
                  sink.flush();
                }
              } catch (IOException e) {
                // One side is gone: so is the other, below.
              }
              closeQuietly(from);
              closeQuietly(to);
              open.remove(from);
              open.remove(to);
            },
            "relay-pump");
    thread.setDaemon(true);
    thread.start();
  }

  private static void closeQuietly(Socket socket) {
    if (socket != null) {
      try {
        socket.close();
      } catch (IOException e) {
        // Closed already.
      }
    }
  }

  @Override
  public void close() throws IOException {
    server.close();
    open.forEach(TcpRelay::closeQuietly);
  }
}
