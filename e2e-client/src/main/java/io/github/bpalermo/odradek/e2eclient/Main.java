package io.github.bpalermo.odradek.e2eclient;

import io.github.bpalermo.odradek.client.OdradekClient;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The test application of Odradek's in-cluster end-to-end run: a producer or a consumer on the Java
 * client, talking to its sidecar over the Unix socket, keeping its own account of what happened to
 * every record. See e2e-client/README.md.
 *
 * <p>Exit status: 0 for a producer that finished with {@code --exit-when-done}, 143 (the JVM's for
 * SIGTERM) for a run that was stopped, 2 for bad settings.
 */
public final class Main {
  private Main() {}

  public static void main(String[] args) throws Exception {
    List<String> arguments = List.of(args);
    if (Settings.wantsHelp(arguments)) {
      System.out.print(Settings.usage());
      return;
    }
    Settings settings;
    try {
      settings = Settings.parse(arguments, System.getenv());
    } catch (IllegalArgumentException e) {
      System.err.println("e2e-client: " + e.getMessage());
      System.err.print(Settings.usage());
      System.exit(2);
      return;
    }

    Events events = Events.lines(System.out);
    Run run =
        settings.mode() == Settings.Mode.PRODUCER
            ? new ProducerRun(settings, events)
            : new ConsumerRun(settings, events);
    CountDownLatch finished = new CountDownLatch(1);
    AtomicBoolean finalWritten = new AtomicBoolean();
    Runnable writeFinal =
        () -> {
          if (finalWritten.compareAndSet(false, true)) {
            events.emit("final", run.results());
          }
        };
    // SIGTERM: stop the run and let it finish before the JVM exits; if it takes longer than
    // --stop-timeout (the client waits for a sidecar that has gone), the final line is written
    // all the same.
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  run.stop();
                  try {
                    finished.await(settings.stopTimeout().toMillis(), TimeUnit.MILLISECONDS);
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  }
                  writeFinal.run();
                },
                "e2e-client-shutdown"));

    try (ResultsServer http = ResultsServer.start(settings.httpPort(), run::results)) {
      events.emit("started", Map.of("config", settings.describe(), "httpPort", http.port()));
      try (OdradekClient client = OdradekClient.connect(settings.socket())) {
        run.run(client);
      }
      if (!run.stopping()) {
        // A producer that sent everything: its results stay available until it is stopped.
        events.emit("done", run.results());
        if (!settings.exitWhenDone()) {
          run.awaitStop();
        }
      }
      writeFinal.run();
    } finally {
      finished.countDown();
    }
  }
}
