package io.github.bpalermo.odradek.e2eclient;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The test application of Odradek's in-cluster end-to-end run: a producer, a consumer or both,
 * through the Odradek sidecar or with the Kafka clients directly, keeping its own account of what
 * happened to every record. See e2e-client/README.md.
 *
 * <p>Exit status: 0 for a producer that finished with {@code --exit-when-done}, 143 (the JVM's for
 * SIGTERM) for a run that was stopped, 2 for bad settings, 1 when the {@code kafka} transport
 * cannot start.
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
    Metrics metrics;
    try {
      settings = Settings.parse(arguments, System.getenv());
      metrics =
          OtlpMetrics.create(
              System.getenv(),
              settings.lane(),
              settings.transport().name().toLowerCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      System.err.println("e2e-client: " + e.getMessage());
      System.err.print(Settings.usage());
      System.exit(2);
      return;
    }

    Events events = Events.lines(System.out);
    Latency latency = new Latency(settings.latencyWindow(), metrics);
    Run run =
        switch (settings.mode()) {
          case PRODUCER -> new ProducerRun(settings, events, latency);
          case CONSUMER -> new ConsumerRun(settings, events, latency);
          case BOTH -> new BothRun(settings, events, latency);
        };
    CountDownLatch finished = new CountDownLatch(1);
    AtomicBoolean finalWritten = new AtomicBoolean();
    Runnable writeFinal =
        () -> {
          if (finalWritten.compareAndSet(false, true)) {
            events.emit("final", run.results());
            metrics.close();
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
      try (Transport transport = open(settings)) {
        Map<String, Object> started = new LinkedHashMap<>();
        started.put("config", settings.describe());
        started.put("httpPort", http.port());
        if (transport instanceof KafkaTransport kafka) {
          started.put("kafkaClients", kafka.describe());
        }
        events.emit("started", started);
        run.run(transport);
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

  /**
   * The transport of the settings. What keeps the {@code kafka} transport from starting (its
   * properties file, its credentials) ends the program with status 1.
   */
  private static Transport open(Settings settings) throws Exception {
    if (settings.transport() == Settings.TransportKind.ODRADEK) {
      return OdradekTransport.connect(settings.socket());
    }
    return KafkaTransport.create(settings);
  }
}
