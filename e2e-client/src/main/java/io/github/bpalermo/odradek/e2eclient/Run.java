package io.github.bpalermo.odradek.e2eclient;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/** One run of a mode: started once, stopped from another thread. */
public abstract class Run {
  protected final Settings settings;
  protected final Events events;
  protected final Latency latency;
  private final CountDownLatch stop = new CountDownLatch(1);
  private final CompletableFuture<Void> stopped = new CompletableFuture<>();
  private final long startedMs = System.currentTimeMillis();
  private volatile String state = "starting";

  protected Run(Settings settings, Events events, Latency latency) {
    this.settings = settings;
    this.events = events;
    this.latency = latency;
  }

  /** Runs until done or stopped. */
  public abstract void run(Transport transport) throws InterruptedException;

  /** The mode's own part of the results. */
  protected abstract Map<String, Object> body();

  /** Asks the run to stop. Returns at once. */
  public void stop() {
    stop.countDown();
    stopped.complete(null);
  }

  /** Completes when the run is asked to stop. */
  protected CompletableFuture<Void> stopped() {
    return stopped;
  }

  public boolean stopping() {
    return stop.getCount() == 0;
  }

  public void awaitStop() throws InterruptedException {
    stop.await();
  }

  /**
   * Waits for {@code duration}, or less if the run is stopped meanwhile.
   *
   * @return whether the run is stopping
   */
  protected boolean pause(Duration duration) throws InterruptedException {
    if (duration.isZero() || duration.isNegative()) {
      return stopping();
    }
    return stop.await(duration.toNanos(), TimeUnit.NANOSECONDS);
  }

  protected void state(String state) {
    this.state = state;
  }

  public String state() {
    return state;
  }

  public Latency latency() {
    return latency;
  }

  /** Everything {@code GET /results} answers. */
  public Map<String, Object> results() {
    Map<String, Object> config = settings.describe();
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("mode", config.get("mode"));
    out.put("transport", config.get("transport"));
    out.put("lane", settings.lane());
    out.put("origin", settings.origin());
    out.put("runId", settings.runId());
    out.put("state", state);
    out.put("startedMs", startedMs);
    out.put("nowMs", System.currentTimeMillis());
    out.put("config", config);
    out.putAll(body());
    out.put("latency", latency.results());
    return out;
  }
}
