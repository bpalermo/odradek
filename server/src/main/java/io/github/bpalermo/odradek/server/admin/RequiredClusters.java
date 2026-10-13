package io.github.bpalermo.odradek.server.admin;

import io.github.bpalermo.odradek.config.v1.Cluster;
import io.github.bpalermo.odradek.config.v1.ServerConfig;
import io.github.bpalermo.odradek.server.tls.TlsSources;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The startup gate of the clusters marked {@code required}: the server does not listen, and does
 * not report ready, until each of them has answered a metadata request.
 *
 * <p>A gate of startup only. Once the server has started, a cluster that stops answering is the
 * business of producing and consuming, which report their own errors; readiness does not follow it.
 */
public final class RequiredClusters {
  private static final Logger LOG = LoggerFactory.getLogger(RequiredClusters.class);

  /** How long one metadata request may take before it counts as failed. */
  static final Duration ATTEMPT_TIMEOUT = Duration.ofSeconds(10);

  /** The pause after a failed attempt. */
  static final Duration RETRY_DELAY = Duration.ofSeconds(1);

  /** How often the wait for one cluster is logged while nothing changes. */
  static final Duration PROGRESS_INTERVAL = Duration.ofSeconds(30);

  /** Asks one cluster for its metadata; returns when it answered and throws when it did not. */
  @FunctionalInterface
  public interface Check {
    void check(String cluster, Map<String, Object> properties) throws Exception;
  }

  private RequiredClusters() {}

  /**
   * Returns once every required cluster has answered a metadata request, however long that takes:
   * what bounds the wait is the startup probe's failure threshold, not the server.
   */
  public static void await(ServerConfig config, TlsSources tls, Readiness readiness)
      throws InterruptedException {
    await(config, tls, readiness, RequiredClusters::describe, RETRY_DELAY, PROGRESS_INTERVAL);
  }

  /**
   * As above, with the check and the pacing given. Clusters are waited for one at a time, in the
   * order of their names; while one is waited for, {@code readiness} names it. The reason of a
   * failure is logged when it differs from the previous one, and the wait itself every {@code
   * progressInterval}.
   */
  static void await(
      ServerConfig config,
      TlsSources tls,
      Readiness readiness,
      Check check,
      Duration retryDelay,
      Duration progressInterval)
      throws InterruptedException {
    for (var entry : new TreeMap<>(config.getClustersMap()).entrySet()) {
      String name = entry.getKey();
      if (!entry.getValue().getRequired()) {
        continue;
      }
      readiness.starting("waiting for cluster " + name);
      Map<String, Object> properties = properties(name, entry.getValue());
      tls.apply(name, properties);
      long started = System.nanoTime();
      long lastProgress = started;
      String lastReason = null;
      for (int attempt = 1; ; attempt++) {
        String reason;
        try {
          check.check(name, new HashMap<>(properties));
          break;
        } catch (InterruptedException e) {
          throw e;
        } catch (Exception e) {
          reason = reason(e);
        }
        if (!reason.equals(lastReason)) {
          LOG.warn("required cluster {} did not answer: {}", name, reason);
          lastReason = reason;
        }
        long now = System.nanoTime();
        if (now - lastProgress >= progressInterval.toNanos()) {
          lastProgress = now;
          LOG.info(
              "still waiting for required cluster {} after {}s and {} attempts: {}",
              name,
              Duration.ofNanos(now - started).toSeconds(),
              attempt,
              reason);
        }
        Thread.sleep(retryDelay.toMillis());
      }
      LOG.info(
          "required cluster {} answered after {}ms",
          name,
          Duration.ofNanos(System.nanoTime() - started).toMillis());
    }
  }

  /**
   * What an Admin client of the cluster is created with: the properties shared by its producers and
   * consumers, which is where the address and the connection security are.
   */
  private static Map<String, Object> properties(String name, Cluster cluster) {
    Map<String, Object> properties = new HashMap<>(cluster.getPropertiesMap());
    properties.putIfAbsent(AdminClientConfig.CLIENT_ID_CONFIG, "odradek-" + name + "-startup");
    // Each attempt has a client of its own, which starts from the bootstrap addresses anyway;
    // left to rebootstrap, a client that reaches no broker logs it hundreds of times a second.
    properties.put(AdminClientConfig.METADATA_RECOVERY_STRATEGY_CONFIG, "none");
    return properties;
  }

  /**
   * One attempt with a client of its own, so that an address that did not resolve, which fails the
   * creation of a client, is retried like a broker that did not answer.
   */
  // The cluster's name is part of the shape await() takes; this implementation has no use for it.
  @SuppressWarnings("PMD.UnusedFormalParameter")
  private static void describe(String cluster, Map<String, Object> properties) throws Exception {
    Admin admin = Admin.create(properties);
    try {
      int timeout = Math.toIntExact(ATTEMPT_TIMEOUT.toMillis());
      admin.describeCluster(new DescribeClusterOptions().timeoutMs(timeout)).nodes().get();
    } finally {
      admin.close(Duration.ZERO);
    }
  }

  /** The failure in one line: of a failed future its cause, and what caused that. */
  private static String reason(Exception e) {
    Throwable failure = e instanceof ExecutionException && e.getCause() != null ? e.getCause() : e;
    StringBuilder reason = new StringBuilder();
    int depth = 0;
    for (Throwable t = failure; t != null && depth < 4; t = t.getCause(), depth++) {
      if (depth > 0) {
        reason.append(": ");
      }
      reason.append(t.getClass().getSimpleName());
      if (t.getMessage() != null) {
        reason.append(": ").append(t.getMessage());
      }
    }
    return reason.toString();
  }
}
