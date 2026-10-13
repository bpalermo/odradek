package io.github.bpalermo.odradek.server.dispatch;

import io.github.bpalermo.odradek.delivery.v1.Declare;
import io.github.bpalermo.odradek.delivery.v1.OffsetReset;
import io.github.bpalermo.odradek.delivery.v1.OnExhausted;
import io.github.bpalermo.odradek.delivery.v1.Ordering;
import io.github.bpalermo.odradek.delivery.v1.Settings;
import io.grpc.Status;
import java.time.Duration;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * What a sidecar's declaration means: whether it is well formed, the settings it asks for with
 * every default filled in, and whether those agree with the settings in force. No I/O and no state.
 */
final class Declarations {
  /** The names Kafka accepts for a topic. */
  private static final Pattern TOPIC = Pattern.compile("^[a-zA-Z0-9._-]{1,249}$");

  private Declarations() {}

  /** A declaration that was refused, with the status the stream ends with. */
  static final class Refused extends Exception {
    private static final long serialVersionUID = 1L;
    private final transient Status status;

    Refused(Status status) {
      super(status.getDescription(), null, false, false);
      this.status = status;
    }

    Status status() {
      return status;
    }
  }

  /**
   * A declaration the dispatcher can act on.
   *
   * @param settings what it asks for, with every default filled in: what {@code Accepted} carries
   *     when this declaration is the one that decides
   */
  record Declared(String cluster, String group, String topic, Settings settings) {

    /** The processing deadline it declares. */
    Duration maxProcessing() {
      return Duration.ofMillis(settings.getMaxProcessingMs());
    }

    /**
     * Whether this asks for the settings in force. The processing deadline takes no part: it comes
     * from the application and changes between its versions, so two declarations that differ only
     * in it agree, and the latest one's deadline is used.
     */
    boolean agreesWith(Settings inForce) {
      return withoutDeadline(settings).equals(withoutDeadline(inForce));
    }

    /** The settings the core starts with, given the bounds the dispatcher sets itself. */
    DispatchSettings toCore(
        long window, int maxBufferedRecords, long maxBufferedBytes, Duration revokeGrace) {
      return new DispatchSettings(
          switch (settings.getOrdering()) {
            case ORDERING_PARTITION -> DispatchSettings.Ordering.PARTITION;
            case ORDERING_NONE -> DispatchSettings.Ordering.NONE;
            default -> DispatchSettings.Ordering.KEY;
          },
          maxProcessing(),
          settings.getMaxAttempts(),
          settings.getOnExhausted() == OnExhausted.ON_EXHAUSTED_DEAD_LETTER
              ? DispatchSettings.OnExhausted.DEAD_LETTER
              : DispatchSettings.OnExhausted.BLOCK,
          window,
          maxBufferedRecords,
          maxBufferedBytes,
          revokeGrace);
    }
  }

  private static Settings withoutDeadline(Settings settings) {
    return settings.toBuilder().clearMaxProcessingMs().build();
  }

  /**
   * Checks a declaration and fills in its defaults.
   *
   * @param clusters the names of the clusters the dispatcher has
   * @param policy what sidecars are allowed, or null when the delivery port has no TLS and so
   *     nobody has an identity: every cluster the dispatcher has is then open to whoever asks
   * @param spiffeId the sidecar's identity; not null when there is a policy
   * @throws Refused with INVALID_ARGUMENT when it is malformed, PERMISSION_DENIED when it names a
   *     cluster the dispatcher does not have or the policy does not allow it, UNIMPLEMENTED when it
   *     asks for dead-lettering
   */
  static Declared check(Declare declare, Set<String> clusters, Policy policy, String spiffeId)
      throws Refused {
    if (declare.getCluster().isEmpty()) {
      throw invalid("Declare.cluster is required");
    }
    if (declare.getGroupId().isEmpty()) {
      throw invalid("Declare.group_id is required");
    }
    if (declare.getTopic().isEmpty()) {
      throw invalid("Declare.topic is required");
    }
    if (!TOPIC.matcher(declare.getTopic()).matches()) {
      throw invalid("Declare.topic is not a topic name");
    }
    Settings asked = declare.getSettings();
    if (asked.getMaxProcessingMs() == 0) {
      throw invalid("Declare.settings.max_processing_ms is required and positive");
    }
    if (asked.getOrdering() == Ordering.UNRECOGNIZED
        || asked.getOffsetReset() == OffsetReset.UNRECOGNIZED
        || asked.getOnExhausted() == OnExhausted.UNRECOGNIZED) {
      throw invalid("Declare.settings has a value this dispatcher does not know");
    }
    if (asked.hasStartTimestampMs() && asked.getStartTimestampMs() < 0) {
      throw invalid("Declare.settings.start_timestamp_ms must not be negative");
    }
    boolean deadLetter = asked.getOnExhausted() == OnExhausted.ON_EXHAUSTED_DEAD_LETTER;
    if (deadLetter == asked.getDeadLetterTopic().isEmpty()) {
      throw invalid(
          "Declare.settings.dead_letter_topic must be set when on_exhausted is"
              + " ON_EXHAUSTED_DEAD_LETTER, and only then");
    }
    if (deadLetter) {
      if (!TOPIC.matcher(asked.getDeadLetterTopic()).matches()) {
        throw invalid("Declare.settings.dead_letter_topic is not a topic name");
      }
      if (asked.getDeadLetterTopic().equals(declare.getTopic())) {
        throw invalid("Declare.settings.dead_letter_topic must not be the topic itself");
      }
    }
    // After everything that is wrong whoever asks, and before anything else: what a sidecar is
    // told about what it may not have must not depend on what else it asked for. One answer for
    // every reason, in the declaration's own words: it tells a sidecar nothing about the clusters
    // the dispatcher has, the rules of other identities, or which part of its own rules failed.
    boolean allowed =
        clusters.contains(declare.getCluster())
            && (policy == null
                || policy.allows(
                    spiffeId,
                    declare.getCluster(),
                    declare.getGroupId(),
                    declare.getTopic(),
                    asked.getDeadLetterTopic()));
    if (!allowed) {
      throw new Refused(
          Status.PERMISSION_DENIED.withDescription(
              (policy == null ? "" : spiffeId + " is ")
                  + "not allowed to consume topic "
                  + declare.getTopic()
                  + " in group "
                  + declare.getGroupId()
                  + " on cluster "
                  + declare.getCluster()
                  + (deadLetter ? " with dead-letter topic " + asked.getDeadLetterTopic() : "")));
    }
    if (deadLetter) {
      throw new Refused(
          Status.UNIMPLEMENTED.withDescription(
              "on_exhausted ON_EXHAUSTED_DEAD_LETTER is not implemented by this dispatcher yet;"
                  + " declare ON_EXHAUSTED_BLOCK, or no max_attempts"));
    }

    Settings.Builder settings =
        Settings.newBuilder()
            .setOrdering(
                asked.getOrdering() == Ordering.ORDERING_UNSPECIFIED
                    ? Ordering.ORDERING_KEY
                    : asked.getOrdering())
            .setMaxProcessingMs(asked.getMaxProcessingMs())
            .setOffsetReset(
                asked.getOffsetReset() == OffsetReset.OFFSET_RESET_UNSPECIFIED
                    ? OffsetReset.OFFSET_RESET_EARLIEST
                    : asked.getOffsetReset())
            .setMaxAttempts(asked.getMaxAttempts())
            .setOnExhausted(OnExhausted.ON_EXHAUSTED_BLOCK);
    if (asked.hasStartTimestampMs()) {
      settings.setStartTimestampMs(asked.getStartTimestampMs());
    }
    return new Declared(
        declare.getCluster(), declare.getGroupId(), declare.getTopic(), settings.build());
  }

  /** The settings on one line, for the refusal of a declaration that asks for others. */
  static String describe(Settings settings) {
    StringBuilder text =
        new StringBuilder()
            .append("ordering ")
            .append(settings.getOrdering())
            .append(", offset_reset ")
            .append(settings.getOffsetReset());
    if (settings.hasStartTimestampMs()) {
      text.append(", start_timestamp_ms ").append(settings.getStartTimestampMs());
    }
    if (settings.getMaxAttempts() == 0) {
      text.append(", no max_attempts");
    } else {
      text.append(", max_attempts ")
          .append(settings.getMaxAttempts())
          .append(", on_exhausted ")
          .append(settings.getOnExhausted());
    }
    if (!settings.getDeadLetterTopic().isEmpty()) {
      text.append(", dead_letter_topic ").append(settings.getDeadLetterTopic());
    }
    return text.toString();
  }

  private static Refused invalid(String description) {
    return new Refused(Status.INVALID_ARGUMENT.withDescription(description));
  }
}
