package io.github.bpalermo.odradek.server.dispatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import io.github.bpalermo.odradek.delivery.v1.Declare;
import io.github.bpalermo.odradek.delivery.v1.OffsetReset;
import io.github.bpalermo.odradek.delivery.v1.OnExhausted;
import io.github.bpalermo.odradek.delivery.v1.Ordering;
import io.github.bpalermo.odradek.delivery.v1.Settings;
import io.github.bpalermo.odradek.server.dispatch.Declarations.Declared;
import io.github.bpalermo.odradek.server.dispatch.Declarations.Refused;
import io.grpc.Status;
import java.time.Duration;
import java.util.Set;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class DeclarationsTest {
  private static final Set<String> CLUSTERS = Set.of("main", "other");

  /** A declaration that is accepted as it is. */
  private static Declare.Builder declare() {
    return Declare.newBuilder()
        .setCluster("main")
        .setGroupId("billing")
        .setTopic("orders.created")
        .setSettings(Settings.newBuilder().setMaxProcessingMs(30_000));
  }

  private static Declare with(Consumer<Settings.Builder> settings) {
    Declare.Builder declare = declare();
    settings.accept(declare.getSettingsBuilder());
    return declare.build();
  }

  private static Declared accepted(Declare declare) throws Refused {
    return Declarations.check(declare, CLUSTERS);
  }

  private static Status refused(Declare declare) {
    Refused refused =
        catchThrowableOfType(Refused.class, () -> Declarations.check(declare, CLUSTERS));
    assertThat(refused).as("refused: %s", declare).isNotNull();
    return refused.status();
  }

  private static void invalid(Declare declare, String description) {
    Status status = refused(declare);
    assertThat(status.getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
    assertThat(status.getDescription()).contains(description);
  }

  @Test
  void defaultsAreFilledIn() throws Refused {
    Declared declared = accepted(declare().build());
    assertThat(declared.cluster()).isEqualTo("main");
    assertThat(declared.group()).isEqualTo("billing");
    assertThat(declared.topic()).isEqualTo("orders.created");
    assertThat(declared.settings())
        .isEqualTo(
            Settings.newBuilder()
                .setOrdering(Ordering.ORDERING_KEY)
                .setMaxProcessingMs(30_000)
                .setOffsetReset(OffsetReset.OFFSET_RESET_EARLIEST)
                .setMaxAttempts(0)
                .setOnExhausted(OnExhausted.ON_EXHAUSTED_BLOCK)
                .build());
    assertThat(declared.settings().hasStartTimestampMs()).isFalse();
  }

  @Test
  void whatIsDeclaredIsKept() throws Refused {
    Declared declared =
        accepted(
            with(
                s ->
                    s.setOrdering(Ordering.ORDERING_PARTITION)
                        .setOffsetReset(OffsetReset.OFFSET_RESET_LATEST)
                        .setStartTimestampMs(1_700_000_000_000L)
                        .setMaxAttempts(5)
                        .setOnExhausted(OnExhausted.ON_EXHAUSTED_BLOCK)));
    Settings settings = declared.settings();
    assertThat(settings.getOrdering()).isEqualTo(Ordering.ORDERING_PARTITION);
    assertThat(settings.getOffsetReset()).isEqualTo(OffsetReset.OFFSET_RESET_LATEST);
    assertThat(settings.getStartTimestampMs()).isEqualTo(1_700_000_000_000L);
    assertThat(settings.getMaxAttempts()).isEqualTo(5);
    assertThat(settings.getOnExhausted()).isEqualTo(OnExhausted.ON_EXHAUSTED_BLOCK);
    assertThat(declared.maxProcessing()).isEqualTo(Duration.ofSeconds(30));
  }

  @Test
  void aDeclarationMustNameClusterGroupAndTopic() {
    invalid(declare().clearCluster().build(), "Declare.cluster is required");
    invalid(declare().clearGroupId().build(), "Declare.group_id is required");
    invalid(declare().clearTopic().build(), "Declare.topic is required");
    invalid(declare().setTopic("no spaces").build(), "Declare.topic is not a topic name");
    invalid(declare().setTopic("a".repeat(250)).build(), "Declare.topic is not a topic name");
  }

  @Test
  void theProcessingDeadlineIsRequired() {
    invalid(declare().clearSettings().build(), "max_processing_ms is required and positive");
    invalid(with(s -> s.setMaxProcessingMs(0)), "max_processing_ms is required and positive");
  }

  @Test
  void valuesFromANewerProtocolAreRefused() {
    invalid(with(s -> s.setOrderingValue(99)), "a value this dispatcher does not know");
    invalid(with(s -> s.setOffsetResetValue(99)), "a value this dispatcher does not know");
    invalid(with(s -> s.setOnExhaustedValue(99)), "a value this dispatcher does not know");
  }

  @Test
  void aNegativeStartTimestampIsRefused() {
    invalid(with(s -> s.setStartTimestampMs(-1)), "start_timestamp_ms must not be negative");
  }

  @Test
  void theDeadLetterTopicGoesWithDeadLetteringAndOnlyWithIt() {
    invalid(
        with(s -> s.setMaxAttempts(3).setOnExhausted(OnExhausted.ON_EXHAUSTED_DEAD_LETTER)),
        "dead_letter_topic must be set when");
    invalid(with(s -> s.setDeadLetterTopic("orders.dlq")), "dead_letter_topic must be set when");
    invalid(
        with(
            s ->
                s.setMaxAttempts(3)
                    .setOnExhausted(OnExhausted.ON_EXHAUSTED_DEAD_LETTER)
                    .setDeadLetterTopic("orders.created")),
        "must not be the topic itself");
    invalid(
        with(
            s ->
                s.setMaxAttempts(3)
                    .setOnExhausted(OnExhausted.ON_EXHAUSTED_DEAD_LETTER)
                    .setDeadLetterTopic("not a topic")),
        "dead_letter_topic is not a topic name");
  }

  @Test
  void aClusterTheDispatcherLacksIsRefusedAsNotAllowed() {
    Status status = refused(declare().setCluster("elsewhere").build());
    assertThat(status.getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
    // The same words as for a cluster the policy does not allow: no probing for names.
    assertThat(status.getDescription())
        .isEqualTo(
            "not allowed to consume topic orders.created in group billing on cluster elsewhere");
  }

  @Test
  void aMalformedDeclarationIsReportedBeforeTheCluster() {
    invalid(declare().setCluster("elsewhere").clearTopic().build(), "Declare.topic is required");
  }

  @Test
  void deadLetteringIsNotImplementedYet() {
    Declare declare =
        with(
            s ->
                s.setMaxAttempts(3)
                    .setOnExhausted(OnExhausted.ON_EXHAUSTED_DEAD_LETTER)
                    .setDeadLetterTopic("orders.dlq"));
    Status status = refused(declare);
    assertThat(status.getCode()).isEqualTo(Status.Code.UNIMPLEMENTED);
    assertThat(status.getDescription()).contains("ON_EXHAUSTED_DEAD_LETTER is not implemented");
    // Not a way to learn which clusters exist.
    assertThat(refused(declare.toBuilder().setCluster("elsewhere").build()).getCode())
        .isEqualTo(Status.Code.PERMISSION_DENIED);
  }

  @Test
  void aDeclarationAgreesWithItsOwnSettingsHoweverTheyAreSpelled() throws Refused {
    Settings inForce = accepted(declare().build()).settings();
    // Every default written out.
    Declared explicit =
        accepted(
            with(
                s ->
                    s.setOrdering(Ordering.ORDERING_KEY)
                        .setOffsetReset(OffsetReset.OFFSET_RESET_EARLIEST)
                        .setMaxAttempts(0)
                        .setOnExhausted(OnExhausted.ON_EXHAUSTED_BLOCK)));
    assertThat(explicit.agreesWith(inForce)).isTrue();
    assertThat(explicit.settings()).isEqualTo(inForce);
  }

  @Test
  void theProcessingDeadlineTakesNoPartInTheComparison() throws Refused {
    Settings inForce = accepted(declare().build()).settings();
    Declared other = accepted(with(s -> s.setMaxProcessingMs(90_000)));
    assertThat(other.agreesWith(inForce)).isTrue();
    assertThat(other.settings()).isNotEqualTo(inForce);
  }

  @Test
  void everyOtherSettingDoes() throws Refused {
    Settings inForce = accepted(declare().build()).settings();
    assertThat(accepted(with(s -> s.setOrdering(Ordering.ORDERING_NONE))).agreesWith(inForce))
        .isFalse();
    assertThat(
            accepted(with(s -> s.setOffsetReset(OffsetReset.OFFSET_RESET_LATEST)))
                .agreesWith(inForce))
        .isFalse();
    assertThat(accepted(with(s -> s.setStartTimestampMs(0))).agreesWith(inForce)).isFalse();
    assertThat(accepted(with(s -> s.setMaxAttempts(3))).agreesWith(inForce)).isFalse();
  }

  @Test
  void settingsBecomeTheCoresWithTheDispatchersBounds() throws Refused {
    DispatchSettings core =
        accepted(with(s -> s.setOrdering(Ordering.ORDERING_NONE).setMaxAttempts(4)))
            .toCore(1000, 50, 4096, Duration.ofSeconds(10));
    assertThat(core)
        .isEqualTo(
            new DispatchSettings(
                DispatchSettings.Ordering.NONE,
                Duration.ofSeconds(30),
                4,
                DispatchSettings.OnExhausted.BLOCK,
                1000,
                50,
                4096,
                Duration.ofSeconds(10)));
    assertThat(accepted(declare().build()).toCore(1, 1, 1, Duration.ZERO).ordering())
        .isEqualTo(DispatchSettings.Ordering.KEY);
    assertThat(
            accepted(with(s -> s.setOrdering(Ordering.ORDERING_PARTITION)))
                .toCore(1, 1, 1, Duration.ZERO)
                .ordering())
        .isEqualTo(DispatchSettings.Ordering.PARTITION);
  }

  @Test
  void settingsAreDescribedForTheRefusalOfADifferentDeclaration() throws Refused {
    assertThat(Declarations.describe(accepted(declare().build()).settings()))
        .isEqualTo("ordering ORDERING_KEY, offset_reset OFFSET_RESET_EARLIEST, no max_attempts");
    assertThat(
            Declarations.describe(
                accepted(
                        with(
                            s ->
                                s.setOrdering(Ordering.ORDERING_NONE)
                                    .setStartTimestampMs(5)
                                    .setMaxAttempts(3)))
                    .settings()))
        .isEqualTo(
            "ordering ORDERING_NONE, offset_reset OFFSET_RESET_EARLIEST, start_timestamp_ms 5,"
                + " max_attempts 3, on_exhausted ON_EXHAUSTED_BLOCK");
  }
}
