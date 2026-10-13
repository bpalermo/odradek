package io.github.bpalermo.odradek.server.dispatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import io.github.bpalermo.odradek.config.v1.PolicyRule;
import io.github.bpalermo.odradek.delivery.v1.Declare;
import io.github.bpalermo.odradek.delivery.v1.OffsetReset;
import io.github.bpalermo.odradek.delivery.v1.OnExhausted;
import io.github.bpalermo.odradek.delivery.v1.Ordering;
import io.github.bpalermo.odradek.delivery.v1.Settings;
import io.github.bpalermo.odradek.server.dispatch.Declarations.Declared;
import io.github.bpalermo.odradek.server.dispatch.Declarations.Refused;
import io.grpc.Status;
import java.time.Duration;
import java.util.List;
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
    return Declarations.check(declare, CLUSTERS, null, null);
  }

  private static Status refused(Declare declare) {
    Refused refused =
        catchThrowableOfType(
            Refused.class, () -> Declarations.check(declare, CLUSTERS, null, null));
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
                .setRedeliveryBackoffMs(1000)
                .setRedeliveryBackoffMaxMs(30_000)
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

  // --- With a policy: the delivery port has TLS, and the sidecar an identity ---

  private static final String BILLING = "spiffe://example.org/ns/billing/sa/billing";
  private static final String AUDIT = "spiffe://example.org/ns/audit/sa/audit";

  /** Billing may consume orders.* in its group, with one dead-letter topic; audit, one topic. */
  private static final Policy POLICY =
      Policy.of(
          List.of(
              PolicyRule.newBuilder()
                  .addSpiffeIds(BILLING)
                  .setCluster("main")
                  .addTopics("orders.*")
                  .addGroups("billing")
                  .addDeadLetterTopics("billing.dlq")
                  .build(),
              PolicyRule.newBuilder()
                  .addSpiffeIds(AUDIT)
                  .setCluster("other")
                  .addTopics("secret.ledger")
                  .addGroups("audit")
                  .build()));

  private static Status refused(Declare declare, String spiffeId) {
    Refused refused =
        catchThrowableOfType(
            Refused.class, () -> Declarations.check(declare, CLUSTERS, POLICY, spiffeId));
    assertThat(refused).as("refused: %s", declare).isNotNull();
    return refused.status();
  }

  @Test
  void whatThePolicyAllowsIsAccepted() throws Refused {
    Declared declared = Declarations.check(declare().build(), CLUSTERS, POLICY, BILLING);
    assertThat(declared.topic()).isEqualTo("orders.created");
  }

  @Test
  void whatThePolicyDoesNotAllowIsRefusedInTheDeclarationsOwnWords() {
    // Another identity's topic, an identity with no rule, a group, a cluster that does not exist:
    // one status, and a message made of nothing but who asked and what was asked.
    Declare ledger =
        declare().setCluster("other").setGroupId("audit").setTopic("secret.ledger").build();
    Status status = refused(ledger, BILLING);
    assertThat(status.getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
    assertThat(status.getDescription())
        .isEqualTo(
            BILLING
                + " is not allowed to consume topic secret.ledger in group audit on cluster"
                + " other");

    assertThat(refused(declare().build(), "spiffe://example.org/nobody").getDescription())
        .isEqualTo(
            "spiffe://example.org/nobody is not allowed to consume topic orders.created in group"
                + " billing on cluster main");
    assertThat(refused(declare().setGroupId("audit").build(), BILLING).getDescription())
        .isEqualTo(
            BILLING
                + " is not allowed to consume topic orders.created in group audit on cluster"
                + " main");
    assertThat(refused(declare().setCluster("elsewhere").build(), BILLING).getDescription())
        .isEqualTo(
            BILLING
                + " is not allowed to consume topic orders.created in group billing on cluster"
                + " elsewhere");
  }

  @Test
  void aRefusalSaysNothingOfTheRulesOfOthers() {
    // What audit may have appears in a refusal only when the declaration itself named it.
    Status status = refused(declare().setTopic("payments").build(), BILLING);
    assertThat(status.getDescription()).doesNotContain("secret.ledger", AUDIT, "audit", "other");
  }

  @Test
  void aDeadLetterTopicIsAllowedOrRefusedByThePolicyWithTheRest() throws Refused {
    Declare elsewhere =
        with(
            s ->
                s.setMaxAttempts(3)
                    .setOnExhausted(OnExhausted.ON_EXHAUSTED_DEAD_LETTER)
                    .setDeadLetterTopic("secret.ledger"));
    Status status = refused(elsewhere, BILLING);
    assertThat(status.getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
    assertThat(status.getDescription())
        .isEqualTo(
            BILLING
                + " is not allowed to consume topic orders.created in group billing on cluster"
                + " main with dead-letter topic secret.ledger");

    // An allowed one is accepted, and is in the settings in force.
    Declare allowed =
        with(
            s ->
                s.setMaxAttempts(3)
                    .setOnExhausted(OnExhausted.ON_EXHAUSTED_DEAD_LETTER)
                    .setDeadLetterTopic("billing.dlq"));
    Declared declared = Declarations.check(allowed, CLUSTERS, POLICY, BILLING);
    assertThat(declared.deadLetterTopic()).isEqualTo("billing.dlq");
    assertThat(declared.settings().getOnExhausted())
        .isEqualTo(OnExhausted.ON_EXHAUSTED_DEAD_LETTER);
  }

  @Test
  void aMalformedDeclarationIsReportedBeforeThePolicy() {
    Refused refused =
        catchThrowableOfType(
            Refused.class,
            () ->
                Declarations.check(
                    declare().clearGroupId().build(), CLUSTERS, POLICY, "spiffe://x.test/nobody"));
    assertThat(refused.status().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
  }

  @Test
  void aMalformedDeclarationIsReportedBeforeTheCluster() {
    invalid(declare().setCluster("elsewhere").clearTopic().build(), "Declare.topic is required");
  }

  @Test
  void deadLetteringIsAcceptedAndBecomesTheCores() throws Refused {
    Declare declare =
        with(
            s ->
                s.setMaxAttempts(3)
                    .setOnExhausted(OnExhausted.ON_EXHAUSTED_DEAD_LETTER)
                    .setDeadLetterTopic("orders.dlq"));
    Declared declared = accepted(declare);
    assertThat(declared.deadLetterTopic()).isEqualTo("orders.dlq");
    assertThat(declared.settings().getDeadLetterTopic()).isEqualTo("orders.dlq");
    DispatchSettings core =
        declared.toCore(1, 1, 1, Duration.ZERO, Duration.ofSeconds(1), Duration.ofSeconds(30));
    assertThat(core.onExhausted()).isEqualTo(DispatchSettings.OnExhausted.DEAD_LETTER);
    assertThat(core.maxAttempts()).isEqualTo(3);
    assertThat(core.deadLetterRetry()).isEqualTo(Duration.ofSeconds(1));
    assertThat(core.deadLetterRetryMax()).isEqualTo(Duration.ofSeconds(30));
    // Another dead-letter topic is another way to consume the topic.
    assertThat(
            accepted(
                    with(
                        s ->
                            s.setMaxAttempts(3)
                                .setOnExhausted(OnExhausted.ON_EXHAUSTED_DEAD_LETTER)
                                .setDeadLetterTopic("orders.other")))
                .agreesWith(declared.settings()))
        .isFalse();
    assertThat(accepted(declare().build()).deadLetterTopic()).isNull();
    // Without a limit nothing is ever exhausted: refused where the mistake is.
    invalid(
        with(
            s ->
                s.setOnExhausted(OnExhausted.ON_EXHAUSTED_DEAD_LETTER)
                    .setDeadLetterTopic("orders.dlq")),
        "on_exhausted needs max_attempts");
  }

  @Test
  void theWaitBeforeARedeliveryHasDefaultsThatGiveWayToEachOther() throws Refused {
    Settings defaults = accepted(declare().build()).settings();
    assertThat(defaults.getRedeliveryBackoffMs()).isEqualTo(1000);
    assertThat(defaults.getRedeliveryBackoffMaxMs()).isEqualTo(30_000);

    Settings none = accepted(with(s -> s.setRedeliveryBackoffMs(0))).settings();
    assertThat(none.hasRedeliveryBackoffMs()).isTrue();
    assertThat(none.getRedeliveryBackoffMs()).isZero();
    assertThat(none.getRedeliveryBackoffMaxMs()).isEqualTo(30_000);

    // A first wait above the default longest one raises it, and a longest wait below the default
    // first one lowers that.
    Settings slow = accepted(with(s -> s.setRedeliveryBackoffMs(60_000))).settings();
    assertThat(slow.getRedeliveryBackoffMaxMs()).isEqualTo(60_000);
    Settings quick = accepted(with(s -> s.setRedeliveryBackoffMaxMs(200))).settings();
    assertThat(quick.getRedeliveryBackoffMs()).isEqualTo(200);
    assertThat(quick.getRedeliveryBackoffMaxMs()).isEqualTo(200);

    Declared both =
        accepted(with(s -> s.setRedeliveryBackoffMs(50).setRedeliveryBackoffMaxMs(400)));
    DispatchSettings core = both.toCore(1, 1, 1, Duration.ZERO, Duration.ZERO, Duration.ZERO);
    assertThat(core.redeliveryBackoff()).isEqualTo(Duration.ofMillis(50));
    assertThat(core.redeliveryBackoffMax()).isEqualTo(Duration.ofMillis(400));

    // It is one of the settings a group shares: declared the default way or written out, the same;
    // otherwise another way to consume the topic.
    assertThat(
            accepted(with(s -> s.setRedeliveryBackoffMs(1000).setRedeliveryBackoffMaxMs(30_000)))
                .agreesWith(defaults))
        .isTrue();
    assertThat(both.agreesWith(defaults)).isFalse();
  }

  @Test
  void aWaitThatContradictsItselfOrIsLongerThanADayIsRefused() {
    invalid(
        with(s -> s.setRedeliveryBackoffMs(500).setRedeliveryBackoffMaxMs(400)),
        "redelivery_backoff_ms must not exceed redelivery_backoff_max_ms");
    invalid(with(s -> s.setRedeliveryBackoffMs(86_400_001)), "must not exceed 86400000");
    invalid(with(s -> s.setRedeliveryBackoffMaxMs(-1)), "must not exceed 86400000");
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
            .toCore(
                1000,
                50,
                4096,
                Duration.ofSeconds(10),
                Duration.ofSeconds(2),
                Duration.ofSeconds(8));
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
                Duration.ofSeconds(10),
                Duration.ofSeconds(1),
                Duration.ofSeconds(30),
                Duration.ofSeconds(2),
                Duration.ofSeconds(8)));
    assertThat(
            accepted(declare().build())
                .toCore(1, 1, 1, Duration.ZERO, Duration.ZERO, Duration.ZERO)
                .ordering())
        .isEqualTo(DispatchSettings.Ordering.KEY);
    assertThat(
            accepted(with(s -> s.setOrdering(Ordering.ORDERING_PARTITION)))
                .toCore(1, 1, 1, Duration.ZERO, Duration.ZERO, Duration.ZERO)
                .ordering())
        .isEqualTo(DispatchSettings.Ordering.PARTITION);
  }

  @Test
  void settingsAreDescribedForTheRefusalOfADifferentDeclaration() throws Refused {
    assertThat(Declarations.describe(accepted(declare().build()).settings()))
        .isEqualTo(
            "ordering ORDERING_KEY, offset_reset OFFSET_RESET_EARLIEST, no max_attempts,"
                + " redelivery_backoff_ms 1000 to 30000");
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
                + " max_attempts 3, on_exhausted ON_EXHAUSTED_BLOCK, redelivery_backoff_ms 1000 to"
                + " 30000");
    assertThat(
            Declarations.describe(
                accepted(
                        with(
                            s ->
                                s.setMaxAttempts(2)
                                    .setOnExhausted(OnExhausted.ON_EXHAUSTED_DEAD_LETTER)
                                    .setDeadLetterTopic("orders.dlq")
                                    .setRedeliveryBackoffMs(0)))
                    .settings()))
        .isEqualTo(
            "ordering ORDERING_KEY, offset_reset OFFSET_RESET_EARLIEST, max_attempts 2,"
                + " on_exhausted ON_EXHAUSTED_DEAD_LETTER, dead_letter_topic orders.dlq,"
                + " redelivery_backoff_ms 0 to 30000");
  }
}
