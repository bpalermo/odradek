package io.github.bpalermo.odradek.server.dispatch;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.bpalermo.odradek.config.v1.PolicyRule;
import java.util.List;
import org.junit.jupiter.api.Test;

class PolicyTest {
  private static final String BILLING = "spiffe://example.org/ns/billing/sa/billing";
  private static final String AUDIT = "spiffe://example.org/ns/audit/sa/audit";
  private static final String PARTNER = "spiffe://partner.test/ns/billing/sa/billing";

  private static PolicyRule.Builder billing() {
    return PolicyRule.newBuilder()
        .addSpiffeIds(BILLING)
        .setCluster("main")
        .addTopics("orders.*")
        .addTopics("payments")
        .addGroups("billing")
        .addDeadLetterTopics("billing.dlq.*");
  }

  private static Policy policy(PolicyRule.Builder... rules) {
    return Policy.of(List.of(rules).stream().map(PolicyRule.Builder::build).toList());
  }

  @Test
  void aRuleAllowsWhatAllOfItMatches() {
    Policy policy = policy(billing());
    assertThat(policy.allows(BILLING, "main", "billing", "orders.created", "")).isTrue();
    assertThat(policy.allows(BILLING, "main", "billing", "payments", "")).isTrue();
  }

  @Test
  void everyPartMustMatch() {
    Policy policy = policy(billing());
    assertThat(policy.allows(AUDIT, "main", "billing", "orders.created", "")).isFalse();
    assertThat(policy.allows(BILLING, "other", "billing", "orders.created", "")).isFalse();
    assertThat(policy.allows(BILLING, "main", "audit", "orders.created", "")).isFalse();
    assertThat(policy.allows(BILLING, "main", "billing", "refunds", "")).isFalse();
    assertThat(policy.allows(BILLING, "main", "billing", "payments.eu", "")).isFalse();
  }

  @Test
  void anEmptyPolicyAllowsNothing() {
    Policy policy = policy();
    assertThat(policy.isEmpty()).isTrue();
    assertThat(policy.allows(BILLING, "main", "billing", "orders.created", "")).isFalse();
    assertThat(policy(billing()).isEmpty()).isFalse();
  }

  @Test
  void aSpiffeIdIsComparedWhole() {
    Policy policy = policy(billing());
    assertThat(policy.allows(BILLING + "/extra", "main", "billing", "payments", "")).isFalse();
    assertThat(
            policy.allows(
                "spiffe://example.org/ns/billing/sa/Billing", "main", "billing", "payments", ""))
        .isFalse();
    assertThat(policy.allows(PARTNER, "main", "billing", "payments", "")).isFalse();
  }

  @Test
  void aTrustDomainCoversEveryIdentityInItAndNoOther() {
    Policy policy =
        policy(
            PolicyRule.newBuilder()
                .setTrustDomain("example.org")
                .setCluster("main")
                .addTopics("audit")
                .addGroups("*"));
    assertThat(policy.allows(BILLING, "main", "anything", "audit", "")).isTrue();
    assertThat(policy.allows(AUDIT, "main", "", "audit", "")).isTrue();
    // The domain of a SPIFFE ID is not case sensitive; its path is.
    assertThat(policy.allows("spiffe://Example.ORG/x", "main", "g", "audit", "")).isTrue();
    assertThat(policy.allows(PARTNER, "main", "g", "audit", "")).isFalse();
    // Not a prefix of a domain, nor a suffix.
    assertThat(policy.allows("spiffe://example.org.evil/x", "main", "g", "audit", "")).isFalse();
    assertThat(policy.allows("spiffe://sub.example.org/x", "main", "g", "audit", "")).isFalse();
    assertThat(policy.allows("spiffe://example.org", "main", "g", "audit", "")).isTrue();
  }

  @Test
  void spiffeIdsAndTrustDomainOfOneRuleAreAlternatives() {
    Policy policy = policy(billing().setTrustDomain("partner.test"));
    assertThat(policy.allows(BILLING, "main", "billing", "payments", "")).isTrue();
    assertThat(policy.allows(PARTNER, "main", "billing", "payments", "")).isTrue();
    assertThat(policy.allows(AUDIT, "main", "billing", "payments", "")).isFalse();
  }

  @Test
  void aDeadLetterTopicMustBeAllowedByTheSameRule() {
    Policy policy = policy(billing());
    assertThat(policy.allows(BILLING, "main", "billing", "payments", "billing.dlq.payments"))
        .isTrue();
    assertThat(policy.allows(BILLING, "main", "billing", "payments", "orders.created")).isFalse();

    // A rule without dead-letter topics allows none, and the first rule's do not carry over.
    Policy two =
        policy(
            billing(),
            PolicyRule.newBuilder()
                .addSpiffeIds(BILLING)
                .setCluster("main")
                .addTopics("refunds")
                .addGroups("billing"));
    assertThat(two.allows(BILLING, "main", "billing", "refunds", "")).isTrue();
    assertThat(two.allows(BILLING, "main", "billing", "refunds", "billing.dlq.refunds")).isFalse();
  }

  @Test
  void rulesDoNotAddUp() {
    // One rule gives the topic in another group, one the group for another topic.
    Policy policy =
        policy(
            PolicyRule.newBuilder()
                .addSpiffeIds(BILLING)
                .setCluster("main")
                .addTopics("orders")
                .addGroups("reporting"),
            PolicyRule.newBuilder()
                .addSpiffeIds(BILLING)
                .setCluster("main")
                .addTopics("payments")
                .addGroups("billing"),
            PolicyRule.newBuilder()
                .addSpiffeIds(AUDIT)
                .setCluster("main")
                .addTopics("orders")
                .addGroups("billing"));
    assertThat(policy.allows(BILLING, "main", "billing", "orders", "")).isFalse();
    assertThat(policy.allows(BILLING, "main", "reporting", "orders", "")).isTrue();
    assertThat(policy.allows(BILLING, "main", "billing", "payments", "")).isTrue();
    assertThat(policy.allows(AUDIT, "main", "billing", "orders", "")).isTrue();
  }

  @Test
  void anyRuleThatAllowsIsEnough() {
    Policy policy =
        policy(
            billing(),
            PolicyRule.newBuilder()
                .setTrustDomain("example.org")
                .setCluster("other")
                .addTopics("*")
                .addGroups("*"));
    assertThat(policy.allows(BILLING, "other", "whatever", "anything", "")).isTrue();
    assertThat(policy.allows(BILLING, "main", "billing", "orders.created", "")).isTrue();
    assertThat(policy.allows(AUDIT, "main", "billing", "orders.created", "")).isFalse();
  }

  @Test
  void aPatternIsANameOrAPrefixAndAStar() {
    assertThat(Policy.matches("orders", "orders")).isTrue();
    assertThat(Policy.matches("orders", "orders.created")).isFalse();
    assertThat(Policy.matches("orders", "order")).isFalse();
    assertThat(Policy.matches("orders.*", "orders.created")).isTrue();
    assertThat(Policy.matches("orders.*", "orders.")).isTrue();
    assertThat(Policy.matches("orders.*", "orders")).isFalse();
    assertThat(Policy.matches("orders.*", "reorders.created")).isFalse();
    assertThat(Policy.matches("*", "anything")).isTrue();
    assertThat(Policy.matches("*", "")).isTrue();
    // A name is never read as a pattern: a dot is a dot.
    assertThat(Policy.matches("orders.created", "ordersXcreated")).isFalse();
  }
}
