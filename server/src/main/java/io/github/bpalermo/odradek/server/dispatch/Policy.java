package io.github.bpalermo.odradek.server.dispatch;

import io.github.bpalermo.odradek.config.v1.PolicyRule;
import io.github.bpalermo.odradek.server.tls.PeerPolicy;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * The dispatcher's policy: which sidecars may consume which topics, in which groups, on which
 * cluster ({@code dispatch.policy}). No I/O and no state.
 *
 * <p>A declaration is allowed when one rule allows all of it: the identity, the cluster, the topic,
 * the group and, when it names one, the dead-letter topic. Two rules that each allow a part do not
 * add up. With no rule nothing is allowed.
 */
final class Policy {
  private final List<Rule> rules;

  /** One rule, as configured but for the trust domain, which is compared in lower case. */
  private record Rule(
      Set<String> spiffeIds,
      String trustDomain,
      String cluster,
      List<String> topics,
      List<String> groups,
      List<String> deadLetterTopics) {

    boolean appliesTo(String spiffeId) {
      return spiffeIds.contains(spiffeId)
          || (!trustDomain.isEmpty() && trustDomain.equals(PeerPolicy.trustDomain(spiffeId)));
    }
  }

  private Policy(List<Rule> rules) {
    this.rules = rules;
  }

  static Policy of(List<PolicyRule> rules) {
    return new Policy(
        rules.stream()
            .map(
                rule ->
                    new Rule(
                        Set.copyOf(rule.getSpiffeIdsList()),
                        rule.getTrustDomain().toLowerCase(Locale.ROOT),
                        rule.getCluster(),
                        List.copyOf(rule.getTopicsList()),
                        List.copyOf(rule.getGroupsList()),
                        List.copyOf(rule.getDeadLetterTopicsList())))
            .toList());
  }

  /** Whether the policy has no rule, and so allows nothing. */
  boolean isEmpty() {
    return rules.isEmpty();
  }

  /**
   * Whether a rule allows this sidecar to consume the topic in the group on the cluster.
   *
   * @param spiffeId the sidecar's SPIFFE ID, as its certificate carries it
   * @param deadLetterTopic the dead-letter topic the declaration names, or empty for none
   */
  boolean allows(
      String spiffeId, String cluster, String group, String topic, String deadLetterTopic) {
    for (Rule rule : rules) {
      if (rule.appliesTo(spiffeId)
          && rule.cluster().equals(cluster)
          && any(rule.topics(), topic)
          && any(rule.groups(), group)
          && (deadLetterTopic.isEmpty() || any(rule.deadLetterTopics(), deadLetterTopic))) {
        return true;
      }
    }
    return false;
  }

  private static boolean any(List<String> patterns, String name) {
    for (String pattern : patterns) {
      if (matches(pattern, name)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Whether a name matches a pattern: an exact name, or a prefix followed by one {@code *}, which
   * stands for any characters, none included. The configuration admits no other wildcard.
   */
  static boolean matches(String pattern, String name) {
    if (pattern.endsWith("*")) {
      return name.startsWith(pattern.substring(0, pattern.length() - 1));
    }
    return pattern.equals(name);
  }
}
