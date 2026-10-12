package io.github.bpalermo.odradek.server.config;

import java.util.List;

/** A configuration file was rejected. Carries every problem found, not only the first. */
public final class ConfigException extends Exception {
  private static final long serialVersionUID = 1L;

  private final transient List<String> problems;

  public ConfigException(List<String> problems) {
    super(String.join("; ", problems));
    this.problems = List.copyOf(problems);
  }

  public ConfigException(String problem, Throwable cause) {
    super(problem, cause);
    this.problems = List.of(problem);
  }

  /** One line per problem. For schema violations: field path, rule id and message. */
  public List<String> problems() {
    return problems;
  }
}
