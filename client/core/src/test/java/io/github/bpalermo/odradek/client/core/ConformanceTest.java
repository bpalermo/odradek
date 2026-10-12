package io.github.bpalermo.odradek.client.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.util.JsonFormat;
import io.github.bpalermo.odradek.client.v1.ConsumeAction;
import io.github.bpalermo.odradek.client.v1.ConsumeStep;
import io.github.bpalermo.odradek.client.v1.ProduceAction;
import io.github.bpalermo.odradek.client.v1.ProduceStep;
import io.github.bpalermo.odradek.client.v1.Scenario;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.snakeyaml.engine.v2.api.Load;
import org.snakeyaml.engine.v2.api.LoadSettings;

/** Runs every conformance scenario against the Java core. */
class ConformanceTest {
  private static final Path SCENARIOS = Path.of("conformance/scenarios");

  private static List<Path> files() throws IOException {
    try (Stream<Path> files = Files.walk(SCENARIOS)) {
      return files.filter(f -> f.toString().endsWith(".yaml")).sorted().toList();
    }
  }

  private static Scenario load(Path file) throws IOException {
    Object yaml = new Load(LoadSettings.builder().build()).loadFromString(Files.readString(file));
    StringBuilder json = new StringBuilder();
    writeJson(json, yaml);
    Scenario.Builder scenario = Scenario.newBuilder();
    JsonFormat.parser().merge(json.toString(), scenario);
    return scenario.build();
  }

  private static void writeJson(StringBuilder out, Object node) {
    switch (node) {
      case null -> out.append("null");
      case Map<?, ?> map -> {
        out.append('{');
        boolean first = true;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
          if (!first) {
            out.append(',');
          }
          first = false;
          writeString(out, String.valueOf(entry.getKey()));
          out.append(':');
          writeJson(out, entry.getValue());
        }
        out.append('}');
      }
      case List<?> list -> {
        out.append('[');
        for (int i = 0; i < list.size(); i++) {
          if (i > 0) {
            out.append(',');
          }
          writeJson(out, list.get(i));
        }
        out.append(']');
      }
      case Number n -> out.append(n);
      case Boolean b -> out.append(b);
      default -> writeString(out, node.toString());
    }
  }

  private static void writeString(StringBuilder out, String value) {
    out.append('"').append(value.replace("\\", "\\\\").replace("\"", "\\\"")).append('"');
  }

  @TestFactory
  List<DynamicTest> scenarios() throws IOException {
    List<DynamicTest> tests = new ArrayList<>();
    for (Path file : files()) {
      Scenario scenario = load(file);
      tests.add(DynamicTest.dynamicTest(scenario.getName(), () -> run(file, scenario)));
    }
    return tests;
  }

  private static void run(Path file, Scenario scenario) {
    switch (scenario.getSubjectCase()) {
      case PRODUCE -> {
        ProduceCore core = new ProduceCore(scenario.getProduce().getSetup());
        int index = 0;
        for (ProduceStep step : scenario.getProduce().getStepsList()) {
          List<ProduceAction> actual = core.handle(step.getEvent());
          assertThat(actual)
              .as("%s, step %s (%s)", file, index, step.getEvent().getKindCase())
              .containsExactlyElementsOf(step.getExpectList());
          index++;
        }
      }
      case CONSUME -> {
        ConsumeCore core = new ConsumeCore(scenario.getConsume().getSetup());
        int index = 0;
        for (ConsumeStep step : scenario.getConsume().getStepsList()) {
          List<ConsumeAction> actual = core.handle(step.getEvent());
          assertThat(actual)
              .as("%s, step %s (%s)", file, index, step.getEvent().getKindCase())
              .containsExactlyElementsOf(step.getExpectList());
          index++;
        }
      }
      case SUBJECT_NOT_SET -> throw new AssertionError(file + " has neither produce nor consume");
    }
  }

  @Test
  void scenarioNamesAreUniqueAndMatchTheirFiles() throws IOException {
    Set<String> names = new HashSet<>();
    List<Path> files = files();
    assertThat(files).isNotEmpty();
    for (Path file : files) {
      Scenario scenario = load(file);
      assertThat(scenario.getDescription()).as("%s description", file).isNotBlank();
      assertThat(file.getFileName().toString()).isEqualTo(scenario.getName() + ".yaml");
      assertThat(names.add(scenario.getName())).as("%s is unique", scenario.getName()).isTrue();
    }
  }
}
