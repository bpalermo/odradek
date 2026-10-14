package io.github.bpalermo.odradek.e2eclient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JsonTest {
  @Test
  void writesValues() {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("s", "a\"b\\c\n\u0001");
    map.put("n", 12L);
    map.put("d", 1.5);
    map.put("nan", Double.NaN);
    map.put("b", true);
    map.put("null", null);
    map.put("list", List.of(1, "x"));
    map.put("empty", Map.of());

    assertThat(Json.write(map))
        .isEqualTo(
            "{\"s\":\"a\\\"b\\\\c\\n\\u0001\",\"n\":12,\"d\":1.5,\"nan\":null,\"b\":true,"
                + "\"null\":null,\"list\":[1,\"x\"],\"empty\":{}}");
  }

  @Test
  void readsWhatItWrites() {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("s", "é \"q\" \t");
    map.put("n", -3L);
    map.put("d", 2.25);
    map.put("b", false);
    map.put("null", null);
    map.put("list", List.of(1L, List.of(), Map.of("k", "v")));

    assertThat(Json.parse(Json.write(map))).isEqualTo(map);
  }

  @Test
  void readsWhitespaceAndEscapes() {
    assertThat(Json.parse(" { \"a\" : [ 1 , 2e3 , \"\\u0041\\/\" ] } "))
        .isEqualTo(Map.of("a", Arrays.asList(1L, 2000.0, "A/")));
  }

  @Test
  void refusesWhatIsNotJson() {
    for (String text :
        List.of(
            "",
            "{",
            "{\"a\":1,}",
            "[1 2]",
            "\"open",
            "01",
            "1.",
            "tru",
            "{} x",
            "{\"a\":1,\"a\":2}")) {
      assertThatThrownBy(() -> Json.parse(text))
          .as(text)
          .isInstanceOf(IllegalArgumentException.class);
    }
  }
}
