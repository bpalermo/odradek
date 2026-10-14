package io.github.bpalermo.odradek.e2eclient;

import java.io.PrintStream;
import java.util.LinkedHashMap;
import java.util.Map;

/** Where anomalies and results go: one JSON object per line. */
@FunctionalInterface
public interface Events {
  /** Emits {@code {"event": event, …fields}}. */
  void emit(String event, Map<String, Object> fields);

  /** Writes each event as one line of {@code out}, with the time it was written. */
  static Events lines(PrintStream out) {
    return (event, fields) -> {
      Map<String, Object> line = new LinkedHashMap<>();
      line.put("event", event);
      line.put("time", System.currentTimeMillis());
      line.putAll(fields);
      String text = Json.write(line);
      synchronized (out) {
        out.println(text);
        out.flush();
      }
    };
  }
}
