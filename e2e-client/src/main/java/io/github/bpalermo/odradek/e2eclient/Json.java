package io.github.bpalermo.odradek.e2eclient;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The little JSON this program needs: writing maps, lists, strings, numbers and booleans, and
 * reading it back. Written here rather than added as a dependency.
 */
public final class Json {
  private Json() {}

  /**
   * Writes {@code value}: a Map (string keys), an Iterable, a CharSequence, a Number, a Boolean.
   */
  public static String write(Object value) {
    StringBuilder out = new StringBuilder();
    write(out, value);
    return out.toString();
  }

  private static void write(StringBuilder out, Object value) {
    switch (value) {
      case null -> out.append("null");
      case CharSequence text -> string(out, text);
      case Boolean flag -> out.append(flag);
      case Double number when number.isNaN() || number.isInfinite() -> out.append("null");
      case Float number when number.isNaN() || number.isInfinite() -> out.append("null");
      case Number number -> out.append(number);
      case Map<?, ?> map -> {
        out.append('{');
        boolean first = true;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
          if (!first) {
            out.append(',');
          }
          first = false;
          string(out, String.valueOf(entry.getKey()));
          out.append(':');
          write(out, entry.getValue());
        }
        out.append('}');
      }
      case Iterable<?> items -> {
        out.append('[');
        boolean first = true;
        for (Object item : items) {
          if (!first) {
            out.append(',');
          }
          first = false;
          write(out, item);
        }
        out.append(']');
      }
      default -> string(out, value.toString());
    }
  }

  private static void string(StringBuilder out, CharSequence text) {
    out.append('"');
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      switch (c) {
        case '"' -> out.append("\\\"");
        case '\\' -> out.append("\\\\");
        case '\n' -> out.append("\\n");
        case '\r' -> out.append("\\r");
        case '\t' -> out.append("\\t");
        default -> {
          if (c < 0x20) {
            out.append(String.format(Locale.ROOT, "\\u%04x", (int) c));
          } else {
            out.append(c);
          }
        }
      }
    }
    out.append('"');
  }

  /**
   * Reads one JSON value. Objects become {@link LinkedHashMap}s, arrays {@link ArrayList}s,
   * integers that fit {@link Long}s, other numbers {@link Double}s.
   *
   * @throws IllegalArgumentException if {@code text} is not one JSON value
   */
  public static Object parse(String text) {
    Reader reader = new Reader(text);
    reader.space();
    Object value = reader.value();
    reader.space();
    if (reader.pos != text.length()) {
      throw reader.error("trailing characters");
    }
    return value;
  }

  private static final class Reader {
    private final String text;
    private int pos;

    Reader(String text) {
      this.text = text;
    }

    IllegalArgumentException error(String what) {
      return error(what, null);
    }

    IllegalArgumentException error(String what, Throwable cause) {
      return new IllegalArgumentException("invalid JSON at " + pos + ": " + what, cause);
    }

    /** Skips a comma, and the space after it, if there is one there. */
    boolean comma() {
      space();
      if (pos < text.length() && text.charAt(pos) == ',') {
        pos++;
        space();
        return true;
      }
      return false;
    }

    void space() {
      while (pos < text.length() && Character.isWhitespace(text.charAt(pos))) {
        pos++;
      }
    }

    char peek() {
      if (pos >= text.length()) {
        throw error("unexpected end");
      }
      return text.charAt(pos);
    }

    void expect(char c) {
      if (peek() != c) {
        throw error("expected '" + c + "'");
      }
      pos++;
    }

    Object value() {
      char c = peek();
      return switch (c) {
        case '{' -> object();
        case '[' -> array();
        case '"' -> string();
        case 't' -> literal("true", Boolean.TRUE);
        case 'f' -> literal("false", Boolean.FALSE);
        case 'n' -> literal("null", null);
        default -> number();
      };
    }

    Object literal(String word, Object value) {
      if (!text.startsWith(word, pos)) {
        throw error("expected " + word);
      }
      pos += word.length();
      return value;
    }

    Map<String, Object> object() {
      expect('{');
      Map<String, Object> map = new LinkedHashMap<>();
      space();
      if (peek() == '}') {
        pos++;
        return map;
      }
      do {
        space();
        String key = string();
        space();
        expect(':');
        space();
        if (map.put(key, value()) != null) {
          throw error("duplicate key " + key);
        }
      } while (comma());
      expect('}');
      return map;
    }

    List<Object> array() {
      expect('[');
      List<Object> list = new ArrayList<>();
      space();
      if (peek() == ']') {
        pos++;
        return list;
      }
      do {
        space();
        list.add(value());
      } while (comma());
      expect(']');
      return list;
    }

    String string() {
      expect('"');
      StringBuilder out = new StringBuilder();
      while (true) {
        char c = peek();
        pos++;
        if (c == '"') {
          return out.toString();
        }
        if (c != '\\') {
          out.append(c);
          continue;
        }
        char escaped = peek();
        pos++;
        switch (escaped) {
          case '"', '\\', '/' -> out.append(escaped);
          case 'b' -> out.append('\b');
          case 'f' -> out.append('\f');
          case 'n' -> out.append('\n');
          case 'r' -> out.append('\r');
          case 't' -> out.append('\t');
          case 'u' -> {
            if (pos + 4 > text.length()) {
              throw error("short unicode escape");
            }
            try {
              out.append((char) Integer.parseInt(text.substring(pos, pos + 4), 16));
            } catch (NumberFormatException e) {
              throw error("bad unicode escape", e);
            }
            pos += 4;
          }
          default -> throw error("bad escape");
        }
      }
    }

    Object number() {
      int start = pos;
      while (pos < text.length() && "+-0123456789.eE".indexOf(text.charAt(pos)) >= 0) {
        pos++;
      }
      String literal = text.substring(start, pos);
      if (literal.isEmpty()) {
        throw error("unexpected character");
      }
      try {
        if (literal.matches("-?(0|[1-9][0-9]*)")) {
          return Long.parseLong(literal);
        }
        if (!literal.matches("-?(0|[1-9][0-9]*)(\\.[0-9]+)?([eE][+-]?[0-9]+)?")) {
          throw error("bad number " + literal);
        }
        return Double.parseDouble(literal);
      } catch (NumberFormatException e) {
        throw error("bad number " + literal, e);
      }
    }
  }
}
