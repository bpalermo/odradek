package io.github.bpalermo.odradek.server.config;

import build.buf.protovalidate.ValidationResult;
import build.buf.protovalidate.Validator;
import build.buf.protovalidate.ValidatorFactory;
import build.buf.protovalidate.exceptions.ValidationException;
import build.buf.validate.FieldPathElement;
import build.buf.validate.Violation;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.github.bpalermo.odradek.config.v1.ServerConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.snakeyaml.engine.v2.api.Load;
import org.snakeyaml.engine.v2.api.LoadSettings;
import org.snakeyaml.engine.v2.exceptions.YamlEngineException;

/**
 * Reads the server configuration: YAML (or JSON, which is YAML) in the protobuf JSON mapping,
 * parsed strictly and validated against the rules declared in the schema.
 *
 * <p>Checks that need the outside world, such as referenced files existing, are not made here.
 */
public final class ConfigLoader {
  private static final Validator VALIDATOR = ValidatorFactory.newBuilder().build();

  private ConfigLoader() {}

  /**
   * Reads and validates a configuration file.
   *
   * @throws IOException if the file cannot be read
   * @throws ConfigException if the content is not a valid configuration
   */
  public static ServerConfig load(Path file) throws IOException, ConfigException {
    return parse(Files.readString(file, StandardCharsets.UTF_8));
  }

  /**
   * Parses and validates configuration text.
   *
   * @throws ConfigException if the text is not YAML, does not fit the schema (an unknown field is
   *     an error), or violates a validation rule
   */
  public static ServerConfig parse(String yaml) throws ConfigException {
    Object document;
    try {
      document = new Load(LoadSettings.builder().build()).loadFromString(yaml);
    } catch (YamlEngineException e) {
      throw new ConfigException("not valid YAML: " + e.getMessage(), e);
    }
    if (!(document instanceof Map<?, ?> root)) {
      throw new ConfigException(List.of("the configuration must be a mapping at the top level"));
    }
    StringBuilder json = new StringBuilder();
    writeMessage(json, root, ServerConfig.getDescriptor());

    ServerConfig.Builder builder = ServerConfig.newBuilder();
    try {
      JsonFormat.parser().merge(json.toString(), builder);
    } catch (InvalidProtocolBufferException e) {
      throw new ConfigException("does not fit the configuration schema: " + e.getMessage(), e);
    }
    ServerConfig config = builder.build();

    ValidationResult result;
    try {
      result = VALIDATOR.validate(config);
    } catch (ValidationException e) {
      throw new ConfigException("validation could not run: " + e.getMessage(), e);
    }
    if (!result.isSuccess()) {
      List<String> problems = new ArrayList<>();
      for (build.buf.protovalidate.Violation violation : result.getViolations()) {
        problems.add(describe(violation.toProto()));
      }
      throw new ConfigException(problems);
    }
    return config;
  }

  private static String describe(Violation violation) {
    StringBuilder path = new StringBuilder();
    for (FieldPathElement element : violation.getField().getElementsList()) {
      if (!path.isEmpty()) {
        path.append('.');
      }
      path.append(element.getFieldName());
      switch (element.getSubscriptCase()) {
        case INDEX -> path.append('[').append(element.getIndex()).append(']');
        case STRING_KEY -> path.append("[\"").append(element.getStringKey()).append("\"]");
        case INT_KEY -> path.append('[').append(element.getIntKey()).append(']');
        case UINT_KEY -> path.append('[').append(element.getUintKey()).append(']');
        case BOOL_KEY -> path.append('[').append(element.getBoolKey()).append(']');
        case SUBSCRIPT_NOT_SET -> {}
      }
    }
    String where = path.isEmpty() ? "(root)" : path.toString();
    return where + ": " + violation.getMessage() + " [" + violation.getRuleId() + "]";
  }

  /**
   * Writes a YAML mapping as the JSON of a message. The schema is consulted for one reason: Kafka
   * properties are strings in the schema, and YAML reads {@code acks: 1} or {@code
   * enable.idempotence: true} as a number or a boolean. Scalars bound for a string field are
   * written as strings, so such values need no quotes in the file. Everything else is written as
   * found, and the protobuf parser judges it.
   */
  private static void writeMessage(StringBuilder out, Map<?, ?> node, Descriptor type) {
    out.append('{');
    boolean first = true;
    for (Map.Entry<?, ?> entry : node.entrySet()) {
      if (!first) {
        out.append(',');
      }
      first = false;
      String name = String.valueOf(entry.getKey());
      writeString(out, name);
      out.append(':');
      FieldDescriptor field = type == null ? null : findField(type, name);
      if (field == null) {
        writeValue(out, entry.getValue(), null, false);
      } else if (field.isMapField()) {
        writeMap(out, entry.getValue(), field.getMessageType().findFieldByNumber(2));
      } else {
        writeValue(out, entry.getValue(), messageType(field), isString(field));
      }
    }
    out.append('}');
  }

  private static void writeMap(StringBuilder out, Object node, FieldDescriptor valueField) {
    if (!(node instanceof Map<?, ?> map)) {
      writeValue(out, node, null, false);
      return;
    }
    out.append('{');
    boolean first = true;
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      if (!first) {
        out.append(',');
      }
      first = false;
      writeString(out, String.valueOf(entry.getKey()));
      out.append(':');
      writeValue(out, entry.getValue(), messageType(valueField), isString(valueField));
    }
    out.append('}');
  }

  private static void writeValue(StringBuilder out, Object node, Descriptor type, boolean string) {
    switch (node) {
      case null -> out.append("null");
      case Map<?, ?> map -> writeMessage(out, map, type);
      case List<?> list -> {
        out.append('[');
        for (int i = 0; i < list.size(); i++) {
          if (i > 0) {
            out.append(',');
          }
          writeValue(out, list.get(i), type, string);
        }
        out.append(']');
      }
      case String s -> writeString(out, s);
      case Number n when !string -> out.append(n);
      case Boolean b when !string -> out.append(b);
      default -> writeString(out, String.valueOf(node));
    }
  }

  private static FieldDescriptor findField(Descriptor type, String name) {
    for (FieldDescriptor field : type.getFields()) {
      if (field.getJsonName().equals(name) || field.getName().equals(name)) {
        return field;
      }
    }
    return null;
  }

  private static Descriptor messageType(FieldDescriptor field) {
    return field.getJavaType() == FieldDescriptor.JavaType.MESSAGE ? field.getMessageType() : null;
  }

  private static boolean isString(FieldDescriptor field) {
    return field.getJavaType() == FieldDescriptor.JavaType.STRING;
  }

  private static void writeString(StringBuilder out, String value) {
    out.append('"');
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      switch (c) {
        case '"' -> out.append("\\\"");
        case '\\' -> out.append("\\\\");
        case '\n' -> out.append("\\n");
        case '\r' -> out.append("\\r");
        case '\t' -> out.append("\\t");
        default -> {
          if (c < 0x20) {
            out.append(String.format("\\u%04x", (int) c));
          } else {
            out.append(c);
          }
        }
      }
    }
    out.append('"');
  }
}
