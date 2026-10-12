package io.github.bpalermo.odradek.server;

import static org.assertj.core.api.Assertions.assertThat;

import build.buf.validate.ValidateProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import io.github.bpalermo.odradek.config.v1.ConfigProto;
import io.github.bpalermo.odradek.kafka.v1.OdradekProto;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Checks the reachability metadata of the native image (ADR 5) against the code, so that it cannot
 * drift: the JVM does not need it, so nothing else would notice.
 *
 * <p>Protobuf's generated messages find their accessors by name through reflection. The metadata
 * must therefore list every generated message the server can parse or print. A field added to the
 * configuration schema, or a message to the API, fails here until it is registered.
 */
class NativeMetadataTest {
  private static final Path METADATA = Path.of("server/native-image/reachability-metadata.json");

  /** Types whose public methods are registered, and every type named at all. */
  private static final Set<String> PUBLIC_METHODS = new HashSet<>();

  private static final Set<String> TYPES = new HashSet<>();

  static {
    try {
      Struct.Builder metadata = Struct.newBuilder();
      JsonFormat.parser().merge(Files.readString(METADATA), metadata);
      for (Value entry : metadata.getFieldsOrThrow("reflection").getListValue().getValuesList()) {
        Struct fields = entry.getStructValue();
        String type = fields.getFieldsOrThrow("type").getStringValue();
        TYPES.add(type);
        if (fields.containsFields("allPublicMethods")
            && fields.getFieldsOrThrow("allPublicMethods").getBoolValue()) {
          PUBLIC_METHODS.add(type);
        }
      }
    } catch (IOException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static void collect(Descriptor message, String prefix, List<String> into) {
    if (message.getOptions().getMapEntry()) {
      return;
    }
    String name = prefix + message.getName();
    into.add(name);
    into.add(name + "$Builder");
    for (Descriptor nested : message.getNestedTypes()) {
      collect(nested, name + "$", into);
    }
  }

  private static List<String> messages(FileDescriptor file) {
    String prefix = file.getOptions().getJavaPackage() + ".";
    List<String> classes = new ArrayList<>();
    for (Descriptor message : file.getMessageTypes()) {
      collect(message, prefix, classes);
    }
    return classes;
  }

  @Test
  void everyConfigurationMessageIsRegistered() {
    assertThat(PUBLIC_METHODS).containsAll(messages(ConfigProto.getDescriptor()));
  }

  @Test
  void everyApiMessageIsRegistered() {
    assertThat(PUBLIC_METHODS).containsAll(messages(OdradekProto.getDescriptor()));
  }

  @Test
  void everyValidationRuleMessageIsRegistered() {
    // protovalidate reads the buf.validate options of the configuration through these.
    assertThat(PUBLIC_METHODS).containsAll(messages(ValidateProto.getDescriptor()));
  }

  @Test
  void wellKnownTypesOfTheConfigurationAreRegistered() {
    assertThat(PUBLIC_METHODS)
        .contains("com.google.protobuf.Duration", "com.google.protobuf.Timestamp");
  }

  @Test
  void theCodecsKafkaShipsAreRegistered() {
    assertThat(TYPES)
        .contains(
            "org.apache.kafka.common.serialization.ByteArrayDeserializer",
            "org.apache.kafka.common.serialization.ByteArraySerializer",
            "org.apache.kafka.common.serialization.StringDeserializer",
            "org.apache.kafka.common.serialization.StringSerializer");
  }

  @Test
  void everyFirstPartyAndKafkaTypeNamedExists() throws ClassNotFoundException {
    for (String type : TYPES) {
      if (type.startsWith("io.github.bpalermo.")
          || type.startsWith("build.buf.")
          || type.startsWith("org.apache.kafka.")) {
        Class.forName(type, false, NativeMetadataTest.class.getClassLoader());
      }
    }
  }
}
