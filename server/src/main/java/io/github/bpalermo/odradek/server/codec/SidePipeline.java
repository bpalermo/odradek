package io.github.bpalermo.odradek.server.codec;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.github.bpalermo.odradek.config.v1.Pipeline;
import io.github.bpalermo.odradek.config.v1.Transform;
import io.github.bpalermo.odradek.extension.RecordTransform;
import java.io.Closeable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One side (key or value) of one codec: the configured instances of its serializer or deserializer
 * and of its transforms.
 *
 * <p>An instance serves one direction. The contract type is passed per call, so an encoding
 * instance is shared by every topic that uses the codec.
 */
final class SidePipeline implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(SidePipeline.class);

  /** What a serializer class declares as its input, as far as reflection can tell. */
  enum Input {
    BYTES,
    MESSAGE,
    UNKNOWN
  }

  private final String description;
  private final Serializer<Object> serializer;
  private final Input serializerInput;
  private final Deserializer<?> deserializer;
  private final List<RecordTransform> transforms;

  private SidePipeline(
      String description,
      Serializer<Object> serializer,
      Input serializerInput,
      Deserializer<?> deserializer,
      List<RecordTransform> transforms) {
    this.description = description;
    this.serializer = serializer;
    this.serializerInput = serializerInput;
    this.deserializer = deserializer;
    this.transforms = transforms;
  }

  /** A pipeline that stores bytes unchanged. */
  static SidePipeline passthrough(String description) {
    return new SidePipeline(description, null, Input.BYTES, null, List.of());
  }

  /**
   * Instantiates and configures the classes of one direction of {@code config}.
   *
   * @param description names the codec and side in error messages
   * @param encoding whether the instances are for producing (serializer) or consuming
   *     (deserializer)
   * @throws CodecConfigException if a class cannot be loaded, has the wrong type, cannot be
   *     instantiated or rejects its configuration
   */
  static SidePipeline create(
      String description, Pipeline config, boolean isKey, boolean encoding, ClassLoader loader) {
    List<Closeable> created = new ArrayList<>();
    try {
      Serializer<Object> serializer = null;
      Input input = Input.BYTES;
      Deserializer<?> deserializer = null;
      if (config.hasSerializer()) {
        // Both classes are checked in both directions: a configuration that can only produce, or
        // only consume, is a mistake to report now rather than when the other side first runs.
        Class<?> serializerClass = load(config.getSerializer(), Serializer.class, loader);
        Class<?> deserializerClass = load(config.getDeserializer(), Deserializer.class, loader);
        if (encoding) {
          @SuppressWarnings("unchecked")
          Serializer<Object> s = (Serializer<Object>) instantiate(serializerClass);
          created.add(s);
          s.configure(config.getPropertiesMap(), isKey);
          serializer = s;
          input = declaredInput(serializerClass);
        } else {
          Deserializer<?> d = (Deserializer<?>) instantiate(deserializerClass);
          created.add(d);
          d.configure(config.getPropertiesMap(), isKey);
          deserializer = d;
        }
      }
      List<RecordTransform> transforms = new ArrayList<>();
      for (Transform transform : config.getTransformsList()) {
        RecordTransform t =
            (RecordTransform)
                instantiate(load(transform.getClassName(), RecordTransform.class, loader));
        created.add(t);
        t.configure(transform.getPropertiesMap(), isKey);
        transforms.add(t);
      }
      return new SidePipeline(
          description, serializer, input, deserializer, List.copyOf(transforms));
    } catch (RuntimeException e) {
      created.forEach(SidePipeline::closeQuietly);
      if (e instanceof CodecConfigException) {
        throw e;
      }
      throw new CodecConfigException(description + ": configure failed: " + e, e);
    }
  }

  private static Class<?> load(String name, Class<?> expected, ClassLoader loader) {
    Class<?> type;
    try {
      type = Class.forName(name, true, loader);
    } catch (ClassNotFoundException | LinkageError e) {
      throw new CodecConfigException("class " + name + " cannot be loaded", e);
    }
    if (!expected.isAssignableFrom(type)) {
      throw new CodecConfigException("class " + name + " does not implement " + expected.getName());
    }
    return type;
  }

  private static Object instantiate(Class<?> type) {
    try {
      return type.getDeclaredConstructor().newInstance();
    } catch (ReflectiveOperationException e) {
      throw new CodecConfigException(
          "class " + type.getName() + " needs a public no-argument constructor", e);
    }
  }

  /**
   * Reads the type argument a class gives to {@link Serializer}. Only a direct {@code implements
   * Serializer<X>} somewhere in the hierarchy is understood; anything else is {@link
   * Input#UNKNOWN}.
   */
  static Input declaredInput(Class<?> serializerClass) {
    for (Class<?> c = serializerClass; c != null; c = c.getSuperclass()) {
      for (Type implemented : c.getGenericInterfaces()) {
        if (implemented instanceof ParameterizedType p
            && p.getRawType() == Serializer.class
            && p.getActualTypeArguments()[0] instanceof Class<?> argument) {
          if (argument == byte[].class) {
            return Input.BYTES;
          }
          if (Message.class.isAssignableFrom(argument)) {
            return Input.MESSAGE;
          }
        }
      }
    }
    return Input.UNKNOWN;
  }

  /** Whether this pipeline's serializer declares that it takes a protobuf message. */
  boolean requiresContractType() {
    return serializer != null && serializerInput == Input.MESSAGE;
  }

  /**
   * Turns what the app sent into what is stored.
   *
   * @param type the topic's contract type for this side, or {@code null} for opaque bytes
   * @param data what the app sent; {@code null} is returned as {@code null}
   */
  byte[] encode(String topic, Headers headers, Descriptor type, byte[] data)
      throws RecordEncodeException {
    if (data == null) {
      return null;
    }
    try {
      byte[] out = data;
      if (serializer != null) {
        Object input =
            type != null && serializerInput != Input.BYTES
                ? DynamicMessage.parseFrom(type, data)
                : data;
        out = serializer.serialize(topic, headers, input);
        if (out == null) {
          throw new IllegalStateException("serializer returned null");
        }
      }
      for (RecordTransform transform : transforms) {
        out = transform.apply(topic, headers, out);
        if (out == null) {
          throw new IllegalStateException(transform.getClass().getName() + " returned null");
        }
      }
      return out;
    } catch (InvalidProtocolBufferException e) {
      throw new RecordEncodeException(
          description + ": payload is not a valid " + type.getFullName(), e);
    } catch (RuntimeException e) {
      throw new RecordEncodeException(description + ": " + e, e);
    }
  }

  /**
   * Turns what is stored into what the app expects.
   *
   * @param headers the record's headers. Transforms may change them before a later step fails, so a
   *     caller that needs the stored headers afterwards must pass a copy.
   * @param type the topic's contract type for this side, or {@code null} for opaque bytes
   * @param data what Kafka returned; {@code null} is returned as {@code null}
   */
  byte[] decode(String topic, Headers headers, Descriptor type, byte[] data)
      throws RecordDecodeException {
    if (data == null) {
      return null;
    }
    try {
      byte[] in = data;
      for (RecordTransform transform : transforms.reversed()) {
        in = transform.reverse(topic, headers, in);
        if (in == null) {
          throw new IllegalStateException(transform.getClass().getName() + " returned null");
        }
      }
      if (deserializer == null) {
        return in;
      }
      Object value = deserializer.deserialize(topic, headers, in);
      if (value instanceof byte[] bytes) {
        return bytes;
      }
      if (value instanceof Message message) {
        String actual = message.getDescriptorForType().getFullName();
        if (type != null && !type.getFullName().equals(actual)) {
          throw new IllegalStateException(
              "deserializer returned " + actual + ", the contract is " + type.getFullName());
        }
        return message.toByteArray();
      }
      throw new IllegalStateException(
          "deserializer returned "
              + (value == null ? "null" : value.getClass().getName())
              + ", expected a protobuf Message or byte[]");
    } catch (RuntimeException e) {
      throw new RecordDecodeException(description + ": " + e, e);
    }
  }

  @Override
  public void close() {
    if (serializer != null) {
      closeQuietly(serializer);
    }
    if (deserializer != null) {
      closeQuietly(deserializer);
    }
    transforms.forEach(SidePipeline::closeQuietly);
  }

  private static void closeQuietly(Closeable closeable) {
    try {
      closeable.close();
    } catch (Exception e) {
      LOG.warn("closing {} failed", closeable.getClass().getName(), e);
    }
  }
}
