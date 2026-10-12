package io.github.bpalermo.odradek.server.codec;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import io.github.bpalermo.odradek.extension.RecordTransform;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

/** Extension classes the codec tests name in configuration. */
final class TestExtensions {
  private TestExtensions() {}

  private static byte[] utf8(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  /** Stores a Timestamp message as "type:seconds" text. */
  public static final class TextSerializer implements Serializer<Message> {
    static final AtomicInteger INSTANCES = new AtomicInteger();
    static final AtomicInteger CLOSED = new AtomicInteger();

    public TextSerializer() {
      INSTANCES.incrementAndGet();
    }

    @Override
    public byte[] serialize(String topic, Message data) {
      Object seconds = data.getField(data.getDescriptorForType().findFieldByName("seconds"));
      return utf8(data.getDescriptorForType().getFullName() + ":" + seconds);
    }

    @Override
    public void close() {
      CLOSED.incrementAndGet();
    }
  }

  /** Reads what {@link TextSerializer} wrote. */
  public static final class TextDeserializer implements Deserializer<Message> {
    static final AtomicInteger INSTANCES = new AtomicInteger();

    public TextDeserializer() {
      INSTANCES.incrementAndGet();
    }

    @Override
    public Message deserialize(String topic, byte[] data) {
      String text = new String(data, StandardCharsets.UTF_8);
      return Timestamp.newBuilder()
          .setSeconds(Long.parseLong(text.substring(text.indexOf(':') + 1)))
          .build();
    }
  }

  /** Declares byte[], so it is handed the app's bytes even when a contract type is set. */
  public static final class ReversingSerializer implements Serializer<byte[]> {
    @Override
    public byte[] serialize(String topic, byte[] data) {
      byte[] out = new byte[data.length];
      for (int i = 0; i < data.length; i++) {
        out[i] = data[data.length - 1 - i];
      }
      return out;
    }
  }

  /** Undoes {@link ReversingSerializer}. */
  public static final class ReversingDeserializer implements Deserializer<byte[]> {
    @Override
    public byte[] deserialize(String topic, byte[] data) {
      return new ReversingSerializer().serialize(topic, data);
    }
  }

  /** Returns a message of a type other than the contract. */
  public static final class WrongTypeDeserializer implements Deserializer<Message> {
    @Override
    public Message deserialize(String topic, byte[] data) {
      try {
        return com.google.protobuf.Duration.parseFrom(new byte[0]);
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  /** Appends its configured "tag" on produce, strips it on consume, and records a header. */
  public static final class TagTransform implements RecordTransform {
    private byte[] tag;
    private boolean isKey;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
      Object configured = configs.get("tag");
      if (configured == null) {
        throw new IllegalArgumentException("tag is required");
      }
      this.tag = utf8(configured.toString());
      this.isKey = isKey;
    }

    @Override
    public byte[] apply(String topic, Headers headers, byte[] data) {
      headers.add(isKey ? "key-tag" : "value-tag", tag);
      byte[] out = Arrays.copyOf(data, data.length + tag.length);
      System.arraycopy(tag, 0, out, data.length, tag.length);
      return out;
    }

    @Override
    public byte[] reverse(String topic, Headers headers, byte[] data) {
      int length = data.length - tag.length;
      if (length < 0 || !Arrays.equals(tag, Arrays.copyOfRange(data, length, data.length))) {
        throw new IllegalArgumentException("data does not end with the tag");
      }
      return Arrays.copyOf(data, length);
    }
  }

  /** Has no public no-argument constructor. */
  public static final class NoDefaultConstructorTransform implements RecordTransform {
    public NoDefaultConstructorTransform(String unused) {}

    @Override
    public byte[] apply(String topic, Headers headers, byte[] data) {
      return data;
    }

    @Override
    public byte[] reverse(String topic, Headers headers, byte[] data) {
      return data;
    }
  }
}
