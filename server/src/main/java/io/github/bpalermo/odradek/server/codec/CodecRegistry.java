package io.github.bpalermo.odradek.server.codec;

import com.google.protobuf.Descriptors.Descriptor;
import io.github.bpalermo.odradek.config.v1.Codec;
import io.github.bpalermo.odradek.config.v1.Consumer;
import io.github.bpalermo.odradek.config.v1.Pipeline;
import io.github.bpalermo.odradek.config.v1.Producer;
import io.github.bpalermo.odradek.config.v1.ServerConfig;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The codecs of one configuration, resolved per topic.
 *
 * <p>A topic uses the codec its entry names, otherwise {@code defaultCodec}, otherwise the built-in
 * {@value #PASSTHROUGH}, which stores bytes unchanged. Building a registry instantiates and
 * configures every class the configuration uses, in both directions, so a configuration that builds
 * can both produce and consume.
 *
 * <p>The configuration must already have passed schema validation. A reload builds a new registry
 * and closes the old one once nothing uses it.
 */
public final class CodecRegistry implements Closeable {
  /** Name of the built-in codec that stores bytes unchanged. */
  public static final String PASSTHROUGH = "passthrough";

  private record Sides(SidePipeline key, SidePipeline value) {}

  private record ConsumerBinding(String codec, Descriptor keyType, Descriptor valueType) {}

  private final Map<String, Codec> codecs;
  private final ClassLoader loader;
  private final List<SidePipeline> shared = new ArrayList<>();
  private final Map<String, RecordEncoder> encoders = new HashMap<>();
  private final Map<String, ConsumerBinding> consumers = new HashMap<>();

  private CodecRegistry(Map<String, Codec> codecs, ClassLoader loader) {
    this.codecs = codecs;
    this.loader = loader;
  }

  /**
   * Builds the codecs of {@code config}.
   *
   * @param contracts the types named by {@code keyType} and {@code valueType}
   * @param loader finds serializer, deserializer and transform classes
   * @throws CodecConfigException if a class cannot be used, a contract type is unknown, or a
   *     serializer that takes a protobuf message is used for a topic without a contract type
   */
  public static CodecRegistry create(
      ServerConfig config, ContractRegistry contracts, ClassLoader loader) {
    CodecRegistry registry = new CodecRegistry(config.getCodecsMap(), loader);
    try {
      registry.bind(config, contracts);
      return registry;
    } catch (RuntimeException e) {
      registry.close();
      throw e;
    }
  }

  private void bind(ServerConfig config, ContractRegistry contracts) {
    Map<String, Sides> encoding = new HashMap<>();
    for (Map.Entry<String, Producer> entry : config.getProducersMap().entrySet()) {
      String topic = entry.getKey();
      Producer producer = entry.getValue();
      String codec = resolve(config, producer.hasCodec() ? producer.getCodec() : "");
      Descriptor keyType = type(contracts, producer.hasKeyType(), producer.getKeyType());
      Descriptor valueType = type(contracts, producer.hasValueType(), producer.getValueType());
      Sides sides = encoding.computeIfAbsent(codec, name -> open(name, true));
      requireType(topic, codec, "key", sides.key(), keyType);
      requireType(topic, codec, "value", sides.value(), valueType);
      encoders.put(topic, new RecordEncoder(topic, sides.key(), keyType, sides.value(), valueType));
    }
    encoding.values().forEach(this::share);

    // Consuming instances are per stream; one set is created here only to prove the classes load
    // and accept their configuration.
    Map<String, Sides> probed = new HashMap<>();
    try {
      for (Map.Entry<String, Consumer> entry : config.getConsumersMap().entrySet()) {
        Consumer consumer = entry.getValue();
        String codec = resolve(config, consumer.hasCodec() ? consumer.getCodec() : "");
        probed.computeIfAbsent(codec, name -> open(name, false));
        consumers.put(
            entry.getKey(),
            new ConsumerBinding(
                codec,
                type(contracts, consumer.hasKeyType(), consumer.getKeyType()),
                type(contracts, consumer.hasValueType(), consumer.getValueType())));
      }
    } finally {
      for (Sides sides : probed.values()) {
        sides.key().close();
        sides.value().close();
      }
    }
  }

  private static String resolve(ServerConfig config, String topicCodec) {
    if (!topicCodec.isEmpty()) {
      return topicCodec;
    }
    return config.hasDefaultCodec() ? config.getDefaultCodec() : PASSTHROUGH;
  }

  private static Descriptor type(ContractRegistry contracts, boolean set, String name) {
    return set ? contracts.find(name) : null;
  }

  private static void requireType(
      String topic, String codec, String side, SidePipeline pipeline, Descriptor type) {
    if (type == null && pipeline.requiresContractType()) {
      throw new CodecConfigException(
          "topic "
              + topic
              + ": the "
              + side
              + " serializer of codec "
              + codec
              + " takes a protobuf message, but the topic declares no "
              + side
              + "Type");
    }
  }

  private Sides open(String name, boolean encoding) {
    if (name.equals(PASSTHROUGH)) {
      return new Sides(
          SidePipeline.passthrough(PASSTHROUGH + " key"),
          SidePipeline.passthrough(PASSTHROUGH + " value"));
    }
    Codec codec = codecs.get(name);
    if (codec == null) {
      throw new CodecConfigException("codec " + name + " is not defined");
    }
    SidePipeline key = side(name, "key", codec.hasKey(), codec.getKey(), true, encoding);
    try {
      return new Sides(
          key, side(name, "value", codec.hasValue(), codec.getValue(), false, encoding));
    } catch (RuntimeException e) {
      key.close();
      throw e;
    }
  }

  private SidePipeline side(
      String codec, String side, boolean set, Pipeline config, boolean isKey, boolean encoding) {
    String description = "codec " + codec + " " + side;
    return set
        ? SidePipeline.create(description, config, isKey, encoding, loader)
        : SidePipeline.passthrough(description);
  }

  private void share(Sides sides) {
    shared.add(sides.key());
    shared.add(sides.value());
  }

  /**
   * Returns the encoder of a topic under {@code producers}, or {@code null} if the topic has no
   * entry there. The encoder is shared and stays valid until this registry is closed.
   */
  public RecordEncoder encoder(String topic) {
    return encoders.get(topic);
  }

  /**
   * Creates a decoder for a topic under {@code consumers}, or returns {@code null} if the topic has
   * no entry there. Each call creates new deserializer and transform instances; the caller closes
   * the decoder.
   *
   * @throws CodecConfigException if a class that configured when this registry was built no longer
   *     does
   */
  public RecordDecoder newDecoder(String topic) {
    ConsumerBinding binding = consumers.get(topic);
    if (binding == null) {
      return null;
    }
    Sides sides = open(binding.codec(), false);
    return new RecordDecoder(
        topic, sides.key(), binding.keyType(), sides.value(), binding.valueType());
  }

  /** Closes the shared producing instances. Decoders are closed by their owners. */
  @Override
  public void close() {
    shared.forEach(SidePipeline::close);
    shared.clear();
    encoders.clear();
    consumers.clear();
  }
}
