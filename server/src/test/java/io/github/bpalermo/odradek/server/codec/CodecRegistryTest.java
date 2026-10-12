package io.github.bpalermo.odradek.server.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Timestamp;
import io.github.bpalermo.odradek.config.v1.Codec;
import io.github.bpalermo.odradek.config.v1.Consumer;
import io.github.bpalermo.odradek.config.v1.Pipeline;
import io.github.bpalermo.odradek.config.v1.ProduceTarget;
import io.github.bpalermo.odradek.config.v1.Producer;
import io.github.bpalermo.odradek.config.v1.ServerConfig;
import io.github.bpalermo.odradek.config.v1.Transform;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

class CodecRegistryTest {
  private static final String TIMESTAMP = "google.protobuf.Timestamp";
  private static final ContractRegistry CONTRACTS =
      ContractRegistry.of(
          List.of(
              FileDescriptorSet.newBuilder()
                  .addFile(Timestamp.getDescriptor().getFile().toProto())
                  .build()));
  private static final ClassLoader LOADER = CodecRegistryTest.class.getClassLoader();
  private static final byte[] APP = Timestamp.newBuilder().setSeconds(42).build().toByteArray();

  private static String name(Class<?> type) {
    return type.getName();
  }

  private static byte[] utf8(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  private static Pipeline.Builder text() {
    return Pipeline.newBuilder()
        .setSerializer(name(TestExtensions.TextSerializer.class))
        .setDeserializer(name(TestExtensions.TextDeserializer.class));
  }

  private static Transform tag(String tag) {
    return Transform.newBuilder()
        .setClassName(name(TestExtensions.TagTransform.class))
        .putProperties("tag", tag)
        .build();
  }

  private static Codec value(Pipeline.Builder pipeline) {
    return Codec.newBuilder().setValue(pipeline).build();
  }

  private static Producer.Builder producer() {
    return Producer.newBuilder().addClusters(ProduceTarget.newBuilder().setCluster("main"));
  }

  private static Consumer.Builder consumer() {
    return Consumer.newBuilder().setCluster("main").setGroupId("group");
  }

  private static CodecRegistry registry(ServerConfig.Builder config) {
    return CodecRegistry.create(config.build(), CONTRACTS, LOADER);
  }

  @Test
  void withoutCodecsBytesPassThrough() throws Exception {
    try (CodecRegistry registry =
            registry(
                ServerConfig.newBuilder()
                    .putProducers("orders", producer().build())
                    .putConsumers("orders", consumer().build()));
        RecordDecoder decoder = registry.newDecoder("orders")) {
      Headers headers = new RecordHeaders();
      assertThat(registry.encoder("orders").encodeValue(headers, utf8("v"))).isEqualTo(utf8("v"));
      assertThat(registry.encoder("orders").encodeKey(headers, utf8("k"))).isEqualTo(utf8("k"));
      assertThat(decoder.decodeValue(headers, utf8("v"))).isEqualTo(utf8("v"));
      assertThat(headers).isEmpty();
    }
  }

  @Test
  void unconfiguredTopicHasNoCodec() {
    try (CodecRegistry registry = registry(ServerConfig.newBuilder())) {
      assertThat(registry.encoder("orders")).isNull();
      assertThat(registry.newDecoder("orders")).isNull();
    }
  }

  @Test
  void defaultCodecAppliesAndTopicsOverrideIt() throws Exception {
    try (CodecRegistry registry =
        registry(
            ServerConfig.newBuilder()
                .setDefaultCodec("text")
                .putCodecs("text", value(text()))
                .putCodecs("tagged", value(Pipeline.newBuilder().addTransforms(tag("#"))))
                .putProducers("defaulted", producer().setValueType(TIMESTAMP).build())
                .putProducers("overridden", producer().setCodec("tagged").build())
                .putProducers("opted-out", producer().setCodec("passthrough").build()))) {
      Headers headers = new RecordHeaders();
      assertThat(registry.encoder("defaulted").encodeValue(headers, APP))
          .isEqualTo(utf8(TIMESTAMP + ":42"));
      assertThat(registry.encoder("overridden").encodeValue(headers, APP))
          .isEqualTo(utf8(new String(APP, StandardCharsets.UTF_8) + "#"));
      assertThat(registry.encoder("opted-out").encodeValue(headers, APP)).isEqualTo(APP);
    }
  }

  @Test
  void serializerSeesTheContractTypeAndRoundTrips() throws Exception {
    try (CodecRegistry registry =
            registry(
                ServerConfig.newBuilder()
                    .putCodecs("text", value(text()))
                    .putProducers(
                        "orders", producer().setCodec("text").setValueType(TIMESTAMP).build())
                    .putConsumers(
                        "orders", consumer().setCodec("text").setValueType(TIMESTAMP).build()));
        RecordDecoder decoder = registry.newDecoder("orders")) {
      Headers headers = new RecordHeaders();
      byte[] stored = registry.encoder("orders").encodeValue(headers, APP);
      assertThat(stored).isEqualTo(utf8(TIMESTAMP + ":42"));
      assertThat(decoder.decodeValue(headers, stored)).isEqualTo(APP);
    }
  }

  @Test
  void serializerDeclaringBytesGetsTheAppBytes() throws Exception {
    Pipeline.Builder reversing =
        Pipeline.newBuilder()
            .setSerializer(name(TestExtensions.ReversingSerializer.class))
            .setDeserializer(name(TestExtensions.ReversingDeserializer.class));
    try (CodecRegistry registry =
            registry(
                ServerConfig.newBuilder()
                    .putCodecs("reversed", value(reversing))
                    .putProducers(
                        "orders", producer().setCodec("reversed").setValueType(TIMESTAMP).build())
                    .putConsumers("orders", consumer().setCodec("reversed").build()));
        RecordDecoder decoder = registry.newDecoder("orders")) {
      Headers headers = new RecordHeaders();
      byte[] stored = registry.encoder("orders").encodeValue(headers, utf8("abc"));
      assertThat(stored).isEqualTo(utf8("cba"));
      assertThat(decoder.decodeValue(headers, stored)).isEqualTo(utf8("abc"));
    }
  }

  @Test
  void transformsRunInOrderOnProduceAndInReverseOnConsume() throws Exception {
    Codec codec =
        Codec.newBuilder()
            .setKey(Pipeline.newBuilder().addTransforms(tag("K")))
            .setValue(Pipeline.newBuilder().addTransforms(tag("1")).addTransforms(tag("2")))
            .build();
    try (CodecRegistry registry =
            registry(
                ServerConfig.newBuilder()
                    .setDefaultCodec("tagged")
                    .putCodecs("tagged", codec)
                    .putProducers("orders", producer().build())
                    .putConsumers("orders", consumer().build()));
        RecordDecoder decoder = registry.newDecoder("orders")) {
      Headers headers = new RecordHeaders();
      byte[] key = registry.encoder("orders").encodeKey(headers, utf8("k"));
      byte[] stored = registry.encoder("orders").encodeValue(headers, utf8("v"));
      assertThat(key).isEqualTo(utf8("kK"));
      assertThat(stored).isEqualTo(utf8("v12"));
      assertThat(headers.headers("value-tag")).hasSize(2);
      assertThat(headers.lastHeader("key-tag").value()).isEqualTo(utf8("K"));
      assertThat(decoder.decodeKey(headers, key)).isEqualTo(utf8("k"));
      assertThat(decoder.decodeValue(headers, stored)).isEqualTo(utf8("v"));
    }
  }

  @Test
  void nullKeysAndTombstonesBypassThePipeline() throws Exception {
    try (CodecRegistry registry =
            registry(
                ServerConfig.newBuilder()
                    .setDefaultCodec("text")
                    .putCodecs("text", value(text().addTransforms(tag("#"))))
                    .putProducers("orders", producer().setValueType(TIMESTAMP).build())
                    .putConsumers("orders", consumer().setValueType(TIMESTAMP).build()));
        RecordDecoder decoder = registry.newDecoder("orders")) {
      Headers headers = new RecordHeaders();
      assertThat(registry.encoder("orders").encodeValue(headers, null)).isNull();
      assertThat(registry.encoder("orders").encodeKey(headers, null)).isNull();
      assertThat(decoder.decodeValue(headers, null)).isNull();
      assertThat(headers).isEmpty();
    }
  }

  @Test
  void payloadThatIsNotTheContractTypeFailsTheRecord() {
    try (CodecRegistry registry =
        registry(
            ServerConfig.newBuilder()
                .putCodecs("text", value(text()))
                .putProducers(
                    "orders", producer().setCodec("text").setValueType(TIMESTAMP).build()))) {
      // 0x08 opens varint field 1 and then the payload ends.
      assertThatThrownBy(
              () -> registry.encoder("orders").encodeValue(new RecordHeaders(), new byte[] {8}))
          .isInstanceOf(RecordEncodeException.class)
          .hasMessageContaining(TIMESTAMP);
    }
  }

  @Test
  void undecodableRecordIsReported() {
    try (CodecRegistry registry =
            registry(
                ServerConfig.newBuilder()
                    .putCodecs("tagged", value(Pipeline.newBuilder().addTransforms(tag("#"))))
                    .putConsumers("orders", consumer().setCodec("tagged").build()));
        RecordDecoder decoder = registry.newDecoder("orders")) {
      assertThatThrownBy(() -> decoder.decodeValue(new RecordHeaders(), utf8("untagged")))
          .isInstanceOf(RecordDecodeException.class)
          .hasMessageContaining("codec tagged value");
    }
  }

  @Test
  void deserializerReturningAnotherTypeIsReported() {
    Pipeline.Builder wrong =
        Pipeline.newBuilder()
            .setSerializer(name(TestExtensions.TextSerializer.class))
            .setDeserializer(name(TestExtensions.WrongTypeDeserializer.class));
    try (CodecRegistry registry =
            registry(
                ServerConfig.newBuilder()
                    .putCodecs("wrong", value(wrong))
                    .putConsumers(
                        "orders", consumer().setCodec("wrong").setValueType(TIMESTAMP).build()));
        RecordDecoder decoder = registry.newDecoder("orders")) {
      assertThatThrownBy(() -> decoder.decodeValue(new RecordHeaders(), utf8("x")))
          .isInstanceOf(RecordDecodeException.class)
          .hasMessageContaining("google.protobuf.Duration");
    }
  }

  @Test
  void producingInstancesAreSharedAndConsumingInstancesArePerDecoder() {
    int serializers = TestExtensions.TextSerializer.INSTANCES.get();
    int closed = TestExtensions.TextSerializer.CLOSED.get();
    CodecRegistry registry =
        registry(
            ServerConfig.newBuilder()
                .setDefaultCodec("text")
                .putCodecs("text", value(text()))
                .putProducers("a", producer().setValueType(TIMESTAMP).build())
                .putProducers("b", producer().setValueType(TIMESTAMP).build())
                .putConsumers("a", consumer().setValueType(TIMESTAMP).build()));
    assertThat(TestExtensions.TextSerializer.INSTANCES.get() - serializers).isEqualTo(1);

    int deserializers = TestExtensions.TextDeserializer.INSTANCES.get();
    registry.newDecoder("a").close();
    registry.newDecoder("a").close();
    assertThat(TestExtensions.TextDeserializer.INSTANCES.get() - deserializers).isEqualTo(2);

    registry.close();
    assertThat(TestExtensions.TextSerializer.CLOSED.get() - closed).isEqualTo(1);
  }

  @Test
  void messageSerializerNeedsAContractType() {
    assertThatThrownBy(
            () ->
                registry(
                    ServerConfig.newBuilder()
                        .putCodecs("text", value(text()))
                        .putProducers("orders", producer().setCodec("text").build())))
        .isInstanceOf(CodecConfigException.class)
        .hasMessageContaining("valueType");
  }

  @Test
  void unknownContractTypeIsRejected() {
    assertThatThrownBy(
            () ->
                registry(
                    ServerConfig.newBuilder()
                        .putConsumers("orders", consumer().setValueType("acme.Missing").build())))
        .isInstanceOf(CodecConfigException.class)
        .hasMessageContaining("acme.Missing");
  }

  @Test
  void classThatCannotBeLoadedIsRejectedEvenForTheUnusedDirection() {
    Pipeline.Builder pipeline =
        Pipeline.newBuilder()
            .setSerializer(name(TestExtensions.TextSerializer.class))
            .setDeserializer("com.example.Missing");
    assertThatThrownBy(
            () ->
                registry(
                    ServerConfig.newBuilder()
                        .putCodecs("text", value(pipeline))
                        .putProducers(
                            "orders", producer().setCodec("text").setValueType(TIMESTAMP).build())))
        .isInstanceOf(CodecConfigException.class)
        .hasMessageContaining("com.example.Missing");
  }

  @Test
  void classOfTheWrongKindIsRejected() {
    Pipeline.Builder pipeline =
        Pipeline.newBuilder()
            .addTransforms(
                Transform.newBuilder().setClassName(name(TestExtensions.TextSerializer.class)));
    assertThatThrownBy(
            () ->
                registry(
                    ServerConfig.newBuilder()
                        .putCodecs("bad", value(pipeline))
                        .putProducers("orders", producer().setCodec("bad").build())))
        .isInstanceOf(CodecConfigException.class)
        .hasMessageContaining("does not implement");
  }

  @Test
  void classWithoutNoArgumentConstructorIsRejected() {
    Pipeline.Builder pipeline =
        Pipeline.newBuilder()
            .addTransforms(
                Transform.newBuilder()
                    .setClassName(name(TestExtensions.NoDefaultConstructorTransform.class)));
    assertThatThrownBy(
            () ->
                registry(
                    ServerConfig.newBuilder()
                        .putCodecs("bad", value(pipeline))
                        .putConsumers("orders", consumer().setCodec("bad").build())))
        .isInstanceOf(CodecConfigException.class)
        .hasMessageContaining("no-argument constructor");
  }

  @Test
  void classRejectingItsConfigurationIsRejected() {
    Pipeline.Builder pipeline =
        Pipeline.newBuilder()
            .addTransforms(
                Transform.newBuilder().setClassName(name(TestExtensions.TagTransform.class)));
    assertThatThrownBy(
            () ->
                registry(
                    ServerConfig.newBuilder()
                        .putCodecs("bad", value(pipeline))
                        .putProducers("orders", producer().setCodec("bad").build())))
        .isInstanceOf(CodecConfigException.class)
        .hasMessageContaining("tag is required");
  }
}
