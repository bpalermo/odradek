package io.github.bpalermo.odradek.server.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Timestamp;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ContractRegistryTest {
  private static FileDescriptorProto file(String name, String type, String... imports) {
    return FileDescriptorProto.newBuilder()
        .setName(name)
        .setPackage("acme")
        .addAllDependency(List.of(imports))
        .addMessageType(
            DescriptorProto.newBuilder()
                .setName(type)
                .addNestedType(DescriptorProto.newBuilder().setName("Inner")))
        .build();
  }

  private static FileDescriptorSet set(FileDescriptorProto... files) {
    return FileDescriptorSet.newBuilder().addAllFile(List.of(files)).build();
  }

  @Test
  void findsTopLevelAndNestedTypes() {
    ContractRegistry registry = ContractRegistry.of(List.of(set(file("a.proto", "Order"))));
    assertThat(registry.find("acme.Order").getFullName()).isEqualTo("acme.Order");
    assertThat(registry.find("acme.Order.Inner").getFullName()).isEqualTo("acme.Order.Inner");
  }

  @Test
  void loadsDescriptorSetFiles(@TempDir Path dir) throws IOException {
    Path path = dir.resolve("contracts.binpb");
    Files.write(path, set(Timestamp.getDescriptor().getFile().toProto()).toByteArray());
    ContractRegistry registry = ContractRegistry.load(List.of(path));
    assertThat(registry.find("google.protobuf.Timestamp").findFieldByName("seconds")).isNotNull();
  }

  @Test
  void resolvesImportsAcrossSetsInAnyOrder() {
    ContractRegistry registry =
        ContractRegistry.of(
            List.of(set(file("b.proto", "Line", "a.proto")), set(file("a.proto", "Order"))));
    assertThat(registry.find("acme.Line").getFile().getDependencies()).hasSize(1);
  }

  @Test
  void sameFileInTwoSetsIsAccepted() {
    FileDescriptorProto a = file("a.proto", "Order");
    assertThat(ContractRegistry.of(List.of(set(a), set(a))).find("acme.Order")).isNotNull();
  }

  @Test
  void unknownTypeIsRejected() {
    assertThatThrownBy(() -> ContractRegistry.empty().find("acme.Order"))
        .isInstanceOf(CodecConfigException.class)
        .hasMessageContaining("acme.Order");
  }

  @Test
  void missingImportIsRejected() {
    assertThatThrownBy(() -> ContractRegistry.of(List.of(set(file("b.proto", "Line", "a.proto")))))
        .isInstanceOf(CodecConfigException.class)
        .hasMessageContaining("a.proto");
  }

  @Test
  void importCycleIsRejected() {
    assertThatThrownBy(
            () ->
                ContractRegistry.of(
                    List.of(
                        set(
                            file("a.proto", "Order", "b.proto"),
                            file("b.proto", "Line", "a.proto")))))
        .isInstanceOf(CodecConfigException.class)
        .hasMessageContaining("cycle");
  }

  @Test
  void differentFilesUnderOneNameAreRejected() {
    assertThatThrownBy(
            () ->
                ContractRegistry.of(
                    List.of(set(file("a.proto", "Order")), set(file("a.proto", "Other")))))
        .isInstanceOf(CodecConfigException.class)
        .hasMessageContaining("a.proto");
  }

  @Test
  void typeDefinedTwiceIsRejected() {
    assertThatThrownBy(
            () ->
                ContractRegistry.of(
                    List.of(set(file("a.proto", "Order"), file("b.proto", "Order")))))
        .isInstanceOf(CodecConfigException.class);
  }
}
