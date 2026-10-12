package io.github.bpalermo.odradek.server.codec;

import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.ExtensionRegistryLite;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The protobuf message types apps exchange with the server, read from descriptor sets.
 *
 * <p>Immutable. A reload builds a new registry.
 */
public final class ContractRegistry {
  private static final ContractRegistry EMPTY = new ContractRegistry(Map.of());

  private final Map<String, Descriptor> types;

  private ContractRegistry(Map<String, Descriptor> types) {
    this.types = types;
  }

  /** A registry without any type. */
  public static ContractRegistry empty() {
    return EMPTY;
  }

  /**
   * Reads serialized {@code FileDescriptorSet} files.
   *
   * @throws IOException if a file cannot be read or is not a descriptor set
   * @throws CodecConfigException if the sets are inconsistent; see {@link #of}
   */
  public static ContractRegistry load(List<Path> descriptorSets) throws IOException {
    List<FileDescriptorSet> sets = new ArrayList<>();
    for (Path path : descriptorSets) {
      try (InputStream in = Files.newInputStream(path)) {
        sets.add(FileDescriptorSet.parseFrom(in, ExtensionRegistryLite.getEmptyRegistry()));
      }
    }
    return of(sets);
  }

  /**
   * Builds a registry from descriptor sets.
   *
   * @throws CodecConfigException if an imported file is in none of the sets, two sets hold
   *     different files under one name, a file is invalid, or a message type is defined twice
   */
  public static ContractRegistry of(List<FileDescriptorSet> descriptorSets) {
    Map<String, FileDescriptorProto> protos = new LinkedHashMap<>();
    for (FileDescriptorSet set : descriptorSets) {
      for (FileDescriptorProto file : set.getFileList()) {
        FileDescriptorProto previous = protos.putIfAbsent(file.getName(), file);
        if (previous != null && !previous.equals(file)) {
          throw new CodecConfigException(
              "descriptor sets hold different files named " + file.getName());
        }
      }
    }
    Map<String, FileDescriptor> built = new HashMap<>();
    Map<String, Descriptor> types = new HashMap<>();
    for (String name : protos.keySet()) {
      FileDescriptor file = build(name, protos, built, new ArrayList<>());
      for (Descriptor message : file.getMessageTypes()) {
        register(message, types);
      }
    }
    return new ContractRegistry(Map.copyOf(types));
  }

  private static FileDescriptor build(
      String name,
      Map<String, FileDescriptorProto> protos,
      Map<String, FileDescriptor> built,
      List<String> path) {
    FileDescriptor done = built.get(name);
    if (done != null) {
      return done;
    }
    FileDescriptorProto proto = protos.get(name);
    if (proto == null) {
      throw new CodecConfigException(
          "descriptor sets do not contain " + name + ", imported by " + path.getLast());
    }
    if (path.contains(name)) {
      throw new CodecConfigException("descriptor sets contain an import cycle through " + name);
    }
    path.add(name);
    FileDescriptor[] dependencies = new FileDescriptor[proto.getDependencyCount()];
    for (int i = 0; i < dependencies.length; i++) {
      dependencies[i] = build(proto.getDependency(i), protos, built, path);
    }
    path.removeLast();
    try {
      FileDescriptor file = FileDescriptor.buildFrom(proto, dependencies);
      built.put(name, file);
      return file;
    } catch (DescriptorValidationException e) {
      throw new CodecConfigException("invalid descriptor for " + name, e);
    }
  }

  private static void register(Descriptor message, Map<String, Descriptor> types) {
    Descriptor previous = types.putIfAbsent(message.getFullName(), message);
    if (previous != null) {
      throw new CodecConfigException(
          "message type "
              + message.getFullName()
              + " is defined in both "
              + previous.getFile().getName()
              + " and "
              + message.getFile().getName());
    }
    for (Descriptor nested : message.getNestedTypes()) {
      register(nested, types);
    }
  }

  /**
   * Returns the descriptor of a message type.
   *
   * @param fullName the fully qualified type name, without a leading dot
   * @throws CodecConfigException if no descriptor set defines the type
   */
  public Descriptor find(String fullName) {
    Descriptor type = types.get(fullName);
    if (type == null) {
      throw new CodecConfigException(
          "message type " + fullName + " is not in contracts.descriptorSets");
    }
    return type;
  }
}
