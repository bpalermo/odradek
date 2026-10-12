package io.github.bpalermo.odradek.server.codec;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/** Loads the jars that hold serializer, deserializer and transform classes. */
public final class Extensions {
  private Extensions() {}

  /**
   * Returns a class loader over every {@code *.jar} directly inside {@code dir}, in file name
   * order, with {@code parent} consulted first. The server's own classes, kafka-clients and
   * protobuf therefore always come from the server.
   *
   * @throws IOException if the directory cannot be listed
   */
  public static URLClassLoader classLoader(Path dir, ClassLoader parent) throws IOException {
    List<URL> jars = new ArrayList<>();
    try (Stream<Path> files = Files.list(dir)) {
      for (Path jar :
          files.filter(f -> f.getFileName().toString().endsWith(".jar")).sorted().toList()) {
        jars.add(jar.toUri().toURL());
      }
    }
    return new URLClassLoader("odradek-extensions", jars.toArray(URL[]::new), parent);
  }
}
