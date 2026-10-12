package io.github.bpalermo.odradek.server.codec;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ExtensionsTest {
  private static void jar(Path path, String entry, String content) throws IOException {
    try (OutputStream out = Files.newOutputStream(path);
        JarOutputStream jar = new JarOutputStream(out)) {
      jar.putNextEntry(new JarEntry(entry));
      jar.write(content.getBytes(StandardCharsets.UTF_8));
      jar.closeEntry();
    }
  }

  @Test
  void loadsOnlyJarsDirectlyInTheDirectory(@TempDir Path dir) throws Exception {
    jar(dir.resolve("b.jar"), "from-b.txt", "b");
    jar(dir.resolve("a.jar"), "from-a.txt", "a");
    jar(dir.resolve("ignored.zip"), "from-zip.txt", "zip");
    Files.createDirectory(dir.resolve("nested"));
    jar(dir.resolve("nested").resolve("c.jar"), "from-c.txt", "c");

    try (URLClassLoader loader = Extensions.classLoader(dir, getClass().getClassLoader())) {
      assertThat(loader.getURLs()).hasSize(2);
      assertThat(loader.getURLs()[0].getPath()).endsWith("a.jar");
      assertThat(loader.getResource("from-a.txt")).isNotNull();
      assertThat(loader.getResource("from-b.txt")).isNotNull();
      assertThat(loader.getResource("from-zip.txt")).isNull();
      assertThat(loader.getResource("from-c.txt")).isNull();
      // The parent stays in front: server classes are never replaced by an extension jar.
      assertThat(loader.loadClass(Extensions.class.getName())).isSameAs(Extensions.class);
    }
  }
}
