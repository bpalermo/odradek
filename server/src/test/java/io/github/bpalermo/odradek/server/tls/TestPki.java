package io.github.bpalermo.odradek.server.tls;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A throwaway certificate authority for tests, driven through the JDK's keytool: the JDK has no
 * public API that issues a certificate.
 */
public final class TestPki {
  private static final String PASSWORD = "changeit";

  private final Path dir;
  private final Path caStore;
  private int serial;

  /** One issued certificate: PEM chain (leaf only), PKCS#8 PEM key. */
  public record Issued(String certPem, String keyPem) {}

  /** Creates an authority whose files live under {@code dir}. */
  public TestPki(Path dir, String name) throws IOException, InterruptedException {
    this.dir = Files.createDirectories(dir.resolve(name));
    this.caStore = this.dir.resolve("ca.p12");
    keytool(
        "-genkeypair",
        "-alias",
        "ca",
        "-keyalg",
        "EC",
        "-groupname",
        "secp256r1",
        "-dname",
        "CN=" + name,
        "-ext",
        "bc:c=ca:true",
        "-ext",
        "ku:c=keyCertSign,cRLSign",
        "-validity",
        "2",
        "-keystore",
        caStore.toString());
  }

  /** The authority's certificate, PEM. */
  public String caPem() throws IOException, InterruptedException {
    Path out = dir.resolve("ca.pem");
    keytool(
        "-exportcert",
        "-alias",
        "ca",
        "-rfc",
        "-file",
        out.toString(),
        "-keystore",
        caStore.toString());
    return Files.readString(out);
  }

  /**
   * Issues a certificate valid for one day, for both TLS roles.
   *
   * @param san the subject alternative names in keytool's syntax, e.g. {@code
   *     uri:spiffe://example.org/broker} or {@code dns:localhost}
   */
  public Issued issue(String commonName, String san)
      throws IOException, InterruptedException, GeneralSecurityException {
    String id = "leaf-" + serial++;
    Path store = dir.resolve(id + ".p12");
    Path request = dir.resolve(id + ".csr");
    Path cert = dir.resolve(id + ".pem");
    keytool(
        "-genkeypair",
        "-alias",
        "leaf",
        "-keyalg",
        "EC",
        "-groupname",
        "secp256r1",
        "-dname",
        "CN=" + commonName,
        "-keystore",
        store.toString());
    keytool(
        "-certreq", "-alias", "leaf", "-file", request.toString(), "-keystore", store.toString());
    keytool(
        "-gencert",
        "-alias",
        "ca",
        "-infile",
        request.toString(),
        "-outfile",
        cert.toString(),
        "-rfc",
        "-ext",
        "san=" + san,
        "-ext",
        "ku:c=digitalSignature",
        "-ext",
        "eku=clientAuth,serverAuth",
        "-validity",
        "1",
        "-keystore",
        caStore.toString());

    KeyStore keys = KeyStore.getInstance("PKCS12");
    try (InputStream in = Files.newInputStream(store)) {
      keys.load(in, PASSWORD.toCharArray());
    }
    byte[] pkcs8 = keys.getKey("leaf", PASSWORD.toCharArray()).getEncoded();
    String keyPem =
        "-----BEGIN PRIVATE KEY-----\n"
            + Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII))
                .encodeToString(pkcs8)
            + "\n-----END PRIVATE KEY-----\n";
    return new Issued(Files.readString(cert), keyPem);
  }

  /** Writes a certificate, its key and a CA bundle under the default file names. */
  public static void write(Path dir, Issued issued, String caPem) throws IOException {
    Files.createDirectories(dir);
    Files.writeString(dir.resolve("tls.crt"), issued.certPem());
    Files.writeString(dir.resolve("tls.key"), issued.keyPem());
    Files.writeString(dir.resolve("ca.crt"), caPem);
  }

  private void keytool(String... arguments) throws IOException, InterruptedException {
    List<String> command = new ArrayList<>();
    command.add(Path.of(System.getProperty("java.home"), "bin", "keytool").toString());
    command.addAll(List.of(arguments));
    command.addAll(List.of("-storetype", "PKCS12", "-storepass", PASSWORD));
    Path log = dir.resolve("keytool.log");
    Process process =
        new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
    if (!process.waitFor(60, TimeUnit.SECONDS) || process.exitValue() != 0) {
      process.destroyForcibly();
      throw new IOException("keytool " + arguments[0] + " failed: " + Files.readString(log));
    }
  }

  /** Runs from the command line, for the end-to-end broker: {@code TestPki <dir>}. */
  public static void main(String[] args) throws Exception {
    Path dir = Path.of(args[0]);
    TestPki pki = new TestPki(dir, "ca");
    String ca = pki.caPem();
    write(
        dir.resolve("broker"),
        pki.issue("broker", "uri:spiffe://e2e.test/broker,dns:localhost"),
        ca);
    write(dir.resolve("client"), pki.issue("client", "uri:spiffe://e2e.test/odradek"), ca);
    write(
        dir.resolve("client-next"), pki.issue("client-next", "uri:spiffe://e2e.test/odradek"), ca);
  }
}
