package io.github.bpalermo.odradek.server.tls;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Credentials from three PEM files in a directory: certificate chain, private key and CA bundle.
 *
 * <p>The files are re-read by {@link #poll()}. Polling, not a file watch: a Kubernetes volume is
 * updated by swapping a symlink, which a watch on the files themselves does not see. New content
 * replaces the current set only if it is usable as a whole. Whoever writes the files may write the
 * certificate and the key at different moments; a mismatched pair is rejected and the next poll
 * tries again.
 */
public final class FileCredentialSource implements CredentialSource {
  private static final Logger LOG = LoggerFactory.getLogger(FileCredentialSource.class);

  private final String cluster;
  private final Path certFile;
  private final Path keyFile;
  private final Path caFile;
  private final Clock clock;
  private final AtomicLong reloads = new AtomicLong();
  private final AtomicLong failures = new AtomicLong();

  private volatile Credentials current;

  /** What the files held when they were last looked at, applied or rejected. */
  private Content seen;

  private record Content(byte[] cert, byte[] key, byte[] ca) {
    boolean sameAs(Content other) {
      return other != null
          && Arrays.equals(cert, other.cert)
          && Arrays.equals(key, other.key)
          && Arrays.equals(ca, other.ca);
    }
  }

  /**
   * Loads the first set.
   *
   * @throws CredentialException if the files cannot be read or are not usable
   */
  public FileCredentialSource(String cluster, Path certFile, Path keyFile, Path caFile, Clock clock)
      throws CredentialException {
    this.cluster = cluster;
    this.certFile = certFile;
    this.keyFile = keyFile;
    this.caFile = caFile;
    this.clock = clock;
    Content content;
    try {
      content = read();
    } catch (IOException e) {
      throw new CredentialException("cannot read " + e.getMessage(), e);
    }
    this.current = parse(content);
    this.seen = content;
    LOG.info(
        "cluster {}: TLS credentials loaded from {}, certificate expires {}",
        cluster,
        certFile.getParent(),
        current.notAfter());
  }

  @Override
  public Credentials current() {
    return current;
  }

  /** How many times new content replaced the current set. */
  public long reloads() {
    return reloads.get();
  }

  /** How many times new content was rejected. */
  public long failures() {
    return failures.get();
  }

  /** Re-reads the files and applies them if they changed and are usable. */
  public synchronized void poll() {
    Content content;
    try {
      content = read();
    } catch (IOException e) {
      // Mid-update, or the volume is gone. The current set stays; say so once per state.
      if (seen != null) {
        seen = null;
        failures.incrementAndGet();
        LOG.warn("cluster {}: cannot read TLS credentials: {}", cluster, e.toString());
      }
      return;
    }
    if (content.sameAs(seen)) {
      return;
    }
    seen = content;
    try {
      current = parse(content);
    } catch (CredentialException e) {
      failures.incrementAndGet();
      LOG.warn(
          "cluster {}: new TLS credentials rejected, keeping the certificate that expires {}: {}",
          cluster,
          current.notAfter(),
          e.getMessage());
      return;
    }
    reloads.incrementAndGet();
    LOG.info(
        "cluster {}: TLS credentials reloaded, certificate expires {}",
        cluster,
        current.notAfter());
  }

  private Content read() throws IOException {
    return new Content(
        Files.readAllBytes(certFile), Files.readAllBytes(keyFile), Files.readAllBytes(caFile));
  }

  private Credentials parse(Content content) throws CredentialException {
    List<X509Certificate> chain = Pem.certificates(content.cert(), certFile.toString());
    PrivateKey key = Pem.privateKey(content.key(), keyFile.toString());
    List<X509Certificate> trust = Pem.certificates(content.ca(), caFile.toString());
    return Credentials.validated(key, chain, trust, clock.instant());
  }
}
