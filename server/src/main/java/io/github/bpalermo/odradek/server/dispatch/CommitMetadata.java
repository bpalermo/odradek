package io.github.bpalermo.odradek.server.dispatch;

import java.util.Arrays;
import java.util.Base64;
import java.util.BitSet;
import java.util.function.LongConsumer;

/**
 * The offsets above a partition's committed offset that are acknowledged, as a commit carries them
 * in its metadata (ADR 8), so that the next owner of the partition does not deliver them again.
 *
 * <p>The metadata is {@value #PREFIX} followed by a bitmap in unpadded base64url, least significant
 * bit first: bit <i>i</i> says whether offset <i>committed</i> + 1 + <i>i</i> is acknowledged.
 * Trailing zero bytes are left out, and nothing acknowledged is the empty string. The bitmap is cut
 * at {@link #MAX_BYTES}; what lies past it is delivered again. The format is a contract with the
 * next replica, of whatever version: a change gets a new prefix, and the old one is still read.
 */
public final class CommitMetadata {

  static final String PREFIX = "odradek.acked/1:";

  /** The longest bitmap a commit carries: 16,384 offsets, 2,747 characters with the prefix. */
  static final int MAX_BYTES = 2048;

  /** Nothing acknowledged above the committed offset, or nothing known of it. */
  public static final CommitMetadata NONE = new CommitMetadata(-1, new BitSet());

  private static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();
  private static final Base64.Decoder DECODER = Base64.getUrlDecoder();

  private final long committed;
  private final BitSet bits;

  private CommitMetadata(long committed, BitSet bits) {
    this.committed = committed;
    this.bits = bits;
  }

  /** Collects the acknowledged offsets above {@code committed}, in any order. */
  public static Builder above(long committed) {
    return new Builder(committed);
  }

  /**
   * What a commit at {@code committed} carried. Metadata that is not in this format, which is to
   * say written by an older dispatcher, by anything else that commits for the group, or by an
   * offset reset, says nothing.
   */
  public static CommitMetadata decode(long committed, String metadata) {
    if (metadata == null || !metadata.startsWith(PREFIX)) {
      return NONE;
    }
    byte[] bytes;
    try {
      bytes = DECODER.decode(metadata.substring(PREFIX.length()));
    } catch (IllegalArgumentException e) {
      return NONE;
    }
    BitSet bits = BitSet.valueOf(Arrays.copyOf(bytes, Math.min(bytes.length, MAX_BYTES)));
    return bits.isEmpty() ? NONE : new CommitMetadata(committed, bits);
  }

  /** The metadata a commit at the committed offset carries. */
  public String encode() {
    return bits.isEmpty() ? "" : PREFIX + ENCODER.encodeToString(bits.toByteArray());
  }

  /**
   * The same offsets, above a committed offset at or above this one: those at or below it are
   * dropped.
   */
  public CommitMetadata rebased(long newCommitted) {
    if (bits.isEmpty() || newCommitted == committed) {
      return this;
    }
    if (newCommitted < committed) {
      throw new IllegalArgumentException(
          "offset " + newCommitted + " is below the committed offset " + committed);
    }
    long shift = newCommitted - committed;
    if (shift >= bits.length()) {
      return NONE;
    }
    BitSet shifted = bits.get((int) shift, bits.length());
    return shifted.isEmpty() ? NONE : new CommitMetadata(newCommitted, shifted);
  }

  /** Whether {@code offset} is one of the acknowledged offsets. */
  public boolean acknowledged(long offset) {
    long index = offset - committed - 1;
    return !bits.isEmpty() && index >= 0 && index < (long) MAX_BYTES * 8 && bits.get((int) index);
  }

  /** Gives {@code action} each acknowledged offset at or above {@code from}, lowest first. */
  public void forEachFrom(long from, LongConsumer action) {
    if (bits.isEmpty()) {
      return;
    }
    long start = Math.max(0, from - committed - 1);
    if (start >= bits.length()) {
      return;
    }
    for (int i = bits.nextSetBit((int) start); i >= 0; i = bits.nextSetBit(i + 1)) {
      action.accept(committed + 1 + i);
    }
  }

  /** How many offsets are acknowledged. */
  public int count() {
    return bits.cardinality();
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof CommitMetadata other
        && (bits.isEmpty() ? other.bits.isEmpty() : committed == other.committed)
        && bits.equals(other.bits);
  }

  @Override
  public int hashCode() {
    return bits.isEmpty() ? 0 : Long.hashCode(committed) * 31 + bits.hashCode();
  }

  @Override
  public String toString() {
    return bits.isEmpty() ? "none" : "acknowledged above " + committed + ": " + bits;
  }

  /** Collects acknowledged offsets above a committed offset. */
  public static final class Builder {
    private final long committed;
    private final BitSet bits = new BitSet();

    private Builder(long committed) {
      this.committed = committed;
    }

    /**
     * Adds an acknowledged offset, and says whether the metadata carries it: not when it lies past
     * what {@link #MAX_BYTES} holds.
     */
    public boolean add(long offset) {
      long index = offset - committed - 1;
      if (index < 0) {
        throw new IllegalArgumentException(
            "offset " + offset + " is not above the committed offset " + committed);
      }
      if (index >= (long) MAX_BYTES * 8) {
        return false;
      }
      bits.set((int) index);
      return true;
    }

    public CommitMetadata build() {
      return bits.isEmpty() ? NONE : new CommitMetadata(committed, (BitSet) bits.clone());
    }
  }
}
