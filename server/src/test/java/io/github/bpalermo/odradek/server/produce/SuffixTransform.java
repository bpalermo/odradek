package io.github.bpalermo.odradek.server.produce;

import io.github.bpalermo.odradek.extension.RecordTransform;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.kafka.common.header.Headers;

/** Appends "!" and adds a header on produce. Fails for the payload "boom". */
public final class SuffixTransform implements RecordTransform {
  @Override
  public byte[] apply(String topic, Headers headers, byte[] data) {
    if (Arrays.equals(data, "boom".getBytes(StandardCharsets.UTF_8))) {
      throw new IllegalArgumentException("cannot encode boom");
    }
    headers.add("suffixed", new byte[] {1});
    byte[] out = Arrays.copyOf(data, data.length + 1);
    out[data.length] = '!';
    return out;
  }

  @Override
  public byte[] reverse(String topic, Headers headers, byte[] data) {
    return Arrays.copyOf(data, data.length - 1);
  }
}
