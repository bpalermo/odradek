package io.github.bpalermo.odradek.server.produce;

import io.github.bpalermo.odradek.extension.RecordTransform;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.kafka.common.header.Headers;

/**
 * Appends "!" and adds a header on produce; undoes both on consume. Fails to encode the payload
 * "boom" and to decode anything that does not end with "!".
 */
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
    if (data.length == 0 || data[data.length - 1] != '!') {
      throw new IllegalArgumentException("data does not end with the suffix");
    }
    headers.remove("suffixed");
    return Arrays.copyOf(data, data.length - 1);
  }
}
