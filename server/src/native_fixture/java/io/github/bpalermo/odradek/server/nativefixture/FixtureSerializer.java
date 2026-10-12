package io.github.bpalermo.odradek.server.nativefixture;

import org.apache.kafka.common.serialization.Serializer;

/**
 * A serializer in an extension jar, for the test that a native image refuses to load classes from
 * one: it is compiled into no image, only into a jar.
 */
public final class FixtureSerializer implements Serializer<byte[]> {
  @Override
  public byte[] serialize(String topic, byte[] data) {
    return data;
  }
}
