# Reachability metadata of the native image

`reachability-metadata.json` tells GraalVM what the server reaches through
reflection, resources and JNI, which it cannot see by analysing the code
([ADR 5](../../docs/adr/0005-graalvm-native-image-for-the-server.md)). It is
read by `//server:server_native` and checked by `NativeMetadataTest`.

What is in it, and why:

- **Generated protobuf messages** (`io.github.bpalermo.odradek.config.v1`, `odradek.kafka.v1`,
  `build.buf.validate`, and the well-known types) with `allPublicMethods`:
  protobuf's reflective accessors look their getters, setters and builders up by name. The test
  fails when a message of the schema is missing.
- **Kafka**: the serializers and deserializers that ship with the clients (a codec names them
  by class), the partition assignors and the JMX reporter (default configuration), and the OAuth
  defaults.
- **Compression libraries**: the JNI classes of zstd-jni and lz4-java, and their native
  libraries as resources (`linux/*/libzstd-jni-*.so`, `net/jpountz/util/linux/*/liblz4-java.so`,
  for both architectures). snappy-java and netty ship their own metadata.
- **JDK and Guava internals** that Netty, Guava and the Kafka clients reach reflectively.

Not in it: anything a library already ships under `META-INF/native-image` (Netty, snappy-java),
the extension classes of a configuration (a native image loads no class it was not built with:
see below), logback (it is configured while the image is built, see `//server:server_native`)
and JMX beans.

## Regenerating it

Run the end-to-end suite against the JVM server under GraalVM's tracing agent, which records
every reflective access that happens, and merge what is new. This needs two Kafka brokers (see
the README) and the GraalVM that the build uses:

1. `bazel build //server:server_native //server:server_deploy.jar` fetches GraalVM and the jars.
2. Write a script `server.sh` that runs
   `<output_base>/external/rules_graalvm++graalvm+graalvm/bin/java
   -agentlib:native-image-agent=config-merge-dir=<dir> -jar bazel-bin/server/server_deploy.jar "$@"`.
   Add a temporary copy of `//e2e:e2e_jvm_binary_test` to `e2e/BUILD.bazel` whose `env` sets
   `ODRADEK_E2E_SERVER` to the script's absolute path, and run it with
   `--config=e2e --strategy=TestRunner=local` (local, because the agent writes outside the
   sandbox). Remove the copy afterwards.
3. Merge `<dir>/reachability-metadata.json` into this file, dropping what the agent records
   but is not needed: classes that do not exist (it records failed lookups too), JDK management
   and JMX classes, locale data, security providers, logback, and whatever Netty or snappy-java
   already ship.
4. Rebuild, run `bazel test //server:native_config_test //e2e:e2e_native_binary_test --config=e2e`
   and add back what a failure names.

The end-to-end suite is the coverage: a code path it does not take is a path the agent did not
see. Put a new library behaviour under test there (`CompressionE2eTest` is an example) before
trusting the metadata for it.

## Codec extensions

A native image contains exactly the classes compiled into it. A configuration naming a class
the image lacks is rejected at load, and so is a class that only an `extensions_dir` jar holds
(`native_config_test` checks both). To use your own serializer or transform in a native image,
build the image yourself with your classes in it, for example (a sketch: this repository's tests
do not build such an image):

```starlark
load("@rules_graalvm//graalvm:defs.bzl", "native_image")

native_image(
    name = "my_server_native",
    data = ["//server:native_metadata", "my-metadata/reachability-metadata.json"],  # + your classes
    executable_name = "%target%",
    extra_args = [
        "-H:ConfigurationFileDirectories=server/native-image,my-metadata",
        "--static-nolibc",
    ],
    initialize_at_build_time = ["ch.qos.logback", "org.slf4j"],
    main_class = "io.github.bpalermo.odradek.server.Main",
    deps = [":my_server_runtime"],   # a java_library with runtime_deps = ["//server:server_runtime", ":my_codecs"]
)
```

Your classes need reflection metadata for their public no-argument constructor, as the
entries for Kafka's own serializers show.
