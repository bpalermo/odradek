"""maven_artifact: a publishable jar that holds only this repository's classes."""

load("@rules_jvm_external//:defs.bzl", "java_export")
load("//bazel:version.bzl", "VERSION")

GROUP = "io.github.bpalermo"

# gRPC and protobuf are built from source in this repository (see MODULE.bazel),
# so their targets carry no Maven coordinates and java_export can neither leave
# them out of the jar nor name them in the POM on its own. They are excluded
# from the jar below and named here, at the versions the build pins.
_GRPC_VERSION = "1.84.0"
_PROTOBUF_VERSION = "4.36.2"

GRPC_DEPENDENCIES = [
    "io.grpc:grpc-api:" + _GRPC_VERSION,
    "io.grpc:grpc-netty:" + _GRPC_VERSION,
    "io.grpc:grpc-protobuf:" + _GRPC_VERSION,
    "io.grpc:grpc-stub:" + _GRPC_VERSION,
    "com.google.protobuf:protobuf-java:" + _PROTOBUF_VERSION,
]

# Everything whose classes must not end up inside a published jar.
_NOT_IN_THE_JAR = [
    "//bazel/java:proto_runtime",
    "@grpc-java//api",
    "@grpc-java//netty",
    "@grpc-java//protobuf",
    "@grpc-java//stub",
    "@protobuf//java/core",
]

def _dependency(coordinates):
    group, artifact, version = coordinates.split(":")
    return "        <dependency>\n            <groupId>%s</groupId>\n            <artifactId>%s</artifactId>\n            <version>%s</version>\n        </dependency>" % (group, artifact, version)

def maven_artifact(name, artifact, exports, extra_dependencies = [], visibility = None):
    """Publishes first-party java_library targets as one Maven artifact.

    The jar contains the classes of `exports` and of the first-party targets
    they depend on (the protocol core, generated API classes). Third-party
    classes are never included; they become POM dependencies.

    Generated targets, as for java_export: `name` (the library), `name-pom`,
    `name.publish` (`bazel run` it to upload; release workflow only).

    Args:
      name: target name.
      artifact: the Maven artifactId; the groupId is GROUP and the version is
        //bazel:version.bzl.
      exports: the java_library targets to publish.
      extra_dependencies: `group:artifact:version` strings added to the POM,
        for dependencies built from source here (see GRPC_DEPENDENCIES).
      visibility: visibility of the generated targets.
    """
    native.genrule(
        name = name + "_pom_template",
        srcs = ["//bazel/maven:pom.tpl"],
        outs = [name + "_pom.tpl"],
        # The marker line is replaced by the extra dependencies; java_export
        # then fills in {dependencies} with the ones it can see.
        cmd = "awk -v extra='%s' '/<!-- extra dependencies -->/ { if (extra != \"\") print extra; next } { print }' $< > $@" %
              "\\n".join([_dependency(d) for d in extra_dependencies]),
    )
    java_export(
        name = name,
        deploy_env = _NOT_IN_THE_JAR,
        maven_coordinates = "%s:%s:%s" % (GROUP, artifact, VERSION),
        pom_template = name + "_pom_template",
        visibility = visibility,
        exports = exports,
    )
