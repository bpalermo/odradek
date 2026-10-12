"""A multi-arch, digest-pinned OCI image for a Java deploy jar.

`java_image` wraps the rules_img rules that always appear together -- layer,
manifest, index, push, load -- plus the build setting that names the mutable
tag. Each carries a decision that is easy to undo by accident and expensive to
notice: no commit in the image itself, so that an image whose content did not
change keeps its digest; STABLE_ rather than volatile status keys in the push
tag; and a per-platform build of the jar. Calling this macro gets all of them.
"""

load("@bazel_skylib//rules:common_settings.bzl", "string_flag")
load("@rules_img//img:image.bzl", "image_index", "image_manifest")
load("@rules_img//img:layer.bzl", "file_metadata", "image_layer")
load("@rules_img//img:load.bzl", "image_load")
load("@rules_img//img:push.bzl", "image_push")

DEFAULT_PLATFORMS = [
    "//bazel/platforms:linux_amd64",
    "//bazel/platforms:linux_arm64",
]

def java_image(
        name,
        deploy_jar,
        registry,
        repository,
        base,
        jar_path = "/app/app.jar",
        jvm_flags = [],
        labels = {},
        platforms = DEFAULT_PLATFORMS,
        default_tag = "dev",
        load_tag = None,
        visibility = ["//visibility:public"]):
    """Builds a multi-arch image around a Java deploy jar and a target to push it.

    Defines, for `name = "image"`:

      //pkg:image_tag       string_flag, the mutable tag (override with
                            --//pkg:image_tag=vX.Y.Z)
      //pkg:image_layer     the deploy jar, one layer
      //pkg:image_manifest  one platform's manifest
      //pkg:image_index     the multi-platform index
      //pkg:image_push      pushes the index under two tags
      //pkg:image_load      loads the index into the local daemon as `load_tag`

    Args:
      name: prefix for the generated targets.
      deploy_jar: label of the java_binary's `_deploy.jar` output.
      registry: registry host: IMAGE_REGISTRY of //bazel:registry.bzl.
      repository: repository path within the registry.
      base: base image (a digest-pinned Java runtime).
      jar_path: absolute path of the jar inside the image.
      jvm_flags: extra arguments to `java`, before `-jar`.
      labels: OCI labels. Constant values only: a label is part of the image
        config, so one naming the commit gives every commit a new digest for
        the same content.
      platforms: platform labels, one manifest per platform.
      default_tag: value of the tag flag when nothing overrides it.
      load_tag: the name the image gets in a local daemon.
      visibility: visibility of the index and push targets.
    """
    string_flag(
        name = name + "_tag",
        build_setting_default = default_tag,
        visibility = ["//visibility:public"],
    )

    image_layer(
        name = name + "_layer",
        srcs = {jar_path: deploy_jar},
        compress = "zstd",
        default_metadata = file_metadata(mode = "0644"),
        include_runfiles = False,
    )

    image_manifest(
        name = name + "_manifest",
        base = base,
        # The base's entrypoint is `java -jar`; spelled out in full so the image
        # does not depend on it. Its default user (nonroot, 65532) is kept.
        entrypoint = ["/usr/bin/java"] + jvm_flags + ["-jar", jar_path],
        # Nothing stamped (no revision label): the digest names the content. The
        # commit is on the push's second tag and in the signature certificate.
        labels = labels,
        layers = [name + "_layer"],
    )

    # One manifest, many platforms: the index performs the platform transition
    # itself, so the deploy jar is built once per platform (selecting that
    # platform's netty native jar).
    image_index(
        name = name + "_index",
        manifests = [name + "_manifest"],
        platforms = platforms,
        visibility = visibility,
    )

    # Two tags: the first is mutable (what a human pulls); the second is
    # immutable and answers "what did this commit publish?". STABLE_, not the
    # volatile GIT_COMMIT, so a cached push cannot republish a stale tag.
    image_push(
        name = name + "_push",
        build_settings = {"tag": name + "_tag"},
        image = name + "_index",
        registry = registry,
        repository = repository,
        # Only this action sees the workspace status: "force" stamps the tag
        # templates below without a global --stamp, which would stamp every
        # other target that asks for it.
        stamp = "force",
        tag_list = [
            "{{.tag}}",
            "{{if .STABLE_GIT_COMMIT}}{{.tag}}-{{.STABLE_GIT_COMMIT}}{{end}}",
        ],
        visibility = visibility,
    )

    image_load(
        name = name + "_load",
        image = name + "_index",
        tag = load_tag or (repository + ":" + default_tag),
        visibility = visibility,
    )

def executable_image(
        name,
        binary,
        registry,
        repository,
        base,
        tag_flag,
        path = "/app/server",
        suffix = "-native",
        labels = {},
        load_tag = None,
        visibility = ["//visibility:public"]):
    """Builds a single-architecture image around one native executable.

    A native executable cannot be cross-compiled (GraalVM compiles for the
    machine it runs on), so unlike `java_image` this does not build an index
    for several platforms: the image is for the architecture of the machine
    running Bazel. Each architecture is built and pushed on a machine of that
    architecture, and the multi-arch index is assembled from the pushed
    manifests (.github/workflows/publish.yml).

    Defines, for `name = "image_native"`:

      //pkg:image_native_layer     the executable, one layer
      //pkg:image_native_manifest  the image of this machine's architecture
      //pkg:image_native_push      pushes it under one tag naming commit and
                                   architecture (`<tag>-<commit>-native-amd64`)
      //pkg:image_native_load      loads it into the local daemon as `load_tag`

    Args:
      name: prefix for the generated targets.
      binary: label of the native executable.
      registry: registry host: IMAGE_REGISTRY of //bazel:registry.bzl.
      repository: repository path within the registry.
      base: base image (a digest-pinned image with glibc).
      tag_flag: the string_flag holding the mutable tag, shared with the
        image this one is a variant of.
      path: absolute path of the executable inside the image.
      suffix: marks the variant in every tag.
      labels: OCI labels; constant values only, as for `java_image`.
      load_tag: the name the image gets in a local daemon.
      visibility: visibility of the manifest, push and load targets.
    """

    # The executable is built by a native-image run, which `bazel build //...`
    # should not start: every target here is named explicitly by CI.
    manual = ["manual"]

    image_layer(
        name = name + "_layer",
        srcs = {path: binary},
        compress = "zstd",
        default_metadata = file_metadata(mode = "0755"),
        include_runfiles = False,
        tags = manual,
    )

    image_manifest(
        name = name + "_manifest",
        base = base,
        entrypoint = [path],
        labels = labels,
        layers = [name + "_layer"],
        tags = manual,
        visibility = visibility,
    )

    # One tag, immutable because it names the commit. STABLE_, as for the JVM
    # image. The architecture is the machine's; select() on the CPU keeps it
    # out of the macro's arguments.
    tag = "{{.tag}}{{if .STABLE_GIT_COMMIT}}-{{.STABLE_GIT_COMMIT}}{{end}}" + suffix
    image_push(
        name = name + "_push",
        build_settings = {"tag": tag_flag},
        image = name + "_manifest",
        registry = registry,
        repository = repository,
        stamp = "force",
        tag_list = select({
            "@platforms//cpu:aarch64": [tag + "-arm64"],
            "//conditions:default": [tag + "-amd64"],
        }),
        tags = manual,
        visibility = visibility,
    )

    image_load(
        name = name + "_load",
        image = name + "_manifest",
        tag = load_tag or (repository + ":native"),
        tags = manual,
        visibility = visibility,
    )
