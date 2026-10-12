"""native_image: compile a java_binary ahead of time with GraalVM native-image.

The class path is the binary's own runtime class path, so the native executable
is built from exactly the jars the JVM binary runs. Everything else is explicit:
which GraalVM (`//bazel/graalvm:distribution`, pinned in MODULE.bazel), which
reachability metadata, which flags.

native-image links with the host C compiler (gcc) and needs the static zlib
(`zlib1g-dev` on Debian and Ubuntu). It cannot run on a remote executor that
lacks them, hence the `no-remote-exec` requirement.
"""

load("@rules_java//java/common:java_info.bzl", "JavaInfo")

def _native_image_impl(ctx):
    java_info = ctx.attr.binary[JavaInfo]
    jars = java_info.transitive_runtime_jars

    dist = ctx.attr.graalvm[DefaultInfo].files
    tool = None
    for f in dist.to_list():
        if f.path.endswith("/bin/native-image"):
            tool = f
            break
    if tool == None:
        fail("no bin/native-image in %s" % ctx.attr.graalvm.label)

    config_dirs = {}
    for f in ctx.files.metadata:
        config_dirs[f.dirname] = True

    out = ctx.actions.declare_file(ctx.label.name)

    args = ctx.actions.args()
    args.use_param_file("@%s", use_always = True)
    args.set_param_file_format("multiline")
    args.add_joined("--class-path", jars, join_with = ":")
    args.add("--no-fallback")

    # Not tuned to the machine that happens to build it: the image runs on
    # whatever node it is scheduled to.
    args.add("-march=compatibility")
    args.add("-J-Xmx%s" % ctx.attr.max_heap)
    for d in config_dirs.keys():
        args.add("-H:ConfigurationFileDirectories=%s" % d)
    args.add_all(ctx.attr.flags)
    args.add("-o", out)
    args.add(ctx.attr.main_class)

    ctx.actions.run(
        executable = tool,
        arguments = [args],
        inputs = depset(ctx.files.metadata, transitive = [jars]),
        tools = dist,
        outputs = [out],
        mnemonic = "NativeImage",
        progress_message = "Compiling %{label} with GraalVM native-image",
        # PATH reaches gcc and ld; HOME is somewhere writable.
        env = {"PATH": "/usr/bin:/bin", "HOME": "/tmp", "LC_ALL": "C.UTF-8"},
        execution_requirements = {"no-remote-exec": "1"},
    )
    return [DefaultInfo(files = depset([out]), executable = out)]

native_image = rule(
    implementation = _native_image_impl,
    attrs = {
        "binary": attr.label(
            mandatory = True,
            providers = [JavaInfo],
            doc = "The java_binary (or library) whose runtime class path is compiled.",
        ),
        "flags": attr.string_list(doc = "Extra native-image arguments."),
        "graalvm": attr.label(
            default = "//bazel/graalvm:distribution",
            doc = "The GraalVM distribution, with bin/native-image.",
        ),
        "main_class": attr.string(mandatory = True),
        "max_heap": attr.string(
            default = "8g",
            doc = "Heap of the image builder (-Xmx). A ceiling, not a reservation.",
        ),
        "metadata": attr.label_list(
            allow_files = [".json", ".properties"],
            doc = "Reachability metadata files; each directory holding some is a configuration directory.",
        ),
    },
    executable = True,
    doc = "Compiles a Java program to a native executable.",
)
