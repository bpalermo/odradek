"""proto_image: a self-contained descriptor set (a buf image) of proto_library targets."""

load("@protobuf//bazel/common:proto_info.bzl", "ProtoInfo")

def _import_path(info, file):
    """The name a .proto has inside its descriptor set: its path below the proto source root.

    That root is "." for a plain proto_library and a _virtual_imports directory
    for one that sets strip_import_prefix or import_prefix.
    """
    root = info.proto_source_root
    if root == ".":
        return file.short_path
    return file.path.removeprefix(root + "/")

def _proto_image_impl(ctx):
    infos = [t[ProtoInfo] for t in ctx.attr.protos]
    sets = depset(transitive = [i.transitive_descriptor_sets for i in infos])
    files = [_import_path(i, f) for i in infos for f in i.direct_sources]
    out = ctx.actions.declare_file(ctx.label.name + ".binpb")
    ctx.actions.run(
        executable = ctx.executable._protoc,
        arguments = [
            "--descriptor_set_in=" + ctx.configuration.host_path_separator.join([f.path for f in sets.to_list()]),
            "--descriptor_set_out=" + out.path,
            "--include_imports",
        ] + files,
        inputs = sets,
        outputs = [out],
        mnemonic = "ProtoImage",
        progress_message = "Building proto image %{label}",
    )
    return [DefaultInfo(files = depset([out]))]

proto_image = rule(
    implementation = _proto_image_impl,
    attrs = {
        "protos": attr.label_list(providers = [ProtoInfo], mandatory = True),
        "_protoc": attr.label(
            default = "//bazel/grpc:protoc",
            executable = True,
            cfg = "exec",
        ),
    },
)
