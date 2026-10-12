"""py_proto_gen: Python code for a proto_library, generated into this package.

protobuf's own py_proto_library is not used for two reasons. Its runtime is
@protobuf//:protobuf_python, which compiles the upb C extension from source,
and this repository compiles no C++ (the runtime here is the `protobuf` wheel).
And it places the generated modules under the proto's own package, while the
wheel needs them inside the `odradek` package next to the client's sources:
`odradek/__init__.py` is hand-written, `odradek/kafka/v1/odradek_pb2.py` is
generated, and Python only sees one `odradek` package if both are in the same
directory.

The messages (`_pb2.py`) and their type stubs (`_pb2.pyi`) come from the
protoc that proto toolchain resolution selects, the same prebuilt binary that
generates the Java code. The gRPC stub (`_pb2_grpc.py`) comes from the plugin
inside the `grpcio-tools` wheel. Both read the descriptor sets the
proto_library already produced, so no .proto file is parsed twice.
"""

load("@bazel_skylib//lib:paths.bzl", "paths")
load("@protobuf//bazel/common:proto_info.bzl", "ProtoInfo")

_TOOLCHAIN_TYPE = "@protobuf//bazel/private:proto_toolchain_type"

def _py_proto_gen_impl(ctx):
    info = ctx.attr.proto[ProtoInfo]
    out_root = paths.join(ctx.bin_dir.path, ctx.label.package)

    # Import paths of the direct sources, e.g. odradek/kafka/v1/odradek.proto.
    imports = [
        paths.relativize(src.path, info.proto_source_root)
        for src in info.direct_sources
    ]
    stems = [paths.replace_extension(path, "") for path in imports]
    modules = [ctx.actions.declare_file(stem + "_pb2.py") for stem in stems]
    stubs = [ctx.actions.declare_file(stem + "_pb2.pyi") for stem in stems]

    descriptor_sets = info.transitive_descriptor_sets
    descriptor_set_in = ctx.actions.args()
    descriptor_set_in.add_joined(
        "--descriptor_set_in",
        descriptor_sets,
        join_with = ctx.configuration.host_path_separator,
    )

    protoc = ctx.toolchains[_TOOLCHAIN_TYPE].proto.proto_compiler
    ctx.actions.run(
        executable = protoc,
        arguments = [
            descriptor_set_in,
            "--python_out=" + out_root,
            "--pyi_out=" + out_root,
        ] + imports,
        inputs = descriptor_sets,
        outputs = modules + stubs,
        mnemonic = "PyProtoGen",
        progress_message = "Generating Python messages for %{label}",
    )

    if ctx.attr.grpc:
        grpc_modules = [ctx.actions.declare_file(stem + "_pb2_grpc.py") for stem in stems]
        ctx.actions.run(
            executable = ctx.executable._grpc_protoc,
            arguments = [descriptor_set_in, "--grpc_python_out=" + out_root] + imports,
            inputs = descriptor_sets,
            outputs = grpc_modules,
            mnemonic = "PyGrpcGen",
            progress_message = "Generating the Python gRPC stub for %{label}",
        )
        modules = modules + grpc_modules

    return [
        DefaultInfo(files = depset(modules)),
        OutputGroupInfo(pyi = depset(stubs)),
    ]

py_proto_gen = rule(
    implementation = _py_proto_gen_impl,
    doc = "Generates _pb2.py (default outputs) and _pb2.pyi (output group `pyi`) for one proto_library.",
    attrs = {
        "grpc": attr.bool(
            doc = "Also generate _pb2_grpc.py, the gRPC stub.",
        ),
        "proto": attr.label(
            doc = "The proto_library to generate for.",
            mandatory = True,
            providers = [ProtoInfo],
        ),
        "_grpc_protoc": attr.label(
            default = "//client/python/tools:grpc_protoc",
            executable = True,
            cfg = "exec",
        ),
    },
    toolchains = [_TOOLCHAIN_TYPE],
)
