"""exec_protoc: the protoc that proto toolchain resolution selects, as an executable target.

`@protobuf//:protoc` is a cc_binary that is always compiled from source; only
toolchain resolution honours --@protobuf//bazel/toolchains:prefer_prebuilt_protoc.
grpc-java's java_grpc_library reaches protoc through that cc_binary, so its
patched `_protoc` default points here instead.
"""

_TOOLCHAIN_TYPE = "@protobuf//bazel/private:proto_toolchain_type"

def _exec_protoc_impl(ctx):
    compiler = ctx.toolchains[_TOOLCHAIN_TYPE].proto.proto_compiler
    out = ctx.actions.declare_file(ctx.label.name)
    ctx.actions.symlink(output = out, target_file = compiler.executable, is_executable = True)
    return [DefaultInfo(
        executable = out,
        files = depset([out]),
        runfiles = ctx.runfiles(files = [compiler.executable]),
    )]

exec_protoc = rule(
    implementation = _exec_protoc_impl,
    executable = True,
    toolchains = [_TOOLCHAIN_TYPE],
)
