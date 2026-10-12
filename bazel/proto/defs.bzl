"""odradek_proto_library: one .proto of the API and its library in every language.

One call per .proto file, in the package that holds it:

    odradek_proto_library(
        name = "odradek",          # odradek.proto
        grpc = True,
        visibility = [...],        # who may use the generated libraries
    )

declares

    <name>_proto            proto_library (public: descriptors are the contract)
    <name>_buf_lint_test    buf lint
    <name>_java_proto       java_proto_library
    <name>_java_grpc        java_grpc_library, with grpc = True
    <name>_py_proto         _pb2.py, _pb2.pyi and, with grpc = True, _pb2_grpc.py
    <name>_clj_proto        clj_library of the namespace protoc-gen-clojure makes,
                            unless clojure = False
    <name>_clj_proto_src    that namespace's source file, for the Clojars jar

Nothing generated is checked in. The Python and Clojure clients ship the
generated code inside their own artifacts, so they place these outputs in
their own source trees: //client/python with py_proto_package (//bazel/python:proto.bzl), //client/clojure
with a copy for the one strip_prefix clj_maven_export takes.
"""

load("@grpc-java//:java_grpc_library.bzl", "java_grpc_library")
load("@protobuf//bazel:java_proto_library.bzl", "java_proto_library")
load("@protobuf//bazel:proto_library.bzl", "proto_library")
load("@protoc_gen_clojure//clojure:defs.bzl", "clojure_proto_library")
load("@rules_clj//clojure:defs.bzl", "clj_library")
load("//bazel/lint:linters.bzl", "buf_test")
load("//bazel/python:proto.bzl", "py_proto_gen")

# Every .proto of the API is imported by its path below this directory.
_ROOT = "api"

def _unmap_cmd(names):
    """Shell that copies $< to $@, freeing each of `names` before its defrecord.

    protoc-gen-clojure 0.7.0 emits `(defrecord Error ...)` for a message named
    Error, which does not load: every namespace already maps Error to
    java.lang.Error. Until the plugin handles such names, the line that frees
    the name is added here, and the build fails if the place for it is gone.
    """
    if not names:
        return "cp $< $@"
    pattern = "|".join(names)
    checks = "\n".join([
        "grep -q '^(ns-unmap \\*ns\\* (quote %s))$$' $@" % n
        for n in names
    ])
    return """
awk 'match($$0, /^\\(defrecord (%s) \\[/) { split($$0, w, " "); print "(ns-unmap *ns* (quote " w[2] "))" } { print }' $< > $@
%s
""" % (pattern, checks)

def odradek_proto_library(
        name,
        deps = [],
        grpc = False,
        clojure = True,
        clj_shadowed = [],
        visibility = None):
    """Declares the proto_library of <name>.proto and its Java, Python and Clojure libraries.

    Args:
      name: the .proto file's name without the extension.
      deps: the proto_library targets it imports.
      grpc: the file declares a service; also generate the gRPC stubs.
      clojure: False for a file that imports a .proto from outside the API:
        the namespace generated for it would require one that nobody generates.
      clj_shadowed: messages whose name every Clojure namespace already maps
        to a java.lang class (Error, Exception, ...).
      visibility: of the generated libraries. The API's own packages are added.
    """
    package = native.package_name()
    if not package.startswith(_ROOT + "/"):
        fail("odradek_proto_library is for packages below //%s" % _ROOT)
    import_dir = package.removeprefix(_ROOT + "/")
    proto = ":%s_proto" % name
    visibility = (visibility or []) + ["//%s:__subpackages__" % _ROOT]

    proto_library(
        name = name + "_proto",
        srcs = [name + ".proto"],
        strip_import_prefix = "/" + _ROOT,
        visibility = ["//visibility:public"],
        deps = deps,
    )

    buf_test(
        name = name + "_buf_lint_test",
        srcs = [proto],
    )

    # --- Java ---
    java_proto_library(
        name = name + "_java_proto",
        visibility = visibility,
        deps = [proto],
    )
    if grpc:
        java_grpc_library(
            name = name + "_java_grpc",
            srcs = [proto],
            visibility = visibility,
            deps = [":%s_java_proto" % name],
        )

    # --- Python ---
    py_proto_gen(
        name = name + "_py_proto",
        grpc = grpc,
        proto = proto,
        visibility = visibility,
    )

    # --- Clojure ---
    if not clojure:
        return

    # protoc-gen-clojure names its output by the import path, so the raw file
    # is <import_dir>/<name>.clj below this package and the one that loads is
    # the same path below clj/.
    clj = "%s/%s.clj" % (import_dir, name)
    clojure_proto_library(
        name = name + "_clj_proto_raw",
        outs = [clj],
        proto = proto,
    )
    native.genrule(
        name = name + "_clj_proto_src",
        srcs = [clj],
        outs = ["clj/" + clj],
        cmd = _unmap_cmd(clj_shadowed),
        visibility = visibility,
    )
    clj_library(
        name = name + "_clj_proto",
        srcs = [":%s_clj_proto_src" % name],
        namespaces = [clj.removesuffix(".clj").replace("/", ".")],
        strip_prefix = package + "/clj",
        visibility = visibility,
        deps = [
            # The namespaces of the API files this one imports.
            d.removesuffix("_proto") + "_clj_proto"
            for d in deps
            if d.startswith("//" + _ROOT + "/")
        ] + [
            # With protoc's Java classes on the class path the generated
            # namespace uses them as its message type, which is what lets a
            # message go from clj-grpc to the Java core and back without a copy.
            ":%s_java_proto" % name,
            "//client/clojure:jvm_deps",
            "@clj_deps//:com_github_bpalermo_clj_grpc",
            "@clj_deps//:com_github_bpalermo_clj_protobuf",
        ],
    )
