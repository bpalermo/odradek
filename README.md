# odradek

odradek is a Kafka client sidecar. It is a Java process that runs next to an application container in a Kubernetes pod, owns the Kafka producer and consumer, and exposes a gRPC API to the application over a Unix domain socket. A thin Java client library for applications lives in the same repository.

## Layout

| Path | Contents |
| --- | --- |
| `api/odradek/kafka/v1/` | The `OdradekService` protobuf/gRPC API and its generated Java targets |
| `sidecar/` | The sidecar `java_binary` (`src/main/java`, `src/test/java`) |
| `client/java/` | Java client library for applications |
| `bazel/` | Shared build code: formatter, linters, JUnit 6 macro, gRPC codegen shims |
| `docs/` | Design documents |

Java root package: `io.github.bpalermo.odradek`.

## Commands

Bazel is pinned in `.bazelversion` (use [Bazelisk](https://github.com/bazelbuild/bazelisk)). The JDK is downloaded by Bazel; no local JDK is needed, and no C++ is compiled for Java builds (protoc and the gRPC plugin are prebuilt binaries; Bazel 9 still needs a C++ compiler to be discoverable for toolchain analysis).

| Task | Command |
| --- | --- |
| Build | `bazel build //...` (`make build`) |
| Test | `bazel test //...` (`make test`) |
| Run the sidecar | `bazel run //sidecar` |
| Format (rewrites files) | `bazel run //:format` (`make format`) |
| Check formatting | `bazel run //:format.check` (`make format-check`) |
| Lint | `bazel build --config=lint //...` (`make lint`); `--config=ci` makes violations fail |
| Re-pin Maven dependencies | `REPIN=1 bazel run @maven//:pin` (`make repin`) after editing `maven.install` in `MODULE.bazel` |
| Tidy the module graph | `bazel mod tidy` (`make tidy`) |
| Proto breaking changes | checked by `//api:breaking_test` inside `bazel test //...` against `api/baseline.binpb`; accept a deliberate break or cut a release baseline with `bazel run //api:update_baseline` (`make baseline`) and commit the file |
| Build the image (linux/amd64 + linux/arm64) | `bazel build //sidecar:image_index` (`make image`) |
| Load the image into the local Docker daemon | `bazel run //sidecar:image_load` (as `localhost/odradek:e2e`; `make image-load`) |
| Push the image (CI) | `bazel run //sidecar:image_push` to `quay.io/odradek/odradek`, tags `dev` and `dev-<commit>`; its digest is in `bazel-bin/sidecar/image_index_digest` |
| Verify a published image's signatures | `bazel run //bazel/cosign:verify_image -- quay.io/odradek/odradek@sha256:<digest>` (exit 0 verified, 1 failed, 2 could not check) |
| Run cosign | `bazel run //bazel/cosign -- <args>` |
| End-to-end tests | start two Kafka brokers, then `ODRADEK_E2E_KAFKA_A=localhost:9092 ODRADEK_E2E_KAFKA_B=localhost:9093 bazel test //e2e/... --config=e2e` (`make e2e`); excluded from plain `bazel test //...` |

Formatting covers Java (google-java-format), Starlark (buildifier, including its lint) and protobuf (buf). Java is linted by Error Prone inside every `javac` action, with warnings as errors for first-party code; protos are linted by buf.

## License

Apache License 2.0. See [LICENSE](LICENSE).
