# odradek

odradek moves the Kafka client out of the application. Its server is a Java process, deployed today as a sidecar container next to the application in a Kubernetes pod, that owns the Kafka producer and consumer, and exposes a gRPC API to the application over a Unix domain socket. Thin client libraries for applications, in Java and Python, live in the same repository.

## Layout

| Path | Contents |
| --- | --- |
| `api/odradek/kafka/v1/` | The `OdradekService` protobuf/gRPC API |
| `api/defs.bzl` | `odradek_proto_library`: one call per `.proto` declares its `proto_library`, lint test and Java, Python and Clojure libraries |
| `server/` | The server `java_binary` (`src/main/java`, `src/test/java`) and its GraalVM native image (`native-image/` holds the reachability metadata) |
| `client/core/` | The client protocol without I/O, shared by the JVM clients (ADR 1) |
| `client/java/` | Java client library for applications |
| `client/clojure/` | Clojure client library for applications (`odradek.client`): the same core on clj-grpc and clj-protobuf, built with rules_clj (ADR 2) |
| `client/python/` | Python client library for applications (asyncio): a port of the protocol core, the generated API and the wheel (ADR 3) |
| `conformance/` | Scenarios every client core must pass |
| `e2e/` | End-to-end suites against real brokers: the server and the Java client (`src/`), the Python client (`python/`) |
| `extension/java/` | Interfaces for extension jars: `RecordTransform`, the byte-to-byte codec step (encryption and similar) |
| `bazel/` | Shared build code: formatter, linters, JUnit 6 macro, gRPC codegen shims |
| `docs/` | The design (`design.md`) and architecture decision records (`adr/`) |

Java root package: `io.github.bpalermo.odradek`.

## Commands

Bazel is pinned in `.bazelversion` (use [Bazelisk](https://github.com/bazelbuild/bazelisk)). The JDK is downloaded by Bazel; no local JDK is needed, and no C++ is compiled for Java builds (protoc and the gRPC plugin are prebuilt binaries; Bazel 9 still needs a C++ compiler to be discoverable for toolchain analysis). The Python interpreter and the PyPI packages (prebuilt wheels, pinned with hashes in `client/python/requirements_lock.txt`) are downloaded by Bazel as well; no local Python is used.

| Task | Command |
| --- | --- |
| Build | `bazel build //...` (`make build`) |
| Test | `bazel test //...` (`make test`) |
| Run the server | `bazel run //server -- --config $PWD/server/testdata/minimal.yaml` (the default is `/etc/odradek/odradek.yaml`; the socket directory `/var/run/kafka` must exist, or set `listener.socketPath`) |
| Format (rewrites files) | `bazel run //:format` (`make format`) |
| Check formatting | `bazel run //:format.check` (`make format-check`) |
| Lint | `bazel build --config=lint //...` (`make lint`); `--config=ci` makes violations fail |
| Re-pin Maven dependencies | `REPIN=1 bazel run @maven//:pin` (`make repin`) after editing `maven.install` in `MODULE.bazel` |
| Re-lock the Clojure dependencies | `bazel run //client/clojure:lock -- --aliases=bazel --deps-edn=$PWD/client/clojure/deps.edn --output=$PWD/client/clojure/deps.lock.json` after editing `client/clojure/deps.edn`; gRPC, Netty and protobuf-java are not in that lock, they come from `MODULE.bazel` |
| Test the Clojure client | `bazel test //client/clojure/...` (unit tests, a stand-in server, and the check that the class path holds one copy of each library); its end-to-end suite is `//e2e:clojure_client_e2e_test`, part of the end-to-end tests below |
| Build the Clojure client's Clojars jar and pom | `bazel build //client/clojure:clojars` (`com.github.bpalermo/odradek-clj`); `bazel run //client/clojure:clojars.publish -- --dry-run` prints the uploads without sending |
| Tidy the module graph | `bazel mod tidy` (`make tidy`) |
| Proto breaking changes | checked by `//api:breaking_test` inside `bazel test //...` against `api/baseline.binpb`; accept a deliberate break or cut a release baseline with `bazel run //api:update_baseline` (`make baseline`) and commit the file |
| Build the image (linux/amd64 + linux/arm64) | `bazel build //server:image_index` (`make image`) |
| Load the image into the local Docker daemon | `bazel run //server:image_load` (as `localhost/odradek:e2e`; `make image-load`) |
| Push the image (CI) | `bazel run //server:image_push` to `quay.io/odradek/odradek`, tags `dev` and `dev-<commit>`; its digest is in `bazel-bin/server/image_index_digest` |
| Verify a published image's signatures | `bazel run //bazel/cosign:verify_image -- quay.io/odradek/odradek@sha256:<digest>` (exit 0 verified, 1 failed, 2 could not check) |
| Run cosign | `bazel run //bazel/cosign -- <args>` |
| Build the native image (linux, this machine's CPU) | `bazel build //server:server_native` (`make native`); needs `gcc` and `zlib1g-dev`, about 4 GB of memory and 1 to 2 minutes; excluded from `//...` |
| Test the native image | `bazel test //server:native_config_test` (no broker), and `ODRADEK_E2E_KAFKA_A=localhost:9092 ODRADEK_E2E_KAFKA_B=localhost:9093 bazel test //e2e:e2e_native_binary_test --config=e2e` (the same end-to-end suite against it) |
| Build and load the native image | `bazel build //server:image_native_manifest` and `bazel run //server:image_native_load` (as `localhost/odradek:e2e-native`; this machine's architecture only, as GraalVM does not cross-compile); `bazel run //server:image_native_push` pushes it as `dev-<commit>-native-<arch>`, and publish.yml assembles the index `dev-native` and `dev-<commit>-native` |
| End-to-end tests | start two Kafka brokers, then `ODRADEK_E2E_KAFKA_A=localhost:9092 ODRADEK_E2E_KAFKA_B=localhost:9093 bazel test //e2e/... --config=e2e` (`make e2e`); excluded from plain `bazel test //...` |
| Build the Python wheel | `bazel build //client/python:wheel` (pure Python, with the generated API code; in `bazel-bin/client/python/odradek-<version>-py3-none-any.whl`). Nothing uploads it. |
| Re-lock Python dependencies | `uv pip compile --universal --generate-hashes --python-version 3.13 --no-header client/python/requirements.in -o client/python/requirements_lock.txt` after editing `requirements.in` (needs [uv](https://docs.astral.sh/uv/)), then `bazel mod deps` to update `MODULE.bazel.lock` |

Formatting covers Java (google-java-format), Python (ruff), Starlark (buildifier, including its lint) and protobuf (buf). Java is linted by Error Prone inside every `javac` action, with warnings as errors for first-party code; protos are linted by buf and Python by ruff (`ruff.toml`).

## Versions and Maven artifacts

Everything released from this repository carries one version ([ADR 4](docs/adr/0004-one-version-for-server-and-clients.md)), held in `bazel/version.bzl`. It is `0.0.0-dev` on `main`.

| Artifact | Target | Contents |
| --- | --- | --- |
| `io.github.bpalermo:odradek-client` | `//client/java:maven` | The Java client, the protocol core and the generated API classes. gRPC, Netty and protobuf are dependencies |
| `io.github.bpalermo:odradek-extension` | `//extension/java:maven` | `RecordTransform`, for codec extension jars |

`bazel build //client/java:maven-pom` writes the POM. `bazel run //client/java:maven.publish` uploads; it is meant for the release workflow.

### Releasing

A release is a tag `vX.Y.Z` on `main`'s tip:

```sh
git tag v1.2.3 origin/main && git push origin v1.2.3
```

`.github/workflows/release.yml` then writes the version into its checkout (`scripts/set-version.sh`, never committed), builds and tests, and publishes:

| What | Where | Status |
| --- | --- | --- |
| Server image, JVM and native | `quay.io/odradek/odradek:X.Y.Z` and `:X.Y.Z-native`, signed | Same jobs as every commit on `main` |
| Java client and extension API | Maven Central | Off until the repository variable `PUBLISH_MAVEN` is `true`; never run |
| Clojure client | Clojars | Off until `PUBLISH_CLOJARS` is `true`; never run |
| Python client | PyPI | Off until `PUBLISH_PYPI` is `true`; never run |
| GitHub release | this repository | Generated notes |

Each library job documents the secrets and registry setup it needs. Only repository admins can create `v*` tags.

## License

Apache License 2.0. See [LICENSE](LICENSE).
