# AGENTS.md

Guidance for coding agents working in this repository. The [README](README.md) has the full command table; [docs/design.md](docs/design.md) is the design, and [docs/adr/](docs/adr/) holds the decisions behind the clients, the version and the native image.

## What this is

Odradek is a server that owns an application's Kafka producers and consumers. The application talks to it over gRPC on a Unix domain socket and never sees the Kafka wire format. The repository holds the API, the server, and client libraries in Java, Clojure and Python.

## Commands

Everything goes through Bazel (pinned in `.bazelversion`; the JDK, Python and protoc are downloaded, none come from the host).

| Task | Command |
| --- | --- |
| Build and test what CI does | `bazel test --config=ci //...` |
| One test target | `bazel test //server:main_test` |
| Format (rewrites files) | `bazel run //:format` |
| End-to-end tests | `make e2e`, after starting the brokers (see the README) |

- `--config=ci` adds the linters as failures and refuses lockfile drift. Run it before pushing: a plain `bazel test //...` passes on code CI rejects.
- End-to-end tests carry the `e2e` tag and are skipped by `bazel test //...`. They need two plaintext Kafka brokers, which nothing in the repository starts, and a mutual TLS one, which `e2e/tls/broker.sh` starts. `//e2e:e2e_native_binary_test` is also `manual` (it builds the GraalVM image).
- `--config=llm` gives plain output with verbose failures.
- Java tests run on the JUnit console launcher (`java_junit_test`, `//bazel/java:junit.bzl`). A new test class runs only once it is listed in the target's `test_classes`, and `--test_filter` has no effect.
- First-party Java compiles with `-Xlint:all -Werror` and Error Prone, so a warning is a build failure.

## Layout

| Path | Contents |
| --- | --- |
| `api/odradek/kafka/v1/` | The gRPC API between an application and the server |
| `api/odradek/config/v1/` | The server's configuration schema, validated with protovalidate |
| `api/odradek/client/v1/` | The client protocol as data: events, actions, conformance scenarios |
| `server/` | The server: `config`, `codec`, `produce`, `consume` and `tls` packages under `io.github.bpalermo.odradek.server` |
| `client/core/` | The client protocol with no I/O: `handle(event) -> actions`, shared by the JVM clients |
| `client/java/`, `client/clojure/`, `client/python/` | The client libraries; Python has its own port of the core |
| `conformance/scenarios/` | YAML scenarios that every core must pass |
| `extension/java/` | `RecordTransform`, the interface codec extension jars implement |
| `e2e/` | Suites against real brokers, for the server and each client |
| `bazel/` | Build rules and macros shared across packages |

## Things to know before changing code

- **Protos.** Declare a `.proto` with `odradek_proto_library` (`//bazel/proto:defs.bzl`); it generates the Java, Python and Clojure libraries. Nothing generated is checked in. `//api:breaking_test` compares against `api/baseline.binpb`; update the baseline only for a release or a deliberate break (`bazel run //api:update_baseline`).
- **Client behaviour.** Protocol logic belongs in the core, not in a language binding. A change to it needs a scenario in `conformance/scenarios/`, and both cores must pass: `//client/core:conformance_test` and `//client/python:conformance_test`.
- **New message types.** The native image needs reflection metadata for them in `server/native-image/reachability-metadata.json`; `NativeMetadataTest` fails until it is there.
- **Dependencies.** After editing `maven.install` in `MODULE.bazel`, run `REPIN=1 bazel run @maven//:pin`. gRPC, Netty and protobuf-java have one pin each, in `MODULE.bazel`; the Clojure and Python clients must not bring their own copies (`//client/clojure:classpath_test` checks).
- **Version.** One version for everything, in `bazel/version.bzl`. Do not edit it by hand; the release workflow sets it from the tag with `scripts/set-version.sh`.
- **Tests that create topics** must wait for partition leaders before producing (see `ServerHarness.createTopic`); producing sooner can wedge Kafka's idempotent producer for minutes.
- **Design changes.** Update `docs/design.md` in the same change. A decision that constrains future work gets a new ADR in `docs/adr/`, in the format of the existing ones.

## Workflow

- `main` is protected: changes go through a pull request, and the `ci` check must pass.
- Stacked pull requests are managed with the `gh stack` extension.
- Releases are `vX.Y.Z` tags, which only repository admins can create. Do not push one unless asked.
