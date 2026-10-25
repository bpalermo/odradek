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
- PMD lints `java_library` and `java_binary` sources under `--config=lint` (ruleset: `pmd.xml`; tests are not visited). An unused pattern binding is written `_`. Exclude a rule in `pmd.xml` with its reason, do not scatter suppressions.

## Layout

| Path | Contents |
| --- | --- |
| `api/odradek/kafka/v1/` | The gRPC API between an application and the server |
| `api/odradek/config/v1/` | The server's configuration schema, validated with protovalidate |
| `api/odradek/delivery/v1/` | The gRPC protocol between a sidecar and a dispatcher (ADR 6). The server serves it when it has a `dispatch` section, and is its client for the consumers whose `source` is the dispatcher |
| `api/odradek/client/v1/` | The client protocol as data: events, actions, conformance scenarios |
| `server/` | The server: `config`, `codec`, `produce`, `consume`, `dispatch` (the dispatcher of ADR 6), `receive` (the sidecar's side of it), `tls` (SPIFFE credentials and verification from kafka-spiffe, ADR 9; `files` sources its own), `admin` (health) and `telemetry` (metrics) packages under `io.github.bpalermo.odradek.server` |
| `client/core/` | The client protocol with no I/O: `handle(event) -> actions`, shared by the JVM clients |
| `client/java/`, `client/clojure/`, `client/python/` | The client libraries; Python has its own port of the core |
| `conformance/scenarios/` | YAML scenarios that every core must pass |
| `extension/java/` | `RecordTransform`, the interface codec extension jars implement |
| `e2e/` | Suites against real brokers, for the server and each client |
| `e2e-client/` | The test application of the in-cluster end-to-end run: one Java program, producer, consumer or both, through the sidecar or with the Kafka clients directly (whose TLS is `//server:tls`), that keeps its own account and latencies of every record and serves them on `GET /results`. Its image is `quay.io/odradek/e2e-client`, published with the server's |
| `deploy/kubernetes/` | Example manifests: a dispatcher, and an application pod that consumes through it. Not built or published; `ExampleManifestsTest` loads the configurations they embed |
| `bazel/` | Build rules and macros shared across packages |

## Things to know before changing code

- **Protos.** Declare a `.proto` with `odradek_proto_library` (`//bazel/proto:defs.bzl`); it generates the Java, Python and Clojure libraries. Nothing generated is checked in. `//api:breaking_test` compares against `api/baseline.binpb`; update the baseline only for a release or a deliberate break (`bazel run //api:update_baseline`).
- **Client behaviour.** Protocol logic belongs in the core, not in a language binding. A change to it needs a scenario in `conformance/scenarios/`, and both cores must pass: `//client/core:conformance_test` and `//client/python:conformance_test`.
- **New message types.** The native image needs reflection metadata for them in `server/native-image/reachability-metadata.json`; `NativeMetadataTest` fails until it is there.
- **Dependencies.** After editing `maven.install` in `MODULE.bazel`, run `REPIN=1 bazel run @maven//:pin`. kafka-spiffe (`io.github.bpalermo:kafka-spiffe`) is taken only as released on Maven Central: a change the server needs from it is made and released there first (ADR 9). gRPC, Netty and protobuf-java have one pin each, in `MODULE.bazel`; the Clojure and Python clients must not bring their own copies (`//client/clojure:classpath_test` checks).
- **Metrics.** Every metric is declared in `ServerMetrics`, or in `KafkaClientMetrics` when it is one of the Kafka clients' own, and listed in `docs/design.md`. Names and attributes are a contract with dashboards: add, do not rename.
- **Version.** One version for everything, in `bazel/version.bzl`. Do not edit it by hand; the release workflow sets it from the tag with `scripts/set-version.sh`.
- **Tests that create topics** must wait for partition leaders before producing (see `ServerHarness.createTopic`); producing sooner can wedge Kafka's idempotent producer for minutes.
- **Tests that start a dispatcher** go through `DispatcherHarness`, which gives each one a coordination group of its own: every suite uses the same brokers, and dispatchers that shared a group would be each other's replicas. Further replicas of one dispatcher come from `replica()`.
- **Dead-letter headers.** The `odradek-*` headers a dispatcher adds to a dead letter (`DeadLetters`) are a contract with whoever reads dead-letter topics, like metric names: add, do not rename.
- **Example manifests.** A change to the configuration schema, a default port, a probe or how long a dispatcher takes to stop has to stay true in `deploy/kubernetes`; `ExampleManifestsTest` (in `//server:main_test`) fails on what it can check. Nothing applies them to a cluster.
- **Design changes.** Update `docs/design.md` in the same change. A decision that constrains future work gets a new ADR in `docs/adr/`, in the format of the existing ones.

## Workflow

- `main` is protected: changes go through a pull request, and the `ci` check must pass.
- Stacked pull requests are managed with the `gh stack` extension.
- Releases are `vX.Y.Z` tags, which only repository admins can create. Do not push one unless asked.
