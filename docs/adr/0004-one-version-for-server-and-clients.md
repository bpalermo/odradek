# 4. Release the server and all clients under one version

## Status

Proposed

## Context

Odradek will ship several artifacts from one repository: the server image, the Java client, the extension interface for codec transforms, the Clojure client and the Python client. They go to different registries: Quay, Maven Central, Clojars and PyPI.

Today only the server image is published, on every commit to `main`, tagged `dev`. Nothing has a version.

The artifacts meet at the gRPC API. A server and a client work together when they agree on the protocol, not when their version numbers match. Still, someone debugging a problem has to work out which client was talking to which server, and whether that combination is expected to work.

The options are one version for everything in the repository, or an independent version per artifact.

## Decision

We will release every artifact from one Git tag, under one version number.

- A tag `vX.Y.Z` publishes the server image, the Java artifacts, the Clojure client and the Python client, all as version `X.Y.Z`.
- An artifact that did not change is published again under the new number.
- The version follows semantic versioning of the gRPC API and the configuration schema together: a breaking change to either is a new major version.
- A client and a server with the same major version are compatible. The server reports the client version it received in the handshake ([ADR 1](0001-shared-client-protocol-core.md)).
- The image built from `main` between releases keeps the `dev` tags.

## Consequences

Compatibility is easy to state and to check: same major version. One changelog covers everything.

Version numbers move even for artifacts with no changes. A Python-only fix produces a new server image with identical content, and users who track versions see an upgrade that does nothing for them.

A breaking change in one client's own API forces a major version on everything. This pushes client APIs towards stability, and makes experimental client APIs expensive; a client that needs to move faster would need a record superseding this one.

Releasing becomes one workflow with four publishing targets and their credentials. A failure halfway leaves some registries with the new version and some without, so each publishing step must be safe to run again.
