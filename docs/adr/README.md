# Architecture decision records

Decisions that shape Odradek, one per file, in the format described by Michael Nygard in [Documenting Architecture Decisions](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions). The process is in [ADR 1](0001-record-architecture-decisions.md).

[`../design.md`](../design.md) describes the system as it is meant to be. The records here say why it is that way, and hold decisions that are not yet in the design.

| # | Decision | Status |
|---|---|---|
| [1](0001-record-architecture-decisions.md) | Record architecture decisions | Accepted |
| [2](0002-shared-client-protocol-core.md) | Share one client protocol core, checked by conformance scenarios | Proposed |
| [3](0003-clojure-client-on-clj-grpc.md) | Build the Clojure client on clj-grpc and the shared core | Proposed |
| [4](0004-python-client-on-grpcio-asyncio.md) | Build the Python client on grpcio with asyncio | Proposed |
| [5](0005-one-version-for-server-and-clients.md) | Release the server and all clients under one version | Proposed |
