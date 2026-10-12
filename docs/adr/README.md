# Architecture decision records

Decisions that shape Odradek, one per file, in the format described by Michael Nygard in [Documenting Architecture Decisions](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions): Title, Status, Context, Decision, Consequences.

[`../design.md`](../design.md) describes the system as it is meant to be. The records here say why it is that way, and hold decisions that are not yet in the design.

| # | Decision | Status |
|---|---|---|
| [1](0001-shared-client-protocol-core.md) | Share one client protocol core, checked by conformance scenarios | Accepted |
| [2](0002-clojure-client-on-clj-grpc.md) | Build the Clojure client on clj-grpc and the shared core | Accepted |
| [3](0003-python-client-on-grpcio-asyncio.md) | Build the Python client on grpcio with asyncio | Accepted |
| [4](0004-one-version-for-server-and-clients.md) | Release the server and all clients under one version | Accepted |
| [5](0005-graalvm-native-image-for-the-server.md) | Build a GraalVM native image of the server | Accepted |
| [6](0006-standalone-dispatcher-for-consumption.md) | Consume through a standalone dispatcher that sidecars connect to | Accepted |
