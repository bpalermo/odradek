# 3. Build the Python client on grpcio with asyncio

## Status

Proposed

## Context

We want a client for Python applications. Python cannot use the Java protocol core from [ADR 1](0001-shared-client-protocol-core.md), so the protocol logic has to be written again.

The API is two long-lived bidirectional streams with messages arriving at any time: produce results, record batches, revocations with a deadline. Python's gRPC library, `grpcio`, supports Unix domain sockets and offers a blocking API and an asyncio API. Bidirectional streams with unsolicited inbound messages fit asyncio naturally; with the blocking API they need a thread per stream and hand-written coordination.

Many Python services are still synchronous, and many run under prefork servers such as gunicorn or Celery. A gRPC channel does not survive `fork()`.

## Decision

We will build the Python client on `grpcio` and asyncio.

- The protocol logic is a Python port of the core, with the same event and action model and no I/O. It must pass the conformance scenarios from ADR 1.
- The first public API is asynchronous. A blocking wrapper, which runs the asynchronous client on a background event loop, follows once the asynchronous API is stable.
- Applications pass protobuf message objects for keys and values. The client serializes them as the topic's contract type.
- The client detects that it is being used in a process other than the one that created it and fails with a clear error. It is created after forking, never before.
- The client is built and tested with Bazel in this repository and published to PyPI as a pure Python wheel that contains the generated API code.

## Consequences

Asynchronous Python services get a client that fits them directly. Synchronous services wait for the wrapper, or run an event loop themselves.

The protocol logic exists in a second language. The conformance scenarios are what keep it equal to the Java core; a behaviour the scenarios do not cover can differ without anyone noticing. Scenario coverage therefore matters more than it would with one implementation.

The repository gains a Python toolchain, a locked set of Python dependencies and a Python end-to-end job.

Two things are unverified and must be checked before work starts: that the Python protobuf runtime matching our compiler loads an Edition 2024 file, and how `grpcio` behaves on a Unix socket across server restarts.

`grpcio` is a native extension. The wheel we publish is pure Python, but applications install a platform-specific `grpcio`, which limits the client to the platforms `grpcio` supports.
