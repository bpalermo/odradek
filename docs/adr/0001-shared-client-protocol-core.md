# 1. Share one client protocol core, checked by conformance scenarios

## Status

Proposed

## Context

Applications reach the Odradek server through a client library. The gRPC API is two bidirectional streams, and the guarantees in the design depend on the client following a protocol on top of them:

- On produce: assigning sequence numbers, staying inside the in-flight window, advancing the low watermark, resending unacknowledged records after a reconnect, and failing them as *outcome unknown* when the server epoch changed.
- On consume: granting credits, acknowledging offset ranges against the right assignment epoch, and completing the revoke handshake before its deadline.

A mistake in this logic breaks a guarantee silently: a duplicate, a lost acknowledgement, a commit past unprocessed records. We want clients in Java, Clojure and Python. None exists yet beyond a placeholder. Written independently, the three would each carry their own version of the same subtle logic and would drift.

Java and Clojure share the JVM. Python shares nothing with them at run time.

## Decision

We will write the client protocol once, as a core with no I/O, and bind it to a transport in each client.

- The core is a plain Java library. It consumes events (an application call, a message received, a stream broken, a timer fired) and returns actions (send this message, complete this result, fail these sends). It does not depend on gRPC, threads or clocks.
- The Java client binds the core to grpc-java. The Clojure client binds the same core to clj-grpc ([ADR 2](0002-clojure-client-on-clj-grpc.md)).
- The Python client is a port of the core ([ADR 3](0003-python-client-on-grpcio-asyncio.md)).
- We will describe the expected behaviour as conformance scenarios: language-neutral files, each a sequence of events with the actions expected in response. Every implementation of the core must pass all of them.
- Each client also runs the same end-to-end test list against real brokers.
- Both stream handshakes will carry the client's language and version, so the server can report them in metrics and logs.

## Consequences

The protocol logic exists twice instead of three times, and the two copies are held to one executable definition. A protocol change starts as a scenario change, which makes the change visible to every implementation.

A core without I/O can be tested exhaustively and deterministically, including reconnects and timeouts, without a server or a broker.

The core's event and action model is a design constraint on the Java client: it cannot use gRPC conveniences that hide stream state. The bindings stay thin, but they own threading and timers, so concurrency bugs remain possible there and are not covered by the scenarios.

The scenarios are a format and a runner that we have to build and maintain, in two languages.

The handshake messages gain two fields. This is a compatible change to the API, made before anything is released.

The server's produce and consume paths have to exist before any client can be tested end to end, so they come first.
