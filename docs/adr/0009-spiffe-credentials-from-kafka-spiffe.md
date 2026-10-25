# 9. Take SPIFFE credentials and peer verification from kafka-spiffe

## Status

Accepted

## Context

The server speaks mutual TLS in two places: to Kafka, for a cluster that configures `tls`, and on the delivery port between a dispatcher and its sidecars (ADR 6), in either role. Each takes its credentials from a `spiffe` source (X.509-SVIDs and bundles from the SPIFFE Workload API) or a `files` source (PEM files that are read again when they change).

For the `spiffe` source the server carries its own SPIFFE implementation, in `server/tls`:

- `WorkloadApi` and `WorkloadApiSource`: a `FetchX509SVID` client over grpc-java, written out by hand "so the server needs no SPIFFE library and the native image no extra metadata", one stream per socket.
- `PeerTrust`: the peer's chain verified against the bundle of the trust domain its SPIFFE ID names, and the ID matched against `accepted_spiffe_ids` and `accepted_trust_domain`.
- `ReloadingSslEngineFactory` for the Kafka clients and `ReloadingSslContext` for the delivery port, each building a JDK context per set of credentials.

[kafka-spiffe](https://github.com/bpalermo/kafka-spiffe) (`io.github.bpalermo:kafka-spiffe`, by the same author, published on Maven Central) does the same job as a library: a Workload API source shared per identity and process (`X509SourceRegistry`), per-trust-domain verification (`SpiffeTrustManager`), a JDK context per snapshot (`SnapshotSslContext`), and Kafka's `SslEngineFactory` over them (`SpiffeSslEngineFactory`). It has no dependency of its own (`kafka-clients` and `slf4j-api` are provided), and it is tested further than the server's copy: on every Kafka version from 3.0 to 4.4, through rotations of SVIDs and roots with no new connection, in in-process KRaft clusters with `StandardAuthorizer`, and end to end against two federated SPIRE servers and the `apache/kafka` images.

Two implementations of the same protocol and checks, by the same author, drift. A fix to either, of a parsing edge or a rotation race, has to be found and made twice.

## Decision

We will take SPIFFE credentials and peer verification from kafka-spiffe, from version 0.2.0, and keep only the `files` source as the server's own.

### One source per identity

- Every `spiffe` source, a cluster's and both roles of the delivery port, is a handle on the library's shared source: `X509SourceRegistry.shared(null).acquire(X509Source.Settings.of(socket))`, with the socket of `SpiffeSource.workload_api_socket`, or `SPIFFE_ENDPOINT_SOCKET` when it is unset. That is the source `SpiffeConfig.acquireSource()` names for the keys the Kafka clients are given below (the socket, no SVID selection, the MBeans off), so the clients' engine factories and the server read one source, and a process keeps one Workload API stream per identity, whatever reads it.
- A `CredentialSource` adapts the handle: each snapshot (`X509Context`) becomes `Credentials`, the key, the chain and a bundle per trust domain. The server still waits up to `FIRST_SVID_TIMEOUT` for the first one at startup, and an expired certificate still makes the health service answer `NOT_SERVING`.
- `WorkloadApi` and `WorkloadApiSource` are deleted. The test `FakeWorkloadApi` stays, to serve the library in tests.

### Kafka clients

- A cluster whose `tls` has a `spiffe` source gets `ssl.engine.factory.class=io.github.bpalermo.kafka.spiffe.kafka.SpiffeSslEngineFactory` and the library's keys, from fields that do not change:

  | Configuration | Library key |
  |---|---|
  | `SpiffeSource.workload_api_socket`, or `SPIFFE_ENDPOINT_SOCKET` | `spiffe.endpoint.socket` |
  | `Tls.accepted_spiffe_ids` | `spiffe.peer.allowed.ids` |
  | `Tls.accepted_trust_domain` | `spiffe.peer.allowed.trust.domains` |
  | (none) | `spiffe.metrics.jmx.enabled=false` |

- A cluster with a `files` source keeps `ReloadingSslEngineFactory`. The e2e-client's direct Kafka clients get the same settings through `//server:tls`.

### The delivery port

- With a `spiffe` source, the port's `JdkSslContext` wraps the library's `SnapshotSslContext` of each snapshot, and its peers are verified by the library's `SpiffeTrustManager`: the chain against the bundle of the peer's trust domain, the ID against what the section accepts (`Tls.accepted_spiffe_ids` and `accepted_trust_domain` on `dispatcher.tls`; on `dispatch.tls`, any SPIFFE ID of a trust domain with a bundle, `SpiffeIdMatcher.none().withTrustDomains(...)` of the snapshot's bundles, since what a sidecar may do is decided after the handshake). An observer (`SpiffeTrustManager.Observer`, public in 0.2.0) counts what it accepts and refuses.
- `PeerTrust` stays for `files` sources only, with its rule that a certificate without a SPIFFE ID completes the handshake and is then refused everything.
- The sidecar rules of `Dispatch.policy`, which authorize by SPIFFE ID after the handshake, are not touched.

### Metrics

- `odradek.tls.certificate.not_after`, `odradek.tls.credential.reloads` and `odradek.tls.credential.reload.failures` keep their names, attributes and meaning: they are a contract with dashboards. For a `spiffe` source their values come from the library: the expiry of the current snapshot, and the counts of the source's statistics (`X509MaterialSource.stats()`, new in 0.2.0: `Stats(connected, updates, rejectedUpdates, streamErrors, lastUpdate)`). Reloads are `updates`, the first included; failures are `rejectedUpdates` plus `streamErrors`, as the server's own source counted them.
- A new counter, `odradek.tls.peer.verifications`, with `side` (the role the server took in the handshake: `client` when it verified a server, `server` when it verified a client), `result` (`accepted`, `rejected`) and, for a refusal, `reason` (the library's refusal reasons: `malformed-svid`, `unknown-trust-domain`, `chain`, `id-not-allowed`). For the Kafka clients it reads the library's peer-verification counts (`PeerVerificationStats.of(null)`, new in 0.2.0: `client()` and `server()`, each a `Side(accepted, rejected by reason)`), which are per process: every cluster shares the source and the count, so their series carry no `cluster`. For the delivery port it is counted by the observer, per section (`delivery`).
- The library's own MBeans are off. The server reports in OpenTelemetry, and the native image need not support JMX.

## Consequences

The server loses its Workload API client and its SPIFFE checks, and with them a hand-written wire format, a stream per socket that is not shared with Kafka's, and a verification path that only the server's tests exercise. What the library fixes or adds, the server has on its next version bump.

The configuration does not change, nor the example manifests. A `files` source behaves as before.

The library's choices become the server's for `spiffe` sources: TLS sessions time out a second after they are made, the shortest the JDK allows (both session caches; zero would mean no limit), which is not the same as never resuming: a connection made again within that second can resume a session and skip the certificate checks, so a certificate that expired in that second can still be used for one more connection. Kafka's connections are long-lived and the window is a second, so the server accepts it; preventing resumption outright would be a change in the library first. And an expired SVID is still presented until a new one arrives (`spiffe.svid.expired=present`, the library's default), which leaves the refusal to the peer.

The server depends on a 0.x library. Both are the same author's, and the server takes a version only once it is released, so a change the server needs is made in the library first: 0.2.0 exists for the `stats()` and peer-verification APIs above, and for declaring the classes the server uses (`SpiffeConfig`, `X509SourceRegistry`, `X509MaterialSource`, `X509Source.Settings`, `X509Context`, `SnapshotSslContext`, `SpiffeTrustManager` and its `Observer`, `PeerVerificationStats`) part of the library's API.

The server's Kafka clients are 4.3.1, between the 4.0 and 4.4 the library's matrix tests. Everything the library calls has the same shape from 3.0 to 4.4.

The native image needs reachability metadata for `SpiffeSslEngineFactory`, which Kafka creates by class name; `NativeMetadataTest` fails until it is there. With the MBeans off, nothing in the library needs JMX.

The end-to-end test against the mutual TLS broker (`e2e/tls/broker.sh`, whose broker certificate carries `spiffe://e2e.test/broker`) already produces and consumes with an SVID served by `FakeWorkloadApi` (`MtlsE2eTest`); from the first change on it does so through the library's source, and from the second through its engine factory, in CI: that test is the coverage, and no other is added. The in-cluster run proves it against real SPIRE.

Work that follows from this record, as a stack after kafka-spiffe 0.2.0:

1. The shared source: the dependency, the `CredentialSource` over the library's handle for every `spiffe` source, the `odradek.tls.*` metrics and readiness from its statistics; `WorkloadApi` and `WorkloadApiSource` deleted.
2. The Kafka clients on `SpiffeSslEngineFactory`, the native-image metadata, and a test that the factory and the server share one source.
3. The delivery port on `SnapshotSslContext` and `SpiffeTrustManager` for `spiffe` sources.
4. `odradek.tls.peer.verifications`, and `docs/design.md` (TLS, the delivery port, the metrics table).

Three alternatives were set aside:

- **Keep the server's implementation.** It works, and depends on nothing. It also duplicates a library its author maintains and tests more thoroughly, and every fix would be made twice.
- **Only the Kafka factory.** The clients would use the library while the server kept its own source for readiness, metrics and the delivery port: two Workload API streams per process, and the readiness and metrics of a cluster describing a stream its clients do not use.
- **Only the source.** The server's own engine factory and `PeerTrust` over the library's source. One stream, but the Kafka path the library tests across versions would not be the one the server runs, and `PeerTrust` would stay a second implementation of the library's checks.
