# Odradek — Kafka client sidecar: design and implementation plan

Odradek moves the Kafka client out of the application process into a sidecar
container in the same pod. The app talks to it over gRPC on a Unix domain
socket. The design goal is that an app using the sidecar keeps the guarantees
it would have with an in-process client; where it cannot, the difference is
named explicitly.

The API is defined in [`api/odradek/kafka/v1/sidecar.proto`](../api/odradek/kafka/v1/sidecar.proto).

## 1. Scope

In scope for v1:

- Produce with broker-ack semantics, per-stream ordering, dedup of app resends, bounded buffering.
- Consume with consumer groups, app-driven offset commits, rebalance propagation, hung-app detection.
- Several named Kafka clusters behind one sidecar; each stream is bound to one cluster.
- One Java sidecar binary and a thin Java client library.

Deferred:

- Transactions / exactly-once (section 7). The design leaves room for them; nothing in v1 is built on them.
- Client libraries for other languages.
- Shared-memory transport. Broker acks dominate latency; revisit only if profiles show IPC cost.

## 2. Components

```mermaid
flowchart LR
  subgraph pod["Pod"]
    subgraph app["App container"]
      code["App code"]
      lib["Odradek client library"]
      code --> lib
    end
    sock(["/var/run/kafka/kafka.sock<br/>emptyDir, medium: Memory"])
    subgraph sidecar["Sidecar container (native sidecar)"]
      grpc["gRPC server<br/>Netty + epoll"]
      ps["ProduceSession"]
      cs["ConsumeSession"]
      prod["KafkaProducer<br/>one per cluster"]
      cons["KafkaConsumer<br/>one per Consume stream"]
      admin["Admin HTTP<br/>health, metrics"]
      grpc --> ps --> prod
      grpc --> cs --> cons
    end
    lib -- "gRPC over UDS" --> sock --> grpc
  end
  cfg[("Config volume<br/>/etc/odradek")] -. "inotify" .-> sidecar
  spire["SPIRE agent<br/>Workload API"] -. "X.509 SVID watch" .-> sidecar
  kubelet["kubelet"] -. "probes" .-> admin
  prod --> brokers[("Kafka clusters")]
  cons --> brokers
```

| Component | Responsibility |
|---|---|
| `//api` | Proto contract. The only thing other languages need. |
| `//sidecar` | Owns all Kafka clients. Enforces the guarantee-bearing Kafka settings. |
| `//client/java` | Hides streams, `seq`, credits, reconnects. Exposes `send() -> CompletableFuture<RecordMetadata>` and a poll/handler consumer API. |

### Sidecar internals

- **Transport**: grpc-java on unshaded Netty (`grpc-netty`, never `grpc-netty-shaded`) with the epoll native transport (`netty-transport-native-epoll`, `linux-x86_64` and `linux-aarch_64`), bound to `/var/run/kafka/kafka.sock`, mode `0660`. Peer UID/GID checked with `SO_PEERCRED` against an allow-list.
- **Admin endpoint**: a small HTTP server on a TCP port for `startupProbe` / `livenessProbe` and Prometheus metrics. Kubelet gRPC probes cannot target a Unix socket, so probes use HTTP.
- **Threading**:
  - One shared `KafkaProducer` (thread-safe) per cluster. Each Produce stream calls `send()` from its own serialized gRPC callback, with manual inbound flow control (`request(1)`), so arrival order equals `send()` order.
  - One `KafkaConsumer` per Consume stream, owned by a dedicated poll thread. gRPC control messages (`Credits`, `Ack`, `RevokeDone`) are queued to that thread; nothing else touches the consumer.
- **Configuration**: read from a mounted volume and hot-reloaded; see below.

### Multiple clusters

The sidecar serves a set of named clusters. A stream selects one in its handshake (`ProduceOpen.cluster`, `Subscribe.cluster`); empty means the configured default. The cluster is never chosen per record: mixing clusters inside one stream would put them in one ordering domain and one window, and a slow cluster would block records bound for a healthy one.

```yaml
# /etc/odradek/odradek.yaml
defaultCluster: orders
clusters:
  orders:
    required: true
    properties:
      bootstrap.servers: orders-kafka:9093
    tls:
      source: spiffe
      acceptedIds: ["spiffe://example.org/ns/kafka/sa/orders-broker"]
  analytics:
    required: false
    properties:
      bootstrap.servers: analytics-kafka:9093
    tls:
      source: files
      dir: /etc/odradek/tls/analytics
```

- **Isolation**: each cluster has its own producer, `buffer.memory`, in-flight window and consumers. A cluster that is down stalls only the streams bound to it.
- **Memory**: producer buffers and windows are per cluster, so the container memory limit scales with the number of clusters.
- **Startup**: `required: true` clusters gate the `startupProbe`. Optional clusters connect on first use.
- **Errors**: an unknown cluster name fails the stream with `NOT_FOUND`; a configured but unreachable one with `UNAVAILABLE`.
- **Enforced settings**: validated per cluster. One invalid cluster makes the whole configuration invalid, so a reload is all-or-nothing.
- **Credentials**: each cluster has its own credential source, trust bundle and accepted broker IDs. Clusters using `spiffe` share the pod's SVID and one Workload API watch; federated bundles cover brokers in another trust domain.
- **Metrics and health**: every series carries a `cluster` label; the admin endpoint reports status per cluster.
- **Hot reload**: the diff is per cluster. Only changed clusters get a producer swap or consumer rebuild. A cluster can be added at runtime. Removing one drains its streams (flush, commit) and then closes them with `UNAVAILABLE`.
- **Cross-cluster flows**: consuming from one cluster and producing to another works by opening one stream on each, at-least-once. A transaction cannot span clusters.

### Configuration and hot reload

**Source**

- All configuration lives in a directory on a mounted volume (ConfigMap, Secret or projected), default `/etc/odradek`. The only process-level inputs are the config directory path and the JVM flags; there are no environment overrides for Kafka settings, because they could not be reloaded.
- Files referenced by the configuration (truststores, keystores, SASL credential files) are part of the watched set.
- The sidecar enforces the settings the guarantees depend on: `acks=all`, `enable.idempotence=true`, `max.in.flight.requests.per.connection<=5`, `enable.auto.commit=false`, `partition.assignment.strategy=cooperative-sticky` (classic group protocol). A configuration that contradicts them is invalid. The app cannot change them over the API.

**Watching**

- `java.nio.file.WatchService`, which is inotify on Linux, on the config directory and on the parent directory of every referenced file.
- Kubernetes updates a volume by swapping the `..data` symlink to a new timestamped directory; the files themselves are never modified in place. The watcher therefore watches directories and treats the `..data` create/move event as "everything may have changed". Watching an individual file would miss updates.
- Volumes mounted with `subPath` never receive updates. The deployment docs and the example pod spec must not use it.
- Events are debounced (a few hundred ms), then the whole configuration is re-read and compared by content hash. An inotify `OVERFLOW` forces a re-read. A slow periodic re-hash is the safety net for missed events.

**Applying a change**

1. Parse and validate the complete new configuration, including the enforced settings.
2. If invalid: keep running on the last good configuration, log the reason, increment `config_reload_failures_total`. Liveness is not affected.
3. If valid: diff against the running configuration and apply per the table below, then publish the new content hash and reload timestamp on the admin endpoint and as metrics.

At startup there is no last good configuration, so an invalid or missing one fails the `startupProbe`.

| What changed | How it is applied | Visible to the app |
|---|---|---|
| Sidecar tunables: default/max `max_processing_ms`, revoke deadline, commit interval, peer allow-list, log level | In place | No |
| Produce window sizes | New streams only (the window is advertised once per stream) | No |
| mTLS key material and trust bundle | In-memory swap inside the running clients (see "mTLS credential rotation") | No |
| Kafka producer properties, SASL credentials | Producer swap (below) | Latency blip only |
| Kafka consumer properties, SASL credentials | Consumer rebuild (below) | `Revoked` then `Assigned` |
| Socket path, admin port | Not reloadable; logged and flagged in `config_restart_required` | No, until restart |

**Producer swap**

Kafka clients cannot change properties after construction, so a change means a new client. Two producers writing to one partition at the same time would break ordering, so the swap is gated:

1. Stop reading from all Produce streams (withhold `request(1)`).
2. `flush()` the old producer; every in-flight record gets its result as usual.
3. Create the new producer. If construction fails, resume on the old one and count a reload failure.
4. Resume the streams on the new producer; close the old one.

The session `seq` tables live above the producer, so `sidecar_epoch` does not change and no send becomes *outcome unknown*. The app sees backpressure through the window for the duration of the flush.

**Consumer rebuild**

1. Run the normal revoke handshake for all partitions of the stream (`Revoked` → final `Ack`s → `RevokeDone`, bounded).
2. `commitSync`, close the consumer.
3. Create the new consumer and subscribe; the app receives `Assigned` with new `assignment_epoch`s.

- The Consume stream stays open throughout.
- A fleet-wide ConfigMap change reaches every pod within about a kubelet sync period, so rebuilds start after a random jitter to avoid a rebalance storm.
- With static membership (`group.instance.id`) and a rejoin inside the session timeout, the rebuild causes no group rebalance.

### mTLS credential rotation

Certificates must not go through the producer swap / consumer rebuild path. SPIRE SVIDs are short-lived (rotated around half of a default one-hour TTL), and a consumer rebuild on every rotation would mean a rebalance every half hour.

The reference is [java-spiffe](https://github.com/spiffe/java-spiffe) and its `java-spiffe-helper`: the helper holds a Workload API watch and, on every X.509 context update, rewrites a keystore and truststore. The sidecar follows the same model — one long-lived watch, an update callback, atomic replacement of key material and bundle together — but replaces the material in memory instead of on disk.

**Mechanism**

- Kafka clients accept a custom TLS engine factory through `ssl.engine.factory.class` (KIP-519). The sidecar sets it to its own `ReloadingSslEngineFactory` on every client it creates.
- The factory reads from a `CredentialSource` that always holds the current `{private key, certificate chain, trust bundle}` as one immutable snapshot.
- Each new TLS handshake builds its engine from the current snapshot. Established connections are left alone; TLS does not re-validate a certificate on a live connection, and connections pick up the new SVID as they are naturally re-established.
- The Kafka clients are never rebuilt for a rotation, so there is no flush, no rebalance, and nothing visible to the app.

**Credential sources**

| Source | How it updates | When to use |
|---|---|---|
| `spiffe` (default for mTLS) | `java-spiffe-core` `X509Source` watching the SPIRE agent's Workload API socket; update callback swaps the snapshot | Clusters with SPIRE. The private key never touches disk. |
| `files` | PEM or PKCS12 files on a volume, written by `java-spiffe-helper`, `spiffe-helper`, cert-manager or similar; the directory watcher above detects the change, the new material is parsed and validated, then swapped | No Workload API access, or non-SPIFFE PKI |

- A snapshot that fails validation (key does not match the leaf, chain does not verify against the bundle, already expired) is rejected; the previous one stays in use and `credential_reload_failures_total` increments.
- Metrics: SVID `not_after` timestamp, last successful rotation, rotation count. Alert on time-to-expiry, not on rotation events.

**Peer verification**

- X.509-SVIDs carry the identity in a URI SAN and usually no DNS SAN, so hostname verification is disabled (`ssl.endpoint.identification.algorithm=`) and replaced by SPIFFE verification in the factory's trust manager: the broker certificate must chain to the trust domain's bundle and its SPIFFE ID must be in the configured allow-list (exact IDs or a trust domain).
- Broker side, outside this repo: brokers need a `principal.builder.class` that maps the client's SPIFFE ID to a Kafka principal for ACLs.

**Deployment**

- The SPIRE agent socket is mounted into the sidecar container only (CSI driver or hostPath). The app container never sees Kafka credentials.
- At startup the sidecar blocks, bounded by the `startupProbe`, until the first SVID is received.

## 3. Lifecycle

- The sidecar is a native sidecar: an `initContainer` with `restartPolicy: Always`.
- **Startup**: `startupProbe` passes once the socket is listening and broker metadata has been fetched for every required cluster. The app container starts after that.
- **App readiness**: the client library reports ready only while it holds a session whose handshake has completed.
- **Shutdown**: the sidecar receives SIGTERM after the app container exits. It stops accepting streams, completes in-flight produce results, `flush()`es, commits acked offsets, leaves the group, and exits. `terminationGracePeriodSeconds` must cover app drain plus `delivery.timeout.ms`.
- **Sidecar restart with the app alive**: the app reconnects, sees a new `sidecar_epoch`, and fails every unacknowledged send with *outcome unknown*. Consumers get a fresh assignment; unacked records are redelivered.
- **App restart with the sidecar alive**: produce sessions idle out; consume streams close, which triggers commit of acked offsets and a group leave.

## 4. Produce path

### Session and dedup

- The app picks a `session_id` once per process and numbers records with a strictly increasing `seq`.
- The sidecar generates a `sidecar_epoch` at start and returns it in `ProduceReady`.
- Per `(cluster, session_id)` the sidecar keeps a table `seq -> {in-flight | result}`:
  - A record whose `seq` is already in the table is not sent again; the stream is attached to the existing outcome.
  - Entries below the app's `low_watermark` are dropped. Because the window bounds unacknowledged records, the table is bounded too.
- After a stream reconnect:
  - Same `sidecar_epoch` → the library resends everything without a result, with the original `seq`. No duplicates are produced.
  - Different `sidecar_epoch` → the table is gone. The library fails pending sends with `OUTCOME_UNKNOWN`. Whether to resend (accepting possible duplicates) is the app's call, as it is after a Kafka timeout in-process.

### Acknowledgement

A `ProduceResult` is written only from the Kafka producer's delivery callback. The sidecar never acknowledges on enqueue. A sidecar crash between broker ack and result delivery surfaces as *outcome unknown*, never as a false success.

### Ordering

- One Produce stream is one ordering domain. Records are passed to `KafkaProducer.send()` in arrival order by a single serialized callback.
- The idempotent producer preserves per-partition order across its own retries.
- Results may complete out of `seq` order across partitions; within a partition they complete in order.
- As in-process, a failed record does not block later records to the same partition. Apps needing stop-on-first-error handle it in the client library (fail-fast option, phase 2).

### Backpressure

- `ProduceReady` advertises `max_in_flight_records` and `max_in_flight_bytes`. The library blocks (or fails after a configurable `max.block` equivalent) when the window is full. Each result returns a slot.
- The window is sized below the producer's `buffer.memory`, so `send()` in the sidecar does not block on buffer exhaustion. gRPC/HTTP-2 flow control is the backstop.
- A record that exceeds the window is rejected with `PROTOCOL_VIOLATION`.

## 5. Consume path

### Delivery and flow control

- The app grants `Credits`. The sidecar delivers at most that many records and pauses all partitions when credits run out, while continuing to `poll()` to stay in the group.
- Records are delivered as per-partition `RecordBatch`es tagged with the partition's `assignment_epoch`.

### Commit after processing

- Auto-commit is off. The app sends `Ack` ranges per partition.
- Per partition the sidecar tracks delivered and acked offsets and commits `highest contiguous acked offset + 1`: asynchronously on an interval, synchronously on revoke and on close.
- Acks carrying a stale `assignment_epoch` are ignored.
- Result: at-least-once, with redelivery limited to records that were delivered but not acked.

### Liveness

In-process, `max.poll.interval.ms` evicts a consumer whose processing thread is stuck. Here the sidecar keeps polling regardless, so it enforces the equivalent itself:

- If the oldest delivered-but-unacked record is older than `max_processing_ms`, the app is considered hung.
- The sidecar commits what was acked, leaves the group, and closes the stream with `ABORTED`. The library surfaces this and fails app readiness.
- `max.poll.interval.ms` on the real consumer is set above `max_processing_ms` plus the revoke deadline so the sidecar's own rule always fires first.

### Rebalance

With the cooperative-sticky assignor, `onPartitionsRevoked` runs on the poll thread inside `poll()`:

1. Stop delivering records for the revoked partitions.
2. Send `Revoked{revoke_id, partitions, deadline_ms}`.
3. Wait, bounded by `deadline_ms`, for `RevokeDone` while still processing incoming `Ack`s.
4. `commitSync` the contiguous acked offsets for those partitions.
5. Return from the callback.

- `deadline_ms` is kept well under the rebalance timeout.
- `onPartitionsLost` sends `Lost`; nothing is committed.
- `onPartitionsAssigned` sends `Assigned` with a new `assignment_epoch` per partition.

Open point: the KIP-848 consumer protocol (`group.protocol=consumer`) changes assignor configuration but keeps the callback contract. v1 targets the classic protocol; the session code keeps the assignor choice in configuration so the switch is not structural.

## 6. Guarantee summary

| In-process guarantee | With the sidecar |
|---|---|
| `send()` success means broker-acked | Preserved. Result comes only from the delivery callback. |
| Per-partition ordering | Preserved per Produce stream. |
| No duplicates from client retries | Preserved while the sidecar stays up. A sidecar restart yields *outcome unknown*; resending may duplicate. |
| Bounded producer memory | Preserved via the in-flight window. |
| Hung consumer is evicted | Preserved via `max_processing_ms`. |
| Commit only processed offsets | Preserved; contiguous-ack rule. |
| Rebalance callbacks | Preserved as `Revoked` / `RevokeDone` with a deadline. |
| Transactions / EOS | Not in v1. |

## 7. Transactions (deferred design)

- A transactional session is a dedicated Produce stream bound to its own `KafkaProducer` with a stable `transactional.id` per pod (StatefulSet ordinal or configured id) for zombie fencing.
- `Begin`, `SendOffsets`, `Commit`, `Abort` travel in-band on that stream rather than as unary RPCs, so their order relative to produced records is unambiguous.
- `SendOffsets` references a Consume stream in the same sidecar and on the same cluster; the sidecar supplies the consumer group metadata and rejects a cross-cluster reference.
- A stream drop mid-transaction aborts it.

## 8. Testing strategy

| Level | What | How |
|---|---|---|
| Unit | Session tables, contiguous-ack tracker, credit accounting, config enforcement | Plain JUnit; Kafka's `MockProducer` / `MockConsumer` |
| Component | Full gRPC stream behaviour over a real UDS against mock Kafka clients | In-process sidecar, real client library |
| Integration | Real broker: ordering, commit positions, rebalance with two sidecars | Testcontainers Kafka (KRaft) |
| Fault | Kill the sidecar mid-produce and mid-revoke; hang the app; drop the stream | Integration harness; assert no false acks and no committed-but-unprocessed offsets |
| Reload | ConfigMap-style `..data` symlink swap; invalid config keeps last good; producer swap under load keeps order and produces no duplicates; consumer rebuild commits before closing | Component and integration harness |
| Soak | Rolling restarts of app and sidecar under load in a cluster | Later, with the deployment work |

## 9. Implementation plan

Each phase ends with something runnable and tested.

**Phase 0 — Repository bootstrap**
- Git repo, Bazel module, hermetic JDK, proto and gRPC codegen for `//api`, Maven pinning, format/lint, skeleton `//sidecar` and `//client/java`.

**Phase 1 — Sidecar skeleton**
- Cluster registry: named clusters, default cluster, required/optional startup gating, per-cluster status.
- Configuration loading from the volume, validation and enforcement of the guarantee-bearing Kafka settings.
- Directory watcher (`..data` swap aware), debounce, last-good fallback, reload metrics. The producer swap and consumer rebuild hooks land with phases 2 and 3.
- `CredentialSource` (`spiffe` and `files`) and `ReloadingSslEngineFactory`; rotation test against a fake Workload API.
- gRPC server on the Unix socket with peer-credential check.
- Admin HTTP: startup/liveness endpoints, Prometheus metrics.
- Graceful shutdown sequence.
- Exit: the sidecar starts, serves an empty `OdradekService`, passes probes, shuts down cleanly.

**Phase 2 — Produce**
- `ProduceSession`: handshake, seq table, low-watermark pruning, window enforcement, delivery-callback results, error mapping.
- Java client: `OdradekProducer.send()`, window blocking, reconnect and resend, epoch-change handling.
- Unit, component and broker integration tests; fault test for sidecar kill mid-flight.
- Exit: no false acks and no duplicates across stream reconnects, demonstrated by tests.

**Phase 3 — Consume**
- `ConsumeSession`: poll thread, credits and pause/resume, per-partition ack tracker, async and sync commits.
- Rebalance listener with the revoke handshake; `Lost` handling.
- Processing-deadline watchdog.
- Java client: handler-based and poll-based consumer APIs, automatic acks after handler success, revoke handling.
- Exit: two-sidecar rebalance test shows no committed-but-unprocessed offsets; hung-app test shows eviction.

**Phase 4 — Packaging and deployment**
- Container image, example pod spec (native sidecar, shared `emptyDir`, probes, grace period).
- Dashboards and alerts on the sidecar metrics.
- Soak test with rolling restarts.

**Phase 5 — Transactions**
- Per section 7, only if a consumer of the sidecar needs exactly-once.

**Phase 6 — More client languages**
- Driven by the first non-Java app.

## 10. Open decisions

- **Java package root**: `io.github.bpalermo.odradek` is a placeholder; change it before anything is published.
- **Consumer group protocol**: classic + cooperative-sticky for v1, or go straight to KIP-848.
- **Serialization**: the API carries bytes. Schema-registry integration, if wanted, belongs in the client library.
