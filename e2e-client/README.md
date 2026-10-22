# e2e-client

The test application of Odradek's in-cluster end-to-end run. One program with three modes (producer, consumer, or both in one process) and two transports: on the Java client (`//client/java`) it runs as the app container of a pod whose Odradek server is a native sidecar and talks to it over the Unix socket; with `--transport kafka` it uses the Kafka clients itself, configured as the server configures its own, so that the two can be compared. Either way it keeps its own account of what happened to every record, and times it. A run is graded from that account, served on `GET /results`, cross-checked against the server's metrics.

Image: `quay.io/odradek/e2e-client` (linux/amd64 and linux/arm64, JVM, on the server's distroless Java 25 base). Tags as for the server: `dev` and `dev-<commit>` from every commit on `main`, `X.Y.Z` from a release, all signed. Entrypoint: the program; flags are its arguments.

| Task | Command |
| --- | --- |
| Run it | `bazel run //e2e-client -- --mode producer --topic t --socket /var/run/kafka/kafka.sock` |
| Run it without a sidecar | `bazel run //e2e-client -- --mode both --transport kafka --bootstrap-servers localhost:9092 --topic t` |
| Unit tests | `bazel test //e2e-client:test` |
| Every mode and transport against real brokers | `E2eClientE2eTest` in `//e2e:e2e_test` (`make e2e`), the mutual TLS broker included |
| Build the image | `bazel build //e2e-client:image_index` |
| Load it into Docker | `bazel run //e2e-client:image_load` (as `localhost/odradek-e2e-client:e2e`) |

## What it checks

**Producer mode** sends records to a topic at a rate R (or a schedule of rates, `--rate-schedule`), round robin over K keys (`key-0` … `key-<K-1>`), for a count N or a duration, whichever ends first, or until stopped. A record of a key is sent only once the previous record of that key has its outcome: that is what Odradek's ordering guarantee covers (records of one stream reach each partition in order), so a consumer may hold the key's records to strict order. The rate is a target: with every key waiting on an acknowledgement it is at most K divided by the acknowledgement latency.

Each record's value is JSON, its Kafka key the key:

```json
{"runId":"3f0c…","key":"key-3","seq":12,"producerTs":1760000000000,"origin":"lane-b-0-1f2e3d4c","producerNanos":81234567890123,"checksum":"1a2b3c4d"}
```

- `origin` is the producing process: by default the pod's name (`POD_NAME`, else `HOSTNAME`) and a random suffix drawn when the JVM starts, so a restarted pod is a new origin and starts its keys at `seq` 1 again. Several producers can share a topic: records are told apart by run, origin and key.
- `seq` counts the key's records of this origin in the run, from 1, with no gaps on the producer's side: a record the client refuses to take (its window stayed full, or the producer is closed) is not sent, and its `seq` is used by the next record of the key.
- `producerTs` is the wall clock at send, in ms; `producerNanos` is `System.nanoTime()` at send, which means something only to the same origin (see Latency).
- `checksum` is CRC-32C, 8 lowercase hex digits, of the UTF-8 bytes of `runId + "\n" + key + "\n" + seq + "\n" + producerTs + "\n" + origin + "\n" + producerNanos`. Records of an older producer have neither `origin` nor `producerNanos`, and a checksum of the first four fields only; they are still read, with an empty origin.
- **Why JSON** and not a proto: the server stores it as bytes with no contract or codec to configure, a person can read it with `kafka-console-consumer` and in a dead-letter topic, and the program needs nothing beyond the JDK to write and read it.

Each record's outcome is recorded: acknowledged by every cluster the topic requires, failed with the server's `ErrorCode` (the record was not written), or unknown (`ERROR_CODE_OUTCOME_UNKNOWN`, `ERROR_CODE_INTERNAL`, or `UNANSWERED` when the producer was closed before its result came). A failed or unknown record is not sent again; the next record of its key has the next `seq`.

**Consumer mode** subscribes to the topic (its group is the sidecar's configuration) and, for every record handed to it:

1. Reads the value and verifies the checksum. A record that is not one of this program's, or whose Kafka key is not its `key`, is **malformed**; one whose checksum does not match is a **checksum failure**. Both are acknowledged and not tracked further. With `--run-id`, records of other runs are counted (`foreignRun`) and skipped.
2. Classifies the delivery, per run, origin and key, against the highest `seq` seen so far:
   - **first delivery**, in order when it is one above the highest seen; a **gap** when it is further above (the ones between were not seen); a **reorder** when it is below the highest seen (it arrived after a later one), whose *depth* is how far below;
   - **expected redelivery**: this program failed the record on purpose before (or never finished it), and here it is again. Never a duplicate nor a reorder, whatever came in between;
   - **duplicate**: the record was processed before. Its first and its new delivery times are kept.
3. Applies the fault knobs, then acknowledges by returning from the handler.

**Failing a record.** The Java client has one way to leave a record unacknowledged: the handler throws, and the consumer stops. Everything not acknowledged is delivered again, and the program starts a new consumer after `--restart-delay`. What that costs differs:

- A consumer that **reads Kafka itself** commits what was acknowledged, leaves the group, and the next consumer resumes at the failed record. The `kafka` transport's consumer does the same.
- **Through a dispatcher** a failure counts against `maxAttempts` (and so leads to a dead letter) only when the record's **processing deadline passed** before the consumer let it go: a record whose consumer merely went away is delivered again under the same attempt. So set `--fail-hold` above the processing deadline (`--max-processing`, or the server's), and `--max-unacked 1`, so that records queued behind a held one do not miss their deadlines too. A record held past its deadline may be given to another pod meanwhile.

**Several consumer pods through a dispatcher.** A key's records are spread over the pods, so one pod sees gaps that are the other pods' records. Run them with `--sole-consumer false`: gaps are then counted but neither logged nor held against `clean`. Reorders and duplicates within a pod still mean what they say; for duplicates across pods, merge the pods' `processedRanges` (below). The fault knobs count failures per pod: a record failed on one pod and delivered to another is new there. The same holds for several pods in one Kafka consumer group, with either transport.

**Both mode** (`--mode both`) runs a producer and a consumer in one process, over one transport, until stopped: the producer sends at its rate, the consumer reads the topic in its group. Without `--run-id` the producer writes a random run id and the consumer tracks every run, so that the records of the other pods of a lane are checked too. The results hold the two ledgers side by side (Results).

## Transports

**`odradek`** (the default): the Java client, over the sidecar's Unix socket (`--socket`). The topic, the group and everything Kafka are the sidecar's configuration.

**`kafka`**: a `KafkaProducer` and a `KafkaConsumer` in this process, for the lane that measures Kafka without Odradek. They are configured the way the server configures its own clients (`ClusterProducers.properties`, `ConsumeService.properties`), so that the lanes differ in the path the records take and not in their settings:

| Client | Properties, in the order applied | Effective values (kafka-clients 4.3.1) |
| --- | --- | --- |
| Producer | `bootstrap.servers`; the `--kafka-properties` file; then what the server enforces: `acks=all`, `enable.idempotence=true`; `client.id` `e2e-client-<origin>` unless the file sets one | `acks=-1` (all), `enable.idempotence=true`, `max.in.flight.requests.per.connection=5`, `linger.ms=5`, `batch.size=16384`, `compression.type=none`, `delivery.timeout.ms=120000`, `request.timeout.ms=30000`: Kafka's defaults beyond what is enforced, as on the server's producers |
| Consumer | `bootstrap.servers`; `auto.offset.reset=earliest` (what a server consumer does by default for a partition the group never committed); the file; then what the server enforces: `group.id` (`--group`), `enable.auto.commit=false`, `partition.assignment.strategy` cooperative-sticky; `client.id` as above | `group.protocol=classic`, `fetch.min.bytes=1`, `fetch.max.wait.ms=500`, `max.poll.records=500`, `max.partition.fetch.bytes=1048576`, `session.timeout.ms=45000`, `heartbeat.interval.ms=3000` |

The `started` line lists these values as the clients see them (`kafkaClients`). The consumer works as the server's does for an app: one record at a time on the poll thread, which polls every 50 ms; what was handled is committed asynchronously every `--commit-interval` (the server's `consume.commitInterval`, 5 s by default: give both the same value), synchronously for partitions being revoked and when the consumer closes. The sidecar polls whatever the app does; this consumer handles records on its poll thread, so with `--handler-delay` (plus `--fail-hold`) it lowers `max.poll.records` until a batch takes at most half of `max.poll.interval.ms`, and refuses to start if one record alone would take longer. Without a handler delay Kafka's defaults stand. Whatever the sidecars' clusters set in `properties`, `producerProperties` or `consumerProperties` should go in the `--kafka-properties` file of the `kafka` lane too.

**TLS** for the `kafka` transport is the server's own code (`//server:tls`): the program builds a `clusters.<name>.tls` section from its flags and loads it with the server's `TlsSources`, which installs the rotating `ReloadingSslEngineFactory` as `ssl.engine.factory.class`. So, as on a sidecar:

- `--tls spiffe`: the X.509-SVID and bundles come from the SPIFFE Workload API (`--spiffe-socket`, else `SPIFFE_ENDPOINT_SOCKET`) over one long-lived stream; a rotated SVID is used from the next handshake on, the private key never touches disk. The program waits up to two minutes for the first SVID. The brokers are verified by SPIFFE ID (`--broker-spiffe-ids` and/or `--broker-trust-domain`, one of them required), not by host name.
- `--tls files`: `tls.crt`, `tls.key` and `ca.crt` in `--tls-dir`, re-read every 5 s. With neither SPIFFE flag the brokers are verified by host name.
- `--tls none` (the default): plaintext, every setting a flag; nothing to mount.

## Latency

Nodes' clocks differ by several milliseconds, so end-to-end latency is timed on one clock: every pod of a lane produces and consumes (`--mode both`), and only the records a process receives from itself are timed end to end. Three latencies, in microseconds:

- **produce**: from send to acknowledgement, on `System.nanoTime()`, for every acknowledged record, either transport.
- **end to end, self** (`clock` `self`): from the record's `producerNanos` to the start of the handler, for first deliveries of records whose `origin` is this process.
- **end to end, cross** (`clock` `cross`): from the record's wall-clock `producerTs` to the start of the handler, for every first delivery. Two clocks: for reference only. Negative values, which only clock skew gives, are recorded as 0 and counted (`negative`).

Each is kept over the whole run and over a rolling window (`--latency-window`, 60 s by default, in twelve slots: the window covers between eleven and twelve twelfths of it), as an HdrHistogram with three significant digits.

**Lanes.** The comparison runs side by side lanes with the same rate schedule and keys, each on a topic of its own, every pod `--mode both` with `--sole-consumer false` and `--lane` naming it: **A** `--transport kafka` (the app's own producer and consumer group, with the sidecars' Kafka settings in `--kafka-properties` and the same `--commit-interval`); **B** `--transport odradek` with the sidecar consuming from Kafka; **C** the same with the sidecar consuming through the dispatcher. Compare `endToEndSelf` and `produce` across lanes; `endToEndCross` only as a sanity check.

**Rate schedule.** `--rate-schedule 20:10m,50:5m,100:5m,200:5m,20:0` sends at 20 records per second for 10 minutes, then 50, 100 and 200 for 5 minutes each, then 20 for good (a last step of duration `0`). The rate steps without a restart, so the ledgers survive. A schedule whose last step has a duration ends there, like `--duration`. It replaces `--rate`.

**Schedule start.** By default the schedule starts when the process does, so pods started minutes apart (a rollout, lanes scheduled one after the other) step at different moments. `--schedule-start 2026-10-22T12:00:00Z` starts it at that instant instead, the same for every pod: a pod started before it waits, and one started after it, or restarted, joins at the step in force without sending what it missed. Pods then step together to within their nodes' clock offset, a few milliseconds.

## Metrics

Only when `OTEL_EXPORTER_OTLP_ENDPOINT` (or `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`) is set: OTLP over gRPC (`OTEL_EXPORTER_OTLP_PROTOCOL` must be `grpc` or unset), pushed every `OTEL_METRIC_EXPORT_INTERVAL` ms (30 s by default), with `service.name` from `OTEL_SERVICE_NAME` (default `odradek-e2e-client`) and the resource attributes of `OTEL_RESOURCE_ATTRIBUTES` (the deployments set `k8s.pod.name`). `service.instance.id` is the process's origin unless that variable sets it: each pod needs its own, or the pods export one series between them, whose cumulative values overwrite each other. On the server's OpenTelemetry, gRPC and Netty: no second copies.

| Metric | Kind, unit | Attributes | What |
| --- | --- | --- | --- |
| `e2e.produce.latency` | histogram, ms | `lane`, `transport` | Send to acknowledgement |
| `e2e.end_to_end.latency` | histogram, ms | `lane`, `transport`, `clock` (`self`, `cross`) | Send to the start of the handler, as under Latency |
| `e2e.records.sent` | counter | `lane`, `transport` | Records handed to the producer |
| `e2e.records.acked` | counter | `lane`, `transport` | Records acknowledged |
| `e2e.records.processed` | counter | `lane`, `transport` | Records processed for the first time |

The histograms have explicit buckets from 0.1 ms to 30 s: every 0.1 ms to 1 ms, then 1.25, 1.5, 1.75, 2, 2.5, 3, 3.5, 4, 5, 6, 7, 8, 10, 12.5, 15, 20, 25, 30, 40, 50, 75, 100, 150, 200, 300, 500, 750 ms, and 1, 1.5, 2, 3, 5, 10, 30 s. `lane` is `--lane`, `transport` `odradek` or `kafka`.

## Settings

Every setting is a flag (`--name value` or `--name=value`) and an environment variable; a flag wins over the variable, the variable over the default. Durations are `0` or a number with `ms`, `s`, `m` or `h` (`250ms`, `1.5s`, `10m`). `--help` prints this list. "Producer" and "consumer" below include `--mode both`.

| Flag | Variable | Default | Meaning |
| --- | --- | --- | --- |
| `--mode` | `E2E_MODE` | required | `producer`, `consumer` or `both` |
| `--transport` | `E2E_TRANSPORT` | `odradek` | `odradek` (through the sidecar) or `kafka` (the Kafka clients in this process): see Transports |
| `--topic` | `E2E_TOPIC` | required | Topic to produce to or consume from; through the sidecar it must be declared in its configuration |
| `--socket` | `E2E_SOCKET` | `/var/run/kafka/kafka.sock` | `odradek`: the sidecar's Unix socket |
| `--run-id` | `E2E_RUN_ID` | producer: a random UUID; consumer: none | Producer: written into every record. Consumer: only records of this run are tracked; without it, every run is, each on its own |
| `--origin` | `E2E_ORIGIN` | `$POD_NAME` (else `$HOSTNAME`) and a random suffix | Producer: this process, written into every record |
| `--lane` | `E2E_LANE` | none | A label carried into the results, the `started` line and the metrics |
| `--stop-timeout` | `E2E_STOP_TIMEOUT` | `25s` | How long a stop may take before the final line is written anyway (the client waits up to 30 s for a sidecar that has gone). Keep it under the pod's `terminationGracePeriodSeconds` |
| `--http-port` | `E2E_HTTP_PORT` | `8080` | Port of `GET /results` and `GET /healthz`, on every interface; `0` picks one |
| `--latency-window` | `E2E_LATENCY_WINDOW` | `60s` | The rolling window of the latency histograms; at least `1s` |
| `--exit-when-done` | `E2E_EXIT_WHEN_DONE` | `false` | Producer mode: exit once everything is sent (a Job). Otherwise it serves its results until stopped (a Deployment). Both mode always runs until stopped |
| `--rate` | `E2E_RATE` | `10` | Producer: records per second over all keys; fractions allowed |
| `--rate-schedule` | `E2E_RATE_SCHEDULE` | none | Producer: `rate:duration` steps, e.g. `20:10m,50:5m,20:0`; replaces `--rate` (Latency) |
| `--schedule-start` | `E2E_SCHEDULE_START` | none | Producer: the instant the schedule starts, the same for every pod, e.g. `2026-10-22T12:00:00Z`; empty for the process's own start (Latency) |
| `--count` | `E2E_COUNT` | `0` | Producer: records to send; `0` for no limit |
| `--duration` | `E2E_DURATION` | `0` | Producer: how long to send; `0` for no limit |
| `--keys` | `E2E_KEYS` | `8` | Producer: number of keys |
| `--max-processing` | `E2E_MAX_PROCESSING` | `0` | Consumer, `odradek`: the processing deadline asked of the server; `0` for the server's default |
| `--max-unacked` | `E2E_MAX_UNACKED` | `0` | Consumer, `odradek`: records delivered ahead of the handler; `0` for the client's default (500) |
| `--fail-every` | `E2E_FAIL_EVERY` | `0` | Consumer: fail every record whose `seq` is a multiple of N; `0` for none |
| `--fail-times` | `E2E_FAIL_TIMES` | `1` | Consumer: how many times each such record fails before it is processed |
| `--poison-keys` | `E2E_POISON_KEYS` | none | Consumer: comma-separated keys whose records always fail (dead letters, or a blocked partition) |
| `--fail-hold` | `E2E_FAIL_HOLD` | `0` | Consumer: how long the handler holds a record it fails before throwing. Above the processing deadline for a dispatcher to count the failure |
| `--handler-delay` | `E2E_HANDLER_DELAY` | `0` | Consumer: how long the handler takes for each record it processes (a slow handler) |
| `--restart-delay` | `E2E_RESTART_DELAY` | `1s` | Consumer: pause before starting a new consumer after one stopped |
| `--sole-consumer` | `E2E_SOLE_CONSUMER` | `true` | Consumer: `false` when several pods share the group |
| `--bootstrap-servers` | `E2E_BOOTSTRAP_SERVERS` | required with `kafka` | `kafka`: the brokers |
| `--group` | `E2E_GROUP` | `e2e-client-<topic>` | `kafka`: the consumer group |
| `--commit-interval` | `E2E_COMMIT_INTERVAL` | `5s` | `kafka`: how often what was handled is committed; match the sidecars' `consume.commitInterval` |
| `--kafka-properties` | `E2E_KAFKA_PROPERTIES` | none | `kafka`: a Java properties file of client settings, applied before what is enforced |
| `--tls` | `E2E_TLS` | `none` | `kafka`: `none`, `spiffe` or `files` |
| `--spiffe-socket` | `E2E_SPIFFE_SOCKET` | `$SPIFFE_ENDPOINT_SOCKET` | `kafka`, `spiffe`: the Workload API, `unix:///path` |
| `--tls-dir` | `E2E_TLS_DIR` | none | `kafka`, `files`: the absolute directory of `tls.crt`, `tls.key` and `ca.crt` |
| `--broker-spiffe-ids` | `E2E_BROKER_SPIFFE_IDS` | none | `kafka`: comma-separated SPIFFE IDs accepted of the brokers |
| `--broker-trust-domain` | `E2E_BROKER_TRUST_DOMAIN` | none | `kafka`: a trust domain whose every SPIFFE ID is accepted of the brokers |

## Output

**stdout** holds JSON lines and nothing else; the client's and Netty's logs go to stderr.

- `{"event":"started", "config": {…}, "httpPort": …}` once, when the transport is ready; with `kafka`, also `kafkaClients`: the effective settings of the producer and the consumer (Transports). `config` has `lane`, `transport` and `origin`.
- `{"event":"anomaly", "type": …, …}` for each anomaly, as it happens. Types: `gap`, `reorder`, `duplicate`, `checksum`, `malformed`, `consumer-failed` (consumer); `produce-failed`, `produce-unknown` (producer). Each carries `runId`, `origin`, `key` and `seq` where they apply, and what the type needs: `expected` and `missing` for a gap, `highestSeen` and `depth` for a reorder, `firstDeliveredMs` and `deliveredAgainMs` for a duplicate, `code` and `message` for a failure.
- `{"event":"done", …results}` when a producer has sent everything.
- `{"event":"final", …results}` last, when it stops: on SIGTERM, or after `done` with `--exit-when-done`.

Every line has `time`, milliseconds since the epoch.

**SIGTERM** stops the run. A producer stops sending and waits (up to a minute, as `Producer.close()` does) for the outcomes of what it sent; what has none is unknown (`UNANSWERED`). A consumer lets the record in hand finish (a sleep of `--handler-delay` or `--fail-hold` is cut short: a record being failed is failed, one being processed is processed), lets the server commit what was acknowledged, and leaves; what was delivered and not handled is not acknowledged, and goes to whoever reads next. The exit status is then 143, the JVM's for SIGTERM; 0 when a producer ends with `--exit-when-done`, 2 for bad settings, 1 when the `kafka` transport cannot start (its properties file, or no SVID within two minutes).

## Results

`GET /results` answers JSON; `GET /healthz` answers `ok` while the process is up. Every mode starts with:

| Field | Meaning |
| --- | --- |
| `mode`, `transport`, `lane`, `origin`, `runId`, `config` | The settings in force |
| `state` | `starting`, `running`, then: producer `draining`, `done` or `stopping`, `stopped`; consumer `restarting` between consumers, `stopping`, `stopped`; both `produced` once the producer is done, `stopping`, `stopped` |
| `startedMs`, `nowMs` | When it started, and when these results were made |
| `clean` | No anomaly that is a failure whatever the knobs: see How to read a result |
| `latency` | The latencies of Latency, below |

The producer's and the consumer's fields follow at the top level in their modes. In both mode they are under `producer` and `consumer`, with `producerState` and `consumerState`, and `clean` is both of theirs.

**Latency.** `latency` is

```json
{"unit": "us", "windowMs": 60000,
 "produce":       {"cumulative": {…}, "window": {…}, "negative": 0},
 "endToEndSelf":  {"clock": "self", "cumulative": {…}, "window": {…}, "negative": 0},
 "endToEndCross": {"clock": "cross", "note": "…for reference only", "cumulative": {…}, "window": {…}, "negative": 0}}
```

where each `cumulative` (the whole run) and `window` (the last `windowMs`) is `{"count", "p50Us", "p90Us", "p99Us", "p999Us", "maxUs"}`, in microseconds; all 0 with no records. They replace the earlier `ackLatency` (producer) and `latency` (consumer) fields, which had a mean and a maximum in ms: `produce` and `endToEndCross` are those, with percentiles.

**Producer.**

| Field | Meaning |
| --- | --- |
| `sent`, `acked`, `failed`, `unknown`, `outstanding` | Records sent, and their outcomes so far; `outstanding` have none yet |
| `notSent`, `notSentByReason` | Records the client refused to take (`BUFFER_FULL`, `CLIENT_CLOSED`): the rate was not met. Not sent, so their `seq` was used again |
| `failedByCode`, `unknownByCode` | Failed and unknown records by the server's `ErrorCode`, `UNANSWERED`, or with `kafka` the Kafka exception's class (unknown when it timed out or could have been retried) |
| `keys[]` | Per key: `runId`, `origin`, `key`, `sent`, `acked`, `lastSentSeq`, `lastAckedSeq`, `failed`, `unknown`, and `failedSeqs` and `unknownSeqs` (`seq`, `code`, `message`; the first 1000 of each) |

**Consumer.**

| Field | Meaning |
| --- | --- |
| `delivered` | Deliveries handed to the handler, every one counted |
| `processed` | Distinct records processed (acknowledged) |
| `duplicates` | Deliveries of a record already processed |
| `gaps` | First deliveries more than one above the highest `seq` of their key |
| `missing` | `seq`s below the highest seen of their key that were never delivered here: what gaps left unfilled |
| `reorders`, `longestReorder` | First deliveries below the highest `seq` of their key, and the greatest depth |
| `expectedRedeliveries` | Deliveries of a record this program had failed on purpose |
| `deliberateFailures` | Times the handler failed a record on purpose |
| `unresolvedFailures` | Records failed on purpose and never processed since: dead-lettered, still waiting, or on another pod |
| `checksumFailures`, `malformed`, `foreignRun` | Records that did not check out, and records of other runs |
| `consumerRestarts` | Consumers started again after a failure on purpose |
| `consumerFailures` | Consumers stopped by the server, by gRPC status (`ABORTED` when a record was held past the deadline of a consumer that reads Kafka itself) |
| `duplicateDetails` | The first 1000 duplicates: `runId`, `origin`, `key`, `seq`, `firstDeliveredMs`, `deliveredAgainMs`; `duplicateDetailsTruncated` says whether there were more |
| `keys[]` | Per run, origin and key (`runId`, `origin`, `key`): `highestSeen`, the counts above, and `processedRanges`, `missingRanges` and `unresolvedRanges`, each a list of inclusive `[first, last]` ranges of `seq` |

## How to read a result

A run passes when what each producer acknowledged was consumed, once, in order per key, intact. Concretely:

| Seen | Means | Failure? |
| --- | --- | --- |
| `checksumFailures` or `malformed` above 0 | Bytes changed between producer and consumer, or something else writes to the topic | Always |
| `missing` above 0 on a sole consumer, at a `seq` the producer acknowledged | An acknowledged record never arrived: lost | Always, once the consumer has caught up (`processed` stops moving). Excused only at the producer's `failedSeqs` (never written) and `unknownSeqs` (perhaps not written) |
| `reorders` above 0 | A key's records arrived out of order | Yes under the default `ORDERING_KEY`. Expected with `ORDERING_NONE`, and possible after a hand-over that was not in good order (design.md, Hand-over between replicas) |
| `duplicates` above 0 | A processed record came again | Yes in a run that should have none: a consumer that reads Kafka itself and is never restarted, or a dispatcher whose hand-overs were all in good order. Expected with `--handler-delay` above the processing deadline (the dispatcher gives the record to another pod, or this one, while it is still being handled: the negative control of the hand-over), after a replica was killed, and when a consumer reading Kafka itself was evicted |
| `failed` or `unknown` above 0 (producer) | The server could not write a record, or could not tell | A failure of the run unless the cluster was being disturbed on purpose; a consumer gap at that `seq` is then expected |
| `expectedRedeliveries`, `deliberateFailures`, `consumerRestarts` above 0 | The fault knobs at work | No: with `--fail-every N --fail-times M`, about `M` deliberate failures per record whose `seq` is a multiple of `N`, each followed by a redelivery |
| `unresolvedFailures` above 0 | Records failed on purpose and not processed since | No for poison keys under `ON_EXHAUSTED_DEAD_LETTER`: their count should match the dead-letter topic and `odradek.dispatch.exhausted` (`action` `dead_letter`). With `--fail-every` it should drop to 0 once the records come back |
| `gaps` above 0 with `--sole-consumer false` | Records that went to other pods | No. Merge every pod's `processedRanges` per run, origin and key instead: their union should cover `1` to that producer's `lastAckedSeq` (the producer's `keys[]` entry with the same `runId`, `origin` and `key`), and overlaps are duplicates across pods |
| `consumerFailures` above 0 | The server stopped a consumer | Depends on the cause, given by its code: `ABORTED` from a slow handler is expected under `--handler-delay`; a refusal (`PERMISSION_DENIED`, `FAILED_PRECONDITION`) is a misconfiguration |

`clean` is `true` when there are no duplicates, no reorders, no checksum failures, no malformed records and, on a sole consumer, nothing missing (on the producer: nothing failed or unknown). Under fault knobs that expect duplicates or reorders, read the counts, not `clean`.

**Cross-checks against the server's metrics** (design.md, Metrics): records the producers acknowledged against the sidecars' `odradek.produce.records`, and records consumed against `odradek.dispatch.records` and `odradek.receive.records`; dead letters against `odradek.dispatch.dead_letter.writes` and `odradek.dispatch.exhausted`; redeliveries against `odradek.dispatch.redeliveries`; and hand-overs that were not in good order against `odradek.dispatch.handover.records`, which should be 0 when the consumers report no duplicates.
