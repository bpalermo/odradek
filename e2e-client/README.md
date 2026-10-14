# e2e-client

The test application of Odradek's in-cluster end-to-end run. One program, two modes, on the Java client (`//client/java`): it runs as the app container of a pod whose Odradek server is a native sidecar, talks to it over the Unix socket, and keeps its own account of what happened to every record. A run is graded from that account, served on `GET /results`, cross-checked against the server's metrics.

Image: `quay.io/odradek/e2e-client` (linux/amd64 and linux/arm64, JVM, on the server's distroless Java 25 base). Tags as for the server: `dev` and `dev-<commit>` from every commit on `main`, `X.Y.Z` from a release, all signed. Entrypoint: the program; flags are its arguments.

| Task | Command |
| --- | --- |
| Run it | `bazel run //e2e-client -- --mode producer --topic t --socket /var/run/kafka/kafka.sock` |
| Unit tests | `bazel test //e2e-client:test` |
| Both modes against a real broker | `E2eClientE2eTest` in `//e2e:e2e_test` (`make e2e`) |
| Build the image | `bazel build //e2e-client:image_index` |
| Load it into Docker | `bazel run //e2e-client:image_load` (as `localhost/odradek-e2e-client:e2e`) |

## What it checks

**Producer mode** sends records to a topic at a rate R, round robin over K keys (`key-0` … `key-<K-1>`), for a count N or a duration, whichever ends first, or until stopped. A record of a key is sent only once the previous record of that key has its outcome: that is what Odradek's ordering guarantee covers (records of one stream reach each partition in order), so a consumer may hold the key's records to strict order. The rate is a target: with every key waiting on an acknowledgement it is at most K divided by the acknowledgement latency.

Each record's value is JSON, its Kafka key the key:

```json
{"runId":"3f0c…","key":"key-3","seq":12,"producerTs":1760000000000,"checksum":"1a2b3c4d"}
```

- `seq` counts the key's records in the run, from 1, with no gaps on the producer's side: a record the client refuses to take (its window stayed full, or the producer is closed) is not sent, and its `seq` is used by the next record of the key.
- `checksum` is CRC-32C, 8 lowercase hex digits, of the UTF-8 bytes of `runId + "\n" + key + "\n" + seq + "\n" + producerTs`.
- **Why JSON** and not a proto: the server stores it as bytes with no contract or codec to configure, a person can read it with `kafka-console-consumer` and in a dead-letter topic, and the program needs nothing beyond the JDK to write and read it.

Each record's outcome is recorded: acknowledged by every cluster the topic requires, failed with the server's `ErrorCode` (the record was not written), or unknown (`ERROR_CODE_OUTCOME_UNKNOWN`, `ERROR_CODE_INTERNAL`, or `UNANSWERED` when the producer was closed before its result came). A failed or unknown record is not sent again; the next record of its key has the next `seq`.

**Consumer mode** subscribes to the topic (its group is the sidecar's configuration) and, for every record handed to it:

1. Reads the value and verifies the checksum. A record that is not one of this program's, or whose Kafka key is not its `key`, is **malformed**; one whose checksum does not match is a **checksum failure**. Both are acknowledged and not tracked further. With `--run-id`, records of other runs are counted (`foreignRun`) and skipped.
2. Classifies the delivery, per run and key, against the highest `seq` seen so far:
   - **first delivery**, in order when it is one above the highest seen; a **gap** when it is further above (the ones between were not seen); a **reorder** when it is below the highest seen (it arrived after a later one), whose *depth* is how far below;
   - **expected redelivery**: this program failed the record on purpose before (or never finished it), and here it is again. Never a duplicate nor a reorder, whatever came in between;
   - **duplicate**: the record was processed before. Its first and its new delivery times are kept.
3. Applies the fault knobs, then acknowledges by returning from the handler.

**Failing a record.** The Java client has one way to leave a record unacknowledged: the handler throws, and the consumer stops. Everything not acknowledged is delivered again, and the program starts a new consumer after `--restart-delay`. What that costs differs:

- A consumer that **reads Kafka itself** commits what was acknowledged, leaves the group, and the next consumer resumes at the failed record.
- **Through a dispatcher** a failure counts against `maxAttempts` (and so leads to a dead letter) only when the record's **processing deadline passed** before the consumer let it go: a record whose consumer merely went away is delivered again under the same attempt. So set `--fail-hold` above the processing deadline (`--max-processing`, or the server's), and `--max-unacked 1`, so that records queued behind a held one do not miss their deadlines too. A record held past its deadline may be given to another pod meanwhile.

**Several consumer pods through a dispatcher.** A key's records are spread over the pods, so one pod sees gaps that are the other pods' records. Run them with `--sole-consumer false`: gaps are then counted but neither logged nor held against `clean`. Reorders and duplicates within a pod still mean what they say; for duplicates across pods, merge the pods' `processedRanges` (below). The fault knobs count failures per pod: a record failed on one pod and delivered to another is new there.

## Settings

Every setting is a flag (`--name value` or `--name=value`) and an environment variable; a flag wins over the variable, the variable over the default. Durations are `0` or a number with `ms`, `s`, `m` or `h` (`250ms`, `1.5s`, `10m`). `--help` prints this list.

| Flag | Variable | Default | Meaning |
| --- | --- | --- | --- |
| `--mode` | `E2E_MODE` | required | `producer` or `consumer` |
| `--topic` | `E2E_TOPIC` | required | Topic to produce to or consume from; it must be declared in the sidecar's configuration |
| `--socket` | `E2E_SOCKET` | `/var/run/kafka/kafka.sock` | The sidecar's Unix socket |
| `--run-id` | `E2E_RUN_ID` | producer: a random UUID; consumer: none | Producer: written into every record. Consumer: only records of this run are tracked; without it, every run is, each on its own |
| `--stop-timeout` | `E2E_STOP_TIMEOUT` | `25s` | How long a stop may take before the final line is written anyway (the client waits up to 30 s for a sidecar that has gone). Keep it under the pod's `terminationGracePeriodSeconds` |
| `--http-port` | `E2E_HTTP_PORT` | `8080` | Port of `GET /results` and `GET /healthz`, on every interface; `0` picks one |
| `--exit-when-done` | `E2E_EXIT_WHEN_DONE` | `false` | Producer: exit once everything is sent (a Job). Otherwise it serves its results until stopped (a Deployment) |
| `--rate` | `E2E_RATE` | `10` | Producer: records per second over all keys; fractions allowed |
| `--count` | `E2E_COUNT` | `0` | Producer: records to send; `0` for no limit |
| `--duration` | `E2E_DURATION` | `0` | Producer: how long to send; `0` for no limit |
| `--keys` | `E2E_KEYS` | `8` | Producer: number of keys |
| `--max-processing` | `E2E_MAX_PROCESSING` | `0` | Consumer: the processing deadline asked of the server; `0` for the server's default |
| `--max-unacked` | `E2E_MAX_UNACKED` | `0` | Consumer: records delivered ahead of the handler; `0` for the client's default (500) |
| `--fail-every` | `E2E_FAIL_EVERY` | `0` | Consumer: fail every record whose `seq` is a multiple of N; `0` for none |
| `--fail-times` | `E2E_FAIL_TIMES` | `1` | Consumer: how many times each such record fails before it is processed |
| `--poison-keys` | `E2E_POISON_KEYS` | none | Consumer: comma-separated keys whose records always fail (dead letters, or a blocked partition) |
| `--fail-hold` | `E2E_FAIL_HOLD` | `0` | Consumer: how long the handler holds a record it fails before throwing. Above the processing deadline for a dispatcher to count the failure |
| `--handler-delay` | `E2E_HANDLER_DELAY` | `0` | Consumer: how long the handler takes for each record it processes (a slow handler) |
| `--restart-delay` | `E2E_RESTART_DELAY` | `1s` | Consumer: pause before starting a new consumer after one stopped |
| `--sole-consumer` | `E2E_SOLE_CONSUMER` | `true` | Consumer: `false` when several pods share the group through a dispatcher |

## Output

**stdout** holds JSON lines and nothing else; the client's and Netty's logs go to stderr.

- `{"event":"started", "config": {…}, "httpPort": …}` once.
- `{"event":"anomaly", "type": …, …}` for each anomaly, as it happens. Types: `gap`, `reorder`, `duplicate`, `checksum`, `malformed`, `consumer-failed` (consumer); `produce-failed`, `produce-unknown` (producer). Each carries `runId`, `key` and `seq` where they apply, and what the type needs: `expected` and `missing` for a gap, `highestSeen` and `depth` for a reorder, `firstDeliveredMs` and `deliveredAgainMs` for a duplicate, `code` and `message` for a failure.
- `{"event":"done", …results}` when a producer has sent everything.
- `{"event":"final", …results}` last, when it stops: on SIGTERM, or after `done` with `--exit-when-done`.

Every line has `time`, milliseconds since the epoch.

**SIGTERM** stops the run. A producer stops sending and waits (up to a minute, as `Producer.close()` does) for the outcomes of what it sent; what has none is unknown (`UNANSWERED`). A consumer lets the record in hand finish (a sleep of `--handler-delay` or `--fail-hold` is cut short: a record being failed is failed, one being processed is processed), lets the server commit what was acknowledged, and leaves; what was delivered and not handled is not acknowledged, and goes to whoever reads next. The exit status is then 143, the JVM's for SIGTERM; 0 when a producer ends with `--exit-when-done`, 2 for bad settings.

## Results

`GET /results` answers JSON; `GET /healthz` answers `ok` while the process is up. Both modes start with:

| Field | Meaning |
| --- | --- |
| `mode`, `runId`, `config` | The settings in force |
| `state` | `starting`, `running`, then: producer `draining`, `done` or `stopping`, `stopped`; consumer `restarting` between consumers, `stopping`, `stopped` |
| `startedMs`, `nowMs` | When it started, and when these results were made |
| `clean` | No anomaly that is a failure whatever the knobs: see How to read a result |

**Producer.**

| Field | Meaning |
| --- | --- |
| `sent`, `acked`, `failed`, `unknown`, `outstanding` | Records sent, and their outcomes so far; `outstanding` have none yet |
| `notSent`, `notSentByReason` | Records the client refused to take (`BUFFER_FULL`, `CLIENT_CLOSED`): the rate was not met. Not sent, so their `seq` was used again |
| `failedByCode`, `unknownByCode` | Failed and unknown records by the server's `ErrorCode`, or `UNANSWERED` |
| `ackLatency` | `meanMs` and `maxMs` from send to acknowledgement |
| `keys[]` | Per key: `sent`, `acked`, `lastSentSeq`, `lastAckedSeq`, `failed`, `unknown`, and `failedSeqs` and `unknownSeqs` (`seq`, `code`, `message`; the first 1000 of each) |

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
| `latency` | `count`, `meanMs`, `maxMs` from `producerTs` to first delivery |
| `duplicateDetails` | The first 1000 duplicates: `runId`, `key`, `seq`, `firstDeliveredMs`, `deliveredAgainMs`; `duplicateDetailsTruncated` says whether there were more |
| `keys[]` | Per run and key: `highestSeen`, the counts above, and `processedRanges`, `missingRanges` and `unresolvedRanges`, each a list of inclusive `[first, last]` ranges of `seq` |

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
| `gaps` above 0 with `--sole-consumer false` | Records that went to other pods | No. Merge every pod's `processedRanges` per key instead: their union should cover `1` to the producer's `lastAckedSeq`, and overlaps are duplicates across pods |
| `consumerFailures` above 0 | The server stopped a consumer | Depends on the cause, given by its code: `ABORTED` from a slow handler is expected under `--handler-delay`; a refusal (`PERMISSION_DENIED`, `FAILED_PRECONDITION`) is a misconfiguration |

`clean` is `true` when there are no duplicates, no reorders, no checksum failures, no malformed records and, on a sole consumer, nothing missing (on the producer: nothing failed or unknown). Under fault knobs that expect duplicates or reorders, read the counts, not `clean`.

**Cross-checks against the server's metrics** (design.md, Metrics): records the producers acknowledged against the sidecars' `odradek.produce.records`, and records consumed against `odradek.dispatch.records` and `odradek.receive.records`; dead letters against `odradek.dispatch.dead_letter.writes` and `odradek.dispatch.exhausted`; redeliveries against `odradek.dispatch.redeliveries`; and hand-overs that were not in good order against `odradek.dispatch.handover.records`, which should be 0 when the consumers report no duplicates.
