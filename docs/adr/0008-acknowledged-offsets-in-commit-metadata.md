# 8. A dispatcher commits the offsets it has acknowledged above its commit position

## Status

Proposed

## Context

A dispatcher commits one number per partition: the offset of its oldest record that is not acknowledged (ADR 6). Under `ORDERING_KEY` and `ORDERING_NONE` records are acknowledged out of offset order, so the records of other keys are acknowledged above a record that is held back, and whoever takes the partition next delivers all of them again. Two things limit that today:

- **The hand-over finishes what it began.** A replica that gives a partition up sends what it held back below the newest record it had sent, for the first half of `consume.revokeDeadline`, so that the commit can move past what was acknowledged above it. A hand-over in good order delivers nothing twice.
- **The window** bounds what lies above the commit position to `dispatch.partitionWindow` offsets.

A hand-over is in good order only when everything held back can be finished in time, and the design lists what keeps it from being: a handler slower than half the wait, an application that takes no records, a key with a long backlog, a record that blocks, a record waiting for a redelivery due after the sending ends, a dead letter that could not be written. A hand-over that counts as `completed` can still leave records above its commit: `completed` says only that everything sent was answered. A replica that is killed, or loses its partitions, commits nothing at all beyond its last interval.

The in-cluster run measured the cost (homelab, scenario G2 on ADR 7, run `e2e-20261023-5`; 3 dispatcher replicas per lane, a poison key whose records the application always fails, `maxAttempts` 5, 2 s deadline, failing records held 3 s). Each record of the poison key took about 25 seconds to exhaust its attempts, and the next one of that key waited behind it, so its partition's commit position stayed at that key's oldest record for the whole run while the other keys of the partition went on being acknowledged above it. When the dispatchers were rolled during the run, the hand-overs of that partition counted as `completed`, since everything sent had been answered, but the record holding the commit position was waiting for its next attempt, due long after the hand-over's sending ended, so the commit stopped at it. Each next owner delivered again everything acknowledged above it: `odradek.dispatch.handover.records` counted 79 (JVM) and 37 (native), and the applications received 78 and 36 records a second time, every one on that partition, with records of a key seen again after later ones. Nothing was lost, and this is the at-least-once the design promises, but every one of those duplicates was a record the old owner knew was done.

Kafka lets a commit carry more than the offset. `OffsetAndMetadata` has a metadata string that the group coordinator stores with the offset and returns with it, up to the broker's `offset.metadata.max.bytes` (4096 bytes unless the broker says otherwise). The dispatcher already reads the committed offset when it takes a partition, in the same call that returns the metadata.

## Decision

We will have a dispatcher record, in the metadata of each commit, which offsets above the committed one it has acknowledged, and have the next owner of the partition skip them.

### What a commit carries

- **The offsets.** The metadata holds a bitmap of the offsets after the committed one: bit *i* says whether offset *committed* + 1 + *i* is acknowledged. The committed offset itself never is, since it is the oldest record that is not. Offsets that hold no record are 0, which costs nothing: the next owner never fetches them. A record counts once it is acknowledged, or its dead letter written; not while it is in flight, waiting, blocked or being written.
- **The encoding.** `odradek.acked/1:` followed by the bitmap in unpadded base64url, least significant bit first, with trailing zero bytes left out. A partition with nothing acknowledged above its commit position commits an empty string, as today.
- **Its size.** Every acknowledged record was first delivered within the window above the commit position of that moment, which is at or below the current one, so the bitmap spans less than `dispatch.partitionWindow` offsets: at the default of 1000, at most 125 bytes, 183 characters with the prefix. A larger window can exceed what a broker stores, so the bitmap is cut at 2048 bytes (16,384 offsets, 2,747 characters). What lies past the cut is delivered again, as today.
- **When it is committed.** Today a partition is committed when its commit position moves. It is now also committed when the acknowledged offsets above it change, at the same `consume.commitInterval`. A partition whose commit position is held, as the poison key's was, is committed once an interval for as long as records above it are acknowledged.
- **A broker that refuses it.** A broker whose `offset.metadata.max.bytes` is smaller than the metadata answers the commit with `OFFSET_METADATA_TOO_LARGE`. The replica then logs it once, commits that group's offsets without metadata until it restarts, and is back where it is today.

### What the next owner does with it

- It reads the metadata with the committed offset, when it takes the partition. Records it then fetches at the offsets the bitmap marks are **acknowledged on arrival**: they are not delivered, they hold back no later record of their key, and the commit position moves past them as past any acknowledged record. They count under a new counter, `odradek.dispatch.handover.skipped` (`cluster`, `group`, `topic`).
- Metadata it does not recognise is ignored, and everything above the committed offset is delivered, as today. That covers commits made by an older dispatcher, by anything else that commits for the same group, and offsets reset with `kafka-consumer-groups`, which commits empty metadata.
- What it skips is only what the old owner had acknowledged. Records that were in flight, waiting for a redelivery, blocked, or not yet sent are delivered again, under a fresh attempt count, as today. Under `ORDERING_KEY` a record is sent only once every earlier record of its key is acknowledged, so what is skipped of a key always precedes what is delivered of it: no application sees a record of a key after a later one because of a change of replica.

### What stays

- **The hand-over** still finishes what it began, and still waits for what is in flight: a record a pod is working on is not delivered to another while the wait lasts, and what it finishes in time moves the commit position. It no longer decides what is delivered twice.
- **Attempt counts** are still one replica's, and still start again with a new owner. Carrying them as well is a separate decision: it changes when a record is exhausted, not how often it is processed.
- **`odradek.dispatch.handover.records`** keeps its meaning, what the next owner delivers again, and so no longer counts what the metadata carries.

## Consequences

A change of replica delivers again only what was not acknowledged when the last commit was made. After a hand-over, whether or not it was in good order, that is what was in flight or not yet sent; a record of another key acknowledged above a stuck one is not processed twice. After a replica is killed or loses its partitions, it is what was acknowledged since its last commit, at most one `consume.commitInterval`, instead of everything above its commit position. Duplicates from a dispatcher roll then follow the slowest handler in flight, not the longest backlog of one key.

The dispatcher writes more to the group coordinator: a partition whose position is held is committed every interval, with a few hundred bytes at the default window. Lag, as consumer-group tools and `odradek.dispatch.fetch.lag` show it, is still measured from the committed offset, so a partition held by a poison key still shows the lag of everything above it, acknowledged or not; the metadata is visible in those tools as an opaque string.

Replicas of two versions can share a group during a rollout. An older replica ignores the metadata and delivers again as today, and its commits carry none, so a newer one taking its partitions does the same. Rolling back is as safe as rolling forward. Nothing changes on the delivery protocol or for sidecars, so `//api:breaking_test` and the conformance scenarios are not affected.

The metadata format is a contract with the next replica, of whatever version, like the dead-letter headers are with whoever reads dead letters: a change to it gets a new prefix, and a replica keeps reading the old one for as long as a rollback could bring it back.

Work that follows from this record, in one change:

- `server/dispatch`: a codec for the metadata; `PartitionAssigned` carries the acknowledged offsets, and `DispatchCore` takes records fetched at them as acknowledged and reports the offsets acknowledged above a partition's commit position; `DispatchGroup` encodes them in every commit, commits when they change, decodes them when it takes a partition, and stops sending them after `OFFSET_METADATA_TOO_LARGE`.
- `ServerMetrics`: `odradek.dispatch.handover.skipped`.
- `docs/design.md`: Hand-over between replicas, the dispatch core, Operating a dispatcher, and the metrics table.
- Tests: the codec (round trip, the cut, metadata it does not recognise); a dispatcher core test that records at the offsets it was given are not delivered, hold back nothing of their key and let the commit position pass, and that what is acknowledged above a held commit position is reported; an end-to-end test with two replicas and a held key in which the replica that takes the partition delivers none of the records acknowledged above it.
- The in-cluster scenario G2, run again with the image change in its own pull request first: duplicates only of records that were in flight at a change of replica.

Four alternatives were set aside:

- **A longer `consume.revokeDeadline`.** It does not help a held key: each of its records takes as long as its attempts and backoffs, the hand-over finishes them one at a time, and one whose next attempt is due after the sending ends is not waited for at all. It also keeps a stopping replica's partitions from everyone for longer.
- **Ranges instead of a bitmap.** A list of acknowledged ranges is smaller when acknowledgements are contiguous, and larger, without a bound short of the window, when a few keys are interleaved, which is exactly the held-key case. The bitmap's size is known from the window.
- **A store of our own**, a compacted topic or a database, for the acknowledged offsets. It survives anything the commit survives and nothing more, and adds a dependency, rights for the dispatcher's principal, and a second write that can disagree with the commit.
- **Handing the state from replica to replica** over the delivery protocol. In a rebalance the replica giving a partition up does not know which one takes it, nor when, and a replica that is killed hands nothing over.
