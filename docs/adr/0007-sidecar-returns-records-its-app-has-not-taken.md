# 7. A sidecar returns records its application has not taken, without using an attempt

## Status

Accepted

## Context

A dispatcher counts a failed attempt when a record is not acknowledged within its processing deadline (ADR 6). The deadline starts when the replica sends the record to a sidecar, because that is all the replica can see. It is meant to measure the application: how long its handler held the record.

The sidecar can hold records its application has not taken yet. It divides the application's credit among its `Deliver` streams, one per replica, and gives a stream with nothing outstanding one credit even when the application's budget is all promised elsewhere. Without that rule, an application that asks for one record at a time would give its only credit to one replica and never hear from the others. The cost is that up to (replicas − 1) records can wait in the sidecar for the application, and the design says so: "they are on the dispatcher's clock while they wait".

A replica picks the receiver with credit that is least loaded, then the one it sent to longest ago (`DispatchCore.pick`). The load is the number of records that pod holds *from this replica*. So a pod whose application is stuck on a record from replica X shows replicas Y and Z a load of 0 and one spare credit. To them it is the most idle receiver there is, and since nothing was sent to it lately, it is their first choice. The records they send wait in its sidecar behind the stuck one until their deadlines pass, and each miss uses an attempt. A redelivery avoids only the receiver that has just failed the record, so the next try can land on another stuck pod.

The in-cluster run measured this (homelab, scenario G, 2026-10-23; 3 dispatcher replicas, 4 pods per lane, `maxAttempts` 5, 2 s deadline, failing records held 3 s, `max-unacked 1`):

- **Transient failures (G1).** The dispatchers counted more missed deadlines than the pods failed on purpose: 361 against 285 (JVM) and 343 against 262 (native). Each surplus miss used an attempt of a record the application never saw. Five records per lane that failed on all four pods picked up a fifth attempt this way and were dead-lettered.
- **A poison key (G2).** A record of a key the application never fails (`key-21`, seq 1) used its five attempts in 25 seconds, all missed deadlines, and was dead-lettered. Its handler would have succeeded.

The trigger is an application that holds a record past its deadline: a slow or hung handler, which is the case `maxAttempts`, dead letters and blocking exist for. When it happens, records with nothing wrong lose attempts. Under `ON_EXHAUSTED_DEAD_LETTER` they can end in the dead-letter topic; under `ON_EXHAUSTED_BLOCK`, the default, they can stop their partition. Neither is a property of those records, and the operator sees a dead letter or a blocked partition that points at the wrong cause.

## Decision

We will let a sidecar hand back, without using an attempt, records it holds that its application has not taken.

### The protocol

- `DeliverRequest` gains a `Return` message: a partition and, for each record the sidecar is giving back unread, its offset and the attempt number it was delivered with. A sidecar returns only records it has not written to the application's stream, and each delivery at most once.
- **Which delivery a return is for.** Offset and stream alone do not say: a replica sends a record again to the same stream when that stream is the only one with credit, and a return of the first copy that arrives late would then match the second, which the application may already have. The attempt number does say. On one stream a record is sent again under the *same* attempt only in answer to that stream's own return (or after the stream ended), and the stream's messages arrive in order, so that return has been handled before the new copy exists. A copy sent again after a missed deadline carries the next attempt. So a return counts only when the stream holds the record in flight under the attempt it names.
- The dispatcher treats a returned record as it treats the records of a receiver that left: it is sent again at once, under the same attempt number, with no wait. It avoids the returning stream for that send when another receiver has credit. It counts the record under `odradek.dispatch.redeliveries` with `reason` `returned`, a new value, not a failed attempt.
- A returned record does not give back its credit, as `Credits` already says of a record that is released or delivered again elsewhere. That is what keeps a stuck pod from attracting records: its streams have spent their credit on the records it returned, and a stalled sidecar grants no new credit.
- **A late return** (the stream holds the record past its deadline, or under another attempt) gives back nothing. The miss was counted when the deadline passed, and a newer copy on the same stream stays where it is. What it does do is forget the expired copy: the stream no longer holds it, so it stops counting towards the stream's load and its acknowledgement is no longer awaited from there, as a late rejection does today. A return for a record the stream does not hold is ignored.
- **The deadline a return is measured against.** The sidecar knows its own application's deadline, but the replica applies the deadline of the declaration it accepted last, and a record keeps the one it was sent under. During a rollout that changes it the two differ. So each `RecordBatch` carries the deadline its records were sent under, and the sidecar measures the half-deadline rule against that. A replica starts a new batch when the deadline changes.
- **Compatibility.** `Accepted` gains a field by which a dispatcher says it understands `Return`. A sidecar sends `Return` only on a stream whose `Accepted` said so. With an older dispatcher it keeps today's behaviour, and an older sidecar never sends one.

### When a sidecar returns

- **When its application stalls.** The sidecar already notices when the application has held a record longer than its processing deadline (it then grants no more credit). At that moment it returns every record it holds that the application has not taken, on every stream.
- **When a record has waited too long.** It returns a record that has waited in the sidecar for half the processing deadline it was sent under (the one its batch carries) without the application taking it, stalled or not. A record that has waited that long would leave the application less than half of the deadline it asked for.
- It never returns a record it has written to the application's stream. Whatever the application has received is acknowledged or misses its deadline as today, and that miss still counts.

## Consequences

An application that holds a record too long costs attempts only for that record. The others it would have been given go to pods that can take them, at once and with their attempts intact, so dead letters and blocked partitions again say something about the records in them.

The deadline does not change meaning on the wire: the replica still starts it when it sends. What changes is that time a record spends in the sidecar no longer counts once it nears half the deadline, because the sidecar gives it back first. A record can still miss a deadline it was never processed against when a sidecar dies or is cut off with records it had not handed on, since a sidecar that is gone returns nothing. That case already uses no attempt when the stream ends cleanly. When it does not end cleanly, the miss is counted at the deadline, as now.

Records can now move between pods without a failed attempt for a new reason. That can reorder delivery under `ORDERING_NONE`, as any redelivery can, and changes nothing under `key` or `partition`: a returned record stays the oldest unacknowledged record of its key or partition, and nothing behind it is sent before it.

The protocol grows a message and a capability field, and the dispatcher core grows a path that is close to the one for a receiver that left. Both are additions, so `//api:breaking_test` is not affected. The behaviour belongs to the server on both ends, not to the client libraries, so no conformance scenario changes.

Work that follows from this record, in one change:

- `api/odradek/delivery/v1/delivery.proto`: `Return` with an offset and an attempt per record, the capability on `Accepted`, and the deadline on `RecordBatch`.
- `server/dispatch`: handle `Return` in `DispatchCore`, with the `returned` reason.
- `server/receive`: return on stall and after half the deadline in `ReceiveSession`; the credit rules in `CreditSplit` stay as they are.
- `docs/design.md`: the credit split, "What a receiving sidecar does", the dispatch core, Redelivery, limits and dead letters, and the metrics table (`returned` on `odradek.dispatch.redeliveries`, and a new `odradek.receive.returned` counter on the sidecar).
- Tests: a dispatcher core test that a returned record is resent at once under the same attempt and avoids the returning stream; that a late return of a copy that missed its deadline, after the record was sent again to the same stream, leaves the new copy alone and forgets the old one; a receive test that a stalled session returns what it holds and nothing it handed on, and that the half-deadline rule follows the deadline a batch carries; an end-to-end test with three replicas and one stuck pod in which no other record uses an attempt.
- The in-cluster scenario G, run again: deadline redeliveries equal to the deliberate failures, and no dead letter of a key the application does not fail.

Two alternatives were set aside:

- **Start the deadline when the application receives the record.** The sidecar would tell the replica when it writes a record to the application. This also needs a protocol change and a message per record, and on its own it would not stop replicas preferring a stuck pod: records would wait there without a deadline instead of missing one.
- **Have a replica stop preferring a pod with a missed deadline outstanding.** It is local to the dispatcher and simple, but a replica cannot see that a pod is stuck on another replica's record, which is exactly the case measured. It narrows the window and does not close it.

Dropping the one-credit rule would fix the measured case and break the one it exists for: an application that takes one record at a time from several replicas would starve all but one of them.
