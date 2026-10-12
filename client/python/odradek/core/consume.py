"""The consume side of the client protocol, with no I/O: events in, actions out.

A port of ``ConsumeCore.java`` (``//client/core``): the same state, the same
rules, the same order of actions. The conformance scenarios hold the two
together; change neither without the other.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto

from odradek.client.v1 import core_pb2
from odradek.kafka.v1 import odradek_pb2

DEFAULT_MAX_UNACKED_RECORDS = 500

# Stream statuses that a new stream would meet again, or that the app must hear about.
_FATAL = frozenset(
    {
        "INVALID_ARGUMENT",
        "NOT_FOUND",
        "FAILED_PRECONDITION",
        "ABORTED",
        "DATA_LOSS",
        "PERMISSION_DENIED",
        "UNAUTHENTICATED",
        "UNIMPLEMENTED",
    }
)

# A partition: topic and number. Tuples order the way the Java core orders them.
_Key = tuple[str, int]


class _State(Enum):
    DISCONNECTED = auto()
    SUBSCRIBING = auto()
    """A stream is open and Subscribe was sent; Subscribed has not arrived."""
    ACTIVE = auto()
    FAILED = auto()


@dataclass(slots=True)
class _Owned:
    epoch: int
    unacked: set[int] = field(default_factory=set)


class ConsumeCore:
    """Subscribes, grants credits, forwards acks and tracks which partitions are owned.

    It grants the server credits so that the app never holds more unacked
    records than it asked for, forwards an ack only while it still means
    something, and tracks which partitions are owned so that it can tell the
    app what it lost when a stream ends.

    Not safe for concurrent use: the binding calls :meth:`handle` from one
    place at a time.
    """

    def __init__(self, setup: core_pb2.ConsumeSetup) -> None:
        if len(setup.topics) == 0:
            raise ValueError("ConsumeSetup.topics is empty")
        self._setup = setup
        self._max_unacked: int = (
            setup.max_unacked_records
            if setup.HasField("max_unacked_records")
            else DEFAULT_MAX_UNACKED_RECORDS
        )
        if self._max_unacked < 1:
            raise ValueError("ConsumeSetup.max_unacked_records must be positive")
        self._owned: dict[_Key, _Owned] = {}
        self._revoking: dict[int, list[_Key]] = {}
        self._state = _State.DISCONNECTED
        self._closed = False
        self._half_closed = False
        # Records the app holds: delivered, not yet acked, on partitions still owned.
        self._unacked = 0
        # Credits granted to the server and not yet used by a delivered record.
        self._outstanding = 0

    def handle(self, event: core_pb2.ConsumeEvent) -> list[core_pb2.ConsumeAction]:
        """Applies one event and returns what the binding must do, in order."""
        actions: list[core_pb2.ConsumeAction] = []
        kind = event.WhichOneof("kind")
        if kind == "stream_opened":
            self._stream_opened(actions)
        elif kind == "received":
            received = event.received
            message = received.WhichOneof("msg")
            if message == "subscribed":
                self._subscribed(actions)
            elif message == "assigned":
                self._assigned(received.assigned, actions)
            elif message == "records":
                self._records(received.records, actions)
            elif message == "revoked":
                self._revoked(received.revoked, actions)
            elif message == "lost":
                self._lost(received.lost, actions)
        elif kind == "stream_ended":
            self._stream_ended(event.stream_ended, actions)
        elif kind == "ack":
            self._ack(event.ack, actions)
        elif kind == "revoke_done":
            self._revoke_done(event.revoke_done, actions)
        elif kind == "close":
            self._close(actions)
        return actions

    def _stream_opened(self, actions: list[core_pb2.ConsumeAction]) -> None:
        if self._state is _State.FAILED:
            return
        self._state = _State.SUBSCRIBING
        self._half_closed = False
        subscribe = odradek_pb2.Subscribe(topics=self._setup.topics)
        if self._setup.HasField("max_processing_ms"):
            subscribe.max_processing_ms = self._setup.max_processing_ms
        if self._setup.HasField("client"):
            subscribe.client.CopyFrom(self._setup.client)
        actions.append(_transmit(odradek_pb2.ConsumeRequest(subscribe=subscribe)))
        if self._closed:
            self._half_close(actions)

    def _subscribed(self, actions: list[core_pb2.ConsumeAction]) -> None:
        if self._state is not _State.SUBSCRIBING:
            return
        self._state = _State.ACTIVE
        self._outstanding = 0
        self._grant(actions)

    def _assigned(
        self, assigned: odradek_pb2.Assigned, actions: list[core_pb2.ConsumeAction]
    ) -> None:
        for partition in assigned.partitions:
            key = (partition.topic, partition.partition)
            previous = self._owned.get(key)
            self._owned[key] = _Owned(partition.assignment_epoch)
            if previous is not None:
                self._unacked -= len(previous.unacked)
        actions.append(core_pb2.ConsumeAction(assigned=assigned))
        self._grant(actions)

    def _records(
        self, batch: odradek_pb2.RecordBatch, actions: list[core_pb2.ConsumeAction]
    ) -> None:
        # Each record used one credit, whether or not it can still be handed to the app.
        self._outstanding = max(0, self._outstanding - len(batch.records))
        partition = self._owned.get((batch.topic, batch.partition))
        if partition is None or partition.epoch != batch.assignment_epoch:
            # Sent before the server learned the partition was given up. Its records will reach
            # whoever owns the partition next.
            self._grant(actions)
            return
        for record in batch.records:
            if record.offset not in partition.unacked:
                partition.unacked.add(record.offset)
                self._unacked += 1
        actions.append(core_pb2.ConsumeAction(deliver=batch))

    def _revoked(self, revoked: odradek_pb2.Revoked, actions: list[core_pb2.ConsumeAction]) -> None:
        # The partitions stay owned until the app answers: its final acks must still go through.
        self._revoking[revoked.revoke_id] = [(p.topic, p.partition) for p in revoked.partitions]
        actions.append(core_pb2.ConsumeAction(revoking=revoked))

    def _revoke_done(
        self, done: odradek_pb2.RevokeDone, actions: list[core_pb2.ConsumeAction]
    ) -> None:
        partitions = self._revoking.pop(done.revoke_id, None)
        if partitions is None:
            return
        for key in partitions:
            self._release(key)
        actions.append(_transmit(odradek_pb2.ConsumeRequest(revoke_done=done)))
        self._grant(actions)

    def _lost(self, lost: odradek_pb2.Lost, actions: list[core_pb2.ConsumeAction]) -> None:
        for partition in lost.partitions:
            self._release((partition.topic, partition.partition))
        actions.append(core_pb2.ConsumeAction(lost=lost))
        self._grant(actions)

    def _ack(self, ack: odradek_pb2.Ack, actions: list[core_pb2.ConsumeAction]) -> None:
        if self._state is not _State.ACTIVE:
            return
        partition = self._owned.get((ack.topic, ack.partition))
        if partition is None or partition.epoch != ack.assignment_epoch:
            # The partition was given up since these records were delivered. They will be
            # delivered again to its next owner; the server would ignore this ack anyway.
            return
        if ack.first_offset == ack.last_offset:
            # The common case, one record: no need to look at every unacked offset.
            acked = {ack.first_offset} & partition.unacked
        else:
            acked = {o for o in partition.unacked if ack.first_offset <= o <= ack.last_offset}
        if not acked:
            return
        partition.unacked -= acked
        self._unacked -= len(acked)
        actions.append(_transmit(odradek_pb2.ConsumeRequest(ack=ack)))
        self._grant(actions)

    def _stream_ended(
        self, ended: core_pb2.StreamEnded, actions: list[core_pb2.ConsumeAction]
    ) -> None:
        if self._state is _State.FAILED:
            return
        was_open = self._state is not _State.DISCONNECTED
        self._state = _State.DISCONNECTED
        if self._owned:
            # Whatever the app holds will be delivered again, here or elsewhere.
            action = core_pb2.ConsumeAction()
            for topic, partition in sorted(self._owned):
                action.lost.partitions.add(topic=topic, partition=partition)
            actions.append(action)
        self._owned.clear()
        self._revoking.clear()
        self._unacked = 0
        self._outstanding = 0
        if ended.status_code in _FATAL:
            self._state = _State.FAILED
            action = core_pb2.ConsumeAction()
            action.fatal.status_code = ended.status_code
            action.fatal.message = ended.message
            actions.append(action)
            return
        if not self._closed and was_open:
            actions.append(core_pb2.ConsumeAction(open_stream=core_pb2.OpenStream()))

    def _close(self, actions: list[core_pb2.ConsumeAction]) -> None:
        self._closed = True
        if self._state in (_State.SUBSCRIBING, _State.ACTIVE):
            self._half_close(actions)

    def _half_close(self, actions: list[core_pb2.ConsumeAction]) -> None:
        if not self._half_closed:
            self._half_closed = True
            actions.append(core_pb2.ConsumeAction(half_close=core_pb2.HalfClose()))

    def _release(self, key: _Key) -> None:
        gone = self._owned.pop(key, None)
        if gone is not None:
            self._unacked -= len(gone.unacked)

    def _grant(self, actions: list[core_pb2.ConsumeAction]) -> None:
        """Grants credits when at least half of the app's allowance is free.

        Granting in halves keeps the stream from carrying one Credits message
        per acked record.
        """
        if self._state is not _State.ACTIVE or self._closed:
            return
        free = self._max_unacked - self._unacked - self._outstanding
        if free * 2 >= self._max_unacked and free > 0:
            self._outstanding += free
            request = odradek_pb2.ConsumeRequest()
            request.credits.records = free
            actions.append(_transmit(request))


def _transmit(request: odradek_pb2.ConsumeRequest) -> core_pb2.ConsumeAction:
    return core_pb2.ConsumeAction(transmit=request)
