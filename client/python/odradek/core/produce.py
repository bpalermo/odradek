"""The produce side of the client protocol, with no I/O: events in, actions out.

A port of ``ProduceCore.java`` (``//client/core``): the same state, the same
rules, the same order of actions. The conformance scenarios hold the two
together; change neither without the other.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto

from odradek.client.v1 import core_pb2
from odradek.kafka.v1 import odradek_pb2

DEFAULT_MAX_BUFFERED_RECORDS = 1000
DEFAULT_MAX_BUFFERED_BYTES = 16 * 1024 * 1024

# Stream statuses that a new stream would meet again.
_FATAL = frozenset(
    {
        "INVALID_ARGUMENT",
        "ABORTED",
        "PERMISSION_DENIED",
        "UNAUTHENTICATED",
        "UNIMPLEMENTED",
        "FAILED_PRECONDITION",
    }
)


class _State(Enum):
    DISCONNECTED = auto()
    """No stream."""
    OPENING = auto()
    """A stream is open and ProduceOpen was sent; ProduceReady has not arrived."""
    READY = auto()
    """Records may be transmitted."""
    FAILED = auto()
    """The server refused the producer for good."""


@dataclass(slots=True)
class _Pending:
    record: odradek_pb2.ProduceRecord
    size: int
    transmitted: bool = False
    # Whether the record was ever written to a stream; if so the server may have produced it.
    ever_transmitted: bool = False


class ProduceCore:
    """Assigns sequence numbers and keeps every record until its result arrives.

    It stays inside the window the server announced, resends after a
    reconnect, and gives up on records the server can no longer answer for. A
    binding opens the first stream on its own, reports what happens as events
    and carries out the returned actions in order.

    Not safe for concurrent use: the binding calls :meth:`handle` from one
    place at a time.
    """

    def __init__(self, setup: core_pb2.ProduceSetup) -> None:
        if not setup.session_id:
            raise ValueError("ProduceSetup.session_id is required")
        self._setup = setup
        # Keyed by seq. Sequence numbers only grow, so insertion order is seq order.
        self._pending: dict[int, _Pending] = {}
        self._state = _State.DISCONNECTED
        self._next_seq = 1
        self._pending_bytes = 0
        self._in_flight_records = 0
        self._in_flight_bytes = 0
        self._max_records: int = (
            setup.max_buffered_records
            if setup.HasField("max_buffered_records")
            else DEFAULT_MAX_BUFFERED_RECORDS
        )
        self._max_bytes: int = (
            setup.max_buffered_bytes
            if setup.HasField("max_buffered_bytes")
            else DEFAULT_MAX_BUFFERED_BYTES
        )
        self._server_epoch: str | None = None
        self._closed = False
        self._half_closed = False

    def handle(self, event: core_pb2.ProduceEvent) -> list[core_pb2.ProduceAction]:
        """Applies one event and returns what the binding must do, in order."""
        actions: list[core_pb2.ProduceAction] = []
        kind = event.WhichOneof("kind")
        if kind == "stream_opened":
            self._stream_opened(actions)
        elif kind == "received":
            message = event.received.WhichOneof("msg")
            if message == "ready":
                self._ready(event.received.ready, actions)
            elif message == "result":
                self._result(event.received.result, actions)
        elif kind == "stream_ended":
            self._stream_ended(event.stream_ended, actions)
        elif kind == "send":
            self._send(event.send, actions)
        elif kind == "close":
            self._close(actions)
        return actions

    def _stream_opened(self, actions: list[core_pb2.ProduceAction]) -> None:
        if self._state is _State.FAILED:
            return
        self._state = _State.OPENING
        self._half_closed = False
        opening = odradek_pb2.ProduceOpen(session_id=self._setup.session_id)
        if self._setup.HasField("client"):
            opening.client.CopyFrom(self._setup.client)
        actions.append(_transmit(odradek_pb2.ProduceRequest(open=opening)))

    def _ready(
        self, ready: odradek_pb2.ProduceReady, actions: list[core_pb2.ProduceAction]
    ) -> None:
        if self._state is not _State.OPENING:
            return
        if self._server_epoch is not None and self._server_epoch != ready.server_epoch:
            # The server restarted and forgot the session. A record it was sent may or may not
            # have been produced, and it can no longer say. Resending could duplicate, so that
            # is left to the app. Records never sent are safe to send now.
            for seq in [s for s, p in self._pending.items() if p.ever_transmitted]:
                record = self._pending.pop(seq)
                self._pending_bytes -= record.size
                actions.append(
                    _completed(
                        seq,
                        "the server restarted before answering; the record may have been produced",
                    )
                )
        self._server_epoch = ready.server_epoch
        self._max_records = ready.max_in_flight_records
        self._max_bytes = ready.max_in_flight_bytes
        self._state = _State.READY
        self._pump(actions)
        self._finish_if_drained(actions)

    def _result(
        self, result: odradek_pb2.ProduceResult, actions: list[core_pb2.ProduceAction]
    ) -> None:
        record = self._pending.pop(result.seq, None)
        if record is None:
            # A result repeated after a resend, or for a record already given up on.
            return
        self._pending_bytes -= record.size
        if record.transmitted:
            self._in_flight_records -= 1
            self._in_flight_bytes -= record.size
        actions.append(core_pb2.ProduceAction(completed=core_pb2.SendCompleted(result=result)))
        self._pump(actions)
        self._finish_if_drained(actions)

    def _stream_ended(
        self, ended: core_pb2.StreamEnded, actions: list[core_pb2.ProduceAction]
    ) -> None:
        if self._state is _State.FAILED:
            return
        was_open = self._state is not _State.DISCONNECTED
        self._state = _State.DISCONNECTED
        self._in_flight_records = 0
        self._in_flight_bytes = 0
        for record in self._pending.values():
            record.transmitted = False
        if ended.status_code in _FATAL:
            self._state = _State.FAILED
            for seq, record in self._pending.items():
                actions.append(
                    _completed(
                        seq,
                        f"the stream ended with {ended.status_code} before the record was"
                        " answered; it may have been produced"
                        if record.ever_transmitted
                        else f"the stream ended with {ended.status_code}; the record was not sent",
                    )
                )
            self._pending.clear()
            self._pending_bytes = 0
            action = core_pb2.ProduceAction()
            action.fatal.status_code = ended.status_code
            action.fatal.message = ended.message
            actions.append(action)
            return
        if self._closed and not self._pending:
            return
        if was_open:
            actions.append(core_pb2.ProduceAction(open_stream=core_pb2.OpenStream()))

    def _send(
        self, record: odradek_pb2.ProduceRecord, actions: list[core_pb2.ProduceAction]
    ) -> None:
        if self._closed or self._state is _State.FAILED:
            actions.append(_rejected(core_pb2.SEND_REJECTION_CLOSED))
            return
        size = record_size(record)
        if size > self._max_bytes:
            actions.append(_rejected(core_pb2.SEND_REJECTION_TOO_LARGE))
            return
        if len(self._pending) >= self._max_records or self._pending_bytes + size > self._max_bytes:
            actions.append(_rejected(core_pb2.SEND_REJECTION_WINDOW_FULL))
            return
        seq = self._next_seq
        self._next_seq += 1
        mine = odradek_pb2.ProduceRecord()
        mine.CopyFrom(record)
        mine.seq = seq
        self._pending[seq] = _Pending(mine, size)
        self._pending_bytes += size
        action = core_pb2.ProduceAction()
        action.accepted.seq = seq
        actions.append(action)
        self._pump(actions)

    def _close(self, actions: list[core_pb2.ProduceAction]) -> None:
        self._closed = True
        self._finish_if_drained(actions)

    def _pump(self, actions: list[core_pb2.ProduceAction]) -> None:
        """Transmits waiting records, oldest first, for as long as the server's window has room."""
        if self._state is not _State.READY:
            return
        low_watermark = next(iter(self._pending), 0)
        for record in self._pending.values():
            if record.transmitted:
                continue
            if (
                self._in_flight_records >= self._max_records
                or self._in_flight_bytes + record.size > self._max_bytes
            ):
                # In order, always: a later record never overtakes one that does not fit yet.
                return
            record.transmitted = True
            record.ever_transmitted = True
            self._in_flight_records += 1
            self._in_flight_bytes += record.size
            request = odradek_pb2.ProduceRequest()
            request.record.CopyFrom(record.record)
            request.record.low_watermark = low_watermark
            actions.append(_transmit(request))

    def _finish_if_drained(self, actions: list[core_pb2.ProduceAction]) -> None:
        if (
            self._closed
            and not self._half_closed
            and not self._pending
            and self._state is not _State.DISCONNECTED
            and self._state is not _State.FAILED
        ):
            self._half_closed = True
            actions.append(core_pb2.ProduceAction(half_close=core_pb2.HalfClose()))


def _transmit(request: odradek_pb2.ProduceRequest) -> core_pb2.ProduceAction:
    return core_pb2.ProduceAction(transmit=request)


def _rejected(reason: core_pb2.SendRejection) -> core_pb2.ProduceAction:
    action = core_pb2.ProduceAction()
    action.rejected.reason = reason
    return action


def _completed(seq: int, message: str) -> core_pb2.ProduceAction:
    action = core_pb2.ProduceAction()
    result = action.completed.result
    result.seq = seq
    result.error.code = odradek_pb2.ERROR_CODE_OUTCOME_UNKNOWN
    result.error.message = message
    return action


def record_size(record: odradek_pb2.ProduceRecord) -> int:
    """The bytes a record counts for in the window: key, value, header keys and header values."""
    size = len(record.key) + len(record.value)
    for header in record.headers:
        size += len(header.key.encode("utf-8")) + len(header.value)
    return size
