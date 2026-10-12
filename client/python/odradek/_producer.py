"""The producer: the produce core bound to a gRPC stream on the event loop."""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections import deque
from collections.abc import Iterable, Sequence
from types import TracebackType
from typing import TYPE_CHECKING, Any

import grpc.aio
from google.protobuf.message import Message

from odradek._stream import Backoff, Outbox, StatusWatch, client_info, stream_ended
from odradek._types import (
    BufferFullError,
    ClientClosedError,
    ProcessGuard,
    ProduceError,
    SendResult,
    Write,
)
from odradek.client.v1 import core_pb2
from odradek.core.produce import ProduceCore
from odradek.kafka.v1 import odradek_pb2

if TYPE_CHECKING:
    from odradek.kafka.v1 import odradek_pb2_grpc

_LOG = logging.getLogger("odradek")

#: A key or a value: raw bytes, text (sent as UTF-8), or a protobuf message
#: (serialized, for a topic whose contract declares that type).
Payload = bytes | str | Message

_CLOSE_MESSAGE = "the producer was closed before the record was answered; it may have been produced"


def _payload(value: Payload) -> bytes:
    if isinstance(value, Message):
        return value.SerializeToString()
    if isinstance(value, str):
        return value.encode("utf-8")
    return bytes(value)


class Producer:
    """Sends records through the server.

    :meth:`send` returns when every cluster the topic requires has
    acknowledged the record, never earlier. Records sent from one task, or
    started in order (``asyncio.gather(*(p.send(...) for ...))``), reach each
    partition in the order they were sent. A producer survives the stream
    breaking: it reconnects and resends without duplicating, and fails a
    record as outcome unknown only when the server restarted before answering
    for it.

    Made by :meth:`OdradekClient.producer`. Belongs to the event loop it was
    made on; not safe to use from other threads.
    """

    def __init__(
        self,
        stub: odradek_pb2_grpc.OdradekServiceStub,
        guard: ProcessGuard,
        *,
        max_block: float,
        max_buffered_records: int | None,
        max_buffered_bytes: int | None,
    ) -> None:
        self._loop = asyncio.get_running_loop()
        self._stub = stub
        self._guard = guard
        self._max_block = max_block
        setup = core_pb2.ProduceSetup(session_id=str(uuid.uuid4()), client=client_info())
        if max_buffered_records:
            setup.max_buffered_records = max_buffered_records
        if max_buffered_bytes:
            setup.max_buffered_bytes = max_buffered_bytes
        self._core = ProduceCore(setup)
        self._futures: dict[int, asyncio.Future[SendResult]] = {}
        self._backoff = Backoff()
        self._drained: asyncio.Future[None] = self._loop.create_future()
        # Sends waiting for room in the window, oldest first. Only the oldest is offered to the
        # core, so a record never overtakes one that was sent before it.
        self._waiting: deque[asyncio.Event] = deque()
        self._outbox: Outbox[odradek_pb2.ProduceRequest] | None = None
        self._call: Any = None
        self._reader: asyncio.Task[None] | None = None
        self._reopen: asyncio.TimerHandle | None = None
        self._generation = 0
        self._closed = False
        self._shut_down = False
        self._fatal: str | None = None
        self._open_stream()

    async def send(
        self,
        topic: str,
        *,
        key: Payload | None = None,
        value: Payload | None = None,
        headers: Iterable[tuple[str, bytes | None]] = (),
        partition: int | None = None,
        timestamp_ms: int | None = None,
    ) -> SendResult:
        """Sends a record and waits for its acknowledgement.

        Which cluster or clusters the topic is written to, and in which format
        it is stored, is the server's configuration. Without a key or value
        the record is a null-keyed tombstone. Headers keep their order and a
        key may repeat. Without ``partition`` the server's partitioner
        decides, by key when present; without ``timestamp_ms`` the server uses
        its clock when it sends.

        Waits while the window is full, up to ``max_block``.

        Cancelling the call stops the waiting, not the record: once taken it
        is sent, and its outcome is no longer reported.

        Returns:
            Where the record was written.

        Raises:
            ProduceError: the record was not acknowledged; it says whether the
                record may have been written, and by which clusters it was.
            BufferFullError: the window stayed full, or the record can never fit.
            ClientClosedError: the producer is closed or was refused by the server.
            ForkedProcessError: called in a process other than the client's.
        """
        self._guard.check()
        record = odradek_pb2.ProduceRecord(topic=topic)
        if key is not None:
            record.key = _payload(key)
        if value is not None:
            record.value = _payload(value)
        for header_key, header_value in headers:
            header = record.headers.add(key=header_key)
            if header_value is not None:
                header.value = header_value
        if partition is not None:
            record.partition = partition
        if timestamp_ms is not None:
            record.timestamp_ms = timestamp_ms
        future = await self._offer(core_pb2.ProduceEvent(send=record))
        return await future

    async def _offer(self, event: core_pb2.ProduceEvent) -> asyncio.Future[SendResult]:
        """Hands a record to the core, waiting for room; returns the future of its outcome."""
        deadline = self._loop.time() + self._max_block
        # My place in line, once I have had to wait.
        waiter: asyncio.Event | None = None
        try:
            while True:
                my_turn = not self._waiting if waiter is None else self._waiting[0] is waiter
                if my_turn:
                    actions = self._core.handle(event)
                    first = actions[0]
                    if first.HasField("accepted"):
                        future: asyncio.Future[SendResult] = self._loop.create_future()
                        self._futures[first.accepted.seq] = future
                        self._perform(actions)
                        return future
                    reason = first.rejected.reason
                    if reason == core_pb2.SEND_REJECTION_TOO_LARGE:
                        raise BufferFullError(
                            "the record is larger than the whole in-flight window"
                        )
                    if reason == core_pb2.SEND_REJECTION_CLOSED:
                        raise ClientClosedError(
                            "the producer is closed"
                            if self._fatal is None
                            else f"the server refused the producer: {self._fatal}"
                        )
                remaining = deadline - self._loop.time()
                if remaining <= 0:
                    raise BufferFullError(
                        f"the in-flight window stayed full for {self._max_block}s"
                    )
                if waiter is None:
                    waiter = asyncio.Event()
                    self._waiting.append(waiter)
                waiter.clear()
                try:
                    async with asyncio.timeout(remaining):
                        await waiter.wait()
                except TimeoutError:
                    pass
        finally:
            if waiter is not None:
                self._waiting.remove(waiter)
                # Whoever is next may fit, or must learn that the producer is closed.
                self._wake()

    def _wake(self) -> None:
        """Lets the oldest waiting send try again."""
        if self._waiting:
            self._waiting[0].set()

    async def close(self, grace: float = 60.0) -> None:
        """Stops taking records, waits for the results of those already sent, ends the stream.

        Waits up to ``grace`` seconds. A record still unanswered then fails
        with a :class:`ProduceError` whose outcome is unknown: the server may
        still produce it.
        """
        self._guard.check()
        if self._shut_down:
            return
        self._apply(core_pb2.ProduceEvent(close=core_pb2.Close()), None)
        try:
            async with asyncio.timeout(grace):
                await asyncio.shield(self._drained)
        except TimeoutError:
            pass
        self._shut_down = True
        self._generation += 1
        if self._reopen is not None:
            self._reopen.cancel()
        if self._call is not None:
            self._call.cancel()
        self._outbox = None
        self._call = None
        reader, self._reader = self._reader, None
        if reader is not None:
            reader.cancel()
            await asyncio.gather(reader, return_exceptions=True)
        for seq, future in self._futures.items():
            if not future.done():
                future.set_exception(
                    ProduceError(odradek_pb2.ERROR_CODE_OUTCOME_UNKNOWN, _CLOSE_MESSAGE)
                )
                _LOG.warning("producer closed with record %d unanswered", seq)
        self._futures.clear()

    async def __aenter__(self) -> Producer:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.close()

    def _apply(self, event: core_pb2.ProduceEvent, from_generation: int | None) -> None:
        """Applies an event that came from the stream of a generation, or from the app (None)."""
        if from_generation is not None and from_generation != self._generation:
            # An event of a stream that has since been replaced.
            return
        kind = event.WhichOneof("kind")
        if kind == "close":
            self._closed = True
        elif kind == "stream_ended":
            self._outbox = None
            self._call = None
        elif kind == "received" and event.received.HasField("ready"):
            self._backoff.reset()
        self._perform(self._core.handle(event))
        if self._closed and not self._futures and not self._drained.done():
            self._drained.set_result(None)
        self._wake()

    def _perform(self, actions: Sequence[core_pb2.ProduceAction]) -> None:
        for action in actions:
            kind = action.WhichOneof("kind")
            if kind == "transmit":
                if self._outbox is not None:
                    self._outbox.write(action.transmit)
            elif kind == "completed":
                result = action.completed.result
                future = self._futures.pop(result.seq, None)
                if future is not None and not future.done():
                    _complete(future, result)
            elif kind == "open_stream":
                self._reopen = self._loop.call_later(self._backoff.next(), self._open_stream)
            elif kind == "half_close":
                if self._outbox is not None:
                    self._outbox.half_close()
            elif kind == "fatal":
                self._fatal = f"{action.fatal.status_code}: {action.fatal.message}"
                _LOG.error("the server refused the producer: %s", self._fatal)

    def _open_stream(self) -> None:
        """Opens a stream and tells the core."""
        self._reopen = None
        if self._shut_down:
            return
        self._generation += 1
        outbox: Outbox[odradek_pb2.ProduceRequest] = Outbox()
        call = self._stub.Produce(outbox.requests())
        self._outbox = outbox
        self._call = call
        self._reader = self._loop.create_task(self._read(call, self._generation))
        self._perform(
            self._core.handle(core_pb2.ProduceEvent(stream_opened=core_pb2.StreamOpened()))
        )

    async def _read(self, call: Any, mine: int) -> None:
        """Turns what one stream delivers into events, until it ends."""
        watch = StatusWatch(call)
        try:
            async for response in call:
                self._apply(core_pb2.ProduceEvent(received=response), mine)
            ended = stream_ended("OK", None)
        except grpc.aio.AioRpcError as e:
            ended = watch.ended(e)
        except Exception as e:
            _LOG.exception("the produce stream failed unexpectedly")
            ended = stream_ended("UNKNOWN", str(e))
        self._apply(core_pb2.ProduceEvent(stream_ended=ended), mine)


def _complete(future: asyncio.Future[SendResult], result: odradek_pb2.ProduceResult) -> None:
    writes = tuple(
        Write(w.cluster, w.topic, w.partition, w.offset, w.timestamp_ms) for w in result.writes
    )
    if result.HasField("error"):
        future.set_exception(
            ProduceError(
                result.error.code, result.error.message, result.error.kafka_exception, writes
            )
        )
    else:
        future.set_result(SendResult(writes))
