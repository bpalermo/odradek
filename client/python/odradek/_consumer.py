"""The consumer: the consume core bound to a gRPC stream and one handler task."""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from collections.abc import Awaitable, Callable, Iterable, Sequence
from dataclasses import dataclass
from types import TracebackType
from typing import TYPE_CHECKING, Any

import grpc.aio

from odradek._stream import Backoff, Outbox, StatusWatch, client_info, stream_ended
from odradek._types import (
    ConsumerFailedError,
    Header,
    Partition,
    ProcessGuard,
    ReceivedRecord,
)
from odradek.client.v1 import core_pb2
from odradek.core.consume import ConsumeCore
from odradek.kafka.v1 import odradek_pb2

if TYPE_CHECKING:
    from odradek.kafka.v1 import odradek_pb2_grpc

_LOG = logging.getLogger("odradek")

#: Processes consumed records.
#:
#: Called for one record at a time, in offset order within a partition.
#: Returning normally marks the record as processed: its offset may then be
#: committed. Raising stops the consumer without marking it; the record, and
#: those after it, are delivered again to whoever reads the partition next.
RecordHandler = Callable[[ReceivedRecord], Awaitable[None]]

# How long close() waits for the handler, and then for the server to end the stream.
_CLOSE_STEP_SECONDS = 30.0


class ConsumerListener:
    """Hears about changes of a :class:`Consumer`. Subclass and override what matters.

    All methods are awaited on the consumer's own task, between records, never
    while the handler is running.
    """

    async def assigned(self, partitions: Sequence[Partition]) -> None:
        """These partitions now belong to this consumer."""

    async def revoked(self, partitions: Sequence[Partition]) -> None:
        """These partitions are being taken away.

        Records of them already handled are committed; records not yet handled
        go to the next owner. Finish or drop any work kept outside the handler
        before returning: the server waits for this method, up to its revoke
        deadline.
        """

    async def lost(self, partitions: Sequence[Partition]) -> None:
        """These partitions are gone without notice, for example because the stream broke."""

    async def failed(self, cause: BaseException) -> None:
        """The consumer stopped for good.

        The handler raised (``cause`` is what it raised), or the server refused
        or evicted the consumer (a :class:`ConsumerFailedError`). Nothing more
        is delivered. An app that reports readiness should report not ready.
        """


# Something for the handler task to do, in order.
@dataclass(frozen=True, slots=True)
class _Records:
    batch: odradek_pb2.RecordBatch


@dataclass(frozen=True, slots=True)
class _Assignment:
    partitions: tuple[Partition, ...]


@dataclass(frozen=True, slots=True)
class _Revocation:
    revoked: odradek_pb2.Revoked


@dataclass(frozen=True, slots=True)
class _Loss:
    partitions: tuple[Partition, ...]


@dataclass(frozen=True, slots=True)
class _Failure:
    cause: BaseException


@dataclass(frozen=True, slots=True)
class _Stop:
    pass


_Work = _Records | _Assignment | _Revocation | _Loss | _Failure | _Stop


class Consumer:
    """Reads records through the server and hands them to a handler.

    A record is marked as processed when the handler returns for it, and only
    processed records are committed, so a crash or a rebalance redelivers at
    most what was handed over and not finished. A consumer survives the stream
    breaking: it reports its partitions lost, reconnects and resumes from the
    committed offsets.

    Records are handled one at a time, on one task owned by the consumer.

    Made by :meth:`OdradekClient.consumer`. Belongs to the event loop it was
    made on; not safe to use from other threads.
    """

    def __init__(
        self,
        stub: odradek_pb2_grpc.OdradekServiceStub,
        guard: ProcessGuard,
        topics: Iterable[str],
        handler: RecordHandler,
        listener: ConsumerListener | None,
        *,
        max_processing: float | None,
        max_unacked_records: int | None,
    ) -> None:
        self._loop = asyncio.get_running_loop()
        self._stub = stub
        self._guard = guard
        self._handler = handler
        self._listener = listener if listener is not None else ConsumerListener()
        setup = core_pb2.ConsumeSetup(topics=list(topics), client=client_info())
        if max_processing:
            setup.max_processing_ms = int(max_processing * 1000)
        if max_unacked_records:
            setup.max_unacked_records = max_unacked_records
        self._core = ConsumeCore(setup)
        self._backoff = Backoff()
        self._work: deque[_Work] = deque()
        self._has_work = asyncio.Event()
        # Partitions whose records may be handled, with the epoch they are owned under.
        self._live: dict[Partition, int] = {}
        # Partitions being taken away: their records still queued are skipped.
        self._leaving: set[Partition] = set()
        self._ended: asyncio.Future[None] = self._loop.create_future()
        self._outbox: Outbox[odradek_pb2.ConsumeRequest] | None = None
        self._call: Any = None
        self._reader: asyncio.Task[None] | None = None
        self._reopen: asyncio.TimerHandle | None = None
        self._closing: asyncio.Task[None] | None = None
        self._generation = 0
        self._shut_down = False
        self._stopped = False
        self._worker = self._loop.create_task(
            self._run(), name="odradek-consumer-" + ",".join(setup.topics)
        )
        self._open_stream()

    async def close(self) -> None:
        """Stops handling records after the one in progress and leaves the consumer group.

        Lets the server commit what was processed. Waits up to 30 seconds for
        the handler to finish its record (then cancels it) and up to 30 more
        for the server. Called from the handler or a listener, it returns at
        once and the consumer closes when that call returns.
        """
        self._guard.check()
        if self._closing is None:
            self._closing = self._loop.create_task(self._close())
        if asyncio.current_task() is not self._worker:
            await asyncio.shield(self._closing)

    async def __aenter__(self) -> Consumer:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.close()

    async def _close(self) -> None:
        self._stopped = True
        self._push_first(_Stop())
        try:
            async with asyncio.timeout(_CLOSE_STEP_SECONDS):
                await asyncio.shield(self._worker)
        except TimeoutError:
            _LOG.warning("the record handler did not return within 30s of close; cancelling it")
            self._worker.cancel()
        except Exception:
            _LOG.exception("the consumer's handler task failed")
        self._apply(core_pb2.ConsumeEvent(close=core_pb2.Close()), None)
        if self._outbox is not None:
            # A stream is open and was just half-closed: the server commits and completes it.
            try:
                async with asyncio.timeout(_CLOSE_STEP_SECONDS):
                    await asyncio.shield(self._ended)
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
        await asyncio.gather(*filter(None, [reader, self._worker]), return_exceptions=True)

    def _push(self, work: _Work) -> None:
        self._work.append(work)
        self._has_work.set()

    def _push_first(self, work: _Work) -> None:
        self._work.appendleft(work)
        self._has_work.set()

    async def _run(self) -> None:
        """The handler task."""
        while True:
            while not self._work:
                self._has_work.clear()
                await self._has_work.wait()
            work = self._work.popleft()
            try:
                if isinstance(work, _Records):
                    await self._handle(work.batch)
                elif isinstance(work, _Assignment):
                    await self._listener.assigned(work.partitions)
                elif isinstance(work, _Revocation):
                    await self._revoke(work.revoked)
                elif isinstance(work, _Loss):
                    await self._listener.lost(work.partitions)
                elif isinstance(work, _Failure):
                    await self._listener.failed(work.cause)
                    return
                else:
                    return
            except Exception:
                _LOG.exception("a consumer listener raised")
                if isinstance(work, _Failure):
                    return

    async def _handle(self, batch: odradek_pb2.RecordBatch) -> None:
        partition = Partition(batch.topic, batch.partition)
        for record in batch.records:
            if (
                self._stopped
                or self._live.get(partition) != batch.assignment_epoch
                or partition in self._leaving
            ):
                # Not ours to process any more. Unacked, so it is delivered again elsewhere.
                return
            try:
                await self._handler(_received(batch, record))
            except Exception as e:
                # Not acked, and nothing after it is processed: the order within the partition
                # holds for whoever reads it next.
                _LOG.error(
                    "the handler raised for %s offset %d; stopping the consumer",
                    partition,
                    record.offset,
                    exc_info=e,
                )
                self._stopped = True
                self._work.clear()
                self._push_first(_Failure(e))
                if self._closing is None:
                    self._closing = self._loop.create_task(self._close())
                return
            self._apply(
                core_pb2.ConsumeEvent(
                    ack=odradek_pb2.Ack(
                        topic=batch.topic,
                        partition=batch.partition,
                        assignment_epoch=batch.assignment_epoch,
                        first_offset=record.offset,
                        last_offset=record.offset,
                    )
                ),
                None,
            )

    async def _revoke(self, revoked: odradek_pb2.Revoked) -> None:
        partitions = _partitions(revoked.partitions)
        try:
            await self._listener.revoked(partitions)
        finally:
            for partition in partitions:
                self._live.pop(partition, None)
                self._leaving.discard(partition)
            self._apply(
                core_pb2.ConsumeEvent(
                    revoke_done=odradek_pb2.RevokeDone(revoke_id=revoked.revoke_id)
                ),
                None,
            )

    def _apply(self, event: core_pb2.ConsumeEvent, from_generation: int | None) -> None:
        """Applies an event that came from the stream of a generation, or from the app (None)."""
        if from_generation is not None and from_generation != self._generation:
            return
        kind = event.WhichOneof("kind")
        if kind == "stream_ended":
            self._outbox = None
            self._call = None
        elif kind == "received" and event.received.HasField("subscribed"):
            self._backoff.reset()
        self._perform(self._core.handle(event))
        if kind == "stream_ended" and (self._stopped or self._shut_down) and not self._ended.done():
            self._ended.set_result(None)

    def _perform(self, actions: Sequence[core_pb2.ConsumeAction]) -> None:
        """Carries out actions. App code runs later, on the handler task."""
        for action in actions:
            kind = action.WhichOneof("kind")
            if kind == "transmit":
                if self._outbox is not None:
                    self._outbox.write(action.transmit)
            elif kind == "assigned":
                assigned = []
                for p in action.assigned.partitions:
                    partition = Partition(p.topic, p.partition)
                    self._leaving.discard(partition)
                    self._live[partition] = p.assignment_epoch
                    assigned.append(partition)
                self._push(_Assignment(tuple(assigned)))
            elif kind == "deliver":
                self._push(_Records(action.deliver))
            elif kind == "revoking":
                self._leaving.update(_partitions(action.revoking.partitions))
                # Ahead of the records already queued: the server is waiting for the answer.
                self._push_first(_Revocation(action.revoking))
            elif kind == "lost":
                partitions = _partitions(action.lost.partitions)
                for partition in partitions:
                    self._live.pop(partition, None)
                self._push_first(_Loss(partitions))
            elif kind == "open_stream":
                self._reopen = self._loop.call_later(self._backoff.next(), self._open_stream)
            elif kind == "half_close":
                if self._outbox is not None:
                    self._outbox.half_close()
            elif kind == "fatal":
                self._stopped = True
                self._push_first(
                    _Failure(ConsumerFailedError(action.fatal.status_code, action.fatal.message))
                )

    def _open_stream(self) -> None:
        """Opens a stream and tells the core."""
        self._reopen = None
        if self._shut_down or self._stopped:
            return
        self._generation += 1
        outbox: Outbox[odradek_pb2.ConsumeRequest] = Outbox()
        call = self._stub.Consume(outbox.requests())
        self._outbox = outbox
        self._call = call
        self._reader = self._loop.create_task(self._read(call, self._generation))
        self._perform(
            self._core.handle(core_pb2.ConsumeEvent(stream_opened=core_pb2.StreamOpened()))
        )

    async def _read(self, call: Any, mine: int) -> None:
        """Turns what one stream delivers into events, until it ends."""
        watch = StatusWatch(call)
        try:
            async for response in call:
                self._apply(core_pb2.ConsumeEvent(received=response), mine)
            ended = stream_ended("OK", None)
        except grpc.aio.AioRpcError as e:
            ended = watch.ended(e)
        except Exception as e:
            _LOG.exception("the consume stream failed unexpectedly")
            ended = stream_ended("UNKNOWN", str(e))
        self._apply(core_pb2.ConsumeEvent(stream_ended=ended), mine)


def _received(batch: odradek_pb2.RecordBatch, record: odradek_pb2.ConsumedRecord) -> ReceivedRecord:
    return ReceivedRecord(
        topic=batch.topic,
        partition=batch.partition,
        offset=record.offset,
        timestamp_ms=record.timestamp_ms,
        key=record.key if record.HasField("key") else None,
        value=record.value if record.HasField("value") else None,
        headers=tuple(
            Header(h.key, h.value if h.HasField("value") else None) for h in record.headers
        ),
        decode_error=record.decode_error if record.HasField("decode_error") else None,
    )


def _partitions(partitions: Iterable[odradek_pb2.TopicPartition]) -> tuple[Partition, ...]:
    return tuple(Partition(p.topic, p.partition) for p in partitions)
