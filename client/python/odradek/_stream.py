"""What the producer and the consumer share: backoff, the outgoing side of a stream, identity."""

from __future__ import annotations

import asyncio
import random
from collections.abc import AsyncIterator, Coroutine
from typing import Any, Generic, TypeVar

import grpc
import grpc.aio

from odradek.client.v1 import core_pb2
from odradek.kafka.v1 import odradek_pb2

#: Replaced at release time; see docs/adr/0004.
VERSION = "0.0.0-dev"

T = TypeVar("T")


def client_info() -> odradek_pb2.ClientInfo:
    """What this library tells the server about itself."""
    return odradek_pb2.ClientInfo(language="python", version=VERSION)


class Backoff:
    """Delay between attempts to open a stream: doubling from 100 ms to 5 s, with jitter."""

    _INITIAL_MS = 100
    _MAX_MS = 5000

    def __init__(self) -> None:
        self._next_ms = self._INITIAL_MS

    def next(self) -> float:
        """The delay before the next attempt, in seconds."""
        delay_ms = self._next_ms // 2 + random.randint(0, self._next_ms // 2)
        self._next_ms = min(self._MAX_MS, self._next_ms * 2)
        return delay_ms / 1000

    def reset(self) -> None:
        """Called when a stream got as far as its handshake reply."""
        self._next_ms = self._INITIAL_MS


class Outbox(Generic[T]):
    """The requests of one stream, in order.

    gRPC reads them through :meth:`requests`; the binding adds to them without
    waiting, which is what lets it carry out a core's actions in one step.
    The core's window bounds how many can be waiting.
    """

    def __init__(self) -> None:
        self._queue: asyncio.Queue[T | None] = asyncio.Queue()

    def write(self, message: T) -> None:
        self._queue.put_nowait(message)

    def half_close(self) -> None:
        self._queue.put_nowait(None)

    async def requests(self) -> AsyncIterator[T]:
        while True:
            message = await self._queue.get()
            if message is None:
                return
            yield message


def stream_ended(code: str, message: str | None) -> core_pb2.StreamEnded:
    ended = core_pb2.StreamEnded(status_code=code)
    if message is not None:
        ended.message = message
    return ended


def _now(coroutine: Coroutine[Any, Any, T]) -> T | None:
    """The result of a coroutine that has nothing to wait for; None if it would wait."""
    try:
        coroutine.send(None)
    except StopIteration as stop:
        value: T = stop.value
        return value
    coroutine.close()
    return None


class StatusWatch:
    """Remembers the status a call ended with, at the moment it ended.

    When the server ends a stream while the client is writing a request to it,
    grpc.aio (1.84) first records the status the server sent and then replaces
    it with INTERNAL, "Internal error from Core", because the write failed.
    The stream is then reported as INTERNAL, whatever the server said. That
    status decides whether the core reconnects (UNAVAILABLE) or gives up
    (ABORTED, INVALID_ARGUMENT), so the one the server sent must not be lost.

    A call's done callbacks run when its status is first set. Its code() and
    details() have nothing to wait for by then, so they can be read on the
    spot, before anything replaces them.
    """

    def __init__(self, call: Any) -> None:
        self._first: tuple[grpc.StatusCode, str | None] | None = None
        call.add_done_callback(self._done)

    def _done(self, call: Any) -> None:
        if self._first is None:
            code = _now(call.code())
            if code is not None:
                self._first = (code, _now(call.details()))

    def ended(self, error: grpc.aio.AioRpcError) -> core_pb2.StreamEnded:
        """The StreamEnded for a stream that failed with `error`."""
        code, details = error.code(), error.details()
        if (
            code is grpc.StatusCode.INTERNAL
            and details == "Internal error from Core"
            and self._first is not None
        ):
            code, details = self._first
        return stream_ended(code.name, details)
