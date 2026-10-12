"""The entry point: a connection to the server, from which producers and consumers are made."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Iterable
from types import TracebackType

import grpc.aio

from odradek._consumer import Consumer, ConsumerListener, RecordHandler
from odradek._producer import Producer
from odradek._types import ProcessGuard
from odradek.kafka.v1 import odradek_pb2_grpc

# gRPC waits between its own attempts to connect, from 1 s up to 2 min by
# default, and fails a new stream at once while it waits. The producer and the
# consumer retry with their own backoff (100 ms to 5 s), so the channel must be
# ready to try again sooner than they ask, or a restarted server would be
# found late.
_CHANNEL_OPTIONS = (
    ("grpc.initial_reconnect_backoff_ms", 100),
    ("grpc.min_reconnect_backoff_ms", 100),
    ("grpc.max_reconnect_backoff_ms", 1000),
)


class OdradekClient:
    """A connection to the Odradek server in the same pod.

    ::

        async with OdradekClient.connect("/var/run/kafka/kafka.sock") as client:
            async with client.producer() as producer:
                await producer.send("orders.created", key=order_id, value=order)

    The app names topics only. Clusters, consumer groups, the stored format
    and encryption are the server's configuration.

    A client belongs to the event loop and the process it was made in. It
    cannot be carried across ``fork()``: under a prefork server (gunicorn,
    Celery) make it in the worker. Used in another process, every call raises
    :class:`ForkedProcessError`.
    """

    def __init__(self, channel: grpc.aio.Channel, *, owns_channel: bool = False) -> None:
        """Uses a channel the caller made and, unless ``owns_channel``, will close."""
        asyncio.get_running_loop()
        self._guard = ProcessGuard()
        self._channel = channel
        self._owns_channel = owns_channel
        self._stub = odradek_pb2_grpc.OdradekServiceStub(channel)

    @classmethod
    def connect(cls, socket_path: str | os.PathLike[str]) -> OdradekClient:
        """Connects to the server's Unix domain socket.

        Must be called with the event loop running. It does not wait for the
        server: producers and consumers keep trying until it is there.
        """
        asyncio.get_running_loop()
        channel = grpc.aio.insecure_channel(
            "unix:" + os.fspath(socket_path), options=_CHANNEL_OPTIONS
        )
        return cls(channel, owns_channel=True)

    def producer(
        self,
        *,
        max_block: float = 60.0,
        max_buffered_records: int | None = None,
        max_buffered_bytes: int | None = None,
    ) -> Producer:
        """Starts a producer.

        Args:
            max_block: how many seconds ``send`` waits when the window is full
                before it fails.
            max_buffered_records: how many records may await a result before
                the server has announced its window; afterwards the server's
                window applies. Default: 1000.
            max_buffered_bytes: the same limit in bytes. Default: 16 MiB.
        """
        self._guard.check()
        return Producer(
            self._stub,
            self._guard,
            max_block=max_block,
            max_buffered_records=max_buffered_records,
            max_buffered_bytes=max_buffered_bytes,
        )

    def consumer(
        self,
        topics: Iterable[str],
        handler: RecordHandler,
        *,
        listener: ConsumerListener | None = None,
        max_processing: float | None = None,
        max_unacked_records: int | None = None,
    ) -> Consumer:
        """Starts a consumer that hands records to ``handler``.

        Args:
            topics: the topics to read. Their cluster and consumer group are
                the server's configuration; all of them must resolve to the
                same cluster and group.
            handler: awaited for each record; see :data:`RecordHandler`.
            listener: hears about assigned, revoked and lost partitions, and
                about the consumer failing.
            max_processing: the longest, in seconds, the handler may take for
                one record before the server considers the app hung and evicts
                it. Default: the server's.
            max_unacked_records: how many records may be delivered ahead of
                the handler. Default: 500.
        """
        self._guard.check()
        if isinstance(topics, str):
            topics = [topics]
        return Consumer(
            self._stub,
            self._guard,
            topics,
            handler,
            listener,
            max_processing=max_processing,
            max_unacked_records=max_unacked_records,
        )

    async def close(self) -> None:
        """Closes the connection. Close producers and consumers first."""
        self._guard.check()
        if self._owns_channel:
            await self._channel.close()

    async def __aenter__(self) -> OdradekClient:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.close()
