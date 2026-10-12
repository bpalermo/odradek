"""The values and errors of the public API."""

from __future__ import annotations

import os
from collections.abc import Sequence
from dataclasses import dataclass

from odradek.kafka.v1 import odradek_pb2


@dataclass(frozen=True, slots=True, order=True)
class Partition:
    """One partition of one topic."""

    topic: str
    partition: int

    def __str__(self) -> str:
        return f"{self.topic}-{self.partition}"


@dataclass(frozen=True, slots=True)
class Header:
    """A record header. The value may be ``None``."""

    key: str
    value: bytes | None


@dataclass(frozen=True, slots=True)
class ReceivedRecord:
    """A record handed to a consumer's handler.

    ``key`` is the record key or ``None``; ``value`` is ``None`` for a
    tombstone and, for a topic with a contract type, the protobuf encoding of
    that type (``MyMessage.FromString(record.value)``).

    ``decode_error`` is ``None`` normally. Otherwise the server could not
    decode the record and the consumer is configured to receive it anyway: key,
    value and headers are then exactly as stored in Kafka, and this text says
    what went wrong.
    """

    topic: str
    partition: int
    offset: int
    timestamp_ms: int
    key: bytes | None
    value: bytes | None
    headers: tuple[Header, ...]
    decode_error: str | None


@dataclass(frozen=True, slots=True)
class Write:
    """One cluster's acknowledgement. Partition and offset are specific to that cluster."""

    cluster: str
    topic: str
    partition: int
    offset: int
    timestamp_ms: int


@dataclass(frozen=True, slots=True)
class SendResult:
    """Where an acknowledged record was written: one entry per cluster, in configuration order."""

    writes: tuple[Write, ...]


class OdradekError(Exception):
    """Base of every error this library raises itself."""


class ProduceError(OdradekError):
    """A record was not acknowledged by every cluster its topic requires.

    ``code`` (an ``odradek.ErrorCode`` value) says whether sending it again is
    safe, pointless, or may duplicate; ``writes`` lists the clusters that hold
    the record anyway.
    """

    def __init__(
        self,
        code: odradek_pb2.ErrorCode,
        message: str,
        kafka_exception: str = "",
        writes: Sequence[Write] = (),
    ) -> None:
        super().__init__(f"{odradek_pb2.ErrorCode.Name(code)}: {message}")
        self.code = code
        self.message = message
        #: Simple class name of the Kafka exception behind this, or empty. For diagnostics.
        self.kafka_exception = kafka_exception
        #: The clusters that acknowledged the record despite the failure.
        self.writes: tuple[Write, ...] = tuple(writes)

    @property
    def outcome_unknown(self) -> bool:
        """Whether the record may have been written: sending it again could then duplicate it."""
        return self.code in (
            odradek_pb2.ERROR_CODE_OUTCOME_UNKNOWN,
            odradek_pb2.ERROR_CODE_INTERNAL,
        )


class BufferFullError(OdradekError):
    """A record could not be taken.

    The window stayed full for the whole ``max_block``, or the record alone is
    larger than the window. The record was not sent.
    """


class ClientClosedError(OdradekError):
    """The producer or consumer was closed, or the server refused it for good."""


class ConsumerFailedError(OdradekError):
    """The server refused or evicted a consumer."""

    def __init__(self, status_code: str, message: str) -> None:
        super().__init__(f"{status_code}: {message}")
        #: Name of the gRPC status code, such as "ABORTED" when the app held a record too long.
        self.status_code = status_code


class ForkedProcessError(OdradekError):
    """The client is being used in a process other than the one that created it.

    A gRPC channel does not survive ``fork()``. Under a prefork server
    (gunicorn, Celery, multiprocessing) create the client in each worker, after
    the fork, never before.
    """


class ProcessGuard:
    """Remembers the process a client was created in."""

    def __init__(self) -> None:
        self._pid = os.getpid()

    def check(self) -> None:
        """Raises :class:`ForkedProcessError` in any other process."""
        pid = os.getpid()
        if pid != self._pid:
            raise ForkedProcessError(
                f"this Odradek client was created in process {self._pid} and is being used in"
                f" process {pid}: a gRPC channel does not survive fork()."
                " Create the client after forking, in the process that uses it."
            )
