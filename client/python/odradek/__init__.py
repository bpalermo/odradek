"""Client for the Odradek server: Kafka through gRPC on a Unix domain socket, with asyncio.

::

    import asyncio
    from odradek import OdradekClient, ReceivedRecord

    async def main() -> None:
        async with OdradekClient.connect("/var/run/kafka/kafka.sock") as client:
            async with client.producer() as producer:
                await producer.send("orders.created", key="42", value=b"...")

            async def handle(record: ReceivedRecord) -> None:
                print(record.topic, record.offset, record.value)

            async with client.consumer(["orders.created"], handle):
                await asyncio.sleep(60)

    asyncio.run(main())

The generated API lives in ``odradek.kafka.v1``; the protocol core, which does
no I/O, in ``odradek.core``.
"""

from odradek import _stream
from odradek._client import OdradekClient
from odradek._consumer import Consumer, ConsumerListener, RecordHandler
from odradek._producer import Payload, Producer
from odradek._types import (
    BufferFullError,
    ClientClosedError,
    ConsumerFailedError,
    ForkedProcessError,
    Header,
    OdradekError,
    Partition,
    ProduceError,
    ReceivedRecord,
    SendResult,
    Write,
)
from odradek.kafka.v1.odradek_pb2 import ErrorCode

__version__ = _stream.VERSION

__all__ = [
    "BufferFullError",
    "ClientClosedError",
    "Consumer",
    "ConsumerFailedError",
    "ConsumerListener",
    "ErrorCode",
    "ForkedProcessError",
    "Header",
    "OdradekClient",
    "OdradekError",
    "Partition",
    "Payload",
    "ProduceError",
    "Producer",
    "ReceivedRecord",
    "RecordHandler",
    "SendResult",
    "Write",
    "__version__",
]
