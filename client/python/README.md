# odradek

Python client for the [Odradek](https://github.com/bpalermo/odradek) server, which owns the Kafka clients of an application and is reached over gRPC on a Unix domain socket. The client is built on `grpcio` and asyncio ([ADR 3](https://github.com/bpalermo/odradek/blob/main/docs/adr/0003-python-client-on-grpcio-asyncio.md)).

```python
import asyncio

from odradek import OdradekClient, ProduceError, ReceivedRecord


async def main() -> None:
    async with OdradekClient.connect("/var/run/kafka/kafka.sock") as client:
        async with client.producer() as producer:
            try:
                result = await producer.send("orders.created", key="42", value=b"...")
            except ProduceError as error:
                print(error.code, error.outcome_unknown, error.writes)

        async def handle(record: ReceivedRecord) -> None:
            print(record.topic, record.partition, record.offset, record.value)

        async with client.consumer(["orders.created"], handle):
            await asyncio.sleep(60)


asyncio.run(main())
```

- `send` returns when every cluster the topic requires has acknowledged the record. Start sends together (`asyncio.gather`) to pipeline them; they keep their order.
- Keys and values are `bytes`, `str` (sent as UTF-8) or protobuf messages (serialized as the topic's contract type).
- The handler is awaited for one record at a time. Returning marks the record as processed; raising stops the consumer and the record is delivered again.
- A `ConsumerListener` hears about partitions assigned, revoked and lost, and about the consumer failing.
- Producers and consumers reconnect on their own when the server restarts.
- A client belongs to the process and the event loop that made it. Under a prefork server (gunicorn, Celery) make it in the worker, after the fork; used in another process it raises `ForkedProcessError`.

The package contains the generated API (`odradek.kafka.v1`) and the protocol core without I/O (`odradek.core`), a port of the Java core that passes the same conformance scenarios.
