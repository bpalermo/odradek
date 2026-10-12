"""The asyncio binding against a scripted gRPC server on a Unix socket, in this process.

The protocol rules are the core's and are covered by the conformance
scenarios. These tests cover what the binding adds: streams, reconnecting,
waiting for room, the handler task, and what the app is told.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import tempfile
import unittest
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from typing import Any, TypeVar
from unittest import mock

import grpc
import grpc.aio

import odradek
from odradek import (
    BufferFullError,
    ClientClosedError,
    ConsumerFailedError,
    ConsumerListener,
    ErrorCode,
    ForkedProcessError,
    OdradekClient,
    Partition,
    ProduceError,
    ReceivedRecord,
)
from odradek._stream import Backoff
from odradek.kafka.v1 import odradek_pb2 as pb
from odradek.kafka.v1 import odradek_pb2_grpc

T = TypeVar("T")
_TIMEOUT = 20.0

ProduceScript = Callable[[AsyncIterator[pb.ProduceRequest], Any], AsyncIterator[pb.ProduceResponse]]
ConsumeScript = Callable[[AsyncIterator[pb.ConsumeRequest], Any], AsyncIterator[pb.ConsumeResponse]]


async def _soon(awaitable: Awaitable[T]) -> T:
    return await asyncio.wait_for(awaitable, _TIMEOUT)


class _Service(odradek_pb2_grpc.OdradekServiceServicer):
    """Answers each stream with the next script; the last one answers all later streams."""

    def __init__(self) -> None:
        self.produce: list[ProduceScript] = []
        self.consume: list[ConsumeScript] = []
        self.streams = 0

    async def Produce(  # noqa: N802
        self, request_iterator: AsyncIterator[pb.ProduceRequest], context: Any
    ) -> AsyncIterator[pb.ProduceResponse]:
        script = self.produce[min(self.streams, len(self.produce) - 1)]
        self.streams += 1
        async for response in script(request_iterator, context):
            yield response

    async def Consume(  # noqa: N802
        self, request_iterator: AsyncIterator[pb.ConsumeRequest], context: Any
    ) -> AsyncIterator[pb.ConsumeResponse]:
        script = self.consume[min(self.streams, len(self.consume) - 1)]
        self.streams += 1
        async for response in script(request_iterator, context):
            yield response


class _Case(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        # A Unix socket path is limited to about 100 bytes, which a Bazel test directory exceeds.
        self.dir = tempfile.mkdtemp(prefix="odradek-", dir="/tmp")
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        self.socket = os.path.join(self.dir, "kafka.sock")
        self.service = _Service()
        self.server = grpc.aio.server()
        odradek_pb2_grpc.add_OdradekServiceServicer_to_server(self.service, self.server)
        self.server.add_insecure_port("unix:" + self.socket)
        await self.server.start()
        self.client = OdradekClient.connect(self.socket)

    async def asyncTearDown(self) -> None:
        await self.client.close()
        await self.server.stop(0)


def _ready(epoch: str = "e1", records: int = 100) -> pb.ProduceResponse:
    return pb.ProduceResponse(
        ready=pb.ProduceReady(
            server_epoch=epoch, max_in_flight_records=records, max_in_flight_bytes=1 << 20
        )
    )


def _ok(record: pb.ProduceRecord) -> pb.ProduceResponse:
    return pb.ProduceResponse(
        result=pb.ProduceResult(
            seq=record.seq,
            writes=[
                pb.RecordMetadata(cluster="a", topic=record.topic, partition=1, offset=10),
                pb.RecordMetadata(cluster="b", topic=record.topic, partition=3, offset=20),
            ],
        )
    )


class ProducerTest(_Case):
    async def test_send_returns_the_writes_when_the_record_is_acknowledged(self) -> None:
        seen: list[pb.ProduceRequest] = []

        async def script(requests: AsyncIterator[pb.ProduceRequest], _: Any) -> Any:
            async for request in requests:
                seen.append(request)
                yield _ready() if request.HasField("open") else _ok(request.record)

        self.service.produce = [script]
        message = pb.ClientInfo(language="x", version="1")
        async with self.client.producer() as producer:
            result = await _soon(
                producer.send(
                    "orders",
                    key="k",
                    value=message,
                    headers=[("h", b"1"), ("none", None)],
                    partition=2,
                    timestamp_ms=5,
                )
            )
        self.assertEqual([w.cluster for w in result.writes], ["a", "b"])
        self.assertEqual((result.writes[1].partition, result.writes[1].offset), (3, 20))

        opening, sent = seen[0].open, seen[1].record
        self.assertEqual(opening.client.language, "python")
        self.assertEqual(opening.client.version, odradek.__version__)
        self.assertTrue(opening.session_id)
        self.assertEqual((sent.seq, sent.low_watermark, sent.topic), (1, 1, "orders"))
        self.assertEqual(sent.key, b"k")
        self.assertEqual(sent.value, message.SerializeToString())
        self.assertEqual((sent.partition, sent.timestamp_ms), (2, 5))
        self.assertEqual(sent.headers[0], pb.Header(key="h", value=b"1"))
        self.assertFalse(sent.headers[1].HasField("value"))

    async def test_a_tombstone_has_neither_key_nor_value(self) -> None:
        seen: list[pb.ProduceRecord] = []

        async def script(requests: AsyncIterator[pb.ProduceRequest], _: Any) -> Any:
            async for request in requests:
                if request.HasField("record"):
                    seen.append(request.record)
                yield _ready() if request.HasField("open") else _ok(request.record)

        self.service.produce = [script]
        async with self.client.producer() as producer:
            await _soon(producer.send("orders"))
        self.assertFalse(seen[0].HasField("key") or seen[0].HasField("value"))
        self.assertFalse(seen[0].HasField("partition") or seen[0].HasField("timestamp_ms"))

    async def test_a_failed_record_raises_with_the_code_and_the_clusters_that_wrote_it(
        self,
    ) -> None:
        async def script(requests: AsyncIterator[pb.ProduceRequest], _: Any) -> Any:
            async for request in requests:
                if request.HasField("open"):
                    yield _ready()
                elif request.record.topic == "bad":
                    yield pb.ProduceResponse(
                        result=pb.ProduceResult(
                            seq=request.record.seq,
                            error=pb.Error(
                                code=pb.ERROR_CODE_INVALID_RECORD,
                                message="cluster b refused it",
                                kafka_exception="RecordTooLargeException",
                            ),
                            writes=[pb.RecordMetadata(cluster="a", topic="bad", offset=7)],
                        )
                    )
                else:
                    yield _ok(request.record)

        self.service.produce = [script]
        async with self.client.producer() as producer:
            with self.assertRaises(ProduceError) as raised:
                await _soon(producer.send("bad", value=b"v"))
            error = raised.exception
            self.assertEqual(error.code, ErrorCode.ERROR_CODE_INVALID_RECORD)
            self.assertFalse(error.outcome_unknown)
            self.assertEqual(error.kafka_exception, "RecordTooLargeException")
            self.assertEqual([(w.cluster, w.offset) for w in error.writes], [("a", 7)])
            self.assertIn("ERROR_CODE_INVALID_RECORD: cluster b refused it", str(error))
            # Only that send failed.
            await _soon(producer.send("good", value=b"v"))

    async def test_send_waits_for_room_and_keeps_the_order(self) -> None:
        release = asyncio.Event()
        received: asyncio.Queue[bytes] = asyncio.Queue()

        async def script(requests: AsyncIterator[pb.ProduceRequest], _: Any) -> Any:
            async for request in requests:
                if request.HasField("open"):
                    yield _ready(records=1)
                    continue
                received.put_nowait(request.record.value)
                await release.wait()
                yield _ok(request.record)

        self.service.produce = [script]
        async with self.client.producer(max_block=_TIMEOUT) as producer:
            sends = [asyncio.create_task(producer.send("t", value=v)) for v in (b"1", b"2", b"3")]
            self.assertEqual(await _soon(received.get()), b"1")
            await asyncio.sleep(0.2)
            self.assertFalse(any(send.done() for send in sends), "the window holds one record")
            self.assertTrue(received.empty())
            release.set()
            await _soon(asyncio.gather(*sends))
            self.assertEqual([await _soon(received.get()) for _ in range(2)], [b"2", b"3"])

    async def test_send_fails_when_the_window_stays_full_or_the_record_can_never_fit(
        self,
    ) -> None:
        received: asyncio.Queue[bytes] = asyncio.Queue()

        async def script(requests: AsyncIterator[pb.ProduceRequest], _: Any) -> Any:
            async for request in requests:
                if request.HasField("open"):
                    yield _ready(records=1)
                else:
                    received.put_nowait(request.record.value)

        self.service.produce = [script]
        producer = self.client.producer(max_block=0.3)
        first = asyncio.create_task(producer.send("t", value=b"1"))
        await _soon(received.get())
        with self.assertRaisesRegex(BufferFullError, "stayed full"):
            await _soon(producer.send("t", value=b"2"))
        with self.assertRaisesRegex(BufferFullError, "larger than the whole"):
            await _soon(producer.send("t", value=b"x" * (2 << 20)))
        self.assertFalse(first.done())

        # Closing gives up on the record the server never answered for: it may have been written.
        await producer.close(grace=0.2)
        with self.assertRaises(ProduceError) as raised:
            await first
        self.assertTrue(raised.exception.outcome_unknown)
        with self.assertRaisesRegex(ClientClosedError, "closed"):
            await producer.send("t", value=b"3")

    async def test_a_broken_stream_is_reopened_and_the_record_sent_again(self) -> None:
        opens: list[str] = []
        records: list[int] = []

        async def breaks(requests: AsyncIterator[pb.ProduceRequest], context: Any) -> Any:
            async for request in requests:
                if request.HasField("open"):
                    opens.append(request.open.session_id)
                    yield _ready()
                else:
                    records.append(request.record.seq)
                    await context.abort(grpc.StatusCode.UNAVAILABLE, "going away")

        async def works(requests: AsyncIterator[pb.ProduceRequest], _: Any) -> Any:
            async for request in requests:
                if request.HasField("open"):
                    opens.append(request.open.session_id)
                    yield _ready()
                else:
                    records.append(request.record.seq)
                    yield _ok(request.record)

        self.service.produce = [breaks, works]
        async with self.client.producer() as producer:
            result = await _soon(producer.send("t", value=b"v"))
        self.assertEqual(len(result.writes), 2)
        self.assertEqual(records, [1, 1], "the same record, with the same sequence number")
        self.assertEqual(len(opens), 2)
        self.assertEqual(opens[0], opens[1], "the same session")

    async def test_a_restarted_server_fails_the_records_it_was_sent_as_outcome_unknown(
        self,
    ) -> None:
        async def breaks(requests: AsyncIterator[pb.ProduceRequest], context: Any) -> Any:
            async for request in requests:
                if request.HasField("open"):
                    yield _ready("e1")
                else:
                    await context.abort(grpc.StatusCode.UNAVAILABLE, "going away")

        async def restarted(requests: AsyncIterator[pb.ProduceRequest], _: Any) -> Any:
            async for request in requests:
                yield _ready("e2") if request.HasField("open") else _ok(request.record)

        self.service.produce = [breaks, restarted]
        async with self.client.producer() as producer:
            with self.assertRaises(ProduceError) as raised:
                await _soon(producer.send("t", value=b"v"))
            self.assertEqual(raised.exception.code, ErrorCode.ERROR_CODE_OUTCOME_UNKNOWN)
            self.assertTrue(raised.exception.outcome_unknown)
            # The producer itself goes on.
            await _soon(producer.send("t", value=b"w"))

    async def test_a_refused_producer_fails_its_records_and_takes_no_more(self) -> None:
        async def refuses(requests: AsyncIterator[pb.ProduceRequest], context: Any) -> Any:
            async for _ in requests:
                await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "no session id")
            yield  # pragma: no cover

        self.service.produce = [refuses]
        producer = self.client.producer()
        with self.assertRaisesRegex(ProduceError, "the record was not sent"):
            await _soon(producer.send("t", value=b"v"))
        with self.assertRaisesRegex(ClientClosedError, "INVALID_ARGUMENT: no session id"):
            await producer.send("t", value=b"v")
        await _soon(producer.close())
        self.assertEqual(self.service.streams, 1, "a refused producer does not reconnect")

    async def test_it_keeps_trying_until_the_server_is_there(self) -> None:
        async def script(requests: AsyncIterator[pb.ProduceRequest], _: Any) -> Any:
            async for request in requests:
                yield _ready() if request.HasField("open") else _ok(request.record)

        self.service.produce = [script]
        await self.server.stop(0)
        producer = self.client.producer()
        send = asyncio.create_task(producer.send("t", value=b"v"))
        await asyncio.sleep(0.5)
        self.assertFalse(send.done())

        self.server = grpc.aio.server()
        odradek_pb2_grpc.add_OdradekServiceServicer_to_server(self.service, self.server)
        self.server.add_insecure_port("unix:" + self.socket)
        await self.server.start()
        self.assertEqual(len((await _soon(send)).writes), 2)
        await _soon(producer.close())


def _batch(epoch: int, *offsets: int) -> pb.ConsumeResponse:
    return pb.ConsumeResponse(
        records=pb.RecordBatch(
            topic="orders",
            partition=0,
            assignment_epoch=epoch,
            records=[
                pb.ConsumedRecord(offset=o, timestamp_ms=100 + o, key=b"k", value=b"v%d" % o)
                for o in offsets
            ],
        )
    )


def _assigned(epoch: int) -> pb.ConsumeResponse:
    return pb.ConsumeResponse(
        assigned=pb.Assigned(
            partitions=[pb.PartitionAssignment(topic="orders", partition=0, assignment_epoch=epoch)]
        )
    )


class _Listener(ConsumerListener):
    def __init__(self) -> None:
        self.events: asyncio.Queue[tuple[str, Any]] = asyncio.Queue()

    async def assigned(self, partitions: Sequence[Partition]) -> None:
        self.events.put_nowait(("assigned", list(partitions)))

    async def revoked(self, partitions: Sequence[Partition]) -> None:
        self.events.put_nowait(("revoked", list(partitions)))

    async def lost(self, partitions: Sequence[Partition]) -> None:
        self.events.put_nowait(("lost", list(partitions)))

    async def failed(self, cause: BaseException) -> None:
        self.events.put_nowait(("failed", cause))


_P0 = Partition("orders", 0)


class ConsumerTest(_Case):
    def _script(
        self, *responses: pb.ConsumeResponse, then: Callable[[Any], Awaitable[None]] | None = None
    ) -> tuple[ConsumeScript, asyncio.Queue[pb.ConsumeRequest | None]]:
        """A stream that answers Subscribe with `responses` and records what it is sent."""
        requests_seen: asyncio.Queue[pb.ConsumeRequest | None] = asyncio.Queue()

        async def script(requests: AsyncIterator[pb.ConsumeRequest], context: Any) -> Any:
            async for request in requests:
                requests_seen.put_nowait(request)
                if request.HasField("subscribe"):
                    yield pb.ConsumeResponse(subscribed=pb.Subscribed(server_epoch="e1"))
                    for response in responses:
                        yield response
                    if then is not None:
                        await then(context)
            # The client half-closed.
            requests_seen.put_nowait(None)

        return script, requests_seen

    async def test_each_record_is_handled_in_order_and_acked_when_the_handler_returns(
        self,
    ) -> None:
        script, requests = self._script(_assigned(4), _batch(4, 0, 1, 2))
        self.service.consume = [script]
        handled: list[ReceivedRecord] = []
        gate = asyncio.Event()

        async def handler(record: ReceivedRecord) -> None:
            handled.append(record)
            if record.offset == 1:
                await gate.wait()

        listener = _Listener()
        consumer = self.client.consumer(
            ["orders"], handler, listener=listener, max_processing=2.5, max_unacked_records=10
        )
        subscribe = (await _soon(requests.get())).subscribe  # type: ignore[union-attr]
        self.assertEqual(list(subscribe.topics), ["orders"])
        self.assertEqual(subscribe.max_processing_ms, 2500)
        self.assertEqual(subscribe.client.language, "python")
        self.assertEqual((await _soon(requests.get())).credits.records, 10)  # type: ignore[union-attr]
        self.assertEqual(await _soon(listener.events.get()), ("assigned", [_P0]))

        first = (await _soon(requests.get())).ack  # type: ignore[union-attr]
        self.assertEqual((first.topic, first.partition, first.assignment_epoch), ("orders", 0, 4))
        self.assertEqual((first.first_offset, first.last_offset), (0, 0))
        await asyncio.sleep(0.2)
        self.assertTrue(requests.empty(), "offset 1 is not acked while its handler runs")
        self.assertEqual([r.offset for r in handled], [0, 1])

        gate.set()
        self.assertEqual((await _soon(requests.get())).ack.first_offset, 1)  # type: ignore[union-attr]
        self.assertEqual((await _soon(requests.get())).ack.first_offset, 2)  # type: ignore[union-attr]
        self.assertEqual(
            (handled[2].topic, handled[2].partition, handled[2].timestamp_ms), ("orders", 0, 102)
        )
        self.assertEqual((handled[2].key, handled[2].value), (b"k", b"v2"))
        self.assertIsNone(handled[2].decode_error)

        await _soon(consumer.close())
        self.assertIsNone(await _soon(requests.get()), "close completes the stream")
        self.assertEqual(self.service.streams, 1)

    async def test_a_handler_that_raises_stops_the_consumer_and_the_record_is_not_acked(
        self,
    ) -> None:
        script, requests = self._script(_assigned(1), _batch(1, 0, 1, 2))
        self.service.consume = [script]
        handled: list[int] = []

        async def handler(record: ReceivedRecord) -> None:
            if record.offset == 1:
                raise ValueError("cannot process 1")
            handled.append(record.offset)

        listener = _Listener()
        consumer = self.client.consumer("orders", handler, listener=listener)
        self.assertEqual((await _soon(listener.events.get()))[0], "assigned")
        kind, cause = await _soon(listener.events.get())
        self.assertEqual(kind, "failed")
        self.assertIsInstance(cause, ValueError)
        await _soon(consumer.close())

        sent = []
        while (request := await _soon(requests.get())) is not None:
            sent.append(request.WhichOneof("msg"))
            if request.HasField("ack"):
                self.assertEqual(request.ack.first_offset, 0)
        self.assertEqual(sent.count("ack"), 1, "only the record handled before the failure")
        self.assertEqual(handled, [0], "nothing after the failed record is handled")
        self.assertEqual(self.service.streams, 1)

    async def test_revocation_is_reported_and_answered(self) -> None:
        revoked = pb.ConsumeResponse(
            revoked=pb.Revoked(
                revoke_id=7,
                partitions=[pb.TopicPartition(topic="orders", partition=0)],
                deadline_ms=5000,
            )
        )
        script, requests = self._script(_assigned(1), revoked, _batch(1, 5))
        self.service.consume = [script]
        handled: list[int] = []

        async def handler(record: ReceivedRecord) -> None:
            handled.append(record.offset)

        listener = _Listener()
        async with self.client.consumer(["orders"], handler, listener=listener):
            kinds = [(await _soon(listener.events.get())) for _ in range(2)]
            self.assertEqual(kinds, [("assigned", [_P0]), ("revoked", [_P0])])
            while not (request := await _soon(requests.get())).HasField("revoke_done"):  # type: ignore[union-attr]
                pass
            self.assertEqual(request.revoke_done.revoke_id, 7)  # type: ignore[union-attr]
            await asyncio.sleep(0.1)
        self.assertEqual(handled, [], "a record of a partition given up is not handled")

    async def test_a_broken_stream_reports_the_partitions_lost_and_subscribes_again(self) -> None:
        async def abort(context: Any) -> None:
            await context.abort(grpc.StatusCode.UNAVAILABLE, "going away")

        breaks, _ = self._script(_assigned(1), then=abort)
        works, requests = self._script(_assigned(2), _batch(2, 9))
        self.service.consume = [breaks, works]
        handled: asyncio.Queue[int] = asyncio.Queue()

        async def handler(record: ReceivedRecord) -> None:
            handled.put_nowait(record.offset)

        listener = _Listener()
        async with self.client.consumer(["orders"], handler, listener=listener):
            kinds = [(await _soon(listener.events.get())) for _ in range(3)]
            self.assertEqual(kinds, [("assigned", [_P0]), ("lost", [_P0]), ("assigned", [_P0])])
            self.assertEqual(await _soon(handled.get()), 9)
            self.assertTrue((await _soon(requests.get())).HasField("subscribe"))  # type: ignore[union-attr]
        self.assertEqual(self.service.streams, 2)

    async def test_an_evicted_consumer_reports_the_failure_and_does_not_come_back(self) -> None:
        async def abort(context: Any) -> None:
            await context.abort(grpc.StatusCode.ABORTED, "the app is considered hung")

        script, _ = self._script(_assigned(1), then=abort)
        self.service.consume = [script]

        async def handler(record: ReceivedRecord) -> None:
            pass

        listener = _Listener()
        consumer = self.client.consumer(["orders"], handler, listener=listener)
        # The failure goes ahead of anything still queued, as the server's revocations do.
        while (event := await _soon(listener.events.get()))[0] != "failed":
            pass
        cause = event[1]
        self.assertIsInstance(cause, ConsumerFailedError)
        self.assertEqual(cause.status_code, "ABORTED")
        self.assertIn("considered hung", str(cause))
        await asyncio.sleep(0.5)
        await _soon(consumer.close())
        self.assertEqual(self.service.streams, 1)


class ForkTest(_Case):
    async def test_a_client_used_in_another_process_fails_clearly(self) -> None:
        async def handler(record: ReceivedRecord) -> None:
            pass

        async def script(requests: AsyncIterator[pb.ProduceRequest], _: Any) -> Any:
            async for request in requests:
                yield _ready() if request.HasField("open") else _ok(request.record)

        self.service.produce = [script]
        producer = self.client.producer()
        await _soon(producer.send("t", value=b"v"))
        with mock.patch("os.getpid", return_value=os.getpid() + 1):
            with self.assertRaisesRegex(ForkedProcessError, "does not survive fork"):
                await producer.send("t", value=b"v")
            with self.assertRaises(ForkedProcessError):
                self.client.producer()
            with self.assertRaises(ForkedProcessError):
                self.client.consumer(["orders"], handler)
            with self.assertRaises(ForkedProcessError):
                await producer.close()
            with self.assertRaises(ForkedProcessError):
                await self.client.close()
        await _soon(producer.close())


class BackoffTest(unittest.TestCase):
    def test_it_doubles_from_100ms_to_5s_with_jitter_and_resets(self) -> None:
        backoff = Backoff()
        ceilings = [0.1, 0.2, 0.4, 0.8, 1.6, 3.2, 5.0, 5.0, 5.0]
        for ceiling in ceilings:
            delay = backoff.next()
            self.assertGreaterEqual(delay, ceiling / 2)
            self.assertLessEqual(delay, ceiling)
        backoff.reset()
        self.assertLessEqual(backoff.next(), 0.1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
