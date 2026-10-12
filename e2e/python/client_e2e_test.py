"""The Python client end to end: its public API against the server and two real clusters.

The same cases as ClientE2eTest.java. The server is the packaged one
(ODRADEK_E2E_SERVER), started as a process with ``--config`` and stopped with
SIGTERM; the brokers are ODRADEK_E2E_KAFKA_A and ODRADEK_E2E_KAFKA_B.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import tempfile
import unittest
import uuid
from collections.abc import Callable, Sequence

from odradek import (
    ConsumerListener,
    ErrorCode,
    OdradekClient,
    Partition,
    ProduceError,
    Producer,
    ReceivedRecord,
)

_TIMEOUT = 60.0
_PARTITIONS = 4

_CONFIG = """\
listener:
  socketPath: {dir}/kafka.sock
clusters:
  a:
    properties:
      bootstrap.servers: {kafka_a}
    consumerProperties:
      session.timeout.ms: 10000
      heartbeat.interval.ms: 1000
  b:
    properties:
      bootstrap.servers: {kafka_b}
consume:
  commitInterval: 0.2s
  revokeDeadline: 5s
producers:
  {topic}:
    clusters: [{cluster: a}, {cluster: b}]
consumers:
  {topic}:
    cluster: a
    groupId: client-{id}
"""


def _env(variable: str) -> str:
    value = os.environ.get(variable, "").strip()
    if not value:
        raise AssertionError(f"{variable} is not set; see e2e/BUILD.bazel")
    return value


_KAFKA_A = _env("ODRADEK_E2E_KAFKA_A")
_KAFKA_B = _env("ODRADEK_E2E_KAFKA_B")
_SERVER = os.path.abspath(_env("ODRADEK_E2E_SERVER"))
_TOPICS = os.path.abspath(_env("ODRADEK_E2E_TOPICS"))


async def _topics(*arguments: str) -> None:
    """Runs the topic tool (//e2e:topics) and fails if it does."""
    process = await asyncio.create_subprocess_exec(_TOPICS, *arguments)
    status = await asyncio.wait_for(process.wait(), _TIMEOUT)
    if status != 0:
        raise AssertionError(f"topics {' '.join(arguments)} exited with status {status}")


class _Server:
    """A running server, in a temporary directory, listening on a Unix socket there."""

    def __init__(self, config: str) -> None:
        # Under /tmp, not the test's own directory: a socket path is limited to about 100 bytes.
        self.dir = tempfile.mkdtemp(prefix="odradek-e2e", dir="/tmp")
        self.socket = os.path.join(self.dir, "kafka.sock")
        self._file = os.path.join(self.dir, "odradek.yaml")
        with open(self._file, "w", encoding="utf-8") as file:
            file.write(config.replace("{dir}", self.dir))
        self._process: asyncio.subprocess.Process | None = None

    async def start(self) -> None:
        process = await asyncio.create_subprocess_exec(_SERVER, "--config", self._file)
        self._process = process
        deadline = asyncio.get_running_loop().time() + _TIMEOUT
        while not os.path.exists(self.socket):  # noqa: ASYNC240 (one stat, in a test)
            if process.returncode is not None:
                raise AssertionError(
                    f"{_SERVER} exited with status {process.returncode} before listening"
                )
            if asyncio.get_running_loop().time() > deadline:
                process.kill()
                raise AssertionError(f"{_SERVER} did not listen on {self.socket} in {_TIMEOUT}s")
            await asyncio.sleep(0.05)

    async def stop(self) -> None:
        """Stops the server with SIGTERM, as a pod does."""
        process, self._process = self._process, None
        if process is None or process.returncode is not None:
            return
        process.terminate()
        try:
            await asyncio.wait_for(process.wait(), _TIMEOUT)
        except TimeoutError:
            process.kill()
            raise AssertionError("the server did not stop within 60s of SIGTERM") from None

    async def restart(self) -> None:
        """A new server with the same configuration and socket: a new server epoch."""
        await self.stop()
        await self.start()

    def remove(self) -> None:
        shutil.rmtree(self.dir, ignore_errors=True)


async def _eventually(what: str, condition: Callable[[], bool]) -> None:
    deadline = asyncio.get_running_loop().time() + _TIMEOUT
    while not condition():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(f"not within {_TIMEOUT}s: {what}")
        await asyncio.sleep(0.02)


def _expected(prefix: str, first: int, last: int) -> set[str]:
    return {f"{prefix}{i}" for i in range(first, last + 1)}


def _text(data: bytes | None) -> str:
    assert data is not None
    return data.decode("utf-8")


class _Tracking(ConsumerListener):
    """Keeps the set of partitions a consumer owns."""

    def __init__(self) -> None:
        self.owned: set[Partition] = set()
        self.lost_ever: set[Partition] = set()
        self.failure: asyncio.Future[BaseException] = asyncio.get_running_loop().create_future()

    async def assigned(self, partitions: Sequence[Partition]) -> None:
        self.owned.update(partitions)

    async def revoked(self, partitions: Sequence[Partition]) -> None:
        self.owned.difference_update(partitions)

    async def lost(self, partitions: Sequence[Partition]) -> None:
        self.owned.difference_update(partitions)
        self.lost_ever.update(partitions)

    async def failed(self, cause: BaseException) -> None:
        self.failure.set_result(cause)


class ClientE2eTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.run_id = str(uuid.uuid4())
        self.topic = f"odradek-e2e-pyclient-{self.run_id}"
        self.unconfigured = f"odradek-e2e-unconfigured-{self.run_id}"
        await asyncio.gather(
            _topics("create", _KAFKA_A, str(_PARTITIONS), self.topic),
            _topics("create", _KAFKA_B, str(_PARTITIONS), self.topic),
        )
        self.addAsyncCleanup(self._delete_topics)
        self.server = _Server(
            _CONFIG.replace("{kafka_a}", _KAFKA_A)
            .replace("{kafka_b}", _KAFKA_B)
            .replace("{topic}", self.topic)
            .replace("{id}", self.run_id)
        )
        self.addCleanup(self.server.remove)
        self.addAsyncCleanup(self.server.stop)
        await self.server.start()
        self.client = OdradekClient.connect(self.server.socket)
        self.addAsyncCleanup(self.client.close)

    async def _delete_topics(self) -> None:
        await asyncio.gather(
            _topics("delete", _KAFKA_A, self.topic),
            _topics("delete", _KAFKA_B, self.topic),
        )

    async def _send(self, producer: Producer, prefix: str, first: int, last: int) -> None:
        """Sends prefix<first>..prefix<last>, in that order, and waits for every acknowledgement."""
        results = await asyncio.wait_for(
            asyncio.gather(
                *(
                    producer.send(self.topic, key=f"key-{i % 7}", value=f"{prefix}{i}")
                    for i in range(first, last + 1)
                )
            ),
            _TIMEOUT,
        )
        for result in results:
            self.assertEqual([w.cluster for w in result.writes], ["a", "b"])

    async def test_produced_records_are_consumed_once_and_in_order_per_key(self) -> None:
        seen: list[str] = []
        by_key: dict[str, list[int]] = {}

        async def handler(record: ReceivedRecord) -> None:
            value = _text(record.value)
            seen.append(value)
            by_key.setdefault(_text(record.key), []).append(int(value[1:]))

        async with self.client.producer() as producer, self.client.consumer([self.topic], handler):
            await self._send(producer, "v", 1, 300)
            await _eventually("all records handled", lambda: len(seen) >= 300)
        self.assertEqual(set(seen), _expected("v", 1, 300))
        self.assertEqual(len(seen), 300, "no duplicates")
        self.assertEqual(len(by_key), 7)
        for values in by_key.values():
            self.assertEqual(values, sorted(values))

        # Everything was processed, so a new consumer of the group has nothing to read.
        again: list[str] = []

        async def second(record: ReceivedRecord) -> None:
            again.append(_text(record.value))

        async with self.client.consumer([self.topic], second):
            await asyncio.sleep(3)
        self.assertEqual(again, [])

    async def test_unconfigured_topic_fails_the_send_without_breaking_the_producer(self) -> None:
        async with self.client.producer() as producer:
            with self.assertRaises(ProduceError) as raised:
                await asyncio.wait_for(producer.send(self.unconfigured, value="v"), _TIMEOUT)
            self.assertEqual(raised.exception.code, ErrorCode.ERROR_CODE_TOPIC_NOT_CONFIGURED)
            self.assertFalse(raised.exception.outcome_unknown)
            self.assertEqual(raised.exception.writes, ())
            await self._send(producer, "v", 1, 5)

    async def test_handler_that_raises_stops_the_consumer_and_the_record_is_delivered_again(
        self,
    ) -> None:
        async with self.client.producer() as producer:
            await self._send(producer, "v", 1, 20)
        first: list[str] = []

        async def fails(record: ReceivedRecord) -> None:
            if _text(record.value) == "v13":
                raise RuntimeError("cannot process v13")
            first.append(_text(record.value))

        listener = _Tracking()
        failing = self.client.consumer([self.topic], fails, listener=listener)
        cause = await asyncio.wait_for(listener.failure, _TIMEOUT)
        self.assertIsInstance(cause, RuntimeError)
        self.assertEqual(str(cause), "cannot process v13")
        await failing.close()
        self.assertNotIn("v13", first)

        second: list[str] = []

        async def works(record: ReceivedRecord) -> None:
            second.append(_text(record.value))

        async with self.client.consumer([self.topic], works):
            await _eventually("v13 is delivered again", lambda: "v13" in second)
            await _eventually(
                "everything is processed by one consumer or the other",
                lambda: set(first) | set(second) == _expected("v", 1, 20),
            )

    async def test_second_consumer_shares_the_group_and_each_record_is_handled_once(self) -> None:
        one: list[str] = []
        two: list[str] = []

        async def handle_one(record: ReceivedRecord) -> None:
            one.append(_text(record.value))

        async def handle_two(record: ReceivedRecord) -> None:
            two.append(_text(record.value))

        track_one, track_two = _Tracking(), _Tracking()
        async with (
            self.client.producer() as producer,
            self.client.consumer([self.topic], handle_one, listener=track_one),
        ):
            await _eventually(
                "the first consumer owns every partition",
                lambda: len(track_one.owned) == _PARTITIONS,
            )
            async with self.client.consumer([self.topic], handle_two, listener=track_two):
                await _eventually(
                    "the partitions are split",
                    lambda: (
                        bool(track_two.owned)
                        and len(track_one.owned) + len(track_two.owned) == _PARTITIONS
                        and track_one.owned.isdisjoint(track_two.owned)
                    ),
                )
                await self._send(producer, "v", 1, 200)
                await _eventually("all records handled", lambda: len(one) + len(two) >= 200)
        everything = one + two
        self.assertEqual(set(everything), _expected("v", 1, 200))
        self.assertEqual(len(everything), 200, "each record was handled by exactly one consumer")
        self.assertTrue(one)
        self.assertTrue(two)

    async def test_producer_and_consumer_survive_a_server_restart(self) -> None:
        seen: list[str] = []

        async def handler(record: ReceivedRecord) -> None:
            seen.append(_text(record.value))

        listener = _Tracking()
        async with (
            self.client.producer() as producer,
            self.client.consumer([self.topic], handler, listener=listener),
        ):
            await self._send(producer, "before", 1, 50)
            await _eventually("the first records are handled", lambda: len(seen) >= 50)

            await self.server.restart()

            # The same producer and consumer objects, against a server with a new epoch.
            await self._send(producer, "after", 1, 50)
            await _eventually(
                "the records sent after the restart are handled",
                lambda: set(seen) >= _expected("after", 1, 50),
            )
        self.assertEqual(
            len(listener.lost_ever), _PARTITIONS, "the consumer was told its partitions were gone"
        )
        self.assertGreaterEqual(set(seen), _expected("before", 1, 50))
        # Records acked just before the restart may not have been committed yet, so "before"
        # records can be handled twice. "after" records cannot: nothing interrupted them.
        self.assertEqual(len([v for v in seen if v.startswith("after")]), 50)


if __name__ == "__main__":
    unittest.main(verbosity=2)
