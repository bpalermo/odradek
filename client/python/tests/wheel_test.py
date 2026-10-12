"""The wheel is pure Python and carries the client, the core and the generated API code."""

from __future__ import annotations

import sys
import unittest
import zipfile

_WHEEL = sys.argv.pop(1)


class WheelTest(unittest.TestCase):
    def test_contents(self) -> None:
        self.assertTrue(_WHEEL.endswith("-py3-none-any.whl"), _WHEEL)
        with zipfile.ZipFile(_WHEEL) as wheel:
            names = set(wheel.namelist())
            metadata = next(n for n in names if n.endswith(".dist-info/METADATA"))
            text = wheel.read(metadata).decode("utf-8")
        for expected in (
            "odradek/__init__.py",
            "odradek/py.typed",
            "odradek/core/produce.py",
            "odradek/core/consume.py",
            "odradek/kafka/v1/odradek_pb2.py",
            "odradek/kafka/v1/odradek_pb2.pyi",
            "odradek/kafka/v1/odradek_pb2_grpc.py",
            "odradek/client/v1/core_pb2.py",
            "odradek/client/v1/core_pb2.pyi",
        ):
            self.assertIn(expected, names)
        self.assertFalse(
            [n for n in names if n.endswith((".so", ".pyd")) or n.startswith(("tests/", "client/"))]
        )
        self.assertIn("Name: odradek", text)
        self.assertIn("Requires-Dist: grpcio", text)
        self.assertIn("Requires-Dist: protobuf", text)


if __name__ == "__main__":
    unittest.main()
