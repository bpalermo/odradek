"""Runs every conformance scenario (//conformance) against the Python core.

The counterpart of ConformanceTest.java: each file is YAML in the protobuf
JSON mapping, parsed into an odradek.client.v1.Scenario; after each event the
core must return exactly the expected actions, compared as messages.
"""

from __future__ import annotations

import pathlib
import unittest
from typing import Any

import yaml
from google.protobuf import json_format

from odradek.client.v1 import core_pb2
from odradek.core import ConsumeCore, ProduceCore

_SCENARIOS = pathlib.Path("conformance/scenarios")


def _files() -> list[pathlib.Path]:
    return sorted(_SCENARIOS.rglob("*.yaml"))


def _load(file: pathlib.Path) -> core_pb2.Scenario:
    return json_format.ParseDict(yaml.safe_load(file.read_text("utf-8")), core_pb2.Scenario())


def _text(actions: Any) -> str:
    return "[" + ", ".join(json_format.MessageToJson(a, indent=None) for a in actions) + "]"


class ConformanceTest(unittest.TestCase):
    def _run(self, file: pathlib.Path) -> None:
        scenario = _load(file)
        subject = scenario.WhichOneof("subject")
        core: ProduceCore | ConsumeCore
        if subject == "produce":
            core = ProduceCore(scenario.produce.setup)
            steps: Any = scenario.produce.steps
        elif subject == "consume":
            core = ConsumeCore(scenario.consume.setup)
            steps = scenario.consume.steps
        else:
            self.fail(f"{file} has neither produce nor consume")
        for index, step in enumerate(steps):
            actual = core.handle(step.event)
            expected = list(step.expect)
            self.assertEqual(
                actual,
                expected,
                f"{file}, step {index} ({step.event.WhichOneof('kind')}):\n"
                f"  returned {_text(actual)}\n  expected {_text(expected)}",
            )

    def test_scenario_names_are_unique_and_match_their_files(self) -> None:
        files = _files()
        self.assertTrue(files, f"no scenarios under {_SCENARIOS.resolve()}")
        names: set[str] = set()
        for file in files:
            scenario = _load(file)
            self.assertTrue(scenario.description.strip(), f"{file} description")
            self.assertEqual(file.name, scenario.name + ".yaml")
            self.assertNotIn(scenario.name, names)
            names.add(scenario.name)


def _add(file: pathlib.Path) -> None:
    def test(self: ConformanceTest) -> None:
        self._run(file)

    setattr(ConformanceTest, "test_" + file.stem.replace("-", "_"), test)


# One test per scenario file, so a failure names the scenario and the count is visible.
for _file in _files():
    _add(_file)

if __name__ == "__main__":
    unittest.main(verbosity=2)
