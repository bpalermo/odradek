"""A real fork(): the child is told it cannot use the parent's client.

Separate from client_test so that this process has no gRPC threads when it forks.
"""

from __future__ import annotations

import os
import unittest

from odradek import ForkedProcessError
from odradek._types import ProcessGuard


class ProcessGuardTest(unittest.TestCase):
    def test_a_forked_child_is_another_process(self) -> None:
        guard = ProcessGuard()
        guard.check()
        child = os.fork()
        if child == 0:
            # In the child: 0 if the guard refuses, as it must.
            try:
                guard.check()
            except ForkedProcessError:
                os._exit(0)
            os._exit(1)
        _, status = os.waitpid(child, 0)
        self.assertEqual(os.waitstatus_to_exitcode(status), 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
