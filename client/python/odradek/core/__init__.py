"""The client protocol core (ADR 1), ported from Java: events in, actions out, no I/O."""

from odradek.core.consume import ConsumeCore
from odradek.core.produce import ProduceCore

__all__ = ["ConsumeCore", "ProduceCore"]
