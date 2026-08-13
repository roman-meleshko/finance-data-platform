"""One seed, independent random streams per component.

Each component gets its own generator spawned from the master SeedSequence, so
adding a component (or drawing more in one of them) never shifts the output of
the others. Global seeding is deliberately avoided.
"""

from __future__ import annotations

import numpy as np

# 'defects' is reserved: defect injection is currently deterministic and draws
# nothing, but the stream keeps its spawn slot so re-adding draws never shifts
# the later streams (spawn children are stable by index). 'assign' (RM
# reassignments) is appended last for the same reason.
STREAM_NAMES = ('master', 'universe', 'market', 'idio', 'trades', 'defects', 'assign')


def make_streams(seed: int) -> dict[str, np.random.Generator]:
    root = np.random.SeedSequence(seed)
    children = root.spawn(len(STREAM_NAMES))
    return {
        name: np.random.default_rng(child)
        for name, child in zip(STREAM_NAMES, children)
    }


def mimesis_seed(seed: int, offset: int) -> int:
    """Stable integer seed for mimesis providers, decoupled from the numpy streams."""
    return (seed * 1_000_003 + offset) % 2**31
