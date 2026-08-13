"""The reproducibility contract, enforced on every PR.

Runs the generator against the committed micro FIRDS fixture (KB-scale, so CI
never needs the real corpus) and asserts three things: the same seed
reproduces the same combined content hash twice, that hash equals the pinned
value below, and a different seed produces a different hash.

The pin is cross-platform: verified byte-identical between ARM macOS and
x86-64 Linux (python:3.12-slim container, pinned dependencies) on 2026-08-10.
numpy's bit-stream RNG is platform-stable and the 4-decimal price rounding
absorbs last-ulp libm differences -- verified empirically, per pin.

PINNED_HASH changes ONLY when generator behaviour changes on purpose. Re-pin
with the value both runs agree on, re-run the container check, and record why
in the commit message.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE = REPO_ROOT / 'tests' / 'fixtures' / 'firds_micro'

PINNED_HASH = '84cb8cee1930bf024f3639eccb9da0101d283799954a3314d4aede8233472e13'

ARGS = [
    '--scale', '0.05',
    '--start', '2026-05-01',
    '--end', '2026-07-17',
    '--parquet-dir', str(FIXTURE),
]


def run_generator(seed: int, out: Path) -> str:
    cmd = [
        sys.executable, '-m', 'ingestion.generate.cli',
        '--seed', str(seed), '--out', str(out), *ARGS,
    ]
    result = subprocess.run(
        cmd, cwd=REPO_ROOT, capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, f'generator failed:\n{result.stderr}'
    manifest = json.loads((out / 'manifest.json').read_text())
    return manifest['combined_sha256']


def test_same_seed_reproduces_pinned_hash(tmp_path):
    h1 = run_generator(42, tmp_path / 'a')
    h2 = run_generator(42, tmp_path / 'b')
    assert h1 == h2, 'same seed produced different content hashes'
    assert h1 == PINNED_HASH, (
        f'behaviour changed: got {h1}, pinned {PINNED_HASH} -- if the change '
        'is intentional, re-pin and say why in the commit message'
    )


def test_different_seed_differs(tmp_path):
    h42 = run_generator(42, tmp_path / 'a')
    h43 = run_generator(43, tmp_path / 'c')
    assert h42 != h43, 'different seeds produced an identical content hash'
