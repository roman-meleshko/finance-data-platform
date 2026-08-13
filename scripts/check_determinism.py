"""Prove the generator's reproducibility contract.

Runs the generator twice with the same seed (expect identical combined content
hashes), then once with a different seed (expect a different hash). Exits
non-zero on any violation.
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

ARGS = ['--scale', '0.05', '--start', '2026-05-01', '--end', '2026-07-17']


def run(seed: int, out: Path) -> str:
    cmd = [
        sys.executable, '-m', 'ingestion.generate.cli',
        '--seed', str(seed), '--out', str(out), *ARGS,
    ]
    try:
        subprocess.run(cmd, cwd=REPO_ROOT, check=True, capture_output=True)
    except subprocess.CalledProcessError as e:
        # surface the child's stderr instead of swallowing it with the capture
        print(e.stderr.decode(errors='replace'), file=sys.stderr)
        raise
    manifest = json.loads((out / 'manifest.json').read_text())
    return manifest['combined_sha256']


def main() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        h1 = run(42, base / 'a')
        h2 = run(42, base / 'b')
        h3 = run(43, base / 'c')

    print(f'seed 42 run 1: {h1}')
    print(f'seed 42 run 2: {h2}')
    print(f'seed 43 run 1: {h3}')

    if h1 != h2:
        print('FAIL: same seed produced different content hashes')
        return 1
    if h1 == h3:
        print('FAIL: different seed produced an identical content hash')
        return 1
    print('PASS: same seed reproduces; different seed differs')
    return 0


if __name__ == '__main__':
    sys.exit(main())
