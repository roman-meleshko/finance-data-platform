"""Deterministic output: stable sort, Parquet write, content hash.

The reproducibility contract is a hash over the sorted row content, not over
file bytes: Parquet embeds the writer version, so byte identity breaks on a
library upgrade even when the data is unchanged.

Every hashed token (column name, each value's repr) is terminated with a
0x1f unit separator: without framing, adjacent values collide -- [12, 3] and
[1, 23] hash identically -- and the hash's whole job is detecting difference.
"""

from __future__ import annotations

import hashlib
import json
import platform
from importlib import metadata
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# environment provenance stamped into the manifest -- outside the content
# hashes (which cover table values only), so purely informational: it answers
# "which library versions produced this dataset" without touching the contract
_ENV_PACKAGES = ('numpy', 'pandas', 'pyarrow', 'mimesis', 'exchange_calendars')


def _environment() -> dict:
    versions = {}
    for pkg in _ENV_PACKAGES:
        try:
            versions[pkg] = metadata.version(pkg)
        except metadata.PackageNotFoundError:
            versions[pkg] = 'unknown'
    versions['python'] = platform.python_version()
    return versions

SORT_KEYS = {
    'gen_desk': ['desk_id'],
    'gen_rm': ['rm_id'],
    'gen_rm_assignment': ['client_id', 'assigned_date'],
    'gen_client': ['client_id'],
    'gen_account': ['account_id'],
    'gen_trade': ['trade_id'],
    'gen_transfer': ['transfer_id'],
    'gen_fx_trade': ['fx_id'],
    'gen_price': ['business_date', 'isin'],
    'gen_cash_event': ['event_id'],
    'gen_position_snapshot': ['snapshot_date', 'account_id', 'isin'],
    'gen_crypto_mapping': ['isin'],
    'gen_calendar': ['calendar_date'],
}


def content_hash(table: pa.Table) -> str:
    digest = hashlib.sha256()
    for name in table.column_names:
        digest.update(name.encode())
        digest.update(b'\x1f')
        for value in table.column(name).to_pylist():
            digest.update(repr(value).encode())
            digest.update(b'\x1f')
    return digest.hexdigest()


def write_tables(
    out_dir: Path,
    tables: dict[str, dict[str, list]],
    run_stats: dict | None = None,
) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest: dict = {'environment': _environment(), 'tables': {}}
    if run_stats is not None:
        # everything the engine declined to do, stamped where a reader looks
        # first -- outside the content hashes, like the environment block
        manifest['run_stats'] = dict(sorted(run_stats.items()))
    for name in sorted(tables):
        table = pa.table(tables[name])
        keys = [(k, 'ascending') for k in SORT_KEYS[name]]
        table = table.sort_by(keys)
        pq.write_table(table, out_dir / f'{name}.parquet')
        manifest['tables'][name] = {
            'rows': table.num_rows,
            'sha256': content_hash(table),
        }
    combined = hashlib.sha256()
    for name in sorted(manifest['tables']):
        combined.update(f"{name}:{manifest['tables'][name]['sha256']}\n".encode())
    manifest['combined_sha256'] = combined.hexdigest()
    (out_dir / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    return manifest
