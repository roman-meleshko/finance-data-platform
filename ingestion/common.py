"""Helpers shared by the table-based normalizers (GLEIF, MIC).

Everything here operates on pyarrow Tables. The FIRDS shredder accumulates
plain Python lists while walking XML, before Arrow enters the picture, so it
keeps its own check logic.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc


def blank_or_null(column: pa.ChunkedArray) -> pa.ChunkedArray:
    """True where a value is null or the empty string.

    Sources pad unused fields with '' rather than leaving them null, so a
    null test alone would miss most genuinely absent values.
    """
    return pc.fill_null(pc.or_(pc.is_null(column), pc.equal(column, "")), True)


def bad_format(column: pa.ChunkedArray, regex: str) -> pa.ChunkedArray:
    """True where a present value does not match the regex."""
    present = pc.invert(blank_or_null(column))
    matches = pc.fill_null(pc.match_substring_regex(column, regex), False)
    return pc.and_(present, pc.invert(matches))


def duplicate_keys(table: pa.Table, keys: list[str]) -> pa.Table:
    """Key combinations occurring more than once."""
    counted = table.group_by(keys).aggregate([(keys[0], "count")])
    return counted.filter(pc.greater(counted[f"{keys[0]}_count"], 1))


def add_lineage(table: pa.Table, path: Path, publication_date: str) -> pa.Table:
    """Append the three constant provenance columns.

    The publication date comes from a source-specific place (filename stamp,
    API metadata, release page), so the caller supplies it.
    """
    values = {
        "source_file": path.name,
        "publication_date": publication_date,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }
    for name, value in values.items():
        table = table.append_column(
            name, pa.array([value] * table.num_rows, type=pa.string())
        )
    return table
