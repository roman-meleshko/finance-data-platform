"""Normalize the ISO 10383 MIC list to Parquet.

One row per MIC, operating and segment alike; filtering by status or type is a
modelling decision that belongs to dbt. Columns are projected to the venue
fields the platform needs, renamed to snake_case, and written as strings so
the raw layer mirrors what ISO published.

Tests:
 - File columns = expected columns (pyarrow raises if an include_column is absent).
 - MIC non-null, unique, and four uppercase alphanumerics.
 - Row count above a floor, so a truncated file cannot load silently.

Run from the repo root: python -m ingestion.iso_mic.normalize
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from ingestion.common import add_lineage, bad_format, blank_or_null, duplicate_keys

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SRC = REPO_ROOT / "data" / "raw" / "iso_mic"
DEFAULT_OUT = REPO_ROOT / "data" / "parquet" / "iso_mic" / "iso_mic.parquet"

MIC_REGEX = r"^[A-Z0-9]{4}$"

# The acquisition script stamps the official publication date (scraped from
# the release page; the CSV itself carries no date) into the filename. That
# name is the contract between the two scripts, so parse it strictly and fail
# loudly when it drifts instead of slicing blindly.
FILENAME_REGEX = re.compile(r"^ISO10383_MIC-(\d{4}-\d{2}-\d{2})\.csv$")

FIELDS = {
    "MIC": "mic",
    "OPERATING MIC": "operating_mic",
    "OPRT/SGMT": "oprt_sgmt",
    "MARKET NAME-INSTITUTION DESCRIPTION": "market_name_institution_description",
    "LEGAL ENTITY NAME": "legal_entity_name",
    "LEI": "lei",
    "MARKET CATEGORY CODE": "market_category_code",
    "ISO COUNTRY CODE (ISO 3166)": "iso_country_code",
    "CITY": "city",
    "STATUS": "status",
    "CREATION DATE": "creation_date",
    "LAST UPDATE DATE": "last_update_date",
    "LAST VALIDATION DATE": "last_validation_date",
    "EXPIRY DATE": "expiry_date",
}

LINEAGE = ("source_file", "publication_date", "ingested_at")


def get_latest_file_path() -> Path:
    matches = sorted(DEFAULT_SRC.glob("ISO10383_MIC-*.csv"))
    if not matches:
        raise FileNotFoundError(f"no ISO MIC file under {DEFAULT_SRC}")
    return matches[-1]


def publication_date(filename: str) -> str:
    """ISO10383_MIC-2026-07-13.csv -> 2026-07-13."""
    match = FILENAME_REGEX.match(filename)
    if not match:
        raise ValueError(f"cannot parse publication date from {filename!r}")
    return match.group(1)


def read_table(path: Path) -> pa.Table:
    """Read the CSV, projecting to FIELDS with every column as a string.

    include_columns is the schema-drift guard: ISO republishes monthly, and a
    renamed column raises here with the file named rather than surfacing as a
    KeyError three functions later. Declared column_types stop type inference,
    which would otherwise load the date columns as int64.
    """
    try:
        return pacsv.read_csv(
            path,
            convert_options=pacsv.ConvertOptions(
                include_columns=list(FIELDS),
                column_types={name: pa.string() for name in FIELDS},
            ),
        )
    except pa.ArrowKeyError as exc:
        raise ValueError(f"{path.name}: expected column missing -- {exc}") from exc


def check_mic(table: pa.Table) -> list[str]:
    """Structural checks on the venue table."""
    problems = []
    n_rows = table.num_rows

    missing = table.filter(blank_or_null(table["MIC"]))
    if missing.num_rows:
        problems.append(f"{missing.num_rows} rows with no MIC")

    # mode="all" counts null as a value; the default ignores nulls and would
    # report a missing MIC a second time as a phantom duplicate.
    distinct = pc.count_distinct(table["MIC"], mode="all").as_py()
    if distinct != n_rows:
        duplicates = duplicate_keys(table, ["MIC"])
        examples = duplicates["MIC"].slice(0, 3).to_pylist()
        problems.append(
            f"{n_rows - distinct} duplicate MIC values, e.g. {examples}"
        )

    bad = table.filter(bad_format(table["MIC"], MIC_REGEX))
    if bad.num_rows:
        problems.append(
            f"{bad.num_rows} rows where MIC is not a valid MIC, "
            f"e.g. {bad['MIC'].slice(0, 3).to_pylist()}"
        )

    if n_rows < 2000:
        problems.append(
            f"number of rows in the file ({n_rows}) is lower than expected"
        )

    return problems


def write(table: pa.Table, destination: Path = DEFAULT_OUT) -> None:
    """Write the table to Parquet under an explicitly declared schema."""
    destination.parent.mkdir(parents=True, exist_ok=True)

    order = list(FIELDS) + list(LINEAGE)
    schema = pa.schema([pa.field(name, pa.string()) for name in order])

    table = table.select(order).cast(schema)
    table = table.rename_columns(list(FIELDS.values()) + list(LINEAGE))

    pq.write_table(table, destination, compression="snappy")


def main() -> int:
    file_path = get_latest_file_path()
    table = read_table(file_path)

    problems = check_mic(table)
    if problems:
        print(f"{file_path.name}: {table.num_rows} venues [FAIL]")
        for problem in problems:
            print(f"    {problem}")
        return 1

    table = add_lineage(table, file_path, publication_date(file_path.name))
    write(table)
    print(f"{table.num_rows} rows saved to {DEFAULT_OUT} [ok]")

    return 0


if __name__ == "__main__":
    sys.exit(main())
