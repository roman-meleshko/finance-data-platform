"""Project GLEIF LEI2 and RR (Relationship Records) GOLDEN COPY as Parquet.

Processing and output grain:
 - For LEI2, only columns relevant to the project are kept. One line per LEI.
 - For RR, unpivot periodType columns to keep only periodType == 'RELATIONSHIP_PERIOD'.
   One line per StartNode + EndNode pair + RelationshipType.
 - Rename columns to snake_case.

Tests:
 - File columns = expected columns (pyarrow raises if an include_column is absent).
 - LEI non-null, unique, and a valid LEI identifier.
 - (child, parent, relationship_type) unique; both nodes valid LEIs.

Run from the repo root: python -m ingestion.gleif.normalize
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from ingestion.common import add_lineage, bad_format, blank_or_null, duplicate_keys

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SRC = REPO_ROOT / "data" / "raw" / "gleif" / "golden-copy"
DEFAULT_OUT = REPO_ROOT / "data" / "parquet" / "gleif"

BLOCK_SIZE = 32 << 20  # 32 MiB per read batch

# As a string, not a compiled pattern: Arrow's match_substring_regex takes RE2 source.
LEI_REGEX = r"^[A-Z0-9]{18}[0-9]{2}$"

# ---------------------------------------------------------------- LEI2
# Source column -> output column. Read columns and written columns are the same
# here, so this single map drives both the reader and the schema.
FIELDS_LEI = {
    "LEI":                                   "lei",
    "Entity.LegalName":                      "legal_name",
    "Entity.LegalAddress.Country":           "legal_country",
    "Entity.LegalAddress.City":              "legal_city",
    "Entity.HeadquartersAddress.Country":    "hq_country",
    "Entity.HeadquartersAddress.City":       "hq_city",
    "Entity.LegalJurisdiction":              "legal_jurisdiction",
    "Entity.EntityCategory":                 "entity_category",
    "Entity.LegalForm.EntityLegalFormCode":  "legal_form_code",
    "Entity.EntityStatus":                   "entity_status",
    "Entity.EntityCreationDate":             "entity_creation_date",
    "Entity.SuccessorEntity.1.SuccessorLEI": "successor_lei",
    "Registration.RegistrationStatus":       "registration_status",
    "Registration.InitialRegistrationDate":  "initial_registration_date",
    "Registration.LastUpdateDate":           "last_update_date",
    "Registration.NextRenewalDate":          "next_renewal_date",
    "Registration.ManagingLOU":              "managing_lou",
    "ConformityFlag":                        "conformity_flag",
}

# ---------------------------------------------------------------- RR
# Unlike LEI2, the columns READ are not the columns WRITTEN: the five period
# slots collapse into one pair of dates. So RR needs three declarations.
FIELDS_RR = {
    "Relationship.StartNode.NodeID":     "child_lei",
    "Relationship.StartNode.NodeIDType": "child_id_type",
    "Relationship.EndNode.NodeID":       "parent_lei",
    "Relationship.EndNode.NodeIDType":   "parent_id_type",
    "Relationship.RelationshipType":     "relationship_type",
    "Relationship.RelationshipStatus":   "relationship_status",
}

# GLEIF flattens a repeating group into fixed numbered slots, and the slot index
# is ARBITRARY: measured on this file, Period.1 is a RELATIONSHIP_PERIOD 67% of
# the time and an ACCOUNTING_PERIOD 32% of the time. Never read by position.
PERIOD_SLOTS = [
    (
        f"Relationship.Period.{i}.periodType",
        f"Relationship.Period.{i}.startDate",
        f"Relationship.Period.{i}.endDate",
    )
    for i in range(1, 6)
]
WANTED_PERIOD = "RELATIONSHIP_PERIOD"

READ_COLUMNS_RR = list(FIELDS_RR) + [c for slot in PERIOD_SLOTS for c in slot]
OUT_COLUMNS_RR = list(FIELDS_RR.values()) + ["rel_start_date", "rel_end_date"]

LINEAGE = ("source_file", "publication_date", "ingested_at")


def build_schema(columns: list[str]) -> pa.Schema:
    """Declare the Parquet schema instead of letting Arrow infer it.

    Every column is a string: the raw layer mirrors what GLEIF published, and
    casting is a modelling decision that belongs in dbt.
    """
    return pa.schema([pa.field(name, pa.string()) for name in columns])


def publication_date(filename: str) -> str:
    """20260723-1600-gleif-goldencopy-lei2-golden-copy.csv -> 2026-07-23."""
    stamp = filename.split("-")[0]
    return f"{stamp[:4]}-{stamp[4:6]}-{stamp[6:8]}"


def source_file(kind: str) -> Path:
    """Newest golden-copy file for 'lei2' or 'rr'."""
    matches = sorted(DEFAULT_SRC.glob(f"*-{kind}-golden-copy.csv"))
    if not matches:
        raise FileNotFoundError(f"no {kind} golden copy under {DEFAULT_SRC}")
    return matches[-1]


def open_reader(path: Path, columns: list[str]) -> pacsv.CSVStreamingReader:
    """Stream the CSV, projecting to `columns` at read time."""
    try:
        return pacsv.open_csv(
            path,
            read_options=pacsv.ReadOptions(block_size=BLOCK_SIZE),
            convert_options=pacsv.ConvertOptions(
                include_columns=columns,
                column_types={name: pa.string() for name in columns},
            ),
        )
    except pa.ArrowKeyError as exc:
        raise ValueError(f"{path.name}: expected column missing -- {exc}") from exc


def _relationship_period(table: pa.Table) -> tuple[pa.ChunkedArray, pa.ChunkedArray]:
    """Collapse the five numbered period slots into one (start, end) pair.

    GLEIF flattens a repeating group into fixed slots and the slot index is
    ARBITRARY, so the slot is selected by matching periodType, never by position.
    """
    start = pa.nulls(table.num_rows, pa.string())
    end = pa.nulls(table.num_rows, pa.string())
    for type_column, start_column, end_column in reversed(PERIOD_SLOTS):
        # fill_null guards the condition: a null periodType would make if_else
        # return null and wipe a value a later slot had already supplied.
        match = pc.fill_null(pc.equal(table[type_column], WANTED_PERIOD), False)
        start = pc.if_else(match, table[start_column], start)
        end = pc.if_else(match, table[end_column], end)
    return start, end


def _read_table(path: Path, columns: list[str], limit: int | None) -> pa.Table:
    """Stream the CSV into one Arrow table, honouring an exact row limit."""
    batches = []
    total = 0
    for batch in open_reader(path, columns):
        if limit is not None:
            remaining = limit - total
            if batch.num_rows > remaining:
                batch = batch.slice(0, remaining)
        batches.append(batch)
        total += batch.num_rows
        if limit is not None and total >= limit:
            break
    return pa.Table.from_batches(batches, schema=batches[0].schema) if batches \
        else pa.table({name: pa.array([], type=pa.string()) for name in columns})


def project_lei(path: Path, limit: int | None = None) -> pa.Table:
    """Project the LEI2 golden copy to the modelled columns. One row per LEI."""
    table = _read_table(path, list(FIELDS_LEI), limit)
    table = table.rename_columns([FIELDS_LEI[name] for name in table.schema.names])
    return add_lineage(table, path, publication_date(path.name))


def project_rr(path: Path, limit: int | None = None) -> pa.Table:
    """Project the relationship records. One row per (child, parent, type)."""
    table = _read_table(path, READ_COLUMNS_RR, limit)
    start, end = _relationship_period(table)
    table = table.select(list(FIELDS_RR))  # drop the 15 slot columns
    table = table.rename_columns([FIELDS_RR[name] for name in table.schema.names])
    table = table.append_column("rel_start_date", start)
    table = table.append_column("rel_end_date", end)
    return add_lineage(table, path, publication_date(path.name))


def check_lei(table: pa.Table) -> list[str]:
    """Structural checks on the entity table."""
    problems = []

    missing = table.filter(blank_or_null(table["lei"]))
    if missing.num_rows:
        problems.append(f"{missing.num_rows} rows with no lei")

    # mode="all" counts null as a value; the default ignores nulls and would
    # report a missing lei a second time as a phantom duplicate.
    distinct = pc.count_distinct(table["lei"], mode="all").as_py()
    if distinct != table.num_rows:
        duplicates = duplicate_keys(table, ["lei"])
        examples = duplicates["lei"].slice(0, 3).to_pylist()
        problems.append(
            f"{table.num_rows - distinct} duplicate lei values, e.g. {examples}"
        )

    bad = table.filter(bad_format(table["lei"], LEI_REGEX))
    if bad.num_rows:
        problems.append(
            f"{bad.num_rows} rows where lei is not a valid LEI, "
            f"e.g. {bad['lei'].slice(0, 3).to_pylist()}"
        )

    return problems


def check_rr(table: pa.Table) -> list[str]:
    """Structural checks on the relationship table."""
    problems = []
    keys = ["child_lei", "parent_lei", "relationship_type"]

    duplicates = duplicate_keys(table, keys)
    if duplicates.num_rows:
        examples = duplicates.select(keys).slice(0, 3).to_pylist()
        problems.append(
            f"{duplicates.num_rows} duplicate {tuple(keys)} keys, e.g. {examples}"
        )

    for column in ("child_lei", "parent_lei"):
        bad = table.filter(bad_format(table[column], LEI_REGEX))
        if bad.num_rows:
            problems.append(
                f"{bad.num_rows} rows where {column} is not a valid LEI, "
                f"e.g. {bad[column].slice(0, 3).to_pylist()}"
            )

    # Both endpoints are declared to be LEIs; anything else means the graph is
    # not the entity graph we think it is.
    for column in ("child_id_type", "parent_id_type"):
        unexpected = table.filter(
            pc.invert(pc.fill_null(pc.equal(table[column], "LEI"), False))
        )
        if unexpected.num_rows:
            problems.append(
                f"{unexpected.num_rows} rows where {column} is not 'LEI', "
                f"e.g. {unexpected[column].slice(0, 3).to_pylist()}"
            )

    return problems


def write(table: pa.Table, order: list[str], destination: Path) -> None:
    """Write the table to Parquet under an explicitly declared schema.

    select() fixes column order and cast() enforces the declared types, so the
    file's schema is stated here rather than inherited from whatever the reader
    happened to produce.
    """
    destination.parent.mkdir(parents=True, exist_ok=True)
    table = table.select(order).cast(build_schema(order))
    pq.write_table(table, destination, compression="snappy")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "files",
        nargs="?",
        choices=("lei", "rr", "both"),
        default="both",
        help="Which golden copy to project (default: both).",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_OUT,
        help="Output directory for the Parquet tables.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Stop after this many records per file (development aid).",
    )
    args = parser.parse_args()
    if args.limit is not None and args.out == DEFAULT_OUT:
        parser.error(
            "--limit needs an explicit --out: it would overwrite the full dataset"
        )
    return args


def main() -> int:
    args = parse_args()

    if args.files in ("lei", "both"):
        path = source_file("lei2")
        table = project_lei(path, args.limit)
        problems = check_lei(table)
        if problems:
            print(f"{path.name}: {table.num_rows} entities [FAIL]")
            for problem in problems:
                print(f"    {problem}")
            return 1

        out_path = args.out / "gleif_entity.parquet"
        write(table, list(FIELDS_LEI.values()) + list(LINEAGE), out_path)
        print(f"{table.num_rows} rows saved to {out_path} [ok]")

    if args.files in ("rr", "both"):
        path = source_file("rr")
        table = project_rr(path, args.limit)
        problems = check_rr(table)
        out_path = args.out / "gleif_relationship.parquet"

        if problems:
            print(f"{path.name}: {table.num_rows} relationships [FAIL]")
            for problem in problems:
                print(f"    {problem}")
            return 1

        write(table, OUT_COLUMNS_RR + list(LINEAGE), out_path)
        print(f"{table.num_rows} rows saved to {out_path} [ok]")

    return 0


if __name__ == "__main__":
    sys.exit(main())
