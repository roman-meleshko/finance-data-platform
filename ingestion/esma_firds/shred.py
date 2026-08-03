"""Shred ESMA FIRDS FULINS files into instrument and underlying tables as Parquet.

One <RefData> element is one instrument admitted on one trading venue, so the
instrument table's grain is (ISIN, MIC). Underlyings are 0..N per record and
become a child table keyed back to that pair.
"""

from __future__ import annotations

import argparse
import re
import sys
import xml.etree.ElementTree as ET
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SRC = REPO_ROOT / "data" / "raw" / "esma_firds" / "fulins"
DEFAULT_OUT = REPO_ROOT / "data" / "parquet"

# instrument-table columns: {column_name: XML path relative to RefData}.
# Paths are fully qualified because two different fields are both named "Id":
# FinInstrmGnlAttrbts/Id is the ISIN, TradgVnRltdAttrbts/Id is the venue MIC.
FIELDS = {
    # FinInstrmGnlAttrbts — always present
    "isin":                       "{*}FinInstrmGnlAttrbts/{*}Id",
    "full_name":                  "{*}FinInstrmGnlAttrbts/{*}FullNm",
    "short_name":                 "{*}FinInstrmGnlAttrbts/{*}ShrtNm",
    "cfi_code":                   "{*}FinInstrmGnlAttrbts/{*}ClssfctnTp",
    "notional_ccy":               "{*}FinInstrmGnlAttrbts/{*}NtnlCcy",
    "commodity_deriv_indicator":  "{*}FinInstrmGnlAttrbts/{*}CmmdtyDerivInd",

    # issuer — direct child of RefData
    "issuer_lei":                 "{*}Issr",

    # TradgVnRltdAttrbts — the venue half of the grain
    "trading_venue_mic":          "{*}TradgVnRltdAttrbts/{*}Id",
    "issuer_requested":           "{*}TradgVnRltdAttrbts/{*}IssrReq",
    "admission_approval_dt":      "{*}TradgVnRltdAttrbts/{*}AdmssnApprvlDtByIssr",
    "admission_request_dt":       "{*}TradgVnRltdAttrbts/{*}ReqForAdmssnDt",
    "first_trade_dt":             "{*}TradgVnRltdAttrbts/{*}FrstTradDt",
    "termination_dt":             "{*}TradgVnRltdAttrbts/{*}TermntnDt",

    # DebtInstrmAttrbts — populated only on debt (null elsewhere)
    "debt_total_issued_nominal":  "{*}DebtInstrmAttrbts/{*}TtlIssdNmnlAmt",
    "debt_maturity_dt":           "{*}DebtInstrmAttrbts/{*}MtrtyDt",
    "debt_nominal_per_unit":      "{*}DebtInstrmAttrbts/{*}NmnlValPerUnit",

    # DerivInstrmAttrbts scalars — populated only on derivatives (null elsewhere)
    "derivative_expiry_dt":       "{*}DerivInstrmAttrbts/{*}XpryDt",
    "price_multiplier":           "{*}DerivInstrmAttrbts/{*}PricMltplr",
    "option_type":                "{*}DerivInstrmAttrbts/{*}OptnTp",
    "option_exercise_style":      "{*}DerivInstrmAttrbts/{*}OptnExrcStyle",
    "delivery_type":              "{*}DerivInstrmAttrbts/{*}DlvryTp",

    # TechAttrbts — record-level technical data
    "valid_from":                 "{*}TechAttrbts/{*}PblctnPrd/{*}FrDt",
    "relevant_competent_authority": "{*}TechAttrbts/{*}RlvntCmptntAuthrty",
    "relevant_trading_venue":     "{*}TechAttrbts/{*}RlvntTradgVn",
    "inconsistency_indicator":    "{*}TechAttrbts/{*}IncnsstncyInd",
    "last_updated":               "{*}TechAttrbts/{*}LastUpd",
}

LINEAGE = ("cfi_category", "source_file", "publication_date", "ingested_at")
INSTRUMENT_COLUMNS = list(FIELDS) + list(LINEAGE)
UNDERLYING_COLUMNS = [
    "parent_isin",
    "parent_mic",
    "underlying_source",
    "underlying_type",
    "underlying_id",
    "underlying_name",
    "ordinal",
    *LINEAGE,
]

# Values read from the source stay strings: the raw layer mirrors what was
# published and dbt does the casting. Only ordinal is typed here, because we
# generate it rather than read it.
INT_COLUMNS = {"ordinal"}

# One list per column, keyed by column name: the accumulator shape used throughout.
Columns = dict[str, list]

UNDERLYING_BASE = "{*}DerivInstrmAttrbts/{*}UndrlygInstrm"

# Identifier formats, lifted verbatim from the FULINS XSD's simpleType restrictions
# (ISINOct2015Identifier, LEIIdentifier, MICIdentifier). Asserting them is cheap and
# catches the failure that matters most here: extracting the WRONG element. Two fields
# in this schema are both called "Id", so a bad path yields a plausible-looking string
# rather than an error.
PATTERNS = {
    "ISIN": re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$"),
    "LEI": re.compile(r"^[A-Z0-9]{18}[0-9]{2}$"),
    "MIC": re.compile(r"^[A-Z0-9]{4}$"),
}


def build_schema(columns: list[str]) -> pa.Schema:
    """Declare the Parquet schema instead of letting Arrow infer it.

    Inference reads the rows it is given, so a column that happens to be all
    null in one file (debt fields in a futures file, for example) would land as
    a null-typed column there and a string elsewhere. The per-file Parquet
    schemas would then disagree and the warehouse load would fail.
    """
    return pa.schema(
        [(c, pa.int32() if c in INT_COLUMNS else pa.string()) for c in columns]
    )


def publication_date(filename: str) -> str:
    """FULINS_F_20260718_01of01.xml -> 2026-07-18."""
    stamp = filename.split("_")[2]
    return f"{stamp[:4]}-{stamp[4:6]}-{stamp[6:8]}"


def cfi_category(filename: str) -> str:
    """The letter in the filename is the CFI category of every record inside."""
    return filename.split("_")[1]


def underlyings(elem: ET.Element):
    """Yield (source, type, identifier, name) for each underlying of one record.

    Four branches, because the data holds four different shapes:
      Sngl/ISIN      one underlying security
      Sngl/LEI       one underlying entity, i.e. a credit reference
      Sngl/Indx      one underlying index or reference rate; its ISIN is
                     optional, so the name can be the only identifier
      Bskt/ISIN|LEI  a basket, where both elements repeat 0..unbounded
    """
    for node in elem.findall(f"{UNDERLYING_BASE}/{{*}}Sngl/{{*}}ISIN"):
        yield "single", "ISIN", node.text, None
    for node in elem.findall(f"{UNDERLYING_BASE}/{{*}}Sngl/{{*}}LEI"):
        yield "single", "LEI", node.text, None
    for node in elem.findall(f"{UNDERLYING_BASE}/{{*}}Sngl/{{*}}Indx"):
        yield (
            "single",
            "INDEX",
            node.findtext("{*}ISIN"),
            node.findtext("{*}Nm/{*}RefRate/{*}Nm"),
        )
    for kind in ("ISIN", "LEI"):
        for node in elem.findall(f"{UNDERLYING_BASE}/{{*}}Bskt/{{*}}{kind}"):
            yield "basket", kind, node.text, None


def shred(path: Path, limit: int | None = None) -> tuple[Columns, Columns]:
    """Stream one FULINS file into instrument columns and underlying columns.

    Rows accumulate COLUMN-WISE (one list per column) rather than as a list of
    per-row dicts. XML forces the parse itself to be row-by-row, but the
    accumulator does not have to be: dict-of-lists avoids allocating a dict per
    record and lets pa.table() take the lists directly, with no transposition.

    The gain here is modest -- measured on FULINS_C, peak RSS 462 -> 352 MB and
    16.8 -> 15.3s, so ~1.3x memory and ~1.1x speed. It is only the container
    overhead that goes away: the XML parser already produced every value as a
    Python string, so both shapes hold the same strings. Compare the CSV path,
    where the values start life inside Arrow and to_pylist() would COPY them
    into new Python objects -- there the same change is worth ~5x.
    """
    lineage = {
        "cfi_category": cfi_category(path.name),
        "source_file": path.name,
        "publication_date": publication_date(path.name),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }
    instruments: Columns = {name: [] for name in INSTRUMENT_COLUMNS}
    underlying: Columns = {name: [] for name in UNDERLYING_COLUMNS}
    n_records = 0

    container = None
    for event, elem in ET.iterparse(path, events=("start", "end")):
        tag = elem.tag.rsplit("}", 1)[-1]
        if event == "start":
            if tag == "FinInstrmRptgRefDataRpt":
                container = elem
            continue
        if tag != "RefData":
            continue

        for column, xpath in FIELDS.items():
            instruments[column].append(elem.findtext(xpath))
        for column, value in lineage.items():
            instruments[column].append(value)
        n_records += 1

        isin = instruments["isin"][-1]
        mic = instruments["trading_venue_mic"][-1]
        for ordinal, (source, kind, ident, name) in enumerate(
            underlyings(elem), start=1
        ):
            underlying["parent_isin"].append(isin)
            underlying["parent_mic"].append(mic)
            underlying["underlying_source"].append(source)  # single or basket
            underlying["underlying_type"].append(kind)      # ISIN, LEI or INDEX
            underlying["underlying_id"].append(ident)
            underlying["underlying_name"].append(name)      # only an index has one
            underlying["ordinal"].append(ordinal)  # XML is ordered, SQL rows are not
            for column, value in lineage.items():
                underlying[column].append(value)

        # Release the finished record, then drop the processed siblings that the
        # parser keeps appending to the report element, so memory stays flat.
        elem.clear()
        if container is not None:
            container.clear()

        if limit and n_records >= limit:
            break

    return instruments, underlying


def check(instruments: Columns, underlying: Columns) -> list[str]:
    """Structural checks: did the shred faithfully represent the file?

    Reads whole columns and pairs them with zip() where a check spans more than
    one, which is the columnar equivalent of iterating rows.
    """
    problems = []

    isins = instruments["isin"]
    mics = instruments["trading_venue_mic"]

    missing_key = sum(1 for isin, mic in zip(isins, mics) if not isin or not mic)
    if missing_key:
        problems.append(f"{missing_key} instrument rows missing isin or mic")

    keys = Counter(zip(isins, mics))
    duplicates = [key for key, n in keys.items() if n > 1]
    if duplicates:
        problems.append(
            f"{len(duplicates)} duplicate (isin, mic) keys, e.g. {duplicates[:3]}"
        )

    orphans = {
        key
        for key in zip(underlying["parent_isin"], underlying["parent_mic"])
        if key not in keys
    }
    if orphans:
        problems.append(f"{len(orphans)} underlying rows with no parent record")

    unidentified = sum(
        1
        for ident, name in zip(
            underlying["underlying_id"], underlying["underlying_name"]
        )
        if not ident and not name
    )
    if unidentified:
        problems.append(f"{unidentified} underlying rows with neither id nor name")

    # Identifier formats. A value that doesn't match its declared pattern almost always
    # means the wrong element was read, not that the publisher sent bad data.
    for column, kind in (
        ("isin", "ISIN"),
        ("trading_venue_mic", "MIC"),
        ("issuer_lei", "LEI"),
    ):
        pattern = PATTERNS[kind]
        bad = [v for v in instruments[column] if v and not pattern.match(v)]
        if bad:
            problems.append(
                f"{len(bad)} instrument rows where {column} is not a valid {kind}, "
                f"e.g. {bad[:3]}"
            )

    # The same test on the child table doubles as a check on the BRANCH LOGIC: an
    # underlying routed into the wrong branch of underlyings() would carry an
    # identifier of the wrong shape. INDEX rows are exempt — their identifier is an
    # optional ISIN, and the name may be the only identifier they have.
    mistyped = [
        (kind, ident)
        for kind, ident in zip(
            underlying["underlying_type"], underlying["underlying_id"]
        )
        if kind in PATTERNS and ident and not PATTERNS[kind].match(ident)
    ]
    if mistyped:
        problems.append(
            f"{len(mistyped)} underlying rows whose id does not match its declared "
            f"type, e.g. {mistyped[:3]}"
        )

    return problems


def write(columns: Columns, order: list[str], destination: Path) -> None:
    """Write accumulated columns to Parquet under a declared schema.

    pa.table() takes the lists as columns directly -- no transposition, unlike
    Table.from_pylist() which has to pivot a list of per-row dicts.
    """
    destination.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {name: columns[name] for name in order}, schema=build_schema(order)
    )
    pq.write_table(table, destination, compression="snappy")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "files",
        nargs="*",
        type=Path,
        help="FULINS XML files (default: every FULINS file in data/raw).",
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
    files = args.files or sorted(DEFAULT_SRC.glob("FULINS_*.xml"))
    if not files:
        print(f"No FULINS files found in {DEFAULT_SRC}")
        return 1

    failed = False
    total_rows = 0
    total_underlyings = 0

    for path in files:
        instruments, underlying = shred(path, args.limit)
        problems = check(instruments, underlying)
        n_instruments = len(instruments["isin"])
        n_underlying = len(underlying["parent_isin"])

        write(
            instruments,
            INSTRUMENT_COLUMNS,
            args.out / "firds_instrument" / f"{path.stem}.parquet",
        )
        write(
            underlying,
            UNDERLYING_COLUMNS,
            args.out / "firds_underlying" / f"{path.stem}.parquet",
        )

        total_rows += n_instruments
        total_underlyings += n_underlying
        print(
            f"{path.name}: {n_instruments} instruments, "
            f"{n_underlying} underlyings [{'FAIL' if problems else 'ok'}]"
        )
        for problem in problems:
            print(f"    {problem}")
            failed = True

    print(
        f"\n{len(files)} file(s): {total_rows} instruments, "
        f"{total_underlyings} underlyings -> {args.out}"
    )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
