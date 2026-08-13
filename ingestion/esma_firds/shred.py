"""Shred ESMA FIRDS reference files into instrument and underlying tables as Parquet.

Two file formats, one extraction. FULINS is the weekly full snapshot: one
<RefData> element is one instrument admitted on one trading venue, so the
instrument table's grain is (ISIN, MIC). DLTINS is the daily delta: the same
payload, wrapped in a <FinInstrm> whose single child names what happened --
NewRcrd, ModfdRcrd, TermntdRcrd or CancRcrd. The field paths are identical in
both.

Underlyings are 0..N per record and become a child table keyed back to the
parent pair.

The delta tables carry three extra columns that the snapshot does not need:
record_type (which of the four), reporting_date (the file's own statement of
its reporting period) and record_sequence (position in the file). The last one
matters downstream: two records for the same key can appear on one publication
day, and once the rows are in a table the publisher's document order is the
only thing that says which came second.
"""

from __future__ import annotations

import argparse
import re
import sys
import xml.etree.ElementTree as ET
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import NamedTuple

import pyarrow as pa
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[2]
RAW_ROOT = REPO_ROOT / 'data' / 'raw' / 'esma_firds'
DEFAULT_SRC = RAW_ROOT / 'fulins'
DELTA_SRC = RAW_ROOT / 'dltins'
DEFAULT_OUT = REPO_ROOT / 'data' / 'parquet'

RECORD_TYPES = {
    'NewRcrd': 'NEW',
    'ModfdRcrd': 'MODFD',
    'TermntdRcrd': 'TERMNTD',
    'CancRcrd': 'CANC',
}

# A cancellation says the record should never have been published, so it
# carries the two key fields and nothing else.
CANC_KEY_FIELDS = ('isin', 'trading_venue_mic')

# instrument-table columns: {column_name: XML path relative to the record
# element}. That element is <RefData> in a snapshot and <NewRcrd> / <ModfdRcrd>
# / <TermntdRcrd> / <CancRcrd> in a delta; the paths below are identical under
# all five, which is the whole reason one FIELDS map serves both formats.
FIELDS = {
    # FinInstrmGnlAttrbts — always present
    'isin':                       '{*}FinInstrmGnlAttrbts/{*}Id',
    'full_name':                  '{*}FinInstrmGnlAttrbts/{*}FullNm',
    'short_name':                 '{*}FinInstrmGnlAttrbts/{*}ShrtNm',
    'cfi_code':                   '{*}FinInstrmGnlAttrbts/{*}ClssfctnTp',
    'notional_ccy':               '{*}FinInstrmGnlAttrbts/{*}NtnlCcy',
    'commodity_deriv_indicator':  '{*}FinInstrmGnlAttrbts/{*}CmmdtyDerivInd',

    # issuer — direct child of RefData
    'issuer_lei':                 '{*}Issr',

    # TradgVnRltdAttrbts — the venue half of the grain
    'trading_venue_mic':          '{*}TradgVnRltdAttrbts/{*}Id',
    'issuer_requested':           '{*}TradgVnRltdAttrbts/{*}IssrReq',
    'admission_approval_dt':      '{*}TradgVnRltdAttrbts/{*}AdmssnApprvlDtByIssr',
    'admission_request_dt':       '{*}TradgVnRltdAttrbts/{*}ReqForAdmssnDt',
    'first_trade_dt':             '{*}TradgVnRltdAttrbts/{*}FrstTradDt',
    'termination_dt':             '{*}TradgVnRltdAttrbts/{*}TermntnDt',

    # DebtInstrmAttrbts — populated only on debt (null elsewhere)
    'debt_total_issued_nominal':  '{*}DebtInstrmAttrbts/{*}TtlIssdNmnlAmt',
    'debt_maturity_dt':           '{*}DebtInstrmAttrbts/{*}MtrtyDt',
    'debt_nominal_per_unit':      '{*}DebtInstrmAttrbts/{*}NmnlValPerUnit',

    # DerivInstrmAttrbts scalars — populated only on derivatives (null elsewhere)
    'derivative_expiry_dt':       '{*}DerivInstrmAttrbts/{*}XpryDt',
    'price_multiplier':           '{*}DerivInstrmAttrbts/{*}PricMltplr',
    'option_type':                '{*}DerivInstrmAttrbts/{*}OptnTp',
    'option_exercise_style':      '{*}DerivInstrmAttrbts/{*}OptnExrcStyle',
    'delivery_type':              '{*}DerivInstrmAttrbts/{*}DlvryTp',

    # TechAttrbts — record-level technical data
    'valid_from':                 '{*}TechAttrbts/{*}PblctnPrd/{*}FrDt',
    'relevant_competent_authority': '{*}TechAttrbts/{*}RlvntCmptntAuthrty',
    'relevant_trading_venue':     '{*}TechAttrbts/{*}RlvntTradgVn',
    'inconsistency_indicator':    '{*}TechAttrbts/{*}IncnsstncyInd',
    'last_updated':               '{*}TechAttrbts/{*}LastUpd',
}

LINEAGE = ('cfi_category', 'source_file', 'publication_date', 'ingested_at')
INSTRUMENT_COLUMNS = list(FIELDS) + list(LINEAGE)
UNDERLYING_COLUMNS = [
    'parent_isin',
    'parent_mic',
    'underlying_source',
    'underlying_type',
    'underlying_id',
    'underlying_name',
    'ordinal',
    *LINEAGE,
]

# Published in the delta and nowhere else.
#
# NvrPblshd flags an instrument reported AFTER its own termination date. ESMA
# states these appear only in the delta and never in a full file.
DELTA_ONLY_FIELDS = {
    'never_published': '{*}TechAttrbts/{*}NvrPblshd',
}

# What a delta row carries that neither format publishes -- generated here.
#
# reporting_date comes from the file's own <RptHdr>, not from its name. The
# name is a convenience and the header is the publisher's statement about its
# own reporting period; when they disagree, check() says so rather than
# silently preferring one.
#
# record_sequence is the position of the <FinInstrm> within the file, and it is
# the reason this column exists at all: a key can be modified and then
# terminated on the same publication day. Same argument as ordinal on the child table.
# It restarts at 1 per file, so the ordering key is (source_file,
# record_sequence). Part names sort correctly, 01of02 before 02of02.
DELTA_GENERATED = ('record_type', 'reporting_date', 'record_sequence')
DELTA_COLUMNS = (
    list(FIELDS) + list(DELTA_ONLY_FIELDS) + list(DELTA_GENERATED) + list(LINEAGE)
)
DELTA_UNDERLYING_COLUMNS = [
    'parent_isin',
    'parent_mic',
    'record_sequence',
    'underlying_source',
    'underlying_type',
    'underlying_id',
    'underlying_name',
    'ordinal',
    *LINEAGE,
]

# Values read from the source stay strings: the raw layer mirrors what was
# published and dbt does the casting. Only ordinal and record_sequence are
# typed here, because we generate them rather than read them.
INT_COLUMNS = {'ordinal', 'record_sequence'}

# One list per column, keyed by column name: the accumulator shape used throughout.
Columns = dict[str, list]


class Problem(NamedTuple):
    """A structural finding, and whether it should block the write.

    The split is about who is at fault. A broken grain, an orphaned child or a
    malformed identifier means THIS CODE misread the file. A record the publisher sent 
    with no name, or a classification it left empty, is ESMA's content faithfully
    represented: worth reporting, not worth refusing to load.
    """

    fatal: bool
    message: str


def fatal(message: str) -> Problem:
    return Problem(True, message)


def warn(message: str) -> Problem:
    return Problem(False, message)

UNDERLYING_BASE = '{*}DerivInstrmAttrbts/{*}UndrlygInstrm'

# Identifier formats, lifted verbatim from the FULINS XSD's simpleType restrictions
# (ISINOct2015Identifier, LEIIdentifier, MICIdentifier).
PATTERNS = {
    'ISIN': re.compile(r'^[A-Z]{2}[A-Z0-9]{9}[0-9]$'),
    'LEI': re.compile(r'^[A-Z0-9]{18}[0-9]{2}$'),
    'MIC': re.compile(r'^[A-Z0-9]{4}$'),
}


def build_schema(columns: list[str]) -> pa.Schema:
    """Declare the Parquet schema instead of letting Arrow infer it."""
    return pa.schema(
        [(c, pa.int32() if c in INT_COLUMNS else pa.string()) for c in columns]
    )


def publication_date(filename: str) -> str:
    """FULINS_F_20260718_01of01.xml -> 2026-07-18.
    DLTINS_20260723_01of02.xml     -> 2026-07-23.
    """
    for part in filename.split('_'):
        if len(part) == 8 and part.isdigit():
            return f'{part[:4]}-{part[4:6]}-{part[6:8]}'
    raise ValueError(f'no publication date in filename: {filename}')


def cfi_category(filename: str) -> str:
    """The letter in a FULINS filename is the CFI category of every record inside.

    FULINS only. The category is a property of the FILE there, because ESMA
    splits the snapshot one file per category. A delta is a single stream of
    everything that changed, so the concept does not transfer and the delta
    path reads the category off each record's own ClssfctnTp instead.
    """
    return filename.split('_')[1]


def underlyings(elem: ET.Element):
    """Yield (source, type, identifier, name) for each underlying of one record.

    Four branches, because the data holds four different shapes:
      Sngl/ISIN      one underlying security
      Sngl/LEI       one underlying entity, i.e. a credit reference
      Sngl/Indx      one underlying index or reference rate; its ISIN is
                     optional, so the name can be the only identifier
      Bskt/ISIN|LEI  a basket, where both elements repeat 0..unbounded
    """
    for node in elem.findall(f'{UNDERLYING_BASE}/{{*}}Sngl/{{*}}ISIN'):
        yield 'single', 'ISIN', node.text, None
    for node in elem.findall(f'{UNDERLYING_BASE}/{{*}}Sngl/{{*}}LEI'):
        yield 'single', 'LEI', node.text, None
    for node in elem.findall(f'{UNDERLYING_BASE}/{{*}}Sngl/{{*}}Indx'):
        yield (
            'single',
            'INDEX',
            node.findtext('{*}ISIN'),
            node.findtext('{*}Nm/{*}RefRate/{*}Nm'),
        )
    for kind in ('ISIN', 'LEI'):
        for node in elem.findall(f'{UNDERLYING_BASE}/{{*}}Bskt/{{*}}{kind}'):
            yield 'basket', kind, node.text, None


def shred(path: Path, limit: int | None = None) -> tuple[Columns, Columns]:
    """Stream one FULINS file into instrument columns and underlying columns."""
    lineage = {
        'cfi_category': cfi_category(path.name),
        'source_file': path.name,
        'publication_date': publication_date(path.name),
        'ingested_at': datetime.now(timezone.utc).isoformat(),
    }
    instruments: Columns = {name: [] for name in INSTRUMENT_COLUMNS}
    underlying: Columns = {name: [] for name in UNDERLYING_COLUMNS}
    n_records = 0

    container = None
    for event, elem in ET.iterparse(path, events=('start', 'end')):
        tag = elem.tag.rsplit('}', 1)[-1]
        if event == 'start':
            if tag == 'FinInstrmRptgRefDataRpt':
                container = elem
            continue
        if tag != 'RefData':
            continue

        for column, xpath in FIELDS.items():
            instruments[column].append(elem.findtext(xpath))
        for column, value in lineage.items():
            instruments[column].append(value)
        n_records += 1

        isin = instruments['isin'][-1]
        mic = instruments['trading_venue_mic'][-1]
        for ordinal, (source, kind, ident, name) in enumerate(
            underlyings(elem), start=1
        ):
            underlying['parent_isin'].append(isin)
            underlying['parent_mic'].append(mic)
            underlying['underlying_source'].append(source)  # single or basket
            underlying['underlying_type'].append(kind)      # ISIN, LEI or INDEX
            underlying['underlying_id'].append(ident)
            underlying['underlying_name'].append(name)      # only an index has one
            underlying['ordinal'].append(ordinal)  # XML is ordered, SQL rows are not
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


def shred_delta(path: Path, limit: int | None = None) -> tuple[Columns, Columns]:
    """Stream one DLTINS file into delta instrument columns and underlying columns.

    Same field extraction as the snapshot. Three things differ:

    1. The record wrapper is <FinInstrm>, and the change type is the NAME of
       its single child rather than a value in a column. So the dispatch reads
       a tag, and the four types are the only legal shapes.

    2. The container to clear is FinInstrmRptgRefDataDltaRpt, not
       FinInstrmRptgRefDataRpt.

    3. cfi_category is read off each record's own ClssfctnTp. ESMA splits the
       weekly snapshot one file per CFI category, so there it is a property of
       the FILE; a delta is one stream of everything that changed, so there it
       is a property of the RECORD. A cancellation has no ClssfctnTp and gets null.
    """
    lineage = {
        'source_file': path.name,
        'publication_date': publication_date(path.name),
        'ingested_at': datetime.now(timezone.utc).isoformat(),
    }
    instruments: Columns = {name: [] for name in DELTA_COLUMNS}
    underlying: Columns = {name: [] for name in DELTA_UNDERLYING_COLUMNS}
    n_records = 0
    reporting_date = None

    container = None
    for event, elem in ET.iterparse(path, events=('start', 'end')):
        tag = elem.tag.rsplit('}', 1)[-1]
        if event == 'start':
            if tag == 'FinInstrmRptgRefDataDltaRpt':
                container = elem
            continue
        # RptHdr closes before the first record, so this is set by the time it
        # is needed. Read into a plain string, because the header element is
        # about to be cleared along with everything else.
        if tag == 'RptgPrd':
            reporting_date = elem.findtext('{*}Dt')
            continue
        if tag != 'FinInstrm':
            continue

        children = list(elem)
        if len(children) != 1:
            raise ValueError(
                f'{path.name}: <FinInstrm> holds {len(children)} children, expected 1'
            )
        record = children[0]
        record_tag = record.tag.rsplit('}', 1)[-1]
        if record_tag not in RECORD_TYPES:
            raise ValueError(f'{path.name}: unknown delta record type <{record_tag}>')

        n_records += 1
        for column, xpath in FIELDS.items():
            instruments[column].append(record.findtext(xpath))
        for column, xpath in DELTA_ONLY_FIELDS.items():
            instruments[column].append(record.findtext(xpath))
        instruments['record_type'].append(RECORD_TYPES[record_tag])
        instruments['reporting_date'].append(reporting_date)
        instruments['record_sequence'].append(n_records)

        cfi = instruments['cfi_code'][-1]
        category = cfi[0] if cfi else None
        instruments['cfi_category'].append(category)
        for column, value in lineage.items():
            instruments[column].append(value)

        isin = instruments['isin'][-1]
        mic = instruments['trading_venue_mic'][-1]
        for ordinal, (source, kind, ident, name) in enumerate(
            underlyings(record), start=1
        ):
            underlying['parent_isin'].append(isin)
            underlying['parent_mic'].append(mic)
            underlying['record_sequence'].append(n_records)
            underlying['underlying_source'].append(source)
            underlying['underlying_type'].append(kind)
            underlying['underlying_id'].append(ident)
            underlying['underlying_name'].append(name)
            underlying['ordinal'].append(ordinal)
            underlying['cfi_category'].append(category)
            for column, value in lineage.items():
                underlying[column].append(value)

        elem.clear()
        if container is not None:
            container.clear()

        if limit and n_records >= limit:
            break

    return instruments, underlying


def check(instruments: Columns, underlying: Columns) -> list[Problem]:
    """Structural checks: did the shred faithfully represent the file?

    Reads whole columns and pairs them with zip() where a check spans more than
    one, which is the columnar equivalent of iterating rows.
    """
    problems = []

    isins = instruments['isin']
    mics = instruments['trading_venue_mic']

    missing_key = sum(1 for isin, mic in zip(isins, mics) if not isin or not mic)
    if missing_key:
        problems.append(fatal(f'{missing_key} instrument rows missing isin or mic'))

    keys = Counter(zip(isins, mics))
    duplicates = [key for key, n in keys.items() if n > 1]
    if duplicates:
        problems.append(
            fatal(
                f'{len(duplicates)} duplicate (isin, mic) keys, '
                f'e.g. {duplicates[:3]}'
            )
        )

    orphans = {
        key
        for key in zip(underlying['parent_isin'], underlying['parent_mic'])
        if key not in keys
    }
    if orphans:
        problems.append(fatal(f'{len(orphans)} underlying rows with no parent record'))

    # ESMA's content, not a shredding error: an index underlying's ISIN is
    # optional and the name may be absent too. Reported, not blocking.
    unidentified = sum(
        1
        for ident, name in zip(
            underlying['underlying_id'], underlying['underlying_name']
        )
        if not ident and not name
    )
    if unidentified:
        problems.append(
            warn(f'{unidentified} underlying rows with neither id nor name')
        )

    problems.extend(identifier_problems(instruments, underlying))
    return problems


def identifier_problems(instruments: Columns, underlying: Columns) -> list[Problem]:
    """Format assertions on the identifier columns. Same for both file shapes."""
    problems = []

    # A value that doesn't match its declared pattern almost always means the wrong
    # element was read, not that the publisher sent bad data.
    for column, kind in (
        ('isin', 'ISIN'),
        ('trading_venue_mic', 'MIC'),
        ('issuer_lei', 'LEI'),
    ):
        pattern = PATTERNS[kind]
        bad = [v for v in instruments[column] if v and not pattern.match(v)]
        if bad:
            problems.append(
                fatal(
                    f'{len(bad)} instrument rows where {column} is not a valid '
                    f'{kind}, e.g. {bad[:3]}'
                )
            )

    # The same test on the child table doubles as a check on the BRANCH LOGIC: an
    # underlying routed into the wrong branch of underlyings() would carry an
    # identifier of the wrong shape. INDEX rows are exempt — their identifier is an
    # optional ISIN, and the name may be the only identifier they have.
    mistyped = [
        (kind, ident)
        for kind, ident in zip(
            underlying['underlying_type'], underlying['underlying_id']
        )
        if kind in PATTERNS and ident and not PATTERNS[kind].match(ident)
    ]
    if mistyped:
        problems.append(
            fatal(
                f'{len(mistyped)} underlying rows whose id does not match its '
                f'declared type, e.g. {mistyped[:3]}'
            )
        )

    return problems


def check_delta(
    instruments: Columns, underlying: Columns, expected_date: str
) -> list[Problem]:
    """Structural checks for a delta file.

    Deliberately does NOT assert (isin, mic) uniqueness the way the snapshot
    check does. The schema permits a key to recur within one publication, and
    across publication days it certainly does. Measured on the 2026-07-18..31
    corpus the triple (isin, mic, publication_date) is in fact unique across
    all 10,634,204 rows.
    """
    problems = []
    types = instruments['record_type']
    known = set(RECORD_TYPES.values())

    unknown = sorted({t for t in types if t not in known})
    if unknown:
        problems.append(fatal(f'unknown record_type value(s): {unknown}'))

    # Keys are mandatory on every record type, cancellations included: the keys
    # are the entire content of a cancellation.
    missing = sum(
        1
        for isin, mic in zip(instruments['isin'], instruments['trading_venue_mic'])
        if not isin or not mic
    )
    if missing:
        problems.append(fatal(f'{missing} delta rows missing isin or mic'))

    # The file's statement about its own reporting period, against its name.
    # Two sources for one fact, so they get compared rather than silently
    # preferred.
    reported = sorted({d for d in instruments['reporting_date'] if d})
    if len(reported) > 1:
        problems.append(
            fatal(f'more than one reporting period in one file: {reported}')
        )
    elif reported and reported[0] != expected_date:
        problems.append(
            fatal(f'filename says {expected_date}, RptHdr/RptgPrd says {reported[0]}')
        )
    elif not reported:
        problems.append(fatal('no RptHdr/RptgPrd/Dt found'))

    # A cancellation carries the two keys and nothing else. If that ever stops
    # being true the schema moved, and the erasure branch in the SCD2 -- which
    # deletes history rather than closing it -- needs re-reading before it runs
    # again.
    payload = [
        column
        for column in list(FIELDS) + list(DELTA_ONLY_FIELDS)
        if column not in CANC_KEY_FIELDS
    ]
    fat = sum(
        1
        for row, kind in enumerate(types)
        if kind == 'CANC'
        and any(instruments[column][row] is not None for column in payload)
    )
    if fat:
        problems.append(fatal(f'{fat} CANC rows carry payload beyond the key fields'))

    # The other three are full records. Publisher content, so reported rather
    # than blocking -- but cfi_code missing also means cfi_category lands null,
    # which the source yml's accepted_values will see.
    for column in ('cfi_code', 'notional_ccy'):
        blank = sum(
            1
            for kind, value in zip(types, instruments[column])
            if kind != 'CANC' and not value
        )
        if blank:
            problems.append(warn(f'{blank} non-CANC rows with no {column}'))

    undated = sum(
        1
        for kind, value in zip(types, instruments['termination_dt'])
        if kind == 'TERMNTD' and not value
    )
    if undated:
        problems.append(warn(f'{undated} TERMNTD rows with no termination_dt'))

    # The child's parent link is the triple, not the pair: the same (isin, mic)
    # can appear twice in one file, and an underlying belongs to one of those
    # records specifically.
    parents = set(
        zip(
            instruments['isin'],
            instruments['trading_venue_mic'],
            instruments['record_sequence'],
        )
    )
    orphans = {
        key
        for key in zip(
            underlying['parent_isin'],
            underlying['parent_mic'],
            underlying['record_sequence'],
        )
        if key not in parents
    }
    if orphans:
        problems.append(fatal(f'{len(orphans)} underlying rows with no parent record'))

    problems.extend(identifier_problems(instruments, underlying))
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
    pq.write_table(table, destination, compression='snappy')


# Everything that differs between the two formats.
FORMATS = {
    'fulins': {
        'source': DEFAULT_SRC,
        'glob': 'FULINS_*.xml',
        'instrument_table': 'firds_instrument',
        'underlying_table': 'firds_underlying',
        'instrument_columns': INSTRUMENT_COLUMNS,
        'underlying_columns': UNDERLYING_COLUMNS,
    },
    'dltins': {
        'source': DELTA_SRC,
        'glob': 'DLTINS_*.xml',
        'instrument_table': 'firds_instrument_delta',
        'underlying_table': 'firds_underlying_delta',
        'instrument_columns': DELTA_COLUMNS,
        'underlying_columns': DELTA_UNDERLYING_COLUMNS,
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        'files',
        nargs='*',
        type=Path,
        help='XML files (default: every file of --kind in data/raw).',
    )
    parser.add_argument(
        '--kind',
        choices=('fulins', 'dltins'),
        default='fulins',
        help='Which ESMA file format to shred (default: fulins).',
    )
    parser.add_argument(
        '--out',
        type=Path,
        default=DEFAULT_OUT,
        help='Output directory for the Parquet tables.',
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Stop after this many records per file (development aid).',
    )
    args = parser.parse_args()
    if args.limit is not None and args.out == DEFAULT_OUT:
        parser.error(
            '--limit needs an explicit --out: it would overwrite the full dataset'
        )
    # Named files and --kind can disagree, and the failure is silent: a DLTINS
    # file put through the snapshot parser finds no <RefData> and writes an
    # empty table, which reads as "that publication had no changes".
    prefix = 'FULINS_' if args.kind == 'fulins' else 'DLTINS_'
    wrong = [f.name for f in args.files if not f.name.startswith(prefix)]
    if wrong:
        parser.error(f'--kind {args.kind} but these are not {prefix}files: {wrong}')
    return args


def main() -> int:
    args = parse_args()
    spec = FORMATS[args.kind]
    files = args.files or sorted(spec['source'].glob(spec['glob']))
    if not files:
        print(f"No {args.kind.upper()} files found in {spec['source']}")
        return 1

    failed = False
    total_rows = 0
    total_underlyings = 0

    for path in files:
        if args.kind == 'dltins':
            instruments, underlying = shred_delta(path, args.limit)
            problems = check_delta(
                instruments, underlying, publication_date(path.name)
            )
        else:
            instruments, underlying = shred(path, args.limit)
            problems = check(instruments, underlying)
        n_instruments = len(instruments['isin'])
        n_underlying = len(underlying['parent_isin'])
        blocking = [p for p in problems if p.fatal]

        # Gate the write on structural failure, matching the GLEIF and MIC
        # normalizers: a file whose shred we do not trust writes nothing at all,
        # rather than landing and being reported afterwards.
        if not blocking:
            write(
                instruments,
                spec['instrument_columns'],
                args.out / spec['instrument_table'] / f'{path.stem}.parquet',
            )
            write(
                underlying,
                spec['underlying_columns'],
                args.out / spec['underlying_table'] / f'{path.stem}.parquet',
            )
            total_rows += n_instruments
            total_underlyings += n_underlying

        summary = f'{n_instruments} instruments'
        if args.kind == 'dltins':
            counts = Counter(instruments['record_type'])
            summary += ' (' + ' '.join(
                f'{RECORD_TYPES[tag]} {counts[RECORD_TYPES[tag]]}'
                for tag in RECORD_TYPES
            ) + ')'
        if blocking:
            status = 'FAIL, not written'
        elif problems:
            status = 'ok, with warnings'
        else:
            status = 'ok'
        print(f'{path.name}: {summary}, {n_underlying} underlyings [{status}]')
        for problem in problems:
            print(f"    {'FATAL' if problem.fatal else 'warn '} {problem.message}")
        if blocking:
            failed = True

    print(
        f'\n{len(files)} file(s): {total_rows} instruments, '
        f'{total_underlyings} underlyings -> {args.out}'
    )
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())
