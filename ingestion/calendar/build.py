"""Build the trading calendar: one row per (calendar_code, date), full daily spine.

Unlike every other table, nothing is downloaded: the pinned libraries ARE the
source. exchange_calendars supplies XFRA sessions, holidays supplies the ECB
TARGET closing days, and a continuous date spine turns both into a gapless
table with an is_trading_day flag, so downstream can distinguish "market was
closed" from "pipeline lost a day". Reproducibility contract: same library
versions + same spans -> same parquet. Lineage names the versions, since there
is no source file to name.

Per-code spans are honest rather than uniform: TARGET runs from its first
business day in 1999; XFRA covers whatever the library covers (2006+).

Checks gate the write: grain uniqueness, every weekend closed in every
calendar, a row floor, and a reconciliation of the TARGET rules against the
OBSERVED ecb_fxref dates -- every date the ECB actually quoted must be a
rule-open day. The reverse direction (rule-open past days with no quotes) is
reported but does not fail: absence of a quote has more innocent explanations
than presence of one on a day the rules call closed.

Run from the repo root: python -m ingestion.calendar.build
"""

from __future__ import annotations

import sys
from datetime import date, datetime, timezone
from pathlib import Path

import exchange_calendars as xcals
import holidays
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from ingestion.common import duplicate_keys

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUT = REPO_ROOT / 'data' / 'parquet' / 'calendar' / 'calendar.parquet'
ECB_FXREF = REPO_ROOT / 'data' / 'parquet' / 'ecb_fxref' / 'ecb_fxref.parquet'

TARGET_START = date(1999, 1, 1)   # TARGET opened 1999-01-04; spine starts at the year
HORIZON = date(2030, 12, 31)
ROW_FLOOR = 10_000

GENERATOR_IDENTITY = (
    f'exchange_calendars=={xcals.__version__};holidays=={holidays.__version__}'
)


def target_days(start: date, end: date) -> dict[str, bool]:
    """TARGET working days from the maintained ECB rules: weekday and not a
    closing day. The years= argument is required -- the holidays object
    populates lazily per year, and membership tests against an unpopulated
    year would silently return False."""
    closing = holidays.financial_holidays(
        'ECB', years=range(start.year, end.year + 1)
    )
    spine = pd.date_range(start, end, freq='D')
    return {
        d.date().isoformat(): (d.weekday() < 5 and d.date() not in closing)
        for d in spine
    }


def xfra_days() -> dict[str, bool]:
    """XFRA trading days over the library's own honest span."""
    calendar = xcals.get_calendar('XFRA')
    start = calendar.first_session.date()
    end = calendar.last_session.date()
    sessions = {s.date().isoformat() for s in calendar.sessions_in_range(
        calendar.first_session, calendar.last_session
    )}
    spine = pd.date_range(start, end, freq='D')
    return {d.date().isoformat(): d.date().isoformat() in sessions for d in spine}


def build_table() -> pa.Table:
    codes = {
        'TARGET': target_days(TARGET_START, HORIZON),
        'XFRA': xfra_days(),
    }
    calendar_code: list[str] = []
    calendar_date: list[str] = []
    is_trading_day: list[bool] = []
    for code, days in codes.items():
        for day, is_open in days.items():
            calendar_code.append(code)
            calendar_date.append(day)
            is_trading_day.append(is_open)

    built_on = datetime.now(timezone.utc)
    n = len(calendar_code)
    return pa.table(
        {
            'calendar_code': pa.array(calendar_code, pa.string()),
            'calendar_date': pa.array(calendar_date, pa.string()),
            # generated, not read from a source, so it gets an honest type --
            # the same reasoning that made the shredder's ordinal an integer
            'is_trading_day': pa.array(is_trading_day, pa.bool_()),
            'source_file': pa.array([GENERATOR_IDENTITY] * n, pa.string()),
            'publication_date': pa.array(
                [built_on.date().isoformat()] * n, pa.string()
            ),
            'ingested_at': pa.array([built_on.isoformat()] * n, pa.string()),
        }
    )


def check_calendar(table: pa.Table) -> list[str]:
    """Structural checks plus the rules-vs-observed reconciliation."""
    problems = []

    distinct = pc.count_distinct(
        pc.binary_join_element_wise(
            table['calendar_code'], table['calendar_date'], '|'
        ),
        mode='all',
    ).as_py()
    if distinct != table.num_rows:
        examples = duplicate_keys(table, ['calendar_code', 'calendar_date'])
        problems.append(
            f'{table.num_rows - distinct} duplicate (calendar_code, calendar_date) '
            f'rows, e.g. {examples["calendar_code"].slice(0, 3).to_pylist()}'
        )

    weekday = pc.day_of_week(pc.cast(table['calendar_date'], pa.date32()))
    open_weekends = table.filter(
        pc.and_(pc.greater_equal(weekday, 5), table['is_trading_day'])
    )
    if open_weekends.num_rows:
        problems.append(
            f'{open_weekends.num_rows} weekend rows marked as trading days, '
            f'e.g. {open_weekends["calendar_date"].slice(0, 3).to_pylist()}'
        )

    if table.num_rows < ROW_FLOOR:
        problems.append(
            f'number of rows ({table.num_rows}) is lower than expected '
            f'({ROW_FLOOR})'
        )

    problems += reconcile_target(table)
    return problems


def reconcile_target(table: pa.Table) -> list[str]:
    """Observed ECB quote dates must all be rule-open TARGET days.

    This is the check that catches a wrong library, a wrong span, or a broken
    builder: reality outvotes the rules. Skipped with a warning when the FX
    parquet is absent (fresh clone) rather than failing a build that cannot
    know better.
    """
    if not ECB_FXREF.exists():
        print(f'    warning: {ECB_FXREF.name} absent, reconciliation skipped')
        return []

    observed = set(
        pc.unique(pq.read_table(ECB_FXREF, columns=['date'])['date']).to_pylist()
    )
    target = table.filter(pc.equal(table['calendar_code'], 'TARGET'))
    rule_open = {
        d for d, is_open in zip(
            target['calendar_date'].to_pylist(),
            target['is_trading_day'].to_pylist(),
        )
        if is_open
    }

    quoted_but_closed = sorted(observed - rule_open)
    if quoted_but_closed:
        return [
            (
                f'{len(quoted_but_closed)} dates have ECB quotes but the rules '
                f'say TARGET was closed, e.g. {quoted_but_closed[:5]}'
            )
        ]

    today = datetime.now(timezone.utc).date().isoformat()
    open_but_unquoted = sorted(
        d for d in rule_open - observed if TARGET_START.isoformat() <= d <= today
    )
    print(
        f'    reconciliation: {len(observed)} observed quote dates all rule-open; '
        f'{len(open_but_unquoted)} past rule-open days without quotes'
        + (f', e.g. {open_but_unquoted[:3]}' if open_but_unquoted else '')
    )
    return []


def write(table: pa.Table, destination: Path = DEFAULT_OUT) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    table = table.sort_by(
        [('calendar_code', 'ascending'), ('calendar_date', 'ascending')]
    )
    pq.write_table(table, destination, compression='snappy')


def main() -> int:
    table = build_table()

    problems = check_calendar(table)
    if problems:
        print(f'calendar: {table.num_rows} rows [FAIL]')
        for problem in problems:
            print(f'    {problem}')
        return 1

    write(table)
    per_code = table.group_by(['calendar_code']).aggregate([('calendar_code', 'count')])
    spans = ', '.join(
        f'{code}={n}'
        for code, n in zip(
            per_code['calendar_code'].to_pylist(),
            per_code['calendar_code_count'].to_pylist(),
        )
    )
    print(f'{table.num_rows} rows saved to {DEFAULT_OUT} [ok] ({spans})')
    return 0


if __name__ == '__main__':
    sys.exit(main())
