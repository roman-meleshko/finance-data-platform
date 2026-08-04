"""Normalize the ECB euro foreign exchange reference rates (full history) to Parquet.

ECB files contains one column per currency and one row per date. Unpivot it to long
format, dropping null values (currently in 'N/A' string format). Those null values 
represent EUR/currency pair for which the FX data is unavailable on that day. 
Also, dropping the empty column(s) at the end of the file.

Tests:
 - Date column values are non-null, unique and in an expected format.
 - Column names correspond to expected format (e.g. USD).
 - Range between min and max date.

Run from the repo root: python -m ingestion.ecb_fxref.normalize
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
DEFAULT_SRC = REPO_ROOT / 'data' / 'raw' / 'ecb_fxref'
DEFAULT_OUT = REPO_ROOT / 'data' / 'parquet' / 'ecb_fxref' / 'ecb_fxref.parquet'

FILENAME_REGEX = re.compile(r'^eurofxref-hist-(\d{4}-\d{2}-\d{2})\.csv$')
CURRENCY_COLUMNS_REGEX = r'^[A-Z]{3}$'
DATE_REGEX = r'^\d{4}-\d{2}-\d{2}$'
DATE_FIELD = 'Date'

OUT_COLUMNS = ['date', 'currency', 'fx_rate']
LINEAGE = ('source_file', 'publication_date', 'ingested_at')


def get_latest_file_path() -> Path:
    matches = sorted(DEFAULT_SRC.glob('eurofxref-hist-*.csv'))
    if not matches:
        raise FileNotFoundError(f'no ECB FXREF file under {DEFAULT_SRC}')
    return matches[-1]


def publication_date(filename: str) -> str:
    """eurofxref-hist-2026-07-13.csv -> 2026-07-13."""
    match = FILENAME_REGEX.match(filename)
    if not match:
        raise ValueError(f'cannot parse publication date from {filename!r}')
    return match.group(1)


def read_table(path: Path) -> pa.Table:
    """Read the CSV, projecting to FIELDS with every column as a string."""
    # Read first to discover columns, then cast all columns to string
    try:
        table = pacsv.read_csv(path)
    except pa.ArrowError as exc:
        raise ValueError(f'failed to read CSV {path.name} -- {exc}') from exc

    # Build a schema with every column as string and cast the table
    schema = pa.schema([(name, pa.string()) for name in table.column_names])
    return table.cast(schema)


def drop_empty_columns(table: pa.Table) -> pa.Table:
    col_names = table.column_names
    if any(name == '' for name in col_names):
        keep = [name for name in col_names if name != '']
        table = table.select(keep)
    return table


def check_fxref(table: pa.Table) -> list[str]:
    """Structural checks on the wide table, before it is unpivoted."""
    problems = []
    n_rows = table.num_rows

    missing = table.filter(blank_or_null(table[DATE_FIELD]))
    if missing.num_rows:
        problems.append(f'{missing.num_rows} rows with no Date')

    # mode="all" counts null as a value; the default ignores nulls and would
    # report a missing Date a second time as a phantom duplicate.
    distinct = pc.count_distinct(table[DATE_FIELD], mode='all').as_py()
    if distinct != n_rows:
        duplicates = duplicate_keys(table, [DATE_FIELD])
        examples = duplicates[DATE_FIELD].slice(0, 3).to_pylist()
        problems.append(
            f'{n_rows - distinct} duplicate Date values, e.g. {examples}'
        )

    bad = table.filter(bad_format(table[DATE_FIELD], DATE_REGEX))
    if bad.num_rows:
        problems.append(
            f'{bad.num_rows} rows where Date is not a valid ISO 8601 date, '
            f'e.g. {bad[DATE_FIELD].slice(0, 3).to_pylist()}'
        )

    if n_rows < 7000:
        problems.append(
            f'number of rows in the file ({n_rows}) is lower than expected (7000)'
        )

    columns = table.column_names
    invalid_cols = [
        col for col in columns
        if col != DATE_FIELD and not re.match(CURRENCY_COLUMNS_REGEX, col)
    ]
    if invalid_cols:
        problems.append(f'invalid column names: {invalid_cols}')

    return problems


def unpivot(table: pa.Table) -> pa.Table:
    """Wide (one column per currency) to long (one row per date-currency pair).

    The loop runs over the ~41 currency COLUMNS: each currency yields one full-height 
    three-column table, and those are stacked. Rows without a quotation are dropped 
    here rather than kept as nulls.
    """
    n_rows = table.num_rows
    date_column = table[DATE_FIELD]
    slices = []

    for currency in table.column_names:
        if currency == DATE_FIELD:
            continue
        rates = table[currency]
        quoted = pc.invert(blank_or_null(rates))
        slices.append(
            pa.table(
                {
                    'date': date_column,
                    'currency': pa.array([currency] * n_rows, pa.string()),
                    'fx_rate': rates,
                }
            ).filter(quoted)
        )

    if not slices:
        raise ValueError('no currency columns found to unpivot')

    long_table = pa.concat_tables(slices)
    return long_table.sort_by([('date', 'ascending'), ('currency', 'ascending')])


def check_unpivot(wide: pa.Table, long: pa.Table) -> list[str]:
    """Reconcile the reshape: every quoted cell must survive as exactly one row.
    Count the quoted cells independently and compare.
    """
    problems = []

    quoted_cells = sum(
        pc.sum(pc.cast(pc.invert(blank_or_null(wide[name])), pa.int64())).as_py() or 0
        for name in wide.column_names
        if name != DATE_FIELD
    )
    if long.num_rows != quoted_cells:
        problems.append(
            f'unpivot lost rows: {quoted_cells} quoted cells in, '
            f'{long.num_rows} rows out'
        )

    currencies_in = sum(1 for name in wide.column_names if name != DATE_FIELD)
    currencies_out = pc.count_distinct(long['currency'], mode='all').as_py()
    if currencies_out > currencies_in:
        problems.append(
            f'unpivot invented currencies: {currencies_in} columns in, '
            f'{currencies_out} distinct out'
        )

    return problems


def write(table: pa.Table, destination: Path = DEFAULT_OUT) -> None:
    """Write the table to Parquet under an explicitly declared schema."""
    destination.parent.mkdir(parents=True, exist_ok=True)

    order = OUT_COLUMNS + list(LINEAGE)
    schema = pa.schema([pa.field(name, pa.string()) for name in order])

    table = table.select(order).cast(schema)

    pq.write_table(table, destination, compression='snappy')


def main() -> int:
    file_path = get_latest_file_path()
    table = read_table(file_path)
    table = drop_empty_columns(table)

    problems = check_fxref(table)
    long_table = unpivot(table) if not problems else None
    if long_table is not None:
        problems += check_unpivot(table, long_table)

    if problems:
        print(f'{file_path.name}: {table.num_rows} quotation days [FAIL]')
        for problem in problems:
            print(f'    {problem}')
        return 1

    long_table = add_lineage(
        long_table, file_path, publication_date(file_path.name)
    )
    write(long_table)
    print(f'{long_table.num_rows} rows saved to {DEFAULT_OUT} [ok]')

    return 0


if __name__ == '__main__':
    sys.exit(main())