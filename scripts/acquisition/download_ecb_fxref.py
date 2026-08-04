"""Download the ECB euro foreign exchange reference rates (full history).

The published archive holds one CSV: one row per TARGET working day, one
column per currency, rates expressed per 1 EUR. It is landed under the date of
its newest quotation, which is also its publication date -- see
publication_date() -- and the normalization step parses that back out.
"""

from __future__ import annotations

import io
import sys
import zipfile
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import requests

FILE_URL = 'https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip'
REPO_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_ROOT = REPO_ROOT / 'data' / 'raw' / 'ecb_fxref'
HEADERS = {'User-Agent': 'finance-data-platform/ecb-fxref-downloader'}
TIMEOUT = (10, 60)
MIN_BYTES = 600_000  # the archive is ~640 KB and only grows
MEMBER_NAME = 'eurofxref-hist.csv'
DATE_COLUMN = 'Date'


def get_ecbfxref_csv() -> bytes:
    """Fetch the archive and return the CSV it contains.

    This endpoint sends no Content-Length, so a transfer-completeness check
    against the protocol is not available. The container supplies a stronger
    one: every ZIP member carries a CRC-32 of its uncompressed bytes, which
    read() verifies while decompressing. That catches a corrupted payload of
    the correct length, which a Content-Length comparison would accept.
    """
    response = requests.get(FILE_URL, headers=HEADERS, timeout=TIMEOUT)
    response.raise_for_status()
    payload = response.content

    if len(payload) < MIN_BYTES:
        raise ValueError(f'suspiciously small ECB FX archive: {len(payload)} bytes')

    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        names = archive.namelist()
        if names != [MEMBER_NAME]:
            raise ValueError(f'unexpected archive contents: {names}')
        return archive.read(MEMBER_NAME)


def publication_date(csv_content: bytes) -> str:
    """Newest quotation date in the file, which is also its publication date.

    ECB runs the concertation procedure around 14:10 CET and publishes around
    16:00 CET the same working day, so the latest row's date is the date that
    row was published. Dates stay strings because ISO-8601 sorts lexicographically, 
    so max() needs no date parsing and no assumption about the file's row order.
    """
    table = pacsv.read_csv(
        io.BytesIO(csv_content),
        convert_options=pacsv.ConvertOptions(
            include_columns=[DATE_COLUMN],
            column_types={DATE_COLUMN: pa.string()},
        ),
    )
    if table.num_rows == 0:
        raise ValueError('ECB CSV contains no rows')
    return pc.max(table[DATE_COLUMN]).as_py()


def main() -> int:
    csv_content = get_ecbfxref_csv()
    published = publication_date(csv_content)

    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_ROOT / f'eurofxref-hist-{published}.csv'
    partial_path = output_path.with_suffix(output_path.suffix + '.part')
    partial_path.write_bytes(csv_content)
    partial_path.replace(output_path)

    print(f'ECB FX reference rates successfully downloaded to: {output_path}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
