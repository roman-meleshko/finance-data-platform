"""Load the local parquet tables to a GCS bucket, then into BigQuery.

Each table is staged under gs://<bucket>/parquet/<table>/ and loaded with one
atomic WRITE_TRUNCATE job, so a rerun replaces tables instead of appending and
a failed load leaves the previous table intact. After loading, local parquet
footers are reconciled against BigQuery table metadata: row counts must match
and every column must arrive with the name and type the parquet declares.

Run with the service account key:
GOOGLE_APPLICATION_CREDENTIALS=... python scripts/load_bigquery.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import pyarrow.parquet as pq
from google.cloud import bigquery, storage

PROJECT_ID = 'finance-data-platform-503113'
BUCKET_NAME = 'finance-data-platform-raw-503113'
DATASET_ID = 'raw'
LOCATION = 'EU'
REPO_ROOT = Path(__file__).resolve().parents[1]
PARQUET_DIR = REPO_ROOT / 'data' / 'parquet'
TABLES = {
    'calendar': 'calendar/calendar.parquet',
    'ecb_fxref': 'ecb_fxref/ecb_fxref.parquet',
    'firds_instrument': 'firds_instrument/FULINS*.parquet',
    'firds_underlying': 'firds_underlying/FULINS*.parquet',
    'gleif_entity': 'gleif/gleif_entity.parquet',
    'gleif_relationship': 'gleif/gleif_relationship.parquet',
    'iso_mic': 'iso_mic/iso_mic.parquet',
}


def table_files(table_name: str, table_path: str) -> list[Path]:
    """Resolve one table's local parquet files, failing loudly on none."""
    files = sorted(PARQUET_DIR.glob(table_path))
    if not files:
        raise FileNotFoundError(
            f'no {table_name} file under {PARQUET_DIR / table_path}'
        )
    return files


def ensure_dataset(client: bigquery.Client) -> None:
    """Create the raw dataset in BigQuery if it is not there yet."""
    dataset = bigquery.Dataset(f'{PROJECT_ID}.{DATASET_ID}')
    dataset.location = LOCATION
    client.create_dataset(dataset, exists_ok=True)


def load_table_to_bucket(
    table_name: str, files: list[Path], bucket: storage.Bucket
) -> int:
    """Upload one table's parquet files to the bucket; return bytes sent."""
    total_bytes = 0
    for file in files:
        blob = bucket.blob(f'parquet/{table_name}/{file.name}')
        blob.upload_from_filename(str(file))
        total_bytes += file.stat().st_size
    return total_bytes


def load_table_to_bigquery(table_name: str, bq: bigquery.Client) -> int:
    """Load one staged table into BigQuery; return the rows written."""
    cfg = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    uri = f'gs://{BUCKET_NAME}/parquet/{table_name}/*.parquet'
    job = bq.load_table_from_uri(
        uri, f'{PROJECT_ID}.{DATASET_ID}.{table_name}', job_config=cfg
    )
    job.result()
    return job.output_rows


# How arrow types in the local parquet must arrive in BigQuery. The loader
# asserts FIDELITY (warehouse == files), not the all-string policy: that
# policy belongs to the normalizers that write the files, and the shredder
# deliberately declares the derived ordinal as an integer.
ARROW_TO_BQ = {
    'string': 'STRING',
    'int32': 'INTEGER',
    'int64': 'INTEGER',
    'bool': 'BOOLEAN',
}


def verify(table_name: str, files: list[Path], bq: bigquery.Client) -> list[str]:
    """Reconcile local parquet footers against BigQuery table metadata.

    Both sides are metadata reads: parquet footers locally, get_table() in
    BigQuery. Row counts must match and every column must arrive with the
    name and type the parquet declares.
    """
    problems = []

    local_rows = sum(pq.ParquetFile(file).metadata.num_rows for file in files)
    table = bq.get_table(f'{PROJECT_ID}.{DATASET_ID}.{table_name}')
    if local_rows != table.num_rows:
        problems.append(
            f'{table_name}: {local_rows:,} rows in local parquet, '
            f'{table.num_rows:,} in BigQuery'
        )

    expected = [
        (field.name, ARROW_TO_BQ.get(str(field.type), f'?{field.type}'))
        for field in pq.ParquetFile(files[0]).schema_arrow
    ]
    loaded = [(field.name, field.field_type) for field in table.schema]
    if expected != loaded:
        diffs = [
            f'{e} -> {g}' for e, g in zip(expected, loaded) if e != g
        ] or [f'{len(expected)} local vs {len(loaded)} loaded columns']
        problems.append(f'{table_name}: schema mismatch: {"; ".join(diffs)}')

    return problems


def main() -> int:
    bq = bigquery.Client(project=PROJECT_ID)
    gcs = storage.Client(project=PROJECT_ID)
    ensure_dataset(bq)
    bucket = gcs.bucket(BUCKET_NAME)

    problems = []
    for table_name, table_path in TABLES.items():
        files = table_files(table_name, table_path)
        sent = load_table_to_bucket(table_name, files, bucket)
        rows = load_table_to_bigquery(table_name, bq)
        table_problems = verify(table_name, files, bq)
        problems += table_problems

        status = 'FAIL' if table_problems else 'ok'
        print(
            f'{table_name}: {len(files)} file(s), {sent / 2**20:.1f} MiB '
            f'-> {rows:,} rows [{status}]'
        )

    for problem in problems:
        print(f'    {problem}')
    return 1 if problems else 0


if __name__ == '__main__':
    sys.exit(main())
