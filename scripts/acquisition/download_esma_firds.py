"""Download and extract the latest ESMA FIRDS FULINS and DLTINS file sets."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import zipfile
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import unquote, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API_URL = 'https://registers.esma.europa.eu/solr/esma_registers_firds_files/select'
REPO_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_ROOT = REPO_ROOT / 'data' / 'raw' / 'esma_firds'
CHUNK_SIZE = 1024 * 1024
PAGE_SIZE = 100
TIMEOUT = (10, 120)


def build_session() -> requests.Session:
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=('GET', 'HEAD'),
    )
    session = requests.Session()
    session.headers['User-Agent'] = 'finance-data-platform/esma-firds-downloader'
    session.mount('https://', HTTPAdapter(max_retries=retry))
    return session


def fetch_documents(session: requests.Session, days: int) -> tuple[list[dict], dict]:
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    date_filter = (
        f'publication_date:[{start:%Y-%m-%dT%H:%M:%SZ} '
        f'TO {end:%Y-%m-%dT%H:%M:%SZ}]'
    )
    documents: list[dict] = []
    offset = 0
    num_found = None

    while num_found is None or offset < num_found:
        response = session.get(
            API_URL,
            params={
                'q': '*',
                'fq': date_filter,
                'wt': 'json',
                'start': offset,
                'rows': PAGE_SIZE,
            },
            timeout=TIMEOUT,
        )
        response.raise_for_status()
        result = response.json()['response']
        num_found = int(result['numFound'])
        page = result['docs']
        documents.extend(page)
        if not page:
            break
        offset += len(page)

    metadata = {
        'queried_at': end.isoformat(),
        'date_filter': date_filter,
        'num_found': num_found,
        'documents': documents,
    }
    return documents, metadata


def latest_file_set(documents: list[dict], file_type: str) -> list[dict]:
    candidates = [doc for doc in documents if doc.get('file_type') == file_type]
    if not candidates:
        return []
    latest_publication = max(doc.get('publication_date', '') for doc in candidates)
    return sorted(
        (
            doc
            for doc in candidates
            if doc.get('publication_date') == latest_publication
        ),
        key=lambda doc: doc.get('file_name', ''),
    )


def format_bytes(size: int | None) -> str:
    if size is None:
        return 'unknown'
    value = float(size)
    for unit in ('B', 'KiB', 'MiB', 'GiB'):
        if value < 1024 or unit == 'GiB':
            return f'{value:.1f} {unit}'
        value /= 1024
    raise AssertionError('unreachable')


def expected_extracted_path(archive_path: Path) -> Path:
    name = archive_path.name
    return archive_path.with_name(name.removesuffix('.zip'))


def output_paths(document: dict, output_dir: Path) -> tuple[Path, Path]:
    url = document['download_link']
    filename = document.get('file_name') or Path(unquote(urlparse(url).path)).name
    archive_path = output_dir / filename
    return archive_path, expected_extracted_path(archive_path)


def remote_size(session: requests.Session, document: dict) -> int | None:
    try:
        response = session.head(
            document['download_link'],
            allow_redirects=True,
            timeout=TIMEOUT,
        )
        response.raise_for_status()
    except requests.RequestException:
        return None
    content_length = response.headers.get('Content-Length')
    return int(content_length) if content_length and content_length.isdigit() else None


def is_safe_member(output_dir: Path, member: str) -> bool:
    destination = (output_dir / member).resolve()
    resolved_output = output_dir.resolve()
    return destination == resolved_output or resolved_output in destination.parents


def download_and_extract(
    session: requests.Session,
    document: dict,
    output_dir: Path,
    keep_zip: bool,
    expected_size: int | None,
    report_bytes: Callable[[int], None],
) -> None:
    archive_path, extracted_path = output_paths(document, output_dir)

    output_dir.mkdir(parents=True, exist_ok=True)
    partial_path = archive_path.with_suffix(archive_path.suffix + '.part')
    digest = hashlib.md5(usedforsecurity=False)
    downloaded = 0
    with session.get(
        document['download_link'], stream=True, timeout=TIMEOUT
    ) as response:
        response.raise_for_status()
        response_size = int(response.headers.get('Content-Length', 0)) or None
        expected_size = expected_size or response_size
        with partial_path.open('wb') as output:
            for chunk in response.iter_content(CHUNK_SIZE):
                if not chunk:
                    continue
                output.write(chunk)
                digest.update(chunk)
                downloaded += len(chunk)
                report_bytes(len(chunk))

    if expected_size is not None and downloaded != expected_size:
        partial_path.unlink()
        raise ValueError(
            f'Size mismatch for {archive_path.name}: expected {expected_size} bytes, '
            f'got {downloaded}'
        )

    expected_checksum = document.get('checksum')
    if expected_checksum and digest.hexdigest().lower() != expected_checksum.lower():
        partial_path.unlink()
        raise ValueError(
            f'Checksum mismatch for {archive_path.name}: expected {expected_checksum}, '
            f'got {digest.hexdigest()}'
        )

    partial_path.replace(archive_path)
    with zipfile.ZipFile(archive_path) as archive:
        unsafe = [
            name
            for name in archive.namelist()
            if not is_safe_member(output_dir, name)
        ]
        if unsafe:
            raise ValueError(f'Unsafe ZIP member(s): {unsafe}')
        archive.extractall(output_dir)

    if not extracted_path.is_file() or extracted_path.stat().st_size == 0:
        raise ValueError(
            f'Expected extracted file is missing or empty: {extracted_path}'
        )

    if not keep_zip:
        archive_path.unlink()


def show_progress(
    downloaded: int,
    total_size: int,
    file_number: int,
    file_count: int,
) -> None:
    ratio = min(downloaded / total_size, 1.0) if total_size else 1.0
    width = 28
    filled = int(width * ratio)
    bar = '#' * filled + '-' * (width - filled)
    print(
        f'\rDownloading [{bar}] {ratio:6.1%}  '
        f'{format_bytes(downloaded)}/{format_bytes(total_size)}  '
        f'file {file_number}/{file_count}\x1b[K',
        end='',
        flush=True,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--kind',
        choices=('all', 'fulins', 'dltins'),
        default='all',
        help='File set to download (default: all).',
    )
    parser.add_argument(
        '--days',
        type=int,
        default=14,
        help='Lookback window; must include a weekly FULINS set (default: 14).',
    )
    parser.add_argument(
        '--yes', action='store_true', help='Download without prompting.'
    )
    parser.add_argument(
        '--list-only',
        action='store_true',
        help='Show selected files without downloading.',
    )
    parser.add_argument(
        '--keep-zip',
        action='store_true',
        help='Keep ZIP archives after extraction.',
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.days < 1:
        raise ValueError('--days must be at least 1')

    session = build_session()
    documents, metadata = fetch_documents(session, args.days)
    selected: list[tuple[str, dict]] = []

    if args.kind in {'all', 'fulins'}:
        selected.extend(('fulins', doc) for doc in latest_file_set(documents, 'FULINS'))
    if args.kind in {'all', 'dltins'}:
        selected.extend(('dltins', doc) for doc in latest_file_set(documents, 'DLTINS'))

    if not selected:
        print('No matching FIRDS files found. Increase --days and try again.')
        return 1

    items = []
    for kind, document in selected:
        output_dir = OUTPUT_ROOT / kind
        _, extracted_path = output_paths(document, output_dir)
        items.append(
            {
                'kind': kind,
                'document': document,
                'output_dir': output_dir,
                'exists': (
                    extracted_path.is_file() and extracted_path.stat().st_size > 0
                ),
                'size': None,
            }
        )

    pending = [item for item in items if not item['exists']]
    for item in pending:
        item['size'] = remote_size(session, item['document'])

    fulins_count = sum(item['kind'] == 'fulins' for item in items)
    dltins_count = sum(item['kind'] == 'dltins' for item in items)
    unknown_sizes = sum(item['size'] is None for item in pending)
    total_size = sum(item['size'] or 0 for item in pending)
    size_summary = format_bytes(total_size)
    if unknown_sizes:
        size_summary += f' plus {unknown_sizes} file(s) of unknown size'

    print(
        f'Latest ESMA FIRDS selection: {len(items)} files '
        f'({fulins_count} FULINS, {dltins_count} DLTINS).'
    )
    print(
        f'Pending download: {len(pending)} files, {size_summary}; '
        f'{len(items) - len(pending)} already present.'
    )

    if args.list_only:
        for item in items:
            document = item['document']
            status = 'existing' if item['exists'] else format_bytes(item['size'])
            print(
                f"  {item['kind'].upper()} {document['publication_date']}: "
                f"{document['file_name']} — {status}"
            )
        return 0
    if not pending:
        return 0
    prompt = f'Download and extract {len(pending)} files ({size_summary})? [y/N] '
    if not args.yes and input(prompt).lower() not in {
        'y',
        'yes',
    }:
        print('Cancelled.')
        return 0

    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    queried_at = datetime.fromisoformat(metadata['queried_at'])
    response_path = OUTPUT_ROOT / f'response-{queried_at:%Y-%m-%dT%H%M%SZ}.json'
    response_path.write_text(json.dumps(metadata, indent=2), encoding='utf-8')

    downloaded = 0
    file_count = len(pending)
    show_progress(downloaded, total_size, 1, file_count)
    for file_number, item in enumerate(pending, start=1):
        def report_bytes(chunk_size: int, file_number: int = file_number) -> None:
            nonlocal downloaded
            downloaded += chunk_size
            show_progress(downloaded, total_size, file_number, file_count)

        download_and_extract(
            session,
            item['document'],
            item['output_dir'],
            args.keep_zip,
            item['size'],
            report_bytes,
        )
        show_progress(downloaded, total_size, file_number, file_count)
    print()
    return 0


if __name__ == '__main__':
    sys.exit(main())
