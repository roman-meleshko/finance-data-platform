"""Download and extract the latest GLEIF Golden Copy and delta files."""

from __future__ import annotations

import argparse
import json
import sys
import zipfile
from pathlib import Path
from urllib.parse import unquote, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API_TEMPLATE = (
    'https://goldencopy.gleif.org/api/v2/golden-copies/publishes/{file_type}/latest'
)
REPO_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_ROOT = REPO_ROOT / 'data' / 'raw' / 'gleif'
CHUNK_SIZE = 1024 * 1024
TIMEOUT = (10, 120)


def build_session() -> requests.Session:
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=('GET',),
    )
    session = requests.Session()
    session.headers['User-Agent'] = 'finance-data-platform/gleif-downloader'
    session.mount('https://', HTTPAdapter(max_retries=retry))
    return session


def format_bytes(size: int | None) -> str:
    if size is None:
        return 'unknown'
    value = float(size)
    for unit in ('B', 'KiB', 'MiB', 'GiB'):
        if value < 1024 or unit == 'GiB':
            return f'{value:.1f} {unit}'
        value /= 1024
    raise AssertionError('unreachable')


def archive_name(url: str) -> str:
    return Path(unquote(urlparse(url).path)).name


def expected_extracted_path(archive_path: Path) -> Path:
    name = archive_path.name
    return archive_path.with_name(name.removesuffix('.zip'))


def is_safe_member(output_dir: Path, member: str) -> bool:
    destination = (output_dir / member).resolve()
    resolved_output = output_dir.resolve()
    return destination == resolved_output or resolved_output in destination.parents


def download_and_extract(
    session: requests.Session,
    item: dict,
    output_dir: Path,
    keep_zip: bool,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    archive_path = output_dir / archive_name(item['url'])
    extracted_path = expected_extracted_path(archive_path)

    if extracted_path.exists():
        if extracted_path.is_file() and extracted_path.stat().st_size > 0:
            print(f'Skip existing: {extracted_path.name}')
            return
        if not extracted_path.is_file():
            raise ValueError(f'Expected a file but found: {extracted_path}')

    partial_path = archive_path.with_suffix(archive_path.suffix + '.part')
    downloaded = 0
    with session.get(item['url'], stream=True, timeout=TIMEOUT) as response:
        response.raise_for_status()
        response_size = int(response.headers.get('Content-Length', 0)) or None
        expected_size = item.get('size') or response_size
        with partial_path.open('wb') as output:
            for chunk in response.iter_content(CHUNK_SIZE):
                if not chunk:
                    continue
                output.write(chunk)
                downloaded += len(chunk)
                if expected_size:
                    print(
                        f'\r{archive_path.name}: {downloaded / expected_size:.1%} '
                        f'({format_bytes(downloaded)}/{format_bytes(expected_size)})',
                        end='',
                        flush=True,
                    )
        print()

    if expected_size is not None and downloaded != expected_size:
        partial_path.unlink()
        raise ValueError(
            f'Size mismatch for {archive_path.name}: expected {expected_size} bytes, '
            f'got {downloaded}'
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
    print(f'Extracted: {extracted_path}')


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--file-type',
        nargs='+',
        choices=('lei2', 'rr', 'repex'),
        default=('lei2',),
        help='GLEIF dataset(s) to download (default: lei2).',
    )
    parser.add_argument(
        '--format',
        choices=('csv', 'json', 'xml'),
        default='csv',
        help='Export format (default: csv).',
    )
    parser.add_argument(
        '--delta',
        choices=('IntraDay', 'LastDay', 'LastWeek', 'LastMonth'),
        default='LastDay',
        help='Delta window (default: LastDay).',
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
    session = build_session()
    items: list[dict] = []
    metadata: dict[str, dict] = {}

    for file_type in args.file_type:
        response = session.get(
            API_TEMPLATE.format(file_type=file_type),
            timeout=TIMEOUT,
        )
        response.raise_for_status()
        payload = response.json()
        data = payload['data']
        metadata[file_type] = payload
        full_file = data['full_file'][args.format]
        delta_file = data['delta_files'][args.delta][args.format]
        items.extend(
            (
                {
                    'label': f'{file_type} Golden Copy',
                    'url': full_file['url'],
                    'size': full_file.get('size'),
                    'record_count': full_file.get('record_count'),
                    'output_dir': OUTPUT_ROOT / 'golden-copy',
                },
                {
                    'label': f'{file_type} {args.delta} delta',
                    'url': delta_file['url'],
                    'size': delta_file.get('size'),
                    'record_count': delta_file.get('record_count'),
                    'output_dir': OUTPUT_ROOT / 'deltas',
                },
            )
        )

    print('Selected GLEIF files:')
    for item in items:
        count = item['record_count']
        count_str = f'{count:,}' if count is not None else 'unknown'
        print(
            f"  {item['label']}: {archive_name(item['url'])}, "
            f"{count_str} records, {format_bytes(item['size'])}"
        )

    if args.list_only:
        return 0
    if not args.yes:
        answer = input('Download and extract these files? [y/N] ').lower()
        if answer not in {'y', 'yes'}:
            print('Cancelled.')
            return 0

    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    (OUTPUT_ROOT / 'latest-api-response.json').write_text(
        json.dumps(metadata, indent=2),
        encoding='utf-8',
    )
    for item in items:
        download_and_extract(session, item, item['output_dir'], args.keep_zip)
    return 0


if __name__ == '__main__':
    sys.exit(main())
