"""Download the ISO 10383 MIC list, stamped with its official publication date.

The CSV endpoint carries no date (and its Last-Modified drifts after release),
so the date is scraped from the release page and encoded in the filename,
which the normalization step parses back out.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

PAGE_URL = "https://www.iso20022.org/market-identifier-codes"
FILE_URL = "https://www.iso20022.org/sites/default/files/ISO10383_MIC/ISO10383_MIC.csv"
REPO_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_ROOT = REPO_ROOT / "data" / "raw" / "iso_mic"
HEADERS = {"User-Agent": "finance-data-platform/iso-mic-downloader"}
TIMEOUT = (10, 60)
MIN_BYTES = 400_000  # the list is ~490 KB and only grows; smaller means a wrong page


def get_publication_date() -> str:
    response = requests.get(PAGE_URL, headers=HEADERS, timeout=TIMEOUT)
    response.raise_for_status()

    table = BeautifulSoup(response.text, "html.parser").find("table")
    headers = [th.get_text(strip=True) for th in table.select("thead th")]
    cells = table.select_one("tbody tr").find_all("td")

    raw_date = cells[headers.index("Publication date")].get_text(strip=True)
    publication_date = (
        datetime.strptime(raw_date, "%d %B %Y").replace(tzinfo=timezone.utc).date()
    )
    return str(publication_date)


def get_mic_csv() -> bytes:
    response = requests.get(FILE_URL, headers=HEADERS, timeout=TIMEOUT)
    response.raise_for_status()
    content = response.content

    declared = response.headers.get("Content-Length")
    if declared and len(content) != int(declared):
        raise ValueError(
            f"truncated download: got {len(content)} bytes, "
            f"Content-Length says {declared}"
        )
    if len(content) < MIN_BYTES:
        raise ValueError(f"suspiciously small MIC file: {len(content)} bytes")
    return content


def main() -> int:
    publication_date = get_publication_date()
    content = get_mic_csv()

    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_ROOT / f"ISO10383_MIC-{publication_date}.csv"
    partial_path = output_path.with_suffix(output_path.suffix + ".part")
    partial_path.write_bytes(content)
    partial_path.replace(output_path)

    print(f"ISO MIC file successfully downloaded to: {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
