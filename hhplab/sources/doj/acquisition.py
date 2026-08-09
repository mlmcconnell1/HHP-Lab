"""DOJ sanctuary source acquisition and raw snapshot retention."""

from __future__ import annotations

from pathlib import Path

import httpx

from hhplab.storage.raw_snapshot import persist_file_snapshot

DOJ_SANCTUARY_URL = (
    "https://www.justice.gov/opa/pr/"
    "justice-department-publishes-list-sanctuary-jurisdictions"
)
DOJ_SANCTUARY_SOURCE_DATE = "2025-08-05"
RAW_SANCTUARY_HTML_FILENAME = "doj_sanctuary_jurisdictions_2025-08-05.html"

def download_doj_sanctuary_page(raw_root: Path | None = None) -> tuple[Path, str, int]:
    """Download and persist the DOJ sanctuary jurisdiction press release HTML."""
    with httpx.Client(timeout=120.0) as client:
        response = client.get(DOJ_SANCTUARY_URL, follow_redirects=True)
        response.raise_for_status()
        raw_content = response.content

    return persist_file_snapshot(
        raw_content,
        "sanctuary",
        RAW_SANCTUARY_HTML_FILENAME,
        raw_root=raw_root,
    )
