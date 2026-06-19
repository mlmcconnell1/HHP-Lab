"""PRISM monthly temperature raw ZIP ingestion."""

from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import httpx

from hhplab.raw_snapshot import hash_zip_contents, persist_file_snapshot
from hhplab.source_registry import check_source_changed, register_source
from hhplab.sources import PRISM_WEB_SERVICE_TEMPLATE

PRISM_TEMPERATURE_VARIABLES: tuple[str, ...] = ("tmin", "tmean", "tmax")
MIN_PRISM_MONTHLY_YEAR = 1895
MAX_PRISM_MONTHLY_YEAR = 2100
DOWNLOAD_TIMEOUT = 180.0

logger = logging.getLogger(__name__)

PRISMTemperatureVariable = Literal["tmin", "tmean", "tmax"]


@dataclass(frozen=True)
class PRISMDownloadResult:
    """Raw PRISM monthly download or cache result."""

    path: Path
    source_url: str
    variable: str
    year: int
    month: int
    downloaded_at: datetime
    file_size: int
    raw_sha256: str
    cached: bool


def validate_prism_monthly_request(variable: str, year: int, month: int) -> None:
    """Validate a PRISM monthly temperature request."""
    if variable not in PRISM_TEMPERATURE_VARIABLES:
        supported = ", ".join(PRISM_TEMPERATURE_VARIABLES)
        raise ValueError(f"Unsupported PRISM variable {variable!r}; expected one of: {supported}.")
    if month < 1 or month > 12:
        raise ValueError(f"Unsupported PRISM month {month}; expected an integer from 1 to 12.")
    if year < MIN_PRISM_MONTHLY_YEAR or year > MAX_PRISM_MONTHLY_YEAR:
        raise ValueError(
            f"Unsupported PRISM year {year}; monthly data are expected in "
            f"{MIN_PRISM_MONTHLY_YEAR}-{MAX_PRISM_MONTHLY_YEAR}."
        )


def get_prism_monthly_source_url(variable: str, year: int, month: int) -> str:
    """Return the current PRISM web-service URL for a monthly 4km grid ZIP."""
    validate_prism_monthly_request(variable, year, month)
    return PRISM_WEB_SERVICE_TEMPLATE.format(variable=variable, year=year, month=month)


def default_prism_monthly_filename(variable: str, year: int, month: int) -> str:
    """Return the stable local filename for a PRISM monthly ZIP."""
    validate_prism_monthly_request(variable, year, month)
    return f"prism_{variable}_us_25m_{year}{month:02d}.zip"


def download_prism_monthly(
    variable: str,
    year: int,
    month: int = 1,
    *,
    force: bool = False,
    raw_root: Path | None = None,
    timeout: float = DOWNLOAD_TIMEOUT,
) -> PRISMDownloadResult:
    """Download or reuse a cached PRISM monthly temperature ZIP."""
    validate_prism_monthly_request(variable, year, month)
    source_url = get_prism_monthly_source_url(variable, year, month)
    filename = default_prism_monthly_filename(variable, year, month)
    raw_path = _raw_path(variable, year, filename, raw_root=raw_root)

    if raw_path.exists() and not force and raw_path.stat().st_size > 0:
        content = raw_path.read_bytes()
        raw_sha256 = (
            hash_zip_contents(content)
            if raw_path.suffix == ".zip"
            else hashlib.sha256(content).hexdigest()
        )
        return PRISMDownloadResult(
            path=raw_path,
            source_url=source_url,
            variable=variable,
            year=year,
            month=month,
            downloaded_at=datetime.now(UTC),
            file_size=len(content),
            raw_sha256=raw_sha256,
            cached=True,
        )

    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        response = client.get(source_url)
    response.raise_for_status()
    content = response.content
    if not content:
        raise FileNotFoundError(
            f"PRISM returned an empty response for {variable} {year}-{month:02d}. "
            "Check the year/month and retry `hhplab ingest prism --variable "
            f"{variable} --year {year} --month {month}`."
        )
    if not _looks_like_zip(response, content):
        raise RuntimeError(
            f"PRISM response for {variable} {year}-{month:02d} was not a ZIP file. "
            f"Source URL: {source_url}"
        )

    response_filename = _filename_from_content_disposition(response) or filename
    raw_path, raw_sha256, file_size = persist_file_snapshot(
        content,
        "prism",
        response_filename,
        subdirs=(variable, "monthly", str(year)),
        raw_root=raw_root,
    )

    changed, details = check_source_changed(
        source_type="prism",
        source_url=source_url,
        current_sha256=raw_sha256,
    )
    if changed:
        logger.warning(
            "PRISM source changed: previous=%s current=%s last_ingested=%s",
            details["previous_sha256"],
            raw_sha256,
            details["previous_ingested_at"],
        )

    register_source(
        source_type="prism",
        source_url=source_url,
        source_name=f"PRISM 4km monthly {variable} {year}-{month:02d}",
        raw_sha256=raw_sha256,
        file_size=file_size,
        local_path=str(raw_path),
        metadata={
            "provider": "prism",
            "product": "temperature",
            "variable": variable,
            "year": year,
            "month": month,
            "temporal_period": "monthly",
            "resolution": "4km",
            "native_geometry": "raster",
            "format": "zip",
        },
    )

    return PRISMDownloadResult(
        path=raw_path,
        source_url=source_url,
        variable=variable,
        year=year,
        month=month,
        downloaded_at=datetime.now(UTC),
        file_size=file_size,
        raw_sha256=raw_sha256,
        cached=False,
    )


def _raw_path(variable: str, year: int, filename: str, *, raw_root: Path | None) -> Path:
    root = raw_root if raw_root is not None else None
    base = root if root is not None else Path("data/raw")
    if raw_root is None:
        from hhplab.paths import raw_root as configured_raw_root

        base = configured_raw_root()
    return base / "prism" / variable / "monthly" / str(year) / filename


def _looks_like_zip(response: httpx.Response, content: bytes) -> bool:
    content_type = response.headers.get("content-type", "").lower()
    return content.startswith(b"PK\x03\x04") or "zip" in content_type


def _filename_from_content_disposition(response: httpx.Response) -> str | None:
    header = response.headers.get("content-disposition")
    if not header:
        return None
    match = re.search(r'filename="?([^";]+)"?', header)
    filename = match.group(1) if match else None
    if not filename:
        return None
    return Path(filename).name
