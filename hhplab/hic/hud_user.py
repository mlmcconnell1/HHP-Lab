"""HUD User HIC raw-file download and discovery helpers."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import httpx

from hhplab.paths import raw_root
from hhplab.source_registry import check_source_changed, register_source
from hhplab.sources import HUD_USER_HIC_COUNTS_BY_STATE_TEMPLATE

DOWNLOAD_TIMEOUT = 120.0
MIN_HIC_YEAR = 2007
MAX_HIC_YEAR = 2030


class HICManualDownloadRequired(RuntimeError):
    """Raised when HUD User blocks automated download with an interactive challenge."""


@dataclass
class HICDownloadResult:
    """Raw HIC download or discovery result."""

    path: Path
    source_url: str
    downloaded_at: datetime
    file_size: int
    raw_sha256: str


def get_hic_source_url(year: int) -> str:
    """Return the expected HUD User HIC by-state CSV URL for a year."""
    if year < MIN_HIC_YEAR or year > MAX_HIC_YEAR:
        raise ValueError(
            f"Year {year} is outside valid HIC data range "
            f"({MIN_HIC_YEAR}-{MAX_HIC_YEAR})."
        )
    return HUD_USER_HIC_COUNTS_BY_STATE_TEMPLATE.format(year=year)


def discover_local_hic_file(year: int, raw_dir: Path | str | None = None) -> Path | None:
    """Find a manually placed HIC raw file for ``year``."""
    directory = Path(raw_dir) if raw_dir is not None else raw_root() / "hic" / str(year)
    if not directory.exists():
        return None
    if directory.is_file():
        return directory
    patterns = [
        str(year),
        f"{year}-HIC-Counts-by-State.csv",
        f"{year}-HIC-Counts-by-State.xlsx",
        f"{year}-HIC-Counts-by-State.xls",
        f"{year}-HIC-Counts-by-State.xlsb",
        "*-HIC-Counts-by-CoC.csv",
        "*-HIC-Counts-by-CoC.xlsx",
        "*-HIC-Counts-by-CoC.xls",
        "*-HIC-Counts-by-CoC.xlsb",
        "*-HIC-Counts-by-State.csv",
        "*-HIC-Counts-by-State.xlsx",
        "*-HIC-Counts-by-State.xls",
        "*-HIC-Counts-by-State.xlsb",
        f"*{year}*HIC*.csv",
        f"*{year}*HIC*.xlsx",
        f"*{year}*HIC*.xls",
        f"*{year}*HIC*.xlsb",
    ]
    candidates: list[Path] = []
    for pattern in patterns:
        candidates.extend(path for path in directory.glob(pattern) if path.is_file())
    return sorted(set(candidates))[0] if candidates else None


def download_hic_data(
    year: int,
    output_dir: Path | str | None = None,
    *,
    force: bool = False,
    timeout: float = DOWNLOAD_TIMEOUT,
) -> HICDownloadResult:
    """Download a HUD User HIC file, detecting WAF challenge responses explicitly."""
    url = get_hic_source_url(year)
    directory = Path(output_dir) if output_dir is not None else raw_root() / "hic" / str(year)
    directory.mkdir(parents=True, exist_ok=True)
    output_path = directory / url.split("/")[-1]

    if output_path.exists() and not force and output_path.stat().st_size > 0:
        content = output_path.read_bytes()
        return HICDownloadResult(
            path=output_path,
            source_url=url,
            downloaded_at=datetime.now(UTC),
            file_size=len(content),
            raw_sha256=hashlib.sha256(content).hexdigest(),
        )

    with httpx.Client(follow_redirects=True, timeout=timeout) as client:
        response = client.get(url)

    if _is_hud_challenge_response(response):
        raise HICManualDownloadRequired(_manual_download_message(year, url, directory))
    if response.status_code == 404:
        raise FileNotFoundError(
            f"HIC data file not found for year {year} at {url}. "
            f"Place a manually downloaded file under {directory} and rerun "
            "`hhplab ingest hic --year <YEAR> --parse-only`."
        )
    response.raise_for_status()
    if not response.content:
        raise FileNotFoundError(
            f"HUD returned an empty HIC response for year {year}. "
            f"Place a manually downloaded file under {directory} and rerun "
            "`hhplab ingest hic --year <YEAR> --parse-only`."
        )

    content = response.content
    raw_sha256 = hashlib.sha256(content).hexdigest()
    output_path.write_bytes(content)
    downloaded_at = datetime.now(UTC)

    changed, details = check_source_changed(
        source_type="hic",
        source_url=url,
        current_sha256=raw_sha256,
    )
    if changed:
        # Keep this warning available to configured logging without making
        # ingest fail for normal upstream corrections.
        import logging

        logging.getLogger(__name__).warning(
            "HUD HIC source changed: previous=%s current=%s last_ingested=%s",
            details["previous_sha256"],
            raw_sha256,
            details["previous_ingested_at"],
        )

    register_source(
        source_type="hic",
        source_url=url,
        source_name=f"HUD HIC Counts {year}",
        raw_sha256=raw_sha256,
        file_size=len(content),
        local_path=str(output_path),
        metadata={"hic_year": year, "format": output_path.suffix.lower().lstrip(".")},
    )

    return HICDownloadResult(
        path=output_path,
        source_url=url,
        downloaded_at=downloaded_at,
        file_size=len(content),
        raw_sha256=raw_sha256,
    )


def _is_hud_challenge_response(response: httpx.Response) -> bool:
    waf_action = response.headers.get("x-amzn-waf-action", "").lower()
    content_type = response.headers.get("content-type", "").lower()
    body_start = response.text[:500].lower() if response.content else ""
    return (
        response.status_code == 202
        and waf_action == "challenge"
        or ("text/html" in content_type and "verify that you're not a robot" in body_start)
        or ("text/html" in content_type and "javascript is disabled" in body_start)
    )


def _manual_download_message(year: int, url: str, directory: Path) -> str:
    return (
        f"HUD User returned an interactive WAF challenge for HIC year {year}. "
        f"Download the file manually from {url}, place it under {directory}, "
        f"then rerun `hhplab ingest hic --year {year} --parse-only`."
    )
