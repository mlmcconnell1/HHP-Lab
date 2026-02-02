"""HUD User PIT data source discovery and download.

This module handles downloading PIT (Point-in-Time) count data from HUD User.
PIT data is provided as Excel files containing CoC-level homeless counts.

The main data source is:
https://www.huduser.gov/portal/datasets/ahar/2024-ahar-part-1-pit-estimates-of-homelessness-in-the-us.html

HUD provides cumulative Excel files that contain PIT counts for all years
from 2007 up to the specified year. Each file contains multiple sheets with
different data breakdowns.

Note: PIT data files are only directly available for vintage years 2013 and later.
Earlier years (2007-2012) are included in the 2013+ files but not available as
separate downloads. The format changed from .xlsx to .xlsb around 2023.
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import httpx

from coclab.source_registry import check_source_changed, register_source
from coclab.sources import HUD_USER_PIT_BASE as HUD_USER_PIT_BASE_URL

logger = logging.getLogger(__name__)

# Base URL for HUD User PIT/HIC data
HUD_USER_PIT_BASE = HUD_USER_PIT_BASE_URL

# Known direct download URLs for PIT data by year
# Note: HUD provides cumulative files (2007-YYYY) containing all years
# Files are only available for vintage years 2013+. Earlier data (2007-2012) is
# included in the 2013+ files. Format changed from .xlsx to .xlsb around 2023.
PIT_DATA_URLS: dict[int, str] = {
    # .xlsb format (2023+)
    2024: "https://www.huduser.gov/portal/sites/default/files/xls/2007-2024-PIT-Counts-by-CoC.xlsb",
    2023: "https://www.huduser.gov/portal/sites/default/files/xls/2007-2023-PIT-Counts-by-CoC.xlsb",
    # .xlsx format (2013-2022)
    2022: "https://www.huduser.gov/portal/sites/default/files/xls/2007-2022-PIT-Counts-by-CoC.xlsx",
    2021: "https://www.huduser.gov/portal/sites/default/files/xls/2007-2021-PIT-Counts-by-CoC.xlsx",
    2020: "https://www.huduser.gov/portal/sites/default/files/xls/2007-2020-PIT-Counts-by-CoC.xlsx",
    2019: "https://www.huduser.gov/portal/sites/default/files/xls/2007-2019-PIT-Counts-by-CoC.xlsx",
    2018: "https://www.huduser.gov/portal/sites/default/files/xls/2007-2018-PIT-Counts-by-CoC.xlsx",
    2017: "https://www.huduser.gov/portal/sites/default/files/xls/2007-2017-PIT-Counts-by-CoC.xlsx",
    2016: "https://www.huduser.gov/portal/sites/default/files/xls/2007-2016-PIT-Counts-by-CoC.xlsx",
    2015: "https://www.huduser.gov/portal/sites/default/files/xls/2007-2015-PIT-Counts-by-CoC.xlsx",
    2014: "https://www.huduser.gov/portal/sites/default/files/xls/2007-2014-PIT-Counts-by-CoC.xlsx",
    2013: "https://www.huduser.gov/portal/sites/default/files/xls/2007-2013-PIT-Counts-by-CoC.xlsx",
}

# Default data directory
DEFAULT_RAW_DIR = Path("data/raw/pit")

# HTTP timeout for downloads
DOWNLOAD_TIMEOUT = 120.0

# Valid year range for direct file downloads
# Note: PIT files are only directly available for vintage 2013+.
# Earlier years (2007-2012) data is included in the 2013+ cumulative files.
MIN_PIT_YEAR = 2013
MAX_PIT_YEAR = 2030


@dataclass
class DownloadResult:
    """Result of a PIT data download operation.

    Attributes:
        path: Path to the downloaded file
        source_url: URL the file was downloaded from
        downloaded_at: UTC timestamp of download
        file_size: Size of downloaded file in bytes
    """

    path: Path
    source_url: str
    downloaded_at: datetime
    file_size: int


def get_pit_source_url(year: int) -> str:
    """Get the download URL for PIT data for a given year.

    HUD provides cumulative Excel files containing all years of data up to
    the specified vintage year. Files are available at huduser.gov.

    Args:
        year: The PIT vintage year (e.g., 2024). This is the latest year
            included in the cumulative file.

    Returns:
        The download URL for the PIT data file.

    Raises:
        ValueError: If the year is before 2013 (files not available) or
            after 2030.
    """
    if year < MIN_PIT_YEAR:
        raise ValueError(
            f"PIT data files are not directly available for vintage year {year}. "
            f"Files are only available for years {MIN_PIT_YEAR} and later. "
            f"To get data for years 2007-2012, use the 2013 vintage file which "
            f"contains cumulative data from 2007-2013."
        )

    if year > MAX_PIT_YEAR:
        raise ValueError(f"Year {year} is outside valid PIT data range (max: {MAX_PIT_YEAR})")

    if year in PIT_DATA_URLS:
        return PIT_DATA_URLS[year]

    # For years not in the explicit list (future years), try .xlsb first
    url = f"{HUD_USER_PIT_BASE}2007-{year}-PIT-Counts-by-CoC.xlsb"
    logger.warning(f"Year {year} not in known URL list, attempting constructed URL: {url}")
    return url


def _try_download_url(
    client: httpx.Client,
    url: str,
) -> httpx.Response | None:
    """Attempt to download from a URL, returning None on 404."""
    try:
        response = client.get(url)
        response.raise_for_status()
        return response
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return None
        raise


def _generate_alternate_urls(url: str, year: int) -> list[str]:
    """Generate alternate URLs to try if primary fails.

    HUD has used different filename patterns over the years:
    - 2007-{year}-PIT-Counts-by-CoC.xlsx (newer format)
    - 2007-{year}-Point-in-Time-Estimates-by-CoC.xlsx (older format)
    - 2007-{year}-PIT-Estimates-by-CoC.xlsx (alternate older format)

    Also tries both .xlsx and .xlsb extensions.
    """
    alternates = []
    seen = set()

    # All known filename patterns
    all_patterns = [
        "PIT-Counts-by-CoC",
        "Point-in-Time-Estimates-by-CoC",
        "PIT-Estimates-by-CoC",
    ]

    # Find which pattern is in the current URL
    current_pattern = None
    for pattern in all_patterns:
        if pattern in url:
            current_pattern = pattern
            break

    if current_pattern is None:
        return alternates

    # Extensions to try
    extensions = [".xlsx", ".xlsb"]

    # Current extension
    current_ext = ".xlsb" if url.endswith(".xlsb") else ".xlsx"

    # Generate all combinations of alternate patterns and extensions
    for pattern in all_patterns:
        for ext in extensions:
            # Skip the original URL
            if pattern == current_pattern and ext == current_ext:
                continue

            alt_url = url.replace(current_pattern, pattern)
            if ext != current_ext:
                alt_url = alt_url.replace(current_ext, ext)

            if alt_url not in seen:
                seen.add(alt_url)
                alternates.append(alt_url)

    return alternates


def download_pit_data(
    year: int,
    output_dir: Path | str | None = None,
    force: bool = False,
    timeout: float = DOWNLOAD_TIMEOUT,
) -> DownloadResult:
    """Download PIT data for a specified year from HUD User.

    Downloads the PIT count data file and saves it to the raw data directory.
    Creates the output directory if it doesn't exist.

    For years not in the known URL list, tries both .xlsb and .xlsx formats.

    Args:
        year: The PIT vintage year to download (e.g., 2024).
        output_dir: Directory to save the downloaded file.
            Defaults to data/raw/pit/{year}/.
        force: If True, re-download even if file exists. Default False.
        timeout: HTTP timeout in seconds. Default 120.

    Returns:
        DownloadResult with path to downloaded file and metadata.

    Raises:
        ValueError: If the year is invalid (before 2013 or after 2030).
        FileNotFoundError: If neither .xlsx nor .xlsb file is available.
        httpx.HTTPStatusError: If the download fails with HTTP error.
        httpx.TimeoutException: If the download times out.
        httpx.RequestError: For other network errors.
    """
    # Validate year (get_pit_source_url does this too)
    url = get_pit_source_url(year)

    if output_dir is None:
        output_dir = DEFAULT_RAW_DIR / str(year)
    else:
        output_dir = Path(output_dir)

    output_dir.mkdir(parents=True, exist_ok=True)

    filename = url.split("/")[-1]
    output_path = output_dir / filename

    if output_path.exists() and not force:
        logger.info(f"File already exists: {output_path}")
        # Return result with existing file info
        file_size = output_path.stat().st_size
        return DownloadResult(
            path=output_path,
            source_url=url,
            downloaded_at=datetime.now(UTC),
            file_size=file_size,
        )

    logger.info(f"Downloading PIT data for {year} from {url}")

    tried_urls = [url]

    with httpx.Client(follow_redirects=True, timeout=timeout) as client:
        response = _try_download_url(client, url)

        # If primary URL failed with 404, try alternate URLs
        if response is None:
            alternate_urls = _generate_alternate_urls(url, year)
            for alt_url in alternate_urls:
                logger.info(f"Primary URL not found, trying: {alt_url}")
                tried_urls.append(alt_url)
                response = _try_download_url(client, alt_url)

                if response is not None:
                    url = alt_url
                    filename = url.split("/")[-1]
                    output_path = output_dir / filename
                    break

        if response is None:
            raise FileNotFoundError(
                f"PIT data file not found for year {year}. "
                f"Tried URLs: {tried_urls}. "
                f"The file may not yet be published by HUD."
            )

    # Compute SHA-256 hash of raw content
    raw_content = response.content
    content_sha256 = hashlib.sha256(raw_content).hexdigest()
    file_size = len(raw_content)

    # Check for upstream changes
    changed, details = check_source_changed(
        source_type="pit",
        source_url=url,
        current_sha256=content_sha256,
    )

    if changed:
        logger.warning(
            f"UPSTREAM DATA CHANGED: PIT data for {year} has changed since last download! "
            f"Previous hash: {details['previous_sha256'][:16]}... "
            f"Current hash: {content_sha256[:16]}... "
            f"Last ingested: {details['previous_ingested_at']}"
        )
    elif details.get("is_new"):
        logger.info(f"First time tracking PIT data for {year} in source registry")

    # Write the file
    with open(output_path, "wb") as f:
        f.write(raw_content)

    downloaded_at = datetime.now(UTC)

    logger.info(f"Downloaded PIT data to {output_path} ({file_size:,} bytes)")

    # Write metadata file as JSON for better parsing
    _write_metadata(
        output_dir=output_dir,
        filename=filename,
        source_url=url,
        downloaded_at=downloaded_at,
        file_size=file_size,
        year=year,
    )

    # Register this download in source registry
    register_source(
        source_type="pit",
        source_url=url,
        source_name=f"HUD PIT Counts 2007-{year}",
        raw_sha256=content_sha256,
        file_size=file_size,
        local_path=str(output_path),
        metadata={
            "pit_year": year,
            "format": "xlsb" if url.endswith(".xlsb") else "xlsx",
        },
    )

    return DownloadResult(
        path=output_path,
        source_url=url,
        downloaded_at=downloaded_at,
        file_size=file_size,
    )


def _write_metadata(
    output_dir: Path,
    filename: str,
    source_url: str,
    downloaded_at: datetime,
    file_size: int,
    year: int,
) -> None:
    """Write metadata file alongside downloaded data.

    Creates a .meta.json file with provenance information.
    """
    metadata = {
        "source_url": source_url,
        "downloaded_at": downloaded_at.isoformat(),
        "pit_year": year,
        "file_size": file_size,
        "original_filename": filename,
    }

    meta_path = output_dir / f"{filename}.meta.json"
    with open(meta_path, "w") as f:
        json.dump(metadata, f, indent=2)

    logger.debug(f"Wrote metadata to {meta_path}")


def list_available_years() -> list[int]:
    """List years with known PIT data URLs.

    Returns:
        List of years with known download URLs, sorted descending.
    """
    return sorted(PIT_DATA_URLS.keys(), reverse=True)


def discover_pit_urls(
    timeout: float = 30.0,
) -> dict[int, str]:
    """Return known PIT data URLs from HUD User.

    This function returns the known mapping of years to download URLs.
    URL discovery is not currently supported since HUD User does not provide
    a standard listing page.

    Args:
        timeout: Unused, kept for API compatibility.

    Returns:
        Mapping of year to download URL for known years (2013+).
    """
    return dict(PIT_DATA_URLS)


def check_pit_availability(year: int, timeout: float = 10.0) -> bool:
    """Check if PIT data is available for a given year.

    Performs a HEAD request to check if the URL is accessible.

    Args:
        year: Year to check.
        timeout: HTTP timeout in seconds.

    Returns:
        True if data appears to be available, False otherwise.
    """
    try:
        url = get_pit_source_url(year)
        with httpx.Client(follow_redirects=True, timeout=timeout) as client:
            response = client.head(url)
            return response.status_code == 200
    except Exception:
        return False


def download_pit_data_range(
    start_year: int,
    end_year: int,
    output_dir: Path | str | None = None,
    force: bool = False,
) -> list[DownloadResult]:
    """Download PIT data for a range of years.

    Args:
        start_year: First year to download (inclusive).
        end_year: Last year to download (inclusive).
        output_dir: Base directory for downloads.
        force: If True, re-download even if files exist.

    Returns:
        List of DownloadResult objects for successful downloads.

    Raises:
        ValueError: If year range is invalid.
    """
    if start_year > end_year:
        raise ValueError(f"Start year {start_year} must be <= end year {end_year}")

    results = []
    for year in range(start_year, end_year + 1):
        try:
            result = download_pit_data(year, output_dir=output_dir, force=force)
            results.append(result)
        except Exception as e:
            logger.warning(f"Failed to download PIT data for year {year}: {e}")
            # Continue with other years

    return results
