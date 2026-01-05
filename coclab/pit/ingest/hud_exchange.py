"""HUD Exchange PIT data source discovery and download.

This module handles downloading PIT (Point-in-Time) count data from HUD Exchange.
PIT data is typically provided as Excel files containing CoC-level homeless counts.

The main data source is:
https://www.hudexchange.info/resource/3031/pit-and-hic-data-since-2007/

HUD Exchange provides cumulative Excel files that contain PIT counts for all years
from 2007 up to the specified year. Each file contains multiple sheets with
different data breakdowns.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urljoin

import httpx

logger = logging.getLogger(__name__)

# Base URL for HUD Exchange PIT/HIC data resource page
HUD_EXCHANGE_PIT_BASE = "https://www.hudexchange.info/resource/3031/pit-and-hic-data-since-2007/"

# Known direct download URLs for PIT data by year
# Note: HUD provides cumulative files (2007-YYYY) containing all years
# As of late 2024, HUD migrated recent data from hudexchange.info to huduser.gov
# and changed the format from .xlsx to .xlsb (Excel Binary)
PIT_DATA_URLS: dict[int, str] = {
    # New HUD User location (2023+) - .xlsb format
    2024: "https://www.huduser.gov/portal/sites/default/files/xls/2007-2024-PIT-Counts-by-CoC.xlsb",
    2023: "https://www.huduser.gov/portal/sites/default/files/xls/2007-2023-PIT-Counts-by-CoC.xlsb",
    # Legacy HUD Exchange location (archived, may not be available)
    2022: "https://www.hudexchange.info/resources/documents/2007-2022-PIT-Counts-by-CoC.xlsx",
    2021: "https://www.hudexchange.info/resources/documents/2007-2021-PIT-Counts-by-CoC.xlsx",
    2020: "https://www.hudexchange.info/resources/documents/2007-2020-PIT-Counts-by-CoC.xlsx",
    2019: "https://www.hudexchange.info/resources/documents/2007-2019-PIT-Counts-by-CoC.xlsx",
    2018: "https://www.hudexchange.info/resources/documents/2007-2018-PIT-Counts-by-CoC.xlsx",
    2017: "https://www.hudexchange.info/resources/documents/2007-2017-PIT-Counts-by-CoC.xlsx",
    2016: "https://www.hudexchange.info/resources/documents/2007-2016-PIT-Counts-by-CoC.xlsx",
    2015: "https://www.hudexchange.info/resources/documents/2007-2015-PIT-Counts-by-CoC.xlsx",
    2014: "https://www.hudexchange.info/resources/documents/2007-2014-PIT-Counts-by-CoC.xlsx",
    2013: "https://www.hudexchange.info/resources/documents/2007-2013-PIT-Counts-by-CoC.xlsx",
    2012: "https://www.hudexchange.info/resources/documents/2007-2012-PIT-Counts-by-CoC.xlsx",
    2011: "https://www.hudexchange.info/resources/documents/2007-2011-PIT-Counts-by-CoC.xlsx",
    2010: "https://www.hudexchange.info/resources/documents/2007-2010-PIT-Counts-by-CoC.xlsx",
    2009: "https://www.hudexchange.info/resources/documents/2007-2009-PIT-Counts-by-CoC.xlsx",
    2008: "https://www.hudexchange.info/resources/documents/2007-2008-PIT-Counts-by-CoC.xlsx",
    2007: "https://www.hudexchange.info/resources/documents/2007-2007-PIT-Counts-by-CoC.xlsx",
}

# Default data directory
DEFAULT_RAW_DIR = Path("data/raw/pit")

# HTTP timeout for downloads
DOWNLOAD_TIMEOUT = 120.0

# Valid year range
MIN_PIT_YEAR = 2007
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

    HUD Exchange typically provides cumulative Excel files containing all
    years of data up to the specified year.

    Args:
        year: The PIT count year (e.g., 2024).

    Returns:
        The download URL for the PIT data file.

    Raises:
        ValueError: If the year is outside the valid range (2007-2030).
    """
    if not MIN_PIT_YEAR <= year <= MAX_PIT_YEAR:
        raise ValueError(
            f"Year {year} is outside valid PIT data range "
            f"({MIN_PIT_YEAR}-{MAX_PIT_YEAR})"
        )

    if year in PIT_DATA_URLS:
        return PIT_DATA_URLS[year]

    # Try to construct URL for years not in the explicit list
    # Use the new HUD User location for recent/future years
    url = f"https://www.huduser.gov/portal/sites/default/files/xls/2007-{year}-PIT-Counts-by-CoC.xlsb"
    logger.warning(
        f"Year {year} not in known URL list, attempting constructed URL: {url}"
    )
    return url


def download_pit_data(
    year: int,
    output_dir: Path | str | None = None,
    force: bool = False,
    timeout: float = DOWNLOAD_TIMEOUT,
) -> DownloadResult:
    """Download PIT data for a specified year from HUD Exchange.

    Downloads the PIT count data file and saves it to the raw data directory.
    Creates the output directory if it doesn't exist.

    Args:
        year: The PIT count year to download (e.g., 2024).
        output_dir: Directory to save the downloaded file.
            Defaults to data/raw/pit/{year}/.
        force: If True, re-download even if file exists. Default False.
        timeout: HTTP timeout in seconds. Default 120.

    Returns:
        DownloadResult with path to downloaded file and metadata.

    Raises:
        ValueError: If the year is invalid.
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

    with httpx.Client(follow_redirects=True, timeout=timeout) as client:
        try:
            response = client.get(url)
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            logger.error(
                f"HTTP error downloading PIT data for year {year}: "
                f"{e.response.status_code}"
            )
            raise
        except httpx.TimeoutException:
            logger.error(f"Timeout downloading PIT data for year {year}")
            raise
        except httpx.RequestError as e:
            logger.error(f"Network error downloading PIT data for year {year}: {e}")
            raise

    # Write the file
    with open(output_path, "wb") as f:
        f.write(response.content)

    file_size = len(response.content)
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
    base_url: str = HUD_EXCHANGE_PIT_BASE,
    timeout: float = 30.0,
) -> dict[int, str]:
    """Attempt to discover PIT data URLs from the HUD Exchange page.

    This function fetches the HUD Exchange resource page and attempts to
    extract download URLs for PIT data files.

    Args:
        base_url: The HUD Exchange resource page URL.
        timeout: HTTP timeout in seconds.

    Returns:
        Mapping of year to download URL.

    Note:
        This is a best-effort function that may not find all URLs depending
        on page structure changes.
    """
    try:
        with httpx.Client(follow_redirects=True, timeout=timeout) as client:
            response = client.get(base_url)
            response.raise_for_status()
        content = response.text

        # Look for links to PIT count Excel files
        # Pattern: 2007-YYYY-PIT-Counts-by-CoC.xlsx
        pattern = r'href=["\']([^"\']*2007-(\d{4})-PIT-Counts-by-CoC\.xlsx)["\']'
        matches = re.findall(pattern, content, re.IGNORECASE)

        urls = {}
        for url_path, year_str in matches:
            year = int(year_str)
            # Make absolute URL if relative
            if url_path.startswith("/"):
                full_url = urljoin("https://www.hudexchange.info", url_path)
            elif url_path.startswith("http"):
                full_url = url_path
            else:
                full_url = urljoin(base_url, url_path)
            urls[year] = full_url

        return urls

    except Exception as e:
        logger.warning(f"Failed to discover PIT URLs: {e}")
        return {}


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
