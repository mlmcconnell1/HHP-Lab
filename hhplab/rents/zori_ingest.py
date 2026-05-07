"""ZORI (Zillow Observed Rent Index) ingestion and normalization.

Downloads ZORI data from Zillow Economic Research and normalizes to
canonical long-format schema for downstream aggregation.

Usage
-----
    from hhplab.rents.zori_ingest import ingest_zori

    # Ingest county-level ZORI data
    path = ingest_zori(geography="county")

Output Schema
-------------
- geo_type (str): "county" or "zip"
- geo_id (str): county FIPS (5 chars) or ZIP code (5 chars)
- date (date): month start (e.g., 2024-01-01)
- zori (float): ZORI value (level)
- region_name (str, optional): Zillow region name
- state (str, optional): state name
- data_source (str): always "Zillow Economic Research"
- metric (str): always "ZORI"
- ingested_at (datetime UTC): timestamp of ingestion
- source_ref (str): download URL
- raw_sha256 (str): SHA256 hash of raw download
"""

from __future__ import annotations

import hashlib
import logging
import re
from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Literal

import httpx
import pandas as pd

from hhplab.paths import curated_dir
from hhplab.provenance import ProvenanceBlock, write_parquet_with_provenance
from hhplab.raw_snapshot import raw_dir
from hhplab.schema.columns import ZORI_INGEST_OUTPUT_COLUMNS
from hhplab.source_registry import check_source_changed, register_source
from hhplab.sources import ZILLOW_ZORI_COUNTY, ZILLOW_ZORI_ZIP

# Pattern for ZORI date columns (YYYY-MM-DD or YYYY-MM)
_DATE_COL_RE = re.compile(r"^\d{4}-\d{2}(-\d{2})?$")

logger = logging.getLogger(__name__)

# Zillow ZORI download URLs by geography
ZORI_URLS = {
    "county": ZILLOW_ZORI_COUNTY,
    "zip": ZILLOW_ZORI_ZIP,
}

# Required Zillow attribution
ZILLOW_ATTRIBUTION = (
    "The Zillow Economic Research team publishes a variety of real estate metrics "
    "including median home values and rents, inventory, sale prices and volumes, "
    "negative equity, home value forecasts and many more. Most datasets are available "
    "at the neighborhood, ZIP code, city, county, metro, state and national levels, "
    "and many include data as far back as the late 1990s. All data accessed and "
    "downloaded from this page is free for public use by consumers, media, analysts, "
    "academics and policymakers, consistent with our published Terms of Use. "
    "Proper and clear attribution of all data to Zillow is required."
)


def download_zori(
    geography: Literal["county", "zip"],
    url: str | None = None,
    raw_dir_override: Path | str | None = None,
    force: bool = False,
) -> tuple[Path, str]:
    """Download raw ZORI data from Zillow.

    Parameters
    ----------
    geography : str
        Geography level: "county" or "zip".
    url : str, optional
        Override URL for download. If None, uses default URL for geography.
    raw_dir_override : Path or str, optional
        Override raw directory. Defaults to canonical
        ``data/raw/zori/<year>/``.
    force : bool
        Re-download even if cached file exists.

    Returns
    -------
    tuple[Path, str]
        Tuple of (path to downloaded file, SHA256 hash of content).

    Raises
    ------
    httpx.HTTPStatusError
        If download fails.
    """
    if url is None:
        if geography not in ZORI_URLS:
            raise ValueError(f"Unknown geography: {geography}. Must be 'county' or 'zip'")
        url = ZORI_URLS[geography]

    # Generate filename with download date
    download_date = date.today().isoformat()
    download_year = date.today().year
    filename = f"zori__{geography}__{download_date}.csv"

    if raw_dir_override is not None:
        dest = Path(raw_dir_override)
    else:
        dest = raw_dir("zori", download_year)
    dest.mkdir(parents=True, exist_ok=True)
    raw_path = dest / filename

    # Check for cached file (same day)
    if raw_path.exists() and not force:
        logger.info(f"Using cached raw file: {raw_path}")
        content = raw_path.read_bytes()
        sha256 = hashlib.sha256(content).hexdigest()
        return raw_path, sha256

    # Download
    logger.info(f"Downloading ZORI {geography} data from {url}")
    with httpx.Client(timeout=120.0, follow_redirects=True) as client:
        response = client.get(url)
        response.raise_for_status()
        content = response.content

    # Compute hash and save
    sha256 = hashlib.sha256(content).hexdigest()
    raw_path.write_bytes(content)
    logger.info(f"Saved raw file to {raw_path} (sha256: {sha256[:10]}...)")

    # Check for upstream changes and register in source registry
    changed, details = check_source_changed(
        source_type="zori",
        source_url=url,
        current_sha256=sha256,
    )

    if changed:
        logger.warning(
            f"⚠️  UPSTREAM DATA CHANGED: ZORI {geography} data has changed since last download!\n"
            f"    Previous hash: {details['previous_sha256'][:16]}...\n"
            f"    Current hash:  {sha256[:16]}...\n"
            f"    Last ingested: {details['previous_ingested_at']}"
        )
    elif details.get("is_new"):
        logger.info(f"First time tracking ZORI {geography} source in registry")

    # Register this download
    register_source(
        source_type="zori",
        source_url=url,
        source_name=f"ZORI {geography.title()} Monthly",
        raw_sha256=sha256,
        file_size=len(content),
        local_path=str(raw_path),
        metadata={
            "geography": geography,
            "download_date": download_date,
            "data_source": "Zillow Economic Research",
        },
    )

    return raw_path, sha256


def _parse_zori_long(
    df: pd.DataFrame,
    geo_type: str,
    id_cols: list[str],
    fips_fn: Callable[[pd.DataFrame], pd.Series],
) -> pd.DataFrame:
    """Shared wide-to-long normalization for ZORI data.

    Parameters
    ----------
    df : pd.DataFrame
        Raw wide-format DataFrame (already read from CSV).
    geo_type : str
        Label used in log messages ("county" or "zip").
    id_cols : list[str]
        Columns to preserve through the melt (besides ``geo_id``).
    fips_fn : Callable[[pd.DataFrame], pd.Series]
        Function ``(df) -> Series`` that builds the ``geo_id`` column
        from raw columns.

    Returns
    -------
    pd.DataFrame
        Long-format DataFrame with columns:
        geo_id, date, zori, region_name, state
    """
    # Identify date columns by positive pattern match (YYYY-MM-DD or YYYY-MM)
    date_cols = [c for c in df.columns if _DATE_COL_RE.match(c)]

    # Build geo_id via caller-supplied function
    df["geo_id"] = fips_fn(df)

    # Melt to long format
    long_df = df.melt(
        id_vars=["geo_id"] + id_cols,
        value_vars=date_cols,
        var_name="date_str",
        value_name="zori",
    )

    # Parse dates - Zillow uses YYYY-MM-DD or YYYY-MM column headers.
    # Try ISO8601 mixed format to handle both forms.
    long_df["date"] = pd.to_datetime(long_df["date_str"], format="mixed", errors="coerce")
    long_df = long_df.dropna(subset=["date"])

    # Rename columns
    long_df = long_df.rename(
        columns={
            "RegionName": "region_name",
            "StateName": "state",
        }
    )

    # Drop rows with null ZORI
    long_df = long_df.dropna(subset=["zori"])

    # Select final columns
    result = long_df[["geo_id", "date", "zori", "region_name", "state"]].copy()

    # Validate geo_id format (should be 5 characters)
    invalid_geoids = result[result["geo_id"].str.len() != 5]
    if len(invalid_geoids) > 0:
        logger.warning(f"Found {len(invalid_geoids)} rows with invalid {geo_type} geo_id length")
        result = result[result["geo_id"].str.len() == 5]

    # Sort by geo_id and date
    result = result.sort_values(["geo_id", "date"]).reset_index(drop=True)

    return result


def parse_zori_county(raw_path: Path) -> pd.DataFrame:
    """Parse Zillow county ZORI CSV to long format.

    Zillow data is in wide format with date columns like "2015-01", "2015-02", etc.
    This function normalizes to long format.

    Parameters
    ----------
    raw_path : Path
        Path to raw CSV file.

    Returns
    -------
    pd.DataFrame
        Long-format DataFrame with columns:
        geo_id, date, zori, region_name, state
    """
    df = pd.read_csv(raw_path, dtype={"StateCodeFIPS": str, "MunicipalCodeFIPS": str})
    return _parse_zori_long(
        df,
        geo_type="county",
        id_cols=["RegionName", "StateName"],
        fips_fn=lambda d: d["StateCodeFIPS"].str.zfill(2) + d["MunicipalCodeFIPS"].str.zfill(3),
    )


def parse_zori_zip(raw_path: Path) -> pd.DataFrame:
    """Parse Zillow ZIP ZORI CSV to long format.

    Parameters
    ----------
    raw_path : Path
        Path to raw CSV file.

    Returns
    -------
    pd.DataFrame
        Long-format DataFrame with columns:
        geo_id, date, zori, region_name, state
    """
    df = pd.read_csv(raw_path, dtype={"RegionName": str})
    return _parse_zori_long(
        df,
        geo_type="zip",
        id_cols=["RegionName", "StateName"],
        fips_fn=lambda d: d["RegionName"].str.zfill(5),
    )


def _validate_monthly_continuity(df: pd.DataFrame, max_warnings: int = 10) -> None:
    """Validate that dates are monthly continuous per geo_id.

    Logs warnings for gaps in monthly series. Does not raise errors,
    as gaps are common in real-world ZORI data.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with 'geo_id' and 'date' columns.
    max_warnings : int
        Maximum number of gap warnings to log.
    """
    warning_count = 0

    for geo_id, group in df.groupby("geo_id"):
        dates = sorted(group["date"].dropna())
        if len(dates) < 2:
            continue

        for i in range(1, len(dates)):
            prev_date = dates[i - 1]
            curr_date = dates[i]

            # Convert to date objects if needed
            if hasattr(prev_date, "date"):
                prev_date = prev_date.date()
            if hasattr(curr_date, "date"):
                curr_date = curr_date.date()

            # Calculate expected next month (by year/month, ignoring day)
            # Zillow uses end-of-month dates, so compare by year/month only
            if prev_date.month == 12:
                expected_year, expected_month = prev_date.year + 1, 1
            else:
                expected_year, expected_month = prev_date.year, prev_date.month + 1

            if curr_date.year != expected_year or curr_date.month != expected_month:
                warning_count += 1
                if warning_count <= max_warnings:
                    logger.warning(
                        f"Gap in ZORI series for {geo_id}: "
                        f"{prev_date.year}-{prev_date.month:02d} -> "
                        f"{curr_date.year}-{curr_date.month:02d}"
                    )

    if warning_count > max_warnings:
        logger.warning(f"... and {warning_count - max_warnings} more gaps (truncated)")
    elif warning_count > 0:
        logger.info(f"Total gaps found in monthly continuity: {warning_count}")


def get_output_path(
    geography: str,
    output_dir: Path | str | None = None,
    max_year: int | str | None = None,
) -> Path:
    """Get canonical output path for normalized ZORI data.

    Parameters
    ----------
    geography : str
        Geography level ("county" or "zip").
    output_dir : Path or str, optional
        Output directory. Defaults to 'data/curated/zori'.
    max_year : int or str, optional
        Maximum year in the ZORI data. When provided, uses temporal
        naming like 'zori__county__Z2026.parquet'.

    Returns
    -------
    Path
        Output path like 'data/curated/zori/zori__county__Z2026.parquet'.
    """
    if output_dir is None:
        output_dir = curated_dir("zori")
    else:
        output_dir = Path(output_dir)
    if max_year is not None:
        from hhplab.naming import zori_ingest_filename

        return output_dir / zori_ingest_filename(geography, max_year)
    return output_dir / f"zori__{geography}.parquet"


def ingest_zori(
    geography: Literal["county", "zip"] = "county",
    url: str | None = None,
    force: bool = False,
    output_dir: Path | str | None = None,
    raw_dir: Path | str | None = None,
    start: date | str | None = None,
    end: date | str | None = None,
) -> Path:
    """Download and normalize ZORI data to canonical format.

    Parameters
    ----------
    geography : str
        Geography level: "county" (default) or "zip".
    url : str, optional
        Override download URL.
    force : bool
        Re-download and reprocess even if cached.
    output_dir : Path or str, optional
        Output directory for curated parquet. Defaults to 'data/curated/zori'.
    raw_dir : Path or str, optional
        Override directory for raw downloads. Defaults to
        ``data/raw/zori/<year>/``.
    start : date or str, optional
        Filter to dates >= start after ingest.
    end : date or str, optional
        Filter to dates <= end after ingest.

    Returns
    -------
    Path
        Path to output Parquet file.

    Raises
    ------
    httpx.HTTPStatusError
        If download fails (exit code 3).
    ValueError
        If parsing/validation fails (exit code 2).
    """
    from hhplab.naming import discover_zori_ingest

    resolved_output_dir = Path(output_dir) if output_dir is not None else curated_dir("zori")

    # Check cache via discovery (max_year unknown until after parse)
    existing = discover_zori_ingest(geography, resolved_output_dir)
    if existing is not None and not force:
        logger.info(f"Using cached file: {existing}")
        return existing

    # Download
    download_url = url or ZORI_URLS.get(geography)
    raw_path, sha256 = download_zori(geography, url, raw_dir_override=raw_dir, force=force)

    # Parse based on geography
    if geography == "county":
        df = parse_zori_county(raw_path)
    elif geography == "zip":
        df = parse_zori_zip(raw_path)
    else:
        raise ValueError(f"Unknown geography: {geography}")

    geo_count = df["geo_id"].nunique()
    logger.info(f"Parsed {len(df)} ZORI records for {geo_count} {geography} geographies")

    # Add metadata columns
    ingested_at = datetime.now(UTC)
    df["geo_type"] = geography
    df["data_source"] = "Zillow Economic Research"
    df["metric"] = "ZORI"
    df["ingested_at"] = ingested_at
    df["source_ref"] = download_url
    df["raw_sha256"] = sha256

    # Apply date filters if specified
    if start is not None:
        if isinstance(start, str):
            start = pd.to_datetime(start).date()
        df = df[df["date"].dt.date >= start]
    if end is not None:
        if isinstance(end, str):
            end = pd.to_datetime(end).date()
        df = df[df["date"].dt.date <= end]

    # Validate
    if len(df) == 0:
        raise ValueError("No ZORI data remaining after filtering")

    # Validate ZORI values are positive
    invalid_zori = df[df["zori"] <= 0]
    if len(invalid_zori) > 0:
        logger.warning(f"Found {len(invalid_zori)} rows with non-positive ZORI values")
        df = df[df["zori"] > 0]

    # Validate monthly continuity (log warnings for gaps)
    _validate_monthly_continuity(df)

    # Derive year/month integers from date for recipe consumption
    df["year"] = df["date"].dt.year.astype("int32")
    df["month"] = df["date"].dt.month.astype("int32")

    # Reorder columns to match schema
    df = df[ZORI_INGEST_OUTPUT_COLUMNS]

    # Derive max year for temporal filename
    max_year = int(df["date"].dt.year.max())
    output_path = get_output_path(geography, output_dir, max_year=max_year)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Build provenance
    provenance = ProvenanceBlock(
        extra={
            "dataset": "zori",
            "geography": geography,
            "metric": "ZORI",
            "max_year": max_year,
            "source": "Zillow Economic Research",
            "attribution": ZILLOW_ATTRIBUTION,
            "download_url": download_url,
            "downloaded_at": ingested_at.isoformat(),
            "raw_sha256": sha256,
            "row_count": len(df),
            "geo_count": df["geo_id"].nunique(),
            "date_range": [
                df["date"].min().isoformat(),
                df["date"].max().isoformat(),
            ],
        },
    )

    # Write output
    write_parquet_with_provenance(df, output_path, provenance)
    logger.info(f"Wrote normalized ZORI data to {output_path}")

    # Clean up old temporal or legacy files for this geography
    for old in resolved_output_dir.glob(f"zori__{geography}__Z*.parquet"):
        if old != output_path:
            logger.info(f"Removing superseded ZORI file: {old}")
            old.unlink()
    legacy = resolved_output_dir / f"zori__{geography}.parquet"
    if legacy.exists():
        logger.info(f"Removing legacy ZORI file: {legacy}")
        legacy.unlink()

    return output_path
