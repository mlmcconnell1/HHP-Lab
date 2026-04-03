"""PEP (Population Estimates Program) county-level data ingestion.

Downloads and normalizes Census Bureau Population Estimates Program
county-level annual population estimates.

Data Sources
------------
- Vintage 2020 (2010-2020): Original postcensal estimates
  https://www2.census.gov/programs-surveys/popest/datasets/2010-2020/counties/totals/co-est2020-alldata.csv

- Vintage 2024 (2020-2024): Current postcensal estimates
  https://www2.census.gov/programs-surveys/popest/datasets/2020-2024/counties/totals/co-est2024-alldata.csv

Usage
-----
    from coclab.pep.ingest import ingest_pep_county

    # Ingest best-available PEP county estimates (postcensal)
    path = ingest_pep_county()

    # Ingest postcensal vintage 2024 data (covers 2020-2024)
    path = ingest_pep_county(series="postcensal", vintage=2024)
"""

from __future__ import annotations

import hashlib
import logging
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Literal

import httpx
import pandas as pd

from coclab.paths import curated_dir
from coclab.provenance import ProvenanceBlock, write_parquet_with_provenance
from coclab.raw_snapshot import raw_dir as canonical_raw_dir
from coclab.source_registry import check_source_changed, register_source
from coclab.sources import CENSUS_PEP_DATASETS_BASE

logger = logging.getLogger(__name__)

# Census Bureau PEP download URLs by vintage (postcensal)
# Vintage 2020 = estimates through July 1, 2020 (released 2021)
# Vintage 2024 = estimates through July 1, 2024 (released 2025)
PEP_URLS = {
    2020: f"{CENSUS_PEP_DATASETS_BASE}/2010-2020/counties/totals/co-est2020-alldata.csv",
    2024: f"{CENSUS_PEP_DATASETS_BASE}/2020-2024/counties/totals/co-est2024-alldata.csv",
}

# Census Bureau attribution (public domain, but cite source)
CENSUS_ATTRIBUTION = "Source: U.S. Census Bureau, Population Estimates Program (PEP)"


# Population columns by vintage
# Vintage 2020 file: POPESTIMATE2010 through POPESTIMATE2020
# Vintage 2024 file: POPESTIMATE2020 through POPESTIMATE2024
VINTAGE_YEARS = {
    2020: list(range(2010, 2021)),  # 2010-2020
    2024: list(range(2020, 2025)),  # 2020-2024
}

POSTCENSAL_SERIES = "postcensal"
AUTO_SERIES = "auto"


def _postcensal_year_range(vintage: int) -> tuple[int, int] | None:
    years = VINTAGE_YEARS.get(vintage)
    if not years:
        return None
    return min(years), max(years)


def _format_postcensal_ranges() -> str:
    parts = []
    for vintage in sorted(PEP_URLS.keys()):
        year_range = _postcensal_year_range(vintage)
        if year_range:
            parts.append(f"{vintage} ({year_range[0]}-{year_range[1]})")
        else:
            parts.append(str(vintage))
    return ", ".join(parts)


def _validate_postcensal_vintage(vintage: int) -> None:
    if vintage not in PEP_URLS:
        available = _format_postcensal_ranges()
        raise ValueError(
            f"Postcensal vintage {vintage} is not available. "
            f"Available postcensal vintages: {available}."
        )


def download_pep(
    vintage: int,
    url: str | None = None,
    raw_dir_override: Path | str | None = None,
    force: bool = False,
) -> tuple[Path, str]:
    """Download raw PEP county data from Census Bureau.

    Parameters
    ----------
    vintage : int
        Data vintage year (2020 or 2024).
    url : str, optional
        Override URL for download. If None, uses default URL for vintage.
    raw_dir_override : Path or str, optional
        Override raw directory. Defaults to canonical
        ``data/raw/pep/<vintage>/``.
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
    ValueError
        If vintage is not supported.
    """
    if url is None:
        if vintage not in PEP_URLS:
            supported = ", ".join(str(v) for v in sorted(PEP_URLS.keys()))
            raise ValueError(f"Unknown vintage: {vintage}. Supported: {supported}")
        url = PEP_URLS[vintage]

    if raw_dir_override is not None:
        dest = Path(raw_dir_override)
    else:
        dest = canonical_raw_dir("pep", vintage)
    dest.mkdir(parents=True, exist_ok=True)

    # Generate filename with download date
    download_date = date.today().isoformat()
    filename = f"pep_county__v{vintage}__{download_date}.csv"
    raw_path = dest / filename

    # Check for cached file (same day)
    if raw_path.exists() and not force:
        logger.info(f"Using cached raw file: {raw_path}")
        content = raw_path.read_bytes()
        sha256 = hashlib.sha256(content).hexdigest()
        return raw_path, sha256

    # Download
    logger.info(f"Downloading PEP county data (vintage {vintage}) from {url}")
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
        source_type="pep_county",
        source_url=url,
        current_sha256=sha256,
    )

    if changed:
        logger.warning(
            f"UPSTREAM DATA CHANGED: PEP county vintage {vintage} has changed "
            "since last download!\n"
            f"    Previous hash: {details['previous_sha256'][:16]}...\n"
            f"    Current hash:  {sha256[:16]}...\n"
            f"    Last ingested: {details['previous_ingested_at']}"
        )
    elif details.get("is_new"):
        logger.info(f"First time tracking PEP county vintage {vintage} source in registry")

    # Register this download
    register_source(
        source_type="pep_county",
        source_url=url,
        source_name=f"PEP County Population Vintage {vintage}",
        raw_sha256=sha256,
        file_size=len(content),
        local_path=str(raw_path),
        metadata={
            "vintage": vintage,
            "download_date": download_date,
            "data_source": "U.S. Census Bureau",
            "program": "Population Estimates Program",
        },
    )

    return raw_path, sha256


def parse_pep_county(raw_path: Path, vintage: int) -> pd.DataFrame:
    """Parse Census Bureau PEP county CSV to long format.

    The Census Bureau provides county population estimates in wide format
    with year columns like POPESTIMATE2010, POPESTIMATE2011, etc.
    This function normalizes to long format with one row per county-year.

    Parameters
    ----------
    raw_path : Path
        Path to raw CSV file.
    vintage : int
        Data vintage year to determine which year columns to extract.

    Returns
    -------
    pd.DataFrame
        Long-format DataFrame with columns:
        county_fips, state_fips, county_name, state_name, year,
        reference_date, population
    """
    # Read CSV with state/county FIPS as strings to preserve leading zeros
    df = pd.read_csv(
        raw_path,
        dtype={"STATE": str, "COUNTY": str},
        encoding="latin-1",  # Census files often use latin-1
    )

    # Validate minimum required columns before proceeding
    required_cols = {"STATE", "COUNTY", "SUMLEV"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(
            f"PEP CSV is missing required column(s): {sorted(missing)}. "
            f"Expected at least {sorted(required_cols)} in the header."
        )

    # Filter to county rows only (SUMLEV == 50)
    # SUMLEV 40 = State, SUMLEV 50 = County
    df = df[df["SUMLEV"] == 50].copy()

    # Build 5-digit county FIPS
    df["county_fips"] = df["STATE"].str.zfill(2) + df["COUNTY"].str.zfill(3)
    df["state_fips"] = df["STATE"].str.zfill(2)

    # Get years for this vintage
    years = VINTAGE_YEARS.get(vintage, [])
    if not years:
        # Try to detect from columns
        pop_cols = [c for c in df.columns if c.startswith("POPESTIMATE")]
        years = [int(c.replace("POPESTIMATE", "")) for c in pop_cols]
        years.sort()

    # Build list of (year, column) pairs
    year_cols = []
    for year in years:
        col = f"POPESTIMATE{year}"
        if col in df.columns:
            year_cols.append((year, col))

    if not year_cols:
        raise ValueError(f"No POPESTIMATE columns found for vintage {vintage}")

    # Identify metadata columns to keep
    meta_cols = ["county_fips", "state_fips"]

    # Try various name columns
    if "CTYNAME" in df.columns:
        df["county_name"] = df["CTYNAME"]
        meta_cols.append("county_name")
    elif "COUNTY_NAME" in df.columns:
        df["county_name"] = df["COUNTY_NAME"]
        meta_cols.append("county_name")

    if "STNAME" in df.columns:
        df["state_name"] = df["STNAME"]
        meta_cols.append("state_name")
    elif "STATE_NAME" in df.columns:
        df["state_name"] = df["STATE_NAME"]
        meta_cols.append("state_name")

    # Melt to long format
    value_cols = [col for _, col in year_cols]
    long_df = df[meta_cols + value_cols].melt(
        id_vars=meta_cols,
        value_vars=value_cols,
        var_name="year_col",
        value_name="population",
    )

    # Extract year from column name
    long_df["year"] = long_df["year_col"].str.replace("POPESTIMATE", "").astype(int)

    # Add reference date (July 1 of each year)
    long_df["reference_date"] = pd.to_datetime(
        long_df["year"].astype(str) + "-07-01"
    )

    # Drop intermediate column
    long_df = long_df.drop(columns=["year_col"])

    # Sort and reset index
    long_df = long_df.sort_values(["county_fips", "year"]).reset_index(drop=True)

    # Validate FIPS length
    invalid_fips = long_df[long_df["county_fips"].str.len() != 5]
    if len(invalid_fips) > 0:
        logger.warning(f"Found {len(invalid_fips)} rows with invalid county_fips length")
        long_df = long_df[long_df["county_fips"].str.len() == 5]

    # Reject state-total rows that slipped through (COUNTY == '000')
    state_total_mask = long_df["county_fips"].str.endswith("000")
    if state_total_mask.any():
        n_bad = state_total_mask.sum()
        logger.warning(f"Dropping {n_bad} state-total rows with county code '000'")
        long_df = long_df[~state_total_mask]

    return long_df


def _format_year_range_suffix(start_year: int | None, end_year: int | None) -> str:
    if start_year is None and end_year is None:
        return ""
    start_label = str(start_year) if start_year is not None else "min"
    end_label = str(end_year) if end_year is not None else "max"
    return f"__y{start_label}-{end_label}"


def _filter_year_range(
    df: pd.DataFrame,
    start_year: int | None,
    end_year: int | None,
) -> pd.DataFrame:
    if start_year is not None:
        df = df[df["year"] >= start_year]
    if end_year is not None:
        df = df[df["year"] <= end_year]
    return df


def get_output_path(
    vintage: int | str,
    output_dir: Path | str | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
) -> Path:
    """Get canonical output path for normalized PEP data.

    Parameters
    ----------
    vintage : int or str
        Data vintage (2020, 2024).
    output_dir : Path or str, optional
        Output directory. Defaults to 'data/curated/pep'.

    Returns
    -------
    Path
        Output path like 'data/curated/pep/pep_county__v2024.parquet',
        optionally suffixed with a year filter like '__y2015-2020'.
    """
    if output_dir is None:
        output_dir = curated_dir("pep")
    else:
        output_dir = Path(output_dir)

    suffix = _format_year_range_suffix(start_year, end_year)
    return output_dir / f"pep_county__v{vintage}{suffix}.parquet"


def ingest_pep_county(
    series: Literal["auto", "postcensal"] = AUTO_SERIES,
    vintage: int | None = None,
    url: str | None = None,
    force: bool = False,
    output_dir: Path | str | None = None,
    raw_dir: Path | str | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
) -> Path:
    """Download and normalize PEP county population estimates.

    Parameters
    ----------
    series : {"auto", "postcensal"}
        Which series to ingest. Both resolve to postcensal.
    vintage : int, optional
        Postcensal vintage year (defaults to latest).
    url : str, optional
        Override download URL.
    force : bool
        Re-download and reprocess even if cached.
    output_dir : Path or str, optional
        Output directory for curated parquet. Defaults to 'data/curated/pep'.
    raw_dir : Path or str, optional
        Directory for raw downloads. Defaults to 'data/raw/pep'.
    start_year : int, optional
        First year to include (inclusive). Defaults to earliest in data.
    end_year : int, optional
        Last year to include (inclusive). Defaults to latest in data.

    Returns
    -------
    Path
        Path to output Parquet file.

    Raises
    ------
    httpx.HTTPStatusError
        If download fails.
    ValueError
        If parsing/validation fails or requested series is unavailable.
    """
    if series not in {AUTO_SERIES, POSTCENSAL_SERIES}:
        raise ValueError(
            f"Unknown series '{series}'. "
            f"Expected one of: {AUTO_SERIES}, {POSTCENSAL_SERIES}."
        )

    if start_year is not None and end_year is not None and start_year > end_year:
        raise ValueError("start_year must be <= end_year.")

    if vintage is None:
        vintage = max(PEP_URLS.keys())
    _validate_postcensal_vintage(vintage)

    output_path = get_output_path(
        vintage,
        output_dir,
        start_year=start_year,
        end_year=end_year,
    )

    # Check cache
    if output_path.exists() and not force:
        logger.info(f"Using cached file: {output_path}")
        return output_path

    # Download
    download_url = url or PEP_URLS.get(vintage)
    if download_url is None:
        _validate_postcensal_vintage(vintage)

    raw_path, sha256 = download_pep(vintage, url, raw_dir_override=raw_dir, force=force)

    # Parse
    df = parse_pep_county(raw_path, vintage)
    df = _filter_year_range(df, start_year, end_year)
    if df.empty:
        raise ValueError("No PEP data found for requested year range.")

    county_count = df["county_fips"].nunique()
    year_range = f"{df['year'].min()}-{df['year'].max()}"
    logger.info(
        f"Parsed {len(df)} PEP records for {county_count} counties, "
        f"years {year_range}"
    )

    # Add metadata columns
    ingested_at = datetime.now(UTC)
    df["vintage"] = vintage
    df["estimate_type"] = "postcensal"  # All current data is postcensal
    df["data_source"] = "census_pep"
    df["source_url"] = download_url
    df["raw_sha256"] = sha256
    df["ingested_at"] = ingested_at

    # Validate population values
    invalid_pop = df[df["population"] < 0]
    if len(invalid_pop) > 0:
        logger.warning(f"Found {len(invalid_pop)} rows with negative population")
        df = df[df["population"] >= 0]

    # Reorder columns
    col_order = [
        "county_fips",
        "state_fips",
        "county_name",
        "state_name",
        "year",
        "reference_date",
        "population",
        "estimate_type",
        "vintage",
        "data_source",
        "source_url",
        "raw_sha256",
        "ingested_at",
    ]
    # Only include columns that exist
    col_order = [c for c in col_order if c in df.columns]
    df = df[col_order]

    # Build provenance
    provenance = ProvenanceBlock(
        extra={
            "dataset": "pep_county_population",
            "series": "postcensal",
            "vintage": vintage,
            "source": "U.S. Census Bureau",
            "program": "Population Estimates Program",
            "attribution": CENSUS_ATTRIBUTION,
            "download_url": download_url,
            "downloaded_at": ingested_at.isoformat(),
            "raw_sha256": sha256,
            "row_count": len(df),
            "county_count": df["county_fips"].nunique(),
            "year_range": [int(df["year"].min()), int(df["year"].max())],
            "year_filter": {
                "start_year": start_year,
                "end_year": end_year,
            },
            "reference_date_convention": "july_1",
            "population_universe": "resident_population",
        },
    )

    # Write output
    write_parquet_with_provenance(df, output_path, provenance)
    logger.info(f"Wrote normalized PEP data to {output_path}")

    return output_path


