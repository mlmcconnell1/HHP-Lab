"""PEP (Population Estimates Program) county-level data ingestion.

Downloads and normalizes Census Bureau Population Estimates Program
county-level annual population estimates.

Data Sources
------------
- Vintage 2020 (2010-2020): Original postcensal estimates
  https://www2.census.gov/programs-surveys/popest/datasets/2010-2020/counties/totals/co-est2020-alldata.csv

- Vintage 2024 (2020-2024): Current postcensal estimates
  https://www2.census.gov/programs-surveys/popest/datasets/2020-2024/counties/totals/co-est2024-alldata.csv

Note: The 2010-2020 intercensal estimates (revised November 2024) may be added
when a combined CSV file becomes available from Census Bureau.

Usage
-----
    from coclab.pep.ingest import ingest_pep_county

    # Ingest best-available PEP county estimates (postcensal for now)
    path = ingest_pep_county()

    # Ingest postcensal vintage 2024 data (covers 2020-2024)
    path = ingest_pep_county(series="postcensal", vintage=2024)

    # Request intercensal (errors until available)
    path = ingest_pep_county(series="intercensal-2010-2020")
"""

from __future__ import annotations

import hashlib
import logging
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Literal

import httpx
import pandas as pd

from coclab.provenance import ProvenanceBlock, read_provenance, write_parquet_with_provenance
from coclab.source_registry import check_source_changed, register_source

logger = logging.getLogger(__name__)

# Census Bureau PEP download URLs by vintage (postcensal)
# Vintage 2020 = estimates through July 1, 2020 (released 2021)
# Vintage 2024 = estimates through July 1, 2024 (released 2025)
PEP_URLS = {
    2020: "https://www2.census.gov/programs-surveys/popest/datasets/2010-2020/counties/totals/co-est2020-alldata.csv",
    2024: "https://www2.census.gov/programs-surveys/popest/datasets/2020-2024/counties/totals/co-est2024-alldata.csv",
}

# Census Bureau attribution (public domain, but cite source)
CENSUS_ATTRIBUTION = "Source: U.S. Census Bureau, Population Estimates Program (PEP)"

# Default directories
DEFAULT_RAW_DIR = Path("data/raw/pep")
DEFAULT_OUTPUT_DIR = Path("data/curated/pep")

# Population columns by vintage
# Vintage 2020 file: POPESTIMATE2010 through POPESTIMATE2020
# Vintage 2024 file: POPESTIMATE2020 through POPESTIMATE2024
VINTAGE_YEARS = {
    2020: list(range(2010, 2021)),  # 2010-2020
    2024: list(range(2020, 2025)),  # 2020-2024
}

# Intercensal series metadata (not yet available as a combined county CSV)
INTERCENSAL_SERIES = "intercensal-2010-2020"
POSTCENSAL_SERIES = "postcensal"
ALL_SERIES = "all"
AUTO_SERIES = "auto"
INTERCENSAL_YEAR_RANGE = (2010, 2020)
INTERCENSAL_YEARS = list(range(2010, 2021))
INTERCENSAL_URLS: dict[int, str] = {}


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


def _intercensal_available() -> bool:
    return bool(INTERCENSAL_URLS)


def _intercensal_url() -> str | None:
    if not INTERCENSAL_URLS:
        return None
    latest_key = sorted(INTERCENSAL_URLS.keys())[-1]
    return INTERCENSAL_URLS[latest_key]


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
    raw_dir: Path | str | None = None,
    force: bool = False,
) -> tuple[Path, str]:
    """Download raw PEP county data from Census Bureau.

    Parameters
    ----------
    vintage : int
        Data vintage year (2020 or 2024).
    url : str, optional
        Override URL for download. If None, uses default URL for vintage.
    raw_dir : Path or str, optional
        Directory to save raw file. Defaults to 'data/raw/pep'.
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

    if raw_dir is None:
        raw_dir = DEFAULT_RAW_DIR
    else:
        raw_dir = Path(raw_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)

    # Generate filename with download date
    download_date = date.today().isoformat()
    filename = f"pep_county__v{vintage}__{download_date}.csv"
    raw_path = raw_dir / filename

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
            f"UPSTREAM DATA CHANGED: PEP county vintage {vintage} has changed since last download!\n"
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


def download_pep_intercensal(
    url: str,
    raw_dir: Path | str | None = None,
    force: bool = False,
) -> tuple[Path, str]:
    """Download raw intercensal PEP county data from Census Bureau."""
    if raw_dir is None:
        raw_dir = DEFAULT_RAW_DIR
    else:
        raw_dir = Path(raw_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)

    download_date = date.today().isoformat()
    filename = f"pep_county__intercensal_2010_2020__{download_date}.csv"
    raw_path = raw_dir / filename

    if raw_path.exists() and not force:
        logger.info(f"Using cached raw file: {raw_path}")
        content = raw_path.read_bytes()
        sha256 = hashlib.sha256(content).hexdigest()
        return raw_path, sha256

    logger.info("Downloading intercensal PEP county data from %s", url)
    with httpx.Client(timeout=120.0, follow_redirects=True) as client:
        response = client.get(url)
        response.raise_for_status()
        content = response.content

    sha256 = hashlib.sha256(content).hexdigest()
    raw_path.write_bytes(content)
    logger.info(f"Saved raw file to {raw_path} (sha256: {sha256[:10]}...)")

    changed, details = check_source_changed(
        source_type="pep_county",
        source_url=url,
        current_sha256=sha256,
    )

    if changed:
        logger.warning(
            "UPSTREAM DATA CHANGED: Intercensal PEP county data has changed since last download!\n"
            f"    Previous hash: {details['previous_sha256'][:16]}...\n"
            f"    Current hash:  {sha256[:16]}...\n"
            f"    Last ingested: {details['previous_ingested_at']}"
        )
    elif details.get("is_new"):
        logger.info("First time tracking intercensal PEP county source in registry")

    register_source(
        source_type="pep_county",
        source_url=url,
        source_name="PEP County Population Intercensal 2010-2020",
        raw_sha256=sha256,
        file_size=len(content),
        local_path=str(raw_path),
        metadata={
            "series": "intercensal_2010_2020",
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

    # Filter to county rows only (SUMLEV == 50)
    # SUMLEV 40 = State, SUMLEV 50 = County
    if "SUMLEV" in df.columns:
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

    # Validate
    invalid_fips = long_df[long_df["county_fips"].str.len() != 5]
    if len(invalid_fips) > 0:
        logger.warning(f"Found {len(invalid_fips)} rows with invalid county_fips length")
        long_df = long_df[long_df["county_fips"].str.len() == 5]

    return long_df


def get_output_path(
    vintage: int | str,
    output_dir: Path | str | None = None,
) -> Path:
    """Get canonical output path for normalized PEP data.

    Parameters
    ----------
    vintage : int or str
        Data vintage (2020, 2024), "combined", or "intercensal-2010-2020".
    output_dir : Path or str, optional
        Output directory. Defaults to 'data/curated/pep'.

    Returns
    -------
    Path
        Output path like 'data/curated/pep/pep_county__v2024.parquet'.
    """
    if output_dir is None:
        output_dir = DEFAULT_OUTPUT_DIR
    else:
        output_dir = Path(output_dir)

    if vintage == "combined":
        return output_dir / "pep_county__combined.parquet"
    if vintage == INTERCENSAL_SERIES:
        return output_dir / "pep_county__intercensal_2010_2020.parquet"
    return output_dir / f"pep_county__v{vintage}.parquet"


def ingest_pep_county(
    series: Literal["auto", "postcensal", "intercensal-2010-2020", "all"] = AUTO_SERIES,
    vintage: int | None = None,
    url: str | None = None,
    force: bool = False,
    output_dir: Path | str | None = None,
    raw_dir: Path | str | None = None,
    prefer_postcensal_2020: bool = False,
) -> Path:
    """Download and normalize PEP county population estimates.

    Parameters
    ----------
    series : {"auto", "postcensal", "intercensal-2010-2020", "all"}
        Which series to ingest. "auto" uses intercensal if available,
        otherwise falls back to postcensal.
    vintage : int, optional
        Postcensal vintage year (required for postcensal or all; defaults to latest).
    url : str, optional
        Override download URL (postcensal only).
    force : bool
        Re-download and reprocess even if cached.
    output_dir : Path or str, optional
        Output directory for curated parquet. Defaults to 'data/curated/pep'.
    raw_dir : Path or str, optional
        Directory for raw downloads. Defaults to 'data/raw/pep'.
    prefer_postcensal_2020 : bool
        When combining intercensal + postcensal, use postcensal for 2020.

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
    if series not in {AUTO_SERIES, POSTCENSAL_SERIES, INTERCENSAL_SERIES, ALL_SERIES}:
        raise ValueError(
            f"Unknown series '{series}'. "
            f"Expected one of: {AUTO_SERIES}, {POSTCENSAL_SERIES}, "
            f"{INTERCENSAL_SERIES}, {ALL_SERIES}."
        )

    if series == INTERCENSAL_SERIES:
        if not _intercensal_available():
            available = _format_postcensal_ranges()
            raise ValueError(
                "Intercensal PEP county estimates are not available yet for ingestion. "
                f"Available postcensal vintages: {available}."
            )
        intercensal_url = _intercensal_url()
        if intercensal_url is None:
            raise ValueError("Intercensal PEP URL is not configured.")
        output_path = get_output_path(INTERCENSAL_SERIES, output_dir)

        if output_path.exists() and not force:
            logger.info(f"Using cached file: {output_path}")
            return output_path

        raw_path, sha256 = download_pep_intercensal(intercensal_url, raw_dir, force)
        df = parse_pep_county(raw_path, vintage=INTERCENSAL_YEAR_RANGE[1])

        ingested_at = datetime.now(UTC)
        df["vintage"] = INTERCENSAL_YEAR_RANGE[1]
        df["estimate_type"] = "intercensal"
        df["data_source"] = "census_pep"
        df["source_url"] = intercensal_url
        df["raw_sha256"] = sha256
        df["ingested_at"] = ingested_at

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
        col_order = [c for c in col_order if c in df.columns]
        df = df[col_order]

        provenance = ProvenanceBlock(
            extra={
                "dataset": "pep_county_population",
                "series": "intercensal_2010_2020",
                "source": "U.S. Census Bureau",
                "program": "Population Estimates Program",
                "attribution": CENSUS_ATTRIBUTION,
                "download_url": intercensal_url,
                "downloaded_at": ingested_at.isoformat(),
                "raw_sha256": sha256,
                "row_count": len(df),
                "county_count": df["county_fips"].nunique(),
                "year_range": [int(df["year"].min()), int(df["year"].max())],
                "reference_date_convention": "july_1",
                "population_universe": "resident_population",
            },
        )

        write_parquet_with_provenance(df, output_path, provenance)
        logger.info(f"Wrote intercensal PEP data to {output_path}")
        return output_path

    if series in {ALL_SERIES, AUTO_SERIES} and _intercensal_available():
        if vintage is None:
            vintage = max(PEP_URLS.keys())
        _validate_postcensal_vintage(vintage)
        return _ingest_combined(
            vintage=vintage,
            force=force,
            output_dir=output_dir,
            raw_dir=raw_dir,
            prefer_postcensal_2020=prefer_postcensal_2020,
        )

    if series in {ALL_SERIES, AUTO_SERIES} and not _intercensal_available():
        logger.warning(
            "Intercensal PEP county estimates are not available; "
            "falling back to postcensal."
        )
        series = POSTCENSAL_SERIES

    if series == POSTCENSAL_SERIES:
        if vintage is None:
            vintage = max(PEP_URLS.keys())
        _validate_postcensal_vintage(vintage)

    output_path = get_output_path(vintage, output_dir)

    # Check cache
    if output_path.exists() and not force:
        logger.info(f"Using cached file: {output_path}")
        return output_path

    # Download
    download_url = url or PEP_URLS.get(vintage)
    if download_url is None:
        _validate_postcensal_vintage(vintage)

    raw_path, sha256 = download_pep(vintage, url, raw_dir, force)

    # Parse
    df = parse_pep_county(raw_path, vintage)

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
            "reference_date_convention": "july_1",
            "population_universe": "resident_population",
        },
    )

    # Write output
    write_parquet_with_provenance(df, output_path, provenance)
    logger.info(f"Wrote normalized PEP data to {output_path}")

    return output_path


def _combine_intercensal_postcensal(
    intercensal_df: pd.DataFrame,
    postcensal_df: pd.DataFrame,
    prefer_postcensal_2020: bool,
) -> pd.DataFrame:
    if prefer_postcensal_2020:
        intercensal_keep = intercensal_df[intercensal_df["year"] < 2020].copy()
        postcensal_keep = postcensal_df[postcensal_df["year"] >= 2020].copy()
    else:
        intercensal_keep = intercensal_df[intercensal_df["year"] <= 2020].copy()
        postcensal_keep = postcensal_df[postcensal_df["year"] >= 2021].copy()

    df = pd.concat([intercensal_keep, postcensal_keep], ignore_index=True)
    return df.sort_values(["county_fips", "year"]).reset_index(drop=True)


def _ingest_combined(
    vintage: int,
    force: bool = False,
    output_dir: Path | str | None = None,
    raw_dir: Path | str | None = None,
    prefer_postcensal_2020: bool = False,
) -> Path:
    """Ingest intercensal + postcensal and create a combined file."""
    output_path = get_output_path("combined", output_dir)

    # Check cache
    if output_path.exists() and not force:
        logger.info(f"Using cached combined file: {output_path}")
        return output_path

    # Ingest intercensal + postcensal
    logger.info("Ingesting intercensal data...")
    path_intercensal = ingest_pep_county(
        series=INTERCENSAL_SERIES,
        force=force,
        output_dir=output_dir,
        raw_dir=raw_dir,
    )

    logger.info(f"Ingesting postcensal data (vintage {vintage})...")
    path_postcensal = ingest_pep_county(
        series=POSTCENSAL_SERIES,
        vintage=vintage,
        force=force,
        output_dir=output_dir,
        raw_dir=raw_dir,
    )

    # Load both
    df_intercensal = pd.read_parquet(path_intercensal)
    df_postcensal = pd.read_parquet(path_postcensal)

    # Combine
    df = _combine_intercensal_postcensal(
        df_intercensal, df_postcensal, prefer_postcensal_2020
    )

    # Build provenance
    ingested_at = datetime.now(UTC)
    intercensal_provenance = read_provenance(path_intercensal)
    postcensal_provenance = read_provenance(path_postcensal)
    provenance = ProvenanceBlock(
        extra={
            "dataset": "pep_county_population_combined",
            "source": "U.S. Census Bureau",
            "program": "Population Estimates Program",
            "attribution": CENSUS_ATTRIBUTION,
            "series_used": {
                "intercensal": INTERCENSAL_YEAR_RANGE,
                "postcensal_vintage": vintage,
                "prefer_postcensal_2020": prefer_postcensal_2020,
            },
            "intercensal_source": intercensal_provenance.to_dict(),
            "postcensal_source": postcensal_provenance.to_dict(),
            "created_at": ingested_at.isoformat(),
            "row_count": len(df),
            "county_count": df["county_fips"].nunique(),
            "year_range": [int(df["year"].min()), int(df["year"].max())],
            "reference_date_convention": "july_1",
            "population_universe": "resident_population",
        },
    )

    # Write output
    write_parquet_with_provenance(df, output_path, provenance)
    logger.info(f"Wrote combined PEP data to {output_path}")

    return output_path
