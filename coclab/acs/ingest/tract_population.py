"""ACS tract population data fetcher.

Fetches and caches tract-level population data (table B01003) from the Census Bureau API.

Usage
-----
    from coclab.acs.ingest.tract_population import ingest_tract_population

    # Fetch and cache tract population data
    path = ingest_tract_population(
        acs_vintage="2019-2023",
        tract_vintage="2023"
    )

Output Schema
-------------
- tract_geoid (str): Census tract GEOID (11 chars, e.g., "08031001000")
- acs_vintage (str): e.g., "2019-2023"
- tract_vintage (str): e.g., "2023"
- total_population (int): population count from B01003_001E
- data_source (str): always "acs_5yr"
- source_ref (str): dataset identifier / retrieval parameters
- ingested_at (datetime UTC): timestamp of ingestion
- moe_total_population (float/int, optional): margin of error if fetched
"""

from __future__ import annotations

import hashlib
import logging
import re
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pandas as pd

from coclab import naming
from coclab.provenance import ProvenanceBlock, write_parquet_with_provenance
from coclab.sources import CENSUS_API_ACS5
from coclab.source_registry import check_source_changed, register_source

logger = logging.getLogger(__name__)

# Census Bureau API endpoint for ACS 5-year estimates
CENSUS_API = CENSUS_API_ACS5

# ACS variables for total population
POPULATION_VARS = {
    "B01003_001E": "total_population",  # Total population estimate
    "B01003_001M": "moe_total_population",  # Margin of error
}

# US State and territory FIPS codes
STATE_FIPS_CODES = [
    "01",
    "02",
    "04",
    "05",
    "06",
    "08",
    "09",
    "10",
    "11",
    "12",
    "13",
    "15",
    "16",
    "17",
    "18",
    "19",
    "20",
    "21",
    "22",
    "23",
    "24",
    "25",
    "26",
    "27",
    "28",
    "29",
    "30",
    "31",
    "32",
    "33",
    "34",
    "35",
    "36",
    "37",
    "38",
    "39",
    "40",
    "41",
    "42",
    "44",
    "45",
    "46",
    "47",
    "48",
    "49",
    "50",
    "51",
    "53",
    "54",
    "55",
    "56",
    "72",  # Puerto Rico
]

# Default data directory
DEFAULT_DATA_DIR = Path("data/curated/acs")


def parse_acs_vintage(acs_vintage: str) -> int:
    """Parse ACS vintage string to extract the end year.

    Parameters
    ----------
    acs_vintage : str
        ACS vintage string like "2019-2023" or "2023".

    Returns
    -------
    int
        The end year of the ACS vintage period.

    Raises
    ------
    ValueError
        If the vintage string cannot be parsed.

    Examples
    --------
    >>> parse_acs_vintage("2019-2023")
    2023
    >>> parse_acs_vintage("2023")
    2023
    """
    # Handle range format like "2019-2023"
    if "-" in acs_vintage:
        match = re.match(r"^(\d{4})-(\d{4})$", acs_vintage)
        if not match:
            raise ValueError(
                f"Invalid ACS vintage format: {acs_vintage!r}. "
                f"Expected format like '2019-2023' or '2023'"
            )
        start_year, end_year = int(match.group(1)), int(match.group(2))
        if end_year - start_year != 4:
            raise ValueError(
                f"Invalid ACS vintage range: {acs_vintage!r}. "
                f"5-year estimates should span exactly 4 years (e.g., 2019-2023)"
            )
        return end_year

    # Handle single year format
    try:
        return int(acs_vintage)
    except ValueError as e:
        raise ValueError(
            f"Invalid ACS vintage format: {acs_vintage!r}. "
            f"Expected format like '2019-2023' or '2023'"
        ) from e


def normalize_geoid(state: str, county: str, tract: str) -> str:
    """Normalize Census GEOID to 11-character format.

    Parameters
    ----------
    state : str
        2-digit state FIPS code.
    county : str
        3-digit county FIPS code.
    tract : str
        6-digit census tract code.

    Returns
    -------
    str
        11-character GEOID (e.g., "08031001000").
    """
    # Ensure proper zero-padding
    state_str = str(state).zfill(2)
    county_str = str(county).zfill(3)
    tract_str = str(tract).zfill(6)
    return f"{state_str}{county_str}{tract_str}"


def fetch_state_tract_population(year: int, state_fips: str) -> tuple[pd.DataFrame, bytes]:
    """Fetch tract population data for a single state.

    Parameters
    ----------
    year : int
        ACS 5-year estimate end year (e.g., 2023 for 2019-2023 estimates).
    state_fips : str
        Two-digit state FIPS code (e.g., "06" for California).

    Returns
    -------
    tuple[pd.DataFrame, bytes]
        Tuple of (DataFrame with tract GEOID and population, raw response content).

    Raises
    ------
    httpx.HTTPStatusError
        If the Census API request fails.
    """
    url = CENSUS_API.format(year=year)
    variables = ",".join(POPULATION_VARS.keys())

    params = {
        "get": f"NAME,{variables}",
        "for": "tract:*",
        "in": f"state:{state_fips}",
    }

    with httpx.Client(timeout=60.0) as client:
        response = client.get(url, params=params)
        response.raise_for_status()
        raw_content = response.content
        data = response.json()

    # First row is headers
    headers = data[0]
    rows = data[1:]

    df = pd.DataFrame(rows, columns=headers)

    # Build GEOID from state, county, tract
    df["tract_geoid"] = df.apply(
        lambda row: normalize_geoid(row["state"], row["county"], row["tract"]), axis=1
    )

    # Convert numeric columns
    for var_code, var_name in POPULATION_VARS.items():
        if var_code in df.columns:
            df[var_name] = pd.to_numeric(df[var_code], errors="coerce")
            # Census uses negative values for missing data
            df.loc[df[var_name] < 0, var_name] = pd.NA

    # Select and rename final columns
    result_cols = ["tract_geoid", "total_population"]
    if "moe_total_population" in df.columns:
        result_cols.append("moe_total_population")

    return df[result_cols].copy(), raw_content


def fetch_tract_population(
    acs_vintage: str,
    tract_vintage: str,
) -> tuple[pd.DataFrame, str, int]:
    """Fetch tract-level population data for all US states and territories.

    Parameters
    ----------
    acs_vintage : str
        ACS vintage string like "2019-2023" representing the 5-year estimate period.
    tract_vintage : str
        Census tract geography vintage (e.g., "2023").

    Returns
    -------
    tuple[pd.DataFrame, str, int]
        Tuple of (DataFrame, SHA-256 hash, total content size).
        DataFrame with columns:
        - tract_geoid (str): 11-character Census tract GEOID
        - acs_vintage (str): ACS vintage string
        - tract_vintage (str): tract geography vintage
        - total_population (int): population count
        - data_source (str): always "acs_5yr"
        - source_ref (str): API retrieval parameters
        - ingested_at (datetime): UTC timestamp
        - moe_total_population (float, optional): margin of error

    Raises
    ------
    ValueError
        If no tract data could be fetched from any state.
    """
    year = parse_acs_vintage(acs_vintage)
    ingested_at = datetime.now(UTC)

    logger.info(f"Fetching ACS {acs_vintage} tract population data (API year: {year})")

    dfs = []
    all_raw_content = []
    for state_fips in STATE_FIPS_CODES:
        try:
            df, raw_content = fetch_state_tract_population(year, state_fips)
            dfs.append(df)
            all_raw_content.append(raw_content)
            logger.debug(f"Fetched {len(df)} tracts for state {state_fips}")
        except httpx.HTTPStatusError as e:
            logger.warning(f"Failed to fetch data for state {state_fips}: {e}")
            continue
        except Exception as e:
            logger.warning(f"Unexpected error for state {state_fips}: {e}")
            continue

    if not dfs:
        raise ValueError("No tract population data could be fetched from any state")

    # Compute SHA-256 hash of all raw content combined
    combined_content = b"".join(all_raw_content)
    content_sha256 = hashlib.sha256(combined_content).hexdigest()
    content_size = len(combined_content)

    # Combine all states
    result = pd.concat(dfs, ignore_index=True)

    # Add metadata columns
    result["acs_vintage"] = acs_vintage
    result["tract_vintage"] = tract_vintage
    result["data_source"] = "acs_5yr"
    result["source_ref"] = f"census_api/acs/acs5/{year}/B01003"
    result["ingested_at"] = ingested_at

    # Ensure proper column types
    result["tract_geoid"] = result["tract_geoid"].astype(str)
    result["total_population"] = result["total_population"].astype("Int64")

    if "moe_total_population" in result.columns:
        result["moe_total_population"] = result["moe_total_population"].astype("Float64")

    # Reorder columns to match schema
    col_order = [
        "tract_geoid",
        "acs_vintage",
        "tract_vintage",
        "total_population",
        "data_source",
        "source_ref",
        "ingested_at",
    ]
    if "moe_total_population" in result.columns:
        col_order.append("moe_total_population")

    result = result[col_order]

    logger.info(f"Fetched population data for {len(result)} tracts")
    return result, content_sha256, content_size


def get_output_path(
    acs_vintage: str,
    tract_vintage: str,
    base_dir: Path | str | None = None,
) -> Path:
    """Get the canonical output path for tract population data.

    Uses temporal shorthand naming: acs_tracts__A{year}xT{tract}.parquet

    Parameters
    ----------
    acs_vintage : str
        ACS vintage string (e.g., "2019-2023").
    tract_vintage : str
        Tract geography vintage (e.g., "2023").
    base_dir : Path or str, optional
        Base directory for output. Defaults to 'data/curated/acs'.

    Returns
    -------
    Path
        Output path like 'data/curated/acs/acs_tracts__A2023xT2023.parquet'.
    """
    if base_dir is None:
        base_dir = DEFAULT_DATA_DIR
    else:
        base_dir = Path(base_dir)
    return base_dir / naming.acs_tracts_filename(acs_vintage, tract_vintage)


def ingest_tract_population(
    acs_vintage: str,
    tract_vintage: str,
    force: bool = False,
    output_dir: Path | str | None = None,
) -> Path:
    """Fetch and cache tract population data.

    Parameters
    ----------
    acs_vintage : str
        ACS vintage string like "2019-2023" representing the 5-year estimate period.
    tract_vintage : str
        Census tract geography vintage (e.g., "2023").
    force : bool, optional
        If True, re-fetch even if cached file exists. Default is False.
    output_dir : Path or str, optional
        Output directory. Defaults to 'data/curated/acs'.

    Returns
    -------
    Path
        Path to the output Parquet file.

    Notes
    -----
    The output file includes embedded provenance metadata with dataset lineage
    information following the coclab.provenance conventions.
    """
    output_path = get_output_path(acs_vintage, tract_vintage, output_dir)

    # Check for cached file
    if output_path.exists() and not force:
        logger.info(f"Using cached file: {output_path}")
        return output_path

    # Fetch data
    df, content_sha256, content_size = fetch_tract_population(acs_vintage, tract_vintage)

    # Build source URL for registry
    year = parse_acs_vintage(acs_vintage)
    source_url = f"{CENSUS_API.format(year=year)}?table=B01003"

    # Check for upstream changes
    changed, details = check_source_changed(
        source_type="acs_tract",
        source_url=source_url,
        current_sha256=content_sha256,
    )

    if changed:
        logger.warning(
            f"UPSTREAM DATA CHANGED: ACS tract population data for {acs_vintage} has changed! "
            f"Previous hash: {details['previous_sha256'][:16]}... "
            f"Current hash: {content_sha256[:16]}... "
            f"Last ingested: {details['previous_ingested_at']}"
        )
    elif details.get("is_new"):
        logger.info(f"First time tracking ACS tract population {acs_vintage} in source registry")

    # Build provenance metadata
    provenance = ProvenanceBlock(
        acs_vintage=acs_vintage,
        tract_vintage=tract_vintage,
        extra={
            "dataset": "tract_population",
            "table": "B01003",
            "variables": ["B01003_001E", "B01003_001M"],
            "api_year": year,
            "retrieved_at": datetime.now(UTC).isoformat(),
            "row_count": len(df),
            "raw_sha256": content_sha256,
        },
    )

    # Write with provenance
    write_parquet_with_provenance(df, output_path, provenance)
    logger.info(f"Wrote tract population data to {output_path}")

    # Register this download in source registry
    register_source(
        source_type="acs_tract",
        source_url=source_url,
        source_name=f"ACS Tract Population {acs_vintage}",
        raw_sha256=content_sha256,
        file_size=content_size,
        local_path=str(output_path),
        metadata={
            "acs_vintage": acs_vintage,
            "tract_vintage": tract_vintage,
            "table": "B01003",
            "row_count": len(df),
        },
    )

    return output_path
