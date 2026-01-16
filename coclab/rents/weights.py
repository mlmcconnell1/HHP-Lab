"""County-level ACS weight computation for ZORI aggregation.

This module provides functions to compute county-level weights from ACS 5-year
estimates for use in aggregating ZORI (Zillow Observed Rent Index) data from
county to CoC geography.

Supported weighting methods:
- renter_households: Renter-occupied housing units (ACS table B25003)
- housing_units: Total housing units (ACS table B25001)
- population: Total population (ACS table B01003)

Usage
-----
    from coclab.rents.weights import build_county_weights

    # Build county weights using renter households
    weights_df = build_county_weights(
        acs_vintage="2019-2023",
        method="renter_households"
    )

Output Schema
-------------
- county_fips (str): 5-character county FIPS code
- acs_vintage (str): ACS 5-year estimate vintage (e.g., "2019-2023")
- weighting_method (str): Method used (renter_households, housing_units, population)
- weight_value (int): Raw count from ACS (renter HH count, housing units, or population)
- data_source (str): always "acs_5yr"
- source_ref (str): Census API endpoint / table reference
- ingested_at (datetime UTC): timestamp of data retrieval

Notes
-----
The Census API is queried at county level directly (rather than aggregating
tract data) to minimize API calls and ensure consistency with official county
totals.
"""

from __future__ import annotations

import hashlib
import logging
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import httpx
import pandas as pd

from coclab.provenance import ProvenanceBlock, write_parquet_with_provenance
from coclab.source_registry import check_source_changed, register_source

logger = logging.getLogger(__name__)

# Census Bureau API endpoint for ACS 5-year estimates
CENSUS_API = "https://api.census.gov/data/{year}/acs/acs5"

# ACS variable mappings for weighting
# B25003_001E: Total tenure (total occupied units)
# B25003_003E: Renter-occupied units
# B25001_001E: Total housing units
# B01003_001E: Total population
ACS_WEIGHT_VARS = {
    "renter_households": {
        "table": "B25003",
        "variable": "B25003_003E",
        "description": "Renter-occupied housing units",
    },
    "housing_units": {
        "table": "B25001",
        "variable": "B25001_001E",
        "description": "Total housing units",
    },
    "population": {
        "table": "B01003",
        "variable": "B01003_001E",
        "description": "Total population",
    },
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

# Type alias for weighting methods
WeightingMethod = Literal["renter_households", "housing_units", "population"]


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
    except ValueError:
        raise ValueError(
            f"Invalid ACS vintage format: {acs_vintage!r}. "
            f"Expected format like '2019-2023' or '2023'"
        ) from None


def normalize_county_fips(state: str, county: str) -> str:
    """Normalize county FIPS to 5-character format.

    Parameters
    ----------
    state : str
        2-digit state FIPS code.
    county : str
        3-digit county FIPS code.

    Returns
    -------
    str
        5-character county FIPS code (e.g., "08031").
    """
    state_str = str(state).zfill(2)
    county_str = str(county).zfill(3)
    return f"{state_str}{county_str}"


def fetch_state_county_acs(
    year: int,
    state_fips: str,
    variable: str,
) -> tuple[pd.DataFrame, bytes]:
    """Fetch county-level ACS data for a single state.

    Parameters
    ----------
    year : int
        ACS 5-year estimate end year (e.g., 2023 for 2019-2023 estimates).
    state_fips : str
        Two-digit state FIPS code (e.g., "06" for California).
    variable : str
        ACS variable code to fetch (e.g., "B25003_003E").

    Returns
    -------
    tuple[pd.DataFrame, bytes]
        Tuple of (DataFrame with county_fips and value columns, raw response content).

    Raises
    ------
    httpx.HTTPStatusError
        If the Census API request fails.
    """
    url = CENSUS_API.format(year=year)

    params = {
        "get": f"NAME,{variable}",
        "for": "county:*",
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

    # Build county FIPS from state and county
    df["county_fips"] = df.apply(
        lambda row: normalize_county_fips(row["state"], row["county"]), axis=1
    )

    # Convert numeric column
    df["value"] = pd.to_numeric(df[variable], errors="coerce")
    # Census uses negative values for missing data
    df.loc[df["value"] < 0, "value"] = pd.NA

    return df[["county_fips", "value", "NAME"]].copy(), raw_content


def fetch_county_acs_totals(
    acs_vintage: str,
    method: WeightingMethod,
) -> tuple[pd.DataFrame, str, int]:
    """Fetch county-level ACS totals for all US states and territories.

    Parameters
    ----------
    acs_vintage : str
        ACS vintage string like "2019-2023" representing the 5-year estimate period.
    method : {"renter_households", "housing_units", "population"}
        Weighting method determining which ACS variable to fetch.

    Returns
    -------
    tuple[pd.DataFrame, str, int]
        Tuple of (DataFrame with county weights, SHA-256 hash of combined raw content,
        total content size in bytes).

        DataFrame columns:
        - county_fips (str): 5-character county FIPS code
        - acs_vintage (str): ACS vintage string
        - weighting_method (str): Method used
        - weight_value (int): Raw count from ACS
        - county_name (str): County name from Census
        - data_source (str): always "acs_5yr"
        - source_ref (str): API endpoint reference
        - ingested_at (datetime): UTC timestamp

    Raises
    ------
    ValueError
        If no county data could be fetched from any state, or if method is invalid.
    """
    if method not in ACS_WEIGHT_VARS:
        raise ValueError(
            f"Invalid weighting method: {method!r}. Valid options: {list(ACS_WEIGHT_VARS.keys())}"
        )

    year = parse_acs_vintage(acs_vintage)
    ingested_at = datetime.now(UTC)
    var_info = ACS_WEIGHT_VARS[method]
    variable = var_info["variable"]

    logger.info(
        f"Fetching ACS {acs_vintage} county {method} data (API year: {year}, variable: {variable})"
    )

    dfs = []
    all_raw_content = []
    for state_fips in STATE_FIPS_CODES:
        try:
            df, raw_content = fetch_state_county_acs(year, state_fips, variable)
            dfs.append(df)
            all_raw_content.append(raw_content)
            logger.debug(f"Fetched {len(df)} counties for state {state_fips}")
        except httpx.HTTPStatusError as e:
            logger.warning(f"Failed to fetch data for state {state_fips}: {e}")
            continue
        except Exception as e:
            logger.warning(f"Unexpected error for state {state_fips}: {e}")
            continue

    if not dfs:
        raise ValueError("No county ACS data could be fetched from any state")

    # Compute SHA-256 hash of all raw content combined
    combined_content = b"".join(all_raw_content)
    content_sha256 = hashlib.sha256(combined_content).hexdigest()
    content_size = len(combined_content)

    # Combine all states
    result = pd.concat(dfs, ignore_index=True)

    # Add metadata columns
    result["acs_vintage"] = acs_vintage
    result["weighting_method"] = method
    result["data_source"] = "acs_5yr"
    result["source_ref"] = f"census_api/acs/acs5/{year}/{var_info['table']}"
    result["ingested_at"] = ingested_at

    # Rename columns to match schema
    result = result.rename(
        columns={
            "value": "weight_value",
            "NAME": "county_name",
        }
    )

    # Ensure proper column types
    result["county_fips"] = result["county_fips"].astype(str)
    result["weight_value"] = result["weight_value"].astype("Int64")

    # Reorder columns to match schema
    col_order = [
        "county_fips",
        "acs_vintage",
        "weighting_method",
        "weight_value",
        "county_name",
        "data_source",
        "source_ref",
        "ingested_at",
    ]
    result = result[col_order]

    logger.info(f"Fetched {method} data for {len(result)} counties")
    return result, content_sha256, content_size


def get_county_weights_path(
    acs_vintage: str,
    method: WeightingMethod,
    base_dir: Path | str | None = None,
) -> Path:
    """Get the canonical output path for county weights data.

    Parameters
    ----------
    acs_vintage : str
        ACS vintage string (e.g., "2019-2023").
    method : str
        Weighting method (renter_households, housing_units, population).
    base_dir : Path or str, optional
        Base directory for output. Defaults to 'data/curated/acs'.

    Returns
    -------
    Path
        Output path like 'data/curated/acs/county_weights__2019-2023__renter_households.parquet'.
    """
    if base_dir is None:
        base_dir = DEFAULT_DATA_DIR
    else:
        base_dir = Path(base_dir)
    return base_dir / f"county_weights__{acs_vintage}__{method}.parquet"


def build_county_weights(
    acs_vintage: str,
    method: WeightingMethod,
    force: bool = False,
    output_dir: Path | str | None = None,
) -> pd.DataFrame:
    """Build county-level weights from ACS data.

    Fetches county-level ACS data for the specified variable and caches
    the result to a parquet file with provenance metadata.

    Parameters
    ----------
    acs_vintage : str
        ACS vintage string like "2019-2023" representing the 5-year estimate period.
    method : {"renter_households", "housing_units", "population"}
        Weighting method determining which ACS variable to fetch:
        - renter_households: B25003_003E (Renter-occupied housing units)
        - housing_units: B25001_001E (Total housing units)
        - population: B01003_001E (Total population)
    force : bool, optional
        If True, re-fetch even if cached file exists. Default is False.
    output_dir : Path or str, optional
        Output directory. Defaults to 'data/curated/acs'.

    Returns
    -------
    pd.DataFrame
        DataFrame with county weights. Schema:
        - county_fips (str): 5-character county FIPS code
        - acs_vintage (str): ACS vintage string
        - weighting_method (str): Method used
        - weight_value (int): Raw count from ACS
        - county_name (str): County name
        - data_source (str): always "acs_5yr"
        - source_ref (str): Census API reference
        - ingested_at (datetime): UTC timestamp

    Notes
    -----
    The output file includes embedded provenance metadata with dataset lineage
    information following the coclab.provenance conventions.

    Examples
    --------
    >>> weights = build_county_weights("2019-2023", "renter_households")
    >>> weights.head()
       county_fips  acs_vintage weighting_method  weight_value  ...
    0        01001   2019-2023  renter_households         8234  ...
    """
    output_dir = Path(output_dir) if output_dir else DEFAULT_DATA_DIR
    output_path = get_county_weights_path(acs_vintage, method, output_dir)

    # Check for cached file
    if output_path.exists() and not force:
        logger.info(f"Using cached file: {output_path}")
        return pd.read_parquet(output_path)

    # Fetch data (now returns sha256 and content size)
    df, content_sha256, content_size = fetch_county_acs_totals(acs_vintage, method)

    # Build source URL for registry
    year = parse_acs_vintage(acs_vintage)
    var_info = ACS_WEIGHT_VARS[method]
    source_url = CENSUS_API.format(year=year)

    # Check for upstream changes in the source registry
    changed, details = check_source_changed(
        source_type="acs_county",
        source_url=source_url,
        current_sha256=content_sha256,
    )

    if changed:
        logger.warning(
            f"UPSTREAM DATA CHANGED: ACS county {method} data for {acs_vintage} has changed!\n"
            f"    Previous hash: {details['previous_sha256'][:16]}...\n"
            f"    Current hash:  {content_sha256[:16]}...\n"
            f"    Last ingested: {details['previous_ingested_at']}"
        )
    elif details.get("is_new"):
        logger.info(f"First time tracking ACS county {method} source in registry")

    # Register this download in the source registry
    register_source(
        source_type="acs_county",
        source_url=source_url,
        source_name=f"ACS 5-Year County {method.replace('_', ' ').title()} ({acs_vintage})",
        raw_sha256=content_sha256,
        file_size=content_size,
        local_path=str(output_path),
        metadata={
            "acs_vintage": acs_vintage,
            "weighting_method": method,
            "table": var_info["table"],
            "variable": var_info["variable"],
            "api_year": year,
            "county_count": len(df),
        },
    )

    # Build provenance metadata
    provenance = ProvenanceBlock(
        acs_vintage=acs_vintage,
        extra={
            "dataset": "county_weights",
            "weighting_method": method,
            "table": var_info["table"],
            "variable": var_info["variable"],
            "description": var_info["description"],
            "api_year": year,
            "retrieved_at": datetime.now(UTC).isoformat(),
            "county_count": len(df),
            "total_weight": int(df["weight_value"].sum()) if len(df) > 0 else 0,
            "raw_sha256": content_sha256,
        },
    )

    # Write with provenance
    write_parquet_with_provenance(df, output_path, provenance)
    logger.info(f"Wrote county weights to {output_path}")

    return df


def load_county_weights(
    acs_vintage: str,
    method: WeightingMethod,
    base_dir: Path | str | None = None,
) -> pd.DataFrame:
    """Load cached county weights from parquet file.

    Parameters
    ----------
    acs_vintage : str
        ACS vintage string (e.g., "2019-2023").
    method : str
        Weighting method (renter_households, housing_units, population).
    base_dir : Path or str, optional
        Base directory for data. Defaults to 'data/curated/acs'.

    Returns
    -------
    pd.DataFrame
        County weights DataFrame.

    Raises
    ------
    FileNotFoundError
        If the cached file does not exist.
    """
    path = get_county_weights_path(acs_vintage, method, base_dir)
    if not path.exists():
        raise FileNotFoundError(
            f"County weights file not found: {path}. "
            f"Run build_county_weights('{acs_vintage}', '{method}') first."
        )
    return pd.read_parquet(path)
