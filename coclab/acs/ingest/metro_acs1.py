"""ACS 1-year metro-native data fetcher.

Fetches ACS 1-year unemployment data (Table B23025) from the Census Bureau
API at CBSA (metropolitan statistical area) geography, maps CBSAs to
Glynn/Fox metro IDs, and computes derived unemployment rates.

Unlike the ACS 5-year tract pipeline, ACS 1-year data is available directly
at CBSA geography -- no crosswalk or tract aggregation is needed.

Usage
-----
    from coclab.acs.ingest.metro_acs1 import ingest_metro_acs1

    path = ingest_metro_acs1(vintage=2023)

Output Schema
-------------
- metro_id (str): Glynn/Fox metro identifier (e.g., "GF01")
- metro_name (str): Metro area name
- definition_version (str): e.g., "glynn_fox_v1"
- acs1_vintage (str): e.g., "2023"
- cbsa_code (str): Census CBSA code for traceability
- pop_16_plus (Int64): Population 16 years and over (B23025_001E)
- civilian_labor_force (Int64): Civilian labor force (B23025_003E)
- unemployed_count (Int64): Unemployed civilians (B23025_005E)
- unemployment_rate_acs1 (Float64): unemployed_count / civilian_labor_force
- data_source (str): always "census_acs1"
- source_ref (str): API URL used
- ingested_at (datetime UTC)
"""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pandas as pd
import coclab.naming as naming

from coclab.acs.variables_acs1 import (
    ACS1_FIRST_RELIABLE_YEAR,
    ACS1_METRO_OUTPUT_COLUMNS,
    ACS1_TABLES,
    ACS1_UNAVAILABLE_VINTAGES,
    ACS1_UNEMPLOYMENT_VARIABLES,
    ACS1_VARIABLE_NAMES,
)
from coclab.metro.definitions import (
    cbsa_to_metro_id,
    metro_name_for_id,
)
from coclab.provenance import ProvenanceBlock, write_parquet_with_provenance
from coclab.sources import CENSUS_API_ACS1

logger = logging.getLogger(__name__)

# Census API geography parameter for CBSA-level queries
CBSA_GEO_PARAM = "metropolitan statistical area/micropolitan statistical area"


def fetch_acs1_cbsa_data(
    vintage: int,
    api_key: str | None = None,
) -> pd.DataFrame:
    """Fetch ACS 1-year B23025 data for all CBSAs from Census API.

    Makes a single request to the Census API to retrieve ACS 1-year
    unemployment data (Table B23025) for all metropolitan and
    micropolitan statistical areas.

    Parameters
    ----------
    vintage : int
        ACS 1-year vintage year (e.g., 2023).
    api_key : str, optional
        Census API key. Falls back to CENSUS_API_KEY environment variable.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns for each B23025 variable and ``cbsa_code``.

    Raises
    ------
    httpx.HTTPStatusError
        If the Census API request fails.
    ValueError
        If the API response cannot be parsed.
    """
    if vintage in ACS1_UNAVAILABLE_VINTAGES:
        raise ValueError(
            f"ACS 1-year data for vintage {vintage} is not available from Census. "
            f"Census did not publish ACS 1-year estimates for {vintage} due to "
            f"COVID-19 data collection disruptions. "
            f"For labor-market measures in {vintage}, consider BLS LAUS data "
            f"('coclab ingest laus-metro --year {vintage}') instead."
        )

    if vintage < ACS1_FIRST_RELIABLE_YEAR:
        logger.warning(
            "ACS 1-year vintage %d is before the first reliable year (%d); "
            "data may have limited coverage or reliability",
            vintage,
            ACS1_FIRST_RELIABLE_YEAR,
        )

    url = CENSUS_API_ACS1.format(year=vintage)
    variables = ",".join(ACS1_UNEMPLOYMENT_VARIABLES)

    params: dict[str, str] = {
        "get": f"NAME,{variables}",
        "for": f"{CBSA_GEO_PARAM}:*",
    }

    # Add API key if available
    if api_key is None:
        api_key = os.environ.get("CENSUS_API_KEY")
    if api_key:
        params["key"] = api_key

    logger.info(
        f"Fetching ACS 1-year {vintage} data for all CBSAs "
        f"(variables: {', '.join(ACS1_UNEMPLOYMENT_VARIABLES)})"
    )

    with httpx.Client(timeout=60.0) as client:
        response = client.get(url, params=params)
        response.raise_for_status()
        data = response.json()

    if not data or len(data) < 2:
        raise ValueError(
            f"Census API returned empty or invalid response for ACS 1-year {vintage}. "
            f"Verify that ACS 1-year data is available for vintage {vintage} at "
            f"{url}"
        )

    headers = data[0]
    rows = data[1:]

    df = pd.DataFrame(rows, columns=headers)

    # The CBSA code is in the last column
    cbsa_col = CBSA_GEO_PARAM
    if cbsa_col not in df.columns:
        # Try to find it -- Census sometimes truncates the column name
        cbsa_candidates = [c for c in df.columns if "metropolitan" in c.lower()]
        if cbsa_candidates:
            cbsa_col = cbsa_candidates[0]
        else:
            raise ValueError(
                f"Cannot find CBSA code column in Census API response. "
                f"Available columns: {list(df.columns)}. "
                f"Expected a column containing 'metropolitan'."
            )

    df = df.rename(columns={cbsa_col: "cbsa_code"})

    # Convert numeric columns; Census uses negative values for missing
    for var_code in ACS1_UNEMPLOYMENT_VARIABLES:
        if var_code in df.columns:
            df[var_code] = pd.to_numeric(df[var_code], errors="coerce")
            df.loc[df[var_code] < 0, var_code] = pd.NA

    logger.info(f"Fetched ACS 1-year data for {len(df)} CBSAs")

    return df


def ingest_metro_acs1(
    vintage: int,
    definition_version: str = "glynn_fox_v1",
    project_root: Path | None = None,
    api_key: str | None = None,
) -> Path:
    """Fetch ACS 1-year unemployment data at CBSA geography and map to metros.

    Fetches ACS 1-year Table B23025 (Employment Status) data for all CBSAs,
    maps them to Glynn/Fox metro IDs, derives unemployment rate, and writes
    a curated Parquet file with provenance metadata.

    Parameters
    ----------
    vintage : int
        ACS 1-year vintage year (e.g., 2023).
    definition_version : str
        Metro definition version (default: "glynn_fox_v1").
    project_root : Path, optional
        Project root for output path resolution. Defaults to current directory.
    api_key : str, optional
        Census API key. Falls back to CENSUS_API_KEY environment variable.

    Returns
    -------
    Path
        Path to the written Parquet file.

    Raises
    ------
    httpx.HTTPStatusError
        If the Census API request fails.
    ValueError
        If the API response cannot be parsed or no metros could be mapped.
    """
    ingested_at = datetime.now(UTC)

    # Fetch all CBSA data in one request
    df = fetch_acs1_cbsa_data(vintage, api_key=api_key)
    total_cbsas = len(df)

    # Map CBSA codes to metro IDs
    df["metro_id"] = df["cbsa_code"].apply(cbsa_to_metro_id)
    mapped = df[df["metro_id"].notna()].copy()
    dropped = total_cbsas - len(mapped)

    logger.info(
        f"CBSA-to-metro mapping: {len(mapped)} of {total_cbsas} CBSAs mapped "
        f"to Glynn/Fox metros ({dropped} CBSAs dropped)"
    )

    if mapped.empty:
        raise ValueError(
            f"No CBSAs from the ACS 1-year {vintage} response could be mapped "
            f"to Glynn/Fox metros. Check that METRO_CBSA_MAPPING in "
            f"coclab.metro.definitions is correct."
        )

    # Add metro name
    mapped["metro_name"] = mapped["metro_id"].apply(metro_name_for_id)

    # Rename raw Census variables to friendly names
    mapped = mapped.rename(columns=ACS1_VARIABLE_NAMES)

    # Compute derived unemployment rate
    # unemployment_rate_acs1 = B23025_005E / B23025_003E
    mapped["unemployment_rate_acs1"] = pd.NA
    valid_denom = (
        mapped["civilian_labor_force"].notna()
        & (mapped["civilian_labor_force"] > 0)
    )
    mapped.loc[valid_denom, "unemployment_rate_acs1"] = (
        mapped.loc[valid_denom, "unemployed_count"]
        / mapped.loc[valid_denom, "civilian_labor_force"]
    )

    # Add provenance columns
    api_url = CENSUS_API_ACS1.format(year=vintage)
    mapped["data_source"] = "census_acs1"
    mapped["source_ref"] = f"{api_url}?tables={'+'.join(ACS1_TABLES)}"
    mapped["ingested_at"] = ingested_at
    mapped["acs1_vintage"] = str(vintage)
    mapped["definition_version"] = definition_version

    # Ensure proper column types
    mapped["metro_id"] = mapped["metro_id"].astype(str)
    mapped["cbsa_code"] = mapped["cbsa_code"].astype(str)

    int_cols = ["pop_16_plus", "civilian_labor_force", "unemployed_count"]
    for col in int_cols:
        if col in mapped.columns:
            mapped[col] = mapped[col].astype("Int64")

    float_cols = ["unemployment_rate_acs1"]
    for col in float_cols:
        if col in mapped.columns:
            mapped[col] = mapped[col].astype("Float64")

    # Reorder columns to canonical order
    col_order = [c for c in ACS1_METRO_OUTPUT_COLUMNS if c in mapped.columns]
    result = mapped[col_order].copy()

    # Sort by metro_id for deterministic output
    result = result.sort_values("metro_id").reset_index(drop=True)

    # Write output
    base_dir = Path("data") if project_root is None else project_root / "data"
    output_path = naming.acs1_metro_path(vintage, definition_version, base_dir=base_dir)

    provenance = ProvenanceBlock(
        acs_vintage=str(vintage),
        geo_type="metro",
        definition_version=definition_version,
        extra={
            "dataset_type": "metro_acs1_unemployment",
            "acs_product": "acs1",
            "tables": ACS1_TABLES,
            "variables": ACS1_UNEMPLOYMENT_VARIABLES,
            "api_year": vintage,
            "retrieved_at": ingested_at.isoformat(),
            "row_count": len(result),
            "total_cbsas_fetched": total_cbsas,
            "cbsas_mapped": len(result),
            "cbsas_dropped": dropped,
            "cbsa_mapping_version": definition_version,
        },
    )

    write_parquet_with_provenance(result, output_path, provenance)
    logger.info(
        f"Wrote ACS 1-year metro unemployment data to {output_path} "
        f"({len(result)} metros)"
    )

    return output_path
