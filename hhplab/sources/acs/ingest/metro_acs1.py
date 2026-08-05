"""ACS 1-year metro-native data fetcher.

Fetches ACS 1-year detailed-table data from the Census Bureau API at CBSA
(metropolitan statistical area) geography, joins onto the canonical metro
universe or an explicit subset profile, and computes derived unemployment
rates.

Unlike the ACS 5-year tract pipeline, ACS 1-year data is available directly at
CBSA geography -- no crosswalk or tract aggregation is needed.

Usage
-----
    from hhplab.sources.acs.ingest.metro_acs1 import ingest_metro_acs1

    path = ingest_metro_acs1(vintage=2023)

Output Schema
-------------
- metro_id (str): canonical CBSA code for the full universe, or profile metro
  identifier for subset outputs (for example "35620" or "GF01")
- metro_name (str): Metro area name
- definition_version (str): e.g., "census_msa_2023" or "glynn_fox_v1"
- acs1_vintage (str): e.g., "2023"
- cbsa_code (str): Census CBSA code for traceability
- pop_16_plus (Int64): Population 16 years and over (B23025_001E)
- civilian_labor_force (Int64): Civilian labor force (B23025_003E)
- unemployed_count (Int64): Unemployed civilians (B23025_005E)
- unemployment_rate_acs1 (Float64): unemployed_count / civilian_labor_force
- additional ACS1 income, housing-cost, utility, tenure, and housing-stock
  measures from the requested detailed tables
- data_source (str): always "census_acs1"
- source_ref (str): API URL used
- ingested_at (datetime UTC)
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

import hhplab.naming as naming
from hhplab.sources.acs.ingest._acs1_api import (
    ACS1_CBSA_GEOGRAPHY,
    fetch_acs1_api_data,
    normalize_acs1_measures,
)
from hhplab.sources.acs.ingest._acs1_api import (
    CBSA_GEO_PARAM as _CBSA_GEO_PARAM,
)
from hhplab.sources.acs.variables_acs1 import (
    ACS1_METRO_OUTPUT_COLUMNS,
    ACS1_TABLES,
    acs1_variables_by_table_for_vintage,
)
from hhplab.metro.metro_definitions import (
    CANONICAL_UNIVERSE_DEFINITION_VERSION,
    METRO_CBSA_MAPPING,
    build_cbsa_alias_df,
    canonicalize_cbsa_code,
    metro_name_for_id,
)
from hhplab.metro.metro_definitions import (
    DEFINITION_VERSION as GLYNN_FOX_DEFINITION_VERSION,
)
from hhplab.metro.metro_io import read_metro_subset_membership, read_metro_universe
from hhplab.provenance import ProvenanceBlock, write_parquet_with_provenance
from hhplab.source_urls import CENSUS_API_ACS1

logger = logging.getLogger(__name__)

# Backward-compatible public constant used by tests and external callers.
CBSA_GEO_PARAM = _CBSA_GEO_PARAM

def _legacy_glynn_fox_targets() -> pd.DataFrame:
    rows = [
        {
            "metro_id": metro_id,
            "cbsa_code": cbsa_code,
            "metro_name": metro_name_for_id(metro_id),
        }
        for metro_id, cbsa_code in sorted(METRO_CBSA_MAPPING.items())
    ]
    return pd.DataFrame(rows)


def _load_metro_targets(
    definition_version: str,
    base_dir: Path | None,
) -> pd.DataFrame:
    if definition_version == CANONICAL_UNIVERSE_DEFINITION_VERSION:
        return read_metro_universe(definition_version, base_dir)[
            ["metro_id", "cbsa_code", "metro_name"]
        ].copy()

    if definition_version == GLYNN_FOX_DEFINITION_VERSION:
        try:
            subset_df = read_metro_subset_membership(
                profile_definition_version=definition_version,
                metro_definition_version=CANONICAL_UNIVERSE_DEFINITION_VERSION,
                base_dir=base_dir,
            )
            return subset_df[
                ["profile_metro_id", "cbsa_code", "profile_metro_name"]
            ].rename(
                columns={
                    "profile_metro_id": "metro_id",
                    "profile_metro_name": "metro_name",
                }
            )[["metro_id", "cbsa_code", "metro_name"]].copy()
        except FileNotFoundError:
            return _legacy_glynn_fox_targets()

    try:
        return read_metro_universe(definition_version, base_dir)[
            ["metro_id", "cbsa_code", "metro_name"]
        ].copy()
    except FileNotFoundError:
        subset_df = read_metro_subset_membership(
            profile_definition_version=definition_version,
            metro_definition_version=CANONICAL_UNIVERSE_DEFINITION_VERSION,
            base_dir=base_dir,
        )
        return subset_df[
            ["profile_metro_id", "cbsa_code", "profile_metro_name"]
        ].rename(
            columns={
                "profile_metro_id": "metro_id",
                "profile_metro_name": "metro_name",
            }
        )[["metro_id", "cbsa_code", "metro_name"]].copy()


def fetch_acs1_cbsa_data(
    vintage: int,
    api_key: str | None = None,
) -> pd.DataFrame:
    """Fetch ACS 1-year detailed-table data for all CBSAs from Census API.

    Makes one Census API request per requested ACS table, then merges the
    responses on CBSA geography.

    Parameters
    ----------
    vintage : int
        ACS 1-year vintage year (e.g., 2023).
    api_key : str, optional
        Census API key. Falls back to CENSUS_API_KEY environment variable.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns for each fetched ACS1 variable and ``cbsa_code``.

    Raises
    ------
    httpx.HTTPStatusError
        If the Census API request fails.
    ValueError
        If the API response cannot be parsed.
    """
    return fetch_acs1_api_data(vintage, ACS1_CBSA_GEOGRAPHY, api_key=api_key)


def ingest_metro_acs1(
    vintage: int,
    definition_version: str = GLYNN_FOX_DEFINITION_VERSION,
    project_root: Path | None = None,
    api_key: str | None = None,
) -> Path:
    """Fetch ACS 1-year detailed-table data at CBSA geography and map to metros.

    Fetches requested ACS 1-year detailed tables for all CBSAs, joins them to
    the canonical metro universe or an explicit subset profile, derives
    unemployment rate, and writes a curated Parquet file with provenance
    metadata.

    Parameters
    ----------
    vintage : int
        ACS 1-year vintage year (e.g., 2023).
    definition_version : str
        Metro definition version (default: legacy Glynn/Fox subset profile).
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

    # Fetch all CBSA data across the requested ACS1 tables
    df = fetch_acs1_cbsa_data(vintage, api_key=api_key)
    total_cbsas = len(df)
    fetched_tables = df.attrs.get("acs1_tables_fetched", ACS1_TABLES)
    unavailable_tables = df.attrs.get("acs1_tables_unavailable", [])

    base_dir = Path("data") if project_root is None else project_root / "data"
    target_df = _load_metro_targets(definition_version, base_dir)
    target_df["metro_id"] = target_df["metro_id"].astype(str)
    target_df["cbsa_code"] = target_df["cbsa_code"].astype(str).str.zfill(5)

    # Normalize historical CBSA aliases before joining to the requested
    # canonical universe or subset profile.
    df["source_cbsa_code"] = df["cbsa_code"].astype(str).str.zfill(5)
    df["cbsa_code"] = df["source_cbsa_code"].map(
        lambda code: canonicalize_cbsa_code(code, year=vintage)
    )
    mapped = df.merge(target_df, on="cbsa_code", how="inner")
    dropped = total_cbsas - len(mapped)

    logger.info(
        "CBSA-to-metro mapping: %d of %d CBSAs mapped to definition %s (%d CBSAs dropped)",
        len(mapped),
        total_cbsas,
        definition_version,
        dropped,
    )

    if mapped.empty:
        raise ValueError(
            f"No CBSAs from the ACS 1-year {vintage} response could be mapped "
            f"to metro definition {definition_version!r}. "
            "Verify that the canonical metro-universe artifacts exist "
            "or that the requested definition version is correct."
        )

    variables_by_table = df.attrs.get(
        "acs1_variables_by_table",
        acs1_variables_by_table_for_vintage(vintage),
    )
    mapped = normalize_acs1_measures(mapped, vintage=vintage)

    # Add provenance columns.
    api_url = CENSUS_API_ACS1.format(year=vintage)
    mapped["data_source"] = "census_acs1"
    mapped["source_ref"] = f"{api_url}?tables={'+'.join(fetched_tables)}"
    mapped["ingested_at"] = ingested_at
    mapped["acs1_vintage"] = str(vintage)
    mapped["definition_version"] = definition_version

    # Ensure proper column types.
    mapped["metro_id"] = mapped["metro_id"].astype(str)
    mapped["cbsa_code"] = mapped["cbsa_code"].astype(str)

    # Reorder columns to canonical order
    col_order = [c for c in ACS1_METRO_OUTPUT_COLUMNS if c in mapped.columns]
    result = mapped[col_order].copy()

    # Sort by metro_id for deterministic output
    result = result.sort_values("metro_id").reset_index(drop=True)

    # Write output
    output_path = naming.acs1_metro_path(vintage, definition_version, base_dir=base_dir)
    alias_rules = build_cbsa_alias_df()
    active_alias_rules = alias_rules[
        (alias_rules["start_year"] <= vintage) & (alias_rules["end_year"] >= vintage)
    ]
    alias_hits = int((mapped["source_cbsa_code"] != mapped["cbsa_code"]).sum())

    provenance = ProvenanceBlock(
        acs_vintage=str(vintage),
        geo_type="metro",
        definition_version=definition_version,
        extra={
            "dataset_type": "metro_acs1",
            "acs_product": "acs1",
            "tables_requested": ACS1_TABLES,
            "tables_fetched": fetched_tables,
            "tables_unavailable_for_vintage": unavailable_tables,
            "variables": [
                variable_code
                for table in fetched_tables
                for variable_code in variables_by_table[table]
            ],
            "api_year": vintage,
            "retrieved_at": ingested_at.isoformat(),
            "row_count": len(result),
            "total_cbsas_fetched": total_cbsas,
            "cbsas_mapped": len(result),
            "cbsas_dropped": dropped,
            "cbsa_mapping_version": definition_version,
            "cbsa_alias_hits": alias_hits,
            "cbsa_alias_rules_applied": active_alias_rules.to_dict(orient="records"),
        },
    )

    write_parquet_with_provenance(result, output_path, provenance)
    logger.info(
        "Wrote ACS 1-year metro data to %s (%d metros)",
        output_path,
        len(result),
    )

    return output_path
