"""ACS 1-year county-native data fetcher."""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

import hhplab.naming as naming
from hhplab.acs.ingest._acs1_api import (
    ACS1_COUNTY_GEOGRAPHY,
    fetch_acs1_api_data,
    normalize_acs1_measures,
)
from hhplab.acs.variables_acs1 import (
    ACS1_COUNTY_OUTPUT_COLUMNS,
    ACS1_SAE_SOURCE_COLUMNS,
    ACS1_SAE_SOURCE_COLUMNS_BY_TABLE,
    ACS1_SAE_SOURCE_OUTPUT_COLUMNS,
    ACS1_TABLES,
    ACS1_UNAVAILABLE_VINTAGES,
    ACS1_VARIABLES_BY_TABLE,
    acs1_tables_for_vintage,
    acs1_unavailable_tables_for_vintage,
)
from hhplab.provenance import ProvenanceBlock, write_parquet_with_provenance
from hhplab.sources import CENSUS_API_ACS1

logger = logging.getLogger(__name__)


def fetch_acs1_county_data(
    vintage: int,
    api_key: str | None = None,
) -> pd.DataFrame:
    """Fetch ACS 1-year detailed-table data for all published counties."""
    return fetch_acs1_api_data(vintage, ACS1_COUNTY_GEOGRAPHY, api_key=api_key)


def _resolve_acs1_vintage(df: pd.DataFrame, acs1_vintage: int | str | None) -> str:
    if acs1_vintage is not None:
        vintage = str(acs1_vintage)
    elif "acs1_vintage" in df.columns and df["acs1_vintage"].notna().any():
        vintages = set(df["acs1_vintage"].dropna().astype(str))
        if len(vintages) != 1:
            raise ValueError(
                "ACS1 county SAE source normalization requires one acs1_vintage. "
                f"Found vintages: {sorted(vintages)}."
            )
        vintage = vintages.pop()
    else:
        raise ValueError(
            "ACS1 county SAE source normalization requires acs1_vintage either "
            "as an argument or as an input column."
        )

    if int(vintage) in ACS1_UNAVAILABLE_VINTAGES:
        raise ValueError(
            f"ACS 1-year county SAE source data is unavailable for vintage {vintage}. "
            "Census did not publish standard ACS 1-year estimates for 2020; "
            "choose a different ACS1 vintage or add an explicit recipe fallback."
        )
    return vintage


def _normalize_county_fips(df: pd.DataFrame) -> pd.Series:
    if "county_fips" in df.columns:
        return df["county_fips"].astype("string").str.zfill(5)
    if {"state", "county"}.issubset(df.columns):
        state = df["state"].astype("string").str.zfill(2)
        county = df["county"].astype("string").str.zfill(3)
        return state + county
    raise ValueError(
        "ACS1 county SAE source normalization requires county_fips or both "
        "state and county columns."
    )


def _available_source_tables_for_vintage(
    vintage: str,
    unavailable_tables: Iterable[str],
) -> list[str]:
    available_tables = set(acs1_tables_for_vintage(int(vintage)))
    unavailable_table_set = set(unavailable_tables)
    return [
        table
        for table in ACS1_SAE_SOURCE_COLUMNS_BY_TABLE
        if table in available_tables and table not in unavailable_table_set
    ]


def normalize_acs1_county_sae_source(
    df: pd.DataFrame,
    *,
    acs1_vintage: int | str | None = None,
    unavailable_tables: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Normalize ACS1 county aggregates into the SAE source contract.

    The output is a county-wide component frame for allocation steps. Source
    components are count-like nullable integers; unsupported or unavailable
    source columns are present as ``NA`` to keep the contract stable.
    """
    vintage = _resolve_acs1_vintage(df, acs1_vintage)
    if any(column.startswith("B") and column.endswith("E") for column in df.columns):
        raw = df.copy()
        for variable_codes in ACS1_VARIABLES_BY_TABLE.values():
            for column in variable_codes:
                if column in raw.columns:
                    raw[column] = pd.to_numeric(raw[column], errors="coerce")
                    raw.loc[raw[column] < 0, column] = pd.NA
        result = normalize_acs1_measures(raw)
    else:
        result = df.copy()

    normalized = pd.DataFrame(
        {
            "county_fips": _normalize_county_fips(result),
            "acs1_vintage": vintage,
        }
    )

    for column in ACS1_SAE_SOURCE_COLUMNS:
        if column in result.columns:
            normalized[column] = pd.to_numeric(result[column], errors="coerce")
            normalized.loc[normalized[column] < 0, column] = pd.NA
        else:
            normalized[column] = pd.NA
        normalized[column] = normalized[column].astype("Int64")

    if unavailable_tables is None:
        unavailable_table_list = acs1_unavailable_tables_for_vintage(int(vintage))
    else:
        unavailable_table_list = sorted(set(unavailable_tables))
    source_tables = _available_source_tables_for_vintage(vintage, unavailable_table_list)
    column_table_map = {
        column: table
        for table, columns in ACS1_SAE_SOURCE_COLUMNS_BY_TABLE.items()
        for column in columns
    }

    normalized["sae_source_tables"] = json.dumps(source_tables, sort_keys=True)
    normalized["sae_unavailable_tables"] = json.dumps(unavailable_table_list, sort_keys=True)
    normalized["sae_source_column_tables"] = json.dumps(column_table_map, sort_keys=True)

    normalized = normalized[ACS1_SAE_SOURCE_OUTPUT_COLUMNS].copy()
    normalized = normalized.sort_values("county_fips").reset_index(drop=True)
    normalized.attrs["acs1_vintage"] = vintage
    normalized.attrs["sae_source_tables"] = source_tables
    normalized.attrs["sae_unavailable_tables"] = unavailable_table_list
    normalized.attrs["sae_source_column_tables"] = column_table_map
    return normalized


def load_acs1_county_sae_source(
    path: str | Path | None = None,
    *,
    vintage: int | str | None = None,
    project_root: Path | None = None,
) -> pd.DataFrame:
    """Load a curated ACS1 county artifact as normalized SAE source aggregates."""
    if path is None:
        if vintage is None:
            raise ValueError("load_acs1_county_sae_source requires path or vintage.")
        base_dir = Path("data") if project_root is None else project_root / "data"
        path = naming.acs1_county_path(int(vintage), base_dir=base_dir)

    df = pd.read_parquet(path)
    return normalize_acs1_county_sae_source(df, acs1_vintage=vintage)


def ingest_county_acs1(
    vintage: int,
    project_root: Path | None = None,
    api_key: str | None = None,
) -> Path:
    """Fetch ACS 1-year detailed-table data at county geography."""
    ingested_at = datetime.now(UTC)

    df = fetch_acs1_county_data(vintage, api_key=api_key)
    fetched_tables = df.attrs.get("acs1_tables_fetched", ACS1_TABLES)
    unavailable_tables = df.attrs.get("acs1_tables_unavailable", [])

    result = normalize_acs1_measures(df)
    result["state"] = result["state"].astype(str).str.zfill(2)
    result["county"] = result["county"].astype(str).str.zfill(3)
    result["county_fips"] = result["state"] + result["county"]
    result["geo_id"] = result["county_fips"]
    result["county_name"] = result["NAME"].astype("string")

    api_url = CENSUS_API_ACS1.format(year=vintage)
    result["data_source"] = "census_acs1"
    result["source_ref"] = f"{api_url}?tables={'+'.join(fetched_tables)}"
    result["ingested_at"] = ingested_at
    result["acs1_vintage"] = str(vintage)

    col_order = [c for c in ACS1_COUNTY_OUTPUT_COLUMNS if c in result.columns]
    result = result[col_order].copy()
    result = result.sort_values("county_fips").reset_index(drop=True)

    base_dir = Path("data") if project_root is None else project_root / "data"
    output_path = naming.acs1_county_path(vintage, base_dir=base_dir)

    provenance = ProvenanceBlock(
        acs_vintage=str(vintage),
        geo_type="county",
        definition_version=None,
        extra={
            "dataset_type": "county_acs1",
            "acs_product": "acs1",
            "tables_requested": ACS1_TABLES,
            "tables_fetched": fetched_tables,
            "tables_unavailable_for_vintage": unavailable_tables,
            "variables": [
                variable_code
                for table in fetched_tables
                for variable_code in ACS1_VARIABLES_BY_TABLE[table]
            ],
            "api_year": vintage,
            "retrieved_at": ingested_at.isoformat(),
            "row_count": len(result),
            "counties_fetched": len(result),
        },
    )

    write_parquet_with_provenance(result, output_path, provenance)
    logger.info("Wrote ACS 1-year county data to %s (%d counties)", output_path, len(result))
    return output_path
