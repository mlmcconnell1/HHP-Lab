"""Shared ACS 1-year Census API fetch and normalization helpers."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass

import httpx
import pandas as pd

from hhplab.acs.variables_acs1 import (
    ACS1_FIRST_RELIABLE_YEAR,
    ACS1_FLOAT_COLUMNS,
    ACS1_INTEGER_COLUMNS,
    ACS1_UNAVAILABLE_VINTAGES,
    ACS1_VARIABLE_NAMES,
    ACS1_VARIABLES_BY_TABLE,
    acs1_tables_for_vintage,
    acs1_unavailable_tables_for_vintage,
)
from hhplab.sources import CENSUS_API_ACS1

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Acs1GeographySpec:
    """Census API geography request and output-column mapping."""

    label: str
    request_params: dict[str, str]
    response_columns: dict[str, str]


CBSA_GEO_PARAM = "metropolitan statistical area/micropolitan statistical area"

ACS1_CBSA_GEOGRAPHY = Acs1GeographySpec(
    label="CBSAs",
    request_params={"for": f"{CBSA_GEO_PARAM}:*"},
    response_columns={CBSA_GEO_PARAM: "cbsa_code"},
)

ACS1_COUNTY_GEOGRAPHY = Acs1GeographySpec(
    label="counties",
    request_params={"for": "county:*", "in": "state:*"},
    response_columns={"state": "state", "county": "county"},
)


def _validate_acs1_vintage(vintage: int) -> None:
    if vintage in ACS1_UNAVAILABLE_VINTAGES:
        raise ValueError(
            f"ACS 1-year data for vintage {vintage} is not available from Census. "
            f"Census did not publish ACS 1-year estimates for {vintage} due to "
            f"COVID-19 data collection disruptions. "
            f"For labor-market measures in {vintage}, consider BLS LAUS data "
            f"('hhplab ingest laus-metro --year {vintage}') instead."
        )

    if vintage < ACS1_FIRST_RELIABLE_YEAR:
        logger.warning(
            "ACS 1-year vintage %d is before the first reliable year (%d); "
            "data may have limited coverage or reliability",
            vintage,
            ACS1_FIRST_RELIABLE_YEAR,
        )


def _rename_geography_columns(
    table_df: pd.DataFrame,
    spec: Acs1GeographySpec,
    table: str,
) -> pd.DataFrame:
    rename_map: dict[str, str] = {}
    for source, target in spec.response_columns.items():
        if source not in table_df.columns:
            raise ValueError(
                f"Cannot find ACS1 {spec.label} geography column {source!r} in "
                f"Census API response for table {table}. Available columns: "
                f"{list(table_df.columns)}."
            )
        rename_map[source] = target
    return table_df.rename(columns=rename_map)


def fetch_acs1_api_data(
    vintage: int,
    geography: Acs1GeographySpec,
    api_key: str | None = None,
) -> pd.DataFrame:
    """Fetch ACS 1-year detailed-table data for a Census API geography."""
    _validate_acs1_vintage(vintage)

    available_tables = acs1_tables_for_vintage(vintage)
    unavailable_tables = acs1_unavailable_tables_for_vintage(vintage)

    if api_key is None:
        api_key = os.environ.get("CENSUS_API_KEY")
    frames: list[pd.DataFrame] = []
    url = CENSUS_API_ACS1.format(year=vintage)

    logger.info(
        "Fetching ACS 1-year %d data for all %s across %d tables",
        vintage,
        geography.label,
        len(available_tables),
    )
    if unavailable_tables:
        logger.info(
            "Skipping ACS1 tables unavailable for vintage %d: %s",
            vintage,
            ", ".join(unavailable_tables),
        )

    geography_columns = list(geography.response_columns.values())
    with httpx.Client(timeout=60.0) as client:
        for table in available_tables:
            table_variables = ACS1_VARIABLES_BY_TABLE[table]
            params: dict[str, str] = {
                "get": f"NAME,{','.join(table_variables)}",
                **geography.request_params,
            }
            if api_key:
                params["key"] = api_key

            response = client.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            if not data or len(data) < 2:
                raise ValueError(
                    f"Census API returned empty or invalid response for ACS 1-year "
                    f"{vintage} table {table}. Verify that the table is available at "
                    f"{url}"
                )

            headers = data[0]
            rows = data[1:]
            table_df = pd.DataFrame(rows, columns=headers)
            table_df = _rename_geography_columns(table_df, geography, table)

            for var_code in table_variables:
                if var_code in table_df.columns:
                    table_df[var_code] = pd.to_numeric(
                        table_df[var_code],
                        errors="coerce",
                    )
                    table_df.loc[table_df[var_code] < 0, var_code] = pd.NA

            keep_columns = ["NAME", *geography_columns, *table_variables]
            frames.append(table_df[keep_columns].copy())

    if not frames:
        raise ValueError(f"No ACS 1-year tables were available to fetch for vintage {vintage}.")

    merge_columns = ["NAME", *geography_columns]
    merged = frames[0]
    for frame in frames[1:]:
        merged = merged.merge(frame, on=merge_columns, how="inner")

    logger.info("Fetched ACS 1-year data for %d %s", len(merged), geography.label)
    merged.attrs["acs1_tables_fetched"] = available_tables
    merged.attrs["acs1_tables_unavailable"] = unavailable_tables
    return merged


def normalize_acs1_measures(df: pd.DataFrame) -> pd.DataFrame:
    """Rename Census variables, derive rates, and enforce stable ACS1 dtypes."""
    result = df.rename(columns=ACS1_VARIABLE_NAMES).copy()

    result["unemployment_rate_acs1"] = pd.NA
    valid_denom = (
        result["civilian_labor_force"].notna()
        & (result["civilian_labor_force"] > 0)
    )
    result.loc[valid_denom, "unemployment_rate_acs1"] = (
        result.loc[valid_denom, "unemployed_count"]
        / result.loc[valid_denom, "civilian_labor_force"]
    )

    expected_measure_columns = list(dict.fromkeys(ACS1_INTEGER_COLUMNS + ACS1_FLOAT_COLUMNS))
    missing_measure_columns = [
        col for col in expected_measure_columns if col not in result.columns
    ]
    result = result.reindex(
        columns=[*result.columns, *missing_measure_columns],
    )

    for col in ACS1_INTEGER_COLUMNS:
        if col in result.columns:
            result[col] = result[col].astype("Int64")

    for col in ACS1_FLOAT_COLUMNS:
        if col in result.columns:
            result[col] = result[col].astype("Float64")

    return result
