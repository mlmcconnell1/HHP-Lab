"""Small-area estimation allocation helpers for ACS components."""

from __future__ import annotations

import json
from collections.abc import Iterable

import pandas as pd

from hhplab.acs.variables import ACS5_SAE_SUPPORT_COLUMNS
from hhplab.acs.variables_acs1 import ACS1_SAE_SOURCE_COLUMNS

SAE_ALLOCATION_METHOD = "tract_share_within_county"


def _json_list(values: Iterable[str]) -> str:
    return json.dumps(sorted(set(values)))


def _json_dict(values: dict[str, float | None]) -> str:
    return json.dumps(values, sort_keys=True)


def _component_columns(component_columns: Iterable[str] | None) -> list[str]:
    if component_columns is not None:
        return list(component_columns)
    support_columns = set(ACS5_SAE_SUPPORT_COLUMNS)
    return [column for column in ACS1_SAE_SOURCE_COLUMNS if column in support_columns]


def _require_columns(df: pd.DataFrame, columns: Iterable[str], label: str) -> None:
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise ValueError(f"{label} is missing required columns: {missing}.")


def _normalize_county_key(series: pd.Series) -> pd.Series:
    return series.astype("string").str.zfill(5)


def _diagnostic_lists(
    support: pd.DataFrame,
    component_columns: list[str],
) -> tuple[dict[int, list[str]], dict[int, list[str]]]:
    missing_by_index: dict[int, list[str]] = {int(index): [] for index in support.index}
    partial_by_index: dict[int, list[str]] = {int(index): [] for index in support.index}

    for column in component_columns:
        missing_mask = support[column].isna()
        for index in support.index[missing_mask]:
            missing_by_index[int(index)].append(column)

        coverage = support.groupby("county_fips", dropna=False)[column].agg(
            missing_count=lambda s: int(s.isna().sum()),
            nonmissing_count=lambda s: int(s.notna().sum()),
        )
        partial_counties = set(
            coverage[
                (coverage["missing_count"] > 0)
                & (coverage["nonmissing_count"] > 0)
            ].index
        )
        partial_mask = support["county_fips"].isin(partial_counties)
        for index in support.index[partial_mask]:
            partial_by_index[int(index)].append(column)

    return missing_by_index, partial_by_index


def allocate_acs1_county_to_tracts(
    county_source: pd.DataFrame,
    tract_support: pd.DataFrame,
    *,
    component_columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Allocate ACS1 county components to ACS5 tracts using within-county shares."""
    components = _component_columns(component_columns)
    if not components:
        raise ValueError("No SAE component columns were requested for allocation.")

    _require_columns(county_source, ["county_fips", "acs1_vintage", *components], "county_source")
    _require_columns(
        tract_support,
        ["tract_geoid", "county_fips", "acs_vintage", "tract_vintage", *components],
        "tract_support",
    )

    source = county_source.copy()
    support = tract_support.copy()
    source["county_fips"] = _normalize_county_key(source["county_fips"])
    support["county_fips"] = _normalize_county_key(support["county_fips"])
    support["tract_geoid"] = support["tract_geoid"].astype("string").str.zfill(11)

    duplicated_source = source["county_fips"].duplicated(keep=False)
    if duplicated_source.any():
        counties = sorted(source.loc[duplicated_source, "county_fips"].astype(str).unique())
        raise ValueError(
            "county_source must have one row per county_fips. "
            f"Duplicates: {counties}."
        )

    source_counties = set(source["county_fips"].dropna().astype(str))
    support_counties = set(support["county_fips"].dropna().astype(str))
    missing_support_counties = sorted(source_counties - support_counties)
    missing_source_counties = sorted(support_counties - source_counties)

    source = source.set_index("county_fips", drop=True)
    for column in components:
        source[column] = pd.to_numeric(source[column], errors="coerce")
        support[column] = pd.to_numeric(support[column], errors="coerce")

    result = support[["tract_geoid", "county_fips", "acs_vintage", "tract_vintage"]].copy()
    result["source_county_fips"] = result["county_fips"]
    result["acs1_vintage"] = result["county_fips"].map(source["acs1_vintage"])
    result["allocation_method"] = SAE_ALLOCATION_METHOD

    missing_by_index, partial_by_index = _diagnostic_lists(support, components)
    zero_by_index: dict[int, list[str]] = {int(index): [] for index in support.index}
    residuals_by_county: dict[str, dict[str, float | None]] = {
        county: {} for county in sorted(support_counties)
    }

    for column in components:
        county_support_total = support.groupby("county_fips", dropna=False)[column].sum(min_count=1)
        source_total = source[column]
        support_total = support["county_fips"].map(county_support_total)
        source_value = support["county_fips"].map(source_total)

        valid = (
            source_value.notna()
            & support[column].notna()
            & support_total.notna()
            & (support_total > 0)
        )
        allocated = pd.Series(pd.NA, index=support.index, dtype="Float64")
        allocated.loc[valid] = (
            source_value.loc[valid]
            * support.loc[valid, column]
            / support_total.loc[valid]
        )
        result[f"sae_{column}"] = allocated.astype("Float64")

        zero_mask = support_total.notna() & (support_total == 0)
        for index in support.index[zero_mask]:
            zero_by_index[int(index)].append(column)

        allocated_by_county = result.groupby("county_fips", dropna=False)[f"sae_{column}"].sum(
            min_count=1,
        )
        for county in sorted(support_counties):
            allocated_total = allocated_by_county.get(county, pd.NA)
            county_source_total = source_total.get(county, pd.NA)
            if pd.isna(allocated_total) or pd.isna(county_source_total):
                residuals_by_county[county][column] = None
            else:
                residuals_by_county[county][column] = float(allocated_total - county_source_total)

    result["sae_missing_support_columns"] = [
        _json_list(missing_by_index[int(index)]) for index in support.index
    ]
    result["sae_zero_denominator_columns"] = [
        _json_list(zero_by_index[int(index)]) for index in support.index
    ]
    result["sae_partial_coverage_columns"] = [
        _json_list(partial_by_index[int(index)]) for index in support.index
    ]
    result["sae_missing_support_count"] = [
        len(missing_by_index[int(index)]) for index in support.index
    ]
    result["sae_zero_denominator_count"] = [
        len(zero_by_index[int(index)]) for index in support.index
    ]
    result["sae_source_county_count"] = result["county_fips"].map(
        pd.Series(1, index=source.index),
    ).fillna(0).astype("Int64")
    result["sae_support_tract_count"] = result["county_fips"].map(
        support.groupby("county_fips").size(),
    ).astype("Int64")
    result["sae_allocation_residuals"] = result["county_fips"].map(
        {
            county: _json_dict(residuals)
            for county, residuals in residuals_by_county.items()
        }
    )

    result = result.sort_values(["county_fips", "tract_geoid"]).reset_index(drop=True)
    result.attrs["allocation_method"] = SAE_ALLOCATION_METHOD
    result.attrs["component_columns"] = components
    result.attrs["missing_support_counties"] = missing_support_counties
    result.attrs["missing_source_counties"] = missing_source_counties
    return result
