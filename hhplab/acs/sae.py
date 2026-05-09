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


def _single_or_json(values: pd.Series) -> str | None:
    unique_values = sorted(values.dropna().astype(str).unique())
    if not unique_values:
        return None
    if len(unique_values) == 1:
        return unique_values[0]
    return json.dumps(unique_values)


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


def _allocated_component_columns(
    tract_allocations: pd.DataFrame,
    component_columns: Iterable[str] | None,
) -> list[str]:
    if component_columns is None:
        return [
            column
            for column in tract_allocations.columns
            if column.startswith("sae_")
            and column
            not in {
                "sae_missing_support_columns",
                "sae_zero_denominator_columns",
                "sae_partial_coverage_columns",
                "sae_missing_support_count",
                "sae_zero_denominator_count",
                "sae_source_county_count",
                "sae_support_tract_count",
                "sae_allocation_residuals",
            }
        ]
    return [
        column if column.startswith("sae_") else f"sae_{column}"
        for column in component_columns
    ]


def rollup_sae_tracts_to_geos(
    tract_allocations: pd.DataFrame,
    tract_crosswalk: pd.DataFrame,
    *,
    geo_id_col: str = "coc_id",
    share_column: str = "area_share",
    component_columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Roll allocated SAE tract components to analysis geographies."""
    components = _allocated_component_columns(tract_allocations, component_columns)
    if not components:
        raise ValueError("No allocated SAE component columns were available to roll up.")

    _require_columns(
        tract_allocations,
        [
            "tract_geoid",
            "source_county_fips",
            "acs1_vintage",
            "acs_vintage",
            "tract_vintage",
            *components,
        ],
        "tract_allocations",
    )
    _require_columns(tract_crosswalk, [geo_id_col, "tract_geoid", share_column], "tract_crosswalk")

    allocations = tract_allocations.copy()
    xwalk = tract_crosswalk.copy()
    allocations["tract_geoid"] = allocations["tract_geoid"].astype("string").str.zfill(11)
    xwalk["tract_geoid"] = xwalk["tract_geoid"].astype("string").str.zfill(11)
    xwalk[share_column] = pd.to_numeric(xwalk[share_column], errors="coerce")

    merged = xwalk.merge(allocations, on="tract_geoid", how="left")
    for column in components:
        merged[column] = pd.to_numeric(merged[column], errors="coerce")
        merged[f"weighted_{column}"] = merged[column] * merged[share_column]

    grouped = merged.groupby(geo_id_col, dropna=False)
    result = pd.DataFrame({geo_id_col: sorted(merged[geo_id_col].dropna().unique())})
    result["target_geo_type"] = geo_id_col.removesuffix("_id")
    result["target_geo_id"] = result[geo_id_col]

    for column in components:
        totals = grouped[f"weighted_{column}"].sum(min_count=1)
        result[column] = result[geo_id_col].map(totals).astype("Float64")

    diagnostics = grouped.agg(
        acs1_vintage=("acs1_vintage", _single_or_json),
        acs5_vintage=("acs_vintage", _single_or_json),
        tract_vintage=("tract_vintage", _single_or_json),
        sae_source_county_count=("source_county_fips", lambda s: int(s.dropna().nunique())),
        sae_crosswalk_tract_count=("tract_geoid", lambda s: int(s.dropna().nunique())),
        sae_allocated_tract_count=("allocation_method", lambda s: int(s.notna().sum())),
    ).reset_index()
    diagnostics["sae_missing_allocation_tract_count"] = (
        diagnostics["sae_crosswalk_tract_count"] - diagnostics["sae_allocated_tract_count"]
    )
    diagnostics["sae_crosswalk_coverage_ratio"] = (
        diagnostics["sae_allocated_tract_count"]
        / diagnostics["sae_crosswalk_tract_count"].replace(0, pd.NA)
    )

    if "sae_missing_support_count" in merged.columns:
        missing_support = grouped["sae_missing_support_count"].sum(min_count=1)
    else:
        missing_support = pd.Series(dtype="float64")
    if "sae_zero_denominator_count" in merged.columns:
        zero_denominator = grouped["sae_zero_denominator_count"].sum(min_count=1)
    else:
        zero_denominator = pd.Series(dtype="float64")
    if "sae_partial_coverage_columns" in merged.columns:
        partial_coverage = grouped["sae_partial_coverage_columns"].apply(
            lambda s: int(sum(bool(json.loads(value)) for value in s.dropna()))
        )
    else:
        partial_coverage = pd.Series(dtype="float64")

    result = result.merge(diagnostics, on=geo_id_col, how="left")
    result["sae_missing_support_count"] = (
        result[geo_id_col].map(missing_support).fillna(0).astype("Int64")
    )
    result["sae_zero_denominator_count"] = (
        result[geo_id_col].map(zero_denominator).fillna(0).astype("Int64")
    )
    result["sae_partial_coverage_count"] = (
        result[geo_id_col].map(partial_coverage).fillna(0).astype("Int64")
    )
    source_counties = grouped["source_county_fips"].apply(
        lambda s: _json_list(s.dropna().astype(str))
    )
    result["sae_source_counties"] = result[geo_id_col].map(source_counties)

    ordered_columns = [
        geo_id_col,
        "target_geo_type",
        "target_geo_id",
        "acs1_vintage",
        "acs5_vintage",
        "tract_vintage",
        *components,
        "sae_source_county_count",
        "sae_source_counties",
        "sae_crosswalk_tract_count",
        "sae_allocated_tract_count",
        "sae_missing_allocation_tract_count",
        "sae_crosswalk_coverage_ratio",
        "sae_missing_support_count",
        "sae_zero_denominator_count",
        "sae_partial_coverage_count",
    ]
    result = result[ordered_columns].sort_values(geo_id_col).reset_index(drop=True)
    result.attrs["geo_id_col"] = geo_id_col
    result.attrs["share_column"] = share_column
    result.attrs["component_columns"] = components
    return result
