"""Tract-mediated county-to-geography crosswalk weights.

This module derives county-to-analysis-geography allocation weights by
composing a tract crosswalk with ACS tract denominator columns.  It is
intentionally separate from direct county polygon overlays so existing
``area_share`` semantics remain unchanged.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from hhplab.paths import curated_dir
from hhplab.provenance import ProvenanceBlock, write_parquet_with_provenance

DENOMINATOR_COLUMNS: dict[str, str] = {
    "area": "tract_area",
    "population": "total_population",
    "household": "total_households",
    "renter_household": "renter_households",
}

WEIGHT_COLUMNS: tuple[str, ...] = (
    "area_weight",
    "population_weight",
    "household_weight",
    "renter_household_weight",
)


def _require_columns(df: pd.DataFrame, required: set[str], *, label: str) -> None:
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(
            f"{label} missing required column(s): {', '.join(missing)}. "
            "Provide a tract crosswalk and ACS tract denominator table with the "
            "canonical HHP-Lab schema."
        )


def _standardize_acs_tracts(acs_tracts: pd.DataFrame) -> pd.DataFrame:
    acs = acs_tracts.copy()
    if "GEOID" in acs.columns and "tract_geoid" not in acs.columns:
        acs = acs.rename(columns={"GEOID": "tract_geoid"})
    _require_columns(acs, {"tract_geoid", "total_population"}, label="acs_tracts")
    acs["tract_geoid"] = acs["tract_geoid"].astype(str).str.zfill(11)

    keep = ["tract_geoid"]
    for denominator_col in set(DENOMINATOR_COLUMNS.values()) - {"tract_area"}:
        if denominator_col in acs.columns:
            acs[denominator_col] = pd.to_numeric(acs[denominator_col], errors="coerce")
            keep.append(denominator_col)
    return acs[keep].drop_duplicates("tract_geoid")


def _safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    denom = denominator.where(denominator > 0)
    return numerator / denom


def build_tract_mediated_county_crosswalk(
    tract_crosswalk: pd.DataFrame,
    acs_tracts: pd.DataFrame,
    *,
    boundary_vintage: str | int,
    county_vintage: str | int,
    tract_vintage: str | int,
    acs_vintage: str | int,
    geo_id_col: str = "coc_id",
) -> pd.DataFrame:
    """Build county-to-geography weights mediated through tracts.

    Parameters
    ----------
    tract_crosswalk : pd.DataFrame
        Analysis geography to tract crosswalk with ``geo_id_col``,
        ``tract_geoid``, ``area_share``, ``intersection_area``, and
        ``tract_area`` columns.
    acs_tracts : pd.DataFrame
        ACS tract table with ``tract_geoid`` or ``GEOID``, ``total_population``,
        and optionally ``total_households`` and ``renter_households``.
    boundary_vintage, county_vintage, tract_vintage, acs_vintage
        Vintage metadata carried into output rows and provenance.
    geo_id_col : str
        Analysis geography identifier column. Defaults to ``"coc_id"``.

    Returns
    -------
    pd.DataFrame
        One row per geography/county pair with normalized allocation weights
        and denominator diagnostics. Weight denominators are county totals,
        so per-county weight sums are also coverage diagnostics.
    """
    _require_columns(
        tract_crosswalk,
        {geo_id_col, "tract_geoid", "area_share", "intersection_area", "tract_area"},
        label="tract_crosswalk",
    )

    xwalk = tract_crosswalk.copy()
    xwalk["tract_geoid"] = xwalk["tract_geoid"].astype(str).str.zfill(11)
    xwalk["county_fips"] = xwalk["tract_geoid"].str[:5]
    for col in ("area_share", "intersection_area", "tract_area"):
        xwalk[col] = pd.to_numeric(xwalk[col], errors="coerce")

    household_available = "total_households" in acs_tracts.columns
    renter_available = "renter_households" in acs_tracts.columns
    acs = _standardize_acs_tracts(acs_tracts)
    merged = xwalk.merge(acs, on="tract_geoid", how="left")

    # Pair-level raw contributions: tract fraction in geography times tract denominator.
    merged["area_denominator"] = merged["intersection_area"]
    for output_name, denominator_col in DENOMINATOR_COLUMNS.items():
        if output_name == "area":
            continue
        pair_col = f"{output_name}_denominator"
        if denominator_col in merged.columns:
            merged[pair_col] = merged["area_share"] * merged[denominator_col]
        else:
            merged[pair_col] = pd.NA

    pair_denominator_cols = [
        "area_denominator",
        "population_denominator",
        "household_denominator",
        "renter_household_denominator",
    ]

    grouped = (
        merged.groupby([geo_id_col, "county_fips"], dropna=False)
        .agg(
            area_denominator=("area_denominator", "sum"),
            population_denominator=("population_denominator", "sum"),
            household_denominator=("household_denominator", "sum"),
            renter_household_denominator=("renter_household_denominator", "sum"),
            tract_count=("tract_geoid", "nunique"),
            missing_population_tract_count=("total_population", lambda s: int(s.isna().sum())),
            missing_household_tract_count=(
                "total_households",
                lambda s: int(s.isna().sum()),
            )
            if "total_households" in merged.columns
            else ("tract_geoid", lambda s: len(s)),
            missing_renter_household_tract_count=(
                "renter_households",
                lambda s: int(s.isna().sum()),
            )
            if "renter_households" in merged.columns
            else ("tract_geoid", lambda s: len(s)),
        )
        .reset_index()
    )

    unique_tracts = merged.drop_duplicates("tract_geoid")
    county_totals = unique_tracts.groupby("county_fips", dropna=False).agg(
        county_area_total=("tract_area", "sum"),
        county_population_total=("total_population", "sum"),
        county_household_total=(
            "total_households",
            "sum",
        )
        if "total_households" in unique_tracts.columns
        else ("tract_geoid", lambda s: pd.NA),
        county_renter_household_total=(
            "renter_households",
            "sum",
        )
        if "renter_households" in unique_tracts.columns
        else ("tract_geoid", lambda s: pd.NA),
    )
    grouped = grouped.merge(county_totals.reset_index(), on="county_fips", how="left")

    geo_totals = grouped.groupby(geo_id_col, dropna=False)[pair_denominator_cols].transform("sum")
    geo_totals = geo_totals.rename(
        columns={
            "area_denominator": "geo_area_total",
            "population_denominator": "geo_population_total",
            "household_denominator": "geo_household_total",
            "renter_household_denominator": "geo_renter_household_total",
        }
    )
    grouped = pd.concat([grouped, geo_totals], axis=1)

    grouped["area_weight"] = _safe_divide(
        grouped["area_denominator"],
        grouped["county_area_total"],
    )
    grouped["population_weight"] = _safe_divide(
        grouped["population_denominator"],
        grouped["county_population_total"],
    )
    grouped["household_weight"] = _safe_divide(
        grouped["household_denominator"],
        grouped["county_household_total"],
    )
    grouped["renter_household_weight"] = _safe_divide(
        grouped["renter_household_denominator"],
        grouped["county_renter_household_total"],
    )

    if not household_available:
        for col in (
            "household_denominator",
            "county_household_total",
            "geo_household_total",
            "household_weight",
        ):
            grouped[col] = pd.NA
    if not renter_available:
        for col in (
            "renter_household_denominator",
            "county_renter_household_total",
            "geo_renter_household_total",
            "renter_household_weight",
        ):
            grouped[col] = pd.NA

    county_weight_sums = grouped.groupby("county_fips", dropna=False)[
        list(WEIGHT_COLUMNS)
    ].transform("sum")
    county_weight_sums = county_weight_sums.rename(
        columns={
            "area_weight": "county_area_coverage_ratio",
            "population_weight": "county_population_coverage_ratio",
            "household_weight": "county_household_coverage_ratio",
            "renter_household_weight": "county_renter_household_coverage_ratio",
        }
    )
    grouped = pd.concat([grouped, county_weight_sums], axis=1)

    grouped["boundary_vintage"] = str(boundary_vintage)
    grouped["county_vintage"] = str(county_vintage)
    grouped["tract_vintage"] = str(tract_vintage)
    grouped["acs_vintage"] = str(acs_vintage)
    grouped["weighting_method"] = "tract_mediated"

    column_order = [
        geo_id_col,
        "boundary_vintage",
        "county_fips",
        "county_vintage",
        "tract_vintage",
        "acs_vintage",
        "weighting_method",
        "area_weight",
        "population_weight",
        "household_weight",
        "renter_household_weight",
        "area_denominator",
        "population_denominator",
        "household_denominator",
        "renter_household_denominator",
        "county_area_total",
        "county_population_total",
        "county_household_total",
        "county_renter_household_total",
        "geo_area_total",
        "geo_population_total",
        "geo_household_total",
        "geo_renter_household_total",
        "county_area_coverage_ratio",
        "county_population_coverage_ratio",
        "county_household_coverage_ratio",
        "county_renter_household_coverage_ratio",
        "tract_count",
        "missing_population_tract_count",
        "missing_household_tract_count",
        "missing_renter_household_tract_count",
    ]
    grouped = grouped[column_order]
    return grouped.sort_values([geo_id_col, "county_fips"]).reset_index(drop=True)


def save_tract_mediated_county_crosswalk(
    crosswalk: pd.DataFrame,
    *,
    boundary_vintage: str | int,
    county_vintage: str | int,
    tract_vintage: str | int,
    acs_vintage: str | int,
    output_dir: Path | str | None = None,
    geo_type: str = "coc",
) -> Path:
    """Save a tract-mediated county crosswalk with embedded provenance."""
    from hhplab.naming import tract_mediated_county_xwalk_filename

    if output_dir is None:
        output_dir = curated_dir("xwalks")
    else:
        output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / tract_mediated_county_xwalk_filename(
        boundary_vintage,
        county_vintage,
        tract_vintage,
        acs_vintage,
    )
    provenance = ProvenanceBlock(
        boundary_vintage=str(boundary_vintage),
        county_vintage=str(county_vintage),
        tract_vintage=str(tract_vintage),
        acs_vintage=str(acs_vintage),
        weighting="tract_mediated",
        geo_type=geo_type,
        extra={
            "dataset_type": "tract_mediated_county_crosswalk",
            "weight_columns": list(WEIGHT_COLUMNS),
        },
    )
    write_parquet_with_provenance(crosswalk, output_path, provenance)
    return output_path
