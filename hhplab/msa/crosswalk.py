"""Build and persist CoC-to-MSA allocation crosswalks for PIT aggregation."""

from __future__ import annotations

import logging
from pathlib import Path

import geopandas as gpd
import pandas as pd

from hhplab.paths import curated_dir
from hhplab.provenance import ProvenanceBlock, write_parquet_with_provenance
from hhplab.xwalks.county import build_county_crosswalk

REQUIRED_MSA_MEMBERSHIP_COLUMNS: tuple[str, ...] = (
    "msa_id",
    "cbsa_code",
    "county_fips",
)

COC_MSA_CROSSWALK_COLUMNS: tuple[str, ...] = (
    "coc_id",
    "msa_id",
    "cbsa_code",
    "boundary_vintage",
    "county_vintage",
    "definition_version",
    "allocation_method",
    "share_column",
    "share_denominator",
    "allocation_share",
    "intersection_area",
    "coc_area",
    "intersecting_county_count",
    "intersecting_county_fips",
)

#: Numerical tolerance for validating CoC-to-MSA allocation shares and totals.
ALLOCATION_SHARE_TOLERANCE = 1e-6

#: Allocation totals below this threshold are treated as materially partial.
FULL_ALLOCATION_THRESHOLD = 1.0 - ALLOCATION_SHARE_TOLERANCE

logger = logging.getLogger(__name__)


def _empty_crosswalk() -> pd.DataFrame:
    return pd.DataFrame(columns=list(COC_MSA_CROSSWALK_COLUMNS))


def _empty_crosswalk_with_warning(message: str) -> pd.DataFrame:
    crosswalk = _empty_crosswalk()
    crosswalk.attrs["warning"] = message
    logger.warning(message)
    return crosswalk


def _standardize_county_geometry_columns(county_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if "GEOID" in county_gdf.columns:
        return county_gdf
    if "geoid" in county_gdf.columns:
        return county_gdf.rename(columns={"geoid": "GEOID"})
    raise ValueError("county_gdf must have 'GEOID' or 'geoid' column")


def _validate_inputs(
    coc_gdf: gpd.GeoDataFrame,
    county_gdf: gpd.GeoDataFrame,
    msa_county_membership: pd.DataFrame,
) -> None:
    if "coc_id" not in coc_gdf.columns:
        raise ValueError("coc_gdf must have 'coc_id' column")
    if "geometry" not in coc_gdf.columns:
        raise ValueError("coc_gdf must have 'geometry' column")
    if "geometry" not in county_gdf.columns:
        raise ValueError("county_gdf must have 'geometry' column")

    missing = [
        col
        for col in REQUIRED_MSA_MEMBERSHIP_COLUMNS
        if col not in msa_county_membership.columns
    ]
    if missing:
        raise ValueError(
            "msa_county_membership is missing required columns "
            f"{missing}. Available: {list(msa_county_membership.columns)}"
        )


def _validate_membership_counties(
    county_gdf: gpd.GeoDataFrame,
    msa_county_membership: pd.DataFrame,
    *,
    county_vintage: str,
) -> None:
    available_counties = set(county_gdf["GEOID"].astype(str))
    required_counties = set(msa_county_membership["county_fips"].astype(str))
    missing = sorted(required_counties - available_counties)
    if not missing:
        return

    preview = ", ".join(missing[:5])
    if len(missing) > 5:
        preview += ", ..."
    raise ValueError(
        "County geometry vintage "
        f"{county_vintage} does not contain all counties referenced by the MSA "
        f"membership artifact. Missing county_fips: {preview}. "
        f"Run: hhplab ingest tiger --year {county_vintage} --type counties"
    )


def _format_county_preview(counties: pd.Series, *, limit: int = 5) -> str:
    values = sorted({value for value in counties.astype(str)})
    preview = values[:limit]
    suffix = ", ..." if len(values) > limit else ""
    return ", ".join(preview) + suffix if preview else "(none)"


def _format_expected_msa_preview(
    msa_county_membership: pd.DataFrame,
    *,
    limit: int = 5,
) -> str:
    membership = msa_county_membership.copy()
    membership["msa_id"] = membership["msa_id"].astype(str)
    membership["county_fips"] = membership["county_fips"].astype(str)
    msa_pairs = (
        membership.groupby("msa_id")["county_fips"]
        .agg(lambda s: ",".join(sorted(set(s))))
        .sort_index()
        .items()
    )
    preview = [f"{msa_id}=[{counties}]" for msa_id, counties in list(msa_pairs)[:limit]]
    suffix = ", ..." if len(preview) == limit and len(membership["msa_id"].unique()) > limit else ""
    return "; ".join(preview) + suffix if preview else "(none)"


def _validate_allocation_shares(crosswalk: pd.DataFrame) -> None:
    """Raise when any allocation share falls materially outside [0.0, 1.0]."""
    invalid = crosswalk[
        (crosswalk["allocation_share"] < -ALLOCATION_SHARE_TOLERANCE)
        | (crosswalk["allocation_share"] > 1.0 + ALLOCATION_SHARE_TOLERANCE)
    ].copy()
    if invalid.empty:
        return

    invalid["coc_id"] = invalid["coc_id"].astype(str)
    invalid["msa_id"] = invalid["msa_id"].astype(str)
    examples = ", ".join(
        f"{row.coc_id}->{row.msa_id}={row.allocation_share:.9f}"
        for row in invalid.itertuples(index=False)
    )
    raise ValueError(
        "Computed allocation_share outside the allowed range [0.0, 1.0] "
        f"with tolerance {ALLOCATION_SHARE_TOLERANCE:g}. Offending rows: {examples}"
    )


def _validate_allocation_totals(summary: pd.DataFrame) -> None:
    """Raise when per-CoC allocation totals fall materially outside [0.0, 1.0]."""
    invalid = summary[
        (summary["allocation_share_sum"] < -ALLOCATION_SHARE_TOLERANCE)
        | (summary["allocation_share_sum"] > 1.0 + ALLOCATION_SHARE_TOLERANCE)
    ].copy()
    if invalid.empty:
        return

    invalid["coc_id"] = invalid["coc_id"].astype(str)
    examples = ", ".join(
        f"{row.coc_id}={row.allocation_share_sum:.9f}"
        for row in invalid.itertuples(index=False)
    )
    raise ValueError(
        "Computed allocation_share_sum outside the allowed range [0.0, 1.0] "
        f"with tolerance {ALLOCATION_SHARE_TOLERANCE:g}. Offending CoCs: {examples}"
    )


def _validate_coc_area_consistency(county_crosswalk: pd.DataFrame) -> None:
    """Raise when one CoC carries multiple material coc_area denominators."""
    if county_crosswalk.empty:
        return

    area_stats = (
        county_crosswalk.groupby("coc_id")["coc_area"]
        .agg(coc_area_min="min", coc_area_max="max")
        .reset_index()
    )
    tolerance = area_stats["coc_area_max"].abs().clip(lower=1.0) * ALLOCATION_SHARE_TOLERANCE
    invalid = area_stats[
        (area_stats["coc_area_max"] - area_stats["coc_area_min"]).abs() > tolerance
    ].copy()
    if invalid.empty:
        return

    examples = ", ".join(
        f"{row.coc_id}: min={row.coc_area_min:.9f}, max={row.coc_area_max:.9f}"
        for row in invalid.itertuples(index=False)
    )
    raise ValueError(
        "CoC-to-county crosswalk produced inconsistent coc_area values for the same "
        "coc_id before MSA aggregation. Rebuild the county crosswalk or inspect the "
        f"input geometries. Offending CoCs: {examples}"
    )


def build_coc_msa_crosswalk(
    coc_gdf: gpd.GeoDataFrame,
    county_gdf: gpd.GeoDataFrame,
    msa_county_membership: pd.DataFrame,
    *,
    boundary_vintage: str,
    county_vintage: str,
    definition_version: str,
) -> pd.DataFrame:
    """Build an area-weighted CoC-to-MSA crosswalk.

    The implementation derives MSA geometry from official MSA-to-county
    membership and county geometries, then sums CoC/county intersections
    into CoC/MSA overlaps. ``allocation_share`` is the fraction of a CoC's
    area that lies inside a given MSA and is the default PIT allocation rule.
    """
    county_gdf = _standardize_county_geometry_columns(county_gdf)
    _validate_inputs(coc_gdf, county_gdf, msa_county_membership)
    _validate_membership_counties(
        county_gdf,
        msa_county_membership,
        county_vintage=county_vintage,
    )

    county_crosswalk = build_county_crosswalk(
        coc_gdf,
        county_gdf,
        boundary_vintage,
        geo_id_col="coc_id",
    ).rename(columns={"geo_area": "coc_area"})

    if county_crosswalk.empty:
        return _empty_crosswalk_with_warning(
            "No CoC-to-county intersections were found while building the CoC-to-MSA "
            f"crosswalk for boundary_vintage={boundary_vintage}, county_vintage={county_vintage}, "
            f"definition_version={definition_version}. This usually indicates a geometry mismatch "
            "or CRS issue between CoC boundaries and county geometries."
        )
    _validate_coc_area_consistency(county_crosswalk)

    membership = msa_county_membership[
        ["msa_id", "cbsa_code", "county_fips"]
    ].copy()
    membership["county_fips"] = membership["county_fips"].astype(str)

    joined = county_crosswalk.merge(membership, on="county_fips", how="inner")
    if joined.empty:
        tried_counties = _format_county_preview(county_crosswalk["county_fips"])
        expected_msa_counties = _format_expected_msa_preview(membership)
        return _empty_crosswalk_with_warning(
            "CoC-to-county intersections were found, but none matched the MSA county membership "
            f"artifact for definition_version={definition_version}. "
            f"Tried county_fips: {tried_counties}. "
            f"MSA counties by msa_id: {expected_msa_counties}."
        )

    grouped = (
        joined.groupby(["coc_id", "msa_id", "cbsa_code"], as_index=False)
        .agg(
            intersection_area=("intersection_area", "sum"),
            coc_area=("coc_area", "first"),
            intersecting_county_count=("county_fips", "nunique"),
            intersecting_county_fips=(
                "county_fips",
                lambda s: ",".join(sorted({value for value in s.astype(str)})),
            ),
        )
        .sort_values(["coc_id", "msa_id"])
        .reset_index(drop=True)
    )

    grouped["boundary_vintage"] = boundary_vintage
    grouped["county_vintage"] = county_vintage
    grouped["definition_version"] = definition_version
    grouped["allocation_method"] = "area"
    grouped["share_column"] = "allocation_share"
    grouped["share_denominator"] = "coc_area"
    grouped["allocation_share"] = grouped["intersection_area"] / grouped["coc_area"]
    _validate_allocation_shares(grouped)

    return grouped.loc[:, COC_MSA_CROSSWALK_COLUMNS]


def summarize_coc_msa_allocation(crosswalk: pd.DataFrame) -> pd.DataFrame:
    """Summarize allocation completeness per CoC."""
    if crosswalk.empty:
        return pd.DataFrame(columns=["coc_id", "allocation_share_sum", "unallocated_share"])

    summary = (
        crosswalk.groupby("coc_id", as_index=False)["allocation_share"]
        .sum()
        .rename(columns={"allocation_share": "allocation_share_sum"})
    )
    _validate_allocation_totals(summary)
    summary["unallocated_share"] = (1.0 - summary["allocation_share_sum"]).clip(lower=0.0)
    return summary.sort_values("coc_id").reset_index(drop=True)


def save_coc_msa_crosswalk(
    crosswalk: pd.DataFrame,
    *,
    boundary_vintage: str,
    county_vintage: str,
    definition_version: str,
    output_dir: Path | str | None = None,
) -> Path:
    """Persist a CoC-to-MSA crosswalk with embedded provenance."""
    from hhplab.naming import msa_coc_xwalk_filename

    if output_dir is None:
        output_dir = curated_dir("xwalks")
    else:
        output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / msa_coc_xwalk_filename(
        boundary_vintage,
        definition_version,
        county_vintage,
    )
    provenance = ProvenanceBlock(
        boundary_vintage=boundary_vintage,
        county_vintage=county_vintage,
        geo_type="msa",
        definition_version=definition_version,
        weighting="area",
        extra={
            "dataset_type": "coc_msa_crosswalk",
            "share_column": "allocation_share",
            "share_denominator": "coc_area",
            "derivation": "coc_county_overlay_plus_msa_county_membership",
        },
    )
    write_parquet_with_provenance(crosswalk, output_path, provenance)
    return output_path


def read_coc_msa_crosswalk(
    boundary_vintage: str,
    definition_version: str,
    county_vintage: str,
    *,
    base_dir: Path | str | None = None,
) -> pd.DataFrame:
    """Read a curated CoC-to-MSA crosswalk from disk."""
    from hhplab.naming import msa_coc_xwalk_path

    path = msa_coc_xwalk_path(
        boundary_vintage,
        definition_version,
        county_vintage,
        base_dir=base_dir,
    )
    try:
        return pd.read_parquet(path)
    except FileNotFoundError:
        raise FileNotFoundError(
            f"CoC-to-MSA crosswalk artifact not found at {path}. "
            "Run: hhplab generate msa-xwalk "
            f"--boundary {boundary_vintage} "
            f"--definition-version {definition_version} "
            f"--counties {county_vintage}"
        ) from None
