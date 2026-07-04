"""Build and persist CoC-to-MSA allocation crosswalks for PIT aggregation."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import geopandas as gpd
import pandas as pd

from hhplab.paths import curated_dir
from hhplab.provenance import ProvenanceBlock, write_parquet_with_provenance
from hhplab.schema.columns import MSA_FRACTIONAL_ROLLUP_COLUMNS
from hhplab.xwalks.county import ALBERS_EQUAL_AREA_CRS, build_county_crosswalk

REQUIRED_MSA_MEMBERSHIP_COLUMNS: tuple[str, ...] = (
    "msa_id",
    "cbsa_code",
    "county_fips",
)

FRACTIONAL_ROLLUP_REQUIRED_CROSSWALK_COLUMNS: tuple[str, ...] = (
    "coc_id",
    "msa_id",
    "boundary_vintage",
    "county_vintage",
    "definition_version",
    "allocation_method",
    "share_column",
    "allocation_share",
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

COC_MSA_BLOCK_POPULATION_CROSSWALK_COLUMNS: tuple[str, ...] = (
    "coc_id",
    "msa_id",
    "cbsa_code",
    "boundary_vintage",
    "county_vintage",
    "block_vintage",
    "decennial_vintage",
    "definition_version",
    "allocation_method",
    "share_column",
    "share_denominator",
    "allocation_share",
    "intersection_population",
    "coc_population_denominator",
    "msa_population_denominator",
    "coc_population_containment_share",
    "msa_population_coverage_share",
    "intersection_area",
    "coc_intersection_area",
    "msa_intersection_area",
    "block_count",
    "missing_population_block_count",
    "zero_population_coc",
    "partial_coc_population_coverage",
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


def _standardize_block_geometry_columns(block_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if "block_geoid" in block_gdf.columns:
        return block_gdf
    if "GEOID" in block_gdf.columns:
        return block_gdf.rename(columns={"GEOID": "block_geoid"})
    if "geoid" in block_gdf.columns:
        return block_gdf.rename(columns={"geoid": "block_geoid"})
    raise ValueError("block_gdf must have 'block_geoid', 'GEOID', or 'geoid' column")


def _standardize_block_population_columns(block_population_df: pd.DataFrame) -> pd.DataFrame:
    if "block_geoid" in block_population_df.columns:
        return block_population_df
    if "GEOID" in block_population_df.columns:
        return block_population_df.rename(columns={"GEOID": "block_geoid"})
    if "geoid" in block_population_df.columns:
        return block_population_df.rename(columns={"geoid": "block_geoid"})
    raise ValueError(
        "block_population_df must have 'block_geoid', 'GEOID', or 'geoid' column"
    )


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


def _require_columns(df: pd.DataFrame, required: set[str], *, label: str) -> None:
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"{label} missing required column(s): {', '.join(missing)}.")


def _project(gdf: gpd.GeoDataFrame, *, label: str) -> gpd.GeoDataFrame:
    if gdf.crs is None:
        raise ValueError(f"{label} GeoDataFrame has no CRS; set CRS before MSA allocation.")
    return gdf.to_crs(ALBERS_EQUAL_AREA_CRS)


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


def build_coc_msa_block_population_crosswalk(
    coc_gdf: gpd.GeoDataFrame,
    county_gdf: gpd.GeoDataFrame,
    msa_county_membership: pd.DataFrame,
    block_gdf: gpd.GeoDataFrame,
    block_population_df: pd.DataFrame,
    *,
    boundary_vintage: str,
    county_vintage: str,
    block_vintage: str,
    decennial_vintage: str,
    definition_version: str,
) -> pd.DataFrame:
    """Build a block-population CoC-to-MSA allocation crosswalk.

    Block population is allocated by the area share of each block intersected
    by CoC and MSA geometries. ``allocation_share`` is the fraction of each
    CoC's block-population denominator allocated to an MSA.
    """
    county_gdf = _standardize_county_geometry_columns(county_gdf)
    block_gdf = _standardize_block_geometry_columns(block_gdf)
    block_population_df = _standardize_block_population_columns(block_population_df)
    _validate_inputs(coc_gdf, county_gdf, msa_county_membership)
    _require_columns(block_gdf, {"block_geoid", "geometry"}, label="block_gdf")
    _require_columns(
        block_population_df,
        {"block_geoid", "total_population"},
        label="block_population_df",
    )
    _validate_membership_counties(
        county_gdf,
        msa_county_membership,
        county_vintage=county_vintage,
    )

    coc = _project(coc_gdf[["coc_id", "geometry"]].copy(), label="coc_gdf")
    counties = _project(
        county_gdf[["GEOID", "geometry"]].copy(),
        label="county_gdf",
    )
    blocks = _prepare_blocks(block_gdf, block_population_df)
    membership = msa_county_membership[
        ["msa_id", "cbsa_code", "county_fips"]
    ].copy()
    membership["msa_id"] = membership["msa_id"].astype(str)
    membership["cbsa_code"] = membership["cbsa_code"].astype(str)
    membership["county_fips"] = membership["county_fips"].astype(str)
    msa = _build_msa_geometry_from_counties(counties, membership)

    if coc.empty or blocks.empty or msa.empty:
        return _empty_block_population_crosswalk()

    coc_blocks = _block_geometry_denominator(
        coc,
        blocks,
        left_id_col="coc_id",
        output_area_col="coc_intersection_area",
    )
    if coc_blocks.empty:
        return _empty_block_population_crosswalk()

    coc_denominators = _summarize_block_denominator(
        coc_blocks,
        id_col="coc_id",
        denominator_col="coc_population_denominator",
        area_col="coc_intersection_area",
        output_area_col="coc_intersection_area",
        missing_col="coc_missing_population_block_count",
    )
    msa_denominators = _summarize_msa_block_denominator_from_membership(blocks, membership)

    intersections = _block_coc_msa_intersections(coc_blocks, msa)
    if intersections.empty:
        return _empty_block_population_crosswalk()

    grouped = (
        intersections.groupby(["coc_id", "msa_id", "cbsa_code"], as_index=False)
        .agg(
            intersection_population=("intersection_population", "sum"),
            intersection_area=("intersection_area", "sum"),
            block_count=("block_geoid", "nunique"),
            missing_population_block_count=(
                "missing_population",
                lambda s: int(s.fillna(False).sum()),
            ),
        )
        .merge(coc_denominators, on="coc_id", how="left")
        .merge(msa_denominators, on="msa_id", how="left")
        .sort_values(["coc_id", "msa_id"])
        .reset_index(drop=True)
    )
    grouped["boundary_vintage"] = boundary_vintage
    grouped["county_vintage"] = county_vintage
    grouped["block_vintage"] = block_vintage
    grouped["decennial_vintage"] = decennial_vintage
    grouped["definition_version"] = definition_version
    grouped["allocation_method"] = "block_population"
    grouped["share_column"] = "allocation_share"
    grouped["share_denominator"] = "coc_population_denominator"
    grouped["zero_population_coc"] = grouped["coc_population_denominator"].fillna(0.0) == 0.0
    grouped["allocation_share"] = grouped["intersection_population"] / grouped[
        "coc_population_denominator"
    ]
    grouped.loc[grouped["zero_population_coc"], "allocation_share"] = 0.0
    grouped["coc_population_containment_share"] = grouped["allocation_share"]
    grouped["msa_population_coverage_share"] = grouped["intersection_population"] / grouped[
        "msa_population_denominator"
    ]
    grouped = grouped.fillna(
        {
            "allocation_share": 0.0,
            "coc_population_containment_share": 0.0,
            "msa_population_coverage_share": 0.0,
        }
    )
    allocation_totals = grouped.groupby("coc_id")["allocation_share"].transform("sum")
    grouped["partial_coc_population_coverage"] = (
        (grouped["coc_missing_population_block_count"] > 0)
        | (
            ~grouped["zero_population_coc"]
            & (allocation_totals < FULL_ALLOCATION_THRESHOLD)
        )
    )
    _validate_allocation_shares(grouped)
    return grouped.loc[:, COC_MSA_BLOCK_POPULATION_CROSSWALK_COLUMNS]


def _empty_block_population_crosswalk() -> pd.DataFrame:
    return pd.DataFrame(columns=list(COC_MSA_BLOCK_POPULATION_CROSSWALK_COLUMNS))


def _summarize_msa_block_denominator_from_membership(
    blocks: gpd.GeoDataFrame,
    membership: pd.DataFrame,
) -> pd.DataFrame:
    block_denominators = blocks[
        ["block_geoid", "total_population", "block_area"]
    ].copy()
    block_denominators["county_fips"] = block_denominators["block_geoid"].astype(str).str[:5]
    block_denominators["total_population"] = pd.to_numeric(
        block_denominators["total_population"],
        errors="coerce",
    )
    block_denominators["missing_population"] = block_denominators["total_population"].isna()
    block_denominators["allocated_population"] = block_denominators[
        "total_population"
    ].fillna(0.0)

    member_counties = membership[["msa_id", "county_fips"]].drop_duplicates().copy()
    joined = member_counties.merge(block_denominators, on="county_fips", how="inner")
    if joined.empty:
        return pd.DataFrame(
            columns=[
                "msa_id",
                "msa_population_denominator",
                "msa_intersection_area",
                "msa_missing_population_block_count",
            ]
        )
    return (
        joined.groupby("msa_id", as_index=False)
        .agg(
            msa_population_denominator=("allocated_population", "sum"),
            msa_intersection_area=("block_area", "sum"),
            msa_missing_population_block_count=(
                "missing_population",
                lambda s: int(s.fillna(False).sum()),
            ),
        )
        .reset_index(drop=True)
    )


def _prepare_blocks(
    block_gdf: gpd.GeoDataFrame,
    block_population_df: pd.DataFrame,
) -> gpd.GeoDataFrame:
    blocks = _project(block_gdf[["block_geoid", "geometry"]].copy(), label="block_gdf")
    population = block_population_df[["block_geoid", "total_population"]].copy()
    blocks["block_geoid"] = blocks["block_geoid"].astype(str)
    population["block_geoid"] = population["block_geoid"].astype(str)
    population["total_population"] = pd.to_numeric(
        population["total_population"],
        errors="coerce",
    )
    blocks = blocks.merge(population, on="block_geoid", how="left")
    blocks["block_area"] = blocks.geometry.area
    return blocks.loc[blocks["block_area"] > 0].copy()


def _build_msa_geometry_from_counties(
    county_gdf: gpd.GeoDataFrame,
    membership: pd.DataFrame,
) -> gpd.GeoDataFrame:
    counties = county_gdf.copy()
    counties["county_fips"] = counties["GEOID"].astype(str)
    joined = membership.merge(counties[["county_fips", "geometry"]], on="county_fips", how="inner")
    if joined.empty:
        return gpd.GeoDataFrame(
            columns=["msa_id", "cbsa_code", "geometry"],
            geometry="geometry",
            crs=county_gdf.crs,
        )
    joined = gpd.GeoDataFrame(joined, geometry="geometry", crs=county_gdf.crs)
    return joined.dissolve(by=["msa_id", "cbsa_code"], as_index=False)[
        ["msa_id", "cbsa_code", "geometry"]
    ]


def _block_geometry_denominator(
    geometry_gdf: gpd.GeoDataFrame,
    blocks: gpd.GeoDataFrame,
    *,
    left_id_col: str,
    output_area_col: str,
) -> gpd.GeoDataFrame:
    query = blocks.sindex.query(geometry_gdf.geometry, predicate="intersects")
    if query.size == 0:
        return _empty_block_geometry_denominator(
            left_id_col=left_id_col,
            output_area_col=output_area_col,
            crs=geometry_gdf.crs,
        )

    left_positions = query[0]
    block_positions = query[1]
    covered_query = blocks.sindex.query(geometry_gdf.geometry, predicate="covers")
    fully_covered_pairs = _single_covered_block_pairs(covered_query)
    full_mask = pd.Series(
        [
            (int(left_position), int(block_position)) in fully_covered_pairs
            for left_position, block_position in zip(
                left_positions,
                block_positions,
                strict=True,
            )
        ]
    ).to_numpy()

    parts: list[gpd.GeoDataFrame] = []
    if full_mask.any():
        full_left_positions = left_positions[full_mask]
        full_block_positions = block_positions[full_mask]
        parts.append(
            gpd.GeoDataFrame(
                {
                    left_id_col: geometry_gdf[left_id_col].iloc[full_left_positions].to_numpy(),
                    "block_geoid": blocks["block_geoid"].iloc[full_block_positions].to_numpy(),
                    "total_population": blocks["total_population"]
                    .iloc[full_block_positions]
                    .to_numpy(),
                    "block_area": blocks["block_area"].iloc[full_block_positions].to_numpy(),
                    output_area_col: blocks["block_area"].iloc[full_block_positions].to_numpy(),
                },
                geometry=blocks.geometry.iloc[full_block_positions].to_numpy(),
                crs=geometry_gdf.crs,
            )
        )

    partial_mask = ~full_mask
    if partial_mask.any():
        partial = _intersect_block_geometry_pairs(
            geometry_gdf,
            blocks,
            left_id_col=left_id_col,
            output_area_col=output_area_col,
            left_positions=left_positions[partial_mask],
            block_positions=block_positions[partial_mask],
        )
        if not partial.empty:
            parts.append(partial)

    if not parts:
        return _empty_block_geometry_denominator(
            left_id_col=left_id_col,
            output_area_col=output_area_col,
            crs=geometry_gdf.crs,
        )

    intersections = gpd.GeoDataFrame(pd.concat(parts, ignore_index=True), crs=geometry_gdf.crs)
    intersections[output_area_col] = intersections.geometry.area
    intersections = intersections.loc[intersections[output_area_col] > 0].copy()
    intersections["total_population"] = pd.to_numeric(
        intersections["total_population"],
        errors="coerce",
    )
    intersections["missing_population"] = intersections["total_population"].isna()
    intersections["allocated_population"] = (
        intersections["total_population"].fillna(0.0)
        * intersections[output_area_col]
        / intersections["block_area"]
    )
    return intersections


def _single_covered_block_pairs(query: object) -> set[tuple[int, int]]:
    if getattr(query, "size", 0) == 0:
        return set()
    left_positions = query[0]
    block_positions = query[1]
    counts = pd.Series(block_positions).value_counts()
    single_blocks = set(counts.loc[counts == 1].index.astype(int))
    return {
        (int(left_position), int(block_position))
        for left_position, block_position in zip(left_positions, block_positions, strict=True)
        if int(block_position) in single_blocks
    }


def _intersect_block_geometry_pairs(
    geometry_gdf: gpd.GeoDataFrame,
    blocks: gpd.GeoDataFrame,
    *,
    left_id_col: str,
    output_area_col: str,
    left_positions: object,
    block_positions: object,
) -> gpd.GeoDataFrame:
    left = geometry_gdf[[left_id_col, "geometry"]].iloc[left_positions].reset_index(drop=True)
    right = (
        blocks[["block_geoid", "total_population", "block_area", "geometry"]]
        .iloc[block_positions]
        .reset_index(drop=True)
    )
    left_geometry = gpd.GeoSeries(left.geometry, crs=geometry_gdf.crs)
    right_geometry = gpd.GeoSeries(right.geometry, crs=blocks.crs)
    intersections = left_geometry.intersection(right_geometry, align=False)
    allocated = gpd.GeoDataFrame(
        {
            left_id_col: left[left_id_col].to_numpy(),
            "block_geoid": right["block_geoid"].to_numpy(),
            "total_population": right["total_population"].to_numpy(),
            "block_area": right["block_area"].to_numpy(),
        },
        geometry=intersections,
        crs=geometry_gdf.crs,
    )
    allocated = allocated.loc[~allocated.geometry.is_empty].copy()
    if allocated.empty:
        return _empty_block_geometry_denominator(
            left_id_col=left_id_col,
            output_area_col=output_area_col,
            crs=geometry_gdf.crs,
        )
    allocated[output_area_col] = allocated.geometry.area
    return allocated.loc[allocated[output_area_col] > 0].copy()


def _empty_block_geometry_denominator(
    *,
    left_id_col: str,
    output_area_col: str,
    crs: object,
) -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        columns=[
            left_id_col,
            "block_geoid",
            "total_population",
            "block_area",
            output_area_col,
            "allocated_population",
            "missing_population",
            "geometry",
        ],
        geometry="geometry",
        crs=crs,
    )


def _summarize_block_denominator(
    intersections: pd.DataFrame,
    *,
    id_col: str,
    denominator_col: str,
    area_col: str,
    output_area_col: str,
    missing_col: str,
) -> pd.DataFrame:
    return (
        intersections.groupby(id_col, as_index=False)
        .agg(
            **{
                denominator_col: ("allocated_population", "sum"),
                output_area_col: (area_col, "sum"),
                missing_col: ("missing_population", lambda s: int(s.fillna(False).sum())),
            }
        )
        .reset_index(drop=True)
    )


def _block_coc_msa_intersections(
    coc_blocks: gpd.GeoDataFrame,
    msa_gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    intersections = gpd.overlay(
        coc_blocks[
            [
                "coc_id",
                "block_geoid",
                "total_population",
                "block_area",
                "missing_population",
                "geometry",
            ]
        ],
        msa_gdf[["msa_id", "cbsa_code", "geometry"]],
        how="intersection",
        keep_geom_type=False,
    )
    if intersections.empty:
        return gpd.GeoDataFrame(
            columns=[
                "coc_id",
                "msa_id",
                "cbsa_code",
                "block_geoid",
                "total_population",
                "missing_population",
                "intersection_area",
                "intersection_population",
                "geometry",
            ],
            geometry="geometry",
            crs=coc_blocks.crs,
        )
    intersections = intersections.loc[~intersections.geometry.is_empty].copy()
    intersections["intersection_area"] = intersections.geometry.area
    intersections = intersections.loc[intersections["intersection_area"] > 0].copy()
    intersections["intersection_population"] = (
        intersections["total_population"].fillna(0.0)
        * intersections["intersection_area"]
        / intersections["block_area"]
    )
    return intersections


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


def aggregate_coc_to_msa_fractional_rollup(
    source_df: pd.DataFrame,
    crosswalk_df: pd.DataFrame,
    *,
    additive_measure_columns: list[str] | tuple[str, ...],
    source_dataset_id: str,
    year_column: str = "year",
    source_coc_id_column: str = "coc_id",
    native_msa_covariate_columns: list[str] | tuple[str, ...] = (),
    min_coc_population_containment_share: float | None = None,
    min_msa_population_coverage_share: float | None = None,
    min_allocation_share: float | None = None,
) -> pd.DataFrame:
    """Roll up additive CoC-native measures to ``msa_id x year`` rows.

    The function is allocation-basis agnostic. Area and block-population
    crosswalks both use ``allocation_share``; basis-specific denominator and
    vintage metadata are carried through output diagnostics.
    """
    measures = _validate_fractional_rollup_source(
        source_df,
        additive_measure_columns,
        year_column=year_column,
        source_coc_id_column=source_coc_id_column,
    )
    crosswalk = _validate_fractional_rollup_crosswalk(crosswalk_df)
    if source_df.empty:
        return _empty_fractional_rollup(measures)

    source = source_df.copy()
    source = source.rename(columns={source_coc_id_column: "coc_id", year_column: "year"})
    source["coc_id"] = source["coc_id"].astype(str)
    source["year"] = pd.to_numeric(source["year"], errors="raise").astype(int)
    for measure in measures:
        source[measure] = pd.to_numeric(source[measure], errors="raise")
    _validate_fractional_rollup_source_grain(source)

    crosswalk = _apply_fractional_rollup_thresholds(
        crosswalk,
        min_coc_population_containment_share=min_coc_population_containment_share,
        min_msa_population_coverage_share=min_msa_population_coverage_share,
        min_allocation_share=min_allocation_share,
    )
    if crosswalk.empty:
        return _empty_fractional_rollup(measures)

    resolved = _fractional_rollup_metadata(crosswalk)
    expected_meta = _fractional_rollup_expected_meta(crosswalk)
    unmapped_source_meta = _fractional_rollup_unmapped_source_meta(
        source,
        crosswalk,
        measures,
    )
    merge_columns = ["coc_id", "msa_id", "allocation_share"]
    if "msa_population_coverage_share" in crosswalk.columns:
        merge_columns.append("msa_population_coverage_share")
    merged = source.merge(
        crosswalk[merge_columns],
        on="coc_id",
        how="inner",
    )
    weighted = merged.copy()
    for measure in measures:
        weighted[measure] = weighted[measure] * weighted["allocation_share"]

    rollup_aggregations: dict[str, tuple[str, object]] = {
        measure: (measure, "sum")
        for measure in measures
    }
    rollup_aggregations.update(
        {
            "covered_coc_count": ("coc_id", "nunique"),
            "allocation_share_sum": ("allocation_share", "sum"),
            "found_cocs": ("coc_id", lambda s: tuple(sorted({str(value) for value in s}))),
        }
    )
    if "msa_population_coverage_share" in weighted.columns:
        rollup_aggregations["actual_msa_population_coverage_share"] = (
            "msa_population_coverage_share",
            "sum",
        )

    rollup = (
        weighted.groupby(["msa_id", "year"], as_index=False)
        .agg(**rollup_aggregations)
        if not weighted.empty
        else pd.DataFrame(
            columns=[
                "msa_id",
                "year",
                *measures,
                "covered_coc_count",
                "allocation_share_sum",
                "found_cocs",
            ]
        )
    )

    years = sorted(source["year"].unique().tolist())
    skeleton = expected_meta[["msa_id"]].drop_duplicates().merge(
        pd.DataFrame({"year": years}),
        how="cross",
    )
    result = (
        skeleton.merge(rollup, on=["msa_id", "year"], how="left")
        .merge(
            expected_meta,
            on="msa_id",
            how="left",
        )
        .merge(
            unmapped_source_meta,
            on="year",
            how="left",
        )
    )

    rows: list[dict[str, object]] = []
    for row in result.itertuples(index=False):
        found_cocs = _tuple_or_empty(getattr(row, "found_cocs", ()))
        expected_cocs = set(row.expected_cocs)
        missing_cocs = sorted(expected_cocs - set(found_cocs))
        allocation_share_sum = (
            float(row.allocation_share_sum) if pd.notna(row.allocation_share_sum) else 0.0
        )
        output_row = {
            "msa_id": row.msa_id,
            "year": int(row.year),
            "source_dataset_id": source_dataset_id,
            "source_geo_type": "coc",
            "source_additive_measure_columns": ",".join(measures),
            "native_msa_covariate_columns": ",".join(native_msa_covariate_columns),
            "allocation_basis": resolved["allocation_basis"],
            "denominator_source": resolved["denominator_source"],
            "boundary_vintage": resolved["boundary_vintage"],
            "county_vintage": resolved["county_vintage"],
            "block_vintage": resolved["block_vintage"],
            "decennial_vintage": resolved["decennial_vintage"],
            "msa_definition_version": resolved["definition_version"],
            "coc_count": int(row.expected_coc_count),
            "covered_coc_count": (
                int(row.covered_coc_count) if pd.notna(row.covered_coc_count) else 0
            ),
            "missing_coc_count": len(missing_cocs),
            "missing_cocs": ",".join(missing_cocs),
            "unmapped_source_coc_count": _int_value(
                row,
                "unmapped_source_coc_count",
                default=0,
            ),
            "unmapped_source_cocs": _string_value(row, "unmapped_source_cocs", default=""),
            "unmapped_source_additive_measure_totals": _string_value(
                row,
                "unmapped_source_additive_measure_totals",
                default=_measure_totals_json({measure: 0.0 for measure in measures}),
            ),
            "zero_population_coc_count": int(row.zero_population_coc_count),
            "missing_population_block_count": int(row.missing_population_block_count),
            "min_coc_population_containment_share": min_coc_population_containment_share,
            "min_msa_population_coverage_share": min_msa_population_coverage_share,
            "min_allocation_share": min_allocation_share,
            "coc_population_coverage_ratio": (
                allocation_share_sum / float(row.expected_allocation_share_sum)
                if float(row.expected_allocation_share_sum) > 0
                else 0.0
            ),
            "msa_population_coverage_ratio": _coverage_value(
                row,
                "actual_msa_population_coverage_share",
                default=_coverage_fraction(
                    allocation_share_sum,
                    float(row.expected_allocation_share_sum),
                ),
            ),
            "allocation_share_sum": allocation_share_sum,
            "expected_allocation_share_sum": float(row.expected_allocation_share_sum),
            "source": "msa_fractional_rollup",
        }
        for measure in measures:
            value = getattr(row, measure)
            output_row[measure] = float(value) if pd.notna(value) else pd.NA
        rows.append(output_row)

    columns = [*MSA_FRACTIONAL_ROLLUP_COLUMNS, *measures]
    out = pd.DataFrame(rows, columns=columns)
    if out.empty:
        return out
    for measure in measures:
        out[measure] = out[measure].astype("Float64")
    return out.sort_values(["msa_id", "year"]).reset_index(drop=True)


def _validate_fractional_rollup_source(
    source_df: pd.DataFrame,
    additive_measure_columns: list[str] | tuple[str, ...],
    *,
    year_column: str,
    source_coc_id_column: str,
) -> tuple[str, ...]:
    measures = tuple(dict.fromkeys(column.strip() for column in additive_measure_columns))
    if not measures or any(not column for column in measures):
        raise ValueError("additive_measure_columns must contain at least one non-blank column.")
    required = {source_coc_id_column, year_column, *measures}
    missing = sorted(required - set(source_df.columns))
    if missing:
        raise ValueError(
            "CoC source data missing required column(s): "
            f"{', '.join(missing)}. Available: {list(source_df.columns)}"
        )
    return measures


def _validate_fractional_rollup_crosswalk(crosswalk_df: pd.DataFrame) -> pd.DataFrame:
    missing = sorted(set(FRACTIONAL_ROLLUP_REQUIRED_CROSSWALK_COLUMNS) - set(crosswalk_df.columns))
    if missing:
        raise ValueError(
            "CoC-to-MSA crosswalk missing required column(s): "
            f"{', '.join(missing)}. Available: {list(crosswalk_df.columns)}"
        )
    crosswalk = crosswalk_df.copy()
    crosswalk["coc_id"] = crosswalk["coc_id"].astype(str)
    crosswalk["msa_id"] = crosswalk["msa_id"].astype(str)
    crosswalk["allocation_share"] = pd.to_numeric(
        crosswalk["allocation_share"],
        errors="raise",
    )
    share_column = _resolve_single_crosswalk_value(crosswalk["share_column"], "share_column")
    if share_column != "allocation_share":
        raise ValueError(
            f"Unsupported share_column '{share_column}'. Expected 'allocation_share'."
        )
    _validate_allocation_shares(crosswalk)
    zero_population = (
        crosswalk["zero_population_coc"].fillna(False).astype(bool)
        if "zero_population_coc" in crosswalk.columns
        else pd.Series(False, index=crosswalk.index)
    )
    crosswalk = crosswalk[
        (crosswalk["allocation_share"] > ALLOCATION_SHARE_TOLERANCE) | zero_population
    ].copy()
    return crosswalk


def _validate_fractional_rollup_source_grain(source: pd.DataFrame) -> None:
    duplicated = source[source.duplicated(subset=["coc_id", "year"], keep=False)]
    if duplicated.empty:
        return

    examples = (
        duplicated[["coc_id", "year"]]
        .drop_duplicates()
        .sort_values(["coc_id", "year"])
        .head(5)
    )
    preview = ", ".join(
        f"{row.coc_id}/{int(row.year)}" for row in examples.itertuples(index=False)
    )
    if len(examples) < duplicated[["coc_id", "year"]].drop_duplicates().shape[0]:
        preview += ", ..."
    raise ValueError(
        "CoC source data must be unique by coc_id and year before MSA fractional "
        f"rollup. Duplicate source row keys: {preview}. "
        "Aggregate or deduplicate the source CoC-year data before calling "
        "aggregate_coc_to_msa_fractional_rollup."
    )


def _apply_fractional_rollup_thresholds(
    crosswalk: pd.DataFrame,
    *,
    min_coc_population_containment_share: float | None,
    min_msa_population_coverage_share: float | None,
    min_allocation_share: float | None,
) -> pd.DataFrame:
    filtered = crosswalk.copy()
    if min_coc_population_containment_share is not None:
        column = (
            "coc_population_containment_share"
            if "coc_population_containment_share" in filtered.columns
            else "allocation_share"
        )
        filtered = filtered[
            pd.to_numeric(filtered[column], errors="coerce")
            >= min_coc_population_containment_share
        ].copy()
    if min_msa_population_coverage_share is not None:
        if "msa_population_coverage_share" not in filtered.columns:
            raise ValueError(
                "min_msa_population_coverage_share requires crosswalk column "
                "'msa_population_coverage_share'."
            )
        filtered = filtered[
            pd.to_numeric(filtered["msa_population_coverage_share"], errors="coerce")
            >= min_msa_population_coverage_share
        ].copy()
    if min_allocation_share is not None:
        filtered = filtered[filtered["allocation_share"] >= min_allocation_share].copy()
    return filtered


def _fractional_rollup_metadata(crosswalk: pd.DataFrame) -> dict[str, object]:
    allocation_basis = _resolve_single_crosswalk_value(
        crosswalk["allocation_method"],
        "allocation_method",
    )
    return {
        "allocation_basis": allocation_basis,
        "denominator_source": _denominator_source_for_allocation_basis(allocation_basis),
        "boundary_vintage": _resolve_single_crosswalk_value(
            crosswalk["boundary_vintage"],
            "boundary_vintage",
        ),
        "county_vintage": _resolve_single_crosswalk_value(
            crosswalk["county_vintage"],
            "county_vintage",
        ),
        "block_vintage": _optional_single_crosswalk_value(crosswalk, "block_vintage"),
        "decennial_vintage": _optional_single_crosswalk_value(crosswalk, "decennial_vintage"),
        "definition_version": _resolve_single_crosswalk_value(
            crosswalk["definition_version"],
            "definition_version",
        ),
    }


def _fractional_rollup_expected_meta(crosswalk: pd.DataFrame) -> pd.DataFrame:
    aggregations: dict[str, tuple[str, object]] = {
        "expected_coc_count": ("coc_id", "nunique"),
        "expected_allocation_share_sum": ("allocation_share", "sum"),
        "expected_cocs": ("coc_id", lambda s: tuple(sorted({str(value) for value in s}))),
    }
    if "zero_population_coc" in crosswalk.columns:
        aggregations["zero_population_coc_count"] = (
            "zero_population_coc",
            lambda s: int(pd.Series(s).fillna(False).astype(bool).sum()),
        )
    if "missing_population_block_count" in crosswalk.columns:
        aggregations["missing_population_block_count"] = (
            "missing_population_block_count",
            "sum",
        )
    if "msa_population_coverage_share" in crosswalk.columns:
        aggregations["expected_msa_population_coverage_share"] = (
            "msa_population_coverage_share",
            "sum",
        )
    meta = crosswalk.groupby("msa_id", as_index=False).agg(**aggregations)
    if "zero_population_coc_count" not in meta.columns:
        meta["zero_population_coc_count"] = 0
    if "missing_population_block_count" not in meta.columns:
        meta["missing_population_block_count"] = 0
    if "expected_msa_population_coverage_share" not in meta.columns:
        meta["expected_msa_population_coverage_share"] = meta[
            "expected_allocation_share_sum"
        ]
    return meta


def _fractional_rollup_unmapped_source_meta(
    source: pd.DataFrame,
    crosswalk: pd.DataFrame,
    measures: tuple[str, ...],
) -> pd.DataFrame:
    mapped_cocs = set(crosswalk["coc_id"].astype(str))
    unmapped = source[~source["coc_id"].isin(mapped_cocs)].copy()
    columns = [
        "year",
        "unmapped_source_coc_count",
        "unmapped_source_cocs",
        "unmapped_source_additive_measure_totals",
    ]
    if unmapped.empty:
        return pd.DataFrame(columns=columns)

    meta = (
        unmapped.groupby("year", as_index=False)
        .agg(
            unmapped_source_coc_count=("coc_id", "nunique"),
            unmapped_source_cocs=(
                "coc_id",
                lambda s: ",".join(sorted({str(value) for value in s})),
            ),
            **{measure: (measure, "sum") for measure in measures},
        )
    )
    meta["unmapped_source_additive_measure_totals"] = meta.apply(
        lambda row: _measure_totals_json(
            {measure: float(getattr(row, measure)) for measure in measures}
        ),
        axis=1,
    )
    return meta[columns]


def _resolve_single_crosswalk_value(series: pd.Series, field_name: str) -> str:
    values = sorted({str(value) for value in series.dropna().unique()})
    if len(values) != 1:
        raise ValueError(f"Crosswalk must have exactly one {field_name} value, found {values}")
    return values[0]


def _optional_single_crosswalk_value(crosswalk: pd.DataFrame, field_name: str) -> str | None:
    if field_name not in crosswalk.columns:
        return None
    values = sorted({str(value) for value in crosswalk[field_name].dropna().unique()})
    if not values:
        return None
    if len(values) != 1:
        raise ValueError(f"Crosswalk must have exactly one {field_name} value, found {values}")
    return values[0]


def _denominator_source_for_allocation_basis(allocation_basis: str) -> str:
    if allocation_basis == "block_population":
        return "pl_94_171_block_population"
    if allocation_basis == "area":
        return "geometry_area"
    return allocation_basis


def _tuple_or_empty(value: object) -> tuple[str, ...]:
    if bool(pd.api.types.is_scalar(value) and pd.isna(value)):
        return ()
    return tuple(value or ())


def _coverage_value(row: object, field_name: str, *, default: float) -> float:
    value = getattr(row, field_name, default)
    if pd.isna(value):
        value = default
    return _bounded_coverage(float(value))


def _coverage_fraction(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return _bounded_coverage(numerator / denominator)


def _bounded_coverage(value: float) -> float:
    return min(max(value, 0.0), 1.0)


def _int_value(row: object, field_name: str, *, default: int) -> int:
    value = getattr(row, field_name, default)
    return int(value) if pd.notna(value) else default


def _string_value(row: object, field_name: str, *, default: str) -> str:
    value = getattr(row, field_name, default)
    return str(value) if pd.notna(value) else default


def _measure_totals_json(totals: dict[str, float]) -> str:
    return json.dumps(totals, sort_keys=True, separators=(",", ":"))


def _empty_fractional_rollup(measures: tuple[str, ...]) -> pd.DataFrame:
    return pd.DataFrame(columns=[*MSA_FRACTIONAL_ROLLUP_COLUMNS, *measures])


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


def save_coc_msa_block_population_crosswalk(
    crosswalk: pd.DataFrame,
    *,
    boundary_vintage: str,
    county_vintage: str,
    block_vintage: str,
    decennial_vintage: str,
    definition_version: str,
    output_dir: Path | str | None = None,
) -> Path:
    """Persist a block-population CoC-to-MSA crosswalk with provenance."""
    from hhplab.naming import msa_coc_block_population_xwalk_filename

    if output_dir is None:
        output_dir = curated_dir("xwalks")
    else:
        output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / msa_coc_block_population_xwalk_filename(
        boundary_vintage,
        definition_version,
        county_vintage,
        block_vintage,
        decennial_vintage,
    )
    provenance = ProvenanceBlock(
        boundary_vintage=boundary_vintage,
        county_vintage=county_vintage,
        geo_type="msa",
        definition_version=definition_version,
        weighting="block_population",
        extra={
            "dataset_type": "coc_msa_crosswalk",
            "allocation_basis": "block_population",
            "block_vintage": block_vintage,
            "decennial_vintage": decennial_vintage,
            "denominator_source": "pl_94_171_block_population",
            "share_column": "allocation_share",
            "share_denominator": "coc_population_denominator",
            "derivation": "block_population_coc_msa_area_intersection",
        },
    )
    write_parquet_with_provenance(crosswalk, output_path, provenance)
    return output_path


def read_coc_msa_crosswalk(
    boundary_vintage: str,
    definition_version: str,
    county_vintage: str,
    *,
    allocation_basis: str = "area",
    block_vintage: str | int = "2020",
    decennial_vintage: str | int = "2020",
    base_dir: Path | str | None = None,
) -> pd.DataFrame:
    """Read a curated CoC-to-MSA crosswalk from disk."""
    from hhplab.naming import msa_coc_block_population_xwalk_path, msa_coc_xwalk_path

    if allocation_basis == "area":
        path = msa_coc_xwalk_path(
            boundary_vintage,
            definition_version,
            county_vintage,
            base_dir=base_dir,
        )
        generate_command = (
            "hhplab generate msa-xwalk "
            f"--boundary {boundary_vintage} "
            f"--definition-version {definition_version} "
            f"--counties {county_vintage}"
        )
    elif allocation_basis == "block_population":
        path = msa_coc_block_population_xwalk_path(
            boundary_vintage,
            definition_version,
            county_vintage,
            block_vintage,
            decennial_vintage,
            base_dir=base_dir,
        )
        generate_command = (
            "hhplab generate msa-xwalk "
            f"--allocation-basis block_population --boundary {boundary_vintage} "
            f"--definition-version {definition_version} --counties {county_vintage} "
            f"--blocks {block_vintage} --decennial {decennial_vintage}"
        )
    else:
        raise ValueError(
            f"Unsupported CoC-to-MSA crosswalk allocation_basis '{allocation_basis}'. "
            "Use one of: area, block_population."
        )
    try:
        return pd.read_parquet(path)
    except FileNotFoundError:
        raise FileNotFoundError(
            f"CoC-to-MSA crosswalk artifact not found at {path}. "
            f"Run: {generate_command}"
        ) from None


def read_coc_msa_block_population_crosswalk(
    boundary_vintage: str,
    definition_version: str,
    county_vintage: str,
    block_vintage: str,
    decennial_vintage: str,
    *,
    base_dir: Path | str | None = None,
) -> pd.DataFrame:
    """Read a curated block-population CoC-to-MSA crosswalk from disk."""
    from hhplab.naming import msa_coc_block_population_xwalk_path

    path = msa_coc_block_population_xwalk_path(
        boundary_vintage,
        definition_version,
        county_vintage,
        block_vintage,
        decennial_vintage,
        base_dir=base_dir,
    )
    try:
        return pd.read_parquet(path)
    except FileNotFoundError:
        raise FileNotFoundError(
            f"Block-population CoC-to-MSA crosswalk artifact not found at {path}. "
            "Run: hhplab generate msa-xwalk "
            f"--allocation-basis block_population --boundary {boundary_vintage} "
            f"--definition-version {definition_version} --counties {county_vintage} "
            f"--blocks {block_vintage} --decennial {decennial_vintage}"
        ) from None
