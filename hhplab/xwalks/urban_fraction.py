"""Block-weighted CoC Urban Area population attribution."""

from __future__ import annotations

import geopandas as gpd
import pandas as pd

from hhplab.schema.columns import COC_URBAN_AREA_DETAIL_COLUMNS, COC_URBAN_FRACTION_COLUMNS
from hhplab.xwalks.county import ALBERS_EQUAL_AREA_CRS

DEFAULT_ALLOCATION_METHOD = "block_area_intersection"
DEFAULT_CLASSIFICATION_METHOD = "block_representative_point_in_urban_area"


def _require_columns(df: pd.DataFrame, required: set[str], *, label: str) -> None:
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"{label} missing required column(s): {', '.join(missing)}.")


def _project(gdf: gpd.GeoDataFrame, *, label: str) -> gpd.GeoDataFrame:
    if gdf.crs is None:
        raise ValueError(f"{label} GeoDataFrame has no CRS; set CRS before urban attribution.")
    return gdf.to_crs(ALBERS_EQUAL_AREA_CRS)


def _classify_blocks(
    blocks: gpd.GeoDataFrame,
    urban_areas: gpd.GeoDataFrame,
) -> pd.DataFrame:
    """Classify each block representative point into at most one Urban Area."""
    if blocks.empty:
        return pd.DataFrame(columns=["block_geoid", "urban_area_geoid", "urban_area_name"])
    if urban_areas.empty:
        return pd.DataFrame(
            {
                "block_geoid": blocks["block_geoid"],
                "urban_area_geoid": pd.NA,
                "urban_area_name": pd.NA,
            }
        )

    points = gpd.GeoDataFrame(
        {"block_geoid": blocks["block_geoid"]},
        geometry=blocks.geometry.representative_point(),
        crs=blocks.crs,
    )
    joined = gpd.sjoin(
        points,
        urban_areas[["urban_area_geoid", "urban_area_name", "geometry"]],
        how="left",
        predicate="within",
    )
    joined = joined.drop(columns=["index_right"], errors="ignore")
    joined = joined.drop_duplicates("block_geoid", keep="first")
    return joined[["block_geoid", "urban_area_geoid", "urban_area_name"]]


def build_coc_urban_fraction(
    coc_gdf: gpd.GeoDataFrame,
    block_gdf: gpd.GeoDataFrame,
    urban_area_gdf: gpd.GeoDataFrame,
    *,
    boundary_vintage: str | int,
    urban_area_vintage: str | int,
    block_vintage: str | int,
    decennial_vintage: str | int,
    denominator_source: str = "pl_94_171_block_population",
    allocation_method: str = DEFAULT_ALLOCATION_METHOD,
    classification_method: str = DEFAULT_CLASSIFICATION_METHOD,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build CoC-level urban population fractions from block and UA geometries.

    The function is pure: it reads no global paths and writes no artifacts.
    Block population is allocated to CoCs by block/CoC intersection area; each
    block is classified as urban when its representative point falls inside a
    Census Urban Area polygon.
    """
    intersections = build_coc_urban_fraction_intersections(
        coc_gdf,
        block_gdf,
        urban_area_gdf,
    )
    if intersections.empty:
        return _empty_summary(), _empty_detail()

    return summarize_coc_urban_fraction_intersections(
        intersections,
        boundary_vintage=boundary_vintage,
        urban_area_vintage=urban_area_vintage,
        block_vintage=block_vintage,
        decennial_vintage=decennial_vintage,
        denominator_source=denominator_source,
        allocation_method=allocation_method,
        classification_method=classification_method,
    )


def build_coc_urban_fraction_intersections(
    coc_gdf: gpd.GeoDataFrame,
    block_gdf: gpd.GeoDataFrame,
    urban_area_gdf: gpd.GeoDataFrame,
) -> pd.DataFrame:
    """Build block/CoC intersection rows used by urban fraction summaries."""
    _require_columns(coc_gdf, {"coc_id", "geometry"}, label="coc_gdf")
    _require_columns(
        block_gdf,
        {"block_geoid", "total_population", "geometry"},
        label="block_gdf",
    )
    _require_columns(
        urban_area_gdf,
        {"urban_area_geoid", "urban_area_name", "geometry"},
        label="urban_area_gdf",
    )

    coc = _project(coc_gdf[["coc_id", "geometry"]].copy(), label="coc_gdf")
    blocks = _project(
        block_gdf[["block_geoid", "total_population", "geometry"]].copy(),
        label="block_gdf",
    )
    urban_areas = _project(
        urban_area_gdf[["urban_area_geoid", "urban_area_name", "geometry"]].copy(),
        label="urban_area_gdf",
    )
    blocks["block_area"] = blocks.geometry.area
    blocks = blocks.loc[blocks["block_area"] > 0].copy()

    if coc.empty or blocks.empty:
        return _empty_intersections()

    block_classification = _classify_blocks(blocks, urban_areas)
    intersections = _allocate_coc_blocks(coc, blocks)
    if intersections.empty:
        return _empty_intersections()

    intersections["total_population"] = pd.to_numeric(
        intersections["total_population"],
        errors="coerce",
    )
    intersections = intersections.merge(block_classification, on="block_geoid", how="left")
    intersections["is_urban"] = intersections["urban_area_geoid"].notna()
    intersections["allocated_population"] = (
        intersections["total_population"] * intersections["allocation_share"]
    )
    intersections["allocated_urban_population"] = intersections["allocated_population"].where(
        intersections["is_urban"],
        0.0,
    )
    intersections["known_denominator_area"] = intersections["intersection_area"].where(
        intersections["total_population"].notna(),
        0.0,
    )
    return pd.DataFrame(intersections)


def summarize_coc_urban_fraction_intersections(
    intersections: pd.DataFrame,
    *,
    boundary_vintage: str | int,
    urban_area_vintage: str | int,
    block_vintage: str | int,
    decennial_vintage: str | int,
    denominator_source: str = "pl_94_171_block_population",
    allocation_method: str = DEFAULT_ALLOCATION_METHOD,
    classification_method: str = DEFAULT_CLASSIFICATION_METHOD,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Summarize prepared block/CoC intersection rows into canonical outputs."""
    if intersections.empty:
        return _empty_summary(), _empty_detail()
    metadata = {
        "boundary_vintage": boundary_vintage,
        "urban_area_vintage": urban_area_vintage,
        "block_vintage": block_vintage,
        "decennial_vintage": decennial_vintage,
        "denominator_source": denominator_source,
        "allocation_method": allocation_method,
        "classification_method": classification_method,
    }
    summary = _summarize_intersections(
        intersections,
        **metadata,
    )
    detail = _summarize_urban_area_detail(
        intersections,
        **metadata,
    )
    return summary, detail


def _allocate_coc_blocks(
    coc: gpd.GeoDataFrame,
    blocks: gpd.GeoDataFrame,
) -> pd.DataFrame:
    """Allocate blocks to CoCs, intersecting only boundary-crossing candidate pairs."""
    query = blocks.sindex.query(coc.geometry, predicate="intersects")
    if query.size == 0:
        return _empty_allocations()

    coc_positions = query[0]
    block_positions = query[1]
    covered_query = blocks.sindex.query(coc.geometry, predicate="covers")
    fully_covered_pairs = _single_covered_pairs(covered_query)

    full_mask = pd.Series(
        [
            (int(coc_position), int(block_position)) in fully_covered_pairs
            for coc_position, block_position in zip(coc_positions, block_positions, strict=True)
        ]
    ).to_numpy()
    parts: list[pd.DataFrame] = []
    if full_mask.any():
        full_coc_positions = coc_positions[full_mask]
        full_block_positions = block_positions[full_mask]
        parts.append(
            pd.DataFrame(
                {
                    "coc_id": coc["coc_id"].iloc[full_coc_positions].to_numpy(),
                    "block_geoid": blocks["block_geoid"].iloc[full_block_positions].to_numpy(),
                    "total_population": blocks["total_population"]
                    .iloc[full_block_positions]
                    .to_numpy(),
                    "block_area": blocks["block_area"].iloc[full_block_positions].to_numpy(),
                    "intersection_area": blocks["block_area"]
                    .iloc[full_block_positions]
                    .to_numpy(),
                    "allocation_share": 1.0,
                }
            )
        )

    partial_mask = ~full_mask
    if partial_mask.any():
        partial = _intersect_coc_block_pairs(
            coc,
            blocks,
            coc_positions=coc_positions[partial_mask],
            block_positions=block_positions[partial_mask],
        )
        if not partial.empty:
            parts.append(partial)

    if not parts:
        return _empty_allocations()
    return pd.concat(parts, ignore_index=True)


def _single_covered_pairs(query: object) -> set[tuple[int, int]]:
    if getattr(query, "size", 0) == 0:
        return set()
    coc_positions = query[0]
    block_positions = query[1]
    counts = pd.Series(block_positions).value_counts()
    single_blocks = set(counts.loc[counts == 1].index.astype(int))
    return {
        (int(coc_position), int(block_position))
        for coc_position, block_position in zip(coc_positions, block_positions, strict=True)
        if int(block_position) in single_blocks
    }


def _intersect_coc_block_pairs(
    coc: gpd.GeoDataFrame,
    blocks: gpd.GeoDataFrame,
    *,
    coc_positions: object,
    block_positions: object,
) -> pd.DataFrame:
    left = coc[["coc_id", "geometry"]].iloc[coc_positions].reset_index(drop=True)
    right = (
        blocks[["block_geoid", "total_population", "block_area", "geometry"]]
        .iloc[block_positions]
        .reset_index(drop=True)
    )
    left_geometry = gpd.GeoSeries(left.geometry, crs=coc.crs)
    right_geometry = gpd.GeoSeries(right.geometry, crs=blocks.crs)
    intersections = left_geometry.intersection(right_geometry, align=False)
    allocated = gpd.GeoDataFrame(
        {
            "coc_id": left["coc_id"].to_numpy(),
            "block_geoid": right["block_geoid"].to_numpy(),
            "total_population": right["total_population"].to_numpy(),
            "block_area": right["block_area"].to_numpy(),
        },
        geometry=intersections,
        crs=coc.crs,
    )
    allocated = allocated.loc[~allocated.geometry.is_empty].copy()
    if allocated.empty:
        return _empty_allocations()
    allocated["intersection_area"] = allocated.geometry.area
    allocated = allocated.loc[allocated["intersection_area"] > 0].copy()
    allocated["allocation_share"] = allocated["intersection_area"] / allocated["block_area"]
    return pd.DataFrame(allocated.drop(columns=["geometry"]))


def _empty_allocations() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "coc_id",
            "block_geoid",
            "total_population",
            "block_area",
            "intersection_area",
            "allocation_share",
        ]
    )


def _summarize_intersections(intersections: pd.DataFrame, **metadata: object) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for coc_id, group in intersections.groupby("coc_id", sort=True):
        total = float(group["allocated_population"].sum(skipna=True))
        urban = float(group["allocated_urban_population"].sum(skipna=True))
        block_ids = group["block_geoid"]
        urban_block_ids = group.loc[group["is_urban"], "block_geoid"]
        missing_block_ids = group.loc[group["total_population"].isna(), "block_geoid"]
        intersection_area = float(group["intersection_area"].sum())
        known_area = float(group["known_denominator_area"].sum())
        rows.append(
            {
                "coc_id": coc_id,
                **metadata,
                "coc_total_population": total,
                "coc_urban_population": urban,
                "coc_rural_population": total - urban,
                "urban_population_fraction": urban / total if total > 0 else pd.NA,
                "block_count": int(block_ids.nunique()),
                "urban_block_count": int(urban_block_ids.nunique()),
                "rural_block_count": int(block_ids.nunique() - urban_block_ids.nunique()),
                "missing_denominator_block_count": int(missing_block_ids.nunique()),
                "population_coverage_ratio": known_area / intersection_area
                if intersection_area > 0
                else pd.NA,
                "source": "coc_urban_fraction",
            }
        )
    return pd.DataFrame(rows, columns=COC_URBAN_FRACTION_COLUMNS)


def _summarize_urban_area_detail(intersections: pd.DataFrame, **metadata: object) -> pd.DataFrame:
    urban = intersections.loc[intersections["is_urban"]].copy()
    if urban.empty:
        return _empty_detail()
    rows: list[dict[str, object]] = []
    for keys, group in urban.groupby(["coc_id", "urban_area_geoid"], sort=True):
        coc_id, urban_area_geoid = keys
        urban_population = float(group["allocated_urban_population"].sum(skipna=True))
        urban_area_names = group["urban_area_name"].dropna().astype(str).drop_duplicates()
        rows.append(
            {
                "coc_id": coc_id,
                "urban_area_geoid": urban_area_geoid,
                "urban_area_name": (
                    urban_area_names.iloc[0] if not urban_area_names.empty else pd.NA
                ),
                **metadata,
                "total_population": urban_population,
                "urban_population": urban_population,
                "urban_population_fraction": 1.0 if urban_population > 0 else pd.NA,
                "block_count": int(group["block_geoid"].nunique()),
                "source": "coc_urban_area_detail",
            }
        )
    return pd.DataFrame(rows, columns=COC_URBAN_AREA_DETAIL_COLUMNS)


def _empty_summary() -> pd.DataFrame:
    return pd.DataFrame(columns=COC_URBAN_FRACTION_COLUMNS)


def _empty_detail() -> pd.DataFrame:
    return pd.DataFrame(columns=COC_URBAN_AREA_DETAIL_COLUMNS)


def _empty_intersections() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "coc_id",
            "block_geoid",
            "total_population",
            "block_area",
            "intersection_area",
            "allocation_share",
            "urban_area_geoid",
            "urban_area_name",
            "is_urban",
            "allocated_population",
            "allocated_urban_population",
            "known_denominator_area",
        ]
    )
