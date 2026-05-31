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
        return _empty_summary(), _empty_detail()

    block_classification = _classify_blocks(blocks, urban_areas)
    intersections = gpd.overlay(
        coc,
        blocks[["block_geoid", "total_population", "block_area", "geometry"]],
        how="intersection",
        keep_geom_type=False,
    )
    intersections = intersections.loc[~intersections.geometry.is_empty].copy()
    if intersections.empty:
        return _empty_summary(), _empty_detail()

    intersections["intersection_area"] = intersections.geometry.area
    intersections = intersections.loc[intersections["intersection_area"] > 0].copy()
    intersections["allocation_share"] = (
        intersections["intersection_area"] / intersections["block_area"]
    )
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

    summary = _summarize_intersections(
        intersections,
        boundary_vintage=boundary_vintage,
        urban_area_vintage=urban_area_vintage,
        block_vintage=block_vintage,
        decennial_vintage=decennial_vintage,
        denominator_source=denominator_source,
        allocation_method=allocation_method,
        classification_method=classification_method,
    )
    detail = _summarize_urban_area_detail(
        intersections,
        boundary_vintage=boundary_vintage,
        urban_area_vintage=urban_area_vintage,
        block_vintage=block_vintage,
        decennial_vintage=decennial_vintage,
        denominator_source=denominator_source,
        allocation_method=allocation_method,
        classification_method=classification_method,
    )
    return summary, detail


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
    for keys, group in urban.groupby(["coc_id", "urban_area_geoid", "urban_area_name"], sort=True):
        coc_id, urban_area_geoid, urban_area_name = keys
        urban_population = float(group["allocated_urban_population"].sum(skipna=True))
        rows.append(
            {
                "coc_id": coc_id,
                "urban_area_geoid": urban_area_geoid,
                "urban_area_name": urban_area_name,
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
