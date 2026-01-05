"""CoC-Tract area-weighted crosswalk generation."""

from pathlib import Path

import geopandas as gpd
import pandas as pd


# ESRI:102003 - USA Contiguous Albers Equal Area Conic
ALBERS_EQUAL_AREA_CRS = "ESRI:102003"


def build_coc_tract_crosswalk(
    coc_gdf: gpd.GeoDataFrame,
    tract_gdf: gpd.GeoDataFrame,
    boundary_vintage: str,
    tract_vintage: str,
) -> pd.DataFrame:
    """Build area-weighted crosswalk between CoC boundaries and census tracts.

    Uses geopandas overlay to compute intersections and calculate:
    area_share = intersection_area / tract_area

    Parameters
    ----------
    coc_gdf : gpd.GeoDataFrame
        CoC boundary geometries with 'coc_number' column.
    tract_gdf : gpd.GeoDataFrame
        Census tract geometries with 'GEOID' column.
    boundary_vintage : str
        Version identifier for CoC boundaries (e.g., "2024").
    tract_vintage : str
        Version identifier for census tracts (e.g., "2020").

    Returns
    -------
    pd.DataFrame
        Crosswalk with columns:
        - coc_id
        - boundary_vintage
        - tract_geoid
        - tract_vintage
        - area_share
        - pop_share (None for v1)
        - intersection_area
        - tract_area
    """
    # Ensure required columns exist
    if "coc_number" not in coc_gdf.columns:
        raise ValueError("coc_gdf must have 'coc_number' column")
    if "GEOID" not in tract_gdf.columns:
        raise ValueError("tract_gdf must have 'GEOID' column")

    # Reproject to equal-area for accurate area calculation
    coc_proj = coc_gdf.to_crs(ALBERS_EQUAL_AREA_CRS)
    tract_proj = tract_gdf.to_crs(ALBERS_EQUAL_AREA_CRS)

    # Calculate tract areas before overlay
    tract_proj = tract_proj.copy()
    tract_proj["tract_area"] = tract_proj.geometry.area

    # Compute intersection with gpd.overlay()
    intersections = gpd.overlay(
        coc_proj[["coc_number", "geometry"]],
        tract_proj[["GEOID", "tract_area", "geometry"]],
        how="intersection",
        keep_geom_type=False,
    )

    # Calculate intersection areas
    intersections["intersection_area"] = intersections.geometry.area

    # Calculate area share (intersection / tract)
    intersections["area_share"] = (
        intersections["intersection_area"] / intersections["tract_area"]
    )

    # Build crosswalk DataFrame
    crosswalk = pd.DataFrame(
        {
            "coc_id": intersections["coc_number"],
            "boundary_vintage": boundary_vintage,
            "tract_geoid": intersections["GEOID"],
            "tract_vintage": tract_vintage,
            "area_share": intersections["area_share"],
            "pop_share": None,  # Placeholder for v2 with population weighting
            "intersection_area": intersections["intersection_area"],
            "tract_area": intersections["tract_area"],
        }
    )

    # Sort by coc_id and tract_geoid for consistent output
    crosswalk = crosswalk.sort_values(["coc_id", "tract_geoid"]).reset_index(drop=True)

    return crosswalk


def save_crosswalk(
    crosswalk: pd.DataFrame,
    boundary_vintage: str,
    tract_vintage: str,
    output_dir: Path | str = "data/curated/xwalks",
) -> Path:
    """Save crosswalk to parquet file.

    Parameters
    ----------
    crosswalk : pd.DataFrame
        Crosswalk DataFrame from build_coc_tract_crosswalk.
    boundary_vintage : str
        Version identifier for CoC boundaries.
    tract_vintage : str
        Version identifier for census tracts.
    output_dir : Path | str
        Output directory for parquet file.

    Returns
    -------
    Path
        Path to saved parquet file.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    filename = f"coc_tract_xwalk__{boundary_vintage}__{tract_vintage}.parquet"
    output_path = output_dir / filename

    crosswalk.to_parquet(output_path, index=False)

    return output_path
