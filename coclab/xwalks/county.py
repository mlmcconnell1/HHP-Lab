"""CoC-County area-weighted crosswalk generation."""

from pathlib import Path

import geopandas as gpd
import pandas as pd

# ESRI:102003 - USA Contiguous Albers Equal Area Conic
ALBERS_EQUAL_AREA_CRS = "ESRI:102003"


def build_coc_county_crosswalk(
    coc_gdf: gpd.GeoDataFrame,
    county_gdf: gpd.GeoDataFrame,
    boundary_vintage: str,
) -> pd.DataFrame:
    """Build area-weighted crosswalk between CoC boundaries and counties.

    Uses geopandas overlay to compute intersections and calculate:
    area_share = intersection_area / county_area

    Parameters
    ----------
    coc_gdf : gpd.GeoDataFrame
        CoC boundary geometries with 'coc_id' column.
    county_gdf : gpd.GeoDataFrame
        County geometries with 'GEOID' column.
    boundary_vintage : str
        Version identifier for CoC boundaries (e.g., "2024").

    Returns
    -------
    pd.DataFrame
        Crosswalk with columns:
        - coc_id
        - boundary_vintage
        - county_fips
        - area_share
    """
    # Ensure required columns exist
    if "coc_id" not in coc_gdf.columns:
        raise ValueError("coc_gdf must have 'coc_id' column")
    if "GEOID" not in county_gdf.columns:
        raise ValueError("county_gdf must have 'GEOID' column")

    # Reproject to equal-area for accurate area calculation
    coc_proj = coc_gdf.to_crs(ALBERS_EQUAL_AREA_CRS)
    county_proj = county_gdf.to_crs(ALBERS_EQUAL_AREA_CRS)

    # Calculate county areas before overlay
    county_proj = county_proj.copy()
    county_proj["county_area"] = county_proj.geometry.area

    # Compute intersection with gpd.overlay()
    intersections = gpd.overlay(
        coc_proj[["coc_id", "geometry"]],
        county_proj[["GEOID", "county_area", "geometry"]],
        how="intersection",
        keep_geom_type=False,
    )

    # Calculate intersection areas
    intersections["intersection_area"] = intersections.geometry.area

    # Calculate area share (intersection / county)
    intersections["area_share"] = intersections["intersection_area"] / intersections["county_area"]

    # Build crosswalk DataFrame
    crosswalk = pd.DataFrame(
        {
            "coc_id": intersections["coc_id"],
            "boundary_vintage": boundary_vintage,
            "county_fips": intersections["GEOID"],
            "area_share": intersections["area_share"],
        }
    )

    # Sort by coc_id and county_fips for consistent output
    crosswalk = crosswalk.sort_values(["coc_id", "county_fips"]).reset_index(drop=True)

    return crosswalk


def save_county_crosswalk(
    crosswalk: pd.DataFrame,
    boundary_vintage: str,
    county_vintage: str | int,
    output_dir: Path | str = "data/curated/xwalks",
) -> Path:
    """Save county crosswalk to parquet file.

    Parameters
    ----------
    crosswalk : pd.DataFrame
        Crosswalk DataFrame from build_coc_county_crosswalk.
    boundary_vintage : str
        Version identifier for CoC boundaries.
    county_vintage : str | int
        Version identifier for county geometries.
    output_dir : Path | str
        Output directory for parquet file.

    Returns
    -------
    Path
        Path to saved parquet file (e.g., xwalk__B2025xC2023.parquet).
    """
    from coclab.naming import county_xwalk_filename

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    filename = county_xwalk_filename(boundary_vintage, county_vintage)
    output_path = output_dir / filename

    crosswalk.to_parquet(output_path, index=False)

    return output_path
