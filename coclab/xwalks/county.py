"""Area-weighted county crosswalk generation.

Provides geometry-neutral crosswalk builders that work with any
analysis geography (CoC, metro, etc.) via the ``geo_id_col`` parameter.
The legacy ``build_coc_county_crosswalk`` name is preserved as a
convenience wrapper.
"""

from pathlib import Path

import geopandas as gpd
import pandas as pd

# ESRI:102003 - USA Contiguous Albers Equal Area Conic
ALBERS_EQUAL_AREA_CRS = "ESRI:102003"


def build_county_crosswalk(
    geo_gdf: gpd.GeoDataFrame,
    county_gdf: gpd.GeoDataFrame,
    boundary_vintage: str,
    *,
    geo_id_col: str = "coc_id",
) -> pd.DataFrame:
    """Build area-weighted crosswalk between analysis geometries and counties.

    Uses geopandas overlay to compute intersections and calculate:
    area_share = intersection_area / county_area

    Parameters
    ----------
    geo_gdf : gpd.GeoDataFrame
        Analysis geometry boundaries with a ``geo_id_col`` column.
    county_gdf : gpd.GeoDataFrame
        County geometries with 'GEOID' column.
    boundary_vintage : str
        Version identifier for analysis boundaries (e.g., "2024").
    geo_id_col : str
        Name of the geography identifier column in *geo_gdf*.
        Defaults to ``"coc_id"`` for backward compatibility.

    Returns
    -------
    pd.DataFrame
        Crosswalk with columns:
        - ``geo_id_col`` (e.g., coc_id or metro_id)
        - boundary_vintage
        - county_fips
        - area_share (intersection_area / county_area, for county->geo aggregation)
        - intersection_area (square meters in ESRI:102003)
        - county_area (square meters in ESRI:102003)
        - geo_area (square meters in ESRI:102003)
    """
    # Ensure required columns exist
    if geo_id_col not in geo_gdf.columns:
        raise ValueError(f"geo_gdf must have '{geo_id_col}' column")
    if "GEOID" not in county_gdf.columns:
        raise ValueError("county_gdf must have 'GEOID' column")

    # Reproject to equal-area for accurate area calculation
    geo_proj = geo_gdf.to_crs(ALBERS_EQUAL_AREA_CRS)
    county_proj = county_gdf.to_crs(ALBERS_EQUAL_AREA_CRS)

    # Calculate geometry areas before overlay
    geo_proj = geo_proj.copy()
    geo_proj["geo_area"] = geo_proj.geometry.area

    county_proj = county_proj.copy()
    county_proj["county_area"] = county_proj.geometry.area

    # Compute intersection with gpd.overlay()
    intersections = gpd.overlay(
        geo_proj[[geo_id_col, "geo_area", "geometry"]],
        county_proj[["GEOID", "county_area", "geometry"]],
        how="intersection",
        keep_geom_type=False,
    )

    # Calculate intersection areas
    intersections["intersection_area"] = intersections.geometry.area

    # Calculate area share (intersection / county) for county->geo aggregation
    intersections["area_share"] = intersections["intersection_area"] / intersections["county_area"]

    # Build crosswalk DataFrame with all area columns
    crosswalk = pd.DataFrame(
        {
            geo_id_col: intersections[geo_id_col],
            "boundary_vintage": boundary_vintage,
            "county_fips": intersections["GEOID"],
            "area_share": intersections["area_share"],
            "intersection_area": intersections["intersection_area"],
            "county_area": intersections["county_area"],
            "geo_area": intersections["geo_area"],
        }
    )

    # Sort for consistent output
    crosswalk = crosswalk.sort_values([geo_id_col, "county_fips"]).reset_index(drop=True)

    return crosswalk


def build_coc_county_crosswalk(
    coc_gdf: gpd.GeoDataFrame,
    county_gdf: gpd.GeoDataFrame,
    boundary_vintage: str,
) -> pd.DataFrame:
    """Build area-weighted crosswalk between CoC boundaries and counties.

    Convenience wrapper around :func:`build_county_crosswalk` with
    ``geo_id_col="coc_id"``.  See that function for full documentation.

    Note: the output column ``geo_area`` is named ``coc_area`` for
    backward compatibility.
    """
    result = build_county_crosswalk(
        coc_gdf,
        county_gdf,
        boundary_vintage,
        geo_id_col="coc_id",
    )
    # Rename geo_area -> coc_area for backward compatibility
    return result.rename(columns={"geo_area": "coc_area"})


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
        Crosswalk DataFrame from build_county_crosswalk.
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
