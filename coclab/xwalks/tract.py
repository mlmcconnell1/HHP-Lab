"""CoC-Tract area-weighted crosswalk generation."""

from pathlib import Path

import geopandas as gpd
import pandas as pd

from coclab.provenance import ProvenanceBlock, write_parquet_with_provenance


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


def add_population_weights(
    crosswalk: pd.DataFrame,
    population_data: pd.DataFrame,
) -> pd.DataFrame:
    """Add population-weighted shares to an existing tract crosswalk.

    Computes pop_share for each tract within a CoC using:
    pop_share = (area_share × tract_pop) / Σ(area_share × tract_pop)

    The sum is taken over all tracts intersecting each CoC.

    Parameters
    ----------
    crosswalk : pd.DataFrame
        Tract crosswalk with 'coc_id', 'tract_geoid', 'area_share' columns.
    population_data : pd.DataFrame
        Population data with 'GEOID' and 'total_population' columns.

    Returns
    -------
    pd.DataFrame
        Crosswalk with pop_share column populated.
    """
    # Merge population data
    xwalk = crosswalk.copy()

    # Standardize GEOID column name
    pop_df = population_data.copy()
    if "GEOID" in pop_df.columns:
        pop_df = pop_df.rename(columns={"GEOID": "tract_geoid"})

    xwalk = xwalk.merge(
        pop_df[["tract_geoid", "total_population"]],
        on="tract_geoid",
        how="left",
    )

    # Compute weighted population: area_share × tract_population
    xwalk["weighted_pop"] = xwalk["area_share"] * xwalk["total_population"].fillna(0)

    # Compute sum of weighted population per CoC
    coc_totals = xwalk.groupby("coc_id")["weighted_pop"].transform("sum")

    # Compute pop_share = weighted_pop / coc_total
    # Handle division by zero (CoCs with no population data)
    xwalk["pop_share"] = xwalk["weighted_pop"] / coc_totals.replace(0, pd.NA)

    # Drop temporary columns
    xwalk = xwalk.drop(columns=["total_population", "weighted_pop"])

    return xwalk


def validate_population_shares(crosswalk: pd.DataFrame) -> pd.DataFrame:
    """Validate that population shares sum to approximately 1 per CoC.

    Parameters
    ----------
    crosswalk : pd.DataFrame
        Crosswalk with pop_share column.

    Returns
    -------
    pd.DataFrame
        Validation results with coc_id, pop_share_sum, and is_valid columns.
    """
    if "pop_share" not in crosswalk.columns:
        raise ValueError("Crosswalk must have 'pop_share' column")

    # Sum pop_share per CoC
    sums = crosswalk.groupby("coc_id")["pop_share"].sum().reset_index()
    sums.columns = ["coc_id", "pop_share_sum"]

    # Check if sum is approximately 1 (within 0.01 tolerance)
    sums["is_valid"] = (sums["pop_share_sum"] > 0.99) & (sums["pop_share_sum"] < 1.01)

    return sums


def save_crosswalk(
    crosswalk: pd.DataFrame,
    boundary_vintage: str,
    tract_vintage: str,
    output_dir: Path | str = "data/curated/xwalks",
    *,
    has_pop_weights: bool = False,
) -> Path:
    """Save crosswalk to parquet file with provenance metadata.

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
    has_pop_weights : bool
        Whether crosswalk includes population weights.

    Returns
    -------
    Path
        Path to saved parquet file.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    filename = f"coc_tract_xwalk__{boundary_vintage}__{tract_vintage}.parquet"
    output_path = output_dir / filename

    # Build provenance block
    provenance = ProvenanceBlock(
        boundary_vintage=boundary_vintage,
        tract_vintage=tract_vintage,
        weighting="area" if not has_pop_weights else "area+population",
        extra={"dataset_type": "coc_tract_crosswalk"},
    )

    write_parquet_with_provenance(crosswalk, output_path, provenance)

    return output_path
