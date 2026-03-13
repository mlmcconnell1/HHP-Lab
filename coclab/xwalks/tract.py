"""Area-weighted tract crosswalk generation.

Provides geometry-neutral crosswalk builders that work with any
analysis geography (CoC, metro, etc.) via the ``geo_id_col`` parameter.
The legacy ``build_coc_tract_crosswalk`` name is preserved as a
convenience wrapper.
"""

from collections.abc import Callable
from pathlib import Path

import geopandas as gpd
import pandas as pd

from coclab.provenance import ProvenanceBlock, write_parquet_with_provenance

# ESRI:102003 - USA Contiguous Albers Equal Area Conic
ALBERS_EQUAL_AREA_CRS = "ESRI:102003"

# Default batch size for progress reporting
DEFAULT_BATCH_SIZE = 40


def build_tract_crosswalk(
    geo_gdf: gpd.GeoDataFrame,
    tract_gdf: gpd.GeoDataFrame,
    boundary_vintage: str,
    tract_vintage: str,
    *,
    geo_id_col: str = "coc_id",
    batch_size: int = DEFAULT_BATCH_SIZE,
    progress_callback: Callable[[int, int], None] | None = None,
) -> pd.DataFrame:
    """Build area-weighted crosswalk between analysis geometries and census tracts.

    Uses geopandas overlay to compute intersections and calculate:
    area_share = intersection_area / tract_area

    Parameters
    ----------
    geo_gdf : gpd.GeoDataFrame
        Analysis geometry boundaries with a ``geo_id_col`` column.
    tract_gdf : gpd.GeoDataFrame
        Census tract geometries with 'GEOID' column.
    boundary_vintage : str
        Version identifier for analysis boundaries (e.g., "2024").
    tract_vintage : str
        Version identifier for census tracts (e.g., "2020").
    geo_id_col : str
        Name of the geography identifier column in *geo_gdf*.
        Defaults to ``"coc_id"`` for backward compatibility.
    batch_size : int
        Number of geometries to process per batch for progress reporting.
        Set to 0 to process all at once (no batching).
    progress_callback : Callable[[int, int], None] | None
        Optional callback called after each batch with (completed, total) counts.

    Returns
    -------
    pd.DataFrame
        Crosswalk with columns:
        - ``geo_id_col`` (e.g., coc_id or metro_id)
        - boundary_vintage
        - tract_geoid
        - tract_vintage
        - area_share
        - pop_share (None for v1)
        - intersection_area
        - tract_area
    """
    # Ensure required columns exist
    if geo_id_col not in geo_gdf.columns:
        raise ValueError(f"geo_gdf must have '{geo_id_col}' column")
    if "GEOID" not in tract_gdf.columns:
        raise ValueError("tract_gdf must have 'GEOID' column")

    # Reproject to equal-area for accurate area calculation
    geo_proj = geo_gdf.to_crs(ALBERS_EQUAL_AREA_CRS)
    tract_proj = tract_gdf.to_crs(ALBERS_EQUAL_AREA_CRS)

    # Calculate tract areas before overlay
    tract_proj = tract_proj.copy()
    tract_proj["tract_area"] = tract_proj.geometry.area

    n_geos = len(geo_proj)

    # Process in batches if batch_size > 0 and we have a callback
    if batch_size > 0 and progress_callback is not None:
        intersection_results = []
        for i in range(0, n_geos, batch_size):
            batch = geo_proj.iloc[i : i + batch_size]
            batch_intersections = gpd.overlay(
                batch[[geo_id_col, "geometry"]],
                tract_proj[["GEOID", "tract_area", "geometry"]],
                how="intersection",
                keep_geom_type=False,
            )
            intersection_results.append(batch_intersections)
            progress_callback(min(i + batch_size, n_geos), n_geos)

        intersections = pd.concat(intersection_results, ignore_index=True)
    else:
        # Single bulk operation (faster, no progress)
        intersections = gpd.overlay(
            geo_proj[[geo_id_col, "geometry"]],
            tract_proj[["GEOID", "tract_area", "geometry"]],
            how="intersection",
            keep_geom_type=False,
        )

    # Calculate intersection areas
    intersections["intersection_area"] = intersections.geometry.area

    # Calculate area share (intersection / tract)
    intersections["area_share"] = intersections["intersection_area"] / intersections["tract_area"]

    # Build crosswalk DataFrame
    crosswalk = pd.DataFrame(
        {
            geo_id_col: intersections[geo_id_col],
            "boundary_vintage": boundary_vintage,
            "tract_geoid": intersections["GEOID"],
            "tract_vintage": tract_vintage,
            "area_share": intersections["area_share"],
            "pop_share": None,  # Placeholder for v2 with population weighting
            "intersection_area": intersections["intersection_area"],
            "tract_area": intersections["tract_area"],
        }
    )

    # Sort for consistent output
    crosswalk = crosswalk.sort_values([geo_id_col, "tract_geoid"]).reset_index(drop=True)

    return crosswalk


def build_coc_tract_crosswalk(
    coc_gdf: gpd.GeoDataFrame,
    tract_gdf: gpd.GeoDataFrame,
    boundary_vintage: str,
    tract_vintage: str,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    progress_callback: Callable[[int, int], None] | None = None,
) -> pd.DataFrame:
    """Build area-weighted crosswalk between CoC boundaries and census tracts.

    Convenience wrapper around :func:`build_tract_crosswalk` with
    ``geo_id_col="coc_id"``.  See that function for full documentation.
    """
    return build_tract_crosswalk(
        coc_gdf,
        tract_gdf,
        boundary_vintage,
        tract_vintage,
        geo_id_col="coc_id",
        batch_size=batch_size,
        progress_callback=progress_callback,
    )


def add_population_weights(
    crosswalk: pd.DataFrame,
    population_data: pd.DataFrame,
    *,
    geo_id_col: str = "coc_id",
) -> pd.DataFrame:
    """Add population-weighted shares to an existing tract crosswalk.

    Computes pop_share for each tract within a geography unit using:
    pop_share = (area_share * tract_pop) / sum(area_share * tract_pop)

    The sum is taken over all tracts intersecting each geography unit.

    Parameters
    ----------
    crosswalk : pd.DataFrame
        Tract crosswalk with ``geo_id_col``, 'tract_geoid', 'area_share' columns.
    population_data : pd.DataFrame
        Population data with 'GEOID' and 'total_population' columns.
    geo_id_col : str
        Name of the geography identifier column.  Defaults to ``"coc_id"``.

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

    # Compute weighted population: area_share * tract_population
    xwalk["weighted_pop"] = xwalk["area_share"] * xwalk["total_population"].fillna(0)

    # Compute sum of weighted population per geography unit
    geo_totals = xwalk.groupby(geo_id_col)["weighted_pop"].transform("sum")

    # Compute pop_share = weighted_pop / geo_total
    # Handle division by zero (units with no population data)
    xwalk["pop_share"] = xwalk["weighted_pop"] / geo_totals.replace(0, pd.NA)

    # Drop temporary columns
    xwalk = xwalk.drop(columns=["total_population", "weighted_pop"])

    return xwalk


def validate_population_shares(
    crosswalk: pd.DataFrame,
    *,
    geo_id_col: str = "coc_id",
) -> pd.DataFrame:
    """Validate that population shares sum to approximately 1 per geography unit.

    Parameters
    ----------
    crosswalk : pd.DataFrame
        Crosswalk with pop_share column.
    geo_id_col : str
        Name of the geography identifier column.  Defaults to ``"coc_id"``.

    Returns
    -------
    pd.DataFrame
        Validation results with ``geo_id_col``, pop_share_sum, and is_valid columns.
    """
    if "pop_share" not in crosswalk.columns:
        raise ValueError("Crosswalk must have 'pop_share' column")

    # Sum pop_share per geography unit
    sums = crosswalk.groupby(geo_id_col)["pop_share"].sum().reset_index()
    sums.columns = [geo_id_col, "pop_share_sum"]

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
        Crosswalk DataFrame from build_tract_crosswalk.
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
        Path to saved parquet file (e.g., xwalk__B2025xT2023.parquet).
    """
    from coclab.naming import tract_xwalk_filename

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    filename = tract_xwalk_filename(boundary_vintage, tract_vintage)
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
