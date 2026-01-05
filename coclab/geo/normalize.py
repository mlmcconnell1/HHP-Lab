"""Geometry normalization utilities for CoC boundary data.

This module provides functions to:
- Normalize CRS to EPSG:4326
- Fix invalid geometries using shapely.make_valid
- Compute stable geometry hashes (SHA-256 of normalized WKB)
- Ensure geometry types are Polygon/MultiPolygon
"""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING

import shapely
from shapely import make_valid
from shapely.geometry import MultiPolygon, Polygon
from shapely.wkb import dumps as wkb_dumps

if TYPE_CHECKING:
    import geopandas as gpd


TARGET_CRS = "EPSG:4326"


def normalize_crs(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Convert GeoDataFrame to EPSG:4326 (WGS84 lat/lon).

    Args:
        gdf: Input GeoDataFrame with any CRS

    Returns:
        GeoDataFrame reprojected to EPSG:4326
    """
    if gdf.crs is None:
        raise ValueError("GeoDataFrame has no CRS defined. Cannot normalize.")

    if gdf.crs.to_epsg() == 4326:
        return gdf

    return gdf.to_crs(TARGET_CRS)


def fix_geometry(geom: shapely.Geometry | None) -> shapely.Geometry | None:
    """Fix an invalid geometry using shapely.make_valid.

    Args:
        geom: Input geometry (possibly invalid)

    Returns:
        Valid geometry, or None if input was None or empty
    """
    if geom is None or geom.is_empty:
        return None

    if not geom.is_valid:
        geom = make_valid(geom)

    return geom


def ensure_polygon_type(
    geom: shapely.Geometry | None,
) -> Polygon | MultiPolygon | None:
    """Ensure geometry is a Polygon or MultiPolygon.

    Handles cases where make_valid may produce GeometryCollections
    by extracting only polygon components.

    Args:
        geom: Input geometry

    Returns:
        Polygon, MultiPolygon, or None if no polygon components exist
    """
    if geom is None or geom.is_empty:
        return None

    if isinstance(geom, (Polygon, MultiPolygon)):
        return geom

    # Handle GeometryCollection by extracting polygon components
    if geom.geom_type == "GeometryCollection":
        polygons = [g for g in geom.geoms if isinstance(g, (Polygon, MultiPolygon))]
        if not polygons:
            return None
        if len(polygons) == 1:
            return polygons[0]
        # Flatten any MultiPolygons
        all_polys = []
        for p in polygons:
            if isinstance(p, MultiPolygon):
                all_polys.extend(p.geoms)
            else:
                all_polys.append(p)
        return MultiPolygon(all_polys)

    # Other geometry types (Point, LineString, etc.) - return None
    return None


def compute_geom_hash(geom: shapely.Geometry | None) -> str | None:
    """Compute a stable SHA-256 hash of a geometry.

    Uses normalized WKB representation for consistency.

    Args:
        geom: Input geometry

    Returns:
        Hex-encoded SHA-256 hash, or None if geometry is None/empty
    """
    if geom is None or geom.is_empty:
        return None

    # Normalize coordinates to a consistent precision (6 decimal places ~11cm)
    # by rounding via set_precision
    normalized_geom = shapely.set_precision(geom, grid_size=1e-6)

    # Get WKB representation (binary, deterministic)
    wkb_data = wkb_dumps(normalized_geom)

    # Compute SHA-256 hash
    return hashlib.sha256(wkb_data).hexdigest()


def normalize_boundaries(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Apply full normalization pipeline to a GeoDataFrame of boundaries.

    This function:
    1. Normalizes CRS to EPSG:4326
    2. Fixes invalid geometries
    3. Ensures all geometries are Polygon/MultiPolygon
    4. Computes geometry hashes

    Args:
        gdf: Input GeoDataFrame with boundary geometries

    Returns:
        Normalized GeoDataFrame with 'geom_hash' column added
    """
    import geopandas as gpd

    # Step 1: Normalize CRS
    result = normalize_crs(gdf)

    # Step 2 & 3: Fix geometries and ensure polygon type
    result = result.copy()
    result["geometry"] = result["geometry"].apply(
        lambda g: ensure_polygon_type(fix_geometry(g))
    )

    # Filter out rows where geometry became None
    valid_mask = result["geometry"].notna() & ~result["geometry"].is_empty
    if not valid_mask.all():
        dropped_count = (~valid_mask).sum()
        # Log warning about dropped geometries (future: proper logging)
        import warnings

        warnings.warn(
            f"Dropped {dropped_count} rows with invalid/non-polygon geometries",
            stacklevel=2,
        )
        result = result[valid_mask].copy()

    # Rebuild GeoDataFrame to ensure proper geometry column handling
    result = gpd.GeoDataFrame(result, geometry="geometry", crs=TARGET_CRS)

    # Step 4: Compute geometry hashes
    result["geom_hash"] = result["geometry"].apply(compute_geom_hash)

    return result
