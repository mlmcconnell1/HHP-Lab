"""GeoParquet I/O utilities for CoC boundary data.

This module provides helper functions for reading and writing GeoParquet files,
following the conventions defined in the boundary infrastructure plan.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import geopandas as gpd


def read_geoparquet(path: Path | str) -> gpd.GeoDataFrame:
    """Read a GeoParquet file into a GeoDataFrame.

    Args:
        path: Path to the GeoParquet file

    Returns:
        GeoDataFrame with geometry column

    Raises:
        FileNotFoundError: If the file doesn't exist
    """
    import geopandas as gpd

    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"GeoParquet file not found: {path}")

    return gpd.read_parquet(path)


def write_geoparquet(
    gdf: gpd.GeoDataFrame,
    path: Path | str,
    *,
    compression: str = "snappy",
) -> Path:
    """Write a GeoDataFrame to GeoParquet format.

    Creates parent directories if they don't exist.

    Args:
        gdf: GeoDataFrame to write
        path: Output path for the GeoParquet file
        compression: Compression algorithm (default: snappy)

    Returns:
        Path to the written file
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    gdf.to_parquet(path, compression=compression)

    return path


def curated_boundary_path(boundary_vintage: str, base_dir: Path | str | None = None) -> Path:
    """Get the canonical path for a curated boundary file.

    Uses the preferred ``coc__BYYYY`` naming convention.

    Args:
        boundary_vintage: Version identifier (e.g., "2025", "HUDOpenData_2025-08-19")
        base_dir: Base data directory (defaults to "data")

    Returns:
        Path like data/curated/coc_boundaries/coc__B2025.parquet
    """
    from coclab.naming import coc_base_path

    return coc_base_path(boundary_vintage, base_dir)


def resolve_curated_boundary_path(
    boundary_vintage: str, base_dir: Path | str | None = None
) -> Path:
    """Resolve an existing curated boundary file across supported naming schemes.

    Preference order:
    1. coc__B{vintage}.parquet
    2. boundaries__B{vintage}.parquet
    3. coc_boundaries__{vintage}.parquet (legacy)
    """
    from coclab.naming import boundary_filename, coc_base_filename

    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)

    boundaries_dir = base_dir / "curated" / "coc_boundaries"
    candidates = [
        boundaries_dir / coc_base_filename(boundary_vintage),
        boundaries_dir / boundary_filename(boundary_vintage),
        boundaries_dir / f"coc_boundaries__{boundary_vintage}.parquet",
    ]

    for candidate in candidates:
        if candidate.exists():
            return candidate

    tried = ", ".join(str(path) for path in candidates)
    raise FileNotFoundError(
        f"Boundary file not found for vintage '{boundary_vintage}'. Tried: {tried}"
    )


def registry_path(base_dir: Path | str | None = None) -> Path:
    """Get the path to the boundary registry file.

    Args:
        base_dir: Base data directory (defaults to "data")

    Returns:
        Path like data/curated/boundary_registry.parquet
    """
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)

    return base_dir / "curated" / "boundary_registry.parquet"
