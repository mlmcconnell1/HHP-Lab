"""Geography and boundary artifact naming."""

from __future__ import annotations

from pathlib import Path

__all__ = [
    "block_geometry_filename",
    "block_geometry_path",
    "boundary_filename",
    "boundary_path",
    "coc_base_filename",
    "coc_base_path",
    "county_filename",
    "county_path",
    "tract_filename",
    "tract_path",
    "urban_area_filename",
    "urban_area_path",
]

def block_geometry_filename(block_vintage: str | int) -> str:
    """Generate filename for Census tabulation block geometry.

    Args:
        block_vintage: Census block geometry vintage (e.g., 2020)

    Returns:
        Filename like ``blocks__K2020.parquet``
    """
    return f"blocks__K{block_vintage}.parquet"

def block_geometry_path(
    block_vintage: str | int,
    base_dir: Path | str | None = None,
) -> Path:
    """Get canonical path for Census block geometry."""
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return base_dir / "curated" / "tiger" / block_geometry_filename(block_vintage)

def boundary_filename(boundary_vintage: str) -> str:
    """Generate filename for CoC boundary data.

    Args:
        boundary_vintage: Boundary vintage year (e.g., "2025")

    Returns:
        Filename like 'boundaries__B2025.parquet'
    """
    return f"boundaries__B{boundary_vintage}.parquet"

def boundary_path(boundary_vintage: str, base_dir: Path | str | None = None) -> Path:
    """Get canonical path for curated boundary file.

    .. deprecated::
        Use :func:`coc_base_path` instead. This function uses the legacy
        ``boundaries__B`` naming convention.

    Args:
        boundary_vintage: Boundary vintage year
        base_dir: Base data directory (defaults to "data")

    Returns:
        Path like data/curated/coc_boundaries/boundaries__B2025.parquet
    """
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return base_dir / "curated" / "coc_boundaries" / boundary_filename(boundary_vintage)

def coc_base_filename(boundary_vintage: str) -> str:
    """Generate filename for CoC base boundary data.

    Args:
        boundary_vintage: Boundary vintage year (e.g., "2025")

    Returns:
        Filename like 'coc__B2025.parquet'
    """
    return f"coc__B{boundary_vintage}.parquet"

def coc_base_path(boundary_vintage: str, base_dir: Path | str | None = None) -> Path:
    """Get canonical path for curated CoC boundary file using preferred naming.

    Args:
        boundary_vintage: Boundary vintage year (e.g., "2025")
        base_dir: Base data directory (defaults to "data")

    Returns:
        Path like data/curated/coc_boundaries/coc__B2025.parquet
    """
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return base_dir / "curated" / "coc_boundaries" / coc_base_filename(boundary_vintage)

def county_filename(county_vintage: str | int) -> str:
    """Generate filename for census county geometry.

    Args:
        county_vintage: TIGER county vintage year (e.g., 2023 or "2023")

    Returns:
        Filename like 'counties__C2023.parquet'
    """
    return f"counties__C{county_vintage}.parquet"

def county_path(county_vintage: str | int, base_dir: Path | str | None = None) -> Path:
    """Get canonical path for census county file.

    Args:
        county_vintage: TIGER county vintage year
        base_dir: Base data directory (defaults to "data")

    Returns:
        Path like data/curated/tiger/counties__C2023.parquet
    """
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return base_dir / "curated" / "tiger" / county_filename(county_vintage)

def tract_filename(tract_vintage: str | int) -> str:
    """Generate filename for census tract geometry.

    Args:
        tract_vintage: TIGER tract vintage year (e.g., 2023 or "2023")

    Returns:
        Filename like 'tracts__T2023.parquet'
    """
    return f"tracts__T{tract_vintage}.parquet"

def tract_path(tract_vintage: str | int, base_dir: Path | str | None = None) -> Path:
    """Get canonical path for census tract file.

    Args:
        tract_vintage: TIGER tract vintage year
        base_dir: Base data directory (defaults to "data")

    Returns:
        Path like data/curated/tiger/tracts__T2023.parquet
    """
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return base_dir / "curated" / "tiger" / tract_filename(tract_vintage)

def urban_area_filename(urban_area_vintage: str | int) -> str:
    """Generate filename for Census Urban Area geometry.

    Args:
        urban_area_vintage: Urban Area vintage year (e.g., 2010 or 2020)

    Returns:
        Filename like ``urban_areas__U2020.parquet``
    """
    return f"urban_areas__U{urban_area_vintage}.parquet"

def urban_area_path(
    urban_area_vintage: str | int,
    base_dir: Path | str | None = None,
) -> Path:
    """Get canonical path for Census Urban Area geometry."""
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return base_dir / "curated" / "tiger" / urban_area_filename(urban_area_vintage)

