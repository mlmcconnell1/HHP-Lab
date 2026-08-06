"""Geospatial utilities for CoC boundary processing."""

from hhplab.geographies.coc.coc_io import read_geoparquet, write_geoparquet
from hhplab.geographies.coc.coc_validate import ValidationResult, validate_boundaries
from hhplab.geographies.coc.normalize import (
    compute_geom_hash,
    ensure_polygon_type,
    fix_geometry,
    normalize_boundaries,
    normalize_crs,
)

__all__ = [
    "normalize_boundaries",
    "normalize_crs",
    "fix_geometry",
    "compute_geom_hash",
    "ensure_polygon_type",
    "read_geoparquet",
    "write_geoparquet",
    "validate_boundaries",
    "ValidationResult",
]
