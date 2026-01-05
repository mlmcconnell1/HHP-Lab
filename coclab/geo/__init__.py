"""Geometry normalization and I/O utilities."""

from coclab.geo.io import read_geoparquet, write_geoparquet
from coclab.geo.normalize import (
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
]
