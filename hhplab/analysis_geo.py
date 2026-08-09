"""Compatibility facade for the canonical analysis-geography contracts.

New internal code should import from :mod:`hhplab.geographies.analysis`.
This module remains supported for downstream callers during the migration.
"""

from hhplab.geographies.analysis import (
    COC_ID_COL,
    GEO_ID_COL,
    GEO_TYPE_COC,
    GEO_TYPE_COL,
    GEO_TYPE_METRO,
    GEO_TYPE_MSA,
    METRO_ID_COL,
    MSA_ID_COL,
    VALID_GEO_TYPES,
    AnalysisGeometryRef,
    ensure_canonical_geo_columns,
    infer_geo_type,
    resolve_geo_col,
)

__all__ = [
    "AnalysisGeometryRef",
    "COC_ID_COL",
    "GEO_ID_COL",
    "GEO_TYPE_COC",
    "GEO_TYPE_COL",
    "GEO_TYPE_METRO",
    "GEO_TYPE_MSA",
    "METRO_ID_COL",
    "MSA_ID_COL",
    "VALID_GEO_TYPES",
    "ensure_canonical_geo_columns",
    "infer_geo_type",
    "resolve_geo_col",
]
