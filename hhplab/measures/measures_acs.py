"""Compatibility facade for ACS-owned aggregation helpers.

Prefer importing new ACS aggregation code from ``hhplab.acs``.  These re-exports
remain for the existing CLI/test surface and downstream callers that still use
``hhplab.measures.measures_acs``.
"""

from hhplab.acs.acs_aggregate import (
    _maybe_remap_ct_planning_regions,
    aggregate_to_coc,
    aggregate_to_geo,
)

__all__ = [
    "aggregate_to_coc",
    "aggregate_to_geo",
    "_maybe_remap_ct_planning_regions",
]
