"""Compatibility facade for geography crosswalk diagnostics.

The implementation now lives in :mod:`hhplab.geographies.xwalks.diagnostics`.
"""

from hhplab.geographies.xwalks.diagnostics import (
    compute_crosswalk_diagnostics,
    compute_measure_diagnostics,
    identify_problem_cocs,
    identify_problem_geos,
    summarize_diagnostics,
)

__all__ = [
    "compute_crosswalk_diagnostics",
    "compute_measure_diagnostics",
    "identify_problem_cocs",
    "identify_problem_geos",
    "summarize_diagnostics",
]
