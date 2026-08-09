"""Compatibility facade for the retired measures diagnostics package."""

from hhplab.measures.measures_diagnostics import (
    compute_crosswalk_diagnostics,
    compute_measure_diagnostics,
    identify_problem_cocs,
    summarize_diagnostics,
)

__all__ = [
    "compute_crosswalk_diagnostics",
    "compute_measure_diagnostics",
    "identify_problem_cocs",
    "summarize_diagnostics",
]
