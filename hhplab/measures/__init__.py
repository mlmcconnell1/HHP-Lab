"""Measure diagnostics package.

Source-specific aggregation code lives with its owning source package
(for example ``hhplab.sources.census.acs``).  This package owns crosswalk and measure
diagnostics used by CLI commands and tests.
"""

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
