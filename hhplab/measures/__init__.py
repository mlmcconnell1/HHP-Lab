"""Compatibility facade for measure aggregation and diagnostics.

New source-specific aggregation code should live with its owning source package
(for example ``hhplab.acs``).  This package remains as the stable import surface
for legacy ACS measure aggregation and crosswalk/measure diagnostics used by
CLI commands and tests.
"""

from hhplab.acs.variables import ACS_VARS, ADULT_VARS
from hhplab.measures.measures_acs import (
    aggregate_to_coc,
    aggregate_to_geo,
)
from hhplab.measures.measures_diagnostics import (
    compute_crosswalk_diagnostics,
    compute_measure_diagnostics,
    identify_problem_cocs,
    summarize_diagnostics,
)

__all__ = [
    "ACS_VARS",
    "ADULT_VARS",
    "aggregate_to_coc",
    "aggregate_to_geo",
    "compute_crosswalk_diagnostics",
    "compute_measure_diagnostics",
    "identify_problem_cocs",
    "summarize_diagnostics",
]
