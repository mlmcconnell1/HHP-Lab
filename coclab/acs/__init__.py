"""ACS (American Community Survey) data ingestion and management.

This package provides tools for fetching and caching ACS data from the Census Bureau API,
including tract-level population data needed for CoC analysis, and aggregation to CoC level.
"""

from coclab.acs.crosscheck import (
    CrosscheckResult,
    crosscheck_population,
    print_crosscheck_report,
    run_crosscheck,
)
from coclab.acs.ingest.tract_population import (
    fetch_tract_population,
    ingest_tract_population,
)
from coclab.acs.rollup import (
    build_coc_population_rollup,
    rollup_tract_population,
)

__all__ = [
    "fetch_tract_population",
    "ingest_tract_population",
    "build_coc_population_rollup",
    "rollup_tract_population",
    "CrosscheckResult",
    "crosscheck_population",
    "run_crosscheck",
    "print_crosscheck_report",
]
