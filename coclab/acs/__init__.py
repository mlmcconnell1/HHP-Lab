"""ACS (American Community Survey) data ingestion and management.

This package provides tools for fetching and caching ACS data from the Census Bureau API,
including tract-level population, income, rent, poverty, and demographic data needed for
CoC analysis.
"""

from coclab.acs.ingest.tract_population import (
    fetch_state_tract_data,
    fetch_tract_data,
    get_output_path,
    ingest_tract_data,
)
from coclab.acs.metro import aggregate_acs_to_metro, build_metro_tract_crosswalk
from coclab.acs.translate import (
    TranslationStats,
    get_source_tract_vintage,
    needs_translation,
    translate_acs_to_target_vintage,
    translate_tracts_2010_to_2020,
)

__all__ = [
    "fetch_state_tract_data",
    "fetch_tract_data",
    "get_output_path",
    "ingest_tract_data",
    # Metro aggregation
    "aggregate_acs_to_metro",
    "build_metro_tract_crosswalk",
    # Translation
    "TranslationStats",
    "get_source_tract_vintage",
    "needs_translation",
    "translate_acs_to_target_vintage",
    "translate_tracts_2010_to_2020",
]
