"""ACS (American Community Survey) data ingestion and management.

This package provides tools for fetching and caching ACS data from the Census Bureau API,
including tract-level population data needed for CoC analysis.
"""

from coclab.acs.ingest.tract_population import (
    fetch_tract_population,
    ingest_tract_population,
)
from coclab.acs.translate import (
    TranslationStats,
    get_source_tract_vintage,
    needs_translation,
    translate_acs_to_target_vintage,
    translate_tracts_2010_to_2020,
)

__all__ = [
    "fetch_tract_population",
    "ingest_tract_population",
    # Translation
    "TranslationStats",
    "get_source_tract_vintage",
    "needs_translation",
    "translate_acs_to_target_vintage",
    "translate_tracts_2010_to_2020",
]
