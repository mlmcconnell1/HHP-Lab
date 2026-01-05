"""ACS data ingestion subpackage.

Provides functions for fetching and caching ACS data from the Census Bureau API.
"""

from coclab.acs.ingest.tract_population import (
    fetch_tract_population,
    ingest_tract_population,
)

__all__ = ["fetch_tract_population", "ingest_tract_population"]
