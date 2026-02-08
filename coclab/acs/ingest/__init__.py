"""ACS data ingestion subpackage.

Provides functions for fetching and caching ACS data from the Census Bureau API.
"""

from coclab.acs.ingest.tract_population import (
    fetch_state_tract_data,
    fetch_tract_data,
    get_output_path,
    ingest_tract_data,
)

__all__ = [
    "fetch_state_tract_data",
    "fetch_tract_data",
    "get_output_path",
    "ingest_tract_data",
]
