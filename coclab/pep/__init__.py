"""PEP (Population Estimates Program) data ingestion and aggregation.

Provides functions for downloading, normalizing, and aggregating
Census Bureau Population Estimates Program county-level data.
"""

from coclab.pep.aggregate import aggregate_pep_counties, aggregate_pep_to_coc
from coclab.pep.ingest import ingest_pep_county
from coclab.pep.metro import aggregate_pep_to_metro

__all__ = [
    "ingest_pep_county",
    "aggregate_pep_counties",
    "aggregate_pep_to_coc",
    "aggregate_pep_to_metro",
]
