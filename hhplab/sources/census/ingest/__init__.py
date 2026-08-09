"""Census data-product ingestion entry points."""

from .decennial_tract_population import (
    fetch_decennial_tract_population,
    get_output_path,
    ingest_decennial_tract_population,
)
from .pl_block_population import ingest_pl_block_population

__all__ = [
    "fetch_decennial_tract_population",
    "get_output_path",
    "ingest_decennial_tract_population",
    "ingest_pl_block_population",
]
