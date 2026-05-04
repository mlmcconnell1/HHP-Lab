"""PIT (Point-in-Time) homelessness count data processing.

This package provides modules for:
- Ingesting PIT data from HUD Exchange and other sources
- Parsing and canonicalizing PIT counts
- Registry tracking for PIT vintages
- Metro aggregation utilities
- Quality assurance and validation
"""

from hhplab.pit.ingest import (
    CANONICAL_COLUMNS,
    DownloadResult,
    InvalidCoCIdError,
    PITParseError,
    PITParseResult,
    PITVintageParseResult,
    check_pit_availability,
    discover_pit_urls,
    download_pit_data,
    download_pit_data_range,
    get_canonical_output_path,
    get_pit_source_url,
    get_vintage_output_path,
    list_available_years,
    normalize_coc_id,
    parse_pit_file,
    parse_pit_vintage,
    write_pit_parquet,
)
from hhplab.pit.msa import aggregate_pit_to_msa, save_msa_pit
from hhplab.pit.pit_metro import aggregate_pit_to_metro
from hhplab.pit.pit_registry import (
    PitRegistryEntry,
    compute_file_hash,
    get_pit_path,
    latest_pit_year,
    list_pit_years,
    register_pit_year,
)
from hhplab.pit.qa import QAIssue, QAReport, Severity, validate_pit_data

__all__ = [
    "CANONICAL_COLUMNS",
    "DownloadResult",
    "InvalidCoCIdError",
    "PITParseError",
    "PITParseResult",
    "PITVintageParseResult",
    "QAReport",
    "QAIssue",
    "Severity",
    "aggregate_pit_to_metro",
    "aggregate_pit_to_msa",
    "check_pit_availability",
    "PitRegistryEntry",
    "compute_file_hash",
    "discover_pit_urls",
    "download_pit_data",
    "download_pit_data_range",
    "get_canonical_output_path",
    "get_pit_path",
    "get_pit_source_url",
    "get_vintage_output_path",
    "latest_pit_year",
    "list_available_years",
    "list_pit_years",
    "normalize_coc_id",
    "parse_pit_file",
    "parse_pit_vintage",
    "register_pit_year",
    "validate_pit_data",
    "write_pit_parquet",
    "save_msa_pit",
]
