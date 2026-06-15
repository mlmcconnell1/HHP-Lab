"""PIT data ingestion and parsing modules.

This package provides:
- hud_exchange: HUD Exchange specific download/parsing
- parser: Core parsing logic for PIT files (CSV, Excel)

WP-3B deliverables:
- normalize_coc_id(): Normalize CoC IDs to ST-NNN format
- parse_pit_file(): Parse PIT files into canonical schema
- write_pit_parquet(): Write curated Parquet with provenance
"""

from hhplab.pit.ingest.hud_exchange import (
    DownloadResult,
    HudUserWafChallengeError,
    check_pit_availability,
    discover_pit_urls,
    download_pit_data,
    download_pit_data_range,
    get_pit_source_url,
    list_available_years,
    pit_source_url_candidates,
)
from hhplab.pit.ingest.parser import (
    CANONICAL_COLUMNS,
    InvalidCoCIdError,
    PITParseError,
    PITParseResult,
    PITVintageParseResult,
    get_canonical_output_path,
    get_vintage_output_path,
    normalize_coc_id,
    parse_pit_file,
    parse_pit_vintage,
    write_pit_parquet,
)

__all__ = [
    # HUD Exchange download functions
    "DownloadResult",
    "HudUserWafChallengeError",
    "check_pit_availability",
    "discover_pit_urls",
    "download_pit_data",
    "download_pit_data_range",
    "get_pit_source_url",
    "list_available_years",
    "pit_source_url_candidates",
    # Parser functions and constants
    "CANONICAL_COLUMNS",
    "InvalidCoCIdError",
    "PITParseError",
    "PITParseResult",
    "PITVintageParseResult",
    "get_canonical_output_path",
    "get_vintage_output_path",
    "normalize_coc_id",
    "parse_pit_file",
    "parse_pit_vintage",
    "write_pit_parquet",
]
