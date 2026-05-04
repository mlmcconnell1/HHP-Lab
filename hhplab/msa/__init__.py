"""Shared Census MSA definitions, IO, and validation utilities."""

from hhplab.msa.crosswalk import (
    ALLOCATION_SHARE_TOLERANCE,
    COC_MSA_CROSSWALK_COLUMNS,
    FULL_ALLOCATION_THRESHOLD,
    build_coc_msa_crosswalk,
    read_coc_msa_crosswalk,
    save_coc_msa_crosswalk,
    summarize_coc_msa_allocation,
)
from hhplab.msa.msa_boundaries import (
    download_msa_boundaries,
    ingest_msa_boundaries,
    read_msa_boundaries,
    validate_curated_msa_boundaries,
    write_msa_boundaries,
)
from hhplab.msa.msa_definitions import (
    DEFINITION_VERSION,
    DELINEATION_FILE_YEAR,
    MSA_AREA_TYPE,
    SOURCE_NAME,
    SOURCE_REF,
    WORKBOOK_FILENAME,
    build_county_membership_df,
    build_definitions_df,
    parse_delineation_workbook,
)
from hhplab.msa.msa_io import (
    download_delineation_rows,
    read_msa_county_membership,
    read_msa_definitions,
    validate_curated_msa,
    write_msa_artifacts,
)
from hhplab.msa.msa_validate import (
    MSAValidationResult,
    validate_msa_artifacts,
    validate_msa_boundaries,
)

__all__ = [
    "ALLOCATION_SHARE_TOLERANCE",
    "COC_MSA_CROSSWALK_COLUMNS",
    "DEFINITION_VERSION",
    "DELINEATION_FILE_YEAR",
    "FULL_ALLOCATION_THRESHOLD",
    "MSA_AREA_TYPE",
    "SOURCE_NAME",
    "SOURCE_REF",
    "WORKBOOK_FILENAME",
    "parse_delineation_workbook",
    "build_definitions_df",
    "build_county_membership_df",
    "build_coc_msa_crosswalk",
    "download_msa_boundaries",
    "download_delineation_rows",
    "ingest_msa_boundaries",
    "read_coc_msa_crosswalk",
    "read_msa_boundaries",
    "read_msa_definitions",
    "read_msa_county_membership",
    "save_coc_msa_crosswalk",
    "summarize_coc_msa_allocation",
    "write_msa_boundaries",
    "write_msa_artifacts",
    "validate_curated_msa",
    "validate_curated_msa_boundaries",
    "MSAValidationResult",
    "validate_msa_artifacts",
    "validate_msa_boundaries",
]
