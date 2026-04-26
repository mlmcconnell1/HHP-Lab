"""Shared Glynn/Fox metro definitions, IO, and validation utilities."""

from coclab.metro.definitions import (
    DEFINITION_VERSION,
    METRO_CBSA_MAPPING,
    METRO_COC_MEMBERSHIP,
    METRO_COUNT,
    METRO_COUNTY_MEMBERSHIP,
    METRO_DEFINITIONS,
    METRO_STATE_FIPS,
    build_coc_membership_df,
    build_county_membership_df,
    build_definitions_df,
    cbsa_to_metro_id,
    metro_name_for_id,
)
from coclab.metro.io import (
    read_metro_coc_membership,
    read_metro_county_membership,
    read_metro_definitions,
    validate_curated_metro,
    write_metro_artifacts,
)
from coclab.metro.validate import MetroValidationResult, validate_metro_artifacts

__all__ = [
    "DEFINITION_VERSION",
    "METRO_CBSA_MAPPING",
    "METRO_STATE_FIPS",
    "METRO_COUNT",
    "METRO_DEFINITIONS",
    "METRO_COC_MEMBERSHIP",
    "METRO_COUNTY_MEMBERSHIP",
    "build_definitions_df",
    "build_coc_membership_df",
    "build_county_membership_df",
    "cbsa_to_metro_id",
    "metro_name_for_id",
    "read_metro_definitions",
    "read_metro_coc_membership",
    "read_metro_county_membership",
    "write_metro_artifacts",
    "validate_curated_metro",
    "MetroValidationResult",
    "validate_metro_artifacts",
]
