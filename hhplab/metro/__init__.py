"""Shared metro-universe, subset-profile, IO, and validation utilities.

Source-specific metro aggregations live under their owning source packages
(``hhplab.acs``, ``hhplab.pep``, ``hhplab.pit``, ``hhplab.rents``). This
package is limited to shared geography artifacts and validation helpers.
"""

from hhplab.metro.metro_boundaries import (
    build_metro_boundaries,
    generate_metro_boundaries,
    read_metro_boundaries,
    validate_curated_metro_boundaries,
    write_metro_boundaries,
)
from hhplab.metro.metro_definitions import (
    CANONICAL_UNIVERSE_DEFINITION_VERSION,
    DEFINITION_VERSION,
    METRO_CBSA_MAPPING,
    METRO_COC_MEMBERSHIP,
    METRO_COUNT,
    METRO_COUNTY_MEMBERSHIP,
    METRO_DEFINITIONS,
    METRO_STATE_FIPS,
    PROFILE_NAME,
    build_coc_membership_df,
    build_county_membership_df,
    build_definitions_df,
    build_glynn_fox_subset_profile_df,
    build_metro_universe_df,
    cbsa_to_metro_id,
    metro_name_for_id,
)
from hhplab.metro.metro_io import (
    read_metro_coc_membership,
    read_metro_county_membership,
    read_metro_definitions,
    read_metro_subset_membership,
    read_metro_universe,
    validate_curated_metro,
    validate_curated_metro_universe,
    write_metro_artifacts,
    write_metro_universe_artifacts,
)
from hhplab.metro.metro_validate import (
    MetroValidationResult,
    validate_metro_artifacts,
    validate_metro_boundaries,
    validate_metro_universe_artifacts,
)

__all__ = [
    "DEFINITION_VERSION",
    "CANONICAL_UNIVERSE_DEFINITION_VERSION",
    "PROFILE_NAME",
    "METRO_CBSA_MAPPING",
    "METRO_STATE_FIPS",
    "METRO_COUNT",
    "METRO_DEFINITIONS",
    "METRO_COC_MEMBERSHIP",
    "METRO_COUNTY_MEMBERSHIP",
    "build_definitions_df",
    "build_coc_membership_df",
    "build_county_membership_df",
    "build_metro_universe_df",
    "build_glynn_fox_subset_profile_df",
    "build_metro_boundaries",
    "cbsa_to_metro_id",
    "generate_metro_boundaries",
    "metro_name_for_id",
    "read_metro_definitions",
    "read_metro_coc_membership",
    "read_metro_county_membership",
    "read_metro_universe",
    "read_metro_subset_membership",
    "read_metro_boundaries",
    "write_metro_artifacts",
    "write_metro_universe_artifacts",
    "write_metro_boundaries",
    "validate_curated_metro",
    "validate_curated_metro_universe",
    "validate_curated_metro_boundaries",
    "MetroValidationResult",
    "validate_metro_artifacts",
    "validate_metro_boundaries",
    "validate_metro_universe_artifacts",
]
