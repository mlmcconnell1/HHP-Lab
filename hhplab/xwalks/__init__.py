"""Crosswalk generation for analysis geographies.

Provides geometry-neutral crosswalk builders (``build_tract_crosswalk``,
``build_county_crosswalk``) and backward-compatible CoC-specific wrappers.
"""

from hhplab.xwalks.county import build_coc_county_crosswalk, build_county_crosswalk
from hhplab.xwalks.tract import (
    add_population_weights,
    build_coc_tract_crosswalk,
    build_tract_crosswalk,
    save_crosswalk,
    validate_population_shares,
)
from hhplab.xwalks.tract_mediated import (
    build_tract_mediated_county_crosswalk,
    save_tract_mediated_county_crosswalk,
)

__all__ = [
    "build_tract_crosswalk",
    "build_coc_tract_crosswalk",
    "build_county_crosswalk",
    "build_coc_county_crosswalk",
    "save_crosswalk",
    "add_population_weights",
    "validate_population_shares",
    "build_tract_mediated_county_crosswalk",
    "save_tract_mediated_county_crosswalk",
]
