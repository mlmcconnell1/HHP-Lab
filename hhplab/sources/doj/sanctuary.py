"""Compatibility facade for DOJ sanctuary source workflows.

Implementation is separated into acquisition, jurisdiction contracts, matching,
and panel-covariate materialization modules while historical imports remain
stable.
"""

from __future__ import annotations

import hhplab.sources.doj.materialize as _materialize
from hhplab.geographies.msa import read_msa_county_membership, read_msa_definitions
from hhplab.sources.census.pep.pep_aggregate import load_pep_county

from .acquisition import (
    DOJ_SANCTUARY_SOURCE_DATE,
    DOJ_SANCTUARY_URL,
    RAW_SANCTUARY_HTML_FILENAME,
    download_doj_sanctuary_page,
)
from .contracts import (
    DOJ_LISTED_CITIES,
    DOJ_LISTED_COUNTIES,
    DOJ_LISTED_STATES,
    SANCTUARY_MSA_MATCH_COLUMNS,
    SANCTUARY_MSA_PANEL_COLUMNS,
    CityDesignation,
    CountyDesignation,
)
from .matching import (
    build_sanctuary_msa_matches,
)
from .materialize import build_sanctuary_msa_panel_covariate as _build_panel_covariate

__all__ = [
    "CityDesignation",
    "CountyDesignation",
    "DOJ_LISTED_CITIES",
    "DOJ_LISTED_COUNTIES",
    "DOJ_LISTED_STATES",
    "DOJ_SANCTUARY_SOURCE_DATE",
    "DOJ_SANCTUARY_URL",
    "RAW_SANCTUARY_HTML_FILENAME",
    "SANCTUARY_MSA_MATCH_COLUMNS",
    "SANCTUARY_MSA_PANEL_COLUMNS",
    "build_sanctuary_msa_matches",
    "build_sanctuary_msa_panel_covariate",
    "download_doj_sanctuary_page",
    "write_sanctuary_msa_matches",
    "write_sanctuary_msa_panel_covariate",
]


def build_sanctuary_msa_panel_covariate(*args, **kwargs):
    """Compatibility wrapper for panel-covariate materialization."""
    _sync_materialize_dependencies()
    return _build_panel_covariate(*args, **kwargs)


def _sync_materialize_dependencies() -> None:
    """Keep monkeypatchable historical dependencies working through the facade."""
    _materialize.download_doj_sanctuary_page = download_doj_sanctuary_page
    _materialize.read_msa_definitions = read_msa_definitions
    _materialize.read_msa_county_membership = read_msa_county_membership
    _materialize.load_pep_county = load_pep_county


def write_sanctuary_msa_matches(*args, **kwargs):
    """Compatibility wrapper for matching artifact persistence."""
    _sync_materialize_dependencies()
    return _materialize.write_sanctuary_msa_matches(*args, **kwargs)


def write_sanctuary_msa_panel_covariate(*args, **kwargs):
    """Compatibility wrapper for panel-covariate artifact persistence."""
    _sync_materialize_dependencies()
    return _materialize.write_sanctuary_msa_panel_covariate(*args, **kwargs)
