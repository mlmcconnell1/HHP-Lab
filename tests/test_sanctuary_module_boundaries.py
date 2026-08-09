"""Focused ownership contracts for DOJ sanctuary source modules."""

from __future__ import annotations

from importlib import import_module

import pytest

import hhplab.sources.doj.sanctuary as sanctuary

SANCTUARY_OWNER_CASES = [
    pytest.param("download_doj_sanctuary_page", "acquisition", id="acquisition"),
    pytest.param("DOJ_LISTED_CITIES", "contracts", id="contracts"),
    pytest.param("build_sanctuary_msa_matches", "matching", id="matching"),
]


@pytest.mark.parametrize(("name", "owner"), SANCTUARY_OWNER_CASES)
def test_sanctuary_facade_reexports_owned_symbols(name: str, owner: str) -> None:
    assert getattr(sanctuary, name) is getattr(
        import_module(f"hhplab.sources.doj.{owner}"), name
    )


def test_sanctuary_contract_columns_are_explicit_and_stable() -> None:
    assert sanctuary.SANCTUARY_MSA_MATCH_COLUMNS == (
        "cbsa_code",
        "msa_name",
        "state_match",
        "county_match",
        "city_match",
        "matched_states",
        "matched_counties",
        "matched_cities",
        "match_basis",
    )
    assert sanctuary.SANCTUARY_MSA_PANEL_COLUMNS[-1] == "doj_sanctuary_source_date"
