"""Tests for deterministic Census MSA selector helpers.

Truth table for 2024 selector fixtures:

| MSA   | Counties      | PEP population | ACS5 county population | ACS5 tract population |
|-------|---------------|----------------|------------------------|-----------------------|
| 11111 | 01001, 01003  | 100 partial    | 330                    | 330                   |
| 22222 | 02001         | 400            | 390                    | 390                   |
| 33333 | 03001         | missing        | 300                    | 300                   |
| 44444 | 04001         | 400            | 390                    | 390                   |

Top-2 PEP ordering is 22222, 44444 because both tie at 400 and the selector
breaks ties by MSA ID. MSA 33333 is reported missing for PEP coverage.
Top-2 ACS5 county and tract ordering is 22222, 44444 for the same
deterministic tie.
"""

from __future__ import annotations

import pandas as pd
import pytest

from hhplab.geographies.msa.selectors import (
    TopMsaSelection,
    select_top_msa_ids_by_population,
    select_top_msa_ids_for_panel_spec,
)
from hhplab.recipe.recipe_schema import MsaCocPanelSpec

REFERENCE_YEAR = 2024
MEMBERSHIP_ROWS: tuple[dict[str, str], ...] = (
    {"msa_id": "11111", "county_fips": "01001"},
    {"msa_id": "11111", "county_fips": "01003"},
    {"msa_id": "22222", "county_fips": "02001"},
    {"msa_id": "33333", "county_fips": "03001"},
    {"msa_id": "44444", "county_fips": "04001"},
)
PEP_ROWS: tuple[dict[str, object], ...] = (
    {"county_fips": "01001", "year": 2024, "population": 100},
    {"county_fips": "02001", "year": 2024, "population": 400},
    {"county_fips": "04001", "year": 2024, "population": 400},
    {"county_fips": "01001", "year": 2023, "population": 9000},
)
PEP_MSA_ROWS: tuple[dict[str, object], ...] = (
    {"msa_id": "11111", "year": 2024, "population": 100},
    {"msa_id": "22222", "year": 2024, "population": 400},
    {"msa_id": "44444", "year": 2024, "population": 400},
    {"msa_id": "11111", "year": 2023, "population": 9000},
)
ACS5_TRACT_ROWS: tuple[dict[str, object], ...] = (
    {"tract_geoid": "01001000100", "year": 2024, "total_population": 110},
    {"tract_geoid": "01003000100", "year": 2024, "total_population": 220},
    {"tract_geoid": "02001000100", "year": 2024, "total_population": 390},
    {"tract_geoid": "03001000100", "year": 2024, "total_population": 300},
    {"tract_geoid": "04001000100", "year": 2024, "total_population": 390},
)
ACS5_COUNTY_ROWS: tuple[dict[str, object], ...] = (
    {"county_fips": "01001", "year": 2024, "total_population": 110},
    {"county_fips": "01003", "year": 2024, "total_population": 220},
    {"county_fips": "02001", "year": 2024, "total_population": 390},
    {"county_fips": "03001", "year": 2024, "total_population": 300},
    {"county_fips": "04001", "year": 2024, "total_population": 390},
)
EXPECTED_PEP_RANKING: tuple[tuple[str, float], ...] = (
    ("22222", 400.0),
    ("44444", 400.0),
    ("11111", 100.0),
)
EXPECTED_ACS5_RANKING: tuple[tuple[str, float], ...] = (
    ("22222", 390.0),
    ("44444", 390.0),
    ("11111", 330.0),
    ("33333", 300.0),
)


def _membership() -> pd.DataFrame:
    return pd.DataFrame(MEMBERSHIP_ROWS)


def _pep() -> pd.DataFrame:
    return pd.DataFrame(PEP_ROWS)


def _pep_msa() -> pd.DataFrame:
    return pd.DataFrame(PEP_MSA_ROWS)


def _acs5_tracts() -> pd.DataFrame:
    return pd.DataFrame(ACS5_TRACT_ROWS)


def _acs5_counties() -> pd.DataFrame:
    return pd.DataFrame(ACS5_COUNTY_ROWS)


@pytest.mark.parametrize(
    "ranking_source,population_df,population_column,expected_ranking",
    [
        ("pep", _pep(), "population", EXPECTED_PEP_RANKING),
        ("pep", _pep_msa(), "population", EXPECTED_PEP_RANKING),
        ("acs5", _acs5_counties(), "total_population", EXPECTED_ACS5_RANKING),
        ("acs5", _acs5_tracts(), "total_population", EXPECTED_ACS5_RANKING),
    ],
    ids=["pep-county", "pep-msa", "acs5-county", "acs5-tract"],
)
def test_top_msa_selector_rolls_population_to_msa(
    ranking_source: str,
    population_df: pd.DataFrame,
    population_column: str,
    expected_ranking: tuple[tuple[str, float], ...],
) -> None:
    selection = select_top_msa_ids_by_population(
        population_df,
        _membership(),
        ranking_source=ranking_source,
        reference_year=REFERENCE_YEAR,
        top_n=2,
        population_column=population_column,
    )

    assert isinstance(selection, TopMsaSelection)
    assert selection.selector_ids == ("22222", "44444")
    assert (
        tuple(selection.ranking[["msa_id", "population"]].itertuples(index=False, name=None))
        == expected_ranking
    )


def test_top_msa_selector_reports_missing_coverage_and_cutoff_ties() -> None:
    selection = select_top_msa_ids_by_population(
        _pep(),
        _membership(),
        ranking_source="pep",
        reference_year=REFERENCE_YEAR,
        top_n=1,
    )

    diagnostics = selection.diagnostics
    assert diagnostics.eligible_msa_count == 3
    assert diagnostics.selected_count == 1
    assert diagnostics.expected_msa_count == 4
    assert diagnostics.missing_msa_ids == ("33333",)
    assert diagnostics.incomplete_msa_ids == ("11111",)
    assert diagnostics.tied_cutoff is True
    assert diagnostics.cutoff_population == 400.0
    assert diagnostics.tied_cutoff_msa_ids == ("22222", "44444")


def test_top_msa_selector_for_recipe_panel_spec_uses_declarative_fields() -> None:
    spec = MsaCocPanelSpec(
        top_n=2,
        ranking_population_source="acs5",
        ranking_reference_year=REFERENCE_YEAR,
        containment_min_share=0.99,
        coc_boundary_vintage=2025,
        msa_definition_version="census_msa_2023",
        msa_population_source="pep",
        unemployment_source="acs5",
    )

    selection = select_top_msa_ids_for_panel_spec(
        spec,
        _acs5_tracts(),
        _membership(),
        population_column="total_population",
    )

    assert selection.selector_ids == ("22222", "44444")
    assert selection.diagnostics.ranking_source == "acs5"
    assert selection.diagnostics.reference_year == REFERENCE_YEAR


@pytest.mark.parametrize(
    "kwargs,match",
    [
        ({"top_n": 0}, "top_n"),
        ({"ranking_source": "decennial"}, "ranking_source"),
        ({"reference_year": 1999}, "reference_year"),
        ({"population_column": "missing_population"}, "missing column"),
    ],
    ids=["bad-top-n", "bad-source", "missing-year", "missing-population"],
)
def test_top_msa_selector_rejects_invalid_inputs(kwargs: dict[str, object], match: str) -> None:
    params = {
        "ranking_source": "pep",
        "reference_year": REFERENCE_YEAR,
        "top_n": 2,
        "population_column": "population",
    }
    params.update(kwargs)

    with pytest.raises(ValueError, match=match):
        select_top_msa_ids_by_population(
            _pep(),
            _membership(),
            **params,
        )
