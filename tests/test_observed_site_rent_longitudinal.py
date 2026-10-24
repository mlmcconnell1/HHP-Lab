"""Timing and observation invariants for the ACS1 gross-rent diagnostic.

Fixture truth table: PIT2010 matches ACS2009; PIT2011 has no matching ACS2010;
PIT2012 matches ACS2011 even though its PIT observation is ineligible. Missing
rent never drops a frozen row, and rent predictors do not require observed PIT.
"""

import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

SCRIPT_PATH = (
    Path(__file__).resolve().parents[1] / "scripts/analyze_observed_site_rent_longitudinal.py"
)
SPEC = importlib.util.spec_from_file_location("observed_site_rent_longitudinal", SCRIPT_PATH)
ANALYSIS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ANALYSIS)

BASE_YEARS = [2010, 2011, 2012]
ACS_YEARS = [2009, 2011]
ACS_RENTS = [700, 900]


@pytest.mark.parametrize("rent_dtype", ["float64", "Int64"])
def test_rent_alignment_preserves_universe_and_observation_flags(rent_dtype):
    base = pd.DataFrame({
        "msa_id": ["site"] * len(BASE_YEARS), "year": BASE_YEARS,
        "pit_unsheltered": [10, 20, 30], "unsheltered_observed": [True, True, False],
    })
    rents = pd.DataFrame({
        "msa_id": ["site"] * len(ACS_YEARS), "acs_year": ACS_YEARS,
        "median_gross_rent": pd.Series(ACS_RENTS, dtype=rent_dtype),
    })

    actual = ANALYSIS.assemble_panel(base, rents)

    assert actual.year.tolist() == BASE_YEARS
    assert actual.acs_year.tolist() == [year - 1 for year in BASE_YEARS]
    assert actual.acs_rent_available.tolist() == [True, False, True]
    assert actual.acs_rent_available.dtype == bool
    assert actual.unsheltered_observed.tolist() == base.unsheltered_observed.tolist()
    assert actual.pit_unsheltered.tolist() == base.pit_unsheltered.tolist()
    assert actual.median_gross_rent.dropna().tolist() == ACS_RENTS


@pytest.mark.parametrize("duplicate_source", ["pit", "acs"])
def test_duplicate_input_keys_fail_closed(duplicate_source):
    base = pd.DataFrame({"msa_id": ["site"], "year": [2010]})
    rents = pd.DataFrame({"msa_id": ["site"], "acs_year": [2009], "median_gross_rent": [700]})
    if duplicate_source == "pit":
        base = pd.concat([base, base], ignore_index=True)
    else:
        rents = pd.concat([rents, rents], ignore_index=True)

    with pytest.raises(ValueError, match="Duplicate"):
        ANALYSIS.assemble_panel(base, rents)


@pytest.mark.parametrize(
    ("years", "expected_years"),
    [([2019, 2020, 2022, 2023], [2020, 2023]),
     ([2010, 2012, 2013], [2013]), ([2010, 2011, 2012], [2011, 2012])],
    ids=["no-covid-bridge", "missing-source-year", "adjacent-years"],
)
def test_growth_requires_exact_calendar_endpoints(years, expected_years):
    values = pd.DataFrame({
        "msa_id": ["site"] * len(years), "year": years,
        "rent": [700 + 10 * offset for offset in range(len(years))],
    })

    actual = ANALYSIS.analysis_series(values, "rent", log1p=False, changes=True)

    assert actual.year.tolist() == expected_years
    assert actual.previous_year.tolist() == [year - 1 for year in expected_years]


@pytest.mark.parametrize(
    ("log1p", "values", "expected"),
    [(False, [700, 770], np.log(770 / 700)), (True, [0, 1], np.log(2))],
    ids=["rent-exact-log-growth", "observed-pit-zero-remains-eligible"],
)
def test_measure_specific_transform(log1p, values, expected):
    frame = pd.DataFrame({"msa_id": ["site", "site"], "year": [2010, 2011], "value": values})

    actual = ANALYSIS.analysis_series(frame, "value", log1p=log1p, changes=True)

    assert actual.value.tolist() == pytest.approx([expected])


@pytest.mark.parametrize("invalid_rent", [0, -666666666, np.nan, np.inf])
def test_invalid_rent_is_not_a_growth_endpoint(invalid_rent):
    frame = pd.DataFrame({
        "msa_id": ["site", "site"], "year": [2010, 2011], "rent": [invalid_rent, 700],
    })

    actual = ANALYSIS.analysis_series(frame, "rent", log1p=False, changes=True)

    assert actual.empty


@pytest.mark.parametrize("lag", [-2, -1, 0, 1, 2])
def test_calendar_lag_is_additional_to_acs_year_alignment(lag):
    predictor = pd.DataFrame({
        "msa_id": ["site", "site"], "year": [2010, 2011], "rent": [700, 770],
    })
    outcome = pd.DataFrame({
        "msa_id": ["site", "site"], "year": [2010 + lag, 2011 + lag], "pit": [10, 12],
    })
    rent_change = ANALYSIS.analysis_series(predictor, "rent", log1p=False, changes=True)
    pit_change = ANALYSIS.analysis_series(outcome, "pit", log1p=True, changes=True)

    actual = ANALYSIS.paired_series(rent_change, pit_change, lag)

    assert actual.aligned_rent_year.tolist() == [2011]
    assert actual.acs_year.tolist() == [2010]
    assert actual.previous_acs_year.tolist() == [2009]
    assert actual.outcome_year.tolist() == [2011 + lag]
    assert actual.previous_pit_year.tolist() == [2010 + lag]


def test_populated_output_refused_without_changing_existing_file(tmp_path):
    original = tmp_path / "existing.txt"
    original.write_text("retain original")

    with pytest.raises(ValueError, match="Choose a fresh"):
        ANALYSIS.prepare_output(tmp_path)

    assert original.read_text() == "retain original"
