"""Timing and observation invariants for the ACS1 gross-rent diagnostic.

Fixture truth table: PIT2010 matches ACS2009; PIT2011 has no matching ACS2010;
PIT2012 matches ACS2011 even though its PIT observation is ineligible. Missing
rent never drops a frozen row, and rent predictors do not require observed PIT.

ZORI truth table: county 01005 has only 11 months in 2015, so regions requiring
it have no valid 2015 value but can enter in 2016. County 01007 has no source
observations in any year and is outside fixed support. Coverage uses all member
weights. A 90% covered site passes; incomplete region-years fail. Every usable
regional value contains the same counties with the same relative weights.
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
ZORI_YEARS = [2015, 2016]
ZORI_FIXTURE_RENTS = {
    ("01001", 2015): 1000., ("01001", 2016): 1100.,
    ("01003", 2015): 2000., ("01003", 2016): 2200.,
    ("01005", 2015): 5000., ("01005", 2016): 5500.,
}
ZORI_MONTHLY_ROWS = [
    {"geo_id": county, "date": f"{year}-{month:02d}-01", "zori": rent}
    for (county, year), rent in ZORI_FIXTURE_RENTS.items()
    for month in range(1, 13)
    if not (county == "01005" and year == 2015 and month == 1)
]
ZORI_COUNTY_WEIGHTS = {"01001": 90., "01003": 10., "01005": 90., "01007": 10.}
ZORI_MEMBERS = [
    ("single", "01001"),
    ("two", "01001"), ("two", "01003"),
    ("low", "01003"), ("low", "01005"),
    ("edge", "01001"), ("edge", "01007"),
    ("none", "01005"),
]
SHORT_STUDY_YEARS = [2016, 2017, 2018, 2019, 2020, 2022, 2023, 2024]
SHORT_STUDY_ROWS = [
    {"msa_id": str(site), "year": year,
     "zori": 1000 * (1 + .11 * site + .03 * t + .01 * site * t + .002 * t * t * site),
     "pit_unsheltered": 10 + 7 * site + 2 * t + site * t * t,
     "zori_available": True, "unsheltered_observed": True}
    for site in range(5) for t, year in enumerate(SHORT_STUDY_YEARS)
]
TOTAL_FLAG_CASES = [
    (True, True, True, True),
    (False, True, True, False),
    (True, False, True, False),
    (True, True, False, False),
    (None, True, True, False),
    (True, None, True, False),
]
OBSERVED_UNSHELTERED_COUNT = 3
OBSERVED_SHELTERED_COUNT = 7
OBSERVED_TOTAL_COUNT = OBSERVED_UNSHELTERED_COUNT + OBSERVED_SHELTERED_COUNT


@pytest.fixture
def zori_inputs():
    monthly = pd.DataFrame(ZORI_MONTHLY_ROWS)
    population = pd.DataFrame({
        "county_fips": list(ZORI_COUNTY_WEIGHTS),
        "population": list(ZORI_COUNTY_WEIGHTS.values()),
        "year": ANALYSIS.ZORI_WEIGHT_YEAR,
    })
    membership = pd.DataFrame(ZORI_MEMBERS, columns=["msa_id", "county_fips"])
    return monthly, population, membership


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


@pytest.mark.parametrize(
    ("year", "msa", "expected_coverage", "expected_counties", "eligible"),
    [(2015, "single", 1., 1, True), (2016, "single", 1., 1, True),
     (2015, "two", 1., 2, True), (2016, "two", 1., 2, True),
     (2015, "edge", .9, 1, True), (2016, "edge", .9, 1, True),
     (2015, "low", .1, 1, False), (2016, "low", 1., 2, True),
     (2015, "none", 0., 0, False), (2016, "none", 1., 1, True)],
)
def test_zori_fixed_support_coverage_and_weighted_means(
    zori_inputs, year, msa, expected_coverage, expected_counties, eligible,
):
    rolled, inventory = ANALYSIS.build_zori_annual(*zori_inputs, ZORI_YEARS)
    actual = rolled.loc[rolled.msa_id.eq(msa) & rolled.year.eq(year)].iloc[0]

    assert actual.coverage_ratio == pytest.approx(expected_coverage)
    assert actual.county_count == expected_counties
    assert pd.notna(actual.zori_coc) == eligible
    if eligible:
        member_ids = {county for metro, county in ZORI_MEMBERS if metro == msa}
        supported = {county for county, _ in ZORI_FIXTURE_RENTS}
        eligible_counties = member_ids & supported
        expected = sum(
            ZORI_FIXTURE_RENTS[(county, year)] * ZORI_COUNTY_WEIGHTS[county]
            for county in eligible_counties
        ) / sum(ZORI_COUNTY_WEIGHTS[county] for county in eligible_counties)
        assert actual.zori_coc == pytest.approx(expected)
    incomplete = inventory.loc[inventory.county_fips.eq("01005")]
    assert incomplete.month_count.tolist() == [11, 12]
    assert incomplete.fixed_support_member.tolist() == [True, True]


@pytest.mark.parametrize("problem", ["duplicate-month", "missing-weight", "invalid-weight"])
def test_zori_bad_source_or_unknown_denominator_fails_closed(zori_inputs, problem):
    monthly, population, membership = zori_inputs
    if problem == "duplicate-month":
        monthly = pd.concat([monthly, monthly.iloc[[0]]], ignore_index=True)
    elif problem == "missing-weight":
        population = population.loc[population.county_fips.ne("01007")].copy()
    else:
        population.loc[population.county_fips.eq("01001"), "population"] = 0

    with pytest.raises(ValueError, match="Duplicate|Missing or invalid"):
        ANALYSIS.build_zori_annual(monthly, population, membership, ZORI_YEARS)


def test_short_pre2020_extreme_lags_are_undefined_instead_of_roundoff_correlations():
    results, pairs = ANALYSIS.run_checks(
        pd.DataFrame(SHORT_STUDY_ROWS), replicates=19,
        rent_measure="zori", available_column="zori_available",
    )
    extreme = results.loc[
        results.analysis.eq("annual_changes_fe") & results.period.eq("pre2020")
        & results.lag.abs().eq(2)
    ]

    assert len(extreme) == 2
    assert not extreme.point_estimate_defined.any()
    assert extreme.pearson_r.isna().all()
    assert not extreme.bootstrap_interval_defined.any()
    assert extreme.undefined_reason.str.contains("No residual variation").all()
    assert not pairs.empty


@pytest.mark.parametrize(
    ("unsheltered", "sheltered", "total", "eligible"), TOTAL_FLAG_CASES,
    ids=["all-observed", "unsheltered-carried", "sheltered-unobserved",
         "total-unobserved", "unsheltered-unknown", "sheltered-unknown"],
)
def test_total_outcome_requires_both_components_observed(unsheltered, sheltered, total, eligible):
    panel = pd.DataFrame({
        "msa_id": ["site"], "year": [2016],
        "pit_unsheltered": [OBSERVED_UNSHELTERED_COUNT],
        "pit_sheltered": [OBSERVED_SHELTERED_COUNT], "pit_total": [OBSERVED_TOTAL_COUNT],
        "unsheltered_observed": pd.Series([unsheltered], dtype="boolean"),
        "sheltered_observed": pd.Series([sheltered], dtype="boolean"),
        "total_observed": pd.Series([total], dtype="boolean"),
    })

    selected = ANALYSIS.observed_outcome_rows(panel, "pit_total")

    assert len(selected) == int(eligible)
    pd.testing.assert_series_equal(
        panel.pit_total, pd.Series([OBSERVED_TOTAL_COUNT], name="pit_total"))


def test_observed_total_arithmetic_mismatch_fails_closed():
    panel = pd.DataFrame({
        "pit_unsheltered": [OBSERVED_UNSHELTERED_COUNT],
        "pit_sheltered": [OBSERVED_SHELTERED_COUNT], "pit_total": [OBSERVED_TOTAL_COUNT + 1],
        "unsheltered_observed": [True], "sheltered_observed": [True], "total_observed": [True],
    })

    with pytest.raises(ValueError, match="differs from its components"):
        ANALYSIS.observed_outcome_rows(panel, "pit_total")


@pytest.fixture(scope="module")
def total_diagnostic():
    panel = pd.DataFrame(SHORT_STUDY_ROWS)
    panel["pit_sheltered"] = 100 + 2 * panel.msa_id.astype(int) + (panel.year - 2016) ** 2
    panel["pit_total"] = panel.pit_unsheltered + panel.pit_sheltered
    panel["sheltered_observed"] = True
    panel["total_observed"] = True
    results, pairs = ANALYSIS.run_checks(
        panel, replicates=19, rent_measure="zori", available_column="zori_available",
        outcome_measure="pit_total",
    )
    return panel, results, pairs


@pytest.mark.parametrize("analysis", ["annual_changes_fe", "log_levels_fe"])
def test_total_diagnostic_uses_total_counts_and_transforms(total_diagnostic, analysis):
    panel, results, pairs = total_diagnostic
    selected = pairs.loc[pairs.analysis.eq(analysis)].copy()
    counts = panel[["msa_id", "year", "pit_total"]].rename(columns={"year": "outcome_year"})
    selected = selected.merge(counts, on=["msa_id", "outcome_year"], validate="many_to_one")

    np.testing.assert_allclose(selected.pit_current_raw, selected.pit_total)
    expected = np.log1p(selected.pit_total)
    if analysis == "annual_changes_fe":
        previous = counts.rename(columns={"pit_total": "previous_total"}).copy()
        previous.outcome_year += 1
        selected = selected.merge(previous, on=["msa_id", "outcome_year"], validate="many_to_one")
        np.testing.assert_allclose(selected.pit_previous_raw, selected.previous_total)
        expected = np.log1p(selected.pit_total) - np.log1p(selected.previous_total)
    np.testing.assert_allclose(selected.pit_value, expected)
    assert results.loc[results.primary, "point_estimate_defined"].all()
