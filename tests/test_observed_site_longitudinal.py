"""Focused invariants for observed-only PIT longitudinal analysis."""

import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "scripts/analyze_observed_site_pit_hic_longitudinal.py"
)
SPEC = importlib.util.spec_from_file_location("observed_site_longitudinal", SCRIPT_PATH)
ANALYSIS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ANALYSIS)


@pytest.mark.parametrize(
    ("years", "expected_outcomes"),
    [
        ([2019, 2020, 2022], [2020]),
        ([2020, 2022, 2023], [2023]),
        ([2019, 2020, 2021, 2022], [2020, 2021, 2022]),
        ([2010, 2012, 2013], [2013]),
    ],
    ids=["omit-2021-no-bridge", "gap-no-bridge", "consecutive-years", "early-gap"],
)
def test_annual_changes_require_exact_calendar_predecessor(years, expected_outcomes):
    frame = pd.DataFrame({
        "msa_id": ["site"] * len(years), "year": years,
        "count": [10.0 + offset for offset in range(len(years))],
    })

    changes = ANALYSIS.exact_annual_changes(frame, ["count"])

    assert changes.year.tolist() == expected_outcomes


@pytest.mark.parametrize(
    ("lag", "expected_year_pairs"),
    [(-2, [(2020, 2018)]), (-1, [(2020, 2019)]), (0, [(2020, 2020)]),
     (1, [(2020, 2021)]), (2, [(2020, 2022)])],
    ids=["pit-before-two", "pit-before-one", "same-year", "beds-lead-one", "beds-lead-two"],
)
def test_lags_join_exact_year_keys(lag, expected_year_pairs):
    predictors = pd.DataFrame({"msa_id": ["site"], "year": [2020], "beds": [4.0]})
    outcomes = pd.DataFrame({
        "msa_id": ["site"] * 5, "year": [2018, 2019, 2020, 2021, 2022],
        "pit": [1., 2., 3., 4., 5.],
    })

    matched = ANALYSIS.exact_lag_pairs(
        predictors, outcomes, predictor_column="beds", outcome_column="pit", lag=lag
    )

    assert list(zip(matched.beds_year, matched.outcome_year, strict=True)) == expected_year_pairs


def test_unbalanced_fixed_effects_match_dummy_least_squares_and_frequency_bootstrap():
    frame = pd.DataFrame({
        "site": ["a", "a", "b", "b", "c", "c"],
        "year": [1, 2, 1, 3, 2, 3],
        "x": [1., 2., 1.5, 4., 3., 5.],
        "y": [2., 1., 3., 4., 2., 7.],
    })
    site = pd.factorize(frame.site, sort=True)[0]
    year = pd.factorize(frame.year, sort=True)[0]
    values = frame[["x", "y"]].to_numpy()
    residuals = ANALYSIS.weighted_two_way_residuals(values, site, year, np.ones(len(frame)))
    design = np.column_stack([np.eye(3)[site], np.eye(3)[year]])
    least_squares = values - design @ np.linalg.lstsq(design, values, rcond=None)[0]
    np.testing.assert_allclose(residuals, least_squares, atol=1e-9)

    site_frequency = np.array([2., 1., 1.])
    weighted = ANALYSIS.weighted_correlation(
        values, site, year, site_frequency[site], fixed_effects=True
    )
    duplicate_pieces = []
    for draw_id, source_site in enumerate(["a", "a", "b", "c"]):
        duplicated_rows = frame.loc[frame.site.eq(source_site)].copy()
        duplicated_rows["site"] = f"draw-{draw_id}"
        duplicate_pieces.append(duplicated_rows)
    duplicate = pd.concat(duplicate_pieces, ignore_index=True)
    duplicate_site = pd.factorize(duplicate.site, sort=True)[0]
    duplicate_year = pd.factorize(duplicate.year, sort=True)[0]
    duplicated = ANALYSIS.weighted_correlation(
        duplicate[["x", "y"]].to_numpy(), duplicate_site, duplicate_year,
        np.ones(len(duplicate)), fixed_effects=True,
    )
    assert weighted == pytest.approx(duplicated, abs=1e-9)


def test_unadjusted_weighted_correlation_is_centered_pearson():
    values = np.array([[10., 2.], [11., 4.], [12., 6.]])
    site = np.array([0, 1, 2])
    year = np.array([0, 1, 2])
    weights = np.ones(3)

    actual = ANALYSIS.weighted_correlation(values, site, year, weights, fixed_effects=False)

    assert actual == pytest.approx(np.corrcoef(values.T)[0, 1])
    assert actual == pytest.approx(1.0)


def test_exactly_absorbed_outcome_is_a_degenerate_fixed_effect_correlation():
    site_labels = np.array([0, 0, 1, 1, 1, 2, 2, 3, 3])
    years = np.array([0, 1, 0, 1, 2, 1, 2, 0, 2])
    x_values = np.arange(9, dtype=float) ** 2 + 0.2
    y_values = 1.3 * site_labels + 2.7 * years
    values = np.column_stack([x_values, y_values])

    actual = ANALYSIS.weighted_correlation(
        values, site_labels, years, np.ones(9), fixed_effects=True
    )

    assert np.isnan(actual)


@pytest.mark.parametrize(
    ("declaration", "expected_unsheltered", "expected_sheltered"),
    [
        ("Sheltered and Unsheltered Count", True, True),
        ("Sheltered and Partial Unsheltered Count", False, True),
        ("Sheltered-Only Count", False, True),
        ("No 2020 PIT Count", False, False),
        (None, False, False),
    ],
    ids=["complete", "partial", "sheltered-only", "explicit-no-count", "unknown"],
)
def test_observation_masks_follow_source_declarations(
    declaration, expected_unsheltered, expected_sheltered
):
    row = pd.DataFrame({
        "pit_year": [2020], "pit_total": [0], "pit_sheltered": [0],
        "pit_unsheltered": [0], "count_types": [declaration],
        "unsheltered_count_observed": [declaration in {
            "Sheltered and Unsheltered Count", "Sheltered and Partial Unsheltered Count"
        }],
        "unsheltered_count_carried_forward": [declaration in {
            "Sheltered-Only Count", "No 2020 PIT Count"
        }],
        "unsheltered_count_partial": [declaration == "Sheltered and Partial Unsheltered Count"],
    })

    assert ANALYSIS.observed_pit_mask(row, "pit_unsheltered").tolist() == [expected_unsheltered]
    assert ANALYSIS.observed_pit_mask(row, "pit_total").tolist() == [expected_unsheltered]
    assert ANALYSIS.observed_pit_mask(row, "pit_sheltered").tolist() == [expected_sheltered]
