"""Synthetic weighted-FE readiness fixtures with explicit LSDV references."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from hhplab.analysis.contracts import AnalysisError, ResolvedSampleFrame
from hhplab.analysis.model import build_absorbed_design
from hhplab.analysis.readiness import check_regression_readiness

GEO_EFFECTS = {"a": 1.0, "b": 2.0, "c": 3.0, "d": -1.0, "e": 0.5, "f": 1.5}
YEAR_EFFECTS = {1: 0.1, 2: 0.2, 3: 0.4, 4: -0.3, 5: 0.7, 6: 0.9}
X_MATRIX = np.array([[0.0, 1.0, 2.0], [1.0, 2.0, 0.0], [2.0, 0.0, 1.0]])
Z_MATRIX = np.array([[0.0, 0.0, 1.0], [0.0, 1.0, 0.0], [1.0, 0.0, 0.0]])
WEIGHT_MATRIX = np.array([[1.0, 2.0, 3.0], [2.0, 4.0, 1.0], [3.0, 1.0, 5.0]])
COMPONENTS = ((("a", "b", "c"), (1, 2, 3)), (("d", "e", "f"), (4, 5, 6)))


@pytest.fixture
def resolved_panel() -> ResolvedSampleFrame:
    records = []
    for geographies, years in COMPONENTS:
        for geo_index, geography in enumerate(geographies):
            for year_index, year in enumerate(years):
                x_value = X_MATRIX[geo_index, year_index]
                z_value = Z_MATRIX[geo_index, year_index]
                records.append(
                    {
                        "geo_id": geography,
                        "year": year,
                        "region": geography,
                        "x": x_value,
                        "z": z_value,
                        "y": (4.0 + GEO_EFFECTS[geography] + YEAR_EFFECTS[year] + 2.0 * x_value),
                        "weight": WEIGHT_MATRIX[geo_index, year_index],
                    }
                )
    frame = pd.DataFrame.from_records(records)
    keys = tuple(zip(frame["geo_id"], frame["year"], strict=True))
    return ResolvedSampleFrame(
        frame=frame,
        row_keys=keys,
        geo_key="geo_id",
        time_key="year",
    )


def _explicit_lsdv(
    frame: pd.DataFrame,
    terms: tuple[str, ...],
    *,
    weights: str | None = None,
) -> np.ndarray:
    dummy_parts = [
        pd.get_dummies(frame[column].astype("string"), dtype=float).to_numpy()
        for column in ("geo_id", "year")
    ]
    design = np.column_stack(
        (np.ones(len(frame)), frame[list(terms)].to_numpy(dtype=float), *dummy_parts)
    )
    raw_weights = (
        frame[weights].to_numpy(dtype=float) if weights is not None else np.ones(len(frame))
    )
    root_weights = np.sqrt(raw_weights)
    beta, *_ = np.linalg.lstsq(
        root_weights[:, None] * design,
        root_weights * frame["y"].to_numpy(dtype=float),
        rcond=None,
    )
    return beta[1 : 1 + len(terms)]


def _absorbed_coefficients(
    frame: pd.DataFrame,
    terms: tuple[str, ...],
    *,
    weights: str | None = "weight",
) -> np.ndarray:
    design = build_absorbed_design(
        frame,
        outcome="y",
        terms=terms,
        fixed_effects=("geo_id", "year"),
        weights=weights,
    )
    coefficients, *_ = np.linalg.lstsq(design.regressors, design.outcome, rcond=None)
    return coefficients


def test_weighted_two_way_absorption_matches_explicit_lsdv_with_zero_slope(resolved_panel):
    frame = resolved_panel.frame
    terms = ("x", "z")
    expected = _explicit_lsdv(frame, terms)
    actual = _absorbed_coefficients(frame, terms)

    np.testing.assert_allclose(actual, expected, atol=1e-10, rtol=1e-10)
    assert actual[0] == pytest.approx(2.0, abs=1e-10)
    assert actual[1] == pytest.approx(0.0, abs=1e-10)


def test_unweighted_two_way_absorption_matches_explicit_lsdv(resolved_panel):
    frame = resolved_panel.frame
    terms = ("x", "z")
    design = build_absorbed_design(
        frame,
        outcome="y",
        terms=terms,
        fixed_effects=("geo_id", "year"),
    )
    actual, *_ = np.linalg.lstsq(design.regressors, design.outcome, rcond=None)

    np.testing.assert_allclose(
        actual,
        _explicit_lsdv(frame, terms),
        atol=1e-10,
        rtol=1e-10,
    )


def test_full_rank_and_nested_cluster_dof_are_separate(resolved_panel):
    result = check_regression_readiness(
        resolved_panel,
        outcome="y",
        predictors=("x", "z"),
        fixed_effects=("geo_id", "year"),
        weights="weight",
        cluster_by="region",
        inference_method="clustered",
    )

    assert result.design_rank == 12
    assert result.k_dof == 8
    assert result.df_resid == len(resolved_panel.frame) - result.design_rank
    assert result.nested_fixed_effects == ("geo_id",)
    assert result.cluster_count == 6
    assert result.warnings[0].startswith("low_cluster_count")
    assert result.weight_validity.to_dict() == {
        "valid": len(resolved_panel.frame),
        "missing": 0,
        "nonnumeric": 0,
        "nonfinite": 0,
        "nonpositive": 0,
    }


def test_weight_rescaling_and_row_order_preserve_absorbed_coefficients(resolved_panel):
    original = resolved_panel.frame
    shuffled = original.sample(frac=1.0, random_state=19).copy()
    rescaled = shuffled.assign(weight=shuffled["weight"] * 1e300)
    baseline = _absorbed_coefficients(original, ("x", "z"))
    reordered = _absorbed_coefficients(shuffled, ("x", "z"))
    scaled = _absorbed_coefficients(rescaled, ("x", "z"))

    np.testing.assert_allclose(reordered, baseline, atol=1e-10, rtol=1e-10)
    np.testing.assert_allclose(scaled, baseline, atol=1e-10, rtol=1e-10)


def test_category_encoding_order_preserves_absorbed_coefficients(resolved_panel):
    frame = resolved_panel.frame.copy()
    for column in ("geo_id", "year"):
        category_order = list(reversed(sorted(frame[column].unique())))
        frame[column] = pd.Categorical(frame[column], categories=category_order, ordered=True)

    np.testing.assert_allclose(
        _absorbed_coefficients(frame, ("x", "z")),
        _explicit_lsdv(frame, ("x", "z"), weights="weight"),
        atol=1e-10,
        rtol=1e-10,
    )


def test_disconnected_fe_components_absorb_to_lsdv_reference(resolved_panel):
    frame = resolved_panel.frame
    assert set(frame.loc[frame["geo_id"].isin({"a", "b", "c"}), "year"]) == {1, 2, 3}
    assert set(frame.loc[frame["geo_id"].isin({"d", "e", "f"}), "year"]) == {4, 5, 6}
    np.testing.assert_allclose(
        _absorbed_coefficients(frame, ("x",)),
        _explicit_lsdv(frame, ("x",)),
        atol=1e-10,
        rtol=1e-10,
    )


def test_no_fixed_effects_keeps_and_counts_weighted_intercept():
    frame = pd.DataFrame(
        {
            "geo_id": ["a", "a", "b", "b"],
            "year": [1, 2, 1, 2],
            "x": [0.0, 1.0, 2.0, 3.0],
            "y": [3.0, 5.0, 7.0, 9.0],
            "weight": [1.0, 2.0, 3.0, 4.0],
        }
    )
    keys = tuple(zip(frame.geo_id, frame.year, strict=True))
    sample = ResolvedSampleFrame(frame, keys, "geo_id", "year")
    result = check_regression_readiness(
        sample,
        outcome="y",
        predictors=("x",),
        weights="weight",
    )
    design = result.design
    explicit = np.column_stack((np.ones(len(frame)), frame["x"].to_numpy()))
    root_weights = np.sqrt(frame["weight"].to_numpy())
    reference, *_ = np.linalg.lstsq(
        root_weights[:, None] * explicit,
        root_weights * frame["y"].to_numpy(),
        rcond=None,
    )

    assert result.design_rank == 2
    assert result.df_resid == 2
    weighted_intercept = design.weighted_intercept
    assert weighted_intercept is not None
    full_fit_x = np.column_stack((weighted_intercept, design.regressors))
    full_fit_y = design.outcome + design.outcome_mean * weighted_intercept
    reconstructed_x = design.regressors + np.outer(weighted_intercept, design.regressor_means)
    np.testing.assert_allclose(
        reconstructed_x,
        np.sqrt(design.normalized_weights)[:, None] * frame[["x"]].to_numpy(),
        atol=1e-12,
        rtol=1e-12,
    )
    np.testing.assert_allclose(
        full_fit_y,
        np.sqrt(design.normalized_weights) * frame["y"].to_numpy(),
        atol=1e-12,
        rtol=1e-12,
    )
    full_beta, *_ = np.linalg.lstsq(full_fit_x, full_fit_y, rcond=None)
    intercept = full_beta[0] - float(design.regressor_means @ full_beta[1:])
    assert intercept == pytest.approx(reference[0])
    assert full_beta[1] == pytest.approx(reference[1])
    assert design.total_sum_squares == pytest.approx(
        float(np.sum(design.normalized_weights * (frame["y"] - design.outcome_mean) ** 2))
    )
    assert design.within_sum_squares == pytest.approx(float(design.outcome @ design.outcome))


def test_fixed_effect_design_exposes_centered_fit_statistics(resolved_panel):
    design = build_absorbed_design(
        resolved_panel.frame,
        outcome="y",
        terms=("x", "z"),
        fixed_effects=("geo_id", "year"),
        weights="weight",
    )
    root_weights = np.sqrt(design.normalized_weights)
    raw_y = resolved_panel.frame["y"].to_numpy(dtype=float)
    expected_total = np.sum(design.normalized_weights * (raw_y - design.outcome_mean) ** 2)

    assert design.weighted_intercept is None
    assert design.total_sum_squares == pytest.approx(expected_total)
    assert design.within_sum_squares == pytest.approx(float(design.outcome @ design.outcome))
    assert design.outcome.shape == (len(resolved_panel.frame),)
    assert root_weights.shape == design.outcome.shape


@pytest.mark.parametrize(
    ("column", "values", "expected_reason"),
    [
        pytest.param("x", [0.0, "bad", 2.0, 3.0], "numeric role 'x'", id="nonnumeric-slope"),
        pytest.param("x", ["0.0", "1.0", "2.0", "3.0"], "nonnumeric", id="numeric-string-slope"),
        pytest.param("weight", [1.0, 0.0, 3.0, 4.0], "nonpositive", id="zero-weight"),
        pytest.param("weight", [1.0, np.inf, 3.0, 4.0], "nonfinite", id="infinite-weight"),
        pytest.param("weight", [1.0, np.nan, 3.0, 4.0], "nonfinite", id="nan-weight"),
        pytest.param("weight", [1.0, "bad", 3.0, 4.0], "nonnumeric", id="nonnumeric-weight"),
        pytest.param(
            "weight",
            ["1.0", "2.0", "3.0", "4.0"],
            "nonnumeric",
            id="numeric-string-weight",
        ),
        pytest.param("weight", [1.0, pd.NA, 3.0, 4.0], "missing", id="null-weight"),
    ],
)
def test_readiness_rejects_invalid_columns_without_dropping_rows(column, values, expected_reason):
    frame = pd.DataFrame(
        {
            "geo_id": ["a", "a", "b", "b"],
            "year": [1, 2, 1, 2],
            "x": [0.0, 1.0, 2.0, 3.0],
            "y": [3.0, 5.0, 7.0, 9.0],
            "weight": [1.0, 2.0, 3.0, 4.0],
        }
    )
    frame[column] = values
    keys = tuple(zip(frame.geo_id, frame.year, strict=True))
    sample = ResolvedSampleFrame(frame, keys, "geo_id", "year")

    with pytest.raises(AnalysisError, match=expected_reason):
        check_regression_readiness(
            sample,
            outcome="y",
            predictors=("x",),
            weights="weight",
        )

    assert len(sample.frame) == len(keys)


def test_rank_check_names_slopes_unidentified_after_absorption(resolved_panel):
    frame = resolved_panel.frame.copy()
    frame["no_within"] = frame.groupby("geo_id")["x"].transform("first")
    sample = ResolvedSampleFrame(
        frame,
        tuple(zip(frame.geo_id, frame.year, strict=True)),
        "geo_id",
        "year",
    )

    with pytest.raises(AnalysisError, match="rank_deficient.*no_within"):
        check_regression_readiness(
            sample,
            outcome="y",
            predictors=("x", "no_within"),
            fixed_effects=("geo_id", "year"),
        )


def test_single_requested_slope_without_within_fe_variation_is_rejected(resolved_panel):
    frame = resolved_panel.frame.copy()
    frame["no_within"] = frame.groupby("geo_id")["x"].transform("first")
    sample = ResolvedSampleFrame(
        frame,
        tuple(zip(frame.geo_id, frame.year, strict=True)),
        "geo_id",
        "year",
    )

    with pytest.raises(AnalysisError, match="rank_deficient.*no_within"):
        check_regression_readiness(
            sample,
            outcome="y",
            predictors=("no_within",),
            fixed_effects=("geo_id", "year"),
        )


def test_collinear_requested_slope_is_rejected_without_dummy_rank_false_positive(
    resolved_panel,
):
    frame = resolved_panel.frame.copy()
    frame["x_copy"] = 2.0 * frame["x"] + frame["geo_id"].map(GEO_EFFECTS)
    sample = ResolvedSampleFrame(
        frame,
        tuple(zip(frame.geo_id, frame.year, strict=True)),
        "geo_id",
        "year",
    )

    with pytest.raises(AnalysisError, match="rank_deficient.*x_copy"):
        check_regression_readiness(
            sample,
            outcome="y",
            predictors=("x", "x_copy"),
            fixed_effects=("geo_id", "year"),
        )


def test_clustered_readiness_requires_two_clusters(resolved_panel):
    frame = resolved_panel.frame.copy()
    frame["one_cluster"] = "all"
    sample = ResolvedSampleFrame(
        frame,
        tuple(zip(frame.geo_id, frame.year, strict=True)),
        "geo_id",
        "year",
    )

    with pytest.raises(AnalysisError, match="at least two clusters"):
        check_regression_readiness(
            sample,
            outcome="y",
            predictors=("x",),
            fixed_effects=("geo_id", "year"),
            cluster_by="one_cluster",
            inference_method="clustered",
        )


def test_absorption_tolerance_cannot_exceed_declared_spec_bound(resolved_panel):
    with pytest.raises(AnalysisError, match=r"in \(0, 1e-12\]"):
        build_absorbed_design(
            resolved_panel.frame,
            outcome="y",
            terms=("x",),
            fixed_effects=("geo_id", "year"),
            tolerance=1.1e-12,
        )
