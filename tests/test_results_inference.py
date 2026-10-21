"""Reference tests for analytic-weight Phase 1 regression inference."""

from __future__ import annotations

from itertools import product

import numpy as np
import pandas as pd
import pytest
from scipy import stats

from hhplab.analysis.contracts import AnalysisError, ResolvedSampleFrame
from hhplab.analysis.estimation import _fit_wls
from hhplab.analysis.model import build_absorbed_design

TERMS = ("x", "z")
FIXED_EFFECTS = ("geo_id", "year")
GEO_EFFECTS = {"a": 1.1, "b": -0.4, "c": 2.0, "d": 0.7}
YEAR_EFFECTS = {1: 0.2, 2: -0.1, 3: 0.8, 4: 0.3}
X_VALUES = np.array(
    [[0.0, 1.0, 2.0, 0.0], [1.0, 2.0, 0.0, 1.0], [2.0, 0.0, 1.0, 2.0], [0.0, 1.0, 2.0, 1.0]]
)
Z_VALUES = np.array(
    [[1.0, 0.0, 1.0, 2.0], [0.0, 2.0, 1.0, 0.0], [2.0, 1.0, 0.0, 1.0], [1.0, 2.0, 0.0, 2.0]]
)
NOISE = np.array(
    [[0.2, -0.1, 0.3, -0.4], [-0.3, 0.5, -0.2, 0.1], [0.4, -0.2, 0.1, -0.3], [-0.1, 0.2, -0.4, 0.3]]
)


@pytest.fixture
def inference_sample() -> ResolvedSampleFrame:
    records = []
    for geo_index, year_index in product(range(4), range(4)):
        geography = tuple(GEO_EFFECTS)[geo_index]
        year = tuple(YEAR_EFFECTS)[year_index]
        x_value = X_VALUES[geo_index, year_index]
        z_value = Z_VALUES[geo_index, year_index]
        records.append(
            {
                "geo_id": geography,
                "year": year,
                "x": x_value,
                "z": z_value,
                "weight": 1.0 + ((geo_index + year_index) % 3),
                "y": (
                    3.0
                    + GEO_EFFECTS[geography]
                    + YEAR_EFFECTS[year]
                    + 1.75 * x_value
                    - 0.4 * z_value
                    + NOISE[geo_index, year_index]
                ),
            }
        )
    frame = pd.DataFrame.from_records(records)
    frame["cluster"] = frame["geo_id"]
    row_keys = tuple(zip(frame["geo_id"], frame["year"], strict=True))
    return ResolvedSampleFrame(frame, row_keys, "geo_id", "year")


def _explicit_reference(
    frame: pd.DataFrame,
    *,
    inference_method: str,
    fixed_effects: tuple[str, ...] = FIXED_EFFECTS,
    cluster_column: str | None = "cluster",
    ci_level: float = 0.95,
) -> dict[str, object]:
    normalized_weights = frame["weight"].to_numpy(dtype=float)
    normalized_weights /= normalized_weights.mean()
    root_weights = np.sqrt(normalized_weights)
    dummy_parts = [
        pd.get_dummies(frame[column].astype("string"), drop_first=True, dtype=float).to_numpy()
        for column in fixed_effects
    ]
    raw_design = np.column_stack((np.ones(len(frame)), frame[list(TERMS)].to_numpy(), *dummy_parts))
    x = root_weights[:, None] * raw_design
    y = root_weights * frame["y"].to_numpy(dtype=float)
    estimates, *_ = np.linalg.lstsq(x, y, rcond=None)
    residuals = y - x @ estimates
    rank = int(np.linalg.matrix_rank(x))
    df_resid = len(frame) - rank
    bread = np.linalg.inv(x.T @ x)
    ssr = float(residuals @ residuals)
    inference_df = df_resid
    if inference_method == "classical":
        covariance = bread * (ssr / df_resid)
    elif inference_method == "hc1":
        scores = x * residuals[:, None]
        covariance = (len(frame) / df_resid) * (bread @ scores.T @ scores @ bread)
    else:
        assert cluster_column is not None
        clusters = frame[cluster_column].to_numpy()
        meat = np.zeros_like(bread)
        for cluster in pd.unique(clusters):
            mask = clusters == cluster
            score = x[mask].T @ residuals[mask]
            meat += np.outer(score, score)
        cluster_count = int(pd.Series(clusters).nunique())
        reduced_fixed_effects = tuple(
            column
            for column in fixed_effects
            if not frame.groupby(column)[cluster_column].nunique().le(1).all()
        )
        reduced_parts = [
            pd.get_dummies(frame[column].astype("string"), drop_first=True, dtype=float).to_numpy()
            for column in reduced_fixed_effects
        ]
        reduced = np.column_stack(
            (np.ones(len(frame)), frame[list(TERMS)].to_numpy(), *reduced_parts)
        )
        k_dof = int(np.linalg.matrix_rank(root_weights[:, None] * reduced))
        covariance = (
            (cluster_count / (cluster_count - 1))
            * ((len(frame) - 1) / (len(frame) - k_dof))
            * (bread @ meat @ bread)
        )
        inference_df = cluster_count - 1

    variances = np.maximum(np.diag(covariance), 0.0)
    standard_errors = np.sqrt(variances)
    critical = stats.t.ppf((1 + ci_level) / 2, inference_df)
    ci_lo = estimates - critical * standard_errors
    ci_hi = estimates + critical * standard_errors
    p_values = 2 * stats.t.sf(np.abs(estimates / standard_errors), inference_df)
    return {
        "estimates": estimates[: 1 + len(TERMS)],
        "standard_errors": standard_errors[: 1 + len(TERMS)],
        "covariance": covariance[: 1 + len(TERMS), : 1 + len(TERMS)],
        "ci_lo": ci_lo[: 1 + len(TERMS)],
        "ci_hi": ci_hi[: 1 + len(TERMS)],
        "p_values": p_values[: 1 + len(TERMS)],
        "residuals": residuals,
        "ssr": ssr,
        "df_resid": df_resid,
        "inference_df": inference_df,
    }


@pytest.mark.parametrize("inference_method", ["classical", "hc1", "clustered"])
def test_weighted_fe_fit_matches_explicit_lsdv_reference(inference_sample, inference_method):
    frame = inference_sample.frame
    design = build_absorbed_design(
        frame,
        outcome="y",
        terms=TERMS,
        fixed_effects=FIXED_EFFECTS,
        weights="weight",
        cluster_by="cluster",
    )
    fit = _fit_wls(
        design=design,
        inference_method=inference_method,
        clusters=frame["cluster"] if inference_method == "clustered" else None,
        ci_level=0.9,
    )
    reference = _explicit_reference(frame, inference_method=inference_method, ci_level=0.9)
    actual_slopes = np.array([fit.estimates[TERMS.index(term)] for term in TERMS])
    actual_ses = np.array([fit.standard_errors[TERMS.index(term)] for term in TERMS])
    actual_ci_lo = np.array([fit.ci_lo[TERMS.index(term)] for term in TERMS])
    actual_ci_hi = np.array([fit.ci_hi[TERMS.index(term)] for term in TERMS])
    actual_p_values = np.array([fit.p_values[TERMS.index(term)] for term in TERMS])
    reference_slice = slice(1, 1 + len(TERMS))

    np.testing.assert_allclose(
        actual_slopes, reference["estimates"][reference_slice], atol=1e-10, rtol=1e-10
    )
    np.testing.assert_allclose(
        actual_ses, reference["standard_errors"][reference_slice], atol=1e-10, rtol=1e-10
    )
    np.testing.assert_allclose(
        actual_ci_lo, reference["ci_lo"][reference_slice], atol=1e-10, rtol=1e-10
    )
    np.testing.assert_allclose(
        actual_ci_hi, reference["ci_hi"][reference_slice], atol=1e-10, rtol=1e-10
    )
    np.testing.assert_allclose(
        actual_p_values, reference["p_values"][reference_slice], atol=1e-10, rtol=1e-10
    )
    assert fit.inference_df == reference["inference_df"]
    assert fit.r2 == pytest.approx(1 - fit.ssr / design.total_sum_squares)
    assert fit.r2_within == pytest.approx(1 - fit.ssr / design.within_sum_squares)


@pytest.mark.parametrize("inference_method", ["classical", "hc1", "clustered"])
def test_no_fe_intercept_covariance_matches_explicit_reference(inference_sample, inference_method):
    frame = inference_sample.frame
    design = build_absorbed_design(
        frame, outcome="y", terms=TERMS, weights="weight", cluster_by="cluster"
    )
    fit = _fit_wls(
        design=design,
        inference_method=inference_method,
        clusters=frame["cluster"] if inference_method == "clustered" else None,
    )
    reference = _explicit_reference(frame, inference_method=inference_method, fixed_effects=())

    np.testing.assert_allclose(fit.estimates, reference["estimates"], atol=1e-10, rtol=1e-10)
    np.testing.assert_allclose(fit.covariance, reference["covariance"], atol=1e-10, rtol=1e-10)
    np.testing.assert_allclose(
        fit.standard_errors, reference["standard_errors"], atol=1e-10, rtol=1e-10
    )
    np.testing.assert_allclose(fit.ci_lo, reference["ci_lo"], atol=1e-10, rtol=1e-10)
    np.testing.assert_allclose(fit.ci_hi, reference["ci_hi"], atol=1e-10, rtol=1e-10)
    np.testing.assert_allclose(fit.p_values, reference["p_values"], atol=1e-10, rtol=1e-10)


def test_large_intercept_does_not_erase_real_residual_variation(inference_sample):
    frame = inference_sample.frame.assign(y=inference_sample.frame["y"] + 1e9)
    design = build_absorbed_design(frame, outcome="y", terms=TERMS, weights="weight")
    fit = _fit_wls(design=design, inference_method="classical")
    centered_reference_frame = frame.assign(y=frame["y"] - 1e9)
    reference = _explicit_reference(
        centered_reference_frame,
        inference_method="classical",
        fixed_effects=(),
    )

    assert fit.ssr > 1.0
    assert all(error > 0 for error in fit.standard_errors)
    assert fit.estimates[0] == pytest.approx(reference["estimates"][0] + 1e9, abs=1e-6)
    np.testing.assert_allclose(fit.estimates[1:], reference["estimates"][1:], atol=1e-8)
    np.testing.assert_allclose(fit.covariance, reference["covariance"], atol=1e-7, rtol=1e-7)


def test_near_perfect_nonzero_noise_retains_positive_variance(inference_sample):
    frame = inference_sample.frame.copy()
    frame["y"] = 2.0 + 3.0 * frame["x"] - 0.5 * frame["z"] + NOISE.ravel() * 1e-8
    design = build_absorbed_design(frame, outcome="y", terms=TERMS, weights="weight")
    fit = _fit_wls(design=design, inference_method="classical")
    reference = _explicit_reference(frame, inference_method="classical", fixed_effects=())

    assert 0 < fit.ssr < 1e-12
    assert all(error > 0 for error in fit.standard_errors)
    np.testing.assert_allclose(fit.covariance, reference["covariance"], atol=1e-24, rtol=1e-7)


def test_fit_inference_is_invariant_to_large_common_weight_rescaling(inference_sample):
    frame = inference_sample.frame
    baseline_design = build_absorbed_design(
        frame, outcome="y", terms=TERMS, fixed_effects=FIXED_EFFECTS, weights="weight"
    )
    scaled_frame = frame.assign(weight=frame["weight"] * 1e300)
    scaled_design = build_absorbed_design(
        scaled_frame,
        outcome="y",
        terms=TERMS,
        fixed_effects=FIXED_EFFECTS,
        weights="weight",
    )
    baseline = _fit_wls(design=baseline_design, inference_method="hc1")
    scaled = _fit_wls(design=scaled_design, inference_method="hc1")

    np.testing.assert_allclose(scaled.estimates, baseline.estimates, atol=1e-10, rtol=1e-10)
    np.testing.assert_allclose(
        scaled.standard_errors, baseline.standard_errors, atol=1e-10, rtol=1e-10
    )
    np.testing.assert_allclose(scaled.ci_lo, baseline.ci_lo, atol=1e-10, rtol=1e-10)
    np.testing.assert_allclose(scaled.p_values, baseline.p_values, atol=1e-10, rtol=1e-10)


def test_zero_residual_variance_has_point_interval_and_explicit_undefined_p():
    frame = pd.DataFrame(
        {
            "geo_id": ["a", "a", "b", "b"],
            "year": [1, 2, 1, 2],
            "x": [0.0, 1.0, 2.0, 3.0],
            "y": [5.0, 5.0, 5.0, 5.0],
            "weight": [1.0, 2.0, 3.0, 4.0],
        }
    )
    design = build_absorbed_design(frame, outcome="y", terms=("x",), weights="weight")
    fit = _fit_wls(design=design, inference_method="classical")

    assert fit.estimates[0] == pytest.approx(5.0)
    assert fit.estimates[1] == pytest.approx(0.0)
    assert fit.standard_errors.tolist() == [0.0, 0.0]
    assert fit.ci_lo.tolist() == fit.estimates.tolist()
    assert fit.ci_hi.tolist() == fit.estimates.tolist()
    assert fit.p_values == (0.0, None)
    assert fit.p_status == ("evaluated", "not_evaluated")
    assert fit.p_reasons == (None, "zero_estimate_zero_se")
    assert fit.r2 is None and fit.r2_status == "not_evaluated"
    assert fit.r2_reason == "zero_total_sum_squares"
    assert fit.r2_within is None and fit.r2_within_status == "not_evaluated"


@pytest.mark.parametrize(
    ("inference_method", "clusters", "error_pattern"),
    [
        pytest.param("clustered", None, "requires a cluster vector", id="missing-clusters"),
        pytest.param(
            "clustered", pd.Series(["only"] * 16), "at least two clusters", id="one-cluster"
        ),
    ],
)
def test_clustered_fit_rejects_missing_or_insufficient_clusters(
    inference_sample, inference_method, clusters, error_pattern
):
    design = build_absorbed_design(
        inference_sample.frame,
        outcome="y",
        terms=TERMS,
        fixed_effects=FIXED_EFFECTS,
        weights="weight",
    )

    with pytest.raises(AnalysisError, match=error_pattern):
        _fit_wls(design=design, inference_method=inference_method, clusters=clusters)


def test_classical_fit_rejects_nonpositive_residual_degrees_of_freedom():
    frame = pd.DataFrame(
        {
            "geo_id": ["a", "b"],
            "year": [1, 1],
            "x": [0.0, 1.0],
            "y": [1.0, 2.0],
        }
    )
    design = build_absorbed_design(frame, outcome="y", terms=("x",))

    with pytest.raises(AnalysisError, match="positive residual degrees"):
        _fit_wls(design=design, inference_method="classical")
