"""Resolved-frame regression API contract and row-identity tests."""

from __future__ import annotations

from dataclasses import replace
from itertools import product

import numpy as np
import pandas as pd
import pytest

import hhplab.analysis.regression as regression
from hhplab.analysis.contracts import AnalysisError, ResolvedSampleFrame
from hhplab.schema.results import validate_result_rows

GEOGRAPHIES = ("a", "b", "c", "d")
YEARS = (2018, 2019, 2020, 2021)
X_VALUES = np.array(
    [[0.0, 1.0, 2.0, 0.5], [1.0, 2.0, 0.5, 1.5], [2.0, 0.5, 1.0, 2.5], [0.5, 1.5, 2.5, 1.0]]
)
Z_VALUES = np.array(
    [[1.0, 0.0, 2.0, 1.5], [0.5, 2.0, 1.0, 0.0], [2.0, 1.0, 0.0, 2.5], [1.5, 2.5, 0.5, 1.0]]
)


@pytest.fixture
def resolved_sample() -> ResolvedSampleFrame:
    rows = []
    for geo_index, year_index in product(range(4), range(4)):
        x_value = X_VALUES[geo_index, year_index]
        z_value = Z_VALUES[geo_index, year_index]
        weight = 1.0 + ((geo_index + 2 * year_index) % 3)
        rows.append(
            {
                "geo_id": GEOGRAPHIES[geo_index],
                "year": YEARS[year_index],
                "x": x_value,
                "z": z_value,
                "x_variant": x_value,
                "weight": weight,
                "cluster": GEOGRAPHIES[geo_index],
                "y": (
                    5.0
                    + 0.9 * geo_index
                    - 0.4 * year_index
                    + 1.7 * x_value
                    - 0.6 * z_value
                    + 0.1 * ((geo_index + year_index) % 2)
                ),
            }
        )
    frame = pd.DataFrame(rows)
    row_keys = tuple(zip(frame["geo_id"], frame["year"], strict=True))
    return ResolvedSampleFrame(frame, row_keys, "geo_id", "year")


@pytest.mark.parametrize("inference_method", ["classical", "hc1", "clustered"])
@pytest.mark.parametrize("fixed_effects", [(), ("geo_id", "year")], ids=["no-fe", "two-way-fe"])
def test_resolved_regression_returns_valid_rows_and_exact_design_evidence(
    resolved_sample, inference_method, fixed_effects
):
    sample = resolved_sample
    result = regression.regress_resolved_frame(
        sample,
        analysis="main",
        variant="base",
        sample_group="main_group",
        outcome="y",
        predictors=("x_variant",),
        controls=("z",),
        fixed_effects=fixed_effects,
        weights="weight",
        inference_method=inference_method,
        cluster_by="cluster",
        base_term_by_effective_term={"x_variant": "x", "z": "z"},
    )

    assert result.execution.status.value == "complete"
    result.execution.validate_sample(sample)
    assert result.execution.consumed_row_keys == sample.row_keys
    validate_result_rows("regression_estimates", result.regression_estimates)
    validate_result_rows("regression_fit", [result.regression_fit])
    expected_terms = ["x_variant", "z"] if fixed_effects else ["(intercept)", "x_variant", "z"]
    assert [row["term"] for row in result.regression_estimates] == expected_terms
    assert [row["base_term"] for row in result.regression_estimates] == (
        ["x", "z"] if fixed_effects else ["(intercept)", "x", "z"]
    )
    assert [row["role"] for row in result.regression_estimates] == (
        ["predictor", "control"] if fixed_effects else ["intercept", "predictor", "control"]
    )
    assert result.regression_fit["sample_source"] == "main_group"
    assert result.regression_fit["n"] == len(sample.row_keys)
    assert result.regression_fit["absorbed"] == list(fixed_effects)

    evidence = result.design_evidence["main@base"]
    assert evidence["sample_group"] == "main_group"
    assert evidence["regressors"] == ["x_variant", "z"]
    assert evidence["fixed_effects"] == list(fixed_effects)
    assert [tuple(row["key"]) for row in evidence["rows"]] == list(sample.row_keys)
    assert [row["regressors"] for row in evidence["rows"]] == sample.frame[
        ["x_variant", "z"]
    ].values.tolist()
    assert np.mean([row["weight"] for row in evidence["rows"]]) == pytest.approx(1.0)


@pytest.mark.parametrize("damage", ["reorder", "identity"], ids=["row-order", "row-identity"])
def test_resolved_regression_rejects_changed_sample_identity(resolved_sample, monkeypatch, damage):
    original = regression.check_regression_readiness

    def altered_readiness(sample, **kwargs):
        if damage == "reorder":
            sample.frame.iloc[:] = sample.frame.iloc[::-1].to_numpy()
        else:
            sample.frame.iloc[0, sample.frame.columns.get_loc("geo_id")] = "changed"
        return original(sample, **kwargs)

    monkeypatch.setattr(regression, "check_regression_readiness", altered_readiness)
    with pytest.raises(AnalysisError, match="sample_contract_violation"):
        regression.regress_resolved_frame(
            resolved_sample,
            analysis="main",
            outcome="y",
            predictors=("x",),
            controls=("z",),
            fixed_effects=("geo_id", "year"),
            weights="weight",
        )


def test_resolved_regression_rejects_estimator_row_count_change(resolved_sample, monkeypatch):
    original_fit = regression._fit_wls

    def altered_fit(**kwargs):
        fit = original_fit(**kwargs)
        return replace(fit, residuals=fit.residuals[:-1])

    monkeypatch.setattr(regression, "_fit_wls", altered_fit)
    with pytest.raises(AnalysisError, match="sample_contract_violation"):
        regression.regress_resolved_frame(
            resolved_sample,
            analysis="main",
            outcome="y",
            predictors=("x",),
            controls=("z",),
            fixed_effects=("geo_id", "year"),
            weights="weight",
        )


@pytest.mark.parametrize("damage", ["reorder", "identity"], ids=["fit-row-order", "fit-row-key"])
def test_resolved_regression_rejects_estimator_frame_mutation(resolved_sample, monkeypatch, damage):
    original_fit = regression._fit_wls

    def altered_fit(**kwargs):
        if damage == "reorder":
            resolved_sample.frame.iloc[:] = resolved_sample.frame.iloc[::-1].to_numpy()
        else:
            resolved_sample.frame.iloc[0, resolved_sample.frame.columns.get_loc("geo_id")] = (
                "changed"
            )
        return original_fit(**kwargs)

    monkeypatch.setattr(regression, "_fit_wls", altered_fit)
    with pytest.raises(AnalysisError, match="sample_contract_violation"):
        regression.regress_resolved_frame(
            resolved_sample,
            analysis="main",
            outcome="y",
            predictors=("x",),
            controls=("z",),
            fixed_effects=("geo_id", "year"),
            weights="weight",
        )


def test_variant_mapping_must_cover_every_estimated_term(resolved_sample):
    with pytest.raises(AnalysisError, match="map every effective"):
        regression.regress_resolved_frame(
            resolved_sample,
            analysis="main",
            variant="alternate",
            outcome="y",
            predictors=("x_variant",),
            controls=("z",),
            base_term_by_effective_term={"x_variant": "x"},
        )
