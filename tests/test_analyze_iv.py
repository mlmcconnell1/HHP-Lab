"""Tests for 2SLS support in hhplab analyze regress.

Fixture design (exact identification, no noise):

- ``z`` is the excluded instrument, balanced and exactly orthogonal to the
  confounder ``u`` and to the intercept.
- ``x = z + u`` is the endogenous predictor.
- ``y = STRUCTURAL_BETA * x + CONFOUND_GAMMA * u``.

Because cov(z, u) == 0 exactly, 2SLS recovers STRUCTURAL_BETA exactly, while
OLS is biased upward by the confounder loading. The OLS expectation is derived
from the same constants rather than hard-coded.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from typer.testing import CliRunner

from hhplab.analyze import (
    AnalysisError,
    _fit_ols,
    _fit_2sls,
    _restricted_iv_fitted_and_residuals,
    _restricted_ols_fitted_and_residuals,
    _wild_cluster_bootstrap_p_values,
    regress_panel,
)
from hhplab.cli.main import app
from hhplab.provenance import ProvenanceBlock, write_parquet_with_provenance

runner = CliRunner()

STRUCTURAL_BETA = 2.0
CONFOUND_GAMMA = 3.0

INSTRUMENT = np.array([-3.0, -1.0, 1.0, 3.0] * 2)
CONFOUNDER = np.array([1.0, -1.0, -1.0, 1.0, 1.0, -1.0, -1.0, 1.0])
ENDOGENOUS = INSTRUMENT + CONFOUNDER
OUTCOME = STRUCTURAL_BETA * ENDOGENOUS + CONFOUND_GAMMA * CONFOUNDER

# Derived, not hard-coded: the OLS slope of y on x with an intercept.
EXPECTED_OLS_BETA = STRUCTURAL_BETA + CONFOUND_GAMMA * float(
    (ENDOGENOUS - ENDOGENOUS.mean()) @ CONFOUNDER
    / ((ENDOGENOUS - ENDOGENOUS.mean()) @ (ENDOGENOUS - ENDOGENOUS.mean()))
)

WRE_OUTCOME = np.array([1.2, 2.6, 2.2, 3.1, 3.6, 4.9, 4.1, 5.4, 6.2, 6.9, 7.4, 8.8])
WRE_ENDOGENOUS = np.array(
    [-1.3, -0.4, -0.2, 0.4, 0.9, 1.4, 1.1, 2.0, 2.5, 2.8, 3.3, 3.9]
)
WRE_INSTRUMENT = np.array(
    [-2.0, -1.5, -1.0, -0.5, 0.0, 0.4, 0.7, 1.2, 1.6, 2.0, 2.3, 2.8]
)
WRE_CLUSTERS = ["A", "A", "B", "B", "C", "C", "D", "D", "E", "E", "F", "F"]
WRE_REPS = 11
# This seed makes the restricted WRE count differ from the old unrestricted WCU count.
WRE_SEED = 1

CLUSTERED_INFERENCE_CLUSTER_CASES = [
    pytest.param(pd.Series(["A"] * len(WRE_OUTCOME), name="geo_id"), id="one-cluster"),
    pytest.param(pd.Series([None] * len(WRE_OUTCOME), name="geo_id"), id="zero-clusters"),
]
CLUSTERED_INFERENCE_CLUSTER_ERROR = (
    "clustered standard errors require at least two non-null clusters"
)


def _iv_panel(path: Path) -> Path:
    df = pd.DataFrame(
        {
            "geo_id": [f"M{i}" for i in range(len(INSTRUMENT))],
            "year": [2020] * len(INSTRUMENT),
            "y": OUTCOME,
            "x": ENDOGENOUS,
            "z": INSTRUMENT,
            "u": CONFOUNDER,
        }
    )
    write_parquet_with_provenance(df, path, ProvenanceBlock(geo_type="msa", extra={}))
    return path


def test_2sls_recovers_structural_coefficient_and_ols_is_confounded(tmp_path: Path) -> None:
    panel = _iv_panel(tmp_path / "panel.parquet")
    iv = regress_panel(
        panel,
        outcome="y",
        predictors=["x"],
        entity_fe=False,
        year_fe=False,
        cluster_by=None,
        endogenous="x",
        instruments=["z"],
        output_path=tmp_path / "iv.parquet",
    )
    structural = iv.table[(iv.table["stage"] == "structural") & (iv.table["term"] == "x")]
    assert structural["estimator"].tolist() == ["2sls"]
    assert structural["estimate"].to_numpy() == pytest.approx([STRUCTURAL_BETA])

    ols = regress_panel(
        panel,
        outcome="y",
        predictors=["x"],
        entity_fe=False,
        year_fe=False,
        cluster_by=None,
        output_path=tmp_path / "ols.parquet",
    )
    ols_x = ols.table[ols.table["term"] == "x"]
    assert ols_x["estimator"].tolist() == ["ols"]
    assert ols_x["estimate"].to_numpy() == pytest.approx([EXPECTED_OLS_BETA])
    assert abs(EXPECTED_OLS_BETA - STRUCTURAL_BETA) > 0.1


def test_2sls_reports_first_stage_rows_and_f_diagnostic(tmp_path: Path) -> None:
    panel = _iv_panel(tmp_path / "panel.parquet")
    iv = regress_panel(
        panel,
        outcome="y",
        predictors=["x"],
        entity_fe=False,
        year_fe=False,
        cluster_by=None,
        endogenous="x",
        instruments=["z"],
        output_path=tmp_path / "iv.parquet",
    )
    first_stage_rows = iv.table[iv.table["stage"] == "first_stage"]
    assert first_stage_rows["term"].tolist() == ["z"]
    assert first_stage_rows["outcome"].tolist() == ["x"]
    # First stage is x = z + u with cov(z, u) = 0, so the instrument loading is 1.
    assert first_stage_rows["estimate"].to_numpy() == pytest.approx([1.0])

    first_stage = iv.metadata["first_stage"]
    assert first_stage["endogenous"] == "x"
    assert first_stage["instruments"] == ["z"]
    assert first_stage["f_statistic"] > 10.0
    assert iv.metadata["estimator"] == "2sls"


def test_2sls_clustered_first_stage_p_value_uses_cluster_degrees_of_freedom(
    tmp_path: Path,
) -> None:
    from scipy import stats  # type: ignore[import-not-found]

    panel = _iv_panel(tmp_path / "panel.parquet")
    iv = regress_panel(
        panel,
        outcome="y",
        predictors=["x"],
        entity_fe=False,
        year_fe=False,
        cluster_by="geo_id",
        endogenous="x",
        instruments=["z"],
        output_path=tmp_path / "iv.parquet",
    )

    first_stage = iv.metadata["first_stage"]
    cluster_dof = len(INSTRUMENT) - 1
    assert first_stage["f_p_value"] == pytest.approx(
        float(stats.f.sf(first_stage["f_statistic"], 1, cluster_dof))
    )


def test_clustered_ols_coefficient_p_value_uses_cluster_degrees_of_freedom() -> None:
    from scipy import stats  # type: ignore[import-not-found]

    x = np.column_stack([np.ones(len(WRE_OUTCOME)), WRE_INSTRUMENT])
    clusters = pd.Series(WRE_CLUSTERS, name="geo_id")
    residual_dof = int(len(WRE_OUTCOME) - np.linalg.matrix_rank(x))
    cluster_dof = clusters.nunique() - 1

    fit = _fit_ols(x=x, y=WRE_OUTCOME, dof=residual_dof, clusters=clusters)

    expected = float(stats.t.sf(abs(fit.t_stats[1]), cluster_dof) * 2.0)
    old_residual_dof_value = float(stats.t.sf(abs(fit.t_stats[1]), residual_dof) * 2.0)
    assert fit.p_values[1] == pytest.approx(expected)
    assert fit.p_values[1] != pytest.approx(old_residual_dof_value)


@pytest.mark.parametrize("clusters", CLUSTERED_INFERENCE_CLUSTER_CASES)
def test_clustered_ols_rejects_too_few_non_null_clusters(
    clusters: pd.Series,
) -> None:
    x = np.column_stack([np.ones(len(WRE_OUTCOME)), WRE_INSTRUMENT])
    residual_dof = int(len(WRE_OUTCOME) - np.linalg.matrix_rank(x))

    with pytest.raises(AnalysisError, match=CLUSTERED_INFERENCE_CLUSTER_ERROR):
        _fit_ols(x=x, y=WRE_OUTCOME, dof=residual_dof, clusters=clusters)


def test_clustered_2sls_coefficient_p_value_uses_cluster_degrees_of_freedom() -> None:
    from scipy import stats  # type: ignore[import-not-found]

    x = np.column_stack([np.ones(len(WRE_OUTCOME)), WRE_ENDOGENOUS])
    z = np.column_stack([np.ones(len(WRE_OUTCOME)), WRE_INSTRUMENT])
    clusters = pd.Series(WRE_CLUSTERS, name="geo_id")
    residual_dof = int(len(WRE_OUTCOME) - np.linalg.matrix_rank(x))
    cluster_dof = clusters.nunique() - 1

    fit = _fit_2sls(x=x, z=z, y=WRE_OUTCOME, dof=residual_dof, clusters=clusters)

    expected = float(stats.t.sf(abs(fit.t_stats[1]), cluster_dof) * 2.0)
    old_residual_dof_value = float(stats.t.sf(abs(fit.t_stats[1]), residual_dof) * 2.0)
    assert fit.p_values[1] == pytest.approx(expected)
    assert fit.p_values[1] != pytest.approx(old_residual_dof_value)


@pytest.mark.parametrize("clusters", CLUSTERED_INFERENCE_CLUSTER_CASES)
def test_clustered_2sls_rejects_too_few_non_null_clusters(
    clusters: pd.Series,
) -> None:
    x = np.column_stack([np.ones(len(WRE_OUTCOME)), WRE_ENDOGENOUS])
    z = np.column_stack([np.ones(len(WRE_OUTCOME)), WRE_INSTRUMENT])
    residual_dof = int(len(WRE_OUTCOME) - np.linalg.matrix_rank(x))

    with pytest.raises(AnalysisError, match=CLUSTERED_INFERENCE_CLUSTER_ERROR):
        _fit_2sls(x=x, z=z, y=WRE_OUTCOME, dof=residual_dof, clusters=clusters)


IV_VALIDATION_CASES = [
    pytest.param(
        {"endogenous": "x"},
        "requires both --endogenous and --instruments",
        id="endogenous-without-instruments",
    ),
    pytest.param(
        {"instruments": ["z"]},
        "requires both --endogenous and --instruments",
        id="instruments-without-endogenous",
    ),
    pytest.param(
        {"endogenous": "u", "instruments": ["z"]},
        "must be one of the model predictors",
        id="endogenous-not-a-predictor",
    ),
    pytest.param(
        {"endogenous": "x", "instruments": ["x"]},
        "must be excluded from the structural equation",
        id="instrument-in-structural-equation",
    ),
    pytest.param(
        {"endogenous": "x", "instruments": ["z"], "inference": "permutation"},
        "Permutation --inference is not supported with 2SLS",
        id="permutation-inference-with-iv",
    ),
]


@pytest.mark.parametrize(("overrides", "match"), IV_VALIDATION_CASES)
def test_2sls_validation_errors(tmp_path: Path, overrides: dict, match: str) -> None:
    panel = _iv_panel(tmp_path / "panel.parquet")
    kwargs = {
        "outcome": "y",
        "predictors": ["x"],
        "entity_fe": False,
        "year_fe": False,
        "cluster_by": None,
        "output_path": tmp_path / "iv.parquet",
        **overrides,
    }
    with pytest.raises(AnalysisError, match=match):
        regress_panel(panel, **kwargs)


def test_2sls_rejects_underidentified_design(tmp_path: Path) -> None:
    panel_path = tmp_path / "panel.parquet"
    df = pd.DataFrame(
        {
            "geo_id": [f"M{i}" for i in range(len(INSTRUMENT))],
            "year": [2020] * len(INSTRUMENT),
            "y": OUTCOME,
            "x": ENDOGENOUS,
            "constant_instrument": [1.0] * len(INSTRUMENT),
        }
    )
    write_parquet_with_provenance(df, panel_path, ProvenanceBlock(geo_type="msa", extra={}))
    with pytest.raises(AnalysisError, match="underidentified"):
        regress_panel(
            panel_path,
            outcome="y",
            predictors=["x"],
            entity_fe=False,
            year_fe=False,
            cluster_by=None,
            endogenous="x",
            instruments=["constant_instrument"],
            output_path=tmp_path / "iv.parquet",
        )


def test_2sls_wild_cluster_bootstrap_records_inference_p_value(tmp_path: Path) -> None:
    panel = _iv_panel(tmp_path / "panel.parquet")
    iv = regress_panel(
        panel,
        outcome="y",
        predictors=["x"],
        entity_fe=False,
        year_fe=False,
        cluster_by="geo_id",
        endogenous="x",
        instruments=["z"],
        inference="wild-cluster",
        inference_reps=19,
        inference_seed=123,
        inference_terms=["x"],
        output_path=tmp_path / "iv_wild.parquet",
    )

    structural = iv.table[(iv.table["stage"] == "structural") & (iv.table["term"] == "x")]
    assert structural["inference_method"].tolist() == ["wild-cluster"]
    assert structural["inference_reps"].tolist() == [19]
    assert structural["inference_term"].tolist() == [True]
    assert 0 <= structural["p_value"].iloc[0] <= 1
    assert iv.metadata["inference"] == "wild-cluster"
    assert iv.metadata["inference_terms"] == ["x"]


def test_2sls_wild_cluster_bootstrap_imposes_null_restriction() -> None:
    """Regression test for WRE: draw residuals from beta_x = 0, not unrestricted IV."""
    y = WRE_OUTCOME
    endogenous = WRE_ENDOGENOUS
    instrument = WRE_INSTRUMENT
    x = np.column_stack([np.ones_like(y), endogenous])
    z = np.column_stack([np.ones_like(y), instrument])
    clusters = pd.Series(WRE_CLUSTERS, name="geo_id")
    design_columns = pd.Index(["Intercept", "x"])
    fit = _fit_2sls(x=x, z=z, y=y, dof=len(y) - np.linalg.matrix_rank(x), clusters=clusters)

    actual = _wild_cluster_bootstrap_p_values(
        x=x,
        y=y,
        z=z,
        fit=fit,
        dof=len(y) - np.linalg.matrix_rank(x),
        clusters=clusters,
        terms=["x"],
        design_columns=design_columns,
        reps=WRE_REPS,
        seed=WRE_SEED,
    )

    term_index = 1
    observed = abs(float(fit.t_stats[term_index]))
    restricted_fitted, restricted_residuals = _restricted_iv_fitted_and_residuals(
        x=x,
        z=z,
        y=y,
        restricted_index=term_index,
    )
    rng = np.random.default_rng(WRE_SEED)
    expected_exceed = 0
    unrestricted_exceed = 0
    cluster_values = clusters.dropna().unique().tolist()
    cluster_array = clusters.to_numpy()
    for _ in range(WRE_REPS):
        weights_by_cluster = {
            cluster: rng.choice(np.array([-1.0, 1.0])) for cluster in cluster_values
        }
        weights = np.array([weights_by_cluster[cluster] for cluster in cluster_array])
        restricted_y_star = restricted_fitted + restricted_residuals * weights
        restricted_boot_fit = _fit_2sls(
            x=x,
            z=z,
            y=restricted_y_star,
            dof=len(y) - np.linalg.matrix_rank(x),
            clusters=clusters,
        )
        restricted_t = abs(
            float(restricted_boot_fit.beta[term_index] / restricted_boot_fit.std_errors[term_index])
        )
        if math.isfinite(restricted_t) and restricted_t >= observed:
            expected_exceed += 1

        unrestricted_y_star = fit.fitted + fit.residuals * weights
        unrestricted_boot_fit = _fit_2sls(
            x=x,
            z=z,
            y=unrestricted_y_star,
            dof=len(y) - np.linalg.matrix_rank(x),
            clusters=clusters,
        )
        unrestricted_t = abs(
            float(
                (unrestricted_boot_fit.beta[term_index] - fit.beta[term_index])
                / unrestricted_boot_fit.std_errors[term_index]
            )
        )
        if math.isfinite(unrestricted_t) and unrestricted_t >= observed:
            unrestricted_exceed += 1

    expected = (expected_exceed + 1) / (WRE_REPS + 1)
    old_unrestricted = (unrestricted_exceed + 1) / (WRE_REPS + 1)
    assert actual == {"x": pytest.approx(expected)}
    assert old_unrestricted != pytest.approx(expected)


def test_restricted_ols_fitted_values_impose_null_restriction() -> None:
    x = np.column_stack([np.ones(len(WRE_OUTCOME)), WRE_INSTRUMENT])
    null_value = 0.5

    fitted, residuals = _restricted_ols_fitted_and_residuals(
        x=x,
        y=WRE_OUTCOME,
        restricted_index=1,
        null_value=null_value,
    )

    adjusted_y = WRE_OUTCOME - WRE_INSTRUMENT * null_value
    expected_intercept = float(adjusted_y.mean())
    expected_fitted = expected_intercept + WRE_INSTRUMENT * null_value
    assert fitted == pytest.approx(expected_fitted)
    assert residuals == pytest.approx(WRE_OUTCOME - expected_fitted)


def test_cli_regress_2sls_emits_first_stage_in_json(tmp_path: Path) -> None:
    panel = _iv_panel(tmp_path / "panel.parquet")
    result = runner.invoke(
        app,
        [
            "analyze",
            "regress",
            "--panel",
            str(panel),
            "--outcome",
            "y",
            "--predictors",
            "x",
            "--no-entity-fe",
            "--no-year-fe",
            "--cluster-by",
            "",
            "--endogenous",
            "x",
            "--instruments",
            "z",
            "--output",
            str(tmp_path / "iv.parquet"),
            "--json",
        ],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["estimator"] == "2sls"
    assert payload["first_stage"]["instruments"] == ["z"]
    assert payload["first_stage"]["f_statistic"] > 10.0


def test_cli_iv_ar_confidence_set_includes_structural_coefficient(tmp_path: Path) -> None:
    panel = _iv_panel(tmp_path / "panel.parquet")
    result = runner.invoke(
        app,
        [
            "analyze",
            "iv-ar",
            "--panel",
            str(panel),
            "--outcome",
            "y",
            "--predictors",
            "x",
            "--endogenous",
            "x",
            "--instruments",
            "z",
            "--no-entity-fe",
            "--no-year-fe",
            "--cluster-by",
            "",
            "--grid-min",
            "1.0",
            "--grid-max",
            "3.0",
            "--grid-step",
            "0.5",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["analysis_type"] == "anderson_rubin"
    assert payload["iv_estimate"] == pytest.approx(STRUCTURAL_BETA)
    assert any(
        interval["lower"] <= STRUCTURAL_BETA <= interval["upper"]
        for interval in payload["confidence_set"]
    )
