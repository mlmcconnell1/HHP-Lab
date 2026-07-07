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
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from typer.testing import CliRunner

from hhplab.analyze import AnalysisError, regress_panel
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
        {"endogenous": "x", "instruments": ["z"], "inference": "wild-cluster"},
        "not supported with 2SLS",
        id="small-sample-inference-with-iv",
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
