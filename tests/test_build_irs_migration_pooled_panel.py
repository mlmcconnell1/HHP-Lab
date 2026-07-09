"""Tests for the tracked pooled IRS migration workflow."""

from __future__ import annotations

import importlib
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

BASE_PANEL_ROWS = [
    {
        "msa_id": "10000",
        "msa_name": "Alpha, CT",
        "year": 2015,
        "population": 100_000,
        "sanctuary": 0,
        "pit_unsheltered": 20,
        "unshelt_per_1000": 0.20,
        "zori": 1000,
        "log_zori": np.log(1000),
        "log_unshelt_rate": np.log(0.20),
        "log_pop": np.log(100_000),
    },
    {
        "msa_id": "10000",
        "msa_name": "Alpha, CT",
        "year": 2016,
        "population": 101_000,
        "sanctuary": 0,
        "pit_unsheltered": 24,
        "unshelt_per_1000": 0.24,
        "zori": 1060,
        "log_zori": np.log(1060),
        "log_unshelt_rate": np.log(0.24),
        "log_pop": np.log(101_000),
    },
    {
        "msa_id": "10000",
        "msa_name": "Alpha, CT",
        "year": 2017,
        "population": 102_000,
        "sanctuary": 0,
        "pit_unsheltered": 25,
        "unshelt_per_1000": 0.245,
        "zori": 1100,
        "log_zori": np.log(1100),
        "log_unshelt_rate": np.log(0.245),
        "log_pop": np.log(102_000),
    },
]
IRS_PANEL_ROWS = [
    {
        "geo_type": "msa",
        "msa_id": "10000",
        "year": 2014,
        "inflow_returns": 10.0,
        "inflow_agi_thousands": 200.0,
        "outflow_returns": 5.0,
        "outflow_agi_thousands": 150.0,
        "net_returns": 5.0,
        "net_agi_thousands": 50.0,
        "intra_msa_returns": 4.0,
        "intra_msa_agi_thousands": 60.0,
        "coverage_ratio": 0.5,
    },
    {
        "geo_type": "msa",
        "msa_id": "10000",
        "year": 2015,
        "inflow_returns": 8.0,
        "inflow_agi_thousands": 240.0,
        "outflow_returns": 6.0,
        "outflow_agi_thousands": 120.0,
        "net_returns": 2.0,
        "net_agi_thousands": 120.0,
        "intra_msa_returns": 3.0,
        "intra_msa_agi_thousands": 45.0,
        "coverage_ratio": 0.6,
    },
    {
        "geo_type": "msa",
        "msa_id": "10000",
        "year": 2016,
        "inflow_returns": 0.0,
        "inflow_agi_thousands": 0.0,
        "outflow_returns": 4.0,
        "outflow_agi_thousands": 80.0,
        "net_returns": -4.0,
        "net_agi_thousands": -80.0,
        "intra_msa_returns": 2.0,
        "intra_msa_agi_thousands": 30.0,
        "coverage_ratio": 0.7,
    },
]
EXPECTED_DIRECT_INCOME_MODELS = {
    ("rent_fd_inflow_agi_per_return_k_year_fe", ("d_log_pop", "inflow_agi_per_return_k"), ("year",)),
    (
        "rent_fd_inflow_agi_per_return_k_region_year_fe",
        ("d_log_pop", "inflow_agi_per_return_k"),
        ("region_year",),
    ),
    (
        "rent_fd_inflow_agi_per_return_k_state_year_fe",
        ("d_log_pop", "inflow_agi_per_return_k"),
        ("primary_state_year",),
    ),
}
EXPECTED_JOINT_MODEL = (
    "rent_fd_inflow_outflow_agi_per_return_joint_state_year_fe",
    ("d_log_pop", "inflow_agi_per_return_k", "outflow_agi_per_return_k"),
    ("primary_state_year",),
)


def _load_builder():
    return importlib.import_module("hhplab.results.workflows.build_irs_migration_pooled_panel")


def _base_panel() -> pd.DataFrame:
    return pd.DataFrame(BASE_PANEL_ROWS)


def test_build_levels_panel_aligns_irs_year_and_derives_direct_income_measures(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    builder = _load_builder()
    panel_path = tmp_path / "covariate_panel__irs_soi_migration__Y2014-2016.parquet"
    pd.DataFrame(IRS_PANEL_ROWS).to_parquet(panel_path, index=False)
    monkeypatch.setattr(builder, "IRS_COVARIATE_PANEL", panel_path)
    monkeypatch.setattr(builder, "load_pooled_base_panel", _base_panel)

    levels = builder.build_levels_panel().set_index("year")

    assert levels.loc[2015, "irs_year"] == 2014
    assert levels.loc[2015, "inflow_agi_per_return_k"] == pytest.approx(20.0)
    assert levels.loc[2015, "outflow_agi_per_return_k"] == pytest.approx(30.0)
    assert levels.loc[2015, "churn_agi_per_return_k"] == pytest.approx(350.0 / 15.0)
    assert levels.loc[2015, "inflow_outflow_agi_gap_k"] == pytest.approx(-10.0)
    assert levels.loc[2016, "inflow_agi_per_return_k"] == pytest.approx(30.0)
    assert levels.loc[2016, "outflow_agi_per_return_k"] == pytest.approx(20.0)
    assert levels.loc[2016, "inflow_outflow_agi_gap_k"] == pytest.approx(10.0)
    assert pd.isna(levels.loc[2017, "inflow_agi_per_return_k"])
    assert levels.loc[2016, "d_log_zori_x_churn_rate"] == pytest.approx(
        levels.loc[2016, "d_log_zori"] * levels.loc[2016, "churn_rate"]
    )


def test_model_specs_cover_direct_income_predictors_across_fe_variants() -> None:
    builder = _load_builder()

    specs = list(builder._model_specs())

    assert {spec.family for spec in specs} == {
        "direct_income_channel",
        "direct_income_joint_channel",
    }
    assert {
        (spec.name, spec.predictors, spec.fixed_effects)
        for spec in specs
        if spec.name.startswith("rent_fd_inflow_agi_per_return_k_")
    } == EXPECTED_DIRECT_INCOME_MODELS
    assert any(
        (spec.name, spec.predictors, spec.fixed_effects) == EXPECTED_JOINT_MODEL
        for spec in specs
    )
    assert len(specs) == len(builder.DIRECT_INCOME_COLUMNS) * 3 + 3
