"""Tests for the tracked QCEW labor-market result workflow."""

from __future__ import annotations

import importlib
from pathlib import Path

import numpy as np
import pandas as pd
import pytest


def _load_builder():
    return importlib.import_module("hhplab.results.workflows.build_qcew_labor_market_panel")


def _base_panel() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "msa_id": ["10000", "10000", "10000"],
            "msa_name": ["Alpha, AA", "Alpha, AA", "Alpha, AA"],
            "year": [2014, 2015, 2016],
            "population": [1000, 1010, 1025],
            "pit_total": [100, 100, 100],
            "pit_sheltered": [50, 50, 50],
            "pit_unsheltered": [50, 50, 50],
            "unshelt_per_1000": [50, 49.5, 48.8],
            "zori": [1000, 1050, 1110],
            "log_zori": np.log([1000, 1050, 1110]),
            "log_unshelt_rate": np.log([0.05, 0.0495, 0.0488]),
            "log_total_rate": np.log([0.10, 0.099, 0.0975]),
            "log_shelt_rate": np.log([0.05, 0.0495, 0.0487]),
            "log_pop": np.log([1000, 1010, 1025]),
            "primary_state": ["AA", "AA", "AA"],
        }
    )


def _qcew_panel(tmp_path: Path) -> Path:
    panel_path = tmp_path / "covariate_panel__qcew__Y2014-2016.parquet"
    pd.DataFrame(
        {
            "geo_type": ["msa"] * 3,
            "geo_id": ["10000"] * 3,
            "msa_id": ["10000"] * 3,
            "year": [2014, 2015, 2016],
            "annual_avg_emplvl": [1000.0, 1050.0, 1100.0],
            "total_annual_wages": [50_000_000.0, 53_000_000.0, 57_000_000.0],
            "annual_avg_estabs": [100.0, 101.0, 103.0],
            "annual_avg_weekly_wage": [961.5, 970.9, 996.5],
            "avg_annual_pay": [50000.0, 50476.2, 51818.2],
            "coverage_ratio": [1.0, 1.0, 1.0],
        }
    ).to_parquet(panel_path, index=False)
    return panel_path


def _cpi_index(tmp_path: Path) -> Path:
    cpi_path = tmp_path / "cpi_u__Aall.parquet"
    pd.DataFrame(
        {
            "year": [2014, 2015, 2016],
            "cpi_u": [236.736, 237.017, 240.007],
        }
    ).to_parquet(cpi_path, index=False)
    return cpi_path


def test_build_levels_panel_derives_real_wage_growth_net_of_cpi(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    builder = _load_builder()
    monkeypatch.setenv("HHPLAB_QCEW_PANEL_PATH", str(_qcew_panel(tmp_path)))
    monkeypatch.setattr(builder, "CPI_U_PATH", _cpi_index(tmp_path))
    monkeypatch.setattr(builder, "load_pooled_base_panel", _base_panel)

    levels, dropped = builder.build_levels_panel()
    levels = levels.set_index("year")

    assert dropped == 0
    d_log_wage_2015 = np.log(53_000_000.0) - np.log(50_000_000.0)
    d_log_cpi_2015 = np.log(237.017) - np.log(236.736)
    assert levels.loc[2015, "d_log_qcew_total_annual_wages"] == pytest.approx(d_log_wage_2015)
    assert levels.loc[2015, "d_log_qcew_total_annual_wages_real"] == pytest.approx(
        d_log_wage_2015 - d_log_cpi_2015
    )


def test_build_levels_panel_excludes_partial_county_coverage_rows(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    builder = _load_builder()
    panel_path = tmp_path / "covariate_panel__qcew__Y2014-2016.parquet"
    pd.DataFrame(
        {
            "geo_type": ["msa"] * 3,
            "geo_id": ["10000"] * 3,
            "msa_id": ["10000"] * 3,
            "year": [2014, 2015, 2016],
            "annual_avg_emplvl": [1000.0, 1050.0, 1100.0],
            "total_annual_wages": [50_000_000.0, 53_000_000.0, 57_000_000.0],
            "annual_avg_estabs": [100.0, 101.0, 103.0],
            "annual_avg_weekly_wage": [961.5, 970.9, 996.5],
            "avg_annual_pay": [50000.0, 50476.2, 51818.2],
            "coverage_ratio": [1.0, 0.5, 1.0],
        }
    ).to_parquet(panel_path, index=False)
    monkeypatch.setenv("HHPLAB_QCEW_PANEL_PATH", str(panel_path))
    monkeypatch.setattr(builder, "CPI_U_PATH", _cpi_index(tmp_path))
    monkeypatch.setattr(builder, "load_pooled_base_panel", _base_panel)

    levels, dropped = builder.build_levels_panel()
    levels = levels.set_index("year")

    assert dropped == 1
    assert pd.isna(levels.loc[2015, "qcew_annual_avg_emplvl"])
    assert levels.loc[2014, "qcew_annual_avg_emplvl"] == pytest.approx(1000.0)
    assert levels.loc[2016, "qcew_annual_avg_emplvl"] == pytest.approx(1100.0)


def test_model_specs_cover_employment_and_real_wage_channels_across_fe_tiers() -> None:
    builder = _load_builder()

    specs = list(builder._model_specs())

    assert {spec.family for spec in specs} == {
        "d_log_qcew_annual_avg_emplvl",
        "d_log_qcew_total_annual_wages_real",
        "d_log_qcew_annual_avg_weekly_wage_real",
    }
    assert {spec.fixed_effects for spec in specs} == {
        ("year",),
        ("primary_state_year",),
        ("region_year",),
    }
    assert all(spec.outcome in {"d_log_zori", "d_log_unshelt_rate"} for spec in specs)
    # 3 channels x 3 FE tiers x 2 outcome families
    assert len(specs) == 18
