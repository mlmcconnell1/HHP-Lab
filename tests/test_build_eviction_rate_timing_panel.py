"""Tests for the tracked eviction-rate timing result workflow."""

from __future__ import annotations

import importlib
from pathlib import Path

import numpy as np
import pandas as pd
import pytest


def _load_builder():
    return importlib.import_module("hhplab.results.workflows.build_eviction_rate_timing_panel")


def _base_panel() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "msa_id": ["10000", "10000", "10000", "10000"],
            "msa_name": ["Alpha, AA", "Alpha, AA", "Alpha, AA", "Alpha, AA"],
            "year": [2015, 2016, 2017, 2018],
            "population": [1000, 1010, 1025, 1040],
            "pit_total": [100, 100, 100, 100],
            "pit_sheltered": [50, 50, 50, 50],
            "pit_unsheltered": [50, 50, 50, 50],
            "unshelt_per_1000": [50, 49.5, 48.8, 48.1],
            "zori": [1000, 1050, 1110, 1175],
            "log_zori": np.log([1000, 1050, 1110, 1175]),
            "log_unshelt_rate": np.log([0.05, 0.0495, 0.0488, 0.0481]),
            "log_total_rate": np.log([0.10, 0.099, 0.0975, 0.096]),
            "log_shelt_rate": np.log([0.05, 0.0495, 0.0487, 0.0479]),
            "log_pop": np.log([1000, 1010, 1025, 1040]),
        }
    )


def test_build_levels_panel_aligns_lagged_and_lead_eviction_rate_diffs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    builder = _load_builder()
    panel_path = tmp_path / "covariate_panel__eviction_lab_national__Y2000-2018.parquet"
    pd.DataFrame(
        {
            "geo_type": ["msa"] * 4,
            "geo_id": ["10000"] * 4,
            "msa_id": ["10000"] * 4,
            "year": [2015, 2016, 2017, 2018],
            "eviction_filings": [100, 110, 132, 145],
            "eviction_rate": [2.0, 2.2, 2.64, 2.9],
            "coverage_ratio": [1.0, 1.0, 1.0, 1.0],
        }
    ).to_parquet(panel_path, index=False)
    monkeypatch.setenv("HHPLAB_EVICTION_NATIONAL_PANEL_PATH", str(panel_path))
    monkeypatch.setattr(builder, "load_eviction_base_panel", _base_panel)

    levels = builder.build_levels_panel().set_index("year")

    diff_2016 = np.log1p(2.2) - np.log1p(2.0)
    diff_2017 = np.log1p(2.64) - np.log1p(2.2)
    diff_2018 = np.log1p(2.9) - np.log1p(2.64)
    assert levels.loc[2016, "d_log_eviction_rate"] == pytest.approx(diff_2016)
    assert pd.isna(levels.loc[2016, "d_log_eviction_rate_lag1"])
    assert levels.loc[2017, "d_log_eviction_rate_lag1"] == pytest.approx(diff_2016)
    assert levels.loc[2017, "d_log_eviction_rate_lead1"] == pytest.approx(diff_2018)
    assert levels.loc[2018, "d_log_eviction_rate_lag1"] == pytest.approx(diff_2017)
    assert pd.isna(levels.loc[2018, "d_log_eviction_rate_lead1"])


def test_eviction_base_uses_canonical_panel_without_untracked_history(monkeypatch) -> None:
    builder = _load_builder()
    base = _base_panel().copy()
    base["cohort"] = "top50"
    base["primary_state"] = "AA"
    monkeypatch.setattr(builder, "load_pooled_base_panel", lambda: base)

    result = builder.load_eviction_base_panel()

    pd.testing.assert_frame_equal(result, base)


def test_model_specs_cover_lag_same_year_lead_and_reverse_directions() -> None:
    builder = _load_builder()

    specs = list(builder._model_specs())

    assert {spec.family for spec in specs} == {
        "same_year_screen",
        "lagged_channel",
        "lead_placebo",
        "reverse_causality",
    }
    assert any("d_log_eviction_rate_lag1" in spec.predictors for spec in specs)
    assert any("d_log_eviction_rate_lead1" in spec.predictors for spec in specs)
    assert any(spec.outcome == "d_log_eviction_rate_lead1" for spec in specs)
    assert {spec.fixed_effects for spec in specs} == {
        "year",
        "region_year",
        "primary_state_year",
    }
    assert len(specs) == 12


def test_eviction_models_run_full_fixed_effect_ladder() -> None:
    builder = _load_builder()
    rows = []
    msas = [
        ("10000", "CT"),
        ("11000", "CT"),
        ("20000", "NY"),
        ("21000", "NY"),
        ("30000", "IL"),
        ("31000", "IL"),
        ("40000", "OH"),
        ("41000", "OH"),
    ]
    for msa_index, (msa_id, state) in enumerate(msas):
        for year_index, year in enumerate((2016, 2017, 2018)):
            rows.append(
                {
                    "msa_id": msa_id,
                    "primary_state": state,
                    "year": year,
                    "d_log_zori": 0.02 + 0.003 * msa_index + 0.002 * year_index,
                    "d_log_pop": 0.01 + 0.001 * year_index,
                    "d_log_eviction_rate": 0.03 + 0.002 * msa_index,
                    "d_log_eviction_rate_lag1": 0.02 + 0.001 * msa_index,
                    "d_log_eviction_rate_lead1": 0.04 + 0.001 * year_index,
                }
            )

    result = builder.fit_clustered_fd_models(pd.DataFrame(rows))

    assert set(result["fixed_effects"]) == {
        "year",
        "region_year",
        "primary_state_year",
    }
    assert set(result["family"]) == {
        "same_year_screen",
        "lagged_channel",
        "lead_placebo",
        "reverse_causality",
    }
