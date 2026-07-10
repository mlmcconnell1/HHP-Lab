"""Tests for the tracked subsidized-housing-stock workflow."""

from __future__ import annotations

import importlib
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

BASE_ROWS = [
    {
        "msa_id": "10000",
        "msa_name": "Alpha, AA",
        "year": 2019,
        "population": 100_000,
        "pit_total": 100,
        "pit_sheltered": 80,
        "pit_unsheltered": 20,
        "unshelt_per_1000": 0.20,
        "zori": 1000,
        "log_zori": np.log(1000),
        "log_unshelt_rate": np.log(0.20),
        "log_total_rate": np.log(1.00),
        "log_shelt_rate": np.log(0.80),
        "log_pop": np.log(100_000),
    },
    {
        "msa_id": "10000",
        "msa_name": "Alpha, AA",
        "year": 2020,
        "population": 101_000,
        "pit_total": 110,
        "pit_sheltered": 85,
        "pit_unsheltered": 25,
        "unshelt_per_1000": 0.25,
        "zori": 1050,
        "log_zori": np.log(1050),
        "log_unshelt_rate": np.log(0.25),
        "log_total_rate": np.log(1.10),
        "log_shelt_rate": np.log(0.85),
        "log_pop": np.log(101_000),
    },
    {
        "msa_id": "10000",
        "msa_name": "Alpha, AA",
        "year": 2021,
        "population": 102_000,
        "pit_total": 120,
        "pit_sheltered": 90,
        "pit_unsheltered": 30,
        "unshelt_per_1000": 0.30,
        "zori": 1100,
        "log_zori": np.log(1100),
        "log_unshelt_rate": np.log(0.30),
        "log_total_rate": np.log(1.20),
        "log_shelt_rate": np.log(0.90),
        "log_pop": np.log(102_000),
    },
]


def _load_builder():
    return importlib.import_module("hhplab.results.workflows.build_subsidized_housing_stock_panel")


def _base_panel() -> pd.DataFrame:
    return pd.DataFrame(BASE_ROWS)


def test_subsidized_housing_helpers_guard_invalid_values() -> None:
    builder = _load_builder()

    per_1000 = builder._safe_per_1000(pd.Series([10, 10]), pd.Series([2, 0]))
    logged = builder._safe_log(pd.Series([4, 0, -2]))

    assert per_1000.iloc[0] == pytest.approx(5000)
    assert pd.isna(per_1000.iloc[1])
    assert logged.iloc[0] == pytest.approx(np.log(4))
    assert pd.isna(logged.iloc[1])
    assert pd.isna(logged.iloc[2])


def test_ensure_hud_psh_msa_panel_requires_curated_source(
    monkeypatch,
    tmp_path: Path,
) -> None:
    builder = _load_builder()
    monkeypatch.setattr(
        builder,
        "default_covariate_panel_path",
        lambda source_id: tmp_path / "msa.parquet",
    )
    monkeypatch.setattr(
        builder,
        "default_covariate_output_path",
        lambda source_id: tmp_path / "county.parquet",
    )

    with pytest.raises(FileNotFoundError, match="ingest covariate --source hud_psh"):
        builder.ensure_hud_psh_msa_panel([2020, 2022])


def test_subsidized_housing_panel_merges_and_derives_per_1000(monkeypatch) -> None:
    builder = _load_builder()
    hud_psh = pd.DataFrame(
        {
            "msa_id": ["10000", "10000"],
            "year": [2020, 2021],
            "subsidized_households": [500, 550],
            "housing_choice_vouchers": [200, 220],
            "hud_psh_coverage_ratio": [1.0, 1.0],
        }
    )
    monkeypatch.setattr(builder, "load_pooled_base_panel", _base_panel)
    monkeypatch.setattr(builder, "load_hud_psh_msa_panel", lambda *, years: hud_psh)

    levels = builder.build_levels_panel().set_index("year")

    assert levels.loc[2020, "subsidized_households_per_1000"] == pytest.approx(500 / 101_000 * 1000)
    assert levels.loc[2020, "log_housing_choice_vouchers_per_1000"] == pytest.approx(
        np.log(200 / 101_000 * 1000)
    )
    assert levels.loc[2021, "d_log_subsidized_households_per_1000"] == pytest.approx(
        np.log(550 / 102_000 * 1000) - np.log(500 / 101_000 * 1000)
    )
    assert pd.isna(levels.loc[2019, "subsidized_households"])


def test_subsidized_housing_fd_models_include_both_stock_predictors() -> None:
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
    for msa_index, (msa_id, state) in enumerate(msas, start=1):
        for year_index, year in enumerate([2020, 2022], start=1):
            rows.append(
                {
                    "msa_id": msa_id,
                    "year": year,
                    "primary_state": state,
                    "d_log_zori": 0.02 * year_index + 0.01 * msa_index,
                    "d_log_pop": 0.01 * year_index,
                    "d_log_subsidized_households_per_1000": 0.004 * msa_index,
                    "d_log_housing_choice_vouchers_per_1000": 0.005 * msa_index,
                }
            )
    fd = pd.DataFrame(rows)

    regressions = builder.fit_clustered_fd_models(fd)

    subsidized_terms = regressions.loc[
        regressions["model"] == "rent_fd_log_subsidized_households_per_1000",
        "term",
    ].tolist()
    voucher_terms = regressions.loc[
        regressions["model"] == "rent_fd_log_housing_choice_vouchers_per_1000",
        "term",
    ].tolist()

    assert subsidized_terms == ["d_log_pop", "d_log_subsidized_households_per_1000"]
    assert voucher_terms == ["d_log_pop", "d_log_housing_choice_vouchers_per_1000"]
    assert set(regressions["fixed_effects"]) == {
        "year",
        "region_year",
        "primary_state_year",
    }
