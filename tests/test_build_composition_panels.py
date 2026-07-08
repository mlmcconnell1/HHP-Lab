"""Tests for the tracked household-composition panel scripts."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
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


def _load_script(name: str):
    if str(SCRIPTS_DIR) not in sys.path:
        sys.path.insert(0, str(SCRIPTS_DIR))
    spec = importlib.util.spec_from_file_location(name, SCRIPTS_DIR / f"{name}.py")
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _base_panel() -> pd.DataFrame:
    return pd.DataFrame(BASE_ROWS)


def test_renter_safe_ratio_and_log_guard_invalid_values() -> None:
    renter = _load_script("build_renter_household_share_composition_panel")

    ratio = renter._safe_ratio(pd.Series([10, 10, 10]), pd.Series([2, 0, -1]))
    logged = renter._safe_log(pd.Series([4, 0, -2]))

    assert ratio.iloc[0] == pytest.approx(5)
    assert pd.isna(ratio.iloc[1])
    assert pd.isna(ratio.iloc[2])
    assert logged.iloc[0] == pytest.approx(np.log(4))
    assert pd.isna(logged.iloc[1])
    assert pd.isna(logged.iloc[2])


def test_renter_panel_uses_acs5_vintage_end_year_plus_one_and_preserves_identity(
    monkeypatch,
    tmp_path: Path,
) -> None:
    renter = _load_script("build_renter_household_share_composition_panel")
    measures_path = tmp_path / "measures__msa__A2019@Mcensusmsa2023xT2010.parquet"
    pd.DataFrame(
        {
            "msa_id": ["10000"],
            "acs_vintage": ["2015-2019"],
            "total_population": [100_000],
            "total_households": [100],
            "owner_households": [60],
            "renter_households": [40],
        }
    ).to_parquet(measures_path, index=False)
    monkeypatch.setattr(renter, "MEASURES_GLOB", str(tmp_path / "measures__msa__A*.parquet"))
    monkeypatch.setattr(renter, "load_pooled_base_panel", _base_panel)

    levels = renter.build_levels_panel().set_index("year")

    assert levels.loc[2020, "acs5_vintage_used"] == 2019
    assert levels.loc[2020, "total_households"] == (
        levels.loc[2020, "owner_households"] + levels.loc[2020, "renter_households"]
    )
    assert levels.loc[2020, "renter_household_share"] == pytest.approx(0.40)
    assert pd.isna(levels.loc[2021, "renter_households"])


def test_household_size_panel_uses_acs1_vintage_plus_one(monkeypatch, tmp_path: Path) -> None:
    household = _load_script("build_household_size_composition_panel")
    acs1_path = tmp_path / "acs1_metro__A2019@Dcensusmsa2023.parquet"
    pd.DataFrame(
        {
            "metro_id": ["10000"],
            "acs1_vintage": ["2019"],
            "average_household_size_total": [2.5],
            "average_household_size_owner_occupied": [2.7],
            "average_household_size_renter_occupied": [2.1],
        }
    ).to_parquet(acs1_path, index=False)
    monkeypatch.setattr(household, "ACS1_METRO_GLOB", str(tmp_path / "acs1_metro__A*.parquet"))
    monkeypatch.setattr(household, "load_pooled_base_panel", _base_panel)

    levels = household.build_levels_panel().set_index("year")

    assert levels.loc[2020, "acs1_vintage_used"] == 2019
    assert levels.loc[2020, "average_household_size_renter_occupied"] == pytest.approx(2.1)
    assert pd.isna(levels.loc[2021, "average_household_size_renter_occupied"])


def test_recent_mover_income_panel_uses_acs1_lag_and_safe_ratios(
    monkeypatch,
    tmp_path: Path,
) -> None:
    recent = _load_script("build_recent_mover_income_composition_panel")
    acs1_path = tmp_path / "acs1_metro__A2019@Dcensusmsa2023.parquet"
    pd.DataFrame(
        {
            "metro_id": ["10000"],
            "acs1_vintage": ["2019"],
            "median_income_mobility_total": [50_000],
            "median_income_same_house_1_year_ago": [0],
            "median_income_moved_within_county": [40_000],
            "median_income_moved_diff_county_same_state": [45_000],
            "median_income_moved_diff_state": [55_000],
            "median_income_moved_from_abroad": [30_000],
        }
    ).to_parquet(acs1_path, index=False)
    monkeypatch.setattr(recent, "ACS1_METRO_GLOB", str(tmp_path / "acs1_metro__A*.parquet"))
    monkeypatch.setattr(recent, "load_pooled_base_panel", _base_panel)

    levels = recent.build_levels_panel().set_index("year")

    assert levels.loc[2020, "acs1_vintage_used"] == 2019
    assert levels.loc[2020, "moved_diff_state_income_ratio_total"] == pytest.approx(1.1)
    assert pd.isna(levels.loc[2020, "moved_diff_state_income_ratio_same_house"])
    assert pd.isna(levels.loc[2021, "median_income_moved_diff_state"])


def _robustness_fixture() -> pd.DataFrame:
    rows = []
    for msa_index, (msa_id, state) in enumerate(
        [("10000", "AA"), ("20000", "AA"), ("30000", "BB"), ("40000", "BB")]
    ):
        for year_index, year in enumerate([2019, 2020, 2022]):
            rows.append(
                {
                    "msa_id": msa_id,
                    "primary_state": state,
                    "year": year,
                    "log_zori": 1.0 + 0.2 * year_index + 0.03 * msa_index,
                    "d_log_zori": 0.1 + 0.01 * year_index + 0.02 * msa_index,
                    "log_pop": 10.0 + 0.05 * year_index + 0.01 * msa_index,
                    "d_log_pop": 0.02 + 0.01 * year_index,
                    "renter_household_share": 0.35 + 0.02 * year_index + 0.01 * msa_index,
                    "d_renter_household_share": 0.01 * year_index + 0.005 * msa_index,
                    "average_household_size_renter_occupied": 2.0 + 0.03 * year_index,
                    "moved_diff_state_income_ratio_total": 1.1 + 0.02 * msa_index,
                }
            )
    return pd.DataFrame(rows)


def test_composition_robustness_runs_state_year_and_levels_fe_specs() -> None:
    robustness = _load_script("analyze_composition_rent_population_robustness")
    frame = _robustness_fixture()

    state_year = robustness.fit_spec(frame, robustness.FD_RENTER_SHARE_SPECS[1])
    levels = robustness.fit_spec(frame, robustness.LEVEL_FE_SPECS[0])

    assert state_year["fixed_effects"].unique().tolist() == ["primary_state_year"]
    assert levels["fixed_effects"].unique().tolist() == ["msa_id+year"]
    assert set(state_year["term"]) == {"d_log_pop", "d_renter_household_share"}
    assert set(levels["term"]) == {"log_pop", "renter_household_share"}
    assert state_year["nobs"].unique().tolist() == [len(frame)]
    assert levels["clusters"].unique().tolist() == [frame["msa_id"].nunique()]


def test_composition_robustness_requires_state_for_state_year_fe() -> None:
    robustness = _load_script("analyze_composition_rent_population_robustness")
    frame = _robustness_fixture().drop(columns=["primary_state"])

    with pytest.raises(ValueError, match="primary_state_year fixed effects require"):
        robustness.fit_spec(frame, robustness.FD_RENTER_SHARE_SPECS[1])
