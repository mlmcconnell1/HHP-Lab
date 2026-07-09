"""Tests for the tracked household-composition panel scripts."""

from __future__ import annotations

import importlib
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from hhplab.census_regions import census_region

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
    return importlib.import_module(f"hhplab.results.workflows.{name}")


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
        [("10000", "CT"), ("20000", "NY"), ("30000", "IL"), ("40000", "OH")]
    ):
        for year_index, year in enumerate([2019, 2020, 2022]):
            rows.append(
                {
                    "msa_id": msa_id,
                    "primary_state": state,
                    "year": year,
                    "log_zori": 1.0 + 0.2 * year_index + 0.03 * msa_index,
                    "d_log_zori": 0.1 + 0.01 * year_index + 0.02 * msa_index,
                    "log_unshelt_rate": -1.5 + 0.08 * year_index + 0.04 * msa_index,
                    "d_log_unshelt_rate": 0.03 + 0.015 * year_index + 0.01 * msa_index,
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

    state_year = robustness.fit_spec(
        frame,
        _spec_by_model(robustness.FD_RENTER_SHARE_SPECS, "rent_fd_renter_household_share_state_year_fe"),
    )
    levels = robustness.fit_spec(
        frame,
        _spec_by_model(
            robustness.LEVEL_FE_SPECS,
            "rent_levels_renter_household_share_msa_year_fe",
        ),
    )

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
        robustness.fit_spec(
            frame,
            _spec_by_model(
                robustness.FD_RENTER_SHARE_SPECS,
                "rent_fd_renter_household_share_state_year_fe",
            ),
        )


def test_composition_robustness_runs_region_year_specs() -> None:
    robustness = _load_script("analyze_composition_rent_population_robustness")
    frame = _robustness_fixture()
    fd_spec = _spec_by_model(
        robustness.FD_RENTER_SHARE_SPECS,
        "rent_fd_renter_household_share_region_year_fe",
    )
    levels_spec = _spec_by_model(
        robustness.LEVEL_FE_SPECS,
        "rent_levels_renter_household_share_msa_region_year_fe",
    )

    fd_result = robustness.fit_spec(frame, fd_spec)
    levels_result = robustness.fit_spec(frame, levels_spec)

    assert fd_result["fixed_effects"].unique().tolist() == ["region_year"]
    assert levels_result["fixed_effects"].unique().tolist() == ["msa_id+region_year"]
    assert set(fd_result["term"]) == {"d_log_pop", "d_renter_household_share"}
    assert set(levels_result["term"]) == {"log_pop", "renter_household_share"}
    assert fd_result["nobs"].unique().tolist() == [len(frame)]
    assert levels_result["clusters"].unique().tolist() == [frame["msa_id"].nunique()]


def test_composition_robustness_requires_state_for_region_year_fe() -> None:
    robustness = _load_script("analyze_composition_rent_population_robustness")
    frame = _robustness_fixture().drop(columns=["primary_state"])
    spec = _spec_by_model(
        robustness.FD_RENTER_SHARE_SPECS,
        "rent_fd_renter_household_share_region_year_fe",
    )

    with pytest.raises(ValueError, match="region_year fixed effects require"):
        robustness.fit_spec(frame, spec)


@pytest.mark.parametrize("value, expected", [("ct", "Northeast"), (" ny ", "Northeast")])
def test_census_region_normalizes_valid_state_codes(value: str, expected: str) -> None:
    assert census_region(value) == expected


def test_census_region_rejects_unmapped_state_codes() -> None:
    with pytest.raises(ValueError, match="Unknown Census state abbreviation"):
        census_region("PR")


@pytest.mark.parametrize(
    "model",
    [
        "rent_fd_renter_household_share_region_year_fe",
        "rent_levels_renter_household_share_msa_region_year_fe",
    ],
)
def test_composition_robustness_rejects_unmapped_state_for_region_year(model: str) -> None:
    robustness = _load_script("analyze_composition_rent_population_robustness")
    frame = _robustness_fixture()
    frame.loc[0, "primary_state"] = "PR"
    spec = _spec_by_model(robustness.FD_RENTER_SHARE_SPECS, model) if model.startswith(
        "rent_fd"
    ) else _spec_by_model(robustness.LEVEL_FE_SPECS, model)

    with pytest.raises(ValueError, match="Unknown Census state abbreviation"):
        robustness.fit_spec(frame, spec)


def _spec_by_model(specs, model: str):
    for spec in specs:
        if spec.model == model:
            return spec
    raise AssertionError(f"no spec named {model!r}")


def test_composition_robustness_runs_msa_state_year_fe_levels_spec() -> None:
    robustness = _load_script("analyze_composition_rent_population_robustness")
    frame = _robustness_fixture()
    spec = _spec_by_model(
        robustness.LEVEL_FE_SPECS,
        "rent_levels_renter_household_share_msa_state_year_fe",
    )

    result = robustness.fit_spec(frame, spec)

    assert result["fixed_effects"].unique().tolist() == ["msa_id+primary_state_year"]
    assert set(result["term"]) == {"log_pop", "renter_household_share"}
    assert result["nobs"].unique().tolist() == [len(frame)]
    assert result["clusters"].unique().tolist() == [frame["msa_id"].nunique()]


def test_composition_robustness_runs_direct_unsheltered_specs() -> None:
    robustness = _load_script("analyze_composition_rent_population_robustness")
    frame = _robustness_fixture()
    fd_spec = _spec_by_model(
        robustness.FD_RENTER_SHARE_SPECS,
        "unsheltered_fd_renter_household_share_state_year_fe",
    )
    levels_spec = _spec_by_model(
        robustness.LEVEL_FE_SPECS,
        "unsheltered_levels_renter_household_share_msa_year_fe",
    )

    fd_result = robustness.fit_spec(frame, fd_spec)
    levels_result = robustness.fit_spec(frame, levels_spec)

    assert fd_result["fixed_effects"].unique().tolist() == ["primary_state_year"]
    assert levels_result["fixed_effects"].unique().tolist() == ["msa_id+year"]
    assert set(fd_result["term"]) == {"d_renter_household_share"}
    assert set(levels_result["term"]) == {"renter_household_share"}
    assert fd_result["focal_term"].unique().tolist() == [True]
    assert levels_result["focal_term"].unique().tolist() == [True]


def test_composition_robustness_builds_centered_interaction_terms() -> None:
    robustness = _load_script("analyze_composition_rent_population_robustness")
    frame = _robustness_fixture()
    spec = _spec_by_model(
        robustness.FD_RENTER_SHARE_SPECS,
        "unsheltered_fd_renter_household_share_interaction_year_fe",
    )

    sample = robustness._prepare_sample(frame, spec)
    result = robustness.fit_spec(frame, spec)

    assert sample["renter_household_share_c"].mean() == pytest.approx(0.0)
    assert sample["d_log_zori_x_renter_share_c"].equals(
        sample["d_log_zori"] * sample["renter_household_share_c"]
    )
    assert set(result["term"]) == {
        "d_log_zori",
        "d_log_pop",
        "renter_household_share_c",
        "d_log_zori_x_renter_share_c",
    }
    focal_terms = result.loc[result["focal_term"], "term"].tolist()
    assert focal_terms == [
        "d_log_zori",
        "renter_household_share_c",
        "d_log_zori_x_renter_share_c",
    ]
