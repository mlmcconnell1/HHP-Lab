"""Tests for the tracked non-compositional rent-population robustness script."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pandas as pd
import pytest

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"


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


def _fd_fixture() -> pd.DataFrame:
    rows = []
    for msa_index, (msa_id, state) in enumerate(
        [("10000", "AA"), ("20000", "AA"), ("30000", "BB"), ("40000", "BB")]
    ):
        supply_constraint_bps = 0.25 + 0.2 * msa_index
        for year_index, year in enumerate([2017, 2018, 2019]):
            rows.append(
                {
                    "msa_id": msa_id,
                    "primary_state": state,
                    "year": year,
                    "d_log_zori": 0.1 + 0.01 * year_index + 0.02 * msa_index,
                    "d_log_pop": 0.02 + 0.01 * year_index,
                    "supply_constraint_bps": supply_constraint_bps,
                    "d_log_pop_x_supply_constraint_bps": (0.02 + 0.01 * year_index)
                    * supply_constraint_bps,
                    "d_seasonal_recreational_vacancy_share": 0.01 * year_index
                    + 0.005 * msa_index,
                }
            )
    return pd.DataFrame(rows)


def test_noncompositional_robustness_runs_str_state_year_fe_spec() -> None:
    robustness = _load_script("analyze_noncompositional_rent_population_robustness")
    frame = _fd_fixture()
    spec = next(
        spec
        for spec in robustness.STATE_YEAR_FE_SPECS
        if spec.model == "rent_fd_seasonal_recreational_vacancy_share_state_year_fe"
    )

    result = robustness.fit_spec(frame, spec)

    assert result["fixed_effects"].unique().tolist() == ["primary_state_year"]
    assert set(result["term"]) == {"d_log_pop", "d_seasonal_recreational_vacancy_share"}
    assert result["nobs"].unique().tolist() == [len(frame)]
    assert result["clusters"].unique().tolist() == [frame["msa_id"].nunique()]


def test_noncompositional_robustness_runs_supply_state_year_fe_spec() -> None:
    robustness = _load_script("analyze_noncompositional_rent_population_robustness")
    frame = _fd_fixture()
    spec = next(
        spec
        for spec in robustness.STATE_YEAR_FE_SPECS
        if spec.model == "rent_fd_population_x_supply_constraint_bps_state_year_fe"
    )

    result = robustness.fit_spec(frame, spec)

    assert result["fixed_effects"].unique().tolist() == ["primary_state_year"]
    assert set(result["term"]) == {
        "d_log_pop",
        "supply_constraint_bps",
        "d_log_pop_x_supply_constraint_bps",
    }
    assert result["nobs"].unique().tolist() == [len(frame)]
    assert result["clusters"].unique().tolist() == [frame["msa_id"].nunique()]


def test_noncompositional_robustness_rejects_supply_exposure_overlap() -> None:
    robustness = _load_script("analyze_noncompositional_rent_population_robustness")
    frame = _fd_fixture()
    frame.loc[0, "year"] = 2015
    spec = next(
        spec
        for spec in robustness.STATE_YEAR_FE_SPECS
        if spec.model == "rent_fd_population_x_supply_constraint_bps_state_year_fe"
    )

    with pytest.raises(ValueError, match="BPS exposure window"):
        robustness.fit_spec(frame, spec)


def test_noncompositional_robustness_requires_state_for_state_year_fe() -> None:
    robustness = _load_script("analyze_noncompositional_rent_population_robustness")
    frame = _fd_fixture().drop(columns=["primary_state"])

    with pytest.raises(ValueError, match="primary_state_year fixed effects require"):
        robustness.fit_spec(frame, robustness.STATE_YEAR_FE_SPECS[0])


def test_noncompositional_robustness_requires_panel_artifact(tmp_path, monkeypatch) -> None:
    robustness = _load_script("analyze_noncompositional_rent_population_robustness")
    missing_path = tmp_path / "does_not_exist.parquet"

    with pytest.raises(FileNotFoundError, match="Run scripts/build_noncompositional"):
        robustness.load_required_parquet(missing_path)
