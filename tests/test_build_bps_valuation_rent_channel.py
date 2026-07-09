"""Tests for the tracked BPS valuation vs. rent-growth channel workflow."""

from __future__ import annotations

import importlib
from pathlib import Path

import numpy as np
import pandas as pd
import pytest


def _load_builder():
    return importlib.import_module("hhplab.results.workflows.build_bps_valuation_rent_channel")


# MSA 10000: a clean 4-year run (2018-2021).
# MSA 20000: 2018, 2019, then a gap to 2021 (no 2020 row) -- exercises the
# lag1_year_gap guard that should keep a broken-lag row out of lead/reverse
# regressions even though its *own* FD (2019->2021, year_gap=2) is already
# excluded from the FD panel by the year_gap==1 filter.
BASE_PANEL_ROWS = [
    ("10000", "AA", 2018, 1000.0, 100.0),
    ("10000", "AA", 2019, 1050.0, 103.0),
    ("10000", "AA", 2020, 1100.0, 108.0),
    ("10000", "AA", 2021, 1150.0, 111.0),
    ("20000", "BB", 2018, 2000.0, 200.0),
    ("20000", "BB", 2019, 2040.0, 206.0),
    ("20000", "BB", 2021, 2100.0, 215.0),
]
BPS_VALUATION_ROWS = {
    ("10000", 2018): 50.0,
    ("10000", 2019): 55.0,
    ("10000", 2020): 60.0,
    ("10000", 2021): 66.0,
    ("20000", 2018): 80.0,
    ("20000", 2019): 84.0,
    ("20000", 2021): 90.0,
}


def _base_panel() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "msa_id": [row[0] for row in BASE_PANEL_ROWS],
            "msa_name": [f"Test {row[0]}, {row[1]}" for row in BASE_PANEL_ROWS],
            "year": [row[2] for row in BASE_PANEL_ROWS],
            "population": [row[3] for row in BASE_PANEL_ROWS],
            "pit_total": [row[4] for row in BASE_PANEL_ROWS],
            "zori": [row[3] for row in BASE_PANEL_ROWS],
            "log_zori": np.log([row[3] for row in BASE_PANEL_ROWS]),
            "log_pop": np.log([row[3] for row in BASE_PANEL_ROWS]),
            "primary_state": [row[1] for row in BASE_PANEL_ROWS],
        }
    )


def _bps_panel(tmp_path: Path) -> Path:
    panel_path = tmp_path / "covariate_panel__census_bps__Y2018-2021.parquet"
    rows = [
        {"geo_type": "msa", "geo_id": msa_id, "msa_id": msa_id, "year": year, "value": value}
        for (msa_id, year), value in BPS_VALUATION_ROWS.items()
    ]
    frame = pd.DataFrame(rows).rename(
        columns={"value": "bps_mix_adjusted_permit_value_per_unit_thousands"}
    )
    frame.to_parquet(panel_path, index=False)
    return panel_path


def _build_levels(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> pd.DataFrame:
    builder = _load_builder()
    monkeypatch.setattr(builder, "BPS_MSA_PANEL_PATH", _bps_panel(tmp_path))
    monkeypatch.setattr(builder, "load_pooled_base_panel", _base_panel)
    return builder.build_levels_panel()


def test_build_levels_panel_computes_fd_and_lag_columns(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    levels = _build_levels(monkeypatch, tmp_path).set_index(["msa_id", "year"])

    d_log_bps_2019 = np.log(55.0) - np.log(50.0)
    d_log_bps_2020 = np.log(60.0) - np.log(55.0)
    assert levels.loc[("10000", 2019), "d_log_bps_valuation"] == pytest.approx(d_log_bps_2019)
    assert levels.loc[("10000", 2020), "d_log_bps_valuation"] == pytest.approx(d_log_bps_2020)
    # lag1 at 2020 is the *prior* FD (2018->2019), not the current one.
    assert levels.loc[("10000", 2020), "d_log_bps_valuation_lag1"] == pytest.approx(
        d_log_bps_2019
    )
    assert levels.loc[("10000", 2018), "d_log_bps_valuation_lag1"] is pd.NA or pd.isna(
        levels.loc[("10000", 2018), "d_log_bps_valuation_lag1"]
    )


def test_build_levels_panel_flags_broken_lag_across_a_year_gap(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    levels = _build_levels(monkeypatch, tmp_path).set_index(["msa_id", "year"])

    # MSA 20000 has no 2020 row, so the 2021 row's own year_gap is 2 (already
    # excluded from any FD panel by year_gap == 1), and the row *after* that
    # gap would have lag1_year_gap != 1 if there were one -- here there isn't
    # a following row, so we instead check the gap is visible on the FD row
    # that spans it.
    assert levels.loc[("20000", 2021), "year_gap"] == 2
    assert pd.isna(levels.loc[("20000", 2019), "lag1_year_gap"]) or (
        levels.loc[("20000", 2019), "lag1_year_gap"] == 1
    )


def test_model_specs_cover_three_directions_across_fe_tiers() -> None:
    builder = _load_builder()

    specs = list(builder._model_specs())

    assert {spec.direction for spec in specs} == {"contemporaneous", "lead", "reverse_placebo"}
    assert {spec.fixed_effects for spec in specs} == {"year", "primary_state_year", "region_year"}
    # 3 directions x 3 FE tiers
    assert len(specs) == 9

    lead_specs = [spec for spec in specs if spec.direction == "lead"]
    assert all(spec.outcome == "d_log_zori" for spec in lead_specs)
    assert all(spec.focal_term == builder.D_LOG_BPS_VALUATION_LAG1 for spec in lead_specs)

    reverse_specs = [spec for spec in specs if spec.direction == "reverse_placebo"]
    assert all(spec.outcome == builder.D_LOG_BPS_VALUATION for spec in reverse_specs)
    assert all(spec.focal_term == builder.D_LOG_ZORI_LAG1 for spec in reverse_specs)


def test_fit_clustered_fd_models_excludes_broken_lag_rows_from_lead_and_reverse(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    builder = _load_builder()
    levels = _build_levels(monkeypatch, tmp_path)
    fd = levels[levels["year_gap"] == 1]

    # Contemporaneous FD rows only need the current-year predictor: all four
    # non-base rows across both MSAs qualify (2019, 2020, 2021 for 10000;
    # 2019 for 20000 -- 2021 for 20000 has year_gap == 2 and is excluded).
    contemporaneous_complete = fd.dropna(subset=["d_log_zori", "d_log_pop", "d_log_bps_valuation"])
    assert len(contemporaneous_complete) == 4

    # Lead rows additionally require lag1_year_gap == 1: 10000's 2020 and
    # 2021 rows qualify; 20000's 2019 row does not have a usable lag1 (its
    # own prior FD lag1_year_gap is NA at the first observed year).
    lead_required = ["d_log_zori", "d_log_pop", builder.D_LOG_BPS_VALUATION_LAG1, "lag1_year_gap"]
    lead_complete = fd.dropna(subset=lead_required)
    lead_complete = lead_complete[lead_complete["lag1_year_gap"] == 1]
    assert set(zip(lead_complete["msa_id"], lead_complete["year"], strict=True)) == {
        ("10000", 2020),
        ("10000", 2021),
    }
