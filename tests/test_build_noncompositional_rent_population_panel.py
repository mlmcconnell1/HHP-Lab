"""Tests for the tracked non-compositional rent/population screens."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pandas as pd
import pytest

SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "scripts"
    / "build_noncompositional_rent_population_panel.py"
)
SCRIPTS_DIR = SCRIPT_PATH.parent


def _load_builder():
    if str(SCRIPTS_DIR) not in sys.path:
        sys.path.insert(0, str(SCRIPTS_DIR))
    spec = importlib.util.spec_from_file_location(
        "build_noncompositional_rent_population_panel",
        SCRIPT_PATH,
    )
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_add_space_demand_columns_derives_named_shares() -> None:
    builder = _load_builder()
    levels = pd.DataFrame(
        {
            "gross_rent_bedrooms_total": [100, 0],
            "gross_rent_1_bedroom_total": [40, 0],
            "gross_rent_2_bedrooms_total": [35, 0],
            "gross_rent_3plus_bedrooms_total": [25, 0],
        }
    )

    result = builder.add_space_demand_columns(levels)

    assert result.loc[0, "gross_rent_2plus_bedroom_share"] == pytest.approx(0.60)
    assert result.loc[0, "gross_rent_3plus_bedroom_share"] == pytest.approx(0.25)
    assert pd.isna(result.loc[1, "gross_rent_2plus_bedroom_share"])
    assert pd.isna(result.loc[1, "gross_rent_3plus_bedroom_share"])


def test_add_first_differences_keeps_supply_interaction_explicit() -> None:
    builder = _load_builder()
    levels = pd.DataFrame(
        {
            "msa_id": ["10000", "10000", "10000"],
            "year": [2019, 2020, 2022],
            "log_zori": [1.0, 1.2, 1.5],
            "log_unshelt_rate": [0.1, 0.2, 0.4],
            "log_pop": [10.0, 10.1, 10.4],
            "gross_rent_2plus_bedroom_share": [0.50, 0.55, 0.56],
            "gross_rent_3plus_bedroom_share": [0.20, 0.22, 0.21],
            "supply_constraint_bps": [2.0, 2.0, 2.0],
            "supply_constraint_bps_long": [3.0, 3.0, 3.0],
        }
    )

    result = builder.add_first_differences(levels)

    assert result.loc[1, "year_gap"] == 1
    assert result.loc[2, "year_gap"] == 2
    assert result.loc[1, "d_gross_rent_2plus_bedroom_share"] == pytest.approx(0.05)
    assert result.loc[1, "d_log_pop_x_supply_constraint_bps"] == pytest.approx(0.2)
    assert result.loc[1, "d_log_pop_x_supply_constraint_bps_long"] == pytest.approx(0.3)


def test_model_specs_cover_supply_and_space_families() -> None:
    builder = _load_builder()

    specs = list(builder._model_specs())

    assert {spec.family for spec in specs} == {"supply_constraint", "space_demand_proxy"}
    assert any("supply_constraint_bps" in spec.predictors for spec in specs)
    assert any("d_gross_rent_2plus_bedroom_share" in spec.predictors for spec in specs)
