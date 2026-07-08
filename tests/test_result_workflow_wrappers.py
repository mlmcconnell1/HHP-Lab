"""Tests for legacy script wrappers around package result workflow modules."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from unittest.mock import patch

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = REPO_ROOT / "scripts"

WRAPPER_CASES = [
    "analyze_composition_rent_population_robustness",
    "analyze_noncompositional_rent_population_robustness",
    "build_household_size_composition_panel",
    "build_irs_migration_pooled_panel",
    "build_noncompositional_rent_population_panel",
    "build_overdose_lag_panel",
    "build_poverty_longitudinal_panel",
    "build_recent_mover_income_composition_panel",
    "build_renter_household_share_composition_panel",
    "build_vera_hic_pit_longitudinal",
    "build_vera_hic_pit_longitudinal_pooled",
    "build_vera_hic_pit_panel",
    "generate_top50_msa_coc_pit_contract_rent_2010_2020",
    "overdose_hic_category_correlations",
    "vera_hic_pit_correlations",
]


def _load_wrapper(name: str):
    path = SCRIPTS_DIR / f"{name}.py"
    spec = importlib.util.spec_from_file_location(f"legacy_wrapper_{name}", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("module_name", WRAPPER_CASES)
def test_legacy_wrapper_delegates_to_package_main(module_name: str) -> None:
    wrapper = _load_wrapper(module_name)

    with patch.object(wrapper, "_workflow_main") as workflow_main:
        wrapper.main()

    workflow_main.assert_called_once_with()


def test_supply_iv_wrapper_passes_explicit_argv() -> None:
    wrapper = _load_wrapper("build_supply_iv_panel")

    with patch.object(wrapper, "_workflow_main") as workflow_main:
        wrapper.main(["--msa-count", "50"])

    workflow_main.assert_called_once_with(["--msa-count", "50"])


def test_supply_iv_wrapper_passes_process_argv(monkeypatch: pytest.MonkeyPatch) -> None:
    wrapper = _load_wrapper("build_supply_iv_panel")
    monkeypatch.setattr("sys.argv", ["scripts/build_supply_iv_panel.py", "--msa-count", "150"])

    with patch.object(wrapper, "_workflow_main") as workflow_main:
        wrapper.main()

    workflow_main.assert_called_once_with(["--msa-count", "150"])
