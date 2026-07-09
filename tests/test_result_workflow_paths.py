"""Regression tests for repository-relative result workflow paths."""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent

WORKFLOW_PATH_CASES = [
    (
        "build_eviction_rate_timing_panel",
        "OUT",
        REPO_ROOT / "outputs" / "eviction_rate_timing",
    ),
    (
        "build_employment_labor_force_composition_panel",
        "OUT",
        REPO_ROOT / "outputs" / "composition_rent_population",
    ),
    (
        "build_household_size_composition_panel",
        "OUT",
        REPO_ROOT / "outputs" / "composition_rent_population",
    ),
    (
        "build_income_inequality_composition_panel",
        "OUT",
        REPO_ROOT / "outputs" / "composition_rent_population",
    ),
    (
        "build_irs_migration_pooled_panel",
        "OUT",
        REPO_ROOT / "outputs" / "irs_migration_pooled",
    ),
    (
        "build_local_income_composition_panel",
        "OUT",
        REPO_ROOT / "outputs" / "composition_rent_population",
    ),
    (
        "build_overdose_lag_panel",
        "OUT",
        REPO_ROOT / "outputs" / "overdose_lag",
    ),
    (
        "build_poverty_longitudinal_panel",
        "OUT",
        REPO_ROOT / "outputs" / "poverty_longitudinal",
    ),
    (
        "build_qcew_labor_market_panel",
        "OUT",
        REPO_ROOT / "outputs" / "qcew_labor_market",
    ),
    (
        "build_supply_iv_panel",
        "OUT",
        REPO_ROOT / "outputs" / "supply_iv",
    ),
    (
        "build_vera_hic_pit_longitudinal",
        "OUT",
        REPO_ROOT / "outputs" / "vera_hic_pit_longitudinal",
    ),
    (
        "build_vera_hic_pit_longitudinal_pooled",
        "OUT",
        REPO_ROOT / "outputs" / "vera_hic_pit_longitudinal_pooled",
    ),
    (
        "build_vera_hic_pit_panel",
        "OUT",
        REPO_ROOT / "outputs" / "vera_hic_pit",
    ),
    (
        "generate_top50_msa_coc_pit_contract_rent_2010_2020",
        "OUTPUT_DIR",
        REPO_ROOT / "outputs" / "top50_msa_nonpr_coc_pit_contract_rent_2010_2020",
    ),
    (
        "overdose_hic_category_correlations",
        "LEVELS_PANEL",
        REPO_ROOT / "outputs" / "overdose_lag" / "overdose_lag_levels.parquet",
    ),
    (
        "vera_hic_pit_correlations",
        "PANEL",
        REPO_ROOT / "outputs" / "vera_hic_pit" / "vera_hic_pit_levels.parquet",
    ),
]


@pytest.mark.parametrize(
    ("module_name", "path_attr", "expected_path"),
    [
        pytest.param(module_name, path_attr, expected_path, id=module_name)
        for module_name, path_attr, expected_path in WORKFLOW_PATH_CASES
    ],
)
def test_result_workflow_modules_resolve_repo_root(
    module_name: str,
    path_attr: str,
    expected_path: Path,
) -> None:
    module = importlib.import_module(f"hhplab.results.workflows.{module_name}")

    assert module.ROOT == REPO_ROOT
    assert getattr(module, path_attr) == expected_path
