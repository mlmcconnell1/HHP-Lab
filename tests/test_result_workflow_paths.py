"""Regression tests for storage-root-aware result workflow paths."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pandas as pd
import pytest

from hhplab.config import load_config
from hhplab.provenance import read_provenance

REPO_ROOT = Path(__file__).resolve().parent.parent

WORKFLOW_PATH_CASES = [
    (
        "build_eviction_rate_timing_panel",
        "OUT",
        "output",
        Path("eviction_rate_timing"),
    ),
    (
        "build_bps_valuation_benchmark",
        "OUT",
        "output",
        Path("bps_valuation_benchmark"),
    ),
    (
        "build_bps_valuation_rent_channel",
        "OUT",
        "output",
        Path("bps_valuation_rent_channel"),
    ),
    (
        "build_employment_labor_force_composition_panel",
        "OUT",
        "output",
        Path("composition_rent_population"),
    ),
    (
        "build_household_size_composition_panel",
        "OUT",
        "output",
        Path("composition_rent_population"),
    ),
    (
        "build_income_inequality_composition_panel",
        "OUT",
        "output",
        Path("composition_rent_population"),
    ),
    (
        "build_irs_migration_pooled_panel",
        "OUT",
        "output",
        Path("irs_migration_pooled"),
    ),
    (
        "build_local_income_composition_panel",
        "OUT",
        "output",
        Path("composition_rent_population"),
    ),
    (
        "build_overdose_lag_panel",
        "OUT",
        "output",
        Path("overdose_lag"),
    ),
    (
        "build_poverty_longitudinal_panel",
        "OUT",
        "output",
        Path("poverty_longitudinal"),
    ),
    (
        "build_qcew_labor_market_panel",
        "OUT",
        "output",
        Path("qcew_labor_market"),
    ),
    (
        "build_supply_iv_panel",
        "OUT",
        "output",
        Path("supply_iv"),
    ),
    (
        "build_vera_hic_pit_longitudinal",
        "OUT",
        "output",
        Path("vera_hic_pit_longitudinal"),
    ),
    (
        "build_vera_hic_pit_longitudinal_pooled",
        "OUT",
        "output",
        Path("vera_hic_pit_longitudinal_pooled"),
    ),
    (
        "build_vera_hic_pit_panel",
        "OUT",
        "output",
        Path("vera_hic_pit"),
    ),
    (
        "generate_top50_msa_coc_pit_contract_rent_2010_2020",
        "OUTPUT_DIR",
        "output",
        Path("top50_msa_nonpr_coc_pit_contract_rent_2010_2020"),
    ),
    (
        "overdose_hic_category_correlations",
        "LEVELS_PANEL",
        "output",
        Path("overdose_lag") / "overdose_lag_levels.parquet",
    ),
    (
        "vera_hic_pit_correlations",
        "PANEL",
        "output",
        Path("vera_hic_pit") / "vera_hic_pit_levels.parquet",
    ),
]

WORKFLOW_INPUT_PATH_CASES = [
    (
        "build_household_size_composition_panel",
        "ACS1_METRO_GLOB",
        "data",
        Path("curated") / "acs" / "acs1_metro__A*@Dcensusmsa2023.parquet",
    ),
    (
        "build_poverty_longitudinal_panel",
        "MEASURES_GLOB",
        "data",
        Path("curated") / "measures" / "measures__msa__A*.parquet",
    ),
    (
        "build_qcew_labor_market_panel",
        "CPI_U_PATH",
        "data",
        Path("curated") / "cpi" / "cpi_u__Aall.parquet",
    ),
    (
        "build_supply_iv_panel",
        "SAIZ",
        "data",
        Path("raw") / "saiz_elasticity" / "saiz2010_supply_elasticity.dta",
    ),
    (
        "generate_top50_msa_coc_pit_contract_rent_2010_2020",
        "PIT_PATH",
        "data",
        Path("curated") / "pit" / "pit_vintage__P2020.parquet",
    ),
]


def _clear_result_modules() -> None:
    for name in list(sys.modules):
        if name == "hhplab.results.findings" or name.startswith("hhplab.results.workflows"):
            sys.modules.pop(name)
    importlib.invalidate_caches()


@pytest.fixture(autouse=True)
def fresh_result_modules():
    _clear_result_modules()
    yield
    _clear_result_modules()


def _import_workflow(module_name: str):
    return importlib.import_module(f"hhplab.results.workflows.{module_name}")


def _expected_path(
    root_kind: str,
    relative_path: Path,
    *,
    data_root: Path,
    output_root: Path,
) -> Path:
    if root_kind == "data":
        return data_root / relative_path
    if root_kind == "output":
        return output_root / relative_path
    raise AssertionError(f"Unknown root kind: {root_kind}")


@pytest.mark.parametrize(
    ("module_name", "path_attr", "root_kind", "relative_path"),
    [
        pytest.param(module_name, path_attr, root_kind, relative_path, id=module_name)
        for module_name, path_attr, root_kind, relative_path in WORKFLOW_PATH_CASES
    ],
)
def test_result_workflow_modules_resolve_default_roots(
    module_name: str,
    path_attr: str,
    root_kind: str,
    relative_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(REPO_ROOT)
    module = _import_workflow(module_name)

    assert module.ROOT == REPO_ROOT
    cfg = load_config(project_root=REPO_ROOT)
    expected_path = _expected_path(
        root_kind,
        relative_path,
        data_root=cfg.asset_store_root,
        output_root=cfg.output_root,
    )
    assert getattr(module, path_attr) == expected_path


@pytest.mark.parametrize(
    ("module_name", "path_attr", "root_kind", "relative_path"),
    [
        pytest.param(module_name, path_attr, root_kind, relative_path, id=module_name)
        for module_name, path_attr, root_kind, relative_path in (
            WORKFLOW_PATH_CASES + WORKFLOW_INPUT_PATH_CASES
        )
    ],
)
def test_result_workflow_modules_honor_configured_storage_roots(
    module_name: str,
    path_attr: str,
    root_kind: str,
    relative_path: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = tmp_path / "asset-store"
    output_root = tmp_path / "outputs"
    monkeypatch.setenv("HHPLAB_ASSET_STORE_ROOT", str(data_root))
    monkeypatch.setenv("HHPLAB_OUTPUT_ROOT", str(output_root))

    module = _import_workflow(module_name)

    expected_path = _expected_path(
        root_kind,
        relative_path,
        data_root=data_root,
        output_root=output_root,
    )
    actual = getattr(module, path_attr)
    if isinstance(actual, str):
        assert actual == str(expected_path)
    else:
        assert actual == expected_path


def test_finding_sidecars_default_to_configured_output_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_root = tmp_path / "configured-outputs"
    monkeypatch.setenv("HHPLAB_OUTPUT_ROOT", str(output_root))

    findings = importlib.import_module("hhplab.results.findings")

    assert findings.FINDINGS_DIR == output_root / "result_findings"


def test_result_workflow_parquet_writer_embeds_representative_provenance(
    tmp_path: Path,
) -> None:
    paths = importlib.import_module("hhplab.results.workflows._paths")
    output_path = tmp_path / "example_result_workflow" / "example_levels.parquet"
    frame = pd.DataFrame({"msa_id": ["10000"], "year": [2024], "value": [1.5]})

    paths.write_result_parquet(frame, output_path, index=False)

    provenance = read_provenance(output_path)
    assert provenance is not None
    assert provenance.extra["dataset_type"] == "result_workflow_artifact"
    assert provenance.extra["workflow_id"] == "example_result_workflow"
    assert provenance.extra["artifact_name"] == "example_levels"
    assert provenance.extra["artifact_role"] == "levels"
    assert provenance.extra["row_count"] == 1
    assert provenance.extra["columns"] == ["msa_id", "year", "value"]
