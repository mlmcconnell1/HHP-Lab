"""Tests for package-controlled result replication workflows."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import numpy as np
import pandas as pd
from typer.testing import CliRunner

from hhplab.cli.build_cmds.results import RESULT_WORKFLOWS, _run_workflow_module
from hhplab.cli.main import app

runner = CliRunner()


def test_run_workflow_module_uses_structured_run_contract() -> None:
    module = SimpleNamespace(
        __name__="hhplab.results.workflows.example",
        run=lambda: {"row_count": 3, "models": [{"term": "x"}]},
        main=lambda: None,
    )

    with patch("hhplab.cli.build_cmds.results.importlib.import_module", return_value=module):
        payload = _run_workflow_module("example")

    assert payload["module"] == "hhplab.results.workflows.example"
    assert payload["stdout"] == []
    assert payload["result"] == {"row_count": 3, "models": [{"term": "x"}]}


def test_run_workflow_module_reads_legacy_summary_stdout(tmp_path) -> None:
    summary_path = tmp_path / "summary.json"
    summary_path.write_text(json.dumps({"row_count": 2}) + "\n", encoding="utf-8")

    def main() -> None:
        print(f"summary -> {summary_path}")

    module = SimpleNamespace(__name__="hhplab.results.workflows.legacy", main=main)

    with patch("hhplab.cli.build_cmds.results.importlib.import_module", return_value=module):
        payload = _run_workflow_module("legacy")

    assert payload["stdout"] == [f"summary -> {summary_path}"]
    assert payload["result"] == {"row_count": 2}


def test_run_workflow_module_coerces_numpy_scalars_so_json_dumps_succeeds() -> None:
    # Regression test: vera_hic_pit_correlations.py and
    # overdose_hic_category_correlations.py returned `year` from
    # `Series.unique()` (numpy.int64) directly in their result dicts, which
    # crashed `hhplab build result <name> --json` with an uncaught
    # `TypeError: Object of type int64 is not JSON serializable` at the final
    # json.dumps in build_result_cmd -- not caught by the per-module
    # try/except, so it surfaced as a raw traceback instead of a clean error.
    module = SimpleNamespace(
        __name__="hhplab.results.workflows.example",
        run=lambda: {
            "year": np.int64(2020),
            "r": np.float64(0.5),
            "flag": np.bool_(True),
            "years": np.array([2019, 2020]),
            "rows": [{"year": np.int64(2021), "n": 5}],
        },
        main=lambda: None,
    )

    with patch("hhplab.cli.build_cmds.results.importlib.import_module", return_value=module):
        payload = _run_workflow_module("example")

    result = payload["result"]
    assert result == {
        "year": 2020,
        "r": 0.5,
        "flag": True,
        "years": [2019, 2020],
        "rows": [{"year": 2021, "n": 5}],
    }
    assert not isinstance(result["year"], np.generic)
    # Must not raise -- this is what actually broke before the fix.
    json.dumps(payload)


def test_build_result_cmd_prints_result_when_run_only_contract_has_no_stdout() -> None:
    # Regression test: modules migrated to run()-only (main() delegates to
    # run() and prints from its return value, but the CLI calls run()
    # directly and never calls main()) produce empty stdout, so plain-text
    # `hhplab build result <name>` (no --json) silently showed nothing but
    # the script name.
    def fake_run_workflow_module(module_name: str) -> dict[str, object]:
        return {
            "script": f"scripts/{module_name}.py",
            "module": f"hhplab.results.workflows.{module_name}",
            "stdout": [],
            "result": {"row_count": 42},
        }

    with patch(
        "hhplab.cli.build_cmds.results._run_workflow_module",
        side_effect=fake_run_workflow_module,
    ):
        result = runner.invoke(app, ["build", "result", "subsidized-housing-stock"])

    assert result.exit_code == 0, result.output
    assert '"row_count": 42' in result.output


def test_build_result_composition_workflow_json() -> None:
    calls: list[str] = []

    def fake_run_workflow_module(module_name: str) -> dict[str, object]:
        calls.append(module_name)
        return {
            "script": f"scripts/{module_name}.py",
            "module": f"hhplab.results.workflows.{module_name}",
            "stdout": [f"ran {module_name}"],
        }

    with patch(
        "hhplab.cli.build_cmds.results._run_workflow_module",
        side_effect=fake_run_workflow_module,
    ):
        result = runner.invoke(
            app,
            ["build", "result", "composition-rent-population", "--json"],
        )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["status"] == "ok"
    assert payload["workflow"] == "composition-rent-population"
    assert calls == [
        "build_renter_household_share_composition_panel",
        "build_household_size_composition_panel",
        "build_recent_mover_income_composition_panel",
        "build_local_income_composition_panel",
        "build_employment_labor_force_composition_panel",
        "build_income_inequality_composition_panel",
        "build_housing_cost_burden_composition_panel",
        "analyze_composition_rent_population_robustness",
    ]
    assert [step["script"] for step in payload["steps"]] == [
        f"scripts/{module_name}.py" for module_name in calls
    ]
    assert [step["module"] for step in payload["steps"]] == [
        f"hhplab.results.workflows.{module_name}" for module_name in calls
    ]


def test_build_result_subsidized_housing_stock_workflow_json() -> None:
    calls: list[str] = []

    def fake_run_workflow_module(module_name: str) -> dict[str, object]:
        calls.append(module_name)
        return {
            "script": f"scripts/{module_name}.py",
            "module": f"hhplab.results.workflows.{module_name}",
            "stdout": [f"ran {module_name}"],
        }

    with patch(
        "hhplab.cli.build_cmds.results._run_workflow_module",
        side_effect=fake_run_workflow_module,
    ):
        result = runner.invoke(
            app,
            ["build", "result", "subsidized-housing-stock", "--json"],
        )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["status"] == "ok"
    assert payload["workflow"] == "subsidized-housing-stock"
    assert calls == ["build_subsidized_housing_stock_panel"]
    assert payload["steps"][0]["script"] == "scripts/build_subsidized_housing_stock_panel.py"
    assert payload["steps"][0]["module"] == (
        "hhplab.results.workflows.build_subsidized_housing_stock_panel"
    )


def test_build_result_housing_cost_burden_workflow_json() -> None:
    calls: list[str] = []

    def fake_run_workflow_module(module_name: str) -> dict[str, object]:
        calls.append(module_name)
        return {
            "script": f"scripts/{module_name}.py",
            "module": f"hhplab.results.workflows.{module_name}",
            "stdout": [f"ran {module_name}"],
        }

    with patch(
        "hhplab.cli.build_cmds.results._run_workflow_module",
        side_effect=fake_run_workflow_module,
    ):
        result = runner.invoke(
            app,
            ["build", "result", "housing-cost-burden-composition", "--json"],
        )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["status"] == "ok"
    assert payload["workflow"] == "housing-cost-burden-composition"
    assert calls == ["build_housing_cost_burden_composition_panel"]
    assert payload["steps"][0]["script"] == "scripts/build_housing_cost_burden_composition_panel.py"
    assert payload["steps"][0]["module"] == (
        "hhplab.results.workflows.build_housing_cost_burden_composition_panel"
    )


def test_build_result_irs_migration_pooled_workflow_json() -> None:
    calls: list[str] = []

    def fake_run_workflow_module(module_name: str) -> dict[str, object]:
        calls.append(module_name)
        return {
            "script": f"scripts/{module_name}.py",
            "module": f"hhplab.results.workflows.{module_name}",
            "stdout": [f"ran {module_name}"],
        }

    with patch(
        "hhplab.cli.build_cmds.results._run_workflow_module",
        side_effect=fake_run_workflow_module,
    ):
        result = runner.invoke(
            app,
            ["build", "result", "irs-migration-pooled", "--json"],
        )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["status"] == "ok"
    assert payload["workflow"] == "irs-migration-pooled"
    assert calls == ["build_irs_migration_pooled_panel"]
    assert payload["steps"][0]["script"] == "scripts/build_irs_migration_pooled_panel.py"
    assert payload["steps"][0]["module"] == (
        "hhplab.results.workflows.build_irs_migration_pooled_panel"
    )


def test_build_result_eviction_rate_timing_workflow_json() -> None:
    calls: list[str] = []

    def fake_run_workflow_module(module_name: str) -> dict[str, object]:
        calls.append(module_name)
        return {
            "script": f"scripts/{module_name}.py",
            "module": f"hhplab.results.workflows.{module_name}",
            "stdout": [f"ran {module_name}"],
        }

    with patch(
        "hhplab.cli.build_cmds.results._run_workflow_module",
        side_effect=fake_run_workflow_module,
    ):
        result = runner.invoke(
            app,
            ["build", "result", "eviction-rate-timing", "--json"],
        )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["status"] == "ok"
    assert payload["workflow"] == "eviction-rate-timing"
    assert calls == ["build_eviction_rate_timing_panel"]
    assert payload["steps"][0]["script"] == "scripts/build_eviction_rate_timing_panel.py"
    assert payload["steps"][0]["module"] == (
        "hhplab.results.workflows.build_eviction_rate_timing_panel"
    )


def test_build_result_bps_valuation_benchmark_workflow_json() -> None:
    calls: list[str] = []

    def fake_run_workflow_module(module_name: str) -> dict[str, object]:
        calls.append(module_name)
        return {
            "script": f"scripts/{module_name}.py",
            "module": f"hhplab.results.workflows.{module_name}",
            "stdout": [f"ran {module_name}"],
        }

    with patch(
        "hhplab.cli.build_cmds.results._run_workflow_module",
        side_effect=fake_run_workflow_module,
    ):
        result = runner.invoke(
            app,
            ["build", "result", "bps-valuation-benchmark", "--json"],
        )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["status"] == "ok"
    assert payload["workflow"] == "bps-valuation-benchmark"
    assert calls == ["build_bps_valuation_benchmark"]
    assert payload["steps"][0]["script"] == "scripts/build_bps_valuation_benchmark.py"
    assert payload["steps"][0]["module"] == (
        "hhplab.results.workflows.build_bps_valuation_benchmark"
    )


def test_build_result_bps_valuation_rent_channel_workflow_json() -> None:
    calls: list[str] = []

    def fake_run_workflow_module(module_name: str) -> dict[str, object]:
        calls.append(module_name)
        return {
            "script": f"scripts/{module_name}.py",
            "module": f"hhplab.results.workflows.{module_name}",
            "stdout": [f"ran {module_name}"],
        }

    with patch(
        "hhplab.cli.build_cmds.results._run_workflow_module",
        side_effect=fake_run_workflow_module,
    ):
        result = runner.invoke(
            app,
            ["build", "result", "bps-valuation-rent-channel", "--json"],
        )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["status"] == "ok"
    assert payload["workflow"] == "bps-valuation-rent-channel"
    assert calls == ["build_bps_valuation_rent_channel"]
    assert payload["steps"][0]["script"] == "scripts/build_bps_valuation_rent_channel.py"
    assert payload["steps"][0]["module"] == (
        "hhplab.results.workflows.build_bps_valuation_rent_channel"
    )


def test_build_result_qcew_labor_market_workflow_json() -> None:
    calls: list[str] = []

    def fake_run_workflow_module(module_name: str) -> dict[str, object]:
        calls.append(module_name)
        return {
            "script": f"scripts/{module_name}.py",
            "module": f"hhplab.results.workflows.{module_name}",
            "stdout": [f"ran {module_name}"],
        }

    with patch(
        "hhplab.cli.build_cmds.results._run_workflow_module",
        side_effect=fake_run_workflow_module,
    ):
        result = runner.invoke(
            app,
            ["build", "result", "qcew-labor-market", "--json"],
        )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["status"] == "ok"
    assert payload["workflow"] == "qcew-labor-market"
    assert calls == ["build_qcew_labor_market_panel"]
    assert payload["steps"][0]["script"] == "scripts/build_qcew_labor_market_panel.py"
    assert payload["steps"][0]["module"] == (
        "hhplab.results.workflows.build_qcew_labor_market_panel"
    )


def test_all_documented_results_includes_eviction_timing_before_qcew() -> None:
    modules = RESULT_WORKFLOWS["all-documented-results"].modules

    assert "build_eviction_rate_timing_panel" in modules
    assert modules.index("build_eviction_rate_timing_panel") < modules.index(
        "build_qcew_labor_market_panel"
    )


def test_registered_result_workflows_have_structured_result_contract() -> None:
    missing_contract: list[str] = []

    for workflow in RESULT_WORKFLOWS.values():
        for module_name in workflow.modules:
            module = __import__(
                f"hhplab.results.workflows.{module_name}",
                fromlist=["run", "__file__"],
            )
            if callable(getattr(module, "run", None)):
                continue
            source = Path(module.__file__).read_text(encoding="utf-8")
            if "summary ->" not in source:
                missing_contract.append(module_name)

    assert missing_contract == []


def test_build_result_workflow_failure_json_is_actionable() -> None:
    with patch(
        "hhplab.cli.build_cmds.results._run_workflow_module",
        side_effect=FileNotFoundError("missing curated source"),
    ):
        result = runner.invoke(
            app,
            ["build", "result", "subsidized-housing-stock", "--json"],
        )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["status"] == "error"
    assert payload["workflow"] == "subsidized-housing-stock"
    assert payload["failed_module"] == "build_subsidized_housing_stock_panel"
    assert payload["failed_script"] == "scripts/build_subsidized_housing_stock_panel.py"
    assert payload["error"] == "missing curated source"
    assert payload["completed_steps"] == []


def test_build_result_unknown_workflow_json_is_actionable() -> None:
    result = runner.invoke(app, ["build", "result", "missing-workflow", "--json"])

    assert result.exit_code == 2
    payload = json.loads(result.output)
    assert payload["status"] == "error"
    assert "Unknown result workflow" in payload["error"]
    assert "all-documented-results" in payload["available_workflows"]
    assert "composition-rent-population" in payload["available_workflows"]


def test_build_result_supply_iv_ignores_outer_cli_args() -> None:
    with (
        patch(
            "hhplab.results.workflows.build_supply_iv_panel.build_supply_iv_panel",
            return_value=(pd.DataFrame([{"row": 1}]), pd.DataFrame([{"row": 2}]), {}),
        ) as build_panel,
        patch(
            "hhplab.results.workflows.build_supply_iv_panel.build_top150_outputs"
        ) as build_top150,
    ):
        result = runner.invoke(app, ["build", "result", "supply-iv", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["status"] == "ok"
    assert payload["workflow"] == "supply-iv"
    assert payload["steps"][0]["module"] == "hhplab.results.workflows.build_supply_iv_panel"
    build_panel.assert_called_once()
    build_top150.assert_called_once_with()
