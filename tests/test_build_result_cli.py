"""Tests for package-controlled result replication workflows."""

from __future__ import annotations

import json
from unittest.mock import patch

import pandas as pd
from typer.testing import CliRunner

from hhplab.cli.main import app

runner = CliRunner()


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
