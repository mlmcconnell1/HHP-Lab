"""Tests for package-controlled result replication workflows."""

from __future__ import annotations

import json
from unittest.mock import patch

from typer.testing import CliRunner

from hhplab.cli.main import app

runner = CliRunner()


def test_build_result_composition_workflow_json() -> None:
    calls: list[str] = []

    def fake_run_script(script_name: str) -> dict[str, object]:
        calls.append(script_name)
        return {"script": f"scripts/{script_name}", "stdout": [f"ran {script_name}"]}

    with patch("hhplab.cli.build_cmds.results._run_script", side_effect=fake_run_script):
        result = runner.invoke(
            app,
            ["build", "result", "composition-rent-population", "--json"],
        )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["status"] == "ok"
    assert payload["workflow"] == "composition-rent-population"
    assert calls == [
        "build_renter_household_share_composition_panel.py",
        "build_household_size_composition_panel.py",
        "build_recent_mover_income_composition_panel.py",
        "analyze_composition_rent_population_robustness.py",
    ]
    assert [step["script"] for step in payload["steps"]] == [
        f"scripts/{script_name}" for script_name in calls
    ]


def test_build_result_unknown_workflow_json_is_actionable() -> None:
    result = runner.invoke(app, ["build", "result", "missing-workflow", "--json"])

    assert result.exit_code == 2
    payload = json.loads(result.output)
    assert payload["status"] == "error"
    assert "Unknown result workflow" in payload["error"]
    assert "all-documented-results" in payload["available_workflows"]
    assert "composition-rent-population" in payload["available_workflows"]
