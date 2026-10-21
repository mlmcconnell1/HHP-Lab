"""Machine-readable results CLI and RecipeV2 build-command routing."""

import json
from copy import deepcopy

import pytest
import yaml
from typer.testing import CliRunner

from hhplab.cli.main import app
from hhplab.results.verify import VerificationReport
from hhplab.storage.run_ledger import RunLedger

RUNNER = CliRunner()
RECIPE = {
    "version": 2,
    "id": "cli-test",
    "title": "CLI test",
    "authors": ["test"],
    "universe": {"range": "2020-2021"},
    "targets": [{"id": "target", "geometry": {"type": "coc", "vintage": 2025}}],
    "datasets": {},
    "pipelines": [{"id": "build", "target": "target", "steps": []}],
    "panels": {
        "panel": {
            "ref": {"pipeline": "build", "output": "panel"},
            "geo_key": "geo",
            "time_key": "year",
        }
    },
    "analyses": [
        {
            "id": "reg",
            "method": "regression",
            "input": "panel",
            "sample": {},
            "outcome": "y",
            "predictors": ["x"],
        }
    ],
    "results": {
        "bundle_dir": "results/{id}/{run_id}",
        "include_panels": ["panel"],
        "report": {"title": "CLI test"},
    },
}


@pytest.fixture
def recipe_path(tmp_path):
    path = tmp_path / "recipe.yaml"
    path.write_text(yaml.safe_dump(RECIPE))
    return path


@pytest.mark.parametrize(
    "command",
    [
        ["results", "check"],
        ["build", "recipe-plan"],
        ["build", "recipe-preflight"],
        ["build", "recipe", "--dry-run"],
    ],
)
def test_static_commands_json(recipe_path, command):
    response = RUNNER.invoke(app, [*command, "--recipe", str(recipe_path), "--json"])
    assert response.exit_code == 0, response.output
    assert json.loads(response.output)["status"] in {"ok", "ready"}


def test_lock_export_and_history(recipe_path, tmp_path):
    export = tmp_path / "lock.json"
    response = RUNNER.invoke(
        app,
        [
            "results",
            "lock",
            "--recipe",
            str(recipe_path),
            "--outputs-root",
            str(tmp_path),
            "--export",
            str(export),
            "--json",
        ],
    )
    assert response.exit_code == 0, response.output
    assert json.loads(response.output)["lock"]["event"] == "lock_created"
    assert export.exists()
    history = RUNNER.invoke(
        app,
        [
            "results",
            "history",
            "--recipe-id",
            "cli-test",
            "--outputs-root",
            str(tmp_path),
            "--json",
        ],
    )
    assert history.exit_code == 0
    assert json.loads(history.output)["runs"] == []
    duplicate = RUNNER.invoke(
        app,
        [
            "results",
            "lock",
            "--recipe",
            str(recipe_path),
            "--outputs-root",
            str(tmp_path),
            "--export",
            str(export),
            "--json",
        ],
    )
    assert duplicate.exit_code == 2


def test_unresolved_lock_refuses_before_ledger(tmp_path):
    data = deepcopy(RECIPE)
    data["parameters"] = {"delta": {"unresolved": "Researcher must choose delta"}}
    path = tmp_path / "unresolved.yaml"
    path.write_text(yaml.safe_dump(data))
    result = RUNNER.invoke(
        app, ["results", "lock", "--recipe", str(path), "--outputs-root", str(tmp_path), "--json"]
    )
    assert result.exit_code == 2
    assert "unresolved" in json.loads(result.output)["error"]
    assert not (tmp_path / "ledger" / "runs.jsonl").exists()


@pytest.mark.parametrize(
    "integrity,reproducibility,reasons,exit_code",
    [
        ("pass", "pass", (), 0),
        ("fail", "not_run", (), 1),
        ("pass", "fail", (), 2),
        ("pass", "partial", ("environment_differs",), 3),
    ],
)
def test_verify_exit_codes(monkeypatch, tmp_path, integrity, reproducibility, reasons, exit_code):
    monkeypatch.setattr(
        "hhplab.results.verify.verify_bundle",
        lambda _: VerificationReport(
            integrity,
            reproducibility,
            partial_reasons=reasons,
        ),
    )
    result = RUNNER.invoke(app, ["results", "verify", str(tmp_path), "--json"])
    assert result.exit_code == exit_code
    assert json.loads(result.output)["exit_code"] == exit_code


def test_unsupported_capability_json(tmp_path):
    data = deepcopy(RECIPE)
    data["analyses"][0]["estimator"] = "2sls"
    path = tmp_path / "unsupported.yaml"
    path.write_text(yaml.safe_dump(data))
    result = RUNNER.invoke(app, ["results", "check", "--recipe", str(path), "--json"])
    assert result.exit_code == 2
    assert "capability_unavailable" in json.loads(result.output)["error"]


@pytest.mark.parametrize("execution,exit_code", [("complete", 0), ("partial", 1), ("failed", 1)])
def test_run_status_json(monkeypatch, recipe_path, tmp_path, execution, exit_code):
    monkeypatch.setattr(
        "hhplab.results.runner.run_results",
        lambda *args, **kwargs: {
            "statuses": {"execution_status": execution},
            "lock_status": {"status": "modified", "components": ["samples"]},
            "run_id": "test-run",
            "bundle_path": "bundle",
        },
    )
    result = RUNNER.invoke(
        app,
        [
            "results",
            "run",
            "--recipe",
            str(recipe_path),
            "--outputs-root",
            str(tmp_path),
            "--json",
            "--non-interactive",
        ],
    )
    assert result.exit_code == exit_code, result.output
    assert json.loads(result.output)["lock_status"]["components"] == ["samples"]


def test_history_filters_active_runs(tmp_path):
    lease = RunLedger(tmp_path).start(
        recipe_id="cli-test",
        recipe_sha256="a" * 64,
        spec_hash="b" * 64,
        components={"samples": "c" * 64},
        environment={},
    )
    try:
        result = RUNNER.invoke(
            app,
            [
                "results",
                "history",
                "--recipe-id",
                "cli-test",
                "--outputs-root",
                str(tmp_path),
                "--json",
            ],
        )
        assert result.exit_code == 0, result.output
        assert json.loads(result.output)["runs"][0]["status"] == "running"
    finally:
        lease.fail(RuntimeError("test completed"))
