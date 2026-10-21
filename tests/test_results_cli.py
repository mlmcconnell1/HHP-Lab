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


def test_build_plan_and_preflight_resolve_recipe_output_root(recipe_path, tmp_path):
    data = deepcopy(RECIPE)
    data["output_root"] = "recipe-output"
    recipe_path.write_text(yaml.safe_dump(data))
    for command in (("build", "recipe-plan"), ("build", "recipe-preflight")):
        response = RUNNER.invoke(
            app,
            [*command, "--recipe", str(recipe_path), "--json"],
        )
        assert response.exit_code == 0, response.output
        payload = json.loads(response.output)
        assert payload["output_root"] == str(recipe_path.parent / "recipe-output")
        assert payload["output_destinations"]["results_bundle"].startswith(
            str(recipe_path.parent / "recipe-output")
        )


def test_build_plan_explicit_cli_root_beats_recipe_root(recipe_path, tmp_path):
    data = deepcopy(RECIPE)
    data["output_root"] = "recipe-output"
    recipe_path.write_text(yaml.safe_dump(data))
    cli_root = tmp_path / "cli-output"
    response = RUNNER.invoke(
        app,
        [
            "build",
            "recipe-plan",
            "--recipe",
            str(recipe_path),
            "--output-root",
            str(cli_root),
            "--json",
        ],
    )
    assert response.exit_code == 0, response.output
    assert json.loads(response.output)["output_root"] == str(cli_root)


def test_recipe_plan_discovers_project_output_root_from_subdirectory(
    recipe_path, tmp_path, monkeypatch
):
    project_root = tmp_path / "project"
    subdirectory = project_root / "nested"
    subdirectory.mkdir(parents=True)
    (project_root / "hhplab.yaml").write_text("output_root: configured-output\n")
    monkeypatch.chdir(subdirectory)

    response = RUNNER.invoke(
        app,
        ["build", "recipe-plan", "--recipe", str(recipe_path), "--json"],
    )

    assert response.exit_code == 0, response.output
    assert "may not be the HHP-Lab project root" in response.stderr
    assert json.loads(response.stdout)["output_root"] == str(project_root / "configured-output")


def test_v1_plan_routes_panel_and_manifest_to_recipe_output_root(tmp_path):
    recipe_path = tmp_path / "recipes" / "panel.yaml"
    recipe_path.parent.mkdir()
    recipe_path.write_text(
        yaml.safe_dump(
            {
                "version": 1,
                "name": "rooted-panel",
                "output_root": "published",
                "universe": {"years": [2020]},
                "targets": [
                    {
                        "id": "panel",
                        "geometry": {"type": "coc", "vintage": 2025},
                        "outputs": ["panel"],
                    }
                ],
                "datasets": {},
                "pipelines": [{"id": "build", "target": "panel", "steps": []}],
            }
        )
    )

    response = RUNNER.invoke(
        app,
        ["build", "recipe-plan", "--recipe", str(recipe_path), "--json"],
    )

    assert response.exit_code == 0, response.output
    payload = json.loads(response.output)
    destinations = payload["output_destinations"]["build"]
    expected_root = recipe_path.parent / "published"
    assert payload["output_root"] == str(expected_root)
    assert destinations["panel_path"].startswith(str(expected_root))
    assert destinations["manifest_path"].startswith(str(expected_root))


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


def test_results_lock_uses_recipe_output_root_when_cli_root_is_absent(recipe_path, tmp_path):
    data = deepcopy(RECIPE)
    data["output_root"] = "recipe-output"
    recipe_path.write_text(yaml.safe_dump(data))
    response = RUNNER.invoke(
        app,
        ["results", "lock", "--recipe", str(recipe_path), "--json"],
    )
    assert response.exit_code == 0, response.output
    assert (recipe_path.parent / "recipe-output" / "ledger" / "runs.jsonl").exists()


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


def test_results_run_uses_recipe_root_and_preserves_cli_override(
    monkeypatch, recipe_path, tmp_path
):
    data = deepcopy(RECIPE)
    data["output_root"] = "recipe-output"
    recipe_path.write_text(yaml.safe_dump(data))
    resolved_roots = []

    def fake_run_results(*args, **kwargs):
        resolved_roots.append(kwargs["outputs_root"])
        return {
            "statuses": {"execution_status": "complete"},
            "lock_status": {"status": "modified", "components": []},
            "run_id": "test-run",
            "bundle_path": "bundle",
        }

    monkeypatch.setattr("hhplab.results.runner.run_results", fake_run_results)
    recipe_root_result = RUNNER.invoke(
        app,
        ["results", "run", "--recipe", str(recipe_path), "--json"],
    )
    cli_root = tmp_path / "cli-output"
    cli_root_result = RUNNER.invoke(
        app,
        [
            "results",
            "run",
            "--recipe",
            str(recipe_path),
            "--outputs-root",
            str(cli_root),
            "--json",
        ],
    )

    assert recipe_root_result.exit_code == 0, recipe_root_result.output
    assert cli_root_result.exit_code == 0, cli_root_result.output
    assert resolved_roots == [recipe_path.parent / "recipe-output", cli_root]


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
