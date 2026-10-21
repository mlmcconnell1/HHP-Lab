"""Lifecycle and pure replay behavior for RecipeV2 results."""

from __future__ import annotations

import hashlib

import pandas as pd
import pytest

from hhplab.recipe.loader import load_recipe
from hhplab.recipe.results_plan import plan_results
from hhplab.results.runner import (
    ResultsRunError,
    check_results,
    replay_results,
    run_results,
)
from hhplab.storage.run_ledger import RunLedger

PANEL = pd.DataFrame(
    {
        "geo_id": ["a", "a", "a", "b", "b", "b"],
        "year": [2020, 2021, 2022, 2020, 2021, 2022],
        "x": [1.0, 2.0, 3.0, 2.0, 4.0, 7.0],
        "y": [3.0, 5.0, 8.0, 4.0, 9.0, 15.0],
    }
)


def _recipe(*, unresolved: bool = False, diagnostics=None, grouped: bool = False):
    parameters = (
        {"delta": {"unresolved": "Researcher-supplied threshold"}} if unresolved else {}
    )
    return load_recipe(
        {
            "version": 2,
            "id": "runner-test",
            "title": "Runner test",
            "authors": ["test"],
            "universe": {"range": "2020-2022"},
            "targets": [{"id": "target", "geometry": {"type": "coc", "vintage": 2025}}],
            "datasets": {},
            "pipelines": [{"id": "build", "target": "target", "steps": []}],
            "panels": {
                "panel": {
                    "ref": {"pipeline": "build", "output": "panel"},
                    "geo_key": "geo_id",
                    "time_key": "year",
                }
            },
            "parameters": parameters,
            "sample_groups": (
                {"shared": {"sample": {"years": {"from": 2020, "to": 2022}}}}
                if grouped
                else {}
            ),
            "analyses": [
                {
                    "id": "fit",
                    "method": "regression",
                    "input": "panels.panel",
                    "sample": {"group": "shared"} if grouped else {},
                    "outcome": "y",
                    "predictors": ["x"],
                    "inference": {"method": "hc1"},
                }
            ]
            + (
                [
                    {
                        "id": "fit2",
                        "method": "regression",
                        "input": "panels.panel",
                        "sample": {"group": "shared"},
                        "outcome": "x",
                        "predictors": ["y"],
                        "inference": {"method": "hc1"},
                    }
                ]
                if grouped
                else []
            ),
            "variants": [
                {
                    "id": "classical",
                    "applies_to": ["fit", "fit2"] if grouped else ["fit"],
                    "override": {"inference": {"method": "classical"}},
                }
            ],
            "diagnostics": diagnostics or [],
            "findings": [],
            "results": {
                "bundle_dir": "results/{id}/{run_id}",
                "include_panels": ["panel"],
                "report": {"title": "Runner test"},
            },
        }
    )


def _replay_inputs(recipe):
    plan = plan_results(recipe, original_recipe_bytes=b"test recipe bytes")
    return (
        {
            "recipe": recipe.model_dump(mode="json", by_alias=True, exclude_none=True),
            "plan": plan.to_dict(),
        },
        {"spec_hash": plan.plan_sha256},
    )


def test_replay_reconstructs_analysis_and_reuses_base_membership_for_inference_variant():
    resolved, execution = _replay_inputs(_recipe())

    replay = replay_results(resolved, execution, {"panel": PANEL})

    assert replay["analyses"]["fit@base"]["status"] == "complete"
    assert replay["analyses"]["fit@classical"]["status"] == "complete"
    pd.testing.assert_frame_equal(
        replay["samples"]["fit@base"], replay["samples"]["fit@classical"]
    )
    assert replay["tables"]["regression_estimates"]
    assert replay["diagnostics"]["diagnostics"] == []
    assert replay["findings"]["findings"] == []


def test_group_inference_variant_reuses_context_membership_for_every_member():
    resolved, execution = _replay_inputs(_recipe(grouped=True))

    replay = replay_results(resolved, execution, {"panel": PANEL})

    assert replay["analyses"]["fit@classical"]["status"] == "complete"
    assert replay["analyses"]["fit2@classical"]["status"] == "complete"
    pd.testing.assert_frame_equal(
        replay["samples"]["fit@base"], replay["samples"]["fit@classical"]
    )
    pd.testing.assert_frame_equal(
        replay["samples"]["fit2@base"], replay["samples"]["fit2@classical"]
    )


def test_replay_rejects_changed_spec_hash_and_plan():
    resolved, execution = _replay_inputs(_recipe())
    with pytest.raises(ValueError, match="spec_hash"):
        replay_results(resolved, {"spec_hash": "wrong"}, {"panel": PANEL})

    resolved["plan"] = {**resolved["plan"], "recipe_id": "changed"}
    with pytest.raises(ValueError, match="canonical resolved plan"):
        replay_results(resolved, execution, {"panel": PANEL})


def test_replay_rejects_recipe_changed_behind_an_unchanged_plan():
    resolved, execution = _replay_inputs(_recipe())
    resolved["recipe"]["analyses"][0]["outcome"] = "z"

    with pytest.raises(ValueError, match="analysis plan does not match RecipeV2"):
        replay_results(resolved, execution, {"panel": PANEL})


def test_readiness_check_does_not_start_ledger_and_reports_frame_failures():
    recipe = _recipe()
    result = check_results(
        recipe,
        original_recipe_bytes=b"test recipe bytes",
        readiness=True,
        panel_frames={"panel": PANEL.drop(columns=["x"])},
    )

    assert result["status"] == "failed"
    assert any(issue.get("result_key") == "fit@base" for issue in result["issues"])


def test_unresolved_values_are_refused_before_ledger_start(tmp_path):
    recipe = _recipe(unresolved=True)
    checked = check_results(recipe, original_recipe_bytes=b"submitted bytes")
    assert checked["status"] == "failed"
    assert "unresolved_value at recipe.parameters.delta" in checked["issues"][0]["message"]
    assert checked["issues"][0]["remediation"]["hint"]

    with pytest.raises(ResultsRunError, match="before ledger start"):
        run_results(
            recipe,
            original_recipe_bytes=b"submitted bytes",
            outputs_root=tmp_path,
            panel_frames={"panel": PANEL},
        )

    assert RunLedger(tmp_path).read()["events"] == []


def test_diagnostic_failure_does_not_erase_estimates_or_findings(monkeypatch):
    from hhplab.results import runner

    def fail(*args, **kwargs):
        raise RuntimeError("injected diagnostic failure")

    monkeypatch.setattr(runner, "_evaluate_diagnostics", fail)
    resolved, execution = _replay_inputs(_recipe())
    replay = replay_results(resolved, execution, {"panel": PANEL})

    assert replay["analyses"]["fit@base"]["status"] == "complete"
    assert replay["tables"]["regression_estimates"]
    assert replay["findings"]["findings"] == []
    assert "injected diagnostic failure" in replay["diagnostics_error"]


def test_missing_panel_diagnostic_is_recorded_as_not_evaluated():
    recipe = _recipe(
        diagnostics=[
            {
                "id": "identity",
                "op": "column_identity",
                "panel": "panel",
                "lhs": "y",
                "rhs": ["x"],
                "rows": "unmasked",
            }
        ]
    )
    resolved, execution = _replay_inputs(recipe)

    replay = replay_results(resolved, execution, {})

    assert replay["diagnostics"]["diagnostics"][0]["status"] == "not_evaluated"
    assert replay["analyses"]["fit@base"]["error"]["code"] == "missing_panel"


def test_analysis_failure_does_not_suppress_independent_analysis():
    recipe = _recipe()
    data = recipe.model_dump(mode="json", by_alias=True, exclude_none=True)
    data["analyses"].append(
        {
            "id": "independent",
            "method": "regression",
            "input": "panels.panel",
            "sample": {},
            "outcome": "y",
            "predictors": ["z"],
            "inference": {"method": "hc1"},
        }
    )
    updated = load_recipe(data)
    frame = PANEL.assign(z=[1.0, 4.0, 2.0, 5.0, 3.0, 7.0]).drop(columns=["x"])
    resolved, execution = _replay_inputs(updated)

    replay = replay_results(resolved, execution, {"panel": frame})

    assert replay["analyses"]["fit@base"]["status"] == "failed"
    assert replay["analyses"]["independent@base"]["status"] == "complete"


def test_cached_panel_identity_receipt_is_content_bound(tmp_path):
    from hhplab.results.runner import (
        _identity_receipt_path,
        _receipt_matches,
        _write_identity_receipt,
    )

    panel_path = tmp_path / "panel.parquet"
    panel_path.write_bytes(b"panel bytes v1")
    build_hash = "a" * 64
    _write_identity_receipt(panel_path, build_hash)
    content_hash = hashlib.sha256(panel_path.read_bytes()).hexdigest()
    receipt = _identity_receipt_path(panel_path, build_hash, content_hash)

    assert _receipt_matches(
        receipt,
        panel_path=panel_path,
        build_spec_hash=build_hash,
        content_sha256=content_hash,
    )
    panel_path.write_bytes(b"panel bytes v2")
    assert not _receipt_matches(
        receipt,
        panel_path=panel_path,
        build_spec_hash=build_hash,
        content_sha256=hashlib.sha256(panel_path.read_bytes()).hexdigest(),
    )


def test_readiness_uses_matching_cached_panel_receipt_without_building(tmp_path):
    from hhplab.recipe.executor import _resolve_panel_output_file
    from hhplab.results.runner import _parquet_bytes, _write_identity_receipt
    from hhplab.storage.config import load_config

    project_root = tmp_path / "project"
    project_root.mkdir()
    (project_root / "hhplab.yaml").write_text("output_root: outputs\n")
    recipe = _recipe()
    plan = plan_results(recipe, original_recipe_bytes=b"test recipe bytes")
    output_path = _resolve_panel_output_file(
        recipe,
        "build",
        project_root,
        load_config(project_root=project_root),
    )
    output_path.parent.mkdir(parents=True)
    output_path.write_bytes(_parquet_bytes(PANEL, dataset_type="cached-test-panel"))
    _write_identity_receipt(output_path, plan.panels[0].build_spec_hash)

    result = check_results(
        recipe,
        original_recipe_bytes=b"test recipe bytes",
        readiness=True,
        project_root=project_root,
    )

    assert result["status"] == "ready"
    assert result["issues"] == []


def test_run_packages_bundle_and_terminal_ledger_event(tmp_path):
    recipe = _recipe()
    result = run_results(
        recipe,
        original_recipe_bytes=b"submitted bytes",
        outputs_root=tmp_path,
        panel_frames={"panel": PANEL},
        private=True,
    )

    assert result["statuses"]["execution_status"] == "complete"
    assert result["statuses"]["distribution_status"] == "private"
    assert (
        tmp_path / "results" / "runner-test" / result["run_id"] / "manifest.json"
    ).is_file()
    events = RunLedger(tmp_path).read()["events"]
    assert [event["event"] for event in events] == ["run_started", "run_finished"]
    assert events[-1]["run_id"] == result["run_id"]


def test_executor_built_panel_bytes_verify_after_bundle_move(tmp_path, monkeypatch):
    from hhplab.recipe import executor
    from hhplab.recipe.executor import PipelineResult, StepResult, _resolve_panel_output_file
    from hhplab.results.runner import _identity_receipt_path
    from hhplab.results.verify import verify_bundle
    from hhplab.storage.config import load_config
    from hhplab.storage.provenance import ProvenanceBlock, write_parquet_with_provenance

    project_root = tmp_path / "project"
    project_root.mkdir()
    (project_root / "hhplab.yaml").write_text("output_root: outputs\n")
    recipe = _recipe()

    def build(_recipe, *, project_root, quiet, storage_config):
        path = _resolve_panel_output_file(
            _recipe, "build", project_root, storage_config
        )
        write_parquet_with_provenance(
            PANEL,
            path,
            ProvenanceBlock(extra={"builder": "executor test"}),
        )
        return [
            PipelineResult(
                pipeline_id="build",
                steps=[StepResult(step_kind="persist", detail="panel", success=True)],
            )
        ]

    monkeypatch.setattr(executor, "execute_recipe", build)
    result = run_results(
        recipe,
        original_recipe_bytes=b"submitted bytes",
        outputs_root=tmp_path / "outputs",
        project_root=project_root,
        allow_unknown_aggregates=True,
    )

    source_panel = _resolve_panel_output_file(
        recipe,
        "build",
        project_root,
        load_config(output_root=tmp_path / "outputs", project_root=project_root),
    )
    source_hash = hashlib.sha256(source_panel.read_bytes()).hexdigest()
    receipt = _identity_receipt_path(
        source_panel,
        plan_results(recipe, original_recipe_bytes=b"submitted bytes")
        .panels[0]
        .build_spec_hash,
        source_hash,
    )
    assert receipt.is_file()

    moved_bundle = tmp_path / "portable-bundle"
    from pathlib import Path

    Path(result["bundle_path"]).rename(moved_bundle)
    report = verify_bundle(moved_bundle)

    assert report.integrity == "pass"
    assert report.reproducibility == "partial"
    assert report.partial_reasons == ("unavailable_inputs",)
    assert report.differences == ()
    assert report.errors == ()
    assert report.unavailable[0]["path"] == "panels/panel.parquet"


def test_executor_failure_does_not_certify_existing_panel_output(tmp_path, monkeypatch):
    from hhplab.recipe import executor
    from hhplab.recipe.executor import (
        ExecutorError,
        PipelineResult,
        StepResult,
        _resolve_panel_output_file,
    )
    from hhplab.results.runner import _identity_receipt_path
    from hhplab.storage.config import load_config

    project_root = tmp_path / "project"
    project_root.mkdir()
    (project_root / "hhplab.yaml").write_text("output_root: outputs\n")
    recipe = _recipe()
    path = _resolve_panel_output_file(
        recipe,
        "build",
        project_root,
        load_config(project_root=project_root),
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"stale panel output")

    def fail(*args, **kwargs):
        raise ExecutorError(
            "build failed",
            partial_results=[
                PipelineResult(
                    pipeline_id="build",
                    steps=[StepResult(step_kind="persist", detail="failed", success=False)],
                )
            ],
        )

    monkeypatch.setattr(executor, "execute_recipe", fail)
    result = run_results(
        recipe,
        original_recipe_bytes=b"submitted bytes",
        outputs_root=tmp_path / "outputs",
        project_root=project_root,
    )

    build_hash = plan_results(
        recipe, original_recipe_bytes=b"submitted bytes"
    ).panels[0].build_spec_hash
    content_hash = hashlib.sha256(path.read_bytes()).hexdigest()
    assert not _identity_receipt_path(path, build_hash, content_hash).exists()
    assert result["statuses"]["execution_status"] == "partial"


def test_executor_cache_hit_without_panel_persist_does_not_certify_output(
    tmp_path, monkeypatch
):
    from hhplab.recipe import executor
    from hhplab.recipe.executor import PipelineResult, StepResult, _resolve_panel_output_file
    from hhplab.results.runner import _identity_receipt_path
    from hhplab.storage.config import load_config

    project_root = tmp_path / "project"
    project_root.mkdir()
    (project_root / "hhplab.yaml").write_text("output_root: outputs\n")
    recipe = _recipe()
    path = _resolve_panel_output_file(
        recipe,
        "build",
        project_root,
        load_config(output_root=tmp_path / "outputs", project_root=project_root),
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"unconfirmed cached panel")

    def cached(*args, **kwargs):
        return [
            PipelineResult(
                pipeline_id="build",
                steps=[StepResult(step_kind="join_cache", detail="cache hit", success=True)],
            )
        ]

    monkeypatch.setattr(executor, "execute_recipe", cached)
    result = run_results(
        recipe,
        original_recipe_bytes=b"submitted bytes",
        outputs_root=tmp_path / "outputs",
        project_root=project_root,
    )

    build_hash = plan_results(
        recipe, original_recipe_bytes=b"submitted bytes"
    ).panels[0].build_spec_hash
    content_hash = hashlib.sha256(path.read_bytes()).hexdigest()
    assert not _identity_receipt_path(path, build_hash, content_hash).exists()
    assert result["statuses"]["execution_status"] == "partial"


def test_small_group_descriptive_quantiles_are_withheld_even_when_aggregates_allowed(
    tmp_path,
):
    import json

    raw_recipe = _recipe().model_dump(
        mode="json", by_alias=True, exclude_none=True
    )
    raw_recipe["analyses"] = [
        {
            "id": "describe",
            "method": "descriptive",
            "input": "panels.panel",
            "sample": {},
            "columns": ["x"],
            "stats": ["min", "median", "p10", "p90", "max"],
            "by": ["geo_id"],
        }
    ]
    raw_recipe["variants"] = []
    recipe = load_recipe(raw_recipe)

    result = run_results(
        recipe,
        original_recipe_bytes=b"submitted bytes",
        outputs_root=tmp_path,
        panel_frames={"panel": PANEL},
        allow_unknown_aggregates=True,
    )

    bundle = tmp_path / "results" / recipe.id / result["run_id"]
    withheld = json.loads((bundle / "withheld.json").read_text())
    withheld_paths = {item["path"] for item in withheld["withheld"]}
    assert "results/descriptive_stats.parquet" in withheld_paths
    assert "report/index.html" in withheld_paths
    assert not (bundle / "results" / "descriptive_stats.parquet").exists()
    if (bundle / "results" / "results.json").is_file():
        result_copy = json.loads((bundle / "results" / "results.json").read_text())
        assert "descriptive_stats" not in result_copy


def test_run_scope_bundle_io_failure_writes_separate_failure_package(
    tmp_path, monkeypatch
):
    from hhplab.results import runner

    write = runner.write_bundle

    def fail_primary(destination, payload):
        if "failures" not in destination.parts:
            raise OSError("injected bundle failure")
        return write(destination, payload)

    monkeypatch.setattr(runner, "write_bundle", fail_primary)
    with pytest.raises(ResultsRunError, match="injected bundle failure"):
        run_results(
            _recipe(),
            original_recipe_bytes=b"submitted bytes",
            outputs_root=tmp_path,
            panel_frames={"panel": PANEL},
            private=True,
        )

    events = RunLedger(tmp_path).read()["events"]
    assert [event["event"] for event in events] == ["run_started", "run_failed"]
    assert events[-1]["failure_bundle_path"]
    assert (
        tmp_path / "failures" / "runner-test" / events[-1]["run_id"] / "manifest.json"
    ).is_file()


def test_run_start_ledger_failure_prevents_build(tmp_path, monkeypatch):
    from hhplab.storage import run_ledger

    def fail_start(*args, **kwargs):
        raise OSError("injected ledger failure")

    monkeypatch.setattr(run_ledger.RunLedger, "start", fail_start)
    built = False

    def build(*args, **kwargs):
        nonlocal built
        built = True

    with pytest.raises(OSError, match="injected ledger failure"):
        run_results(
            _recipe(),
            original_recipe_bytes=b"submitted bytes",
            outputs_root=tmp_path,
            build_callable=build,
        )

    assert not built
    assert not (tmp_path / "results").exists()
