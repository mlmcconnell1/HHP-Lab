"""Bundle verification truth tables for integrity, replay, and partial outcomes."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from hhplab.results.verify import VerificationReport, verify_bundle

SHA256_A = "a" * 64
SHA256_B = "b" * 64
TABLE_NAMES = (
    "regression_estimates",
    "regression_fit",
    "descriptive_stats",
    "correlations",
)
ENVIRONMENT = {"python": "3.13.0", "platform": "fixture", "packages": {}}
BASE_TABLES = {name: [] for name in TABLE_NAMES}


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def _make_bundle(
    directory: Path,
    *,
    tables: dict[str, list[dict[str, Any]]] | None = None,
    withheld: list[dict[str, Any]] | None = None,
) -> Path:
    tables = BASE_TABLES if tables is None else tables
    execution = {
        "spec_hash": SHA256_A,
        "tolerance": {"atol": 1e-10, "rtol": 1e-9},
        "panels": {},
        "derived": {},
        "samples": {},
    }
    contents = {
        "recipe/resolved.json": _json_bytes({"recipe": {}, "plan": {}}),
        "recipe/execution.json": _json_bytes(execution),
        "results/results.json": _json_bytes(tables),
        "diagnostics.json": _json_bytes({"diagnostics": []}),
        "findings.json": _json_bytes({"findings": []}),
        "environment/summary.json": _json_bytes(ENVIRONMENT),
    }
    if withheld is not None:
        contents["withheld.json"] = _json_bytes({"withheld": withheld})
    for table_name, rows in tables.items():
        table_path = directory / "results" / f"{table_name}.parquet"
        table_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rows).to_parquet(table_path)
        contents[table_path.relative_to(directory).as_posix()] = table_path.read_bytes()
    for relative, content in contents.items():
        _write(directory / relative, content)
    manifest = {
        "bundle_format_version": "1.0",
        "recipe_id": "fixture",
        "run_id": "run-fixture",
        "created_at": "2026-10-21T12:00:00Z",
        "execution_status": "complete",
        "diagnostics_status": "pass",
        "findings_status": "all_evaluated",
        "distribution_status": "withheld" if withheld else "full",
        "analysis_statuses": {"fit@base": {"status": "complete", "errors": []}},
        "spec_hash": SHA256_A,
        "component_hashes": {"analyses": SHA256_B},
        "lock_status": {"status": "unlocked"},
        "input_identities": [],
        "private": False,
        "files": [],
    }
    _write(directory / "manifest.json", _json_bytes(manifest))
    _refresh_inventory(directory)
    return directory


def _refresh_inventory(directory: Path) -> None:
    manifest_path = directory / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["files"] = [
        {
            "path": path.relative_to(directory).as_posix(),
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "bytes": path.stat().st_size,
            "role": "fixture",
        }
        for path in sorted(directory.rglob("*"))
        if path.is_file() and path.name not in {"manifest.json", "manifest.sha256"}
    ]
    manifest_bytes = _json_bytes(manifest)
    manifest_path.write_bytes(manifest_bytes)
    digest = hashlib.sha256(manifest_bytes).hexdigest()
    _write(directory / "manifest.sha256", f"{digest}  manifest.json\n".encode())


def _replay(
    tables: dict[str, list[dict[str, Any]]] | None = None,
    *,
    unavailable: list[dict[str, Any]] | None = None,
):
    return lambda resolved, execution, panels: {
        "tables": BASE_TABLES if tables is None else tables,
        "analyses": {"fit@base": {"status": "complete"}},
        "diagnostics": {"diagnostics": []},
        "findings": {"findings": []},
        "samples": {},
        "derived": {},
        "unavailable": unavailable or [],
    }


def test_moved_bundle_replays_offline_with_exact_discrete_and_null_values(tmp_path) -> None:
    tables = BASE_TABLES | {
        "descriptive_stats": [{"value": 5e-12, "reason": None}],
    }
    bundle = _make_bundle(tmp_path / "moved" / "bundle", tables=tables)

    report = verify_bundle(
        bundle,
        replay=_replay(tables),
        current_environment=ENVIRONMENT,
    )

    assert report.integrity == "pass"
    assert report.reproducibility == "pass"
    assert report.exit_code == 0


def test_near_zero_values_use_tolerance_but_discrete_counts_are_exact(tmp_path) -> None:
    expected = BASE_TABLES | {
        "regression_fit": [{"r2": 0.0, "n": 4}],
    }
    bundled = BASE_TABLES | {
        "regression_fit": [{"r2": 5e-11, "n": 4}],
    }
    bundle = _make_bundle(tmp_path / "bundle", tables=bundled)

    within = verify_bundle(
        bundle,
        replay=_replay(expected),
        current_environment=ENVIRONMENT,
    )
    assert within.reproducibility == "pass"

    changed_count = BASE_TABLES | {"regression_fit": [{"r2": 5e-11, "n": 5}]}
    _write(bundle / "results/results.json", _json_bytes(changed_count))
    pd.DataFrame(changed_count["regression_fit"]).to_parquet(
        bundle / "results/regression_fit.parquet"
    )
    _refresh_inventory(bundle)
    outside = verify_bundle(
        bundle,
        replay=_replay(expected),
        current_environment=ENVIRONMENT,
    )
    assert outside.reproducibility == "fail"
    assert any(item["path"].endswith(".n") for item in outside.differences)

    float_count = BASE_TABLES | {"regression_fit": [{"r2": 0.0, "n": 4.0}]}
    _write(bundle / "results/results.json", _json_bytes(float_count))
    pd.DataFrame(float_count["regression_fit"]).to_parquet(
        bundle / "results/regression_fit.parquet"
    )
    _refresh_inventory(bundle)
    type_mismatch = verify_bundle(
        bundle,
        replay=_replay(expected),
        current_environment=ENVIRONMENT,
    )
    assert type_mismatch.reproducibility == "fail"


def test_manifest_consistent_estimate_tampering_fails_reproducibility(tmp_path) -> None:
    expected = BASE_TABLES | {"regression_estimates": [{"estimate": 2e-6, "se": None}]}
    bundle = _make_bundle(tmp_path / "bundle", tables=expected)
    changed = expected | {"regression_estimates": [{"estimate": 3e-6, "se": None}]}
    _write(bundle / "results/results.json", _json_bytes(changed))
    pd.DataFrame(changed["regression_estimates"]).to_parquet(
        bundle / "results/regression_estimates.parquet"
    )
    _refresh_inventory(bundle)

    report = verify_bundle(
        bundle,
        replay=_replay(expected),
        current_environment=ENVIRONMENT,
    )

    assert report.integrity == "pass"
    assert report.reproducibility == "fail"
    assert report.exit_code == 2
    assert any("numeric_value_differs" in item["reason"] for item in report.differences)


def test_replay_compares_bundled_panel_derived_and_sample_membership(tmp_path) -> None:
    bundle = _make_bundle(tmp_path / "bundle")
    panel = pd.DataFrame({"geo_id": ["A", "B"], "year": [2020, 2021], "x": [1.0, 2.0]})
    derived = pd.DataFrame({"geo_id": ["A", "B"], "year": [2020, 2021], "dx": [0.0, 1.0]})
    samples = pd.DataFrame(
        {
            "geo_id": ["A", "B"],
            "year": [2020, 2021],
            "included": [True, False],
            "reason": [None, "excluded_by_mask"],
            "sample_source": ["fit@base", "fit@base"],
        }
    )
    (bundle / "panels").mkdir()
    (bundle / "derived").mkdir()
    (bundle / "samples").mkdir()
    panel.to_parquet(bundle / "panels/base.parquet")
    derived.to_parquet(bundle / "derived/base.parquet")
    samples.to_parquet(bundle / "samples/fit@base.parquet")
    execution = json.loads((bundle / "recipe/execution.json").read_text(encoding="utf-8"))
    execution["panels"] = {"base": "panels/base.parquet"}
    execution["derived"] = {"base": "derived/base.parquet"}
    execution["samples"] = {"fit@base": "samples/fit@base.parquet"}
    _write(bundle / "recipe/execution.json", _json_bytes(execution))
    _write(
        bundle / "recipe/resolved.json",
        _json_bytes(
            {
                "recipe": {"derive": [{"id": "dx"}]},
                "plan": {"panels": [{"name": "base", "geo_key": "geo_id", "time_key": "year"}]},
            }
        ),
    )
    manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
    manifest["input_identities"] = [
        {
            "panel": "base",
            "build_spec_hash": SHA256_B,
            "declared_vintages": {},
            "content_sha256": hashlib.sha256(
                (bundle / "panels/base.parquet").read_bytes()
            ).hexdigest(),
        }
    ]
    _write(bundle / "manifest.json", _json_bytes(manifest))
    _refresh_inventory(bundle)
    expected_samples = samples.to_dict(orient="records")

    def replay(resolved, execution, panel_frames):
        assert panel_frames["base"].equals(panel)
        outputs = _replay()(resolved, execution, panel_frames)
        return {
            **outputs,
            "derived": {"base": derived},
            "samples": {"fit@base": expected_samples},
        }

    report = verify_bundle(
        bundle,
        replay=replay,
        current_environment=ENVIRONMENT,
    )

    assert report.integrity == "pass"
    assert report.reproducibility == "pass"


def test_changed_file_without_inventory_update_skips_replay(tmp_path) -> None:
    bundle = _make_bundle(tmp_path / "bundle")
    _write(bundle / "results/results.json", _json_bytes({"corrupt": True}))
    called = False

    def replay(resolved, execution, panels):
        nonlocal called
        called = True
        return _replay()(resolved, execution, panels)

    report = verify_bundle(bundle, replay=replay)

    assert report.integrity == "fail"
    assert report.reproducibility == "not_run"
    assert report.exit_code == 1
    assert not called


def test_withheld_and_environment_reasons_combine_with_partial_precedence(tmp_path) -> None:
    withheld = [
        {
            "path": "panels/private.parquet",
            "sources": ["restricted"],
            "replay_dependencies": ["panel:private"],
            "reason": "restricted",
            "rebuild": "restore authorized data",
        }
    ]
    bundle = _make_bundle(tmp_path / "bundle", withheld=withheld)
    report = verify_bundle(
        bundle,
        replay=_replay(),
        current_environment={"python": "different", "platform": "fixture", "packages": {}},
    )

    assert report.integrity == "pass"
    assert report.reproducibility == "partial"
    assert set(report.partial_reasons) == {"unavailable_inputs", "environment_differs"}
    assert report.exit_code == 3


def test_withheld_panel_excludes_only_dependent_results_and_lists_them(tmp_path) -> None:
    private_diagnostic = {
        "id": "private_identity",
        "op": "linear_identity",
        "terms": [{"analysis": "fit", "term": "x", "sign": 1}],
        "variants": "all",
    }
    private_finding = {
        "id": "private_finding",
        "criteria_source": {"estimate": {"analysis": "fit", "term": "x"}},
    }
    private_diagnostic_row = {
        "id": "private_identity",
        "variant": "base",
        "status": "not_evaluated",
        "value": None,
        "tolerance": None,
        "reason": "missing_or_failed_analysis",
        "details": {},
    }
    private_finding_row = {
        "id": "private_finding",
        "base": {
            "status": "not_evaluated",
            "label": None,
            "compatible_with": [],
            "reason": "analysis_failed:fit@base:panel_unavailable",
            "criteria": {},
        },
        "variants": {},
        "robustness_status": "not_evaluated",
    }
    tables = BASE_TABLES | {
        "descriptive_stats": [
            {
                "analysis": "public",
                "variant": "base",
                "column": "x",
                "group": "all",
                "stat": "mean",
                "value": 2.0,
                "status": "complete",
                "reason": None,
            }
        ]
    }
    withheld = [
        {
            "path": "panels/private.parquet",
            "sources": ["restricted"],
            "replay_dependencies": ["panel:private"],
            "reason": "restricted",
            "rebuild": "restore authorized data",
        }
    ]
    bundle = _make_bundle(tmp_path / "bundle", tables=tables, withheld=withheld)
    _write(
        bundle / "recipe/resolved.json",
        _json_bytes(
            {
                "recipe": {
                    "derive": [{"id": "derive_x", "panels": ["private"]}],
                    "diagnostics": [private_diagnostic],
                    "findings": [private_finding],
                },
                "plan": {
                    "analyses": [
                        {
                            "result_key": "fit@base",
                            "panel_name": "private",
                            "variant_id": "base",
                        }
                    ]
                },
            }
        ),
    )
    _write(
        bundle / "diagnostics.json",
        _json_bytes({"bundle_format_version": "1.0", "diagnostics": [private_diagnostic_row]}),
    )
    _write(
        bundle / "findings.json",
        _json_bytes({"bundle_format_version": "1.0", "findings": [private_finding_row]}),
    )
    _refresh_inventory(bundle)

    def replay(resolved, execution, panels):
        return {
            "tables": tables,
            "analyses": {
                "fit@base": {
                    "status": "failed",
                    "error": {"code": "panel_unavailable", "message": "withheld"},
                }
            },
            "diagnostics": {
                "bundle_format_version": "1.0",
                "diagnostics": [private_diagnostic_row],
            },
            "findings": {
                "bundle_format_version": "1.0",
                "findings": [private_finding_row],
            },
            "samples": {},
            "derived": {},
            "panel_errors": {"private": "panel input withheld"},
        }

    report = verify_bundle(bundle, replay=replay, current_environment=ENVIRONMENT)

    assert report.integrity == "pass"
    assert report.reproducibility == "partial"
    unavailable_paths = {item["path"] for item in report.unavailable}
    assert "analysis:fit@base" in unavailable_paths
    assert "samples/fit@base.parquet" in unavailable_paths
    assert "derived/private.parquet" in unavailable_paths
    assert "diagnostics.json#private_identity@base" in unavailable_paths
    assert "findings.json#private_finding@base" in unavailable_paths
    assert not report.differences


def test_unrelated_mismatch_still_fails_when_panel_outputs_are_withheld(tmp_path) -> None:
    tables = BASE_TABLES | {
        "descriptive_stats": [
            {
                "analysis": "public",
                "variant": "base",
                "column": "x",
                "group": "all",
                "stat": "mean",
                "value": 2.0,
                "status": "complete",
                "reason": None,
            }
        ]
    }
    withheld = [
        {
            "path": "panels/private.parquet",
            "sources": ["restricted"],
            "replay_dependencies": ["panel:private"],
            "reason": "restricted",
            "rebuild": "restore authorized data",
        }
    ]
    bundle = _make_bundle(tmp_path / "bundle", tables=tables, withheld=withheld)
    _write(
        bundle / "recipe/resolved.json",
        _json_bytes(
            {
                "recipe": {},
                "plan": {
                    "analyses": [
                        {
                            "result_key": "fit@base",
                            "panel_name": "private",
                            "variant_id": "base",
                        }
                    ]
                },
            }
        ),
    )
    _refresh_inventory(bundle)
    changed_tables = tables | {
        "descriptive_stats": [{**tables["descriptive_stats"][0], "value": 3.0}]
    }

    report = verify_bundle(
        bundle,
        replay=_replay(changed_tables),
        current_environment=ENVIRONMENT,
    )

    assert report.integrity == "pass"
    assert report.reproducibility == "fail"
    assert report.exit_code == 2
    assert report.unavailable
    assert report.differences


def test_unlisted_file_and_symlink_are_integrity_failures(tmp_path) -> None:
    unlisted = _make_bundle(tmp_path / "unlisted")
    _write(unlisted / "extra.txt", b"not in inventory")
    assert verify_bundle(unlisted, replay=_replay()).integrity == "fail"

    symlinked = _make_bundle(tmp_path / "symlinked")
    target = symlinked / "outside.json"
    target.write_text("{}", encoding="utf-8")
    link = symlinked / "recipe" / "linked.json"
    link.symlink_to(target)
    report = verify_bundle(symlinked, replay=_replay())
    assert report.integrity == "fail"
    assert any("symlink_forbidden" in error for error in report.errors)


def test_execution_path_traversal_is_rejected_before_replay(tmp_path) -> None:
    bundle = _make_bundle(tmp_path / "bundle")
    execution = json.loads((bundle / "recipe/execution.json").read_text(encoding="utf-8"))
    execution["panels"] = {"panel": "../../outside.parquet"}
    _write(bundle / "recipe/execution.json", _json_bytes(execution))
    _refresh_inventory(bundle)
    called = False

    def replay(resolved, execution, panels):
        nonlocal called
        called = True
        return _replay()(resolved, execution, panels)

    report = verify_bundle(
        bundle,
        replay=replay,
        current_environment=ENVIRONMENT,
    )
    assert report.integrity == "pass"
    assert report.reproducibility == "fail"
    assert not called


def test_replay_uses_input_measure_metadata_from_measures_document(tmp_path) -> None:
    bundle = _make_bundle(tmp_path / "bundle")
    measures = {
        "bundle_format_version": "1.0",
        "panels": {},
        "input_measures": {"panel": {"population": {"unit": "persons"}}},
    }
    _write(bundle / "measures.json", _json_bytes(measures))
    _refresh_inventory(bundle)

    def replay(resolved, execution, panels):
        assert execution["input_measures"] == measures["input_measures"]
        return _replay()(resolved, execution, panels)

    report = verify_bundle(bundle, replay=replay, current_environment=ENVIRONMENT)

    assert report.integrity == "pass"
    assert report.reproducibility == "pass"


def test_measures_document_overrides_incomplete_execution_input_metadata(tmp_path) -> None:
    bundle = _make_bundle(tmp_path / "bundle")
    execution = json.loads((bundle / "recipe/execution.json").read_text(encoding="utf-8"))
    execution["input_measures"] = {"panel": {"population": {"unit": "people"}}}
    _write(bundle / "recipe/execution.json", _json_bytes(execution))
    _write(
        bundle / "measures.json",
        _json_bytes(
            {
                "bundle_format_version": "1.0",
                "panels": {},
                "input_measures": {"panel": {"population": {"unit": "persons"}}},
            }
        ),
    )
    _refresh_inventory(bundle)

    def replay(resolved, execution, panels):
        assert execution["input_measures"] == {"panel": {"population": {"unit": "persons"}}}
        return _replay()(resolved, execution, panels)

    report = verify_bundle(bundle, replay=replay, current_environment=ENVIRONMENT)

    assert report.integrity == "pass"
    assert report.reproducibility == "pass"


@pytest.mark.parametrize(
    ("integrity", "reproducibility", "expected"),
    [("fail", "not_run", 1), ("pass", "fail", 2), ("pass", "partial", 3), ("pass", "pass", 0)],
)
def test_exit_code_precedence(integrity: str, reproducibility: str, expected: int) -> None:
    assert VerificationReport(integrity, reproducibility).exit_code == expected
