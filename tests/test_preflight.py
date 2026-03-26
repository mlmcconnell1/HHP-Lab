"""Tests for the recipe preflight system (probes, analyzer, CLI, gaps manifest)."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from coclab.cli.main import app
from coclab.recipe.loader import load_recipe
from coclab.recipe.preflight import (
    FindingKind,
    PreflightFinding,
    PreflightReport,
    Severity,
    run_preflight,
)
from coclab.recipe.probes import (
    probe_dataset_schema,
    probe_geo_column,
    probe_measures,
    probe_static_broadcast,
    probe_temporal_filter,
    probe_year_column,
)
from coclab.recipe.recipe_schema import DatasetSpec, GeometryRef, TemporalFilter

runner = CliRunner()


# ---------------------------------------------------------------------------
# Probe unit tests
# ---------------------------------------------------------------------------

class TestProbeYearColumn:

    def test_declared_present(self):
        r = probe_year_column(["year", "geo_id", "pop"], "year")
        assert r.ok
        assert r.detail["year_column"] == "year"

    def test_declared_missing(self):
        r = probe_year_column(["geo_id", "pop"], "year")
        assert not r.ok
        assert "year" in r.message

    def test_auto_detect_single(self):
        r = probe_year_column(["geo_id", "year", "pop"], None)
        assert r.ok
        assert r.detail["year_column"] == "year"

    def test_auto_detect_none(self):
        r = probe_year_column(["geo_id", "pop"], None)
        assert r.ok
        assert r.detail["year_column"] is None

    def test_ambiguous(self):
        r = probe_year_column(["year", "pit_year", "pop"], None)
        assert not r.ok
        assert "Ambiguous" in r.message


class TestProbeGeoColumn:

    def test_declared_present(self):
        r = probe_geo_column(["coc_id", "year", "pop"], "coc_id")
        assert r.ok
        assert r.detail["geo_column"] == "coc_id"

    def test_declared_missing(self):
        r = probe_geo_column(["year", "pop"], "coc_id")
        assert not r.ok

    def test_auto_detect_single(self):
        r = probe_geo_column(["coc_id", "year", "pop"], None)
        assert r.ok
        assert r.detail["geo_column"] == "coc_id"

    def test_no_candidates(self):
        r = probe_geo_column(["foo", "bar"], None)
        assert not r.ok
        assert "Cannot find" in r.message

    def test_ambiguous(self):
        r = probe_geo_column(["geo_id", "coc_id", "pop"], None)
        assert not r.ok
        assert "Ambiguous" in r.message


class TestProbeMeasures:

    def test_all_present(self):
        r = probe_measures(["geo_id", "pop", "income"], ["pop", "income"], "ds1")
        assert r.ok

    def test_some_missing(self):
        r = probe_measures(["geo_id", "pop"], ["pop", "income"], "ds1")
        assert not r.ok
        assert "income" in r.message
        assert r.detail["missing_measures"] == ["income"]


class TestProbeTemporalFilter:

    def test_column_present(self):
        filt = TemporalFilter(column="date", method="point_in_time", month=1)
        r = probe_temporal_filter(["date", "year", "pop"], filt, "ds1")
        assert r.ok

    def test_column_missing(self):
        filt = TemporalFilter(column="date", method="point_in_time", month=1)
        r = probe_temporal_filter(["year", "pop"], filt, "ds1")
        assert not r.ok
        assert "date" in r.message


class TestProbeStaticBroadcast:

    def _make_ds(self, **overrides) -> DatasetSpec:
        defaults = {
            "provider": "test",
            "product": "test",
            "version": 1,
            "native_geometry": GeometryRef(type="coc"),
        }
        defaults.update(overrides)
        return DatasetSpec(**defaults)

    def test_with_year_column(self):
        ds = self._make_ds()
        r = probe_static_broadcast(ds, "ds1", True, 3)
        assert r.ok

    def test_single_year_universe(self):
        ds = self._make_ds()
        r = probe_static_broadcast(ds, "ds1", False, 1)
        assert r.ok

    def test_broadcast_static_opt_in(self):
        ds = self._make_ds(params={"broadcast_static": True})
        r = probe_static_broadcast(ds, "ds1", False, 3)
        assert r.ok

    def test_distinct_paths_safe(self):
        ds = self._make_ds()
        r = probe_static_broadcast(ds, "ds1", False, 3, distinct_paths=3)
        assert r.ok

    def test_implicit_broadcast_blocked(self):
        ds = self._make_ds()
        r = probe_static_broadcast(ds, "ds1", False, 3)
        assert not r.ok
        assert "broadcast" in r.message


class TestProbeDatasetSchema:

    def test_valid_parquet(self, tmp_path: Path):
        path = tmp_path / "test.parquet"
        pd.DataFrame({"geo_id": ["A"], "year": [2020]}).to_parquet(path)
        r = probe_dataset_schema(path)
        assert r.ok
        assert "geo_id" in r.detail["columns"]

    def test_missing_file(self, tmp_path: Path):
        r = probe_dataset_schema(tmp_path / "nope.parquet")
        assert not r.ok
        assert "not found" in r.message


# ---------------------------------------------------------------------------
# Preflight analyzer tests
# ---------------------------------------------------------------------------

def _preflight_recipe(
    *,
    with_path: bool = False,
    with_file_set: bool = False,
    identity_only: bool = False,
) -> dict:
    """Return a recipe dict for preflight testing."""
    base: dict = {
        "version": 1,
        "name": "preflight-test",
        "universe": {"range": "2020-2022"},
        "targets": [
            {"id": "coc_panel", "geometry": {"type": "coc", "vintage": 2025}},
        ],
        "datasets": {
            "pit": {
                "provider": "hud",
                "product": "pit",
                "version": 1,
                "native_geometry": {"type": "coc"},
                "years": "2020-2022",
            },
        },
        "transforms": [],
        "pipelines": [
            {
                "id": "main",
                "target": "coc_panel",
                "steps": [
                    {
                        "resample": {
                            "dataset": "pit",
                            "to_geometry": {"type": "coc", "vintage": 2025},
                            "method": "identity",
                            "measures": ["pit_total"],
                        },
                    },
                    {
                        "join": {
                            "datasets": ["pit"],
                            "join_on": ["geo_id", "year"],
                        },
                    },
                ],
            },
        ],
    }

    if with_path:
        base["datasets"]["pit"]["path"] = "data/pit.parquet"

    if not identity_only:
        base["datasets"]["acs"] = {
            "provider": "census",
            "product": "acs5",
            "version": 1,
            "native_geometry": {"type": "tract", "vintage": 2020},
            "years": "2020-2022",
        }
        base["transforms"] = [
            {
                "id": "tract_to_coc",
                "type": "crosswalk",
                "from": {"type": "tract", "vintage": 2020},
                "to": {"type": "coc", "vintage": 2025},
                "spec": {"weighting": {"scheme": "area"}},
            },
        ]
        base["pipelines"][0]["steps"].insert(0, {
            "materialize": {"transforms": ["tract_to_coc"]},
        })
        base["pipelines"][0]["steps"].insert(2, {
            "resample": {
                "dataset": "acs",
                "to_geometry": {"type": "coc", "vintage": 2025},
                "method": "aggregate",
                "via": "tract_to_coc",
                "measures": ["total_population"],
            },
        })
        base["pipelines"][0]["steps"][-1]["join"]["datasets"] = ["pit", "acs"]

    return base


def _setup_preflight_fixtures(
    tmp_path: Path,
    *,
    include_xwalk: bool = True,
    include_pit: bool = True,
    include_acs: bool = True,
) -> None:
    """Set up dataset and transform files for preflight tests."""
    if include_xwalk:
        xwalk_dir = tmp_path / "data" / "curated" / "xwalks"
        xwalk_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({
            "coc_id": ["COC1", "COC2"],
            "tract_geoid": ["T1", "T2"],
            "area_share": [1.0, 1.0],
        }).to_parquet(xwalk_dir / "xwalk__B2025xT2020.parquet")

    if include_pit:
        pit_path = tmp_path / "data" / "pit.parquet"
        pit_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({
            "coc_id": ["COC1", "COC2", "COC1", "COC2", "COC1", "COC2"],
            "year": [2020, 2020, 2021, 2021, 2022, 2022],
            "pit_total": [10, 20, 11, 21, 12, 22],
        }).to_parquet(pit_path)

    if include_acs:
        acs_path = tmp_path / "data" / "acs.parquet"
        (tmp_path / "data").mkdir(parents=True, exist_ok=True)
        pd.DataFrame({
            "GEOID": ["T1", "T2", "T1", "T2", "T1", "T2"],
            "year": [2020, 2020, 2021, 2021, 2022, 2022],
            "total_population": [100, 200, 110, 210, 120, 220],
        }).to_parquet(acs_path)


class TestRunPreflight:

    def test_clean_preflight(self, tmp_path: Path):
        data = _preflight_recipe(with_path=True, identity_only=True)
        _setup_preflight_fixtures(tmp_path, include_xwalk=False, include_acs=False)
        recipe = load_recipe(data)
        report = run_preflight(recipe, project_root=tmp_path)
        assert report.is_ready
        assert report.blocking_count == 0
        assert len(report.pipelines) == 1
        assert report.pipelines[0].task_count > 0

    def test_missing_transform_artifact(self, tmp_path: Path):
        data = _preflight_recipe(with_path=True)
        data["datasets"]["acs"]["path"] = "data/acs.parquet"
        _setup_preflight_fixtures(tmp_path, include_xwalk=False)
        recipe = load_recipe(data)
        report = run_preflight(recipe, project_root=tmp_path)
        assert not report.is_ready
        transform_findings = [
            f for f in report.findings
            if f.kind == FindingKind.MISSING_TRANSFORM
        ]
        assert len(transform_findings) >= 1
        assert transform_findings[0].transform_id == "tract_to_coc"
        assert transform_findings[0].remediation is not None

    def test_missing_dataset_file(self, tmp_path: Path):
        data = _preflight_recipe(with_path=True, identity_only=True)
        # Don't create the pit file
        recipe = load_recipe(data)
        report = run_preflight(recipe, project_root=tmp_path)
        ds_findings = [
            f for f in report.findings
            if f.kind == FindingKind.MISSING_DATASET
        ]
        assert len(ds_findings) >= 1
        assert ds_findings[0].dataset_id == "pit"

    def test_planner_error_captured(self, tmp_path: Path):
        data = _preflight_recipe(with_path=True, identity_only=True)
        # Add step referencing a dataset not in the recipe
        data["pipelines"][0]["steps"].insert(0, {
            "resample": {
                "dataset": "nonexistent",
                "to_geometry": {"type": "coc", "vintage": 2025},
                "method": "identity",
                "measures": ["x"],
            },
        })
        # Pydantic will reject this since 'nonexistent' isn't in datasets —
        # add a dummy dataset to pass schema validation
        data["datasets"]["nonexistent"] = {
            "provider": "test",
            "product": "pit",
            "version": 1,
            "native_geometry": {"type": "coc"},
            # No years declared and no path — planner won't fail here,
            # but the path/schema probes will catch missing files
        }
        recipe = load_recipe(data)
        report = run_preflight(recipe, project_root=tmp_path)
        # Report should have findings (missing dataset at minimum)
        assert len(report.findings) > 0

    def test_multi_pipeline_recipe(self, tmp_path: Path):
        data = _preflight_recipe(with_path=True, identity_only=True)
        _setup_preflight_fixtures(tmp_path, include_xwalk=False, include_acs=False)
        # Add a second pipeline
        data["pipelines"].append({
            "id": "secondary",
            "target": "coc_panel",
            "steps": [
                {
                    "resample": {
                        "dataset": "pit",
                        "to_geometry": {"type": "coc", "vintage": 2025},
                        "method": "identity",
                        "measures": ["pit_total"],
                    },
                },
                {
                    "join": {
                        "datasets": ["pit"],
                        "join_on": ["geo_id", "year"],
                    },
                },
            ],
        })
        recipe = load_recipe(data)
        report = run_preflight(recipe, project_root=tmp_path)
        assert len(report.pipelines) == 2
        assert report.pipelines[0].pipeline_id == "main"
        assert report.pipelines[1].pipeline_id == "secondary"

    def test_schema_probe_missing_measure(self, tmp_path: Path):
        data = _preflight_recipe(with_path=True, identity_only=True)
        # Create a pit file WITHOUT the pit_total column
        pit_path = tmp_path / "data" / "pit.parquet"
        pit_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({
            "coc_id": ["COC1"],
            "year": [2020],
            "wrong_column": [10],
        }).to_parquet(pit_path)

        recipe = load_recipe(data)
        report = run_preflight(recipe, project_root=tmp_path)
        measure_findings = [
            f for f in report.findings
            if f.kind == FindingKind.MISSING_MEASURE
        ]
        assert len(measure_findings) >= 1
        assert "pit_total" in measure_findings[0].message


# ---------------------------------------------------------------------------
# Report model tests
# ---------------------------------------------------------------------------

class TestPreflightReport:

    def test_to_dict(self):
        report = PreflightReport(
            recipe_name="test",
            recipe_version=1,
            universe_years=[2020, 2021],
        )
        d = report.to_dict()
        assert d["ready"] is True
        assert d["blocking_count"] == 0
        assert d["warning_count"] == 0

    def test_gaps_manifest(self):
        report = PreflightReport(
            recipe_name="test",
            recipe_version=1,
            universe_years=[2020, 2021],
            findings=[
                PreflightFinding(
                    severity=Severity.ERROR,
                    kind=FindingKind.MISSING_DATASET,
                    message="Dataset 'pit' missing",
                    dataset_id="pit",
                ),
                PreflightFinding(
                    severity=Severity.WARNING,
                    kind=FindingKind.MISSING_DATASET,
                    message="Dataset 'optional' missing",
                    dataset_id="optional",
                ),
                PreflightFinding(
                    severity=Severity.ERROR,
                    kind=FindingKind.PLANNER_ERROR,
                    message="planner failed",
                ),
            ],
        )
        manifest = report.gaps_manifest()
        assert manifest["total_gaps"] == 2  # planner_error is not a gap kind
        assert manifest["blocking_gaps"] == 1
        assert "missing_dataset" in manifest["gaps_by_kind"]
        assert len(manifest["gaps_by_kind"]["missing_dataset"]) == 2

    def test_finding_to_dict_with_remediation(self):
        f = PreflightFinding(
            severity=Severity.ERROR,
            kind=FindingKind.MISSING_TRANSFORM,
            message="Transform 'x' missing",
            transform_id="x",
            remediation=_make_remediation(),
        )
        d = f.to_dict()
        assert d["severity"] == "error"
        assert d["kind"] == "missing_transform"
        assert d["transform_id"] == "x"
        assert "remediation" in d
        assert d["remediation"]["command"] == "coclab generate xwalks"

    def test_blocking_findings(self):
        report = PreflightReport(
            recipe_name="test",
            recipe_version=1,
            universe_years=[2020],
            findings=[
                PreflightFinding(
                    severity=Severity.ERROR,
                    kind=FindingKind.MISSING_DATASET,
                    message="err",
                ),
                PreflightFinding(
                    severity=Severity.WARNING,
                    kind=FindingKind.MISSING_DATASET,
                    message="warn",
                ),
            ],
        )
        blockers = report.blocking_findings()
        assert len(blockers) == 1
        assert blockers[0].message == "err"


def _make_remediation():
    from coclab.recipe.preflight import Remediation
    return Remediation(
        hint="Generate crosswalk artifacts.",
        command="coclab generate xwalks",
    )


# ---------------------------------------------------------------------------
# CLI tests
# ---------------------------------------------------------------------------

def _make_project_root(tmp_path: Path) -> None:
    """Create marker files so _check_working_directory() doesn't warn."""
    (tmp_path / "pyproject.toml").write_text("[project]\nname='test'\n")
    (tmp_path / "coclab").mkdir(exist_ok=True)
    (tmp_path / "data").mkdir(exist_ok=True)


def _write_recipe(tmp_path: Path, data: dict) -> Path:
    import yaml

    recipe_file = tmp_path / "recipe.yaml"
    recipe_file.write_text(yaml.dump(data), encoding="utf-8")
    return recipe_file


class TestPreflightCLI:

    def test_human_output_clean(self, tmp_path: Path, monkeypatch):
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        data = _preflight_recipe(with_path=True, identity_only=True)
        _setup_preflight_fixtures(
            tmp_path, include_xwalk=False, include_acs=False,
        )
        rf = _write_recipe(tmp_path, data)
        result = runner.invoke(app, [
            "build", "recipe-preflight",
            "--recipe", str(rf),
        ])
        assert result.exit_code == 0
        assert "Ready to build" in result.output

    def test_human_output_blockers(self, tmp_path: Path, monkeypatch):
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        data = _preflight_recipe(with_path=True, identity_only=True)
        # Don't create fixtures — missing files
        rf = _write_recipe(tmp_path, data)
        result = runner.invoke(app, [
            "build", "recipe-preflight",
            "--recipe", str(rf),
        ])
        assert result.exit_code == 1
        assert "Blocker" in result.output or "FAILED" in result.output

    def test_json_output_ready(self, tmp_path: Path, monkeypatch):
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        data = _preflight_recipe(with_path=True, identity_only=True)
        _setup_preflight_fixtures(
            tmp_path, include_xwalk=False, include_acs=False,
        )
        rf = _write_recipe(tmp_path, data)
        result = runner.invoke(app, [
            "build", "recipe-preflight",
            "--recipe", str(rf),
            "--json",
        ])
        assert result.exit_code == 0
        out = json.loads(result.output)
        assert out["status"] == "ok"
        assert out["ready"] is True

    def test_json_output_blocked(self, tmp_path: Path, monkeypatch):
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        data = _preflight_recipe(with_path=True, identity_only=True)
        rf = _write_recipe(tmp_path, data)
        result = runner.invoke(app, [
            "build", "recipe-preflight",
            "--recipe", str(rf),
            "--json",
        ])
        assert result.exit_code == 1
        out = json.loads(result.output)
        assert out["status"] == "blocked"
        assert out["blocking_count"] > 0

    def test_gaps_output(self, tmp_path: Path, monkeypatch):
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        data = _preflight_recipe(with_path=True, identity_only=True)
        rf = _write_recipe(tmp_path, data)
        result = runner.invoke(app, [
            "build", "recipe-preflight",
            "--recipe", str(rf),
            "--gaps",
        ])
        out = json.loads(result.output)
        assert "total_gaps" in out
        assert "gaps_by_kind" in out

    def test_invalid_recipe(self, tmp_path: Path, monkeypatch):
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        rf = tmp_path / "bad.yaml"
        rf.write_text("not: a: recipe", encoding="utf-8")
        result = runner.invoke(app, [
            "build", "recipe-preflight",
            "--recipe", str(rf),
        ])
        assert result.exit_code == 2


class TestBuildRecipeWithPreflight:

    def test_preflight_blocks_execution(self, tmp_path: Path, monkeypatch):
        """When dataset files exist but transform is missing, preflight catches it."""
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        data = _preflight_recipe(with_path=True)
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        data["datasets"]["acs"]["path"] = "data/acs.parquet"
        _setup_preflight_fixtures(
            tmp_path, include_xwalk=False,
        )
        rf = _write_recipe(tmp_path, data)
        result = runner.invoke(app, [
            "build", "recipe",
            "--recipe", str(rf),
        ])
        assert result.exit_code == 1
        assert "Preflight" in result.output or "blocker" in result.output

    def test_preflight_blocks_json(self, tmp_path: Path, monkeypatch):
        """JSON mode emits blocked status with preflight details."""
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        data = _preflight_recipe(with_path=True)
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        data["datasets"]["acs"]["path"] = "data/acs.parquet"
        _setup_preflight_fixtures(
            tmp_path, include_xwalk=False,
        )
        rf = _write_recipe(tmp_path, data)
        result = runner.invoke(app, [
            "build", "recipe",
            "--recipe", str(rf),
            "--json",
        ])
        assert result.exit_code == 1
        out = json.loads(result.output)
        assert out["status"] == "blocked"
        assert "preflight" in out

    def test_skip_preflight(self, tmp_path: Path, monkeypatch):
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        # Use a recipe without path so old validation passes
        data = _preflight_recipe(with_path=False, identity_only=True)
        rf = _write_recipe(tmp_path, data)
        result = runner.invoke(app, [
            "build", "recipe",
            "--recipe", str(rf),
            "--dry-run", "--skip-preflight",
        ])
        assert result.exit_code == 0

    def test_clean_preflight_allows_dry_run(
        self, tmp_path: Path, monkeypatch,
    ):
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        data = _preflight_recipe(with_path=True, identity_only=True)
        _setup_preflight_fixtures(
            tmp_path, include_xwalk=False, include_acs=False,
        )
        rf = _write_recipe(tmp_path, data)
        result = runner.invoke(app, [
            "build", "recipe",
            "--recipe", str(rf),
            "--dry-run",
        ])
        assert result.exit_code == 0
        assert "all clear" in result.output
