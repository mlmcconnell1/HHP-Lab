"""Tests for ACS 1-year metro pipeline recipe integration.

Verifies that the recipe planner, preflight, and executor correctly
handle ACS 1-year metro-native datasets using identity resampling.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import yaml

from hhplab.recipe.executor import execute_recipe
from hhplab.recipe.loader import load_recipe
from hhplab.recipe.planner import resolve_plan
from hhplab.recipe.preflight import (
    FindingKind,
    Severity,
    run_preflight,
)
from hhplab.recipe.probes import (
    probe_geo_column,
    probe_measures,
    probe_year_column,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parent.parent
ACS1_RECIPE_PATH = REPO_ROOT / "recipes" / "metro25-glynnfox-acs1.yaml"


def _acs1_recipe_dict() -> dict:
    """Minimal recipe with an ACS1 identity resample step."""
    return {
        "version": 1,
        "name": "acs1-test",
        "universe": {"years": [2023]},
        "targets": [
            {"id": "metro_panel", "geometry": {"type": "metro", "source": "glynn_fox_v1"}},
        ],
        "datasets": {
            "acs1_metro": {
                "provider": "census",
                "product": "acs1",
                "version": 1,
                "native_geometry": {
                    "type": "metro",
                    "source": "glynn_fox_v1",
                },
                "years": {"years": [2023]},
                "year_column": "acs1_vintage",
                "geo_column": "metro_id",
                "path": "data/curated/acs/acs1_metro.parquet",
            },
        },
        "transforms": [],
        "pipelines": [
            {
                "id": "main",
                "target": "metro_panel",
                "steps": [
                    {
                        "resample": {
                            "dataset": "acs1_metro",
                            "to_geometry": {"type": "metro", "source": "glynn_fox_v1"},
                            "method": "identity",
                            "measures": {
                                "unemployment_rate_acs1": {"aggregation": "mean"},
                            },
                        },
                    },
                    {
                        "join": {
                            "datasets": ["acs1_metro"],
                            "join_on": ["geo_id", "year"],
                        },
                    },
                ],
            },
        ],
    }


def _make_acs1_parquet(path: Path) -> None:
    """Write a minimal ACS1 metro parquet file for testing."""
    df = pd.DataFrame(
        {
            "metro_id": ["GF01", "GF02"],
            "unemployment_rate_acs1": [0.05, 0.03],
            "acs1_vintage": [2023, 2023],
            "metro_name": ["New York", "Los Angeles"],
            "definition_version": ["glynn_fox_v1", "glynn_fox_v1"],
            "cbsa_code": ["35620", "31080"],
            "pop_16_plus": [16000000, 10500000],
            "civilian_labor_force": [10000000, 6800000],
            "unemployed_count": [500000, 340000],
            "data_source": ["census_acs1", "census_acs1"],
            "source_ref": [
                "https://api.census.gov/data/2023/acs/acs1",
                "https://api.census.gov/data/2023/acs/acs1",
            ],
            "ingested_at": [pd.Timestamp.now(), pd.Timestamp.now()],
        }
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path)


def _setup_committed_recipe_execution_fixtures(tmp_path: Path) -> None:
    """Create the lagged ACS1 inputs needed to execute the committed recipe."""
    from hhplab.metro.metro_io import write_metro_artifacts

    write_metro_artifacts(base_dir=tmp_path / "data")

    tract_dir = tmp_path / "data" / "curated" / "tiger"
    tract_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "GEOID": ["36061000100", "06037000100"],
        }
    ).to_parquet(tract_dir / "tracts__T2020.parquet")

    pit_dir = tmp_path / "data" / "curated" / "pit"
    pit_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "coc_id": ["NY-600", "CA-600"],
            "pit_year": [2023, 2023],
            "pit_total": [100, 200],
            "pit_sheltered": [60, 120],
            "pit_unsheltered": [40, 80],
        }
    ).to_parquet(pit_dir / "pit_vintage__P2024.parquet")

    pep_dir = tmp_path / "data" / "curated" / "pep"
    pep_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "county_fips": ["36061", "06037"],
            "year": [2023, 2023],
            "population": [1600000, 950000],
        }
    ).to_parquet(pep_dir / "pep_county__v2024.parquet")

    acs_dir = tmp_path / "data" / "curated" / "acs"
    acs_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "tract_geoid": ["36061000100", "06037000100"],
            "total_population": [500000, 450000],
            "adult_population": [400000, 350000],
            "population_below_poverty": [75000, 90000],
            "median_household_income": [85000.0, 70000.0],
            "median_gross_rent": [2200.0, 1800.0],
        }
    ).to_parquet(acs_dir / "acs5_tracts__A2022xT2020.parquet")

    pd.DataFrame(
        {
            "metro_id": ["GF01", "GF02"],
            "acs1_vintage": [2022, 2022],
            "unemployment_rate_acs1": [0.041, 0.052],
        }
    ).to_parquet(acs_dir / "acs1_metro__A2022@Dglynnfoxv1.parquet")


def _find_panel_output(tmp_path: Path) -> Path:
    matches = list(tmp_path.rglob("panel__*.parquet"))
    assert matches, f"No panel output found under {tmp_path}"
    return matches[0]


# ===========================================================================
# Plan resolution tests
# ===========================================================================


class TestPlanResolutionACS1:
    """Recipe with ACS1 identity resample resolves plan correctly."""

    def test_resolves_identity_resample(self):
        recipe = load_recipe(_acs1_recipe_dict())
        plan = resolve_plan(recipe, "main")

        acs1_tasks = [t for t in plan.resample_tasks if t.dataset_id == "acs1_metro"]
        assert len(acs1_tasks) == 1

        task = acs1_tasks[0]
        assert task.method == "identity"
        assert task.transform_id is None
        assert task.year == 2023
        assert "unemployment_rate_acs1" in task.measures

    def test_resample_task_carries_year_and_geo_columns(self):
        recipe = load_recipe(_acs1_recipe_dict())
        plan = resolve_plan(recipe, "main")

        task = next(t for t in plan.resample_tasks if t.dataset_id == "acs1_metro")
        assert task.year_column == "acs1_vintage"
        assert task.geo_column == "metro_id"

    def test_resample_task_carries_measure_aggregations(self):
        recipe = load_recipe(_acs1_recipe_dict())
        plan = resolve_plan(recipe, "main")

        task = next(t for t in plan.resample_tasks if t.dataset_id == "acs1_metro")
        assert task.measure_aggregations == {"unemployment_rate_acs1": "mean"}

    def test_input_path_resolved(self):
        recipe = load_recipe(_acs1_recipe_dict())
        plan = resolve_plan(recipe, "main")

        task = next(t for t in plan.resample_tasks if t.dataset_id == "acs1_metro")
        assert task.input_path == "data/curated/acs/acs1_metro.parquet"

    def test_effective_geometry_matches_native(self):
        recipe = load_recipe(_acs1_recipe_dict())
        plan = resolve_plan(recipe, "main")

        task = next(t for t in plan.resample_tasks if t.dataset_id == "acs1_metro")
        assert task.effective_geometry.type == "metro"
        assert task.effective_geometry.source == "glynn_fox_v1"


# ===========================================================================
# Committed recipe file test
# ===========================================================================


class TestCommittedACS1Recipe:
    """Verify the committed recipes/metro25-glynnfox-acs1.yaml loads and plans."""

    def test_recipe_loads(self):
        with ACS1_RECIPE_PATH.open(encoding="utf-8") as f:
            data = yaml.safe_load(f)
        recipe = load_recipe(data)
        assert recipe.name == "glynn_fox_metro_panel_2023_acs1"
        assert recipe.targets[0].panel_policy.column_aliases["population"] == "pep_population"

    def test_plan_resolves(self):
        with ACS1_RECIPE_PATH.open(encoding="utf-8") as f:
            data = yaml.safe_load(f)
        recipe = load_recipe(data)
        plan = resolve_plan(recipe, "build_metro_panel")

        acs1_tasks = [t for t in plan.resample_tasks if t.dataset_id == "acs1_metro"]
        assert len(acs1_tasks) == 1
        assert acs1_tasks[0].method == "identity"
        assert acs1_tasks[0].transform_id is None

    def test_plan_uses_lagged_acs5_support_path(self):
        with ACS1_RECIPE_PATH.open(encoding="utf-8") as f:
            data = yaml.safe_load(f)
        recipe = load_recipe(data)
        plan = resolve_plan(recipe, "build_metro_panel")

        acs5_tasks = [t for t in plan.resample_tasks if t.dataset_id == "acs_tract"]
        assert len(acs5_tasks) == 1
        assert acs5_tasks[0].input_path == "data/curated/acs/acs5_tracts__A2022xT2020.parquet"

    def test_plan_uses_lagged_acs1_path(self):
        with ACS1_RECIPE_PATH.open(encoding="utf-8") as f:
            data = yaml.safe_load(f)
        recipe = load_recipe(data)
        plan = resolve_plan(recipe, "build_metro_panel")

        acs1_tasks = [t for t in plan.resample_tasks if t.dataset_id == "acs1_metro"]
        assert len(acs1_tasks) == 1
        assert acs1_tasks[0].input_path == "data/curated/acs/acs1_metro__A2022@Dglynnfoxv1.parquet"

    def test_join_includes_acs1(self):
        with ACS1_RECIPE_PATH.open(encoding="utf-8") as f:
            data = yaml.safe_load(f)
        recipe = load_recipe(data)
        plan = resolve_plan(recipe, "build_metro_panel")

        assert len(plan.join_tasks) == 1
        assert "acs1_metro" in plan.join_tasks[0].datasets

    def test_execute_recipe_uses_lagged_acs1_vintage(self, tmp_path: Path):
        """The committed recipe executes with ACS1 vintage 2022 for analysis year 2023."""
        with ACS1_RECIPE_PATH.open(encoding="utf-8") as f:
            data = yaml.safe_load(f)
        recipe = load_recipe(data)
        _setup_committed_recipe_execution_fixtures(tmp_path)

        results = execute_recipe(recipe, project_root=tmp_path)

        assert len(results) == 1
        assert results[0].success

        panel = pd.read_parquet(_find_panel_output(tmp_path)).sort_values("metro_id")
        assert set(panel["year"]) == {2023}
        assert list(panel["metro_id"]) == ["GF01", "GF02"]
        assert set(panel["acs1_vintage_used"]) == {"2022"}
        assert list(panel["unemployment_rate_acs1"]) == [0.041, 0.052]


# ===========================================================================
# Probe tests for ACS1 columns
# ===========================================================================


class TestProbesACS1:
    """Verify probes detect ACS1-specific columns correctly."""

    def test_geo_column_detects_metro_id(self):
        columns = ["metro_id", "acs1_vintage", "unemployment_rate_acs1"]
        result = probe_geo_column(columns, None)
        assert result.ok
        assert result.detail["geo_column"] == "metro_id"

    def test_geo_column_declared_metro_id(self):
        columns = ["metro_id", "acs1_vintage", "unemployment_rate_acs1"]
        result = probe_geo_column(columns, "metro_id")
        assert result.ok
        assert result.detail["geo_column"] == "metro_id"

    def test_year_column_declared_acs1_vintage(self):
        columns = ["metro_id", "acs1_vintage", "unemployment_rate_acs1"]
        result = probe_year_column(columns, "acs1_vintage")
        assert result.ok
        assert result.detail["year_column"] == "acs1_vintage"

    def test_year_column_auto_detects_acs1_vintage(self):
        columns = ["metro_id", "acs1_vintage", "unemployment_rate_acs1"]
        result = probe_year_column(columns, None)
        assert result.ok
        assert result.detail["year_column"] == "acs1_vintage"

    def test_measures_present(self):
        columns = ["metro_id", "acs1_vintage", "unemployment_rate_acs1"]
        result = probe_measures(columns, ["unemployment_rate_acs1"], "acs1_metro")
        assert result.ok

    def test_measures_missing(self):
        columns = ["metro_id", "acs1_vintage"]
        result = probe_measures(columns, ["unemployment_rate_acs1"], "acs1_metro")
        assert not result.ok
        assert "unemployment_rate_acs1" in result.message


# ===========================================================================
# Preflight tests
# ===========================================================================


class TestPreflightACS1:
    """Preflight checks for ACS1 datasets."""

    def test_preflight_clean(self, tmp_path: Path):
        """Preflight passes when ACS1 artifact exists with correct schema."""
        data = _acs1_recipe_dict()
        recipe = load_recipe(data)

        artifact_path = tmp_path / "data" / "curated" / "acs" / "acs1_metro.parquet"
        _make_acs1_parquet(artifact_path)

        report = run_preflight(recipe, project_root=tmp_path)
        assert report.is_ready, (
            f"Preflight should pass but found blocking findings: "
            f"{[f.message for f in report.blocking_findings()]}"
        )

    def test_preflight_missing_artifact(self, tmp_path: Path):
        """Preflight reports missing ACS1 artifact with remediation hint."""
        data = _acs1_recipe_dict()
        recipe = load_recipe(data)

        report = run_preflight(recipe, project_root=tmp_path)

        missing = [
            f
            for f in report.findings
            if f.kind == FindingKind.MISSING_DATASET and f.dataset_id == "acs1_metro"
        ]
        assert len(missing) >= 1
        assert missing[0].severity == Severity.ERROR
        assert missing[0].remediation is not None
        assert "acs1" in missing[0].remediation.hint.lower()

    def test_preflight_schema_detects_measure(self, tmp_path: Path):
        """Preflight detects unemployment_rate_acs1 as a valid measure."""
        data = _acs1_recipe_dict()
        recipe = load_recipe(data)

        artifact_path = tmp_path / "data" / "curated" / "acs" / "acs1_metro.parquet"
        _make_acs1_parquet(artifact_path)

        report = run_preflight(recipe, project_root=tmp_path)

        measure_findings = [f for f in report.findings if f.kind == FindingKind.MISSING_MEASURE]
        assert len(measure_findings) == 0, (
            f"Should not report missing measures but found: {[f.message for f in measure_findings]}"
        )

    def test_preflight_detects_missing_measure(self, tmp_path: Path):
        """Preflight detects when a declared measure is missing from the artifact."""
        data = _acs1_recipe_dict()
        # Add a measure that doesn't exist in the parquet
        data["pipelines"][0]["steps"][0]["resample"]["measures"]["nonexistent_measure"] = {
            "aggregation": "mean",
        }
        recipe = load_recipe(data)

        artifact_path = tmp_path / "data" / "curated" / "acs" / "acs1_metro.parquet"
        _make_acs1_parquet(artifact_path)

        report = run_preflight(recipe, project_root=tmp_path)

        measure_findings = [f for f in report.findings if f.kind == FindingKind.MISSING_MEASURE]
        assert len(measure_findings) >= 1
        assert "nonexistent_measure" in measure_findings[0].message

    def test_preflight_plan_resolves(self, tmp_path: Path):
        """Preflight resolves plan without planner errors."""
        data = _acs1_recipe_dict()
        recipe = load_recipe(data)

        artifact_path = tmp_path / "data" / "curated" / "acs" / "acs1_metro.parquet"
        _make_acs1_parquet(artifact_path)

        report = run_preflight(recipe, project_root=tmp_path)

        planner_findings = [f for f in report.findings if f.kind == FindingKind.PLANNER_ERROR]
        assert len(planner_findings) == 0

        assert len(report.pipelines) == 1
        assert report.pipelines[0].plan is not None
        assert report.pipelines[0].task_count > 0


# ===========================================================================
# Identity resample execution test
# ===========================================================================


class TestExecutorIdentityResampleACS1:
    """Test that identity resample passes ACS1 data through correctly."""

    def test_identity_resample_passthrough(self, tmp_path: Path):
        """Identity resample of ACS1 data passes through measures unchanged."""
        from hhplab.recipe.executor import _resample_identity
        from hhplab.recipe.planner import GeometryRef, ResampleTask

        df = pd.DataFrame(
            {
                "metro_id": ["GF01", "GF02"],
                "unemployment_rate_acs1": [0.05, 0.03],
                "year": [2023, 2023],
            }
        )

        task = ResampleTask(
            dataset_id="acs1_metro",
            year=2023,
            input_path="data/curated/acs/acs1_metro.parquet",
            effective_geometry=GeometryRef(type="metro", source="glynn_fox_v1"),
            method="identity",
            transform_id=None,
            to_geometry=GeometryRef(type="metro", source="glynn_fox_v1"),
            measures=["unemployment_rate_acs1"],
            geo_column="metro_id",
        )

        result = _resample_identity(df, task)

        assert "geo_id" in result.columns
        assert "unemployment_rate_acs1" in result.columns
        assert len(result) == 2
        assert list(result["unemployment_rate_acs1"]) == [0.05, 0.03]
        assert list(result["geo_id"]) == ["GF01", "GF02"]

    def test_identity_resample_renames_geo_column(self, tmp_path: Path):
        """Identity resample renames metro_id to geo_id."""
        from hhplab.recipe.executor import _resample_identity
        from hhplab.recipe.planner import GeometryRef, ResampleTask

        df = pd.DataFrame(
            {
                "metro_id": ["GF01"],
                "unemployment_rate_acs1": [0.05],
                "year": [2023],
            }
        )

        task = ResampleTask(
            dataset_id="acs1_metro",
            year=2023,
            input_path="dummy.parquet",
            effective_geometry=GeometryRef(type="metro", source="glynn_fox_v1"),
            method="identity",
            transform_id=None,
            to_geometry=GeometryRef(type="metro", source="glynn_fox_v1"),
            measures=["unemployment_rate_acs1"],
            geo_column="metro_id",
        )

        result = _resample_identity(df, task)
        assert "geo_id" in result.columns
        assert "metro_id" not in result.columns


# ===========================================================================
# Adapter validation tests
# ===========================================================================


class TestACS1Adapter:
    """Test the census/acs1 dataset adapter validation."""

    def test_valid_acs1_spec(self):
        from hhplab.recipe.default_dataset_adapters import _validate_census_acs1
        from hhplab.recipe.recipe_schema import DatasetSpec, GeometryRef

        spec = DatasetSpec(
            provider="census",
            product="acs1",
            version=1,
            native_geometry=GeometryRef(type="metro", source="glynn_fox_v1"),
            path="data/acs1.parquet",
        )
        diags = _validate_census_acs1(spec)
        assert len(diags) == 0

    def test_wrong_version(self):
        from hhplab.recipe.default_dataset_adapters import _validate_census_acs1
        from hhplab.recipe.recipe_schema import DatasetSpec, GeometryRef

        spec = DatasetSpec(
            provider="census",
            product="acs1",
            version=2,
            native_geometry=GeometryRef(type="metro", source="glynn_fox_v1"),
            path="data/acs1.parquet",
        )
        diags = _validate_census_acs1(spec)
        errors = [d for d in diags if d.level == "error"]
        assert len(errors) == 1
        assert "version" in errors[0].message

    def test_wrong_native_geometry_without_artifact(self):
        from hhplab.recipe.default_dataset_adapters import _validate_census_acs1
        from hhplab.recipe.recipe_schema import DatasetSpec, GeometryRef

        spec = DatasetSpec(
            provider="census",
            product="acs1",
            version=1,
            native_geometry=GeometryRef(type="tract"),
        )
        diags = _validate_census_acs1(spec)
        errors = [d for d in diags if d.level == "error"]
        assert len(errors) == 1
        assert "metro" in errors[0].message

    def test_wrong_native_geometry_with_artifact(self):
        from hhplab.recipe.default_dataset_adapters import _validate_census_acs1
        from hhplab.recipe.recipe_schema import DatasetSpec, GeometryRef

        spec = DatasetSpec(
            provider="census",
            product="acs1",
            version=1,
            native_geometry=GeometryRef(type="tract"),
            path="data/acs1.parquet",
        )
        diags = _validate_census_acs1(spec)
        # Should NOT error because path is set (pre-materialized artifact)
        errors = [d for d in diags if d.level == "error"]
        assert len(errors) == 0

    def test_unknown_params_warned(self):
        from hhplab.recipe.default_dataset_adapters import _validate_census_acs1
        from hhplab.recipe.recipe_schema import DatasetSpec, GeometryRef

        spec = DatasetSpec(
            provider="census",
            product="acs1",
            version=1,
            native_geometry=GeometryRef(type="metro", source="glynn_fox_v1"),
            path="data/acs1.parquet",
            params={"unknown_param": "value"},
        )
        diags = _validate_census_acs1(spec)
        warnings = [d for d in diags if d.level == "warning"]
        assert len(warnings) == 1
        assert "unknown_param" in warnings[0].message
