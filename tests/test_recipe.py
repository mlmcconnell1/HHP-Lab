"""Tests for recipe loading, adapter registries, executor, and CLI command."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from coclab.cli.main import app
from coclab.cli.recipe import _missing_file_level
from coclab.recipe.adapters import (
    DatasetAdapterRegistry,
    GeometryAdapterRegistry,
    ValidationDiagnostic,
    validate_recipe_adapters,
)
from coclab.recipe.cache import RecipeCache, _sha256_file
from coclab.recipe.executor import (
    ExecutionContext,
    ExecutorError,
    PipelineResult,
    StepResult,
    _apply_temporal_filter,
    _execute_materialize,
    _execute_resample,
    _resolve_transform_path,
    execute_recipe,
)
from coclab.recipe.manifest import (
    AssetRecord,
    RecipeManifest,
    export_bundle,
    read_manifest,
    write_manifest,
)
from coclab.recipe.planner import (
    ExecutionPlan,
    MaterializeTask,
    ResampleTask,
    resolve_plan,
)
from coclab.recipe.loader import RecipeLoadError, load_recipe
from coclab.recipe.recipe_schema import (
    DatasetSpec,
    FileSetSegment,
    FileSetSpec,
    GeometryRef,
    RecipeV1,
    TemporalFilter,
    VintageSetRule,
    VintageSetSpec,
    YearSpec,
    expand_year_spec,
)

runner = CliRunner()


# ---------------------------------------------------------------------------
# Minimal valid recipe dict (reusable fixture)
# ---------------------------------------------------------------------------

def _minimal_recipe() -> dict:
    """Return a minimal valid v1 recipe dict."""
    return {
        "version": 1,
        "name": "test-recipe",
        "universe": {"range": "2020-2022"},
        "targets": [
            {
                "id": "coc_panel",
                "geometry": {"type": "coc", "vintage": 2025},
            }
        ],
        "datasets": {
            "acs": {
                "provider": "census",
                "product": "acs5",
                "version": 1,
                "native_geometry": {"type": "tract", "vintage": 2020},
            }
        },
        "transforms": [
            {
                "id": "tract_to_coc",
                "type": "crosswalk",
                "from": {"type": "tract", "vintage": 2020},
                "to": {"type": "coc", "vintage": 2025},
                "spec": {"weighting": {"scheme": "area"}},
            }
        ],
        "pipelines": [
            {
                "id": "main",
                "target": "coc_panel",
                "steps": [
                    {"kind": "materialize", "transforms": ["tract_to_coc"]},
                    {
                        "kind": "resample",
                        "dataset": "acs",
                        "to_geometry": {"type": "coc", "vintage": 2025},
                        "method": "allocate",
                        "via": "tract_to_coc",
                        "measures": ["total_population"],
                    },
                ],
            }
        ],
    }


# ===========================================================================
# Loader tests
# ===========================================================================


class TestLoadRecipeFromDict:
    """Test load_recipe() with pre-parsed dicts."""

    def test_valid_v1(self):
        recipe = load_recipe(_minimal_recipe())
        assert isinstance(recipe, RecipeV1)
        assert recipe.name == "test-recipe"
        assert recipe.version == 1

    def test_missing_version_key(self):
        data = _minimal_recipe()
        del data["version"]
        with pytest.raises(RecipeLoadError, match="missing required 'version'"):
            load_recipe(data)

    def test_non_integer_version(self):
        data = _minimal_recipe()
        data["version"] = "one"
        with pytest.raises(RecipeLoadError, match="must be an integer"):
            load_recipe(data)

    def test_unsupported_version(self):
        data = _minimal_recipe()
        data["version"] = 99
        with pytest.raises(RecipeLoadError, match="Unsupported recipe version 99"):
            load_recipe(data)

    def test_non_mapping_input(self):
        with pytest.raises(RecipeLoadError, match="must be a YAML mapping"):
            load_recipe([1, 2, 3])  # type: ignore[arg-type]

    def test_schema_violation(self):
        data = _minimal_recipe()
        del data["name"]
        with pytest.raises(RecipeLoadError, match="schema validation failed"):
            load_recipe(data)

    def test_dataset_path_accepts_relative(self):
        data = _minimal_recipe()
        data["datasets"]["acs"]["path"] = "data/curated/measures/coc_measures__2020__2019.parquet"
        data["datasets"]["acs"]["years"] = "2020-2022"
        recipe = load_recipe(data)
        assert (
            recipe.datasets["acs"].path
            == "data/curated/measures/coc_measures__2020__2019.parquet"
        )

    def test_dataset_path_rejects_absolute(self):
        data = _minimal_recipe()
        data["datasets"]["acs"]["path"] = "/tmp/acs.parquet"
        with pytest.raises(RecipeLoadError, match="DatasetSpec.path must be a relative path"):
            load_recipe(data)


class TestLoadRecipeFromFile:
    """Test load_recipe() with YAML file paths."""

    def test_valid_yaml_file(self, tmp_path: Path):
        import yaml

        recipe_file = tmp_path / "recipe.yaml"
        recipe_file.write_text(yaml.dump(_minimal_recipe()), encoding="utf-8")
        recipe = load_recipe(recipe_file)
        assert recipe.name == "test-recipe"

    def test_file_not_found(self, tmp_path: Path):
        missing = tmp_path / "nope.yaml"
        with pytest.raises(RecipeLoadError, match="not found"):
            load_recipe(missing)

    def test_malformed_yaml(self, tmp_path: Path):
        bad = tmp_path / "bad.yaml"
        bad.write_text(":\n  - :\n  bad: [unterminated", encoding="utf-8")
        with pytest.raises(RecipeLoadError, match="Malformed YAML"):
            load_recipe(bad)

    def test_yaml_list_not_mapping(self, tmp_path: Path):
        f = tmp_path / "list.yaml"
        f.write_text("- item1\n- item2\n", encoding="utf-8")
        with pytest.raises(RecipeLoadError, match="must be a YAML mapping"):
            load_recipe(f)


# ===========================================================================
# GeometryAdapterRegistry tests
# ===========================================================================


class TestGeometryAdapterRegistry:

    def test_unregistered_type_returns_error(self):
        reg = GeometryAdapterRegistry()
        ref = GeometryRef(type="alien", vintage=2025)
        diags = reg.validate(ref)
        assert len(diags) == 1
        assert diags[0].level == "error"
        assert "alien" in diags[0].message

    def test_registered_adapter_called(self):
        reg = GeometryAdapterRegistry()
        reg.register("coc", lambda ref: [])
        ref = GeometryRef(type="coc", vintage=2025)
        assert reg.validate(ref) == []

    def test_adapter_returns_diagnostics(self):
        reg = GeometryAdapterRegistry()

        def warn_adapter(ref: GeometryRef) -> list[ValidationDiagnostic]:
            if ref.vintage is None:
                return [ValidationDiagnostic("warning", "Vintage recommended for tract.")]
            return []

        reg.register("tract", warn_adapter)
        assert reg.validate(GeometryRef(type="tract")) == [
            ValidationDiagnostic("warning", "Vintage recommended for tract.")
        ]
        assert reg.validate(GeometryRef(type="tract", vintage=2020)) == []

    def test_registered_types(self):
        reg = GeometryAdapterRegistry()
        reg.register("coc", lambda r: [])
        reg.register("tract", lambda r: [])
        assert reg.registered_types() == ["coc", "tract"]

    def test_reset(self):
        reg = GeometryAdapterRegistry()
        reg.register("coc", lambda r: [])
        reg.reset()
        assert reg.registered_types() == []


# ===========================================================================
# DatasetAdapterRegistry tests
# ===========================================================================


class TestDatasetAdapterRegistry:

    def _make_spec(self, provider: str = "census", product: str = "acs5") -> DatasetSpec:
        return DatasetSpec(
            provider=provider,
            product=product,
            version=1,
            native_geometry=GeometryRef(type="tract", vintage=2020),
        )

    def test_unregistered_returns_error(self):
        reg = DatasetAdapterRegistry()
        diags = reg.validate(self._make_spec())
        assert len(diags) == 1
        assert diags[0].level == "error"
        assert "census" in diags[0].message
        assert "acs5" in diags[0].message

    def test_registered_adapter_called(self):
        reg = DatasetAdapterRegistry()
        reg.register("census", "acs5", lambda s: [])
        assert reg.validate(self._make_spec()) == []

    def test_registered_products(self):
        reg = DatasetAdapterRegistry()
        reg.register("hud", "pit", lambda s: [])
        reg.register("census", "acs5", lambda s: [])
        assert reg.registered_products() == [("census", "acs5"), ("hud", "pit")]

    def test_reset(self):
        reg = DatasetAdapterRegistry()
        reg.register("hud", "pit", lambda s: [])
        reg.reset()
        assert reg.registered_products() == []


# ===========================================================================
# validate_recipe_adapters integration tests
# ===========================================================================


class TestValidateRecipeAdapters:

    def test_all_unregistered_returns_errors(self):
        recipe = load_recipe(_minimal_recipe())
        geo_reg = GeometryAdapterRegistry()
        ds_reg = DatasetAdapterRegistry()
        diags = validate_recipe_adapters(recipe, geo_reg, ds_reg)
        errors = [d for d in diags if d.level == "error"]
        # Should have errors for: target geometry (coc), transform from (tract),
        # transform to (coc), dataset native_geometry (tract), dataset adapter (census/acs5)
        assert len(errors) >= 3

    def test_all_registered_returns_empty(self):
        recipe = load_recipe(_minimal_recipe())
        geo_reg = GeometryAdapterRegistry()
        geo_reg.register("coc", lambda r: [])
        geo_reg.register("tract", lambda r: [])
        ds_reg = DatasetAdapterRegistry()
        ds_reg.register("census", "acs5", lambda s: [])
        diags = validate_recipe_adapters(recipe, geo_reg, ds_reg)
        assert diags == []


# ===========================================================================
# CLI tests
# ===========================================================================


class TestRecipeCLI:

    def test_missing_recipe_file(self, tmp_path: Path):
        result = runner.invoke(app, [
            "build", "recipe",
            "--recipe", str(tmp_path / "missing.yaml"),
        ])
        assert result.exit_code == 2
        assert "not found" in result.output

    def test_valid_recipe_loads(self, tmp_path: Path):
        import yaml

        recipe_file = tmp_path / "recipe.yaml"
        recipe_file.write_text(yaml.dump(_minimal_recipe()), encoding="utf-8")
        result = runner.invoke(app, [
            "build", "recipe",
            "--recipe", str(recipe_file),
        ])
        # Will have adapter errors (no adapters registered) but should load OK
        assert "Loaded recipe: test-recipe" in result.output

    def test_invalid_yaml_exits_2(self, tmp_path: Path):
        bad = tmp_path / "bad.yaml"
        bad.write_text("version: notanint\nname: bad", encoding="utf-8")
        result = runner.invoke(app, [
            "build", "recipe",
            "--recipe", str(bad),
        ])
        assert result.exit_code == 2

    def test_schema_error_exits_2(self, tmp_path: Path):
        import yaml

        data = _minimal_recipe()
        del data["name"]
        f = tmp_path / "noname.yaml"
        f.write_text(yaml.dump(data), encoding="utf-8")
        result = runner.invoke(app, [
            "build", "recipe",
            "--recipe", str(f),
        ])
        assert result.exit_code == 2

    def test_dry_run_succeeds(self, tmp_path: Path):
        import yaml

        recipe_file = tmp_path / "recipe.yaml"
        recipe_file.write_text(yaml.dump(_minimal_recipe()), encoding="utf-8")
        result = runner.invoke(app, [
            "build", "recipe",
            "--recipe", str(recipe_file),
            "--dry-run",
        ])
        assert "Loaded recipe: test-recipe" in result.output

    def test_missing_static_path_reported(self, tmp_path: Path):
        import yaml

        data = _minimal_recipe()
        data["datasets"]["acs"]["path"] = "data/does_not_exist.parquet"
        data["datasets"]["acs"]["years"] = "2020-2022"
        recipe_file = tmp_path / "recipe.yaml"
        recipe_file.write_text(yaml.dump(data), encoding="utf-8")
        result = runner.invoke(app, [
            "build", "recipe",
            "--recipe", str(recipe_file),
        ])
        assert "Missing file" in result.output
        assert "does_not_exist.parquet" in result.output

    def test_missing_file_set_paths_reported(self, tmp_path: Path):
        import yaml

        data = _recipe_with_file_set()
        # Narrow universe so we don't get too many missing-file messages
        data["universe"] = {"range": "2015-2015"}
        recipe_file = tmp_path / "recipe.yaml"
        recipe_file.write_text(yaml.dump(data), encoding="utf-8")
        result = runner.invoke(app, [
            "build", "recipe",
            "--recipe", str(recipe_file),
        ])
        assert "Missing file" in result.output
        assert "acs_2015.parquet" in result.output

    def test_existing_path_no_missing_file_error(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        import yaml

        monkeypatch.chdir(tmp_path)

        # Create the actual file
        data_dir = tmp_path / "data" / "curated"
        data_dir.mkdir(parents=True)
        parquet = data_dir / "acs.parquet"
        parquet.write_bytes(b"fake")

        data = _minimal_recipe()
        data["datasets"]["acs"]["path"] = "data/curated/acs.parquet"
        data["datasets"]["acs"]["years"] = "2020-2022"
        recipe_file = tmp_path / "recipe.yaml"
        recipe_file.write_text(yaml.dump(data), encoding="utf-8")
        result = runner.invoke(app, [
            "build", "recipe",
            "--recipe", str(recipe_file),
        ])
        assert "Missing file" not in result.output

    def test_optional_dataset_missing_warns_not_errors(self, tmp_path: Path):
        import yaml

        data = _minimal_recipe()
        data["datasets"]["acs"]["path"] = "data/missing.parquet"
        data["datasets"]["acs"]["years"] = "2020-2022"
        data["datasets"]["acs"]["optional"] = True
        recipe_file = tmp_path / "recipe.yaml"
        recipe_file.write_text(yaml.dump(data), encoding="utf-8")
        result = runner.invoke(app, [
            "build", "recipe",
            "--recipe", str(recipe_file),
        ])
        # Should appear as warning, not "Missing file" error
        assert "Warning" in result.output
        assert "missing.parquet" in result.output
        assert "Missing file" not in result.output

    def test_policy_default_warn_downgrades_to_warning(self, tmp_path: Path):
        import yaml

        data = _minimal_recipe()
        data["datasets"]["acs"]["path"] = "data/missing.parquet"
        data["datasets"]["acs"]["years"] = "2020-2022"
        data["validation"] = {"missing_dataset": {"default": "warn"}}
        recipe_file = tmp_path / "recipe.yaml"
        recipe_file.write_text(yaml.dump(data), encoding="utf-8")
        result = runner.invoke(app, [
            "build", "recipe",
            "--recipe", str(recipe_file),
        ])
        assert "Warning" in result.output
        assert "missing.parquet" in result.output
        assert "Missing file" not in result.output

    def test_per_dataset_policy_override(self, tmp_path: Path):
        import yaml

        data = _minimal_recipe()
        data["datasets"]["acs"]["path"] = "data/missing.parquet"
        data["datasets"]["acs"]["years"] = "2020-2022"
        # Default is fail, but override acs to warn
        data["validation"] = {"missing_dataset": {"default": "fail", "acs": "warn"}}
        recipe_file = tmp_path / "recipe.yaml"
        recipe_file.write_text(yaml.dump(data), encoding="utf-8")
        result = runner.invoke(app, [
            "build", "recipe",
            "--recipe", str(recipe_file),
        ])
        assert "Warning" in result.output
        assert "Missing file" not in result.output

    def test_per_dataset_policy_fail_overrides_optional(self, tmp_path: Path):
        import yaml

        data = _minimal_recipe()
        data["datasets"]["acs"]["path"] = "data/missing.parquet"
        data["datasets"]["acs"]["years"] = "2020-2022"
        data["datasets"]["acs"]["optional"] = True
        # Policy explicitly says fail for this dataset
        data["validation"] = {"missing_dataset": {"default": "warn", "acs": "fail"}}
        recipe_file = tmp_path / "recipe.yaml"
        recipe_file.write_text(yaml.dump(data), encoding="utf-8")
        result = runner.invoke(app, [
            "build", "recipe",
            "--recipe", str(recipe_file),
        ])
        assert "Missing file" in result.output


# ===========================================================================
# _missing_file_level unit tests
# ===========================================================================


class TestMissingFileLevel:

    def test_default_fail_returns_error(self):
        assert _missing_file_level("ds", False, "fail", {}) == "error"

    def test_default_warn_returns_warning(self):
        assert _missing_file_level("ds", False, "warn", {}) == "warning"

    def test_optional_returns_warning(self):
        assert _missing_file_level("ds", True, "fail", {}) == "warning"

    def test_per_dataset_warn_overrides_default_fail(self):
        assert _missing_file_level("ds", False, "fail", {"ds": "warn"}) == "warning"

    def test_per_dataset_fail_overrides_optional(self):
        assert _missing_file_level("ds", True, "warn", {"ds": "fail"}) == "error"

    def test_per_dataset_takes_precedence_over_default(self):
        assert _missing_file_level("ds", False, "warn", {"ds": "fail"}) == "error"

    def test_unmatched_per_dataset_key_ignored(self):
        assert _missing_file_level("ds", False, "fail", {"other": "warn"}) == "error"


# ===========================================================================
# expand_year_spec tests
# ===========================================================================


class TestExpandYearSpec:

    def test_from_range_string(self):
        assert expand_year_spec("2018-2020") == [2018, 2019, 2020]

    def test_from_year_spec_range(self):
        spec = YearSpec(range="2020-2022")
        assert expand_year_spec(spec) == [2020, 2021, 2022]

    def test_from_year_spec_years(self):
        spec = YearSpec(years=[2022, 2020, 2021])
        assert expand_year_spec(spec) == [2020, 2021, 2022]

    def test_from_list(self):
        assert expand_year_spec([2023, 2021]) == [2021, 2023]


# ===========================================================================
# FileSet schema tests
# ===========================================================================


def _recipe_with_file_set(**overrides) -> dict:
    """Build a recipe dict that includes a dataset with file_set."""
    file_set = {
        "path_template": "data/acs/acs_{year}.parquet",
        "segments": [
            {
                "years": {"range": "2015-2019"},
                "geometry": {"type": "tract", "vintage": 2010, "source": "nhgis"},
            },
            {
                "years": {"range": "2020-2024"},
                "geometry": {"type": "tract", "vintage": 2020, "source": "tiger"},
            },
        ],
    }
    file_set.update(overrides)
    return {
        "version": 1,
        "name": "fileset-test",
        "universe": {"range": "2015-2024"},
        "targets": [
            {"id": "coc_panel", "geometry": {"type": "coc", "vintage": 2025}},
        ],
        "datasets": {
            "acs": {
                "provider": "census",
                "product": "acs",
                "version": 1,
                "native_geometry": {"type": "tract"},
                "file_set": file_set,
            },
        },
        "transforms": [
            {
                "id": "coc_to_tract_2010",
                "type": "crosswalk",
                "from": {"type": "coc", "vintage": 2025},
                "to": {"type": "tract", "vintage": 2010},
                "spec": {"weighting": {"scheme": "area"}},
            },
            {
                "id": "coc_to_tract_2020",
                "type": "crosswalk",
                "from": {"type": "coc", "vintage": 2025},
                "to": {"type": "tract", "vintage": 2020},
                "spec": {"weighting": {"scheme": "area"}},
            },
        ],
        "pipelines": [
            {
                "id": "main",
                "target": "coc_panel",
                "steps": [
                    {
                        "resample": {
                            "dataset": "acs",
                            "to_geometry": {"type": "coc", "vintage": 2025},
                            "method": "aggregate",
                            "via": "auto",
                            "measures": ["total_population"],
                        },
                    },
                    {
                        "join": {
                            "datasets": ["acs"],
                            "join_on": ["geo_id", "year"],
                        },
                    },
                ],
            },
        ],
    }


class TestFileSetSchema:

    def test_valid_file_set_parses(self):
        recipe = load_recipe(_recipe_with_file_set())
        assert recipe.datasets["acs"].file_set is not None
        assert len(recipe.datasets["acs"].file_set.segments) == 2

    def test_path_template_without_year_placeholder_allowed(self):
        data = _recipe_with_file_set()
        data["datasets"]["acs"]["file_set"]["path_template"] = (
            "data/curated/measures/measures__A{acs_end}@B{boundary}xT{tract}.parquet"
        )
        data["datasets"]["acs"]["file_set"]["segments"][0]["constants"] = {"tract": 2010}
        data["datasets"]["acs"]["file_set"]["segments"][0]["year_offsets"] = {
            "acs_end": -1,
            "boundary": 0,
        }
        data["datasets"]["acs"]["file_set"]["segments"][1]["constants"] = {"tract": 2020}
        data["datasets"]["acs"]["file_set"]["segments"][1]["year_offsets"] = {
            "acs_end": -1,
            "boundary": 0,
        }
        recipe = load_recipe(data)
        assert recipe.datasets["acs"].file_set.path_template.startswith(
            "data/curated/measures/measures__A"
        )

    def test_segment_overlap_detected(self):
        data = _recipe_with_file_set()
        # Make second segment overlap with first (2019 is in both)
        data["datasets"]["acs"]["file_set"]["segments"][1]["years"] = {"range": "2019-2024"}
        with pytest.raises(RecipeLoadError, match="overlap on years.*2019"):
            load_recipe(data)

    def test_override_outside_segment_rejected(self):
        data = _recipe_with_file_set()
        data["datasets"]["acs"]["file_set"]["segments"][0]["overrides"] = {
            2020: "data/acs/special.parquet",
        }
        with pytest.raises(RecipeLoadError, match="override for year 2020.*not in segment"):
            load_recipe(data)

    def test_segment_geometry_type_mismatch_rejected(self):
        data = _recipe_with_file_set()
        # Change one segment's geometry type to something other than tract
        data["datasets"]["acs"]["file_set"]["segments"][0]["geometry"]["type"] = "county"
        with pytest.raises(RecipeLoadError, match="geometry type 'county' does not match.*'tract'"):
            load_recipe(data)

    def test_override_within_segment_accepted(self):
        data = _recipe_with_file_set()
        data["datasets"]["acs"]["file_set"]["segments"][0]["overrides"] = {
            2017: "data/acs/special_2017.parquet",
        }
        recipe = load_recipe(data)
        seg = recipe.datasets["acs"].file_set.segments[0]
        assert seg.overrides[2017] == "data/acs/special_2017.parquet"

    def test_missing_template_variables_rejected(self):
        data = _recipe_with_file_set()
        data["datasets"]["acs"]["file_set"]["path_template"] = (
            "data/curated/measures/measures__A{acs_end}@B{boundary}xT{tract}.parquet"
        )
        data["datasets"]["acs"]["file_set"]["segments"][0]["constants"] = {"tract": 2010}
        data["datasets"]["acs"]["file_set"]["segments"][0]["year_offsets"] = {"acs_end": -1}
        data["datasets"]["acs"]["file_set"]["segments"][1]["constants"] = {"tract": 2020}
        data["datasets"]["acs"]["file_set"]["segments"][1]["year_offsets"] = {"acs_end": -1}
        with pytest.raises(RecipeLoadError, match="path_template requires variables"):
            load_recipe(data)

    def test_duplicate_segment_variable_keys_rejected(self):
        data = _recipe_with_file_set()
        data["datasets"]["acs"]["file_set"]["segments"][0]["constants"] = {"acs_end": 2018}
        data["datasets"]["acs"]["file_set"]["segments"][0]["year_offsets"] = {"acs_end": -1}
        with pytest.raises(RecipeLoadError, match="both constants and year_offsets"):
            load_recipe(data)


# ===========================================================================
# Static-path multi-year validation
# ===========================================================================


class TestStaticPathMultiYear:

    def test_static_path_no_years_multi_year_universe_rejected(self):
        data = _minimal_recipe()
        data["datasets"]["acs"]["path"] = "data/acs.parquet"
        with pytest.raises(RecipeLoadError, match="does not declare.*years"):
            load_recipe(data)

    def test_static_path_with_years_multi_year_universe_accepted(self):
        data = _minimal_recipe()
        data["datasets"]["acs"]["path"] = "data/acs.parquet"
        data["datasets"]["acs"]["years"] = "2020-2022"
        recipe = load_recipe(data)
        assert recipe.datasets["acs"].path == "data/acs.parquet"

    def test_static_path_no_years_single_year_universe_accepted(self):
        data = _minimal_recipe()
        data["universe"] = {"years": [2020]}
        data["datasets"]["acs"]["path"] = "data/acs.parquet"
        recipe = load_recipe(data)
        assert recipe.datasets["acs"].path == "data/acs.parquet"

    def test_no_path_no_years_multi_year_universe_accepted(self):
        """Dataset without path (e.g. dynamically loaded) should not trigger the check."""
        data = _minimal_recipe()
        # No path set — should be fine even without years
        recipe = load_recipe(data)
        assert recipe.datasets["acs"].path is None


# ===========================================================================
# via:auto schema tests
# ===========================================================================


class TestViaAuto:

    def test_via_auto_accepted_for_aggregate(self):
        data = _recipe_with_file_set()
        recipe = load_recipe(data)
        # The resample step should have via="auto"
        step = recipe.pipelines[0].steps[0]
        assert step.via == "auto"

    def test_via_auto_accepted_for_allocate(self):
        data = _recipe_with_file_set()
        data["pipelines"][0]["steps"][0]["resample"]["method"] = "allocate"
        recipe = load_recipe(data)
        step = recipe.pipelines[0].steps[0]
        assert step.via == "auto"

    def test_via_auto_not_in_transform_id_check(self):
        """via='auto' should not be checked against transform ids."""
        data = _minimal_recipe()
        data["pipelines"][0]["steps"][1]["via"] = "auto"
        data["pipelines"][0]["steps"][1]["method"] = "aggregate"
        recipe = load_recipe(data)
        assert recipe.pipelines[0].steps[1].via == "auto"


# ===========================================================================
# join_on schema tests
# ===========================================================================


class TestJoinOn:

    def test_join_on_parses(self):
        data = _recipe_with_file_set()
        recipe = load_recipe(data)
        join_step = recipe.pipelines[0].steps[1]
        assert join_step.join_on == ["geo_id", "year"]

    def test_join_on_default(self):
        data = _minimal_recipe()
        data["pipelines"][0]["steps"].append(
            {"kind": "join", "datasets": ["acs"]}
        )
        recipe = load_recipe(data)
        join_step = recipe.pipelines[0].steps[-1]
        assert join_step.join_on == ["geo_id", "year"]


# ===========================================================================
# DatasetSpec.years tests
# ===========================================================================


class TestDatasetYears:

    def test_years_range_string_accepted(self):
        data = _minimal_recipe()
        data["datasets"]["acs"]["years"] = "2020-2022"
        recipe = load_recipe(data)
        assert recipe.datasets["acs"].years is not None
        assert recipe.datasets["acs"].years.range == "2020-2022"

    def test_years_spec_accepted(self):
        data = _minimal_recipe()
        data["datasets"]["acs"]["years"] = {"years": [2020, 2021]}
        recipe = load_recipe(data)
        assert recipe.datasets["acs"].years.years == [2020, 2021]

    def test_years_defaults_to_none(self):
        recipe = load_recipe(_minimal_recipe())
        assert recipe.datasets["acs"].years is None


# ===========================================================================
# YearSpec bare string coercion tests
# ===========================================================================


class TestYearSpecCoercion:

    def test_bare_string_coerced_in_universe(self):
        data = _minimal_recipe()
        data["universe"] = "2020-2022"
        recipe = load_recipe(data)
        assert recipe.universe.range == "2020-2022"

    def test_bare_string_coerced_in_file_set_segment(self):
        data = _recipe_with_file_set()
        # segments already use {"range": ...} — switch to bare strings
        data["datasets"]["acs"]["file_set"]["segments"][0]["years"] = "2015-2019"
        data["datasets"]["acs"]["file_set"]["segments"][1]["years"] = "2020-2024"
        recipe = load_recipe(data)
        assert recipe.datasets["acs"].file_set.segments[0].years.range == "2015-2019"


# ===========================================================================
# Optional transforms/pipelines tests
# ===========================================================================


class TestOptionalTransformsPipelines:

    def test_missing_transforms_defaults_empty(self):
        data = _minimal_recipe()
        del data["transforms"]
        del data["pipelines"]
        recipe = load_recipe(data)
        assert recipe.transforms == []
        assert recipe.pipelines == []

    def test_empty_transforms_accepted(self):
        data = _minimal_recipe()
        data["transforms"] = []
        data["pipelines"] = []
        recipe = load_recipe(data)
        assert recipe.transforms == []


# ===========================================================================
# vintage_sets schema tests
# ===========================================================================


class TestVintageSetsSchema:

    def test_vintage_sets_defaults_empty(self):
        """Existing recipes without vintage_sets still parse."""
        recipe = load_recipe(_minimal_recipe())
        assert recipe.vintage_sets == {}

    def test_valid_vintage_set_parses(self):
        data = _minimal_recipe()
        data["vintage_sets"] = {
            "acs_measures": {
                "dimensions": ["analysis_year", "acs_end", "boundary", "tract"],
                "rules": [
                    {
                        "years": "2015-2019",
                        "constants": {"tract": 2010},
                        "year_offsets": {"analysis_year": 0, "acs_end": -1, "boundary": 0},
                    },
                    {
                        "years": "2020-2024",
                        "constants": {"tract": 2020},
                        "year_offsets": {"analysis_year": 0, "acs_end": -1, "boundary": 0},
                    },
                ],
            }
        }
        recipe = load_recipe(data)
        assert "acs_measures" in recipe.vintage_sets
        vs = recipe.vintage_sets["acs_measures"]
        assert vs.dimensions == ["analysis_year", "acs_end", "boundary", "tract"]
        assert len(vs.rules) == 2

    def test_vintage_set_year_overlap_rejected(self):
        data = _minimal_recipe()
        data["vintage_sets"] = {
            "test": {
                "dimensions": ["d"],
                "rules": [
                    {"years": "2015-2020", "year_offsets": {"d": 0}},
                    {"years": "2019-2024", "year_offsets": {"d": 0}},
                ],
            }
        }
        with pytest.raises(RecipeLoadError, match="overlap"):
            load_recipe(data)

    def test_vintage_set_missing_dimension_rejected(self):
        data = _minimal_recipe()
        data["vintage_sets"] = {
            "test": {
                "dimensions": ["a", "b", "c"],
                "rules": [
                    {"years": "2015-2019", "year_offsets": {"a": 0, "b": -1}},
                ],
            }
        }
        with pytest.raises(RecipeLoadError, match="dimension"):
            load_recipe(data)

    def test_vintage_set_duplicate_key_rejected(self):
        data = _minimal_recipe()
        data["vintage_sets"] = {
            "test": {
                "dimensions": ["d"],
                "rules": [
                    {"years": "2015-2019", "constants": {"d": 2010}, "year_offsets": {"d": 0}},
                ],
            }
        }
        with pytest.raises(RecipeLoadError, match="both constants and year_offsets"):
            load_recipe(data)

    def test_vintage_set_bare_string_years_coerced(self):
        data = _minimal_recipe()
        data["vintage_sets"] = {
            "test": {
                "dimensions": ["d"],
                "rules": [
                    {"years": "2020-2024", "year_offsets": {"d": 0}},
                ],
            }
        }
        recipe = load_recipe(data)
        assert recipe.vintage_sets["test"].rules[0].years.range == "2020-2024"

    def test_multiple_vintage_sets(self):
        data = _minimal_recipe()
        data["vintage_sets"] = {
            "set_a": {
                "dimensions": ["x"],
                "rules": [{"years": "2020-2024", "year_offsets": {"x": 0}}],
            },
            "set_b": {
                "dimensions": ["y", "z"],
                "rules": [
                    {"years": "2020-2022", "year_offsets": {"y": 0}, "constants": {"z": 1}},
                    {"years": "2023-2024", "year_offsets": {"y": 0}, "constants": {"z": 2}},
                ],
            },
        }
        recipe = load_recipe(data)
        assert len(recipe.vintage_sets) == 2


# ---------------------------------------------------------------------------
# Default adapter registration tests
# ---------------------------------------------------------------------------


class TestDefaultAdapters:
    """Tests for built-in adapter registration."""

    def test_register_defaults_idempotent(self):
        from coclab.recipe.default_adapters import register_defaults
        from coclab.recipe.adapters import geometry_registry, dataset_registry

        geometry_registry.reset()
        dataset_registry.reset()
        register_defaults()
        types_1 = geometry_registry.registered_types()
        products_1 = dataset_registry.registered_products()
        register_defaults()
        assert geometry_registry.registered_types() == types_1
        assert dataset_registry.registered_products() == products_1

    def test_geometry_types_registered(self):
        from coclab.recipe.default_adapters import register_defaults
        from coclab.recipe.adapters import geometry_registry

        geometry_registry.reset()
        register_defaults()
        assert "coc" in geometry_registry.registered_types()
        assert "tract" in geometry_registry.registered_types()
        assert "county" in geometry_registry.registered_types()

    def test_dataset_products_registered(self):
        from coclab.recipe.default_adapters import register_defaults
        from coclab.recipe.adapters import dataset_registry

        dataset_registry.reset()
        register_defaults()
        products = dataset_registry.registered_products()
        assert ("hud", "pit") in products
        assert ("census", "acs5") in products
        assert ("census", "acs") in products
        assert ("zillow", "zori") in products

    def test_coc_valid(self):
        from coclab.recipe.default_geometry_adapters import _validate_coc

        diags = _validate_coc(GeometryRef(type="coc", vintage=2025, source="hud_exchange"))
        assert diags == []

    def test_coc_no_vintage_valid(self):
        from coclab.recipe.default_geometry_adapters import _validate_coc

        diags = _validate_coc(GeometryRef(type="coc"))
        assert diags == []

    def test_coc_early_vintage_warns(self):
        from coclab.recipe.default_geometry_adapters import _validate_coc

        diags = _validate_coc(GeometryRef(type="coc", vintage=1990))
        assert len(diags) == 1
        assert diags[0].level == "warning"

    def test_tract_decennial_valid(self):
        from coclab.recipe.default_geometry_adapters import _validate_tract

        diags = _validate_tract(GeometryRef(type="tract", vintage=2020))
        assert diags == []

    def test_tract_non_decennial_warns(self):
        from coclab.recipe.default_geometry_adapters import _validate_tract

        diags = _validate_tract(GeometryRef(type="tract", vintage=2023))
        assert len(diags) == 1
        assert diags[0].level == "warning"
        assert "decennial" in diags[0].message

    def test_hud_pit_valid(self):
        from coclab.recipe.default_dataset_adapters import _validate_hud_pit

        spec = DatasetSpec(
            provider="hud", product="pit", version=1,
            native_geometry=GeometryRef(type="coc"),
            params={"vintage": 2024, "align": "point_in_time_jan"},
        )
        diags = _validate_hud_pit(spec)
        assert diags == []

    def test_hud_pit_bad_version(self):
        from coclab.recipe.default_dataset_adapters import _validate_hud_pit

        spec = DatasetSpec(
            provider="hud", product="pit", version=2,
            native_geometry=GeometryRef(type="coc"),
        )
        diags = _validate_hud_pit(spec)
        assert any(d.level == "error" and "version" in d.message for d in diags)

    def test_hud_pit_wrong_geometry(self):
        from coclab.recipe.default_dataset_adapters import _validate_hud_pit

        spec = DatasetSpec(
            provider="hud", product="pit", version=1,
            native_geometry=GeometryRef(type="tract"),
        )
        diags = _validate_hud_pit(spec)
        assert any(d.level == "error" and "coc" in d.message for d in diags)

    def test_hud_pit_unknown_params_warns(self):
        from coclab.recipe.default_dataset_adapters import _validate_hud_pit

        spec = DatasetSpec(
            provider="hud", product="pit", version=1,
            native_geometry=GeometryRef(type="coc"),
            params={"vintage": 2024, "unknown_param": True},
        )
        diags = _validate_hud_pit(spec)
        assert any(d.level == "warning" and "unrecognized" in d.message for d in diags)

    def test_census_acs5_valid(self):
        from coclab.recipe.default_dataset_adapters import _validate_census_acs5

        spec = DatasetSpec(
            provider="census", product="acs5", version=1,
            native_geometry=GeometryRef(type="tract", vintage=2020),
        )
        assert _validate_census_acs5(spec) == []

    def test_census_acs_valid(self):
        from coclab.recipe.default_dataset_adapters import _validate_census_acs

        spec = DatasetSpec(
            provider="census", product="acs", version=1,
            native_geometry=GeometryRef(type="tract"),
        )
        assert _validate_census_acs(spec) == []

    def test_zillow_zori_valid(self):
        from coclab.recipe.default_dataset_adapters import _validate_zillow_zori

        spec = DatasetSpec(
            provider="zillow", product="zori", version=1,
            native_geometry=GeometryRef(type="county"),
        )
        assert _validate_zillow_zori(spec) == []

    def test_zillow_zori_wrong_geometry_warns(self):
        from coclab.recipe.default_dataset_adapters import _validate_zillow_zori

        spec = DatasetSpec(
            provider="zillow", product="zori", version=1,
            native_geometry=GeometryRef(type="zip"),
        )
        diags = _validate_zillow_zori(spec)
        assert any(d.level == "warning" and "county" in d.message for d in diags)

    def test_recipe_integration_no_adapter_errors(self):
        """Full recipe validation with defaults registered produces no errors."""
        from coclab.recipe.default_adapters import register_defaults
        from coclab.recipe.adapters import geometry_registry, dataset_registry

        geometry_registry.reset()
        dataset_registry.reset()
        register_defaults()

        recipe = load_recipe({
            "version": 1,
            "name": "test",
            "universe": {"range": "2020-2022"},
            "targets": [{"id": "t", "geometry": {"type": "coc", "vintage": 2025}}],
            "datasets": {
                "pit": {
                    "provider": "hud", "product": "pit", "version": 1,
                    "native_geometry": {"type": "coc"},
                    "params": {"vintage": 2024, "align": "point_in_time_jan"},
                },
            },
        })
        diags = validate_recipe_adapters(recipe, geometry_registry, dataset_registry)
        errors = [d for d in diags if d.level == "error"]
        assert errors == [], f"Unexpected errors: {[e.message for e in errors]}"


# ---------------------------------------------------------------------------
# Check dataset paths (file_set template expansion) tests
# ---------------------------------------------------------------------------


class TestCheckDatasetPathsFileSet:
    """Tests for _check_dataset_paths with file_set templates."""

    def test_file_set_template_variables_expanded(self, tmp_path):
        """file_set path template should expand segment constants and year_offsets."""
        from coclab.cli.recipe import _check_dataset_paths

        recipe = load_recipe({
            "version": 1,
            "name": "test",
            "universe": {"range": "2020-2021"},
            "targets": [{"id": "t", "geometry": {"type": "coc"}}],
            "datasets": {
                "acs": {
                    "provider": "census",
                    "product": "acs",
                    "version": 1,
                    "native_geometry": {"type": "tract"},
                    "file_set": {
                        "path_template": "data/acs__A{acs_end}xT{tract}.parquet",
                        "segments": [{
                            "years": "2020-2021",
                            "geometry": {"type": "tract", "vintage": 2020},
                            "constants": {"tract": 2020},
                            "year_offsets": {"acs_end": -1},
                        }],
                    },
                },
            },
        })

        # Create the expected files
        for year in [2020, 2021]:
            acs_end = year - 1
            p = tmp_path / f"data/acs__A{acs_end}xT2020.parquet"
            p.parent.mkdir(parents=True, exist_ok=True)
            p.touch()

        diags = _check_dataset_paths(recipe, project_root=tmp_path)
        assert diags == [], f"Expected no diagnostics, got: {[d.message for d in diags]}"

    def test_file_set_template_missing_file_reported(self, tmp_path):
        """Missing file with correct template expansion should be reported."""
        from coclab.cli.recipe import _check_dataset_paths

        recipe = load_recipe({
            "version": 1,
            "name": "test",
            "universe": {"range": "2020-2020"},
            "targets": [{"id": "t", "geometry": {"type": "coc"}}],
            "datasets": {
                "acs": {
                    "provider": "census",
                    "product": "acs",
                    "version": 1,
                    "native_geometry": {"type": "tract"},
                    "file_set": {
                        "path_template": "data/acs__A{acs_end}xT{tract}.parquet",
                        "segments": [{
                            "years": "2020-2020",
                            "geometry": {"type": "tract", "vintage": 2020},
                            "constants": {"tract": 2020},
                            "year_offsets": {"acs_end": -1},
                        }],
                    },
                },
            },
        })

        # Don't create the file - should report missing
        diags = _check_dataset_paths(recipe, project_root=tmp_path)
        assert len(diags) == 1
        assert "A2019xT2020" in diags[0].message


# ---------------------------------------------------------------------------
# Vintage set expansion tests
# ---------------------------------------------------------------------------


class TestExpandVintageSet:
    """Tests for vintage set tuple expansion."""

    def test_basic_expansion(self):
        from coclab.recipe.planner import expand_vintage_set

        spec = VintageSetSpec(
            dimensions=["analysis_year", "acs_end", "tract"],
            rules=[
                VintageSetRule(
                    years=YearSpec(range="2015-2017"),
                    constants={"tract": 2010},
                    year_offsets={"analysis_year": 0, "acs_end": -1},
                ),
                VintageSetRule(
                    years=YearSpec(range="2020-2022"),
                    constants={"tract": 2020},
                    year_offsets={"analysis_year": 0, "acs_end": -1},
                ),
            ],
        )
        result = expand_vintage_set(spec)
        assert result[2015] == {"analysis_year": 2015, "acs_end": 2014, "tract": 2010}
        assert result[2017] == {"analysis_year": 2017, "acs_end": 2016, "tract": 2010}
        assert result[2020] == {"analysis_year": 2020, "acs_end": 2019, "tract": 2020}
        assert result[2022] == {"analysis_year": 2022, "acs_end": 2021, "tract": 2020}
        assert 2018 not in result

    def test_overlapping_rules_raises(self):
        from coclab.recipe.planner import PlannerError, expand_vintage_set

        spec = VintageSetSpec(
            dimensions=["x"],
            rules=[
                VintageSetRule(
                    years=YearSpec(range="2015-2020"),
                    year_offsets={"x": 0},
                ),
                VintageSetRule(
                    years=YearSpec(range="2019-2024"),
                    year_offsets={"x": 0},
                ),
            ],
        )
        with pytest.raises(PlannerError, match="overlapping"):
            expand_vintage_set(spec)

    def test_missing_dimension_raises(self):
        from coclab.recipe.planner import PlannerError, expand_vintage_set

        spec = VintageSetSpec(
            dimensions=["x", "y"],
            rules=[
                VintageSetRule(
                    years=YearSpec(range="2020-2020"),
                    year_offsets={"x": 0},
                ),
            ],
        )
        with pytest.raises(PlannerError, match="dimensions"):
            expand_vintage_set(spec)

    def test_single_tuple_resolution(self):
        from coclab.recipe.planner import resolve_vintage_tuple

        recipe_data = _minimal_recipe()
        recipe_data["vintage_sets"] = {
            "test_vs": {
                "dimensions": ["boundary", "tract"],
                "rules": [{
                    "years": "2020-2022",
                    "year_offsets": {"boundary": 0},
                    "constants": {"tract": 2020},
                }],
            },
        }
        recipe = load_recipe(recipe_data)
        result = resolve_vintage_tuple("test_vs", 2021, recipe)
        assert result == {"boundary": 2021, "tract": 2020}

    def test_resolve_missing_vintage_set(self):
        from coclab.recipe.planner import PlannerError, resolve_vintage_tuple

        recipe = load_recipe(_minimal_recipe())
        with pytest.raises(PlannerError, match="not found"):
            resolve_vintage_tuple("nonexistent", 2020, recipe)

    def test_resolve_uncovered_year(self):
        from coclab.recipe.planner import PlannerError, resolve_vintage_tuple

        recipe_data = _minimal_recipe()
        recipe_data["vintage_sets"] = {
            "test_vs": {
                "dimensions": ["x"],
                "rules": [{"years": "2020-2022", "year_offsets": {"x": 0}}],
            },
        }
        recipe = load_recipe(recipe_data)
        with pytest.raises(PlannerError, match="no rule covering"):
            resolve_vintage_tuple("test_vs", 2025, recipe)


# ===========================================================================
# Executor unit tests
# ===========================================================================


def _recipe_with_pipeline() -> dict:
    """Build a recipe with a full pipeline (materialize, resample, join)."""
    return {
        "version": 1,
        "name": "executor-test",
        "universe": {"range": "2020-2021"},
        "targets": [
            {"id": "coc_panel", "geometry": {"type": "coc", "vintage": 2025}},
        ],
        "datasets": {
            "pit": {
                "provider": "hud",
                "product": "pit",
                "version": 1,
                "native_geometry": {"type": "coc"},
                "years": "2020-2021",
            },
            "acs": {
                "provider": "census",
                "product": "acs5",
                "version": 1,
                "native_geometry": {"type": "tract", "vintage": 2020},
                "years": "2020-2021",
            },
        },
        "transforms": [
            {
                "id": "tract_to_coc",
                "type": "crosswalk",
                "from": {"type": "tract", "vintage": 2020},
                "to": {"type": "coc", "vintage": 2025},
                "spec": {"weighting": {"scheme": "area"}},
            },
        ],
        "pipelines": [
            {
                "id": "main",
                "target": "coc_panel",
                "steps": [
                    {"materialize": {"transforms": ["tract_to_coc"]}},
                    {
                        "resample": {
                            "dataset": "pit",
                            "to_geometry": {"type": "coc", "vintage": 2025},
                            "method": "identity",
                            "measures": ["pit_total"],
                        },
                    },
                    {
                        "resample": {
                            "dataset": "acs",
                            "to_geometry": {"type": "coc", "vintage": 2025},
                            "method": "aggregate",
                            "via": "tract_to_coc",
                            "measures": ["total_population"],
                            "aggregation": "sum",
                        },
                    },
                    {
                        "join": {
                            "datasets": ["pit", "acs"],
                            "join_on": ["geo_id", "year"],
                        },
                    },
                ],
            },
        ],
    }


def _setup_pipeline_fixtures(tmp_path: Path) -> None:
    """Create the crosswalk + dataset files needed by _recipe_with_pipeline."""
    # Crosswalk
    xwalk_dir = tmp_path / "data" / "curated" / "xwalks"
    xwalk_dir.mkdir(parents=True)
    xwalk = pd.DataFrame({
        "coc_id": ["COC1", "COC2"],
        "tract_geoid": ["T1", "T2"],
        "area_share": [1.0, 1.0],
    })
    xwalk.to_parquet(xwalk_dir / "xwalk__B2025xT2020.parquet")

    # PIT dataset (identity passthrough) — includes both universe years
    pit_path = tmp_path / "data" / "pit.parquet"
    pit_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({
        "coc_id": ["COC1", "COC2", "COC1", "COC2"],
        "year": [2020, 2020, 2021, 2021],
        "pit_total": [10, 20, 11, 21],
    }).to_parquet(pit_path)

    # ACS dataset (aggregate) — includes both universe years
    acs_path = tmp_path / "data" / "acs.parquet"
    pd.DataFrame({
        "GEOID": ["T1", "T2", "T1", "T2"],
        "year": [2020, 2020, 2021, 2021],
        "total_population": [100, 200, 110, 210],
    }).to_parquet(acs_path)


class TestExecutor:

    def test_execute_recipe_returns_results(self, tmp_path: Path):
        _setup_pipeline_fixtures(tmp_path)
        data = _recipe_with_pipeline()
        # Use the fixture dataset paths
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        data["datasets"]["acs"]["path"] = "data/acs.parquet"
        recipe = load_recipe(data)
        results = execute_recipe(recipe, project_root=tmp_path)
        assert len(results) == 1
        assert results[0].pipeline_id == "main"
        assert results[0].success

    def test_execute_recipe_runs_all_step_types(self, tmp_path: Path):
        _setup_pipeline_fixtures(tmp_path)
        data = _recipe_with_pipeline()
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        data["datasets"]["acs"]["path"] = "data/acs.parquet"
        recipe = load_recipe(data)
        results = execute_recipe(recipe, project_root=tmp_path)
        kinds = [s.step_kind for s in results[0].steps]
        assert "materialize" in kinds
        assert "resample" in kinds
        assert "join" in kinds

    def test_execute_recipe_step_count(self, tmp_path: Path):
        """1 materialize + 2×2 resample + 2 join + 1 persist = 8 steps."""
        _setup_pipeline_fixtures(tmp_path)
        data = _recipe_with_pipeline()
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        data["datasets"]["acs"]["path"] = "data/acs.parquet"
        recipe = load_recipe(data)
        results = execute_recipe(recipe, project_root=tmp_path)
        assert len(results[0].steps) == 8

    def test_execute_recipe_rejects_implicit_static_broadcast(self, tmp_path: Path):
        """A yearless dataset should not be silently reused across many years."""
        _setup_pipeline_fixtures(tmp_path)
        acs_path = tmp_path / "data" / "acs.parquet"
        pd.DataFrame({
            "GEOID": ["T1", "T2"],
            "total_population": [100, 200],
        }).to_parquet(acs_path)

        data = _recipe_with_pipeline()
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        data["datasets"]["acs"]["path"] = "data/acs.parquet"
        recipe = load_recipe(data)

        with pytest.raises(
            ExecutorError,
            match="broadcast a static snapshot across time",
        ):
            execute_recipe(recipe, project_root=tmp_path)

    def test_execute_recipe_allows_explicit_static_broadcast(self, tmp_path: Path):
        """Explicit broadcast opt-in should preserve current passthrough behavior."""
        _setup_pipeline_fixtures(tmp_path)
        acs_path = tmp_path / "data" / "acs.parquet"
        pd.DataFrame({
            "GEOID": ["T1", "T2"],
            "total_population": [100, 200],
        }).to_parquet(acs_path)

        data = _recipe_with_pipeline()
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        data["datasets"]["acs"]["path"] = "data/acs.parquet"
        data["datasets"]["acs"]["params"] = {"broadcast_static": True}
        recipe = load_recipe(data)

        results = execute_recipe(recipe, project_root=tmp_path)
        assert results[0].success

        panel_path = (
            tmp_path
            / "data"
            / "curated"
            / "panel"
            / "panel__Y2020-2021@B2025.parquet"
        )
        panel = pd.read_parquet(panel_path).sort_values(["geo_id", "year"])
        assert list(panel["total_population"]) == [100.0, 100.0, 200.0, 200.0]

    def test_execute_recipe_allows_yearless_file_set_with_distinct_paths(
        self, tmp_path: Path,
    ):
        """Year-specific files should not require a row-level year column."""
        _setup_pipeline_fixtures(tmp_path)
        data = _recipe_with_pipeline()
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        data["datasets"]["acs"].pop("path", None)
        data["datasets"]["acs"]["file_set"] = {
            "path_template": "data/acs_{year}.parquet",
            "segments": [
                {
                    "years": {"range": "2020-2021"},
                    "geometry": {"type": "tract", "vintage": 2020},
                }
            ],
        }

        pd.DataFrame({
            "GEOID": ["T1", "T2"],
            "total_population": [100, 200],
        }).to_parquet(tmp_path / "data" / "acs_2020.parquet")
        pd.DataFrame({
            "GEOID": ["T1", "T2"],
            "total_population": [110, 210],
        }).to_parquet(tmp_path / "data" / "acs_2021.parquet")

        recipe = load_recipe(data)
        results = execute_recipe(recipe, project_root=tmp_path)
        assert results[0].success

        panel_path = (
            tmp_path
            / "data"
            / "curated"
            / "panel"
            / "panel__Y2020-2021@B2025.parquet"
        )
        panel = pd.read_parquet(panel_path).sort_values(["geo_id", "year"])
        assert list(panel["total_population"]) == [100.0, 110.0, 200.0, 210.0]

    def test_execute_recipe_rejects_file_set_reusing_same_static_path(
        self, tmp_path: Path,
    ):
        """file_set should still fail when all years resolve to one static file."""
        _setup_pipeline_fixtures(tmp_path)
        static_path = tmp_path / "data" / "acs_static.parquet"
        pd.DataFrame({
            "GEOID": ["T1", "T2"],
            "total_population": [100, 200],
        }).to_parquet(static_path)

        data = _recipe_with_pipeline()
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        data["datasets"]["acs"].pop("path", None)
        data["datasets"]["acs"]["file_set"] = {
            "path_template": "data/acs__A{acs_vintage}.parquet",
            "segments": [
                {
                    "years": {"range": "2020-2021"},
                    "geometry": {"type": "tract", "vintage": 2020},
                    "constants": {"acs_vintage": 2024},
                }
            ],
        }
        static_path.rename(tmp_path / "data" / "acs__A2024.parquet")

        recipe = load_recipe(data)
        with pytest.raises(
            ExecutorError,
            match="broadcast a static snapshot across time",
        ):
            execute_recipe(recipe, project_root=tmp_path)

    def test_execute_recipe_no_pipelines(self, tmp_path: Path):
        data = _minimal_recipe()
        data["pipelines"] = []
        recipe = load_recipe(data)
        results = execute_recipe(recipe, project_root=tmp_path)
        assert results == []

    def test_planner_error_wrapped_in_executor_error(self, tmp_path: Path):
        """Planner failures should be raised as ExecutorError with pipeline context."""
        data = _recipe_with_pipeline()
        # Add a resample with via:auto but no matching transform
        data["pipelines"][0]["steps"].insert(1, {
            "resample": {
                "dataset": "acs",
                "to_geometry": {"type": "county"},
                "method": "aggregate",
                "via": "auto",
                "measures": ["total_population"],
            },
        })
        recipe = load_recipe(data)
        with pytest.raises(ExecutorError, match="Pipeline 'main'.*planning failed"):
            execute_recipe(recipe, project_root=tmp_path)


class TestTargetOutputsEnforcement:
    """Regression tests for coclab-xndr: target.outputs enforcement."""

    def test_panel_output_persisted_by_default(self, tmp_path: Path):
        """Default target outputs=['panel'] should produce a persist step."""
        _setup_pipeline_fixtures(tmp_path)
        data = _recipe_with_pipeline()
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        data["datasets"]["acs"]["path"] = "data/acs.parquet"
        # Default: outputs=["panel"]
        recipe = load_recipe(data)
        results = execute_recipe(recipe, project_root=tmp_path)
        kinds = [s.step_kind for s in results[0].steps]
        assert "persist" in kinds

    def test_empty_outputs_skips_persist(self, tmp_path: Path):
        """targets with outputs=[] should not produce a persist step."""
        _setup_pipeline_fixtures(tmp_path)
        data = _recipe_with_pipeline()
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        data["datasets"]["acs"]["path"] = "data/acs.parquet"
        data["targets"][0]["outputs"] = []
        recipe = load_recipe(data)
        results = execute_recipe(recipe, project_root=tmp_path)
        kinds = [s.step_kind for s in results[0].steps]
        assert "persist" not in kinds

    def test_diagnostics_only_skips_panel_persist(self, tmp_path: Path):
        """targets with outputs=['diagnostics'] should not persist a panel."""
        _setup_pipeline_fixtures(tmp_path)
        data = _recipe_with_pipeline()
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        data["datasets"]["acs"]["path"] = "data/acs.parquet"
        data["targets"][0]["outputs"] = ["diagnostics"]
        recipe = load_recipe(data)
        with pytest.warns(UserWarning, match="not yet implemented"):
            results = execute_recipe(recipe, project_root=tmp_path)
        kinds = [s.step_kind for s in results[0].steps]
        assert "persist" not in kinds


class TestPipelineResult:

    def test_success_all_ok(self):
        r = PipelineResult(pipeline_id="test", steps=[
            StepResult(step_kind="resample", detail="ok", success=True),
            StepResult(step_kind="join", detail="ok", success=True),
        ])
        assert r.success
        assert r.error_count == 0

    def test_failure_detected(self):
        r = PipelineResult(pipeline_id="test", steps=[
            StepResult(step_kind="resample", detail="ok", success=True),
            StepResult(step_kind="join", detail="boom", success=False, error="fail"),
        ])
        assert not r.success
        assert r.error_count == 1


class TestExecutorCLI:
    """Test that the CLI invokes the executor when --dry-run is not set."""

    def test_non_dry_run_invokes_executor(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        import yaml

        monkeypatch.chdir(tmp_path)
        _setup_pipeline_fixtures(tmp_path)
        data = _recipe_with_pipeline()
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        data["datasets"]["acs"]["path"] = "data/acs.parquet"
        recipe_file = tmp_path / "recipe.yaml"
        recipe_file.write_text(yaml.dump(data), encoding="utf-8")
        result = runner.invoke(app, [
            "build", "recipe",
            "--recipe", str(recipe_file),
        ])
        # Should see execution output (not just validation)
        assert "Executing pipeline" in result.output
        assert "completed" in result.output or "executed" in result.output

    def test_dry_run_does_not_execute(self, tmp_path: Path):
        import yaml

        recipe_file = tmp_path / "recipe.yaml"
        recipe_file.write_text(yaml.dump(_recipe_with_pipeline()), encoding="utf-8")
        result = runner.invoke(app, [
            "build", "recipe",
            "--recipe", str(recipe_file),
            "--dry-run",
        ])
        assert "Executing pipeline" not in result.output

    def test_executor_error_exits_1(self, tmp_path: Path):
        import yaml

        data = _recipe_with_pipeline()
        # Add an auto-resample that can't resolve a transform
        data["pipelines"][0]["steps"].insert(1, {
            "resample": {
                "dataset": "acs",
                "to_geometry": {"type": "county"},
                "method": "aggregate",
                "via": "auto",
                "measures": ["total_population"],
            },
        })
        recipe_file = tmp_path / "recipe.yaml"
        recipe_file.write_text(yaml.dump(data), encoding="utf-8")
        result = runner.invoke(app, [
            "build", "recipe",
            "--recipe", str(recipe_file),
        ])
        assert result.exit_code == 1
        assert "Execution error" in result.output


# ===========================================================================
# Materialize step tests
# ===========================================================================


class TestMaterialize:

    def _make_ctx(self, tmp_path: Path) -> ExecutionContext:
        recipe = load_recipe(_recipe_with_pipeline())
        return ExecutionContext(
            project_root=tmp_path,
            recipe=recipe,
        )

    def test_resolve_tract_to_coc_crosswalk_path(self, tmp_path: Path):
        recipe = load_recipe(_recipe_with_pipeline())
        path = _resolve_transform_path("tract_to_coc", recipe, tmp_path)
        assert "xwalk__B2025xT2020" in str(path)
        assert path.suffix == ".parquet"

    def test_resolve_county_to_coc_crosswalk_path(self, tmp_path: Path):
        data = _recipe_with_pipeline()
        data["transforms"] = [{
            "id": "county_to_coc",
            "type": "crosswalk",
            "from": {"type": "county", "vintage": 2023},
            "to": {"type": "coc", "vintage": 2025},
            "spec": {"weighting": {"scheme": "area"}},
        }]
        data["pipelines"][0]["steps"] = [
            {"materialize": {"transforms": ["county_to_coc"]}},
            {
                "resample": {
                    "dataset": "acs",
                    "to_geometry": {"type": "coc", "vintage": 2025},
                    "method": "aggregate",
                    "via": "county_to_coc",
                    "measures": ["total_population"],
                    "aggregation": "sum",
                },
            },
        ]
        recipe = load_recipe(data)
        path = _resolve_transform_path("county_to_coc", recipe, tmp_path)
        assert "xwalk__B2025xC2023" in str(path)

    def test_unknown_transform_raises(self, tmp_path: Path):
        recipe = load_recipe(_recipe_with_pipeline())
        with pytest.raises(ExecutorError, match="not found in recipe"):
            _resolve_transform_path("nonexistent", recipe, tmp_path)

    def test_unsupported_geometry_pair_raises(self, tmp_path: Path):
        data = _recipe_with_pipeline()
        # zip↔state: no crosswalk path resolver for this pair
        data["transforms"] = [{
            "id": "zip_to_state",
            "type": "crosswalk",
            "from": {"type": "zip", "vintage": 2023},
            "to": {"type": "state", "vintage": 2023},
            "spec": {"weighting": {"scheme": "area"}},
        }]
        data["pipelines"][0]["steps"] = [
            {"materialize": {"transforms": ["zip_to_state"]}},
        ]
        recipe = load_recipe(data)
        with pytest.raises(ExecutorError, match="no 'coc' geometry"):
            _resolve_transform_path("zip_to_state", recipe, tmp_path)

    def test_materialize_reuses_existing_artifact(self, tmp_path: Path):
        ctx = self._make_ctx(tmp_path)
        # Create the expected crosswalk file
        xwalk_dir = tmp_path / "data" / "curated" / "xwalks"
        xwalk_dir.mkdir(parents=True)
        xwalk_file = xwalk_dir / "xwalk__B2025xT2020.parquet"
        pd.DataFrame({"a": [1]}).to_parquet(xwalk_file)

        from coclab.recipe.planner import MaterializeTask
        task = MaterializeTask(transform_ids=["tract_to_coc"])
        result = _execute_materialize(task, ctx)
        assert result.success
        assert "tract_to_coc" in ctx.transform_paths

    def test_materialize_fails_missing_artifact(self, tmp_path: Path):
        ctx = self._make_ctx(tmp_path)
        from coclab.recipe.planner import MaterializeTask
        task = MaterializeTask(transform_ids=["tract_to_coc"])
        result = _execute_materialize(task, ctx)
        assert not result.success
        assert "not found" in result.error
        assert "coclab generate xwalks" in result.error


# ===========================================================================
# Resample step tests
# ===========================================================================


def _make_dataset_parquet(path: Path, geo_col: str = "geo_id") -> None:
    """Write a minimal dataset parquet for testing."""
    df = pd.DataFrame({
        geo_col: ["A", "B", "C"],
        "year": [2020, 2020, 2020],
        "pop": [100, 200, 300],
        "income": [50000, 60000, 70000],
    })
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path)


def _make_xwalk_parquet(path: Path, geo_type: str = "tract") -> None:
    """Write a minimal crosswalk parquet for testing."""
    if geo_type == "tract":
        df = pd.DataFrame({
            "coc_id": ["COC1", "COC1", "COC2"],
            "tract_geoid": ["A", "B", "C"],
            "area_share": [0.8, 0.5, 1.0],
            "pop_share": [0.6, 0.4, 1.0],
        })
    else:
        df = pd.DataFrame({
            "coc_id": ["COC1", "COC1", "COC2"],
            "county_fips": ["A", "B", "C"],
            "area_share": [0.8, 0.5, 1.0],
        })
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path)


class TestResampleIdentity:

    def test_identity_passthrough(self, tmp_path: Path):
        ds_path = tmp_path / "data" / "pit.parquet"
        _make_dataset_parquet(ds_path, geo_col="coc_id")

        recipe = load_recipe(_recipe_with_pipeline())
        ctx = ExecutionContext(project_root=tmp_path, recipe=recipe)

        task = ResampleTask(
            dataset_id="pit",
            year=2020,
            input_path="data/pit.parquet",
            effective_geometry=GeometryRef(type="coc"),
            method="identity",
            transform_id=None,
            to_geometry=GeometryRef(type="coc", vintage=2025),
            measures=["pop"],
        )
        result = _execute_resample(task, ctx)
        assert result.success
        df = ctx.intermediates[("pit", 2020)]
        assert "geo_id" in df.columns
        assert "pop" in df.columns
        assert len(df) == 3

    def test_identity_missing_measures_fails(self, tmp_path: Path):
        ds_path = tmp_path / "data" / "pit.parquet"
        _make_dataset_parquet(ds_path, geo_col="coc_id")

        recipe = load_recipe(_recipe_with_pipeline())
        ctx = ExecutionContext(project_root=tmp_path, recipe=recipe)

        task = ResampleTask(
            dataset_id="pit",
            year=2020,
            input_path="data/pit.parquet",
            effective_geometry=GeometryRef(type="coc"),
            method="identity",
            transform_id=None,
            to_geometry=GeometryRef(type="coc", vintage=2025),
            measures=["nonexistent"],
        )
        result = _execute_resample(task, ctx)
        assert not result.success
        assert "missing measure columns" in result.error

    def test_identity_no_input_path_fails(self, tmp_path: Path):
        recipe = load_recipe(_recipe_with_pipeline())
        ctx = ExecutionContext(project_root=tmp_path, recipe=recipe)

        task = ResampleTask(
            dataset_id="pit",
            year=2020,
            input_path=None,
            effective_geometry=GeometryRef(type="coc"),
            method="identity",
            transform_id=None,
            to_geometry=GeometryRef(type="coc", vintage=2025),
            measures=["pop"],
        )
        result = _execute_resample(task, ctx)
        assert not result.success
        assert "no input path" in result.error

    def test_identity_missing_file_fails(self, tmp_path: Path):
        recipe = load_recipe(_recipe_with_pipeline())
        ctx = ExecutionContext(project_root=tmp_path, recipe=recipe)

        task = ResampleTask(
            dataset_id="pit",
            year=2020,
            input_path="data/nonexistent.parquet",
            effective_geometry=GeometryRef(type="coc"),
            method="identity",
            transform_id=None,
            to_geometry=GeometryRef(type="coc", vintage=2025),
            measures=["pop"],
        )
        result = _execute_resample(task, ctx)
        assert not result.success
        assert "not found" in result.error


class TestResampleAggregate:

    def _setup(self, tmp_path: Path) -> ExecutionContext:
        ds_path = tmp_path / "data" / "acs.parquet"
        _make_dataset_parquet(ds_path, geo_col="GEOID")

        xwalk_path = tmp_path / "data" / "curated" / "xwalks" / "xwalk__B2025xT2020.parquet"
        _make_xwalk_parquet(xwalk_path, geo_type="tract")

        recipe = load_recipe(_recipe_with_pipeline())
        ctx = ExecutionContext(project_root=tmp_path, recipe=recipe)
        ctx.transform_paths["tract_to_coc"] = xwalk_path
        return ctx

    def test_aggregate_sum(self, tmp_path: Path):
        ctx = self._setup(tmp_path)
        task = ResampleTask(
            dataset_id="acs",
            year=2020,
            input_path="data/acs.parquet",
            effective_geometry=GeometryRef(type="tract", vintage=2020),
            method="aggregate",
            transform_id="tract_to_coc",
            to_geometry=GeometryRef(type="coc", vintage=2025),
            measures=["pop"],
            aggregation="sum",
        )
        result = _execute_resample(task, ctx)
        assert result.success
        df = ctx.intermediates[("acs", 2020)]
        assert "geo_id" in df.columns
        assert "year" in df.columns
        # COC1 = 100*0.8 + 200*0.5 = 180, COC2 = 300*1.0 = 300
        coc1 = df[df.geo_id == "COC1"]["pop"].iloc[0]
        coc2 = df[df.geo_id == "COC2"]["pop"].iloc[0]
        assert coc1 == pytest.approx(180.0)
        assert coc2 == pytest.approx(300.0)

    def test_aggregate_weighted_mean(self, tmp_path: Path):
        ctx = self._setup(tmp_path)
        task = ResampleTask(
            dataset_id="acs",
            year=2020,
            input_path="data/acs.parquet",
            effective_geometry=GeometryRef(type="tract", vintage=2020),
            method="aggregate",
            transform_id="tract_to_coc",
            to_geometry=GeometryRef(type="coc", vintage=2025),
            measures=["income"],
            aggregation="weighted_mean",
        )
        result = _execute_resample(task, ctx)
        assert result.success
        df = ctx.intermediates[("acs", 2020)]
        # COC1: pop_share = [0.6, 0.4], income = [50000, 60000]
        # weighted_mean = (50000*0.6 + 60000*0.4) / (0.6+0.4) = 54000
        coc1_income = df[df.geo_id == "COC1"]["income"].iloc[0]
        assert coc1_income == pytest.approx(54000.0)

    def test_aggregate_missing_transform_fails(self, tmp_path: Path):
        ds_path = tmp_path / "data" / "acs.parquet"
        _make_dataset_parquet(ds_path, geo_col="GEOID")

        recipe = load_recipe(_recipe_with_pipeline())
        ctx = ExecutionContext(project_root=tmp_path, recipe=recipe)
        # Don't populate transform_paths

        task = ResampleTask(
            dataset_id="acs",
            year=2020,
            input_path="data/acs.parquet",
            effective_geometry=GeometryRef(type="tract", vintage=2020),
            method="aggregate",
            transform_id="tract_to_coc",
            to_geometry=GeometryRef(type="coc", vintage=2025),
            measures=["pop"],
            aggregation="sum",
        )
        result = _execute_resample(task, ctx)
        assert not result.success
        assert "not materialized" in result.error

    def test_aggregate_zero_join_rows_fails(self, tmp_path: Path):
        # Dataset has geo_ids that don't match crosswalk
        ds_path = tmp_path / "data" / "acs.parquet"
        df = pd.DataFrame({
            "GEOID": ["X", "Y"],
            "year": [2020, 2020],
            "pop": [100, 200],
        })
        ds_path.parent.mkdir(parents=True)
        df.to_parquet(ds_path)

        xwalk_path = tmp_path / "data" / "curated" / "xwalks" / "xwalk__B2025xT2020.parquet"
        _make_xwalk_parquet(xwalk_path, geo_type="tract")

        recipe = load_recipe(_recipe_with_pipeline())
        ctx = ExecutionContext(project_root=tmp_path, recipe=recipe)
        ctx.transform_paths["tract_to_coc"] = xwalk_path

        task = ResampleTask(
            dataset_id="acs",
            year=2020,
            input_path="data/acs.parquet",
            effective_geometry=GeometryRef(type="tract", vintage=2020),
            method="aggregate",
            transform_id="tract_to_coc",
            to_geometry=GeometryRef(type="coc", vintage=2025),
            measures=["pop"],
            aggregation="sum",
        )
        result = _execute_resample(task, ctx)
        assert not result.success
        assert "zero rows" in result.error

    def test_aggregate_mean(self, tmp_path: Path):
        ctx = self._setup(tmp_path)
        task = ResampleTask(
            dataset_id="acs",
            year=2020,
            input_path="data/acs.parquet",
            effective_geometry=GeometryRef(type="tract", vintage=2020),
            method="aggregate",
            transform_id="tract_to_coc",
            to_geometry=GeometryRef(type="coc", vintage=2025),
            measures=["pop"],
            aggregation="mean",
        )
        result = _execute_resample(task, ctx)
        assert result.success
        df = ctx.intermediates[("acs", 2020)]
        # COC1 has tracts A(100) and B(200) → mean = 150
        coc1 = df[df.geo_id == "COC1"]["pop"].iloc[0]
        assert coc1 == pytest.approx(150.0)

    def test_aggregate_county_crosswalk(self, tmp_path: Path):
        ds_path = tmp_path / "data" / "zori.parquet"
        _make_dataset_parquet(ds_path, geo_col="geo_id")

        xwalk_path = tmp_path / "xwalk_county.parquet"
        _make_xwalk_parquet(xwalk_path, geo_type="county")

        data = _recipe_with_pipeline()
        data["transforms"] = [{
            "id": "county_to_coc",
            "type": "crosswalk",
            "from": {"type": "county"},
            "to": {"type": "coc", "vintage": 2025},
            "spec": {"weighting": {"scheme": "area"}},
        }]
        data["pipelines"][0]["steps"] = [
            {"materialize": {"transforms": ["county_to_coc"]}},
            {"resample": {
                "dataset": "acs",
                "to_geometry": {"type": "coc", "vintage": 2025},
                "method": "aggregate",
                "via": "county_to_coc",
                "measures": ["pop"],
                "aggregation": "sum",
            }},
        ]
        recipe = load_recipe(data)
        ctx = ExecutionContext(project_root=tmp_path, recipe=recipe)
        ctx.transform_paths["county_to_coc"] = xwalk_path

        task = ResampleTask(
            dataset_id="zori",
            year=2020,
            input_path="data/zori.parquet",
            effective_geometry=GeometryRef(type="county"),
            method="aggregate",
            transform_id="county_to_coc",
            to_geometry=GeometryRef(type="coc", vintage=2025),
            measures=["pop"],
            aggregation="sum",
        )
        result = _execute_resample(task, ctx)
        assert result.success

    def test_per_measure_aggregation_dict(self, tmp_path: Path):
        """Dict-format measure_aggregations applies different agg per measure."""
        ctx = self._setup(tmp_path)
        task = ResampleTask(
            dataset_id="acs",
            year=2020,
            input_path="data/acs.parquet",
            effective_geometry=GeometryRef(type="tract", vintage=2020),
            method="aggregate",
            transform_id="tract_to_coc",
            to_geometry=GeometryRef(type="coc", vintage=2025),
            measures=["pop", "income"],
            measure_aggregations={"pop": "sum", "income": "weighted_mean"},
        )
        result = _execute_resample(task, ctx)
        assert result.success
        df = ctx.intermediates[("acs", 2020)]

        # --- sum for pop (value * area_share, then sum per CoC) ---
        # COC1 = 100*0.8 + 200*0.5 = 180, COC2 = 300*1.0 = 300
        coc1_pop = df[df.geo_id == "COC1"]["pop"].iloc[0]
        coc2_pop = df[df.geo_id == "COC2"]["pop"].iloc[0]
        assert coc1_pop == pytest.approx(180.0)
        assert coc2_pop == pytest.approx(300.0)

        # --- weighted_mean for income (sum(val*w)/sum(w), w=pop_share) ---
        # COC1 = (50000*0.6 + 60000*0.4) / (0.6+0.4) = 54000
        # COC2 = 70000*1.0 / 1.0 = 70000
        coc1_income = df[df.geo_id == "COC1"]["income"].iloc[0]
        coc2_income = df[df.geo_id == "COC2"]["income"].iloc[0]
        assert coc1_income == pytest.approx(54000.0)
        assert coc2_income == pytest.approx(70000.0)

    def test_measures_list_backward_compat(self, tmp_path: Path):
        """Old list format (measures: [a, b] + aggregation: sum) still works."""
        data = _recipe_with_pipeline()
        # The second resample step already uses the legacy format:
        #   measures: ["total_population"], aggregation: "sum"
        # Confirm the schema coerces it and the planner round-trips correctly.
        recipe = load_recipe(data)
        resample_steps = [
            s for pipe in recipe.pipelines
            for s in pipe.steps
            if hasattr(s, "kind") and s.kind == "resample"
        ]
        # Find the aggregate step (method == aggregate)
        agg_step = [s for s in resample_steps if s.method == "aggregate"][0]
        # After coercion, measures should be a dict mapping name → config
        assert isinstance(agg_step.measures, dict)
        assert "total_population" in agg_step.measures
        assert agg_step.measures["total_population"].aggregation == "sum"

        # Now verify end-to-end: execute with the coerced recipe
        ds_path = tmp_path / "data" / "acs.parquet"
        _make_dataset_parquet(ds_path, geo_col="GEOID")
        xwalk_path = tmp_path / "data" / "curated" / "xwalks" / "xwalk__B2025xT2020.parquet"
        _make_xwalk_parquet(xwalk_path, geo_type="tract")

        ctx = ExecutionContext(project_root=tmp_path, recipe=recipe)
        ctx.transform_paths["tract_to_coc"] = xwalk_path

        task = ResampleTask(
            dataset_id="acs",
            year=2020,
            input_path="data/acs.parquet",
            effective_geometry=GeometryRef(type="tract", vintage=2020),
            method="aggregate",
            transform_id="tract_to_coc",
            to_geometry=GeometryRef(type="coc", vintage=2025),
            measures=["pop"],
            aggregation="sum",
        )
        result = _execute_resample(task, ctx)
        assert result.success
        df = ctx.intermediates[("acs", 2020)]
        # COC1 = 100*0.8 + 200*0.5 = 180, COC2 = 300*1.0 = 300
        coc1 = df[df.geo_id == "COC1"]["pop"].iloc[0]
        assert coc1 == pytest.approx(180.0)


# ===========================================================================
# Temporal filter behavior tests
# ===========================================================================


class TestTemporalFilters:

    def test_calendar_mean_preserves_year_groups(self):
        """Calendar mean should aggregate within year, not across years."""
        df = pd.DataFrame({
            "geo_id": ["A", "A", "A", "A"],
            "year": [2020, 2020, 2021, 2021],
            "month": [1, 2, 1, 2],
            "value": [10.0, 20.0, 30.0, 50.0],
        })

        out = _apply_temporal_filter(
            df,
            filt=TemporalFilter(type="temporal", column="month", method="calendar_mean"),
            year=2020,
            dataset_id="demo",
        )

        assert set(out["year"]) == {2020, 2021}
        val_2020 = out.loc[out["year"] == 2020, "value"].iloc[0]
        val_2021 = out.loc[out["year"] == 2021, "value"].iloc[0]
        assert val_2020 == pytest.approx(15.0)
        assert val_2021 == pytest.approx(40.0)

    def test_calendar_median_preserves_declared_year_column(self):
        """Calendar median should keep declared year column as grouping key."""
        df = pd.DataFrame({
            "geo_id": ["A", "A", "A", "A"],
            "pit_year": [2020, 2020, 2021, 2021],
            "month": [1, 2, 1, 2],
            "value": [10.0, 30.0, 20.0, 80.0],
        })

        out = _apply_temporal_filter(
            df,
            filt=TemporalFilter(type="temporal", column="month", method="calendar_median"),
            year=2020,
            dataset_id="demo",
            year_column="pit_year",
        )

        assert set(out["pit_year"]) == {2020, 2021}
        val_2020 = out.loc[out["pit_year"] == 2020, "value"].iloc[0]
        val_2021 = out.loc[out["pit_year"] == 2021, "value"].iloc[0]
        assert val_2020 == pytest.approx(20.0)
        assert val_2021 == pytest.approx(50.0)


# ===========================================================================
# Column resolution safety tests
# ===========================================================================


class TestColumnResolution:

    def test_declared_geo_column_used(self, tmp_path: Path):
        """When geo_column is declared, the executor uses it."""
        ds_path = tmp_path / "data" / "ds.parquet"
        ds_path.parent.mkdir(parents=True)
        pd.DataFrame({
            "my_geo": ["T1", "T2"],
            "year": [2020, 2020],
            "val": [10, 20],
        }).to_parquet(ds_path)

        data = _recipe_with_pipeline()
        data["datasets"]["acs"]["geo_column"] = "my_geo"
        recipe = load_recipe(data)
        ctx = ExecutionContext(project_root=tmp_path, recipe=recipe)

        task = ResampleTask(
            dataset_id="acs",
            year=2020,
            input_path="data/ds.parquet",
            effective_geometry=GeometryRef(type="coc", vintage=2025),
            method="identity",
            transform_id=None,
            to_geometry=GeometryRef(type="coc", vintage=2025),
            measures=["val"],
            geo_column="my_geo",
        )
        result = _execute_resample(task, ctx)
        assert result.success
        df = ctx.intermediates[("acs", 2020)]
        assert "geo_id" in df.columns

    def test_declared_geo_column_missing_errors(self, tmp_path: Path):
        """Declared geo_column that doesn't exist in data raises error."""
        ds_path = tmp_path / "data" / "ds.parquet"
        ds_path.parent.mkdir(parents=True)
        pd.DataFrame({
            "GEOID": ["T1"],
            "year": [2020],
            "val": [10],
        }).to_parquet(ds_path)

        data = _recipe_with_pipeline()
        recipe = load_recipe(data)
        ctx = ExecutionContext(project_root=tmp_path, recipe=recipe)

        task = ResampleTask(
            dataset_id="acs",
            year=2020,
            input_path="data/ds.parquet",
            effective_geometry=GeometryRef(type="coc", vintage=2025),
            method="identity",
            transform_id=None,
            to_geometry=GeometryRef(type="coc", vintage=2025),
            measures=["val"],
            geo_column="nonexistent",
        )
        result = _execute_resample(task, ctx)
        assert not result.success
        assert "nonexistent" in result.error

    def test_declared_year_column_used(self, tmp_path: Path):
        """When year_column is declared, the executor filters on it."""
        ds_path = tmp_path / "data" / "ds.parquet"
        ds_path.parent.mkdir(parents=True)
        pd.DataFrame({
            "coc_id": ["COC1", "COC1"],
            "pit_year": [2020, 2021],
            "val": [10, 20],
        }).to_parquet(ds_path)

        data = _recipe_with_pipeline()
        data["datasets"]["pit"]["year_column"] = "pit_year"
        recipe = load_recipe(data)
        ctx = ExecutionContext(project_root=tmp_path, recipe=recipe)

        task = ResampleTask(
            dataset_id="pit",
            year=2020,
            input_path="data/ds.parquet",
            effective_geometry=GeometryRef(type="coc"),
            method="identity",
            transform_id=None,
            to_geometry=GeometryRef(type="coc", vintage=2025),
            measures=["val"],
            year_column="pit_year",
        )
        result = _execute_resample(task, ctx)
        assert result.success
        df = ctx.intermediates[("pit", 2020)]
        assert len(df) == 1
        assert df["val"].iloc[0] == 10

    def test_ambiguous_geo_column_errors(self, tmp_path: Path):
        """Multiple geo-ID candidate columns without declaration should error."""
        ds_path = tmp_path / "data" / "ds.parquet"
        ds_path.parent.mkdir(parents=True)
        pd.DataFrame({
            "geo_id": ["T1"],
            "GEOID": ["T1"],
            "year": [2020],
            "val": [10],
        }).to_parquet(ds_path)

        data = _recipe_with_pipeline()
        recipe = load_recipe(data)
        ctx = ExecutionContext(project_root=tmp_path, recipe=recipe)

        task = ResampleTask(
            dataset_id="acs",
            year=2020,
            input_path="data/ds.parquet",
            effective_geometry=GeometryRef(type="coc", vintage=2025),
            method="identity",
            transform_id=None,
            to_geometry=GeometryRef(type="coc", vintage=2025),
            measures=["val"],
        )
        result = _execute_resample(task, ctx)
        assert not result.success
        assert "Ambiguous geo-ID" in result.error


# ===========================================================================
# Join output persistence and provenance tests
# ===========================================================================


class TestJoinPersistence:

    def test_full_pipeline_writes_panel(self, tmp_path: Path):
        """End-to-end: materialize → resample → join → persist."""
        _setup_pipeline_fixtures(tmp_path)
        data = _recipe_with_pipeline()
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        data["datasets"]["acs"]["path"] = "data/acs.parquet"
        recipe = load_recipe(data)
        results = execute_recipe(recipe, project_root=tmp_path)
        assert len(results) == 1
        assert results[0].success
        # Check panel file was written
        panel_file = tmp_path / "data" / "curated" / "panel" / "panel__Y2020-2021@B2025.parquet"
        assert panel_file.exists()

    def test_panel_contains_all_years(self, tmp_path: Path):
        _setup_pipeline_fixtures(tmp_path)
        data = _recipe_with_pipeline()
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        data["datasets"]["acs"]["path"] = "data/acs.parquet"
        recipe = load_recipe(data)
        execute_recipe(recipe, project_root=tmp_path)
        panel_file = tmp_path / "data" / "curated" / "panel" / "panel__Y2020-2021@B2025.parquet"
        panel = pd.read_parquet(panel_file)
        assert set(panel["year"].unique()) == {2020, 2021}

    def test_panel_has_provenance_metadata(self, tmp_path: Path):
        import json as json_mod
        import pyarrow.parquet as pq

        _setup_pipeline_fixtures(tmp_path)
        data = _recipe_with_pipeline()
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        data["datasets"]["acs"]["path"] = "data/acs.parquet"
        recipe = load_recipe(data)
        execute_recipe(recipe, project_root=tmp_path)
        panel_file = tmp_path / "data" / "curated" / "panel" / "panel__Y2020-2021@B2025.parquet"
        table = pq.read_table(panel_file)
        metadata = table.schema.metadata
        assert b"coclab_provenance" in metadata
        prov = json_mod.loads(metadata[b"coclab_provenance"])
        assert prov["recipe_name"] == "executor-test"
        assert prov["pipeline_id"] == "main"
        assert "pit" in prov["datasets"]
        assert "acs" in prov["datasets"]
        assert "tract_to_coc" in prov["transforms"]

    def test_persist_step_in_results(self, tmp_path: Path):
        _setup_pipeline_fixtures(tmp_path)
        data = _recipe_with_pipeline()
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        data["datasets"]["acs"]["path"] = "data/acs.parquet"
        recipe = load_recipe(data)
        results = execute_recipe(recipe, project_root=tmp_path)
        kinds = [s.step_kind for s in results[0].steps]
        assert "persist" in kinds

    def test_no_join_no_persist(self, tmp_path: Path):
        """Pipeline with no join steps should skip persist."""
        _setup_pipeline_fixtures(tmp_path)
        data = _recipe_with_pipeline()
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        data["datasets"]["acs"]["path"] = "data/acs.parquet"
        # Remove the join step
        data["pipelines"][0]["steps"] = [
            s for s in data["pipelines"][0]["steps"]
            if not (isinstance(s, dict) and "join" in s)
        ]
        recipe = load_recipe(data)
        results = execute_recipe(recipe, project_root=tmp_path)
        kinds = [s.step_kind for s in results[0].steps]
        assert "persist" not in kinds

    def test_cli_shows_persist_summary(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        import yaml

        monkeypatch.chdir(tmp_path)
        _setup_pipeline_fixtures(tmp_path)
        data = _recipe_with_pipeline()
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        data["datasets"]["acs"]["path"] = "data/acs.parquet"
        recipe_file = tmp_path / "recipe.yaml"
        recipe_file.write_text(yaml.dump(data), encoding="utf-8")
        result = runner.invoke(app, [
            "build", "recipe",
            "--recipe", str(recipe_file),
        ])
        assert result.exit_code == 0
        assert "persist" in result.output.lower()
        assert "panel" in result.output.lower()


# ===========================================================================
# Asset caching tests
# ===========================================================================


class TestRecipeCache:

    def test_cache_avoids_reread(self, tmp_path: Path):
        """Second read of same file returns cached DataFrame."""
        f = tmp_path / "data.parquet"
        pd.DataFrame({"x": [1, 2]}).to_parquet(f)
        cache = RecipeCache(enabled=True)
        df1 = cache.read_parquet(f)
        df2 = cache.read_parquet(f)
        assert df1.equals(df2)
        assert cache.cached_count == 1

    def test_cache_returns_copy(self, tmp_path: Path):
        """Mutating a cached result does not corrupt the cache."""
        f = tmp_path / "data.parquet"
        pd.DataFrame({"x": [1, 2, 3]}).to_parquet(f)
        cache = RecipeCache(enabled=True)
        df1 = cache.read_parquet(f)
        df1["x"] = 999
        df2 = cache.read_parquet(f)
        assert list(df2["x"]) == [1, 2, 3]

    def test_disabled_cache_does_not_store(self, tmp_path: Path):
        f = tmp_path / "data.parquet"
        pd.DataFrame({"x": [1]}).to_parquet(f)
        cache = RecipeCache(enabled=False)
        cache.read_parquet(f)
        assert cache.cached_count == 0

    def test_file_identity(self, tmp_path: Path):
        f = tmp_path / "data.parquet"
        pd.DataFrame({"x": [1]}).to_parquet(f)
        cache = RecipeCache()
        identity = cache.file_identity(f)
        assert len(identity.sha256) == 64
        assert identity.size == f.stat().st_size
        # Repeated call returns cached identity
        identity2 = cache.file_identity(f)
        assert identity2 is identity

    def test_sha256_file(self, tmp_path: Path):
        f = tmp_path / "hello.txt"
        f.write_text("hello world")
        h = _sha256_file(f)
        assert len(h) == 64
        assert h == _sha256_file(f)  # deterministic


# ===========================================================================
# Manifest tests
# ===========================================================================


class TestRecipeManifest:

    def test_roundtrip_json(self):
        m = RecipeManifest(
            recipe_name="test",
            recipe_version=1,
            pipeline_id="main",
            assets=[
                AssetRecord(
                    role="dataset",
                    path="data/pit.parquet",
                    sha256="abc123",
                    size=1000,
                    dataset_id="pit",
                ),
            ],
        )
        json_str = m.to_json()
        m2 = RecipeManifest.from_json(json_str)
        assert m2.recipe_name == "test"
        assert len(m2.assets) == 1
        assert m2.assets[0].sha256 == "abc123"

    def test_write_and_read_manifest(self, tmp_path: Path):
        m = RecipeManifest(
            recipe_name="test",
            recipe_version=1,
            pipeline_id="main",
        )
        path = tmp_path / "manifest.json"
        write_manifest(m, path)
        assert path.exists()
        m2 = read_manifest(path)
        assert m2.recipe_name == "test"

    def test_export_bundle_copies_assets(self, tmp_path: Path):
        # Create source files
        (tmp_path / "data").mkdir()
        src = tmp_path / "data" / "pit.parquet"
        pd.DataFrame({"x": [1]}).to_parquet(src)

        m = RecipeManifest(
            recipe_name="test",
            recipe_version=1,
            pipeline_id="main",
            assets=[
                AssetRecord(
                    role="dataset",
                    path="data/pit.parquet",
                    sha256="abc",
                    size=100,
                ),
            ],
        )
        out = tmp_path / "bundle"
        export_bundle(m, tmp_path, out)
        assert (out / "manifest.json").exists()
        assert (out / "assets" / "data" / "pit.parquet").exists()

    def test_export_skips_missing_files(self, tmp_path: Path):
        """Non-existent assets are skipped without error."""
        m = RecipeManifest(
            recipe_name="test",
            recipe_version=1,
            pipeline_id="main",
            assets=[
                AssetRecord(
                    role="dataset",
                    path="does/not/exist.parquet",
                    sha256="abc",
                    size=0,
                ),
            ],
        )
        out = tmp_path / "bundle"
        export_bundle(m, tmp_path, out)
        assert (out / "manifest.json").exists()


# ===========================================================================
# Provenance manifest integration tests
# ===========================================================================


class TestProvenanceManifest:

    def test_execution_writes_manifest_sidecar(self, tmp_path: Path):
        """Full pipeline writes both panel and manifest.json sidecar."""
        _setup_pipeline_fixtures(tmp_path)
        data = _recipe_with_pipeline()
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        data["datasets"]["acs"]["path"] = "data/acs.parquet"
        recipe = load_recipe(data)
        execute_recipe(recipe, project_root=tmp_path)
        manifest_file = (
            tmp_path / "data" / "curated" / "panel"
            / "panel__Y2020-2021@B2025.manifest.json"
        )
        assert manifest_file.exists()
        m = read_manifest(manifest_file)
        assert m.recipe_name == "executor-test"
        assert m.pipeline_id == "main"

    def test_manifest_records_consumed_assets(self, tmp_path: Path):
        _setup_pipeline_fixtures(tmp_path)
        data = _recipe_with_pipeline()
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        data["datasets"]["acs"]["path"] = "data/acs.parquet"
        recipe = load_recipe(data)
        execute_recipe(recipe, project_root=tmp_path)
        manifest_file = (
            tmp_path / "data" / "curated" / "panel"
            / "panel__Y2020-2021@B2025.manifest.json"
        )
        m = read_manifest(manifest_file)
        # Should have crosswalk + 2 datasets (pit, acs) deduplicated
        roles = {a.role for a in m.assets}
        assert "crosswalk" in roles
        assert "dataset" in roles
        # Each asset should have a sha256
        for a in m.assets:
            assert len(a.sha256) == 64

    def test_provenance_includes_consumed_assets(self, tmp_path: Path):
        """Parquet metadata provenance includes consumed_assets."""
        import json as json_mod
        import pyarrow.parquet as pq

        _setup_pipeline_fixtures(tmp_path)
        data = _recipe_with_pipeline()
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        data["datasets"]["acs"]["path"] = "data/acs.parquet"
        recipe = load_recipe(data)
        execute_recipe(recipe, project_root=tmp_path)
        panel_file = (
            tmp_path / "data" / "curated" / "panel"
            / "panel__Y2020-2021@B2025.parquet"
        )
        table = pq.read_table(panel_file)
        prov = json_mod.loads(table.schema.metadata[b"coclab_provenance"])
        assert "consumed_assets" in prov
        assert len(prov["consumed_assets"]) > 0
        asset = prov["consumed_assets"][0]
        assert "sha256" in asset
        assert "size" in asset

    def test_cache_reuse_during_execution(self, tmp_path: Path):
        """Cache should avoid re-reading the same file for multiple years."""
        _setup_pipeline_fixtures(tmp_path)
        data = _recipe_with_pipeline()
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        data["datasets"]["acs"]["path"] = "data/acs.parquet"
        recipe = load_recipe(data)
        cache = RecipeCache(enabled=True)
        execute_recipe(recipe, project_root=tmp_path, cache=cache)
        # With 2 years and 2 datasets in same files + 1 crosswalk,
        # cache should hold 3 unique files (pit, acs, crosswalk)
        assert cache.cached_count == 3

    def test_no_cache_flag_disables_caching(self, tmp_path: Path):
        _setup_pipeline_fixtures(tmp_path)
        data = _recipe_with_pipeline()
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        data["datasets"]["acs"]["path"] = "data/acs.parquet"
        recipe = load_recipe(data)
        cache = RecipeCache(enabled=False)
        execute_recipe(recipe, project_root=tmp_path, cache=cache)
        assert cache.cached_count == 0


# ===========================================================================
# CLI provenance / export command tests
# ===========================================================================


class TestRecipeProvenanceCLI:

    def test_provenance_shows_manifest(self, tmp_path: Path):
        m = RecipeManifest(
            recipe_name="demo",
            recipe_version=1,
            pipeline_id="main",
            assets=[
                AssetRecord(
                    role="dataset",
                    path="data/pit.parquet",
                    sha256="a" * 64,
                    size=2048,
                    dataset_id="pit",
                ),
            ],
            output_path="data/curated/panel/out.parquet",
        )
        mf = tmp_path / "test.manifest.json"
        write_manifest(m, mf)

        result = runner.invoke(app, [
            "build", "recipe-provenance",
            "--manifest", str(mf),
        ])
        assert result.exit_code == 0
        assert "demo" in result.output
        assert "pit" in result.output
        assert "aaaaaaaaaaaa" in result.output  # sha256 prefix

    def test_provenance_missing_manifest(self, tmp_path: Path):
        result = runner.invoke(app, [
            "build", "recipe-provenance",
            "--manifest", str(tmp_path / "nope.json"),
        ])
        assert result.exit_code == 1

    def test_export_creates_bundle(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "data").mkdir()
        pd.DataFrame({"x": [1]}).to_parquet(tmp_path / "data" / "pit.parquet")

        m = RecipeManifest(
            recipe_name="demo",
            recipe_version=1,
            pipeline_id="main",
            assets=[
                AssetRecord(
                    role="dataset",
                    path="data/pit.parquet",
                    sha256="a" * 64,
                    size=100,
                ),
            ],
        )
        mf = tmp_path / "test.manifest.json"
        write_manifest(m, mf)
        out = tmp_path / "my_bundle"

        result = runner.invoke(app, [
            "build", "recipe-export",
            "--manifest", str(mf),
            "--output", str(out),
        ])
        assert result.exit_code == 0
        assert (out / "manifest.json").exists()
        assert (out / "assets" / "data" / "pit.parquet").exists()

    def test_no_cache_cli_flag(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        import yaml

        monkeypatch.chdir(tmp_path)
        _setup_pipeline_fixtures(tmp_path)
        data = _recipe_with_pipeline()
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        data["datasets"]["acs"]["path"] = "data/acs.parquet"
        recipe_file = tmp_path / "recipe.yaml"
        recipe_file.write_text(yaml.dump(data), encoding="utf-8")
        result = runner.invoke(app, [
            "build", "recipe",
            "--recipe", str(recipe_file),
            "--no-cache",
        ])
        assert result.exit_code == 0
        assert "executed" in result.output.lower()


# ===========================================================================
# --json output mode tests
# ===========================================================================


def _make_project_root(tmp_path: Path) -> None:
    """Create marker files so _check_working_directory() doesn't warn."""
    (tmp_path / "pyproject.toml").write_text("[project]\nname='test'\n")
    (tmp_path / "coclab").mkdir(exist_ok=True)
    (tmp_path / "data").mkdir(exist_ok=True)


class TestRecipeJsonMode:

    def _write_recipe(self, tmp_path: Path, data: dict) -> Path:
        import yaml

        recipe_file = tmp_path / "recipe.yaml"
        recipe_file.write_text(yaml.dump(data), encoding="utf-8")
        return recipe_file

    def test_json_dry_run(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        data = _recipe_with_pipeline()
        rf = self._write_recipe(tmp_path, data)
        result = runner.invoke(app, [
            "build", "recipe",
            "--recipe", str(rf),
            "--dry-run", "--json",
        ])
        assert result.exit_code == 0
        out = json.loads(result.output)
        assert out["status"] == "ok"
        assert out["recipe_name"] == "executor-test"
        assert out["dry_run"] is True
        assert "validation" in out

    def test_json_full_execution(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ):
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        _setup_pipeline_fixtures(tmp_path)
        data = _recipe_with_pipeline()
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        data["datasets"]["acs"]["path"] = "data/acs.parquet"
        rf = self._write_recipe(tmp_path, data)
        result = runner.invoke(app, [
            "build", "recipe",
            "--recipe", str(rf),
            "--json",
        ])
        assert result.exit_code == 0
        out = json.loads(result.output)
        assert out["status"] == "ok"
        assert len(out["pipelines"]) == 1
        pipeline = out["pipelines"][0]
        assert pipeline["pipeline_id"] == "main"
        assert pipeline["success"] is True
        kinds = [s["step_kind"] for s in pipeline["steps"]]
        assert "materialize" in kinds
        assert "resample" in kinds
        assert "join" in kinds

    def test_json_suppresses_progress(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ):
        """JSON mode should not include human-readable progress lines."""
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        _setup_pipeline_fixtures(tmp_path)
        data = _recipe_with_pipeline()
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        data["datasets"]["acs"]["path"] = "data/acs.parquet"
        rf = self._write_recipe(tmp_path, data)
        result = runner.invoke(app, [
            "build", "recipe",
            "--recipe", str(rf),
            "--json",
        ])
        # Output should be valid JSON (no interleaved echo lines)
        out = json.loads(result.output)
        assert isinstance(out, dict)

    def test_json_validation_error(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ):
        """Validation errors should produce structured JSON error."""
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        data = _recipe_with_pipeline()
        # Introduce a missing required dataset path
        data["datasets"]["pit"]["path"] = "missing.parquet"
        data["datasets"]["acs"]["path"] = "also_missing.parquet"
        data["validation"] = {"missing_dataset": {"default": "fail"}}
        rf = self._write_recipe(tmp_path, data)
        result = runner.invoke(app, [
            "build", "recipe",
            "--recipe", str(rf),
            "--json",
        ])
        assert result.exit_code == 1
        out = json.loads(result.output)
        assert out["status"] == "error"
        assert len(out["validation"]["errors"]) >= 2

    def test_json_provenance(self, tmp_path: Path):
        m = RecipeManifest(
            recipe_name="demo",
            recipe_version=1,
            pipeline_id="main",
            assets=[
                AssetRecord(
                    role="dataset", path="data/x.parquet",
                    sha256="a" * 64, size=100, dataset_id="x",
                ),
            ],
        )
        mf = tmp_path / "test.manifest.json"
        write_manifest(m, mf)
        result = runner.invoke(app, [
            "build", "recipe-provenance",
            "--manifest", str(mf),
            "--json",
        ])
        assert result.exit_code == 0
        out = json.loads(result.output)
        assert out["status"] == "ok"
        assert out["recipe_name"] == "demo"
        assert len(out["assets"]) == 1

    def test_json_export(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        pd.DataFrame({"x": [1]}).to_parquet(
            tmp_path / "data" / "pit.parquet",
        )
        m = RecipeManifest(
            recipe_name="demo",
            recipe_version=1,
            pipeline_id="main",
            assets=[
                AssetRecord(
                    role="dataset", path="data/pit.parquet",
                    sha256="a" * 64, size=100,
                ),
            ],
        )
        mf = tmp_path / "test.manifest.json"
        write_manifest(m, mf)
        out_dir = tmp_path / "bundle"
        result = runner.invoke(app, [
            "build", "recipe-export",
            "--manifest", str(mf),
            "--output", str(out_dir),
            "--json",
        ])
        assert result.exit_code == 0
        out = json.loads(result.output)
        assert out["status"] == "ok"
        assert out["assets_copied"] == 1

    def test_json_provenance_missing_manifest(self, tmp_path: Path):
        result = runner.invoke(app, [
            "build", "recipe-provenance",
            "--manifest", str(tmp_path / "nope.json"),
            "--json",
        ])
        assert result.exit_code == 1
        out = json.loads(result.output)
        assert out["status"] == "error"


# ===========================================================================
# recipe-plan command tests
# ===========================================================================


class TestRecipePlanCmd:

    def _write_recipe(self, tmp_path: Path, data: dict) -> Path:
        import yaml

        recipe_file = tmp_path / "recipe.yaml"
        recipe_file.write_text(yaml.dump(data), encoding="utf-8")
        return recipe_file

    def test_plan_human_output(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ):
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        data = _recipe_with_pipeline()
        rf = self._write_recipe(tmp_path, data)
        result = runner.invoke(app, [
            "build", "recipe-plan",
            "--recipe", str(rf),
        ])
        assert result.exit_code == 0
        assert "Pipeline 'main'" in result.output
        assert "[materialize]" in result.output
        assert "[resample]" in result.output
        assert "[join]" in result.output

    def test_plan_json_output(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ):
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        data = _recipe_with_pipeline()
        rf = self._write_recipe(tmp_path, data)
        result = runner.invoke(app, [
            "build", "recipe-plan",
            "--recipe", str(rf),
            "--json",
        ])
        assert result.exit_code == 0
        out = json.loads(result.output)
        assert out["status"] == "ok"
        assert out["recipe_name"] == "executor-test"
        assert len(out["pipelines"]) == 1
        plan = out["pipelines"][0]
        assert plan["pipeline_id"] == "main"
        assert len(plan["materialize_tasks"]) == 1
        assert len(plan["resample_tasks"]) == 4  # 2 datasets × 2 years
        assert len(plan["join_tasks"]) == 2  # 2 years
        assert plan["task_count"] == 7

    def test_plan_json_shows_resolved_paths(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ):
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        # Create the dataset file so validation passes
        pit_path = tmp_path / "data" / "pit.parquet"
        pd.DataFrame({"coc_id": ["C1"], "year": [2020], "pit_total": [1]}).to_parquet(pit_path)
        data = _recipe_with_pipeline()
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        rf = self._write_recipe(tmp_path, data)
        result = runner.invoke(app, [
            "build", "recipe-plan",
            "--recipe", str(rf),
            "--json",
        ])
        out = json.loads(result.output)
        resample_tasks = out["pipelines"][0]["resample_tasks"]
        pit_tasks = [t for t in resample_tasks if t["dataset_id"] == "pit"]
        assert pit_tasks[0]["input_path"] == "data/pit.parquet"

    def test_plan_json_shows_geometry(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ):
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        data = _recipe_with_pipeline()
        rf = self._write_recipe(tmp_path, data)
        result = runner.invoke(app, [
            "build", "recipe-plan",
            "--recipe", str(rf),
            "--json",
        ])
        out = json.loads(result.output)
        resample_tasks = out["pipelines"][0]["resample_tasks"]
        acs_task = next(
            t for t in resample_tasks if t["dataset_id"] == "acs"
        )
        assert acs_task["effective_geometry"]["type"] == "tract"
        assert acs_task["effective_geometry"]["vintage"] == 2020
        assert acs_task["to_geometry"]["type"] == "coc"

    def test_plan_json_shows_transform_selection(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ):
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        data = _recipe_with_pipeline()
        rf = self._write_recipe(tmp_path, data)
        result = runner.invoke(app, [
            "build", "recipe-plan",
            "--recipe", str(rf),
            "--json",
        ])
        out = json.loads(result.output)
        resample_tasks = out["pipelines"][0]["resample_tasks"]
        acs_task = next(
            t for t in resample_tasks if t["dataset_id"] == "acs"
        )
        assert acs_task["transform_id"] == "tract_to_coc"

    def test_plan_planner_error_json(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ):
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        data = _recipe_with_pipeline()
        # Add an unresolvable auto transform
        data["pipelines"][0]["steps"].insert(1, {
            "resample": {
                "dataset": "acs",
                "to_geometry": {"type": "county"},
                "method": "aggregate",
                "via": "auto",
                "measures": ["total_population"],
            },
        })
        rf = self._write_recipe(tmp_path, data)
        result = runner.invoke(app, [
            "build", "recipe-plan",
            "--recipe", str(rf),
            "--json",
        ])
        assert result.exit_code == 1
        out = json.loads(result.output)
        assert out["status"] == "error"


class TestExecutionPlanToDict:

    def test_plan_to_dict(self):
        recipe = load_recipe(_recipe_with_pipeline())
        plan = resolve_plan(recipe, "main")
        d = plan.to_dict()
        assert d["pipeline_id"] == "main"
        assert "materialize_tasks" in d
        assert "resample_tasks" in d
        assert "join_tasks" in d
        assert d["task_count"] == (
            len(d["materialize_tasks"])
            + len(d["resample_tasks"])
            + len(d["join_tasks"])
        )

    def test_plan_to_dict_geometry_fields(self):
        recipe = load_recipe(_recipe_with_pipeline())
        plan = resolve_plan(recipe, "main")
        d = plan.to_dict()
        rt = d["resample_tasks"][0]
        assert "type" in rt["effective_geometry"]
        assert "type" in rt["to_geometry"]
