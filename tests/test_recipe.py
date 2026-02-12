"""Tests for recipe loading, adapter registries, and CLI command."""

from __future__ import annotations

from pathlib import Path

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
from coclab.recipe.loader import RecipeLoadError, load_recipe
from coclab.recipe.recipe_schema import (
    DatasetSpec,
    FileSetSegment,
    FileSetSpec,
    GeometryRef,
    RecipeV1,
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
