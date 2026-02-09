"""Tests for recipe loading, adapter registries, and CLI command."""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from coclab.cli.main import app
from coclab.recipe.adapters import (
    DatasetAdapterRegistry,
    GeometryAdapterRegistry,
    ValidationDiagnostic,
    validate_recipe_adapters,
)
from coclab.recipe.loader import RecipeLoadError, load_recipe
from coclab.recipe.recipe_schema import DatasetSpec, GeometryRef, RecipeV1

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
