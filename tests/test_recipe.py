"""Tests for recipe loading, adapter registries, executor, and CLI command."""

from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Polygon
from typer.testing import CliRunner

from hhplab.cli.main import app
from hhplab.geo.ct_planning_regions import CtPlanningRegionCrosswalk
from hhplab.panel.assemble import _load_coc_areas
from hhplab.provenance import ProvenanceBlock, write_parquet_with_provenance
from hhplab.recipe.adapters import (
    DatasetAdapterRegistry,
    GeometryAdapterRegistry,
    ValidationDiagnostic,
    dataset_registry,
    geometry_registry,
    validate_recipe_adapters,
)
from hhplab.recipe.cache import RecipeCache, _sha256_file
from hhplab.recipe.executor import (
    ExecutionContext,
    ExecutorError,
    PipelineResult,
    StepResult,
    _apply_temporal_filter,
    _assemble_panel,
    _canonicalize_panel_for_target,
    _execute_materialize,
    _execute_resample,
    _recipe_output_dirname,
    _resolve_panel_output_file,
    _resolve_transform_path,
    execute_recipe,
)
from hhplab.recipe.loader import RecipeLoadError, load_recipe
from hhplab.recipe.manifest import (
    AssetRecord,
    RecipeManifest,
    export_bundle,
    read_manifest,
    write_manifest,
)
from hhplab.recipe.planner import (
    ExecutionPlan,
    JoinTask,
    MaterializeTask,
    ResampleTask,
    SmallAreaEstimateTask,
    resolve_plan,
)
from hhplab.recipe.recipe_schema import (
    Acs1Policy,
    ContainmentSpec,
    DatasetSpec,
    GeometryRef,
    MapSpec,
    PanelPolicy,
    RecipeV1,
    SAEDiagnosticsSpec,
    SAEMeasureConfig,
    SmallAreaEstimateStep,
    TemporalFilter,
    YearSpec,
    ZoriPolicy,
    expand_year_spec,
)

runner = CliRunner()

STALE_TRANSLATED_ACS_PATH = "data/curated/acs/acs5_tracts__A2019xT2020.parquet"
STALE_TRANSLATED_ACS_VINTAGE = "2015-2019"
STALE_TRANSLATED_ACS_REBUILD = "hhplab ingest acs5-tract --acs 2015-2019 --tracts 2020 --force"


def _write_stale_translated_acs_cache(path: Path) -> None:
    """Write a stale translated ACS tract cache missing translation metadata."""
    write_parquet_with_provenance(
        pd.DataFrame(
            {
                "tract_geoid": ["T1"],
                "year": [2020],
                "acs_vintage": [STALE_TRANSLATED_ACS_VINTAGE],
                "tract_vintage": ["2020"],
                "total_population": [100],
            }
        ),
        path,
        ProvenanceBlock(
            acs_vintage=STALE_TRANSLATED_ACS_VINTAGE,
            tract_vintage="2020",
            extra={"dataset": "acs5_tract_data"},
        ),
    )


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


def _sae_recipe() -> dict:
    """Return a minimal valid recipe with an ACS1/ACS5 SAE step."""
    return {
        "version": 1,
        "name": "sae-test-recipe",
        "universe": {"years": [2023]},
        "targets": [
            {
                "id": "coc_panel",
                "geometry": {"type": "coc", "vintage": 2025},
            }
        ],
        "datasets": {
            "acs1_county": {
                "provider": "census",
                "product": "acs1",
                "version": 1,
                "native_geometry": {"type": "county", "vintage": 2020, "source": "tiger"},
                "path": "data/curated/acs/acs1_county_sae__A2023.parquet",
            },
            "acs5_tract_support": {
                "provider": "census",
                "product": "acs5",
                "version": 1,
                "native_geometry": {"type": "tract", "vintage": 2020, "source": "tiger"},
                "path": "data/curated/acs/acs5_tract_sae_support__A2022xT2020.parquet",
            },
        },
        "pipelines": [
            {
                "id": "main",
                "target": "coc_panel",
                "steps": [
                    {
                        "kind": "small_area_estimate",
                        "output_dataset": "acs_sae_coc",
                        "source_dataset": "acs1_county",
                        "support_dataset": "acs5_tract_support",
                        "source_geometry": {
                            "type": "county",
                            "vintage": 2020,
                            "source": "tiger",
                        },
                        "support_geometry": {
                            "type": "tract",
                            "vintage": 2020,
                            "source": "tiger",
                        },
                        "target_geometry": {"type": "coc", "vintage": 2025},
                        "terminal_acs5_vintage": 2022,
                        "tract_vintage": 2020,
                        "denominators": {
                            "rent_burden": "gross_rent_pct_income_total",
                            "labor_force": "civilian_labor_force",
                        },
                        "measures": {
                            "household_income_bins": {
                                "outputs": [
                                    "sae_household_income_median",
                                    "sae_household_income_quintile_cutoff_20",
                                ]
                            },
                            "rent_burden": {
                                "outputs": [
                                    "sae_rent_burden_30_plus",
                                    "sae_rent_burden_50_plus",
                                ]
                            },
                        },
                        "diagnostics": {"direct_county_comparison": True},
                    },
                    {
                        "kind": "join",
                        "datasets": ["acs_sae_coc"],
                        "join_on": ["geo_id", "year"],
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
            recipe.datasets["acs"].path == "data/curated/measures/coc_measures__2020__2019.parquet"
        )

    def test_dataset_path_rejects_absolute(self):
        data = _minimal_recipe()
        data["datasets"]["acs"]["path"] = "/tmp/acs.parquet"
        with pytest.raises(RecipeLoadError, match="DatasetSpec.path must be a relative path"):
            load_recipe(data)

    def test_target_map_output_requires_map_spec(self):
        data = _minimal_recipe()
        data["targets"][0]["outputs"] = ["map"]
        with pytest.raises(RecipeLoadError, match="require 'map_spec'"):
            load_recipe(data)

    def test_target_map_spec_requires_map_output(self):
        data = _minimal_recipe()
        data["targets"][0]["map_spec"] = {
            "layers": [
                {
                    "geometry": {"type": "coc", "vintage": 2025},
                    "selector_ids": ["CO-500"],
                }
            ]
        }
        with pytest.raises(RecipeLoadError, match="requires outputs to include 'map'"):
            load_recipe(data)

    def test_valid_map_target_loads(self):
        data = _minimal_recipe()
        data["targets"][0]["outputs"] = ["map"]
        data["targets"][0]["map_spec"] = {
            "layers": [
                {
                    "geometry": {"type": "coc", "vintage": 2025},
                    "selector_ids": ["CO-500"],
                    "tooltip_fields": ["coc_id", "coc_name"],
                    "style_mode": "distinct",
                }
            ],
            "viewport": {"fit_layers": True, "padding": 24},
        }
        recipe = load_recipe(data)
        assert isinstance(recipe.targets[0].map_spec, MapSpec)
        assert recipe.targets[0].map_spec.layers[0].selector_ids == ["CO-500"]
        assert recipe.targets[0].map_spec.layers[0].style_mode == "distinct"

    def test_target_containment_output_requires_containment_spec(self):
        data = _minimal_recipe()
        data["targets"][0]["outputs"] = ["containment"]
        with pytest.raises(RecipeLoadError, match="require 'containment_spec'"):
            load_recipe(data)

    def test_target_containment_spec_requires_containment_output(self):
        data = _minimal_recipe()
        data["targets"][0]["containment_spec"] = {
            "container": {"type": "msa", "source": "census_msa_2023", "vintage": 2023},
            "candidate": {"type": "coc", "vintage": 2025},
            "selector_ids": ["17460"],
        }
        with pytest.raises(RecipeLoadError, match="requires outputs to include 'containment'"):
            load_recipe(data)

    def test_valid_panel_containment_filter_loads_without_containment_output(self):
        data = _minimal_recipe()
        data["targets"][0]["outputs"] = ["panel"]
        data["targets"][0]["containment_filter"] = {
            "container": {"type": "msa", "source": "census_msa_2023", "vintage": 2023},
            "candidate": {"type": "coc", "vintage": 2025},
            "selector_ids": ["17460"],
            "min_share": 0.5,
        }

        recipe = load_recipe(data)

        assert isinstance(recipe.targets[0].containment_filter, ContainmentSpec)
        assert recipe.targets[0].containment_filter.selector_ids == ["17460"]

    def test_panel_containment_filter_requires_panel_output(self):
        data = _minimal_recipe()
        data["targets"][0]["outputs"] = ["diagnostics"]
        data["targets"][0]["containment_filter"] = {
            "container": {"type": "msa", "source": "census_msa_2023", "vintage": 2023},
            "candidate": {"type": "coc", "vintage": 2025},
        }

        with pytest.raises(RecipeLoadError, match="requires outputs to include 'panel'"):
            load_recipe(data)

    def test_panel_containment_filter_candidate_must_match_target_geometry(self):
        data = _minimal_recipe()
        data["targets"][0]["outputs"] = ["panel"]
        data["targets"][0]["containment_filter"] = {
            "container": {"type": "coc", "vintage": 2025},
            "candidate": {"type": "county", "vintage": 2023},
        }

        with pytest.raises(RecipeLoadError, match="candidate geometry must match"):
            load_recipe(data)

    def test_valid_panel_target_selector_ids_load(self):
        data = _minimal_recipe()
        data["targets"][0]["outputs"] = ["panel"]
        data["targets"][0]["selector_ids"] = ["COC-A", "COC-B"]

        recipe = load_recipe(data)

        assert recipe.targets[0].selector_ids == ["COC-A", "COC-B"]

    def test_target_selector_ids_requires_panel_output(self):
        data = _minimal_recipe()
        data["targets"][0]["outputs"] = ["containment"]
        data["targets"][0]["containment_spec"] = {
            "container": {"type": "coc", "vintage": 2025},
            "candidate": {"type": "county", "vintage": 2023},
        }
        data["targets"][0]["selector_ids"] = ["COC-A"]

        with pytest.raises(RecipeLoadError, match="selector_ids.*requires outputs"):
            load_recipe(data)

    @pytest.mark.parametrize("selector_ids", [[], ["COC-A", " "]])
    def test_target_selector_ids_rejects_empty_or_blank_items(self, selector_ids):
        data = _minimal_recipe()
        data["targets"][0]["selector_ids"] = selector_ids

        with pytest.raises(RecipeLoadError, match="selector_ids"):
            load_recipe(data)

    @pytest.mark.parametrize(
        ("container", "candidate", "selector_ids"),
        [
            pytest.param(
                {"type": "msa", "source": "census_msa_2023", "vintage": 2023},
                {"type": "coc", "vintage": 2025},
                ["17460"],
                id="msa-coc",
            ),
            pytest.param(
                {"type": "coc", "vintage": 2025},
                {"type": "county", "vintage": 2023},
                ["CA-600"],
                id="coc-county",
            ),
        ],
    )
    def test_valid_containment_target_loads(self, container, candidate, selector_ids):
        data = _minimal_recipe()
        data["targets"][0]["outputs"] = ["containment"]
        data["targets"][0]["containment_spec"] = {
            "container": container,
            "candidate": candidate,
            "selector_ids": selector_ids,
            "candidate_selector_ids": ["candidate-1"],
            "min_share": 0.2,
            "denominator": "candidate_area",
            "method": "planar_intersection",
            "definition_version": "test_definition_v1",
        }
        recipe = load_recipe(data)
        spec = recipe.targets[0].containment_spec
        assert isinstance(spec, ContainmentSpec)
        assert spec.container.type == container["type"]
        assert spec.candidate.type == candidate["type"]
        assert spec.selector_ids == selector_ids
        assert spec.candidate_selector_ids == ["candidate-1"]
        assert spec.min_share == pytest.approx(0.2)

    @pytest.mark.parametrize(
        ("container", "candidate"),
        [
            pytest.param(
                {"type": "tract", "vintage": 2020},
                {"type": "coc", "vintage": 2025},
                id="tract-coc",
            ),
            pytest.param(
                {"type": "coc", "vintage": 2025},
                {"type": "msa", "vintage": 2023},
                id="coc-msa",
            ),
        ],
    )
    def test_containment_rejects_unsupported_geometry_pair(self, container, candidate):
        data = _minimal_recipe()
        data["targets"][0]["outputs"] = ["containment"]
        data["targets"][0]["containment_spec"] = {
            "container": container,
            "candidate": candidate,
        }
        with pytest.raises(
            RecipeLoadError,
            match="Unsupported containment geometry pair .* Supported pairs:",
        ):
            load_recipe(data)

    @pytest.mark.parametrize(
        "field",
        ["selector_ids", "candidate_selector_ids"],
    )
    def test_containment_rejects_empty_selector_lists(self, field):
        data = _minimal_recipe()
        data["targets"][0]["outputs"] = ["containment"]
        data["targets"][0]["containment_spec"] = {
            "container": {"type": "msa", "source": "census_msa_2023", "vintage": 2023},
            "candidate": {"type": "coc", "vintage": 2025},
            field: [],
        }
        with pytest.raises(RecipeLoadError, match="selector lists may not be empty"):
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

    def test_defaults_accept_county_pep(self):
        from hhplab.recipe.default_adapters import register_defaults

        geometry_registry.reset()
        dataset_registry.reset()
        register_defaults()

        spec = DatasetSpec(
            provider="census",
            product="pep",
            version=1,
            native_geometry=GeometryRef(type="county", vintage=2020),
        )
        assert dataset_registry.validate(spec) == []


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

    def test_defaults_accept_metro_target_and_preaggregated_datasets(self):
        from hhplab.recipe.default_adapters import register_defaults

        geometry_registry.reset()
        dataset_registry.reset()
        register_defaults()

        recipe = load_recipe(_recipe_with_metro_pipeline())
        diags = validate_recipe_adapters(recipe, geometry_registry, dataset_registry)
        errors = [d.message for d in diags if d.level == "error"]
        assert errors == []


# ===========================================================================
# CLI tests
# ===========================================================================


class TestRecipeCLI:
    def test_missing_recipe_file(self, tmp_path: Path):
        result = runner.invoke(
            app,
            [
                "build",
                "recipe",
                "--recipe",
                str(tmp_path / "missing.yaml"),
            ],
        )
        assert result.exit_code == 2
        assert "not found" in result.output

    def test_valid_recipe_loads(self, tmp_path: Path):
        import yaml

        recipe_file = tmp_path / "recipe.yaml"
        recipe_file.write_text(yaml.dump(_minimal_recipe()), encoding="utf-8")
        result = runner.invoke(
            app,
            [
                "build",
                "recipe",
                "--recipe",
                str(recipe_file),
            ],
        )
        # Will have adapter errors (no adapters registered) but should load OK
        assert "Loaded recipe: test-recipe" in result.output

    def test_invalid_yaml_exits_2(self, tmp_path: Path):
        bad = tmp_path / "bad.yaml"
        bad.write_text("version: notanint\nname: bad", encoding="utf-8")
        result = runner.invoke(
            app,
            [
                "build",
                "recipe",
                "--recipe",
                str(bad),
            ],
        )
        assert result.exit_code == 2

    def test_schema_error_exits_2(self, tmp_path: Path):
        import yaml

        data = _minimal_recipe()
        del data["name"]
        f = tmp_path / "noname.yaml"
        f.write_text(yaml.dump(data), encoding="utf-8")
        result = runner.invoke(
            app,
            [
                "build",
                "recipe",
                "--recipe",
                str(f),
            ],
        )
        assert result.exit_code == 2

    def test_dry_run_succeeds(self, tmp_path: Path):
        import yaml

        recipe_file = tmp_path / "recipe.yaml"
        recipe_file.write_text(yaml.dump(_minimal_recipe()), encoding="utf-8")
        result = runner.invoke(
            app,
            [
                "build",
                "recipe",
                "--recipe",
                str(recipe_file),
                "--dry-run",
            ],
        )
        assert "Loaded recipe: test-recipe" in result.output

    def test_missing_static_path_reported(self, tmp_path: Path):
        import yaml

        data = _minimal_recipe()
        data["datasets"]["acs"]["path"] = "data/does_not_exist.parquet"
        data["datasets"]["acs"]["years"] = "2020-2022"
        recipe_file = tmp_path / "recipe.yaml"
        recipe_file.write_text(yaml.dump(data), encoding="utf-8")
        result = runner.invoke(
            app,
            [
                "build",
                "recipe",
                "--recipe",
                str(recipe_file),
            ],
        )
        # Missing dataset now routes through shared preflight output
        assert "does_not_exist.parquet" in result.output
        assert "blocker" in result.output.lower() or "Preflight" in result.output

    def test_missing_file_set_paths_reported(self, tmp_path: Path):
        import yaml

        data = _recipe_with_file_set()
        # Narrow universe so we don't get too many missing-file messages
        data["universe"] = {"range": "2015-2015"}
        recipe_file = tmp_path / "recipe.yaml"
        recipe_file.write_text(yaml.dump(data), encoding="utf-8")
        result = runner.invoke(
            app,
            [
                "build",
                "recipe",
                "--recipe",
                str(recipe_file),
            ],
        )
        # Missing file_set paths now route through shared preflight output
        assert "acs_2015.parquet" in result.output or "missing" in result.output.lower()
        assert "blocker" in result.output.lower() or "Preflight" in result.output

    def test_existing_path_no_missing_file_error(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ):
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
        result = runner.invoke(
            app,
            [
                "build",
                "recipe",
                "--recipe",
                str(recipe_file),
            ],
        )
        assert "Missing file" not in result.output

    def test_optional_dataset_missing_warns_not_errors(self, tmp_path: Path):
        import yaml

        data = _minimal_recipe()
        data["datasets"]["acs"]["path"] = "data/missing.parquet"
        data["datasets"]["acs"]["years"] = "2020-2022"
        data["datasets"]["acs"]["optional"] = True
        recipe_file = tmp_path / "recipe.yaml"
        recipe_file.write_text(yaml.dump(data), encoding="utf-8")
        result = runner.invoke(
            app,
            [
                "build",
                "recipe",
                "--recipe",
                str(recipe_file),
            ],
        )
        # Optional missing datasets appear as preflight warning, not blocker
        assert "missing.parquet" in result.output
        assert "Warning" in result.output or "warning" in result.output.lower()
        assert "all clear" in result.output

    def test_policy_default_warn_downgrades_to_warning(self, tmp_path: Path):
        import yaml

        data = _minimal_recipe()
        data["datasets"]["acs"]["path"] = "data/missing.parquet"
        data["datasets"]["acs"]["years"] = "2020-2022"
        data["validation"] = {"missing_dataset": {"default": "warn"}}
        recipe_file = tmp_path / "recipe.yaml"
        recipe_file.write_text(yaml.dump(data), encoding="utf-8")
        result = runner.invoke(
            app,
            [
                "build",
                "recipe",
                "--recipe",
                str(recipe_file),
            ],
        )
        assert "missing.parquet" in result.output
        assert "Warning" in result.output or "warning" in result.output.lower()
        assert "all clear" in result.output

    def test_per_dataset_policy_override(self, tmp_path: Path):
        import yaml

        data = _minimal_recipe()
        data["datasets"]["acs"]["path"] = "data/missing.parquet"
        data["datasets"]["acs"]["years"] = "2020-2022"
        # Default is fail, but override acs to warn
        data["validation"] = {"missing_dataset": {"default": "fail", "acs": "warn"}}
        recipe_file = tmp_path / "recipe.yaml"
        recipe_file.write_text(yaml.dump(data), encoding="utf-8")
        result = runner.invoke(
            app,
            [
                "build",
                "recipe",
                "--recipe",
                str(recipe_file),
            ],
        )
        # Per-dataset warn policy downgrades to preflight warning
        assert "Warning" in result.output or "warning" in result.output.lower()
        assert "all clear" in result.output

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
        result = runner.invoke(
            app,
            [
                "build",
                "recipe",
                "--recipe",
                str(recipe_file),
            ],
        )
        # Per-dataset fail policy overrides optional → preflight blocker
        assert "blocker" in result.output.lower() or "Preflight" in result.output


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


class TestTransformSetSchema:
    def test_transform_set_accepted_for_aggregate(self):
        data = _recipe_with_file_set()
        resample = data["pipelines"][0]["steps"][0]["resample"]
        resample.pop("via")
        resample["transform_set"] = {
            "segments": [
                {"years": {"range": "2015-2019"}, "via": "coc_to_tract_2010"},
                {"years": {"range": "2020-2024"}, "via": "coc_to_tract_2020"},
            ]
        }

        recipe = load_recipe(data)

        step = recipe.pipelines[0].steps[0]
        assert step.transform_set is not None
        assert [segment.via for segment in step.transform_set.segments] == [
            "coc_to_tract_2010",
            "coc_to_tract_2020",
        ]

    def test_transform_set_and_via_are_mutually_exclusive(self):
        data = _recipe_with_file_set()
        data["pipelines"][0]["steps"][0]["resample"]["transform_set"] = {
            "segments": [{"years": {"range": "2015-2024"}, "via": "coc_to_tract_2010"}]
        }

        with pytest.raises(RecipeLoadError, match="either 'via' or 'transform_set'"):
            load_recipe(data)

    def test_identity_rejects_transform_set(self):
        data = _minimal_recipe()
        resample = data["pipelines"][0]["steps"][1]
        resample["method"] = "identity"
        resample.pop("via")
        resample["transform_set"] = {
            "segments": [{"years": {"range": "2020-2022"}, "via": "coc_to_tract_2010"}]
        }

        with pytest.raises(RecipeLoadError, match="must not set 'via' or 'transform_set'"):
            load_recipe(data)

    def test_transform_set_rejects_unknown_transform(self):
        data = _recipe_with_file_set()
        resample = data["pipelines"][0]["steps"][0]["resample"]
        resample.pop("via")
        resample["transform_set"] = {
            "segments": [{"years": {"range": "2015-2024"}, "via": "missing_transform"}]
        }

        with pytest.raises(RecipeLoadError, match="transform_set references unknown transforms"):
            load_recipe(data)

    def test_transform_set_rejects_overlapping_years(self):
        data = _recipe_with_file_set()
        resample = data["pipelines"][0]["steps"][0]["resample"]
        resample.pop("via")
        resample["transform_set"] = {
            "segments": [
                {"years": {"range": "2015-2020"}, "via": "coc_to_tract_2010"},
                {"years": {"range": "2020-2024"}, "via": "coc_to_tract_2020"},
            ]
        }

        with pytest.raises(RecipeLoadError, match="TransformSetSpec segments overlap"):
            load_recipe(data)


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
        data["pipelines"][0]["steps"].append({"kind": "join", "datasets": ["acs"]})
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


# ===========================================================================
# MissingDatasetPolicy validation tests
# ===========================================================================


class TestMissingDatasetPolicyValidation:
    def test_valid_per_dataset_policy_accepted(self):
        """Per-dataset policy keys matching declared datasets are accepted."""
        data = _minimal_recipe()
        data["validation"] = {
            "missing_dataset": {"default": "fail", "acs": "warn"},
        }
        recipe = load_recipe(data)
        assert recipe.validation.missing_dataset.default == "fail"
        assert recipe.validation.missing_dataset.model_extra == {"acs": "warn"}

    def test_unknown_per_dataset_policy_key_rejected(self):
        """Per-dataset policy key not matching any declared dataset raises."""
        data = _minimal_recipe()
        data["validation"] = {
            "missing_dataset": {"default": "fail", "nonexistent_ds": "warn"},
        }
        with pytest.raises(RecipeLoadError, match="unknown dataset"):
            load_recipe(data)

    def test_typo_in_policy_key_rejected(self):
        """Typo in per-dataset policy key (e.g., 'acs_data' vs 'acs') raises."""
        data = _minimal_recipe()
        data["validation"] = {
            "missing_dataset": {"default": "warn", "acs_data": "fail"},
        }
        with pytest.raises(RecipeLoadError, match="unknown dataset"):
            load_recipe(data)


class TestTemporalFilterPolicyValidation:
    def test_acs_temporal_filter_rejected(self):
        data = _minimal_recipe()
        data["filters"] = {
            "acs": {
                "type": "temporal",
                "column": "month",
                "method": "calendar_mean",
            },
        }
        with pytest.raises(RecipeLoadError, match="ACS estimates are annual"):
            load_recipe(data)

    def test_interpolate_to_month_rejected_for_non_pep_dataset(self):
        data = _minimal_recipe()
        data["datasets"]["acs"]["provider"] = "zillow"
        data["datasets"]["acs"]["product"] = "zori"
        data["filters"] = {
            "acs": {
                "type": "temporal",
                "column": "date",
                "method": "interpolate_to_month",
                "month": 1,
            },
        }
        with pytest.raises(RecipeLoadError, match="only supported for census/pep"):
            load_recipe(data)

    def test_interpolate_to_month_requires_january_for_pep(self):
        data = _minimal_recipe()
        data["datasets"]["acs"]["product"] = "pep"
        data["filters"] = {
            "acs": {
                "type": "temporal",
                "column": "reference_date",
                "method": "interpolate_to_month",
                "month": 7,
            },
        }
        with pytest.raises(RecipeLoadError, match="must target January"):
            load_recipe(data)


# ---------------------------------------------------------------------------
# Default adapter registration tests
# ---------------------------------------------------------------------------


class TestDefaultAdapters:
    """Tests for built-in adapter registration."""

    def test_register_defaults_idempotent(self):
        from hhplab.recipe.adapters import dataset_registry, geometry_registry
        from hhplab.recipe.default_adapters import register_defaults

        geometry_registry.reset()
        dataset_registry.reset()
        register_defaults()
        types_1 = geometry_registry.registered_types()
        products_1 = dataset_registry.registered_products()
        register_defaults()
        assert geometry_registry.registered_types() == types_1
        assert dataset_registry.registered_products() == products_1

    def test_geometry_types_registered(self):
        from hhplab.recipe.adapters import geometry_registry
        from hhplab.recipe.default_adapters import register_defaults

        geometry_registry.reset()
        register_defaults()
        assert "coc" in geometry_registry.registered_types()
        assert "tract" in geometry_registry.registered_types()
        assert "county" in geometry_registry.registered_types()

    def test_dataset_products_registered(self):
        from hhplab.recipe.adapters import dataset_registry
        from hhplab.recipe.default_adapters import register_defaults

        dataset_registry.reset()
        register_defaults()
        products = dataset_registry.registered_products()
        assert ("hud", "pit") in products
        assert ("census", "acs5") in products
        assert ("census", "acs") in products
        assert ("zillow", "zori") in products

    def test_coc_valid(self):
        from hhplab.recipe.default_geometry_adapters import _validate_coc

        diags = _validate_coc(GeometryRef(type="coc", vintage=2025, source="hud_exchange"))
        assert diags == []

    def test_coc_no_vintage_valid(self):
        from hhplab.recipe.default_geometry_adapters import _validate_coc

        diags = _validate_coc(GeometryRef(type="coc"))
        assert diags == []

    def test_coc_early_vintage_warns(self):
        from hhplab.recipe.default_geometry_adapters import _validate_coc

        diags = _validate_coc(GeometryRef(type="coc", vintage=1990))
        assert len(diags) == 1
        assert diags[0].level == "warning"

    def test_tract_decennial_valid(self):
        from hhplab.recipe.default_geometry_adapters import _validate_tract

        diags = _validate_tract(GeometryRef(type="tract", vintage=2020))
        assert diags == []

    def test_tract_non_decennial_warns(self):
        from hhplab.recipe.default_geometry_adapters import _validate_tract

        diags = _validate_tract(GeometryRef(type="tract", vintage=2023))
        assert len(diags) == 1
        assert diags[0].level == "warning"
        assert "decennial" in diags[0].message

    def test_hud_pit_valid(self):
        from hhplab.recipe.default_dataset_adapters import _validate_hud_pit

        spec = DatasetSpec(
            provider="hud",
            product="pit",
            version=1,
            native_geometry=GeometryRef(type="coc"),
            params={"vintage": 2024, "align": "point_in_time_jan"},
        )
        diags = _validate_hud_pit(spec)
        assert diags == []

    def test_hud_pit_bad_version(self):
        from hhplab.recipe.default_dataset_adapters import _validate_hud_pit

        spec = DatasetSpec(
            provider="hud",
            product="pit",
            version=2,
            native_geometry=GeometryRef(type="coc"),
        )
        diags = _validate_hud_pit(spec)
        assert any(d.level == "error" and "version" in d.message for d in diags)

    def test_hud_pit_wrong_geometry(self):
        from hhplab.recipe.default_dataset_adapters import _validate_hud_pit

        spec = DatasetSpec(
            provider="hud",
            product="pit",
            version=1,
            native_geometry=GeometryRef(type="tract"),
        )
        diags = _validate_hud_pit(spec)
        assert any(d.level == "error" and "coc" in d.message for d in diags)

    def test_hud_pit_unknown_params_warns(self):
        from hhplab.recipe.default_dataset_adapters import _validate_hud_pit

        spec = DatasetSpec(
            provider="hud",
            product="pit",
            version=1,
            native_geometry=GeometryRef(type="coc"),
            params={"vintage": 2024, "unknown_param": True},
        )
        diags = _validate_hud_pit(spec)
        assert any(d.level == "warning" and "unrecognized" in d.message for d in diags)

    def test_census_acs5_valid(self):
        from hhplab.recipe.default_dataset_adapters import _validate_census_acs5

        spec = DatasetSpec(
            provider="census",
            product="acs5",
            version=1,
            native_geometry=GeometryRef(type="tract", vintage=2020),
        )
        assert _validate_census_acs5(spec) == []

    def test_census_acs_valid(self):
        from hhplab.recipe.default_dataset_adapters import _validate_census_acs

        spec = DatasetSpec(
            provider="census",
            product="acs",
            version=1,
            native_geometry=GeometryRef(type="tract"),
        )
        assert _validate_census_acs(spec) == []

    def test_zillow_zori_valid(self):
        from hhplab.recipe.default_dataset_adapters import _validate_zillow_zori

        spec = DatasetSpec(
            provider="zillow",
            product="zori",
            version=1,
            native_geometry=GeometryRef(type="county"),
        )
        assert _validate_zillow_zori(spec) == []

    def test_zillow_zori_wrong_geometry_warns(self):
        from hhplab.recipe.default_dataset_adapters import _validate_zillow_zori

        spec = DatasetSpec(
            provider="zillow",
            product="zori",
            version=1,
            native_geometry=GeometryRef(type="zip"),
        )
        diags = _validate_zillow_zori(spec)
        assert any(d.level == "warning" and "county" in d.message for d in diags)

    def test_recipe_integration_no_adapter_errors(self):
        """Full recipe validation with defaults registered produces no errors."""
        from hhplab.recipe.adapters import dataset_registry, geometry_registry
        from hhplab.recipe.default_adapters import register_defaults

        geometry_registry.reset()
        dataset_registry.reset()
        register_defaults()

        recipe = load_recipe(
            {
                "version": 1,
                "name": "test",
                "universe": {"range": "2020-2022"},
                "targets": [{"id": "t", "geometry": {"type": "coc", "vintage": 2025}}],
                "datasets": {
                    "pit": {
                        "provider": "hud",
                        "product": "pit",
                        "version": 1,
                        "native_geometry": {"type": "coc"},
                        "params": {"vintage": 2024, "align": "point_in_time_jan"},
                    },
                },
            }
        )
        diags = validate_recipe_adapters(recipe, geometry_registry, dataset_registry)
        errors = [d for d in diags if d.level == "error"]
        assert errors == [], f"Unexpected errors: {[e.message for e in errors]}"


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
    boundaries_dir = tmp_path / "data" / "curated" / "coc_boundaries"
    xwalk_dir.mkdir(parents=True)
    boundaries_dir.mkdir(parents=True)
    xwalk = pd.DataFrame(
        {
            "coc_id": ["COC1", "COC2"],
            "tract_geoid": ["T1", "T2"],
            "area_share": [1.0, 1.0],
        }
    )
    xwalk.to_parquet(xwalk_dir / "xwalk__B2025xT2020.parquet")
    gpd.GeoDataFrame(
        {
            "coc_id": ["COC1", "COC2"],
            "coc_name": ["Test CoC 1", "Test CoC 2"],
            "boundary_vintage": ["2025", "2025"],
            "source": ["test_fixture", "test_fixture"],
        },
        geometry=[
            Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)]),
            Polygon([(1, 0), (2, 0), (2, 1), (1, 1), (1, 0)]),
        ],
        crs="EPSG:4326",
    ).to_parquet(boundaries_dir / "coc__B2025.parquet")

    # PIT dataset (identity passthrough) — includes both universe years
    pit_path = tmp_path / "data" / "pit.parquet"
    pit_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "coc_id": ["COC1", "COC2", "COC1", "COC2"],
            "year": [2020, 2020, 2021, 2021],
            "pit_total": [10, 20, 11, 21],
        }
    ).to_parquet(pit_path)

    # ACS dataset (aggregate) — includes both universe years
    acs_path = tmp_path / "data" / "acs.parquet"
    pd.DataFrame(
        {
            "GEOID": ["T1", "T2", "T1", "T2"],
            "year": [2020, 2020, 2021, 2021],
            "total_population": [100, 200, 110, 210],
        }
    ).to_parquet(acs_path)


def _default_recipe_output_dir(project_root: Path, recipe_name: str) -> Path:
    """Return the default per-recipe output directory."""
    return project_root / "outputs" / _recipe_output_dirname(recipe_name)


class TestRecipeOutputPaths:
    def test_recipe_output_dirname_normalizes_free_form_names(self):
        assert _recipe_output_dirname("Metro Executor Test") == "metro-executor-test"
        assert _recipe_output_dirname("  demo/report v1  ") == "demo-report-v1"

    def test_recipe_output_dirname_falls_back_when_name_has_no_safe_chars(self):
        assert _recipe_output_dirname("!!!") == "recipe"


def _recipe_with_metro_pipeline() -> dict:
    """Build a recipe that joins pre-aggregated metro artifacts."""
    return {
        "version": 1,
        "name": "metro-executor-test",
        "universe": {"range": "2020-2021"},
        "targets": [
            {
                "id": "metro_panel",
                "geometry": {"type": "metro", "source": "glynn_fox_v1"},
            },
        ],
        "datasets": {
            "pit": {
                "provider": "hud",
                "product": "pit",
                "version": 1,
                "native_geometry": {"type": "metro", "source": "glynn_fox_v1"},
                "years": "2020-2021",
                "path": "data/metro_pit.parquet",
            },
            "acs": {
                "provider": "census",
                "product": "acs5",
                "version": 1,
                "native_geometry": {"type": "metro", "source": "glynn_fox_v1"},
                "years": "2020-2021",
                "path": "data/metro_acs.parquet",
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
                            "dataset": "pit",
                            "to_geometry": {"type": "metro", "source": "glynn_fox_v1"},
                            "method": "identity",
                            "measures": ["pit_total"],
                        },
                    },
                    {
                        "resample": {
                            "dataset": "acs",
                            "to_geometry": {"type": "metro", "source": "glynn_fox_v1"},
                            "method": "identity",
                            "measures": ["total_population"],
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


def _setup_metro_pipeline_fixtures(tmp_path: Path) -> None:
    """Create metro-native dataset files for _recipe_with_metro_pipeline."""
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "metro_id": ["GF01", "GF02", "GF01", "GF02"],
            "year": [2020, 2020, 2021, 2021],
            "pit_total": [10, 20, 11, 21],
        }
    ).to_parquet(data_dir / "metro_pit.parquet")
    pd.DataFrame(
        {
            "metro_id": ["GF01", "GF02", "GF01", "GF02"],
            "year": [2020, 2020, 2021, 2021],
            "total_population": [100, 200, 110, 210],
        }
    ).to_parquet(data_dir / "metro_acs.parquet")


def _setup_curated_metro_artifacts(tmp_path: Path) -> None:
    """Write the curated Glynn/Fox metro definition artifacts for tests."""
    from hhplab.metro.metro_io import write_metro_artifacts

    write_metro_artifacts(base_dir=tmp_path / "data")


def _setup_curated_metro_universe_subset_artifacts(tmp_path: Path) -> None:
    """Write minimal canonical-metro and subset artifacts for recipe tests."""
    from hhplab.naming import (
        metro_subset_membership_path,
        metro_universe_path,
        msa_county_membership_path,
    )

    data_root = tmp_path / "data"
    universe_path = metro_universe_path("census_msa_2023", data_root)
    subset_path = metro_subset_membership_path(
        "glynn_fox_v1",
        "census_msa_2023",
        data_root,
    )
    msa_county_path = msa_county_membership_path("census_msa_2023", data_root)

    universe_path.parent.mkdir(parents=True, exist_ok=True)
    msa_county_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "metro_id": ["35620", "31080"],
            "cbsa_code": ["35620", "31080"],
            "metro_name": [
                "New York-Newark-Jersey City, NY-NJ-PA",
                "Los Angeles-Long Beach-Anaheim, CA",
            ],
            "definition_version": ["census_msa_2023", "census_msa_2023"],
        }
    ).to_parquet(universe_path)
    pd.DataFrame(
        {
            "metro_id": ["35620", "31080"],
            "cbsa_code": ["35620", "31080"],
            "profile": ["glynn_fox", "glynn_fox"],
            "profile_definition_version": ["glynn_fox_v1", "glynn_fox_v1"],
            "metro_definition_version": ["census_msa_2023", "census_msa_2023"],
            "profile_metro_id": ["GF01", "GF02"],
            "profile_metro_name": ["New York", "Los Angeles"],
            "profile_rank": [1, 2],
        }
    ).to_parquet(subset_path)
    pd.DataFrame(
        {
            "msa_id": ["35620", "31080"],
            "county_fips": ["36061", "06037"],
            "definition_version": ["census_msa_2023", "census_msa_2023"],
        }
    ).to_parquet(msa_county_path)


def _county_to_metro_recipe(
    *,
    name: str,
    target_geometry: dict[str, object],
    to_geometry: dict[str, object],
) -> dict[str, object]:
    """Build a minimal county-to-metro recipe for legacy/subset parity tests."""
    return {
        "version": 1,
        "name": name,
        "universe": {"range": "2020-2021"},
        "targets": [
            {
                "id": "metro_panel",
                "geometry": target_geometry,
            }
        ],
        "datasets": {
            "pep_county": {
                "provider": "census",
                "product": "pep",
                "version": 1,
                "native_geometry": {"type": "county", "vintage": 2020, "source": "tiger"},
                "years": "2020-2021",
                "year_column": "year",
                "geo_column": "county_fips",
                "path": "data/county_population.parquet",
            }
        },
        "transforms": [
            {
                "id": "county_to_metro",
                "type": "crosswalk",
                "from": {"type": "county", "vintage": 2020, "source": "tiger"},
                "to": to_geometry,
                "spec": {"weighting": {"scheme": "area"}},
            }
        ],
        "pipelines": [
            {
                "id": "build_metro_panel",
                "target": "metro_panel",
                "steps": [
                    {"materialize": {"transforms": ["county_to_metro"]}},
                    {
                        "resample": {
                            "dataset": "pep_county",
                            "to_geometry": to_geometry,
                            "method": "aggregate",
                            "via": "county_to_metro",
                            "measures": ["population"],
                            "aggregation": "sum",
                        }
                    },
                    {"join": {"datasets": ["pep_county"], "join_on": ["geo_id", "year"]}},
                ],
            }
        ],
    }


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

    def test_materialize_generates_subset_filtered_canonical_metro_artifact(self, tmp_path: Path):
        _setup_curated_metro_universe_subset_artifacts(tmp_path)

        data = _recipe_with_pipeline()
        data["targets"] = [
            {
                "id": "metro_panel",
                "geometry": {
                    "type": "metro",
                    "source": "census_msa_2023",
                    "subset_profile": "glynn_fox",
                    "subset_profile_definition_version": "glynn_fox_v1",
                },
            }
        ]
        data["pipelines"][0]["target"] = "metro_panel"
        data["pipelines"][0]["steps"] = [
            {"materialize": {"transforms": ["county_to_metro"]}},
        ]
        data["transforms"] = [
            {
                "id": "county_to_metro",
                "type": "crosswalk",
                "from": {"type": "county", "vintage": 2020},
                "to": {
                    "type": "metro",
                    "source": "census_msa_2023",
                    "subset_profile": "glynn_fox",
                    "subset_profile_definition_version": "glynn_fox_v1",
                },
                "spec": {"weighting": {"scheme": "area"}},
            }
        ]
        recipe = load_recipe(data)
        ctx = ExecutionContext(project_root=tmp_path, recipe=recipe)

        result = _execute_materialize(MaterializeTask(transform_ids=["county_to_metro"]), ctx)

        assert result.success
        xwalk = pd.read_parquet(ctx.transform_paths["county_to_metro"]).sort_values("metro_id")
        assert list(xwalk["metro_id"]) == ["31080", "35620"]
        assert list(xwalk["profile_metro_id"]) == ["GF02", "GF01"]
        assert (xwalk["definition_version"] == "census_msa_2023").all()

    def test_assemble_panel_adds_subset_profile_metadata_for_canonical_metros(self, tmp_path: Path):
        _setup_curated_metro_universe_subset_artifacts(tmp_path)

        recipe = load_recipe(
            {
                "version": 1,
                "name": "metro-subset-panel",
                "universe": {"years": [2023]},
                "targets": [
                    {
                        "id": "metro_panel",
                        "geometry": {
                            "type": "metro",
                            "source": "census_msa_2023",
                            "subset_profile": "glynn_fox",
                            "subset_profile_definition_version": "glynn_fox_v1",
                        },
                    }
                ],
                "datasets": {},
                "transforms": [],
                "pipelines": [{"id": "main", "target": "metro_panel", "steps": []}],
            }
        )
        ctx = ExecutionContext(project_root=tmp_path, recipe=recipe)
        ctx.intermediates[("__joined__", 2023)] = pd.DataFrame(
            {
                "geo_id": ["35620", "31080"],
                "year": [2023, 2023],
                "pit_total": [100, 200],
            }
        )
        plan = ExecutionPlan(
            pipeline_id="main",
            join_tasks=[JoinTask(datasets=[], join_on=["geo_id", "year"], year=2023)],
        )

        assembled = _assemble_panel(plan, ctx)

        assert not isinstance(assembled, StepResult)
        panel = assembled.panel.sort_values("metro_id").reset_index(drop=True)
        assert list(panel["metro_id"]) == ["31080", "35620"]
        assert list(panel["geo_id"]) == ["31080", "35620"]
        assert list(panel["profile_definition_version"]) == ["glynn_fox_v1", "glynn_fox_v1"]
        assert list(panel["profile_metro_id"]) == ["GF02", "GF01"]
        assert panel.loc[0, "metro_name"] == "Los Angeles-Long Beach-Anaheim, CA"

    def test_execute_recipe_coc_panel_preserves_name_and_derives_density(
        self,
        tmp_path: Path,
    ):
        _setup_pipeline_fixtures(tmp_path)
        pd.DataFrame(
            {
                "coc_id": ["COC1", "COC2", "COC1", "COC2"],
                "year": [2020, 2020, 2021, 2021],
                "pit_total": [10, 20, 11, 21],
                "pit_sheltered": [6, 12, 7, 13],
                "pit_unsheltered": [4, 8, 4, 8],
            }
        ).to_parquet(tmp_path / "data" / "pit.parquet")

        data = _recipe_with_pipeline()
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        data["datasets"]["acs"]["path"] = "data/acs.parquet"
        data["pipelines"][0]["steps"][1]["resample"]["measures"] = [
            "pit_total",
            "pit_sheltered",
            "pit_unsheltered",
        ]

        recipe = load_recipe(data)
        results = execute_recipe(recipe, project_root=tmp_path)
        assert results[0].success

        panel_path = (
            _default_recipe_output_dir(tmp_path, "executor-test")
            / "panel__Y2020-2021@B2025.parquet"
        )
        panel = pd.read_parquet(panel_path).sort_values(["geo_id", "year"]).reset_index(drop=True)
        areas = (
            _load_coc_areas("2025", boundaries_dir=tmp_path / "data" / "curated" / "coc_boundaries")
            .set_index("coc_id")["coc_area_sq_km"]
            .to_dict()
        )

        assert list(panel["coc_name"]) == [
            "Test CoC 1",
            "Test CoC 1",
            "Test CoC 2",
            "Test CoC 2",
        ]
        assert list(panel["pit_sheltered"]) == [6, 7, 12, 13]
        assert list(panel["pit_unsheltered"]) == [4, 4, 8, 8]
        expected_density = [
            100.0 / areas["COC1"],
            110.0 / areas["COC1"],
            200.0 / areas["COC2"],
            210.0 / areas["COC2"],
        ]
        assert list(panel["population_density_per_sq_km"]) == pytest.approx(expected_density)

    def test_execute_recipe_rejects_stale_translated_acs_cache(self, tmp_path: Path):
        _setup_pipeline_fixtures(tmp_path)
        stale_path = tmp_path / STALE_TRANSLATED_ACS_PATH
        stale_path.parent.mkdir(parents=True, exist_ok=True)
        _write_stale_translated_acs_cache(stale_path)

        data = _recipe_with_pipeline()
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        data["datasets"]["acs"]["path"] = STALE_TRANSLATED_ACS_PATH
        recipe = load_recipe(data)

        with pytest.raises(ExecutorError) as exc_info:
            execute_recipe(recipe, project_root=tmp_path)
        message = str(exc_info.value)
        assert "stale translated ACS tract cache" in message
        assert STALE_TRANSLATED_ACS_REBUILD in message

    def test_execute_recipe_rejects_implicit_static_broadcast(self, tmp_path: Path):
        """A yearless dataset should not be silently reused across many years."""
        _setup_pipeline_fixtures(tmp_path)
        acs_path = tmp_path / "data" / "acs.parquet"
        pd.DataFrame(
            {
                "GEOID": ["T1", "T2"],
                "total_population": [100, 200],
            }
        ).to_parquet(acs_path)

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
        pd.DataFrame(
            {
                "GEOID": ["T1", "T2"],
                "total_population": [100, 200],
            }
        ).to_parquet(acs_path)

        data = _recipe_with_pipeline()
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        data["datasets"]["acs"]["path"] = "data/acs.parquet"
        data["datasets"]["acs"]["params"] = {"broadcast_static": True}
        recipe = load_recipe(data)

        results = execute_recipe(recipe, project_root=tmp_path)
        assert results[0].success

        panel_path = (
            _default_recipe_output_dir(tmp_path, "executor-test")
            / "panel__Y2020-2021@B2025.parquet"
        )
        panel = pd.read_parquet(panel_path).sort_values(["geo_id", "year"])
        assert list(panel["total_population"]) == [100.0, 100.0, 200.0, 200.0]
        assert list(panel["acs5_vintage_used"]) == ["2020", "2021", "2020", "2021"]

    def test_execute_recipe_allows_yearless_file_set_with_distinct_paths(
        self,
        tmp_path: Path,
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

        pd.DataFrame(
            {
                "GEOID": ["T1", "T2"],
                "total_population": [100, 200],
            }
        ).to_parquet(tmp_path / "data" / "acs_2020.parquet")
        pd.DataFrame(
            {
                "GEOID": ["T1", "T2"],
                "total_population": [110, 210],
            }
        ).to_parquet(tmp_path / "data" / "acs_2021.parquet")

        recipe = load_recipe(data)
        results = execute_recipe(recipe, project_root=tmp_path)
        assert results[0].success

        panel_path = (
            _default_recipe_output_dir(tmp_path, "executor-test")
            / "panel__Y2020-2021@B2025.parquet"
        )
        panel = pd.read_parquet(panel_path).sort_values(["geo_id", "year"])
        assert list(panel["total_population"]) == [100.0, 110.0, 200.0, 210.0]
        assert list(panel["acs5_vintage_used"]) == ["2020", "2021", "2020", "2021"]

    def test_execute_recipe_rejects_file_set_reusing_same_static_path(
        self,
        tmp_path: Path,
    ):
        """file_set should still fail when all years resolve to one static file."""
        _setup_pipeline_fixtures(tmp_path)
        static_path = tmp_path / "data" / "acs_static.parquet"
        pd.DataFrame(
            {
                "GEOID": ["T1", "T2"],
                "total_population": [100, 200],
            }
        ).to_parquet(static_path)

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
        data["pipelines"][0]["steps"].insert(
            1,
            {
                "resample": {
                    "dataset": "acs",
                    "to_geometry": {"type": "county"},
                    "method": "aggregate",
                    "via": "auto",
                    "measures": ["total_population"],
                },
            },
        )
        recipe = load_recipe(data)
        with pytest.raises(ExecutorError, match="Pipeline 'main'.*planning failed"):
            execute_recipe(recipe, project_root=tmp_path)

    def test_execute_recipe_supports_metro_target(self, tmp_path: Path):
        _setup_metro_pipeline_fixtures(tmp_path)
        recipe = load_recipe(_recipe_with_metro_pipeline())
        results = execute_recipe(recipe, project_root=tmp_path)
        assert results[0].success

        panel_path = (
            _default_recipe_output_dir(tmp_path, "metro-executor-test")
            / "panel__metro__Y2020-2021@Dglynnfoxv1.parquet"
        )
        panel = pd.read_parquet(panel_path).sort_values(["geo_id", "year"])
        assert list(panel["geo_id"]) == ["GF01", "GF01", "GF02", "GF02"]
        assert (panel["geo_type"] == "metro").all()
        assert list(panel["metro_id"]) == ["GF01", "GF01", "GF02", "GF02"]
        assert (panel["definition_version_used"] == "glynn_fox_v1").all()

    def test_execute_recipe_explicit_subset_matches_legacy_glynn_fox_outputs(
        self,
        tmp_path: Path,
    ):
        _setup_curated_metro_artifacts(tmp_path)
        _setup_curated_metro_universe_subset_artifacts(tmp_path)
        pd.DataFrame(
            {
                "county_fips": ["36061", "06037", "36061", "06037"],
                "year": [2020, 2020, 2021, 2021],
                "population": [1000, 2000, 1100, 2100],
            }
        ).to_parquet(tmp_path / "data" / "county_population.parquet")

        legacy_recipe = load_recipe(
            _county_to_metro_recipe(
                name="legacy-glynn-fox",
                target_geometry={"type": "metro", "source": "glynn_fox_v1"},
                to_geometry={"type": "metro", "source": "glynn_fox_v1"},
            )
        )
        subset_recipe = load_recipe(
            _county_to_metro_recipe(
                name="canonical-subset-glynn-fox",
                target_geometry={
                    "type": "metro",
                    "source": "census_msa_2023",
                    "subset_profile": "glynn_fox",
                    "subset_profile_definition_version": "glynn_fox_v1",
                },
                to_geometry={
                    "type": "metro",
                    "source": "census_msa_2023",
                    "subset_profile": "glynn_fox",
                    "subset_profile_definition_version": "glynn_fox_v1",
                },
            )
        )

        legacy_results = execute_recipe(legacy_recipe, project_root=tmp_path)
        subset_results = execute_recipe(subset_recipe, project_root=tmp_path)

        assert legacy_results[0].success
        assert subset_results[0].success

        legacy_panel_path = next(
            _default_recipe_output_dir(tmp_path, "legacy-glynn-fox").glob("panel__metro__*.parquet")
        )
        subset_panel_path = next(
            _default_recipe_output_dir(tmp_path, "canonical-subset-glynn-fox").glob(
                "panel__metro__*.parquet"
            )
        )
        legacy_panel = (
            pd.read_parquet(legacy_panel_path)
            .sort_values(["metro_id", "year"])
            .reset_index(drop=True)
        )
        subset_panel = (
            pd.read_parquet(subset_panel_path)
            .sort_values(["profile_metro_id", "year"])
            .reset_index(drop=True)
        )

        assert list(subset_panel["metro_id"]) == ["35620", "35620", "31080", "31080"]
        assert list(subset_panel["geo_id"]) == ["35620", "35620", "31080", "31080"]
        assert list(subset_panel["profile_metro_id"]) == ["GF01", "GF01", "GF02", "GF02"]

        legacy_projection = legacy_panel[["metro_id", "year", "population"]].rename(
            columns={"metro_id": "profile_metro_id"}
        )
        subset_projection = subset_panel[["profile_metro_id", "year", "population"]]
        pd.testing.assert_frame_equal(legacy_projection, subset_projection, check_dtype=False)

        subset_canonical_names = (
            subset_panel[["profile_metro_id", "metro_name"]]
            .drop_duplicates()
            .sort_values("profile_metro_id")
            .reset_index(drop=True)
        )
        assert list(subset_canonical_names["metro_name"]) == [
            "New York-Newark-Jersey City, NY-NJ-PA",
            "Los Angeles-Long Beach-Anaheim, CA",
        ]
        subset_profile_names = (
            subset_panel[["profile_metro_id", "profile_metro_name"]]
            .drop_duplicates()
            .sort_values("profile_metro_id")
            .reset_index(drop=True)
        )
        assert list(subset_profile_names["profile_metro_name"]) == ["New York", "Los Angeles"]


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
        """targets with outputs=['diagnostics'] should persist diagnostics but not panel."""
        _setup_pipeline_fixtures(tmp_path)
        data = _recipe_with_pipeline()
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        data["datasets"]["acs"]["path"] = "data/acs.parquet"
        data["targets"][0]["outputs"] = ["diagnostics"]
        recipe = load_recipe(data)
        results = execute_recipe(recipe, project_root=tmp_path)
        kinds = [s.step_kind for s in results[0].steps]
        # Panel persistence should be skipped
        assert "persist" not in kinds
        # Diagnostics persistence should run
        assert "persist_diagnostics" in kinds
        # Verify the diagnostics JSON file was written
        diag_files = list(
            _default_recipe_output_dir(tmp_path, "executor-test").glob("*__diagnostics.json")
        )
        assert len(diag_files) == 1

    def test_map_output_adds_explicit_failure_step(self, tmp_path: Path):
        """Map outputs should persist a recipe-native HTML artifact."""
        _setup_pipeline_fixtures(tmp_path)
        data = _recipe_with_pipeline()
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        data["datasets"]["acs"]["path"] = "data/acs.parquet"
        data["targets"][0]["outputs"] = ["map"]
        data["targets"][0]["map_spec"] = {
            "layers": [
                {
                    "geometry": {"type": "coc", "vintage": 2025},
                    "selector_ids": ["COC1"],
                }
            ]
        }
        recipe = load_recipe(data)
        results = execute_recipe(recipe, project_root=tmp_path)
        map_steps = [s for s in results[0].steps if s.step_kind == "persist_map"]
        assert len(map_steps) == 1
        assert results[0].success
        assert map_steps[0].success
        assert (
            _default_recipe_output_dir(tmp_path, "executor-test") / "map__Y2020-2021@B2025.html"
        ).exists()


class TestPersistDiagnostics:
    """Dedicated tests for _persist_diagnostics output generation (coclab-yorw)."""

    def _run_with_diagnostics(self, tmp_path: Path, *, outputs=None):
        """Helper: execute a recipe requesting diagnostics and return results."""
        _setup_pipeline_fixtures(tmp_path)
        data = _recipe_with_pipeline()
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        data["datasets"]["acs"]["path"] = "data/acs.parquet"
        if outputs is not None:
            data["targets"][0]["outputs"] = outputs
        recipe = load_recipe(data)
        return execute_recipe(recipe, project_root=tmp_path)

    def test_diagnostics_file_written_to_expected_path(self, tmp_path: Path):
        """Diagnostics JSON is written to the recipe output directory with correct stem."""
        self._run_with_diagnostics(tmp_path, outputs=["diagnostics"])
        expected = (
            _default_recipe_output_dir(tmp_path, "executor-test")
            / "panel__Y2020-2021@B2025__diagnostics.json"
        )
        assert expected.exists(), f"Expected diagnostics file at {expected}"

    def test_diagnostics_file_is_valid_json_with_expected_keys(self, tmp_path: Path):
        """Diagnostics file must be valid JSON containing the DiagnosticsReport keys."""
        self._run_with_diagnostics(tmp_path, outputs=["diagnostics"])
        diag_path = (
            _default_recipe_output_dir(tmp_path, "executor-test")
            / "panel__Y2020-2021@B2025__diagnostics.json"
        )
        data = json.loads(diag_path.read_text())
        expected_keys = {"coverage", "boundary_changes", "missingness", "weighting", "panel_info"}
        assert set(data.keys()) == expected_keys

    def test_diagnostics_panel_info_reflects_fixture_data(self, tmp_path: Path):
        """panel_info section should contain row_count, year range, and geo info."""
        self._run_with_diagnostics(tmp_path, outputs=["diagnostics"])
        diag_path = (
            _default_recipe_output_dir(tmp_path, "executor-test")
            / "panel__Y2020-2021@B2025__diagnostics.json"
        )
        info = json.loads(diag_path.read_text())["panel_info"]
        assert info["row_count"] > 0
        assert info["year_min"] == 2020
        assert info["year_max"] == 2021

    def test_diagnostics_coverage_and_missingness_are_lists(self, tmp_path: Path):
        """coverage and missingness should serialize as list-of-dicts (records)."""
        self._run_with_diagnostics(tmp_path, outputs=["diagnostics"])
        diag_path = (
            _default_recipe_output_dir(tmp_path, "executor-test")
            / "panel__Y2020-2021@B2025__diagnostics.json"
        )
        data = json.loads(diag_path.read_text())
        assert isinstance(data["coverage"], list)
        assert isinstance(data["missingness"], list)

    def test_diagnostics_step_result_on_success(self, tmp_path: Path):
        """persist_diagnostics step should report success with file path detail."""
        results = self._run_with_diagnostics(tmp_path, outputs=["diagnostics"])
        diag_steps = [s for s in results[0].steps if s.step_kind == "persist_diagnostics"]
        assert len(diag_steps) == 1
        step = diag_steps[0]
        assert step.success
        assert "__diagnostics.json" in step.detail

    def test_diagnostics_no_joined_outputs_fails(self, tmp_path: Path):
        """When no joined intermediates exist, persist_diagnostics returns failure."""
        from hhplab.recipe.executor import _persist_diagnostics

        _setup_pipeline_fixtures(tmp_path)
        data = _recipe_with_pipeline()
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        data["datasets"]["acs"]["path"] = "data/acs.parquet"
        recipe = load_recipe(data)
        plan = resolve_plan(recipe, "main")
        # Build context with empty intermediates (no joined data)
        ctx = ExecutionContext(
            project_root=tmp_path,
            recipe=recipe,
            quiet=True,
        )
        result = _persist_diagnostics(plan, ctx)
        assert not result.success
        assert result.step_kind == "persist_diagnostics"
        assert "No joined outputs" in result.error

    def test_diagnostics_alongside_panel(self, tmp_path: Path):
        """When outputs=['panel', 'diagnostics'], both artifacts are produced."""
        self._run_with_diagnostics(tmp_path, outputs=["panel", "diagnostics"])
        panel_dir = _default_recipe_output_dir(tmp_path, "executor-test")
        parquet_files = list(panel_dir.glob("*.parquet"))
        diag_files = list(panel_dir.glob("*__diagnostics.json"))
        assert len(parquet_files) >= 1, "Panel parquet should be written"
        assert len(diag_files) == 1, "Diagnostics JSON should be written"


class TestPanelPolicy:
    """Tests for declarative PanelPolicy in recipe schema (coclab-gude.4)."""

    def test_panel_policy_defaults(self):
        policy = PanelPolicy()
        assert policy.source_label is None
        assert policy.zori is None
        assert policy.acs1 is None
        assert policy.canonical_population_source is None
        assert policy.column_aliases == {}

    def test_zori_policy_defaults(self):
        zori = ZoriPolicy()
        assert zori.min_coverage == 0.90

    def test_zori_policy_custom(self):
        zori = ZoriPolicy(min_coverage=0.80)
        assert zori.min_coverage == 0.80

    def test_acs1_policy_defaults(self):
        acs1 = Acs1Policy()
        assert acs1.include is False

    def test_panel_policy_with_aliases(self):
        policy = PanelPolicy(
            column_aliases={
                "total_population": "total_population_acs5",
                "population": "pep_population",
            },
        )
        assert policy.column_aliases["total_population"] == "total_population_acs5"
        assert policy.column_aliases["population"] == "pep_population"

    def test_target_with_panel_policy(self):
        data = _minimal_recipe()
        data["targets"][0]["panel_policy"] = {
            "source_label": "custom_source",
            "zori": {"min_coverage": 0.85},
            "canonical_population_source": "acs5",
            "column_aliases": {"total_population": "total_population_acs5"},
        }
        recipe = load_recipe(data)
        target = recipe.targets[0]
        assert target.panel_policy is not None
        assert target.panel_policy.source_label == "custom_source"
        assert target.panel_policy.zori.min_coverage == 0.85
        assert target.panel_policy.canonical_population_source == "acs5"
        assert target.panel_policy.column_aliases == {
            "total_population": "total_population_acs5",
        }

    def test_target_without_panel_policy(self):
        data = _minimal_recipe()
        recipe = load_recipe(data)
        assert recipe.targets[0].panel_policy is None

    def test_panel_policy_rejects_extra_fields(self):
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            PanelPolicy(unknown_field="bad")

    def test_zori_coverage_bounds(self):
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            ZoriPolicy(min_coverage=1.5)
        with pytest.raises(ValidationError):
            ZoriPolicy(min_coverage=-0.1)

    def test_full_panel_policy_round_trip(self):
        policy = PanelPolicy(
            source_label="test_panel",
            zori=ZoriPolicy(min_coverage=0.95),
            acs1=Acs1Policy(include=True),
            canonical_population_source="pep",
            column_aliases={"zori_coc": "zori"},
        )
        d = policy.model_dump()
        restored = PanelPolicy(**d)
        assert restored.source_label == "test_panel"
        assert restored.zori.min_coverage == 0.95
        assert restored.acs1.include is True
        assert restored.canonical_population_source == "pep"
        assert restored.column_aliases == {"zori_coc": "zori"}


class TestSmallAreaEstimateSchema:
    """Schema tests for explicit ACS1/ACS5 small-area estimation steps."""

    def test_sae_step_loads_with_explicit_contract(self):
        recipe = load_recipe(_sae_recipe())
        step = recipe.pipelines[0].steps[0]

        assert isinstance(step, SmallAreaEstimateStep)
        assert step.output_dataset == "acs_sae_coc"
        assert step.source_dataset == "acs1_county"
        assert step.support_dataset == "acs5_tract_support"
        assert step.source_geometry.type == "county"
        assert step.support_geometry.type == "tract"
        assert step.target_geometry == recipe.targets[0].geometry
        assert step.terminal_acs5_vintage == 2022
        assert step.tract_vintage == 2020
        assert step.allocation_method == "tract_share_within_county"
        assert step.zero_denominator_policy == "null_rate"
        assert step.fallback_policy == "diagnose_only"
        assert step.denominators["rent_burden"] == "gross_rent_pct_income_total"
        assert "rent_burden" in step.measures
        assert step.measures["rent_burden"].outputs == [
            "sae_rent_burden_30_plus",
            "sae_rent_burden_50_plus",
        ]
        assert step.diagnostics.direct_county_comparison is True

    def test_sae_wrapper_step_shorthand_loads(self):
        data = _sae_recipe()
        step = data["pipelines"][0]["steps"][0]
        data["pipelines"][0]["steps"][0] = {"small_area_estimate": step}

        recipe = load_recipe(data)

        assert isinstance(recipe.pipelines[0].steps[0], SmallAreaEstimateStep)

    def test_join_can_reference_sae_output_dataset(self):
        recipe = load_recipe(_sae_recipe())
        join_step = recipe.pipelines[0].steps[1]

        assert join_step.datasets == ["acs_sae_coc"]

    @pytest.mark.parametrize(
        ("field", "value", "message"),
        [
            ("source_geometry", {"type": "tract", "vintage": 2020}, "source_geometry.type"),
            ("support_geometry", {"type": "county", "vintage": 2020}, "support_geometry.type"),
        ],
    )
    def test_sae_rejects_invalid_source_or_support_geometry(self, field, value, message):
        data = _sae_recipe()
        data["pipelines"][0]["steps"][0][field] = value

        with pytest.raises(RecipeLoadError, match=message):
            load_recipe(data)

    def test_sae_rejects_support_geometry_vintage_mismatch(self):
        data = _sae_recipe()
        data["pipelines"][0]["steps"][0]["support_geometry"]["vintage"] = 2010

        with pytest.raises(RecipeLoadError, match="support_geometry.vintage must match"):
            load_recipe(data)

    def test_sae_rejects_direct_median_outputs(self):
        data = _sae_recipe()
        data["pipelines"][0]["steps"][0]["measures"]["household_income_bins"][
            "outputs"
        ] = ["median_household_income"]

        with pytest.raises(RecipeLoadError, match="direct ACS median/context columns"):
            load_recipe(data)

    def test_sae_rejects_non_sae_output_columns(self):
        with pytest.raises(ValueError, match="sae_"):
            SAEMeasureConfig(outputs=["rent_burden_30_plus"])

    def test_sae_rejects_direct_median_denominator(self):
        data = _sae_recipe()
        data["pipelines"][0]["steps"][0]["denominators"] = {
            "household_income_bins": "median_household_income",
        }

        with pytest.raises(RecipeLoadError, match="denominators cannot use direct ACS"):
            load_recipe(data)

    def test_sae_rejects_unknown_source_dataset(self):
        data = _sae_recipe()
        data["pipelines"][0]["steps"][0]["source_dataset"] = "missing_acs1"

        with pytest.raises(RecipeLoadError, match="unknown dataset"):
            load_recipe(data)

    def test_sae_output_dataset_must_not_conflict_with_declared_dataset(self):
        data = _sae_recipe()
        data["pipelines"][0]["steps"][0]["output_dataset"] = "acs1_county"

        with pytest.raises(RecipeLoadError, match="conflicts with a declared dataset"):
            load_recipe(data)

    def test_sae_target_geometry_must_match_pipeline_target(self):
        data = _sae_recipe()
        data["pipelines"][0]["steps"][0]["target_geometry"] = {
            "type": "metro",
            "source": "glynn_fox_v1",
        }

        with pytest.raises(RecipeLoadError, match="target_geometry must match"):
            load_recipe(data)

    def test_sae_diagnostics_defaults(self):
        diagnostics = SAEDiagnosticsSpec()

        assert diagnostics.conservation is True
        assert diagnostics.denominator is True
        assert diagnostics.direct_county_comparison is True

    def test_plan_exposes_resolved_sae_task_metadata(self):
        recipe = load_recipe(_sae_recipe())

        plan = resolve_plan(recipe, "main")

        assert len(plan.small_area_estimate_tasks) == 1
        task = plan.small_area_estimate_tasks[0]
        assert isinstance(task, SmallAreaEstimateTask)
        assert task.output_dataset == "acs_sae_coc"
        assert task.year == 2023
        assert task.source_dataset == "acs1_county"
        assert task.support_dataset == "acs5_tract_support"
        assert task.source_path == "data/curated/acs/acs1_county_sae__A2023.parquet"
        assert task.support_path == "data/curated/acs/acs5_tract_sae_support__A2022xT2020.parquet"
        assert task.source_geometry.type == "county"
        assert task.support_geometry.type == "tract"
        assert task.target_geometry.type == "coc"
        assert task.terminal_acs5_vintage == "2022"
        assert task.tract_vintage == "2020"
        assert task.allocation_method == "tract_share_within_county"
        assert task.denominators["rent_burden"] == "gross_rent_pct_income_total"
        assert task.measure_families == ["household_income_bins", "rent_burden"]
        assert task.derived_outputs["rent_burden"] == [
            "sae_rent_burden_30_plus",
            "sae_rent_burden_50_plus",
        ]
        assert task.diagnostics["direct_county_comparison"] is True

    def test_plan_serializes_sae_tasks_to_json_safe_dict(self):
        recipe = load_recipe(_sae_recipe())

        plan_dict = resolve_plan(recipe, "main").to_dict()

        assert plan_dict["task_count"] == 2
        sae_task = plan_dict["small_area_estimate_tasks"][0]
        assert sae_task["output_dataset"] == "acs_sae_coc"
        assert sae_task["source_geometry"] == {
            "type": "county",
            "vintage": 2020,
            "source": "tiger",
        }
        assert sae_task["diagnostics"]["conservation"] is True


class TestPipelineResult:
    def test_success_all_ok(self):
        r = PipelineResult(
            pipeline_id="test",
            steps=[
                StepResult(step_kind="resample", detail="ok", success=True),
                StepResult(step_kind="join", detail="ok", success=True),
            ],
        )
        assert r.success
        assert r.error_count == 0

    def test_failure_detected(self):
        r = PipelineResult(
            pipeline_id="test",
            steps=[
                StepResult(step_kind="resample", detail="ok", success=True),
                StepResult(step_kind="join", detail="boom", success=False, error="fail"),
            ],
        )
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
        result = runner.invoke(
            app,
            [
                "build",
                "recipe",
                "--recipe",
                str(recipe_file),
            ],
        )
        # Should see execution output (not just validation)
        assert "Executing pipeline" in result.output
        assert "completed" in result.output or "executed" in result.output

    def test_dry_run_does_not_execute(self, tmp_path: Path):
        import yaml

        recipe_file = tmp_path / "recipe.yaml"
        recipe_file.write_text(yaml.dump(_recipe_with_pipeline()), encoding="utf-8")
        result = runner.invoke(
            app,
            [
                "build",
                "recipe",
                "--recipe",
                str(recipe_file),
                "--dry-run",
            ],
        )
        assert "Executing pipeline" not in result.output

    def test_executor_error_exits_1(self, tmp_path: Path):
        import yaml

        data = _recipe_with_pipeline()
        # Add an auto-resample that can't resolve a transform
        data["pipelines"][0]["steps"].insert(
            1,
            {
                "resample": {
                    "dataset": "acs",
                    "to_geometry": {"type": "county"},
                    "method": "aggregate",
                    "via": "auto",
                    "measures": ["total_population"],
                },
            },
        )
        recipe_file = tmp_path / "recipe.yaml"
        recipe_file.write_text(yaml.dump(data), encoding="utf-8")
        result = runner.invoke(
            app,
            [
                "build",
                "recipe",
                "--recipe",
                str(recipe_file),
            ],
        )
        assert result.exit_code == 1
        # Preflight catches the planner error before execution starts
        assert "Preflight" in result.output or "blocker" in result.output


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
        data["transforms"] = [
            {
                "id": "county_to_coc",
                "type": "crosswalk",
                "from": {"type": "county", "vintage": 2023},
                "to": {"type": "coc", "vintage": 2025},
                "spec": {"weighting": {"scheme": "area"}},
            }
        ]
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

    def test_resolve_tract_mediated_county_crosswalk_path(self, tmp_path: Path):
        data = _recipe_with_pipeline()
        data["transforms"] = [
            {
                "id": "county_to_coc_tract_mediated",
                "type": "crosswalk",
                "from": {"type": "county", "vintage": 2020},
                "to": {"type": "coc", "vintage": 2025},
                "spec": {
                    "weighting": {
                        "scheme": "tract_mediated",
                        "variety": "renter_households",
                        "tract_vintage": 2020,
                        "acs_vintage": "2019-2023",
                    },
                },
            }
        ]
        data["pipelines"][0]["steps"] = [
            {"materialize": {"transforms": ["county_to_coc_tract_mediated"]}},
        ]
        recipe = load_recipe(data)

        path = _resolve_transform_path(
            "county_to_coc_tract_mediated",
            recipe,
            tmp_path,
        )

        assert "xwalk_tract_mediated_county__A2023@B2025xC2020xT2020" in str(path)

    def test_resolve_tract_mediated_decennial_county_crosswalk_path(
        self,
        tmp_path: Path,
    ):
        data = _recipe_with_pipeline()
        data["transforms"] = [
            {
                "id": "county_to_coc_tract_mediated",
                "type": "crosswalk",
                "from": {"type": "county", "vintage": 2020},
                "to": {"type": "coc", "vintage": 2025},
                "spec": {
                    "weighting": {
                        "scheme": "tract_mediated",
                        "variety": "population",
                        "tract_vintage": 2020,
                        "denominator_source": "decennial",
                        "denominator_vintage": 2020,
                    },
                },
            }
        ]
        data["pipelines"][0]["steps"] = [
            {"materialize": {"transforms": ["county_to_coc_tract_mediated"]}},
        ]
        recipe = load_recipe(data)

        path = _resolve_transform_path(
            "county_to_coc_tract_mediated",
            recipe,
            tmp_path,
        )

        assert "xwalk_tract_mediated_county__N2020@B2025xC2020xT2020" in str(path)

    def test_panel_output_filename_encodes_tract_mediated_varieties(self, tmp_path: Path):
        data = _recipe_with_pipeline()
        data["transforms"] = [
            {
                "id": "county_to_coc_tract_mediated",
                "type": "crosswalk",
                "from": {"type": "county", "vintage": 2020},
                "to": {"type": "coc", "vintage": 2025},
                "spec": {
                    "weighting": {
                        "scheme": "tract_mediated",
                        "varieties": ["area", "population"],
                        "tract_vintage": 2020,
                        "acs_vintage": 2023,
                    },
                },
            }
        ]
        data["pipelines"][0]["steps"] = [
            {"materialize": {"transforms": ["county_to_coc_tract_mediated"]}},
        ]
        recipe = load_recipe(data)

        path = _resolve_panel_output_file(recipe, "main", tmp_path)

        assert path.name.endswith("__warea-population.parquet")

    def test_resolve_coc_to_metro_crosswalk_path(self, tmp_path: Path):
        data = _recipe_with_pipeline()
        data["targets"] = [
            {
                "id": "metro_panel",
                "geometry": {"type": "metro", "source": "glynn_fox_v1"},
            }
        ]
        data["pipelines"][0]["target"] = "metro_panel"
        data["pipelines"][0]["steps"] = [
            {"materialize": {"transforms": ["coc_to_metro"]}},
        ]
        data["transforms"] = [
            {
                "id": "coc_to_metro",
                "type": "crosswalk",
                "from": {"type": "coc", "vintage": 2025},
                "to": {"type": "metro", "source": "glynn_fox_v1"},
                "spec": {"weighting": {"scheme": "area"}},
            }
        ]
        recipe = load_recipe(data)
        path = _resolve_transform_path("coc_to_metro", recipe, tmp_path)
        assert ".recipe_cache/transforms" in str(path)
        assert "coc_to_metro__coc_2025__glynn_fox_v1.parquet" in str(path)

    def test_resolve_coc_to_msa_crosswalk_path(self, tmp_path: Path):
        data = _recipe_with_pipeline()
        data["targets"] = [
            {
                "id": "msa_panel",
                "geometry": {"type": "msa", "source": "census_msa_2023"},
            }
        ]
        data["pipelines"][0]["target"] = "msa_panel"
        data["pipelines"][0]["steps"] = [
            {"materialize": {"transforms": ["coc_to_msa"]}},
        ]
        data["transforms"] = [
            {
                "id": "coc_to_msa",
                "type": "crosswalk",
                "from": {"type": "coc", "vintage": 2025},
                "to": {"type": "msa", "source": "census_msa_2023"},
                "spec": {"weighting": {"scheme": "area"}},
            }
        ]
        recipe = load_recipe(data)
        path = _resolve_transform_path("coc_to_msa", recipe, tmp_path)
        assert ".recipe_cache/transforms" in str(path)
        assert "coc_to_msa__coc_2025__census_msa_2023.parquet" in str(path)

    def test_unknown_transform_raises(self, tmp_path: Path):
        recipe = load_recipe(_recipe_with_pipeline())
        with pytest.raises(ExecutorError, match="not found in recipe"):
            _resolve_transform_path("nonexistent", recipe, tmp_path)

    def test_unsupported_geometry_pair_raises(self, tmp_path: Path):
        data = _recipe_with_pipeline()
        # zip↔state: no crosswalk path resolver for this pair
        data["transforms"] = [
            {
                "id": "zip_to_state",
                "type": "crosswalk",
                "from": {"type": "zip", "vintage": 2023},
                "to": {"type": "state", "vintage": 2023},
                "spec": {"weighting": {"scheme": "area"}},
            }
        ]
        data["pipelines"][0]["steps"] = [
            {"materialize": {"transforms": ["zip_to_state"]}},
        ]
        recipe = load_recipe(data)
        with pytest.raises(ExecutorError, match="no 'coc' geometry"):
            _resolve_transform_path("zip_to_state", recipe, tmp_path)

    def test_none_coc_vintage_raises(self, tmp_path: Path):
        data = _recipe_with_pipeline()
        data["transforms"] = [
            {
                "id": "tract_to_coc",
                "type": "crosswalk",
                "from": {"type": "tract", "vintage": 2020},
                "to": {"type": "coc"},  # no vintage
                "spec": {"weighting": {"scheme": "area"}},
            }
        ]
        recipe = load_recipe(data)
        with pytest.raises(ExecutorError, match="no vintage"):
            _resolve_transform_path("tract_to_coc", recipe, tmp_path)

    def test_none_base_vintage_raises(self, tmp_path: Path):
        data = _recipe_with_pipeline()
        data["transforms"] = [
            {
                "id": "tract_to_coc",
                "type": "crosswalk",
                "from": {"type": "tract"},  # no vintage
                "to": {"type": "coc", "vintage": 2025},
                "spec": {"weighting": {"scheme": "area"}},
            }
        ]
        recipe = load_recipe(data)
        with pytest.raises(ExecutorError, match="no vintage"):
            _resolve_transform_path("tract_to_coc", recipe, tmp_path)

    def test_materialize_reuses_existing_artifact(self, tmp_path: Path):
        ctx = self._make_ctx(tmp_path)
        # Create the expected crosswalk file
        xwalk_dir = tmp_path / "data" / "curated" / "xwalks"
        xwalk_dir.mkdir(parents=True)
        xwalk_file = xwalk_dir / "xwalk__B2025xT2020.parquet"
        pd.DataFrame({"a": [1]}).to_parquet(xwalk_file)

        from hhplab.recipe.planner import MaterializeTask

        task = MaterializeTask(transform_ids=["tract_to_coc"])
        result = _execute_materialize(task, ctx)
        assert result.success
        assert "tract_to_coc" in ctx.transform_paths

    def test_materialize_generates_coc_to_metro_artifact(self, tmp_path: Path):
        _setup_curated_metro_artifacts(tmp_path)
        data = _recipe_with_pipeline()
        data["targets"] = [
            {
                "id": "metro_panel",
                "geometry": {"type": "metro", "source": "glynn_fox_v1"},
            }
        ]
        data["pipelines"][0]["target"] = "metro_panel"
        data["pipelines"][0]["steps"] = [
            {"materialize": {"transforms": ["coc_to_metro"]}},
        ]
        data["transforms"] = [
            {
                "id": "coc_to_metro",
                "type": "crosswalk",
                "from": {"type": "coc", "vintage": 2025},
                "to": {"type": "metro", "source": "glynn_fox_v1"},
                "spec": {"weighting": {"scheme": "area"}},
            }
        ]
        recipe = load_recipe(data)
        ctx = ExecutionContext(project_root=tmp_path, recipe=recipe)

        task = MaterializeTask(transform_ids=["coc_to_metro"])
        result = _execute_materialize(task, ctx)

        assert result.success
        xwalk_path = ctx.transform_paths["coc_to_metro"]
        xwalk = pd.read_parquet(xwalk_path)
        assert {"metro_id", "coc_id", "area_share"} <= set(xwalk.columns)
        assert (xwalk["area_share"] == 1.0).all()

    def test_materialize_generates_tract_to_metro_artifact(self, tmp_path: Path):
        _setup_curated_metro_artifacts(tmp_path)
        tract_dir = tmp_path / "data" / "curated" / "tiger"
        tract_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            {
                "GEOID": ["36061000100", "06037000100"],
            }
        ).to_parquet(tract_dir / "tracts__T2020.parquet")

        data = _recipe_with_pipeline()
        data["targets"] = [
            {
                "id": "metro_panel",
                "geometry": {"type": "metro", "source": "glynn_fox_v1"},
            }
        ]
        data["pipelines"][0]["target"] = "metro_panel"
        data["pipelines"][0]["steps"] = [
            {"materialize": {"transforms": ["tract_to_metro"]}},
        ]
        data["transforms"] = [
            {
                "id": "tract_to_metro",
                "type": "crosswalk",
                "from": {"type": "tract", "vintage": 2020},
                "to": {"type": "metro", "source": "glynn_fox_v1"},
                "spec": {"weighting": {"scheme": "area"}},
            }
        ]
        recipe = load_recipe(data)
        ctx = ExecutionContext(project_root=tmp_path, recipe=recipe)

        task = MaterializeTask(transform_ids=["tract_to_metro"])
        result = _execute_materialize(task, ctx)

        assert result.success
        xwalk = pd.read_parquet(ctx.transform_paths["tract_to_metro"])
        assert {"metro_id", "tract_geoid", "area_share"} <= set(xwalk.columns)
        assert set(xwalk["metro_id"]) == {"GF01", "GF02"}

    def test_materialize_tract_to_metro_with_lowercase_geoid(self, tmp_path: Path):
        """Tract artifacts with lowercase 'geoid' column should work."""
        _setup_curated_metro_artifacts(tmp_path)
        tract_dir = tmp_path / "data" / "curated" / "tiger"
        tract_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            {
                "geoid": ["36061000100", "06037000100"],
            }
        ).to_parquet(tract_dir / "tracts__T2020.parquet")

        data = _recipe_with_pipeline()
        data["targets"] = [
            {
                "id": "metro_panel",
                "geometry": {"type": "metro", "source": "glynn_fox_v1"},
            }
        ]
        data["pipelines"][0]["target"] = "metro_panel"
        data["pipelines"][0]["steps"] = [
            {"materialize": {"transforms": ["tract_to_metro"]}},
        ]
        data["transforms"] = [
            {
                "id": "tract_to_metro",
                "type": "crosswalk",
                "from": {"type": "tract", "vintage": 2020},
                "to": {"type": "metro", "source": "glynn_fox_v1"},
                "spec": {"weighting": {"scheme": "area"}},
            }
        ]
        recipe = load_recipe(data)
        ctx = ExecutionContext(project_root=tmp_path, recipe=recipe)

        task = MaterializeTask(transform_ids=["tract_to_metro"])
        result = _execute_materialize(task, ctx)

        assert result.success
        xwalk = pd.read_parquet(ctx.transform_paths["tract_to_metro"])
        assert {"metro_id", "tract_geoid", "area_share"} <= set(xwalk.columns)
        assert set(xwalk["metro_id"]) == {"GF01", "GF02"}

    def test_materialize_fails_missing_artifact(self, tmp_path: Path):
        ctx = self._make_ctx(tmp_path)
        from hhplab.recipe.planner import MaterializeTask

        task = MaterializeTask(transform_ids=["tract_to_coc"])
        result = _execute_materialize(task, ctx)
        assert not result.success
        assert "not found" in result.error
        assert "hhplab generate xwalks" in result.error


# ===========================================================================
# Resample step tests
# ===========================================================================


def _make_dataset_parquet(path: Path, geo_col: str = "geo_id") -> None:
    """Write a minimal dataset parquet for testing."""
    df = pd.DataFrame(
        {
            geo_col: ["A", "B", "C"],
            "year": [2020, 2020, 2020],
            "pop": [100, 200, 300],
            "income": [50000, 60000, 70000],
        }
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path)


def _make_xwalk_parquet(path: Path, geo_type: str = "tract") -> None:
    """Write a minimal crosswalk parquet for testing."""
    if geo_type == "tract":
        df = pd.DataFrame(
            {
                "coc_id": ["COC1", "COC1", "COC2"],
                "tract_geoid": ["A", "B", "C"],
                "area_share": [0.8, 0.5, 1.0],
                "pop_share": [0.6, 0.4, 1.0],
            }
        )
    else:
        df = pd.DataFrame(
            {
                "coc_id": ["COC1", "COC1", "COC2"],
                "county_fips": ["A", "B", "C"],
                "area_share": [0.8, 0.5, 1.0],
            }
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path)


CT_RECIPE_ALIGNMENT_MAPPING = pd.DataFrame(
    {
        "legacy_county_fips": ["09001", "09001", "09003", "09003"],
        "planning_region_fips": ["09110", "09120", "09120", "09130"],
        "legacy_share": [0.80, 0.20, 0.70, 0.30],
        "planning_share": [1.0, 0.30, 0.70, 1.0],
    }
)

CT_RECIPE_ALIGNMENT_EXPECTED_POP = {
    "AL-500": 400.0,
    "CT-500": 1000.0,
}

CT_RECIPE_ALIGNMENT_EXPECTED_RENT = {
    "AL-500": 900.0,
    "CT-500": 1410.0,
}


def _make_ct_recipe_alignment_crosswalk() -> CtPlanningRegionCrosswalk:
    return CtPlanningRegionCrosswalk(
        mapping=CT_RECIPE_ALIGNMENT_MAPPING.copy(),
        legacy_vintage=2020,
        planning_vintage=2023,
    )


def _patch_ct_recipe_alignment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "hhplab.recipe.executor_ct_alignment.build_ct_county_planning_region_crosswalk",
        lambda **kwargs: _make_ct_recipe_alignment_crosswalk(),
    )
    monkeypatch.setattr(
        "hhplab.recipe.preflight.build_ct_county_planning_region_crosswalk",
        lambda **kwargs: _make_ct_recipe_alignment_crosswalk(),
    )


def _patch_ct_recipe_alignment_failure(
    monkeypatch: pytest.MonkeyPatch,
    *,
    message: str = "missing counties__C2023.parquet",
) -> None:
    def _raise(**kwargs):
        raise FileNotFoundError(message)

    monkeypatch.setattr(
        "hhplab.recipe.executor_ct_alignment.build_ct_county_planning_region_crosswalk",
        _raise,
    )
    monkeypatch.setattr(
        "hhplab.recipe.preflight.build_ct_county_planning_region_crosswalk",
        _raise,
    )


def _setup_ct_alignment_recipe(tmp_path: Path) -> dict:
    """Create a minimal recipe and fixtures that exercise CT county alignment."""
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        {
            "county_fips": ["09110", "09120", "09130", "01001"],
            "year": [2024, 2024, 2024, 2024],
            "population": [500.0, 300.0, 200.0, 400.0],
        }
    ).to_parquet(data_dir / "pep.parquet")

    pd.DataFrame(
        {
            "county_fips": ["09001", "09003", "01001"],
            "year": [2024, 2024, 2024],
            "rent": [1000.0, 2000.0, 900.0],
        }
    ).to_parquet(data_dir / "zori.parquet")

    pd.DataFrame(
        {
            "county_fips": ["09110", "09120", "09130", "01001"],
            "year": [2024, 2024, 2024, 2024],
            "population": [500.0, 300.0, 200.0, 100.0],
        }
    ).to_parquet(data_dir / "weights.parquet")

    xwalk_dir = data_dir / "curated" / "xwalks"
    xwalk_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "coc_id": ["CT-500", "CT-500", "AL-500"],
            "county_fips": ["09001", "09003", "01001"],
            "area_share": [1.0, 1.0, 1.0],
        }
    ).to_parquet(xwalk_dir / "xwalk__B2025xC2020.parquet")

    return {
        "version": 1,
        "name": "ct-alignment-cli",
        "universe": {"range": "2024-2024"},
        "targets": [
            {"id": "coc_panel", "geometry": {"type": "coc", "vintage": 2025}},
        ],
        "datasets": {
            "pep_county": {
                "provider": "census",
                "product": "pep",
                "version": 1,
                "native_geometry": {"type": "county", "vintage": 2020},
                "years": "2024-2024",
                "path": "data/pep.parquet",
                "geo_column": "county_fips",
            },
            "zori_county": {
                "provider": "zillow",
                "product": "zori",
                "version": 1,
                "native_geometry": {"type": "county", "vintage": 2020},
                "years": "2024-2024",
                "path": "data/zori.parquet",
                "geo_column": "county_fips",
            },
            "weights": {
                "provider": "census",
                "product": "pep",
                "version": 1,
                "native_geometry": {"type": "county", "vintage": 2020},
                "years": "2024-2024",
                "path": "data/weights.parquet",
                "geo_column": "county_fips",
            },
        },
        "transforms": [
            {
                "id": "county_to_coc_area",
                "type": "crosswalk",
                "from": {"type": "county", "vintage": 2020},
                "to": {"type": "coc", "vintage": 2025},
                "spec": {"weighting": {"scheme": "area"}},
            },
            {
                "id": "county_to_coc_population",
                "type": "crosswalk",
                "from": {"type": "county", "vintage": 2020},
                "to": {"type": "coc", "vintage": 2025},
                "spec": {
                    "weighting": {
                        "scheme": "population",
                        "population_source": "weights",
                        "population_field": "population",
                    }
                },
            },
        ],
        "pipelines": [
            {
                "id": "main",
                "target": "coc_panel",
                "steps": [
                    {
                        "materialize": {
                            "transforms": ["county_to_coc_area", "county_to_coc_population"],
                        }
                    },
                    {
                        "resample": {
                            "dataset": "pep_county",
                            "to_geometry": {"type": "coc", "vintage": 2025},
                            "method": "aggregate",
                            "via": "county_to_coc_area",
                            "measures": ["population"],
                            "aggregation": "sum",
                        }
                    },
                    {
                        "resample": {
                            "dataset": "zori_county",
                            "to_geometry": {"type": "coc", "vintage": 2025},
                            "method": "aggregate",
                            "via": "county_to_coc_population",
                            "measures": {
                                "rent": {"aggregation": "weighted_mean"},
                            },
                        }
                    },
                    {
                        "join": {
                            "datasets": ["pep_county", "zori_county"],
                            "join_on": ["geo_id", "year"],
                        }
                    },
                ],
            }
        ],
    }


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
            measure_aggregations={"pop": "sum"},
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

    def test_aggregate_multi_tract_mediated_varieties_side_by_side(self, tmp_path: Path):
        ds_path = tmp_path / "data" / "pep.parquet"
        _make_dataset_parquet(ds_path, geo_col="county_fips")

        xwalk_path = (
            tmp_path
            / "data"
            / "curated"
            / "xwalks"
            / "xwalk_tract_mediated_county__A2023@B2025xC2020xT2020.parquet"
        )
        xwalk_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            {
                "coc_id": ["COC1", "COC1", "COC2"],
                "county_fips": ["A", "B", "C"],
                "area_weight": [0.8, 0.5, 1.0],
                "population_weight": [0.25, 0.75, 1.0],
                "weighting_method": ["tract_mediated"] * 3,
                "boundary_vintage": ["2025"] * 3,
                "county_vintage": ["2020"] * 3,
                "tract_vintage": ["2020"] * 3,
                "acs_vintage": ["2023"] * 3,
            }
        ).to_parquet(xwalk_path)

        data = _recipe_with_pipeline()
        data["datasets"]["pep"] = {
            "provider": "census",
            "product": "pep",
            "version": 1,
            "native_geometry": {"type": "county", "vintage": 2020},
            "years": "2020-2020",
            "path": "data/pep.parquet",
            "geo_column": "county_fips",
        }
        data["transforms"] = [
            {
                "id": "county_to_coc_tract_mediated",
                "type": "crosswalk",
                "from": {"type": "county", "vintage": 2020},
                "to": {"type": "coc", "vintage": 2025},
                "spec": {
                    "weighting": {
                        "scheme": "tract_mediated",
                        "varieties": ["area", "population"],
                        "tract_vintage": 2020,
                        "acs_vintage": 2023,
                    },
                },
            }
        ]
        data["pipelines"][0]["steps"] = [
            {"materialize": {"transforms": ["county_to_coc_tract_mediated"]}},
        ]
        recipe = load_recipe(data)
        ctx = ExecutionContext(project_root=tmp_path, recipe=recipe)
        ctx.transform_paths["county_to_coc_tract_mediated"] = xwalk_path

        base_task = dict(
            dataset_id="pep",
            year=2020,
            input_path="data/pep.parquet",
            effective_geometry=GeometryRef(type="county", vintage=2020),
            method="aggregate",
            transform_id="county_to_coc_tract_mediated",
            to_geometry=GeometryRef(type="coc", vintage=2025),
            measures=["pop"],
            measure_aggregations={"pop": "sum"},
            geo_column="county_fips",
            weighting_variety_count=2,
        )

        area_result = _execute_resample(
            ResampleTask(
                **base_task,
                weighting_variety="area",
                weight_column="area_weight",
            ),
            ctx,
        )
        population_result = _execute_resample(
            ResampleTask(
                **base_task,
                weighting_variety="population",
                weight_column="population_weight",
            ),
            ctx,
        )

        assert area_result.success
        assert population_result.success
        df = ctx.intermediates[("pep", 2020)]
        coc1 = df[df.geo_id == "COC1"].iloc[0]
        assert coc1["pop__warea"] == pytest.approx(180.0)
        assert coc1["pop__wpopulation"] == pytest.approx(175.0)
        assert "pop" not in df.columns

    def test_pep_decennial_tract_mediated_population_uses_baseline_scaling(
        self,
        tmp_path: Path,
    ):
        from hhplab.pep.pep_aggregate import aggregate_pep_counties

        ds_path = tmp_path / "data" / "pep.parquet"
        ds_path.parent.mkdir(parents=True, exist_ok=True)
        pep_df = pd.DataFrame(
            {
                "county_fips": ["A", "B", "A", "B"],
                "year": [2020, 2020, 2024, 2024],
                "population": [100.0, 200.0, 120.0, 260.0],
            }
        )
        pep_df.to_parquet(ds_path)

        xwalk_path = (
            tmp_path
            / "data"
            / "curated"
            / "xwalks"
            / "xwalk_tract_mediated_county__N2020@B2025xC2020xT2020.parquet"
        )
        xwalk_path.parent.mkdir(parents=True, exist_ok=True)
        xwalk = pd.DataFrame(
            {
                "coc_id": ["COC1", "COC1", "COC2"],
                "county_fips": ["A", "B", "B"],
                "population_weight": [0.4, 0.6, 0.4],
                "county_population_total": [90.0, 210.0, 210.0],
                "denominator_source": ["decennial"] * 3,
                "denominator_vintage": [2020] * 3,
                "weighting_method": ["tract_mediated"] * 3,
                "boundary_vintage": ["2025"] * 3,
                "county_vintage": ["2020"] * 3,
                "tract_vintage": ["2020"] * 3,
            }
        )
        xwalk.to_parquet(xwalk_path)

        expected = aggregate_pep_counties(
            pep_df,
            xwalk,
            weighting="population_weight",
            boundary_vintage="2025",
            county_vintage="2020",
        )
        expected_2024 = expected[expected["year"] == 2024].set_index("coc_id")

        data = _recipe_with_pipeline()
        data["universe"] = {"range": "2024-2024"}
        data["datasets"]["pep"] = {
            "provider": "census",
            "product": "pep",
            "version": 1,
            "native_geometry": {"type": "county", "vintage": 2020},
            "years": "2020-2024",
            "path": "data/pep.parquet",
            "geo_column": "county_fips",
        }
        data["transforms"] = [
            {
                "id": "county_to_coc_tract_mediated",
                "type": "crosswalk",
                "from": {"type": "county", "vintage": 2020},
                "to": {"type": "coc", "vintage": 2025},
                "spec": {
                    "weighting": {
                        "scheme": "tract_mediated",
                        "variety": "population",
                        "tract_vintage": 2020,
                        "denominator_source": "decennial",
                        "denominator_vintage": 2020,
                    },
                },
            }
        ]
        data["pipelines"][0]["steps"] = [
            {"materialize": {"transforms": ["county_to_coc_tract_mediated"]}},
        ]
        recipe = load_recipe(data)
        ctx = ExecutionContext(project_root=tmp_path, recipe=recipe)
        ctx.transform_paths["county_to_coc_tract_mediated"] = xwalk_path

        result = _execute_resample(
            ResampleTask(
                dataset_id="pep",
                year=2024,
                input_path="data/pep.parquet",
                effective_geometry=GeometryRef(type="county", vintage=2020),
                method="aggregate",
                transform_id="county_to_coc_tract_mediated",
                to_geometry=GeometryRef(type="coc", vintage=2025),
                measures=["population"],
                measure_aggregations={"population": "sum"},
                geo_column="county_fips",
                weighting_variety="population",
                weight_column="population_weight",
            ),
            ctx,
        )

        assert result.success
        df = ctx.intermediates[("pep", 2024)].set_index("geo_id")
        assert df["total_population"].to_dict() == pytest.approx(
            expected_2024["population"].to_dict(),
        )
        assert set(df["population_scaling_method"]) == {"decennial_pep_baseline_ratio"}
        assert set(df["population_scaling_baseline_year"]) == {2020}
        assert set(df["total_population_source"]) == {"pep"}
        assert set(df["total_population_method"]) == {"tract_mediated_crosswalk"}

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
            measure_aggregations={"income": "weighted_mean"},
        )
        result = _execute_resample(task, ctx)
        assert result.success
        df = ctx.intermediates[("acs", 2020)]
        # COC1: pop_share = [0.6, 0.4], income = [50000, 60000]
        # weighted_mean = (50000*0.6 + 60000*0.4) / (0.6+0.4) = 54000
        coc1_income = df[df.geo_id == "COC1"]["income"].iloc[0]
        assert coc1_income == pytest.approx(54000.0)

    def test_aggregate_sum_with_coc_to_metro_crosswalk(self, tmp_path: Path):
        _setup_curated_metro_artifacts(tmp_path)
        ds_path = tmp_path / "data" / "pit.parquet"
        pd.DataFrame(
            {
                "coc_id": ["NY-600", "CA-600"],
                "year": [2020, 2020],
                "pop": [100, 200],
            }
        ).to_parquet(ds_path)

        data = _recipe_with_pipeline()
        data["targets"] = [
            {
                "id": "metro_panel",
                "geometry": {"type": "metro", "source": "glynn_fox_v1"},
            }
        ]
        data["pipelines"][0]["target"] = "metro_panel"
        data["pipelines"][0]["steps"] = [
            {"materialize": {"transforms": ["coc_to_metro"]}},
        ]
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        data["transforms"] = [
            {
                "id": "coc_to_metro",
                "type": "crosswalk",
                "from": {"type": "coc", "vintage": 2025},
                "to": {"type": "metro", "source": "glynn_fox_v1"},
                "spec": {"weighting": {"scheme": "area"}},
            }
        ]
        recipe = load_recipe(data)
        ctx = ExecutionContext(project_root=tmp_path, recipe=recipe)
        materialize_result = _execute_materialize(
            MaterializeTask(transform_ids=["coc_to_metro"]),
            ctx,
        )
        assert materialize_result.success

        task = ResampleTask(
            dataset_id="pit",
            year=2020,
            input_path="data/pit.parquet",
            effective_geometry=GeometryRef(type="coc", vintage=2025),
            method="aggregate",
            transform_id="coc_to_metro",
            to_geometry=GeometryRef(type="metro", source="glynn_fox_v1"),
            measures=["pop"],
            measure_aggregations={"pop": "sum"},
        )
        result = _execute_resample(task, ctx)

        assert result.success
        df = ctx.intermediates[("pit", 2020)].sort_values("geo_id").reset_index(drop=True)
        assert list(df["geo_id"]) == ["GF01", "GF02"]
        assert list(df["pop"]) == [100.0, 200.0]

    def test_aggregate_weighted_mean_with_dynamic_metro_pop_share(self, tmp_path: Path):
        _setup_curated_metro_artifacts(tmp_path)
        zori_path = tmp_path / "data" / "zori.parquet"
        pd.DataFrame(
            {
                "county_fips": ["36061", "06037"],
                "year": [2020, 2020],
                "rent": [1000.0, 2000.0],
            }
        ).to_parquet(zori_path)
        weights_path = tmp_path / "data" / "county_weights.parquet"
        pd.DataFrame(
            {
                "county_fips": ["36061", "06037"],
                "year": [2020, 2020],
                "total_population": [1.0, 3.0],
            }
        ).to_parquet(weights_path)

        data = _recipe_with_pipeline()
        data["targets"] = [
            {
                "id": "metro_panel",
                "geometry": {"type": "metro", "source": "glynn_fox_v1"},
            }
        ]
        data["pipelines"][0]["target"] = "metro_panel"
        data["pipelines"][0]["steps"] = [
            {"materialize": {"transforms": ["county_to_metro"]}},
        ]
        data["datasets"]["pit"] = {
            "provider": "zillow",
            "product": "zori",
            "version": 1,
            "native_geometry": {"type": "county", "vintage": 2020},
            "years": "2020-2021",
            "path": "data/zori.parquet",
            "geo_column": "county_fips",
        }
        data["datasets"]["weights"] = {
            "provider": "census",
            "product": "acs5",
            "version": 1,
            "native_geometry": {"type": "county", "vintage": 2020},
            "years": "2020-2021",
            "path": "data/county_weights.parquet",
            "geo_column": "county_fips",
        }
        data["transforms"] = [
            {
                "id": "county_to_metro",
                "type": "crosswalk",
                "from": {"type": "county", "vintage": 2020},
                "to": {"type": "metro", "source": "glynn_fox_v1"},
                "spec": {
                    "weighting": {
                        "scheme": "population",
                        "population_source": "weights",
                        "population_field": "total_population",
                    }
                },
            }
        ]
        recipe = load_recipe(data)
        ctx = ExecutionContext(project_root=tmp_path, recipe=recipe)
        materialize_result = _execute_materialize(
            MaterializeTask(transform_ids=["county_to_metro"]),
            ctx,
        )
        assert materialize_result.success

        task = ResampleTask(
            dataset_id="pit",
            year=2020,
            input_path="data/zori.parquet",
            effective_geometry=GeometryRef(type="county", vintage=2020),
            method="aggregate",
            transform_id="county_to_metro",
            to_geometry=GeometryRef(type="metro", source="glynn_fox_v1"),
            measures=["rent"],
            measure_aggregations={"rent": "weighted_mean"},
            geo_column="county_fips",
        )
        result = _execute_resample(task, ctx)

        assert result.success
        df = ctx.intermediates[("pit", 2020)].sort_values("geo_id").reset_index(drop=True)
        assert list(df["geo_id"]) == ["GF01", "GF02"]
        assert list(df["rent"]) == [1000.0, 2000.0]

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
            measure_aggregations={"pop": "sum"},
        )
        result = _execute_resample(task, ctx)
        assert not result.success
        assert "not materialized" in result.error

    def test_aggregate_zero_join_rows_fails(self, tmp_path: Path):
        # Dataset has geo_ids that don't match crosswalk
        ds_path = tmp_path / "data" / "acs.parquet"
        df = pd.DataFrame(
            {
                "GEOID": ["X", "Y"],
                "year": [2020, 2020],
                "pop": [100, 200],
            }
        )
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
            measure_aggregations={"pop": "sum"},
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
            measure_aggregations={"pop": "mean"},
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
        data["transforms"] = [
            {
                "id": "county_to_coc",
                "type": "crosswalk",
                "from": {"type": "county"},
                "to": {"type": "coc", "vintage": 2025},
                "spec": {"weighting": {"scheme": "area"}},
            }
        ]
        data["pipelines"][0]["steps"] = [
            {"materialize": {"transforms": ["county_to_coc"]}},
            {
                "resample": {
                    "dataset": "acs",
                    "to_geometry": {"type": "coc", "vintage": 2025},
                    "method": "aggregate",
                    "via": "county_to_coc",
                    "measures": ["pop"],
                    "aggregation": "sum",
                }
            },
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
            measure_aggregations={"pop": "sum"},
        )
        result = _execute_resample(task, ctx)
        assert result.success

    def test_aggregate_sum_translates_ct_planning_source_to_legacy(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ):
        _patch_ct_recipe_alignment(monkeypatch)

        ds_path = tmp_path / "data" / "pep.parquet"
        ds_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            {
                "county_fips": ["09110", "09120", "09130", "01001"],
                "year": [2024, 2024, 2024, 2024],
                "population": [500.0, 300.0, 200.0, 400.0],
            }
        ).to_parquet(ds_path)

        xwalk_path = tmp_path / "xwalk_county.parquet"
        pd.DataFrame(
            {
                "coc_id": ["CT-500", "CT-500", "AL-500"],
                "county_fips": ["09001", "09003", "01001"],
                "area_share": [1.0, 1.0, 1.0],
            }
        ).to_parquet(xwalk_path)

        data = _recipe_with_pipeline()
        data["universe"] = {"range": "2024-2024"}
        data["datasets"]["pep"] = {
            "provider": "census",
            "product": "pep",
            "version": 1,
            "native_geometry": {"type": "county", "vintage": 2020},
            "years": "2024-2024",
            "path": "data/pep.parquet",
            "geo_column": "county_fips",
        }
        data["transforms"] = [
            {
                "id": "county_to_coc",
                "type": "crosswalk",
                "from": {"type": "county", "vintage": 2020},
                "to": {"type": "coc", "vintage": 2025},
                "spec": {"weighting": {"scheme": "area"}},
            }
        ]
        data["pipelines"][0]["steps"] = [
            {"materialize": {"transforms": ["county_to_coc"]}},
        ]
        recipe = load_recipe(data)
        ctx = ExecutionContext(project_root=tmp_path, recipe=recipe)
        ctx.transform_paths["county_to_coc"] = xwalk_path

        task = ResampleTask(
            dataset_id="pep",
            year=2024,
            input_path="data/pep.parquet",
            effective_geometry=GeometryRef(type="county", vintage=2020),
            method="aggregate",
            transform_id="county_to_coc",
            to_geometry=GeometryRef(type="coc", vintage=2025),
            measures=["population"],
            measure_aggregations={"population": "sum"},
            geo_column="county_fips",
        )
        result = _execute_resample(task, ctx)

        assert result.success
        df = ctx.intermediates[("pep", 2024)].sort_values("geo_id").reset_index(drop=True)
        assert "population" not in df.columns
        assert dict(zip(df["geo_id"], df["total_population"], strict=True)) == pytest.approx(
            CT_RECIPE_ALIGNMENT_EXPECTED_POP,
        )
        assert set(df["total_population_source"]) == {"pep"}
        assert set(df["total_population_method"]) == {"area_crosswalk"}
        assert set(df["total_population_crosswalk_id"]) == {"county_to_coc"}

    def test_aggregate_weighted_mean_translates_ct_population_source_to_legacy(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ):
        _patch_ct_recipe_alignment(monkeypatch)

        zori_path = tmp_path / "data" / "zori.parquet"
        zori_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            {
                "county_fips": ["09001", "09003", "01001"],
                "year": [2024, 2024, 2024],
                "rent": [1000.0, 2000.0, 900.0],
            }
        ).to_parquet(zori_path)

        weights_path = tmp_path / "data" / "weights.parquet"
        pd.DataFrame(
            {
                "county_fips": ["09110", "09120", "09130", "01001"],
                "year": [2024, 2024, 2024, 2024],
                "population": [500.0, 300.0, 200.0, 100.0],
            }
        ).to_parquet(weights_path)

        xwalk_path = tmp_path / "xwalk_county.parquet"
        pd.DataFrame(
            {
                "coc_id": ["CT-500", "CT-500", "AL-500"],
                "county_fips": ["09001", "09003", "01001"],
                "area_share": [1.0, 1.0, 1.0],
            }
        ).to_parquet(xwalk_path)

        data = _recipe_with_pipeline()
        data["universe"] = {"range": "2024-2024"}
        data["datasets"]["zori"] = {
            "provider": "zillow",
            "product": "zori",
            "version": 1,
            "native_geometry": {"type": "county", "vintage": 2020},
            "years": "2024-2024",
            "path": "data/zori.parquet",
            "geo_column": "county_fips",
        }
        data["datasets"]["weights"] = {
            "provider": "census",
            "product": "pep",
            "version": 1,
            "native_geometry": {"type": "county", "vintage": 2020},
            "years": "2024-2024",
            "path": "data/weights.parquet",
            "geo_column": "county_fips",
        }
        data["transforms"] = [
            {
                "id": "county_to_coc",
                "type": "crosswalk",
                "from": {"type": "county", "vintage": 2020},
                "to": {"type": "coc", "vintage": 2025},
                "spec": {
                    "weighting": {
                        "scheme": "population",
                        "population_source": "weights",
                        "population_field": "population",
                    }
                },
            }
        ]
        data["pipelines"][0]["steps"] = [
            {"materialize": {"transforms": ["county_to_coc"]}},
        ]
        recipe = load_recipe(data)
        ctx = ExecutionContext(project_root=tmp_path, recipe=recipe)
        ctx.transform_paths["county_to_coc"] = xwalk_path

        task = ResampleTask(
            dataset_id="zori",
            year=2024,
            input_path="data/zori.parquet",
            effective_geometry=GeometryRef(type="county", vintage=2020),
            method="aggregate",
            transform_id="county_to_coc",
            to_geometry=GeometryRef(type="coc", vintage=2025),
            measures=["rent"],
            measure_aggregations={"rent": "weighted_mean"},
            geo_column="county_fips",
        )
        result = _execute_resample(task, ctx)

        assert result.success
        df = ctx.intermediates[("zori", 2024)].sort_values("geo_id").reset_index(drop=True)
        assert dict(zip(df["geo_id"], df["rent"], strict=True)) == pytest.approx(
            CT_RECIPE_ALIGNMENT_EXPECTED_RENT,
        )

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
            s
            for pipe in recipe.pipelines
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
            measure_aggregations={"pop": "sum"},
        )
        result = _execute_resample(task, ctx)
        assert result.success
        df = ctx.intermediates[("acs", 2020)]
        # COC1 = 100*0.8 + 200*0.5 = 180, COC2 = 300*1.0 = 300
        coc1 = df[df.geo_id == "COC1"]["pop"].iloc[0]
        assert coc1 == pytest.approx(180.0)


# ===========================================================================
# _attach_dynamic_pop_share failure-mode tests (coclab-0tyg)
# ===========================================================================


class TestAttachDynamicPopShareFailures:
    """Tests for failure branches in _attach_dynamic_pop_share."""

    @staticmethod
    def _pop_weighted_recipe(
        *,
        population_field: str = "total_population",
        from_type: str = "county",
    ) -> dict:
        """Recipe with a population-weighted transform."""
        data = _recipe_with_pipeline()
        data["datasets"]["weights"] = {
            "provider": "census",
            "product": "acs5",
            "version": 1,
            "native_geometry": {"type": from_type, "vintage": 2020},
            "years": "2020-2021",
            "path": "data/county_weights.parquet",
            "geo_column": "county_fips",
        }
        data["transforms"] = [
            {
                "id": "county_to_coc",
                "type": "crosswalk",
                "from": {"type": from_type, "vintage": 2020},
                "to": {"type": "coc", "vintage": 2025},
                "spec": {
                    "weighting": {
                        "scheme": "population",
                        "population_source": "weights",
                        "population_field": population_field,
                    }
                },
            }
        ]
        data["pipelines"][0]["steps"] = [
            {"materialize": {"transforms": ["county_to_coc"]}},
            {
                "resample": {
                    "dataset": "acs",
                    "to_geometry": {"type": "coc", "vintage": 2025},
                    "method": "aggregate",
                    "via": "county_to_coc",
                    "measures": ["pop"],
                    "aggregation": "sum",
                }
            },
        ]
        return data

    def _write_fixtures(
        self,
        tmp_path: Path,
        *,
        population_field: str = "total_population",
        pop_values: list | None = None,
    ) -> tuple:
        """Create dataset, weights, and crosswalk files; return (recipe, xwalk_path)."""
        # Source dataset
        ds_path = tmp_path / "data" / "acs.parquet"
        ds_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            {
                "county_fips": ["36061", "06037"],
                "year": [2020, 2020],
                "pop": [100.0, 200.0],
            }
        ).to_parquet(ds_path)

        # Weights (support) dataset
        weights_path = tmp_path / "data" / "county_weights.parquet"
        weights_data: dict = {
            "county_fips": ["36061", "06037"],
            "year": [2020, 2020],
        }
        if pop_values is not None:
            weights_data[population_field] = pop_values
        else:
            weights_data[population_field] = [1.0, 3.0]
        pd.DataFrame(weights_data).to_parquet(weights_path)

        # Crosswalk (no pop_share — forces dynamic attachment)
        xwalk_path = tmp_path / "xwalk.parquet"
        pd.DataFrame(
            {
                "coc_id": ["COC1", "COC1"],
                "county_fips": ["36061", "06037"],
                "area_share": [1.0, 1.0],
            }
        ).to_parquet(xwalk_path)

        return xwalk_path

    # ---- Test 1: geometry type not in _XWALK_JOIN_KEYS ----------------------

    def test_unknown_geometry_type_raises(self, tmp_path: Path):
        """effective_geometry.type not in _XWALK_JOIN_KEYS → ExecutorError."""
        xwalk_path = self._write_fixtures(tmp_path)
        data = self._pop_weighted_recipe()
        recipe = load_recipe(data)
        ctx = ExecutionContext(project_root=tmp_path, recipe=recipe)
        ctx.transform_paths["county_to_coc"] = xwalk_path

        task = ResampleTask(
            dataset_id="acs",
            year=2020,
            input_path="data/acs.parquet",
            # "zipcode" is not a recognised geometry type in _XWALK_JOIN_KEYS
            effective_geometry=GeometryRef(type="zipcode"),
            method="aggregate",
            transform_id="county_to_coc",
            to_geometry=GeometryRef(type="coc", vintage=2025),
            measures=["pop"],
            measure_aggregations={"pop": "sum"},
            geo_column="county_fips",
        )
        result = _execute_resample(task, ctx)
        assert not result.success
        assert "cannot derive pop_share" in result.error
        assert "zipcode" in result.error

    # ---- Test 2: join key present in map but absent from xwalk columns ------

    def test_join_key_not_in_xwalk_columns_raises(self, tmp_path: Path):
        """source_key exists in _XWALK_JOIN_KEYS but is missing from the
        crosswalk DataFrame → ExecutorError."""
        # Dataset keyed on tract_geoid, but crosswalk only has county_fips
        ds_path = tmp_path / "data" / "acs.parquet"
        ds_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            {
                "tract_geoid": ["T1", "T2"],
                "year": [2020, 2020],
                "pop": [100.0, 200.0],
            }
        ).to_parquet(ds_path)

        weights_path = tmp_path / "data" / "county_weights.parquet"
        pd.DataFrame(
            {
                "county_fips": ["36061", "06037"],
                "year": [2020, 2020],
                "total_population": [1.0, 3.0],
            }
        ).to_parquet(weights_path)

        # Crosswalk has county_fips + coc_id but NOT tract_geoid
        xwalk_path = tmp_path / "xwalk.parquet"
        pd.DataFrame(
            {
                "coc_id": ["COC1", "COC1"],
                "county_fips": ["36061", "06037"],
                "area_share": [1.0, 1.0],
            }
        ).to_parquet(xwalk_path)

        data = self._pop_weighted_recipe(from_type="tract")
        # Override the weights geo_column to match what the dataset actually has
        data["datasets"]["weights"]["geo_column"] = "county_fips"
        recipe = load_recipe(data)
        ctx = ExecutionContext(project_root=tmp_path, recipe=recipe)
        ctx.transform_paths["county_to_coc"] = xwalk_path

        task = ResampleTask(
            dataset_id="acs",
            year=2020,
            input_path="data/acs.parquet",
            # effective_geometry is "tract" → source_key = "tract_geoid"
            # but xwalk only has county_fips
            effective_geometry=GeometryRef(type="tract", vintage=2020),
            method="aggregate",
            transform_id="county_to_coc",
            to_geometry=GeometryRef(type="coc", vintage=2025),
            measures=["pop"],
            measure_aggregations={"pop": "sum"},
            geo_column="tract_geoid",
        )
        result = _execute_resample(task, ctx)
        assert not result.success
        assert "cannot derive pop_share" in result.error

    # ---- Test 3: population_field missing from weights DataFrame ------------

    def test_population_field_missing_from_weights(self, tmp_path: Path):
        """population_field declared in transform but absent from the weights
        dataset → ExecutorError listing available columns."""
        # Write weights WITHOUT the expected population_field
        ds_path = tmp_path / "data" / "acs.parquet"
        ds_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            {
                "county_fips": ["36061", "06037"],
                "year": [2020, 2020],
                "pop": [100.0, 200.0],
            }
        ).to_parquet(ds_path)

        weights_path = tmp_path / "data" / "county_weights.parquet"
        pd.DataFrame(
            {
                "county_fips": ["36061", "06037"],
                "year": [2020, 2020],
                "unrelated_col": [99, 88],
            }
        ).to_parquet(weights_path)

        xwalk_path = tmp_path / "xwalk.parquet"
        pd.DataFrame(
            {
                "coc_id": ["COC1", "COC1"],
                "county_fips": ["36061", "06037"],
                "area_share": [1.0, 1.0],
            }
        ).to_parquet(xwalk_path)

        data = self._pop_weighted_recipe(population_field="total_population")
        recipe = load_recipe(data)
        ctx = ExecutionContext(project_root=tmp_path, recipe=recipe)
        ctx.transform_paths["county_to_coc"] = xwalk_path

        task = ResampleTask(
            dataset_id="acs",
            year=2020,
            input_path="data/acs.parquet",
            effective_geometry=GeometryRef(type="county", vintage=2020),
            method="aggregate",
            transform_id="county_to_coc",
            to_geometry=GeometryRef(type="coc", vintage=2025),
            measures=["pop"],
            measure_aggregations={"pop": "sum"},
            geo_column="county_fips",
        )
        result = _execute_resample(task, ctx)
        assert not result.success
        assert "missing population field" in result.error
        assert "total_population" in result.error

    # ---- Test 4: all-NaN population → zero group_sum → NaN pop_share --------

    def test_all_nan_population_produces_nan_pop_share(self, tmp_path: Path):
        """When every population value is NaN the group_sum is zero and
        pop_share should be NaN (not Inf or an error).  The downstream
        aggregate falls back to area_share for weighted_mean because
        has_pop_share is False when all pop_share values are NaN."""
        xwalk_path = self._write_fixtures(
            tmp_path,
            pop_values=[float("nan"), float("nan")],
        )
        data = self._pop_weighted_recipe()
        recipe = load_recipe(data)
        ctx = ExecutionContext(project_root=tmp_path, recipe=recipe)
        ctx.transform_paths["county_to_coc"] = xwalk_path

        task = ResampleTask(
            dataset_id="acs",
            year=2020,
            input_path="data/acs.parquet",
            effective_geometry=GeometryRef(type="county", vintage=2020),
            method="aggregate",
            transform_id="county_to_coc",
            to_geometry=GeometryRef(type="coc", vintage=2025),
            measures=["pop"],
            measure_aggregations={"pop": "weighted_mean"},
            geo_column="county_fips",
        )
        result = _execute_resample(task, ctx)
        # The resample succeeds: all-NaN pop_share causes has_pop_share=False,
        # so weighted_mean falls back to area_share.  With area_share=1.0 for
        # both source rows, the weighted mean of (100, 200) is 150.
        assert result.success
        df = ctx.intermediates[("acs", 2020)]
        assert not df.empty
        assert df["pop"].iloc[0] == pytest.approx(150.0)


# ===========================================================================
# Allocate resample tests (coclab-8t3f)
# ===========================================================================


class TestResampleAllocate:
    """Dedicated tests for _resample_allocate (coclab-8t3f)."""

    @staticmethod
    def _allocate_recipe() -> dict:
        """Recipe with a coc_to_tract crosswalk for allocate tests."""
        data = _recipe_with_pipeline()
        data["transforms"].append(
            {
                "id": "coc_to_tract",
                "type": "crosswalk",
                "from": {"type": "coc", "vintage": 2025},
                "to": {"type": "tract", "vintage": 2020},
                "spec": {"weighting": {"scheme": "area"}},
            }
        )
        return data

    def _setup(self, tmp_path: Path) -> ExecutionContext:
        # Dataset: coarse CoC-level data
        ds_path = tmp_path / "data" / "coc_data.parquet"
        ds_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            {
                "coc_id": ["COC1", "COC2"],
                "pop": [1000.0, 2000.0],
                "income": [50000.0, 60000.0],
            }
        ).to_parquet(ds_path)

        # Crosswalk: CoC → tract with area_share weights
        xwalk_path = tmp_path / "data" / "curated" / "xwalks" / "xwalk_alloc.parquet"
        xwalk_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            {
                "coc_id": ["COC1", "COC1", "COC2"],
                "tract_geoid": ["T1", "T2", "T3"],
                "area_share": [0.6, 0.4, 1.0],
            }
        ).to_parquet(xwalk_path)

        recipe = load_recipe(self._allocate_recipe())
        ctx = ExecutionContext(project_root=tmp_path, recipe=recipe)
        ctx.transform_paths["coc_to_tract"] = xwalk_path
        return ctx

    def test_basic_allocate(self, tmp_path: Path):
        """Allocate distributes coarse values to fine geometry by weight."""
        ctx = self._setup(tmp_path)
        task = ResampleTask(
            dataset_id="coc_data",
            year=2020,
            input_path="data/coc_data.parquet",
            effective_geometry=GeometryRef(type="coc", vintage=2025),
            method="allocate",
            transform_id="coc_to_tract",
            to_geometry=GeometryRef(type="tract", vintage=2020),
            measures=["pop"],
        )
        result = _execute_resample(task, ctx)
        assert result.success
        df = ctx.intermediates[("coc_data", 2020)]
        # COC1(1000) × 0.6 = 600, COC1(1000) × 0.4 = 400, COC2(2000) × 1.0 = 2000
        t1 = df[df.geo_id == "T1"]["pop"].iloc[0]
        t2 = df[df.geo_id == "T2"]["pop"].iloc[0]
        t3 = df[df.geo_id == "T3"]["pop"].iloc[0]
        assert t1 == pytest.approx(600.0)
        assert t2 == pytest.approx(400.0)
        assert t3 == pytest.approx(2000.0)

    def test_allocate_missing_target_key_fails(self, tmp_path: Path):
        """Allocate must fail when crosswalk lacks the target geo key."""
        ctx = self._setup(tmp_path)
        task = ResampleTask(
            dataset_id="coc_data",
            year=2020,
            input_path="data/coc_data.parquet",
            effective_geometry=GeometryRef(type="coc", vintage=2025),
            method="allocate",
            transform_id="coc_to_tract",
            # county geometry won't find county_fips in the crosswalk
            to_geometry=GeometryRef(type="county"),
            measures=["pop"],
        )
        result = _execute_resample(task, ctx)
        assert not result.success
        assert "target key" in result.error

    def test_allocate_empty_join_fails(self, tmp_path: Path):
        """Allocate must fail when no rows match between data and crosswalk."""
        ctx = self._setup(tmp_path)
        # Overwrite with geo_ids that don't match the crosswalk
        ds_path = tmp_path / "data" / "coc_data.parquet"
        pd.DataFrame(
            {
                "coc_id": ["ZZZ1"],
                "pop": [999.0],
            }
        ).to_parquet(ds_path)
        task = ResampleTask(
            dataset_id="coc_data",
            year=2020,
            input_path="data/coc_data.parquet",
            effective_geometry=GeometryRef(type="coc", vintage=2025),
            method="allocate",
            transform_id="coc_to_tract",
            to_geometry=GeometryRef(type="tract", vintage=2020),
            measures=["pop"],
        )
        result = _execute_resample(task, ctx)
        assert not result.success
        assert "zero rows" in result.error

    def test_allocate_nan_weight_propagates(self, tmp_path: Path):
        """NaN area_share produces NaN allocated values (not silently dropped)."""
        ds_path = tmp_path / "data" / "coc_data.parquet"
        ds_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            {
                "coc_id": ["COC1"],
                "pop": [1000.0],
            }
        ).to_parquet(ds_path)

        xwalk_path = tmp_path / "data" / "curated" / "xwalks" / "xwalk_alloc.parquet"
        xwalk_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            {
                "coc_id": ["COC1", "COC1"],
                "tract_geoid": ["T1", "T2"],
                "area_share": [0.6, float("nan")],
            }
        ).to_parquet(xwalk_path)

        recipe = load_recipe(self._allocate_recipe())
        ctx = ExecutionContext(project_root=tmp_path, recipe=recipe)
        ctx.transform_paths["coc_to_tract"] = xwalk_path

        task = ResampleTask(
            dataset_id="coc_data",
            year=2020,
            input_path="data/coc_data.parquet",
            effective_geometry=GeometryRef(type="coc", vintage=2025),
            method="allocate",
            transform_id="coc_to_tract",
            to_geometry=GeometryRef(type="tract", vintage=2020),
            measures=["pop"],
        )
        result = _execute_resample(task, ctx)
        assert result.success
        df = ctx.intermediates[("coc_data", 2020)]
        t1 = df[df.geo_id == "T1"]["pop"].iloc[0]
        t2 = df[df.geo_id == "T2"]["pop"].iloc[0]
        assert t1 == pytest.approx(600.0)
        assert pd.isna(t2)


# ===========================================================================
# Temporal filter behavior tests
# ===========================================================================


class TestTemporalFilters:
    def test_calendar_mean_preserves_year_groups(self):
        """Calendar mean should aggregate within year, not across years."""
        df = pd.DataFrame(
            {
                "geo_id": ["A", "A", "A", "A"],
                "year": [2020, 2020, 2021, 2021],
                "month": [1, 2, 1, 2],
                "value": [10.0, 20.0, 30.0, 50.0],
            }
        )

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
        df = pd.DataFrame(
            {
                "geo_id": ["A", "A", "A", "A"],
                "pit_year": [2020, 2020, 2021, 2021],
                "month": [1, 2, 1, 2],
                "value": [10.0, 30.0, 20.0, 80.0],
            }
        )

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

    def test_declared_year_column_missing_raises(self):
        """Declared year_column absent from DataFrame should raise ExecutorError."""
        df = pd.DataFrame(
            {
                "geo_id": ["A", "A"],
                "month": [1, 2],
                "value": [10.0, 20.0],
            }
        )

        with pytest.raises(ExecutorError, match="declared year column.*'pit_year'.*not found"):
            _apply_temporal_filter(
                df,
                filt=TemporalFilter(type="temporal", column="month", method="calendar_mean"),
                year=2020,
                dataset_id="demo",
                year_column="pit_year",
            )

    def test_empty_group_cols_raises(self):
        """When temporal column is the only non-numeric column, group_cols is empty."""
        # All columns except 'month' are numeric; no year column present.
        df = pd.DataFrame(
            {
                "month": [1, 2, 3],
                "value_a": [10.0, 20.0, 30.0],
                "value_b": [1.0, 2.0, 3.0],
            }
        )

        with pytest.raises(ExecutorError, match="no grouping columns found"):
            _apply_temporal_filter(
                df,
                filt=TemporalFilter(type="temporal", column="month", method="calendar_mean"),
                year=2020,
                dataset_id="demo",
            )

    def test_all_numeric_except_temporal_with_year_fallback(self):
        """Numeric 'year' column is auto-added as group key even when all others are numeric."""
        # Every column except 'month' is numeric, but 'year' exists so the
        # fallback on line 545-546 rescues group_cols from being empty.
        df = pd.DataFrame(
            {
                "year": [2020, 2020, 2021, 2021],
                "month": [1, 2, 1, 2],
                "value": [10.0, 30.0, 50.0, 70.0],
            }
        )

        out = _apply_temporal_filter(
            df,
            filt=TemporalFilter(type="temporal", column="month", method="calendar_mean"),
            year=2020,
            dataset_id="demo",
        )

        # year should be preserved as a group key
        assert "year" in out.columns
        assert set(out["year"]) == {2020, 2021}
        val_2020 = out.loc[out["year"] == 2020, "value"].iloc[0]
        val_2021 = out.loc[out["year"] == 2021, "value"].iloc[0]
        assert val_2020 == pytest.approx(20.0)
        assert val_2021 == pytest.approx(60.0)

    # --- interpolate_to_month tests ----------------------------------------

    def test_interpolate_to_month_basic_january(self):
        """PEP July→January: jan(Y) = 0.5 * jul(Y-1) + 0.5 * jul(Y)."""
        df = pd.DataFrame(
            {
                "county_fips": ["01001"] * 3,
                "year": [2018, 2019, 2020],
                "reference_date": pd.to_datetime(["2018-07-01", "2019-07-01", "2020-07-01"]),
                "population": [1000.0, 1100.0, 1200.0],
            }
        )

        out = _apply_temporal_filter(
            df,
            filt=TemporalFilter(
                type="temporal",
                column="reference_date",
                method="interpolate_to_month",
                month=1,
            ),
            year=2019,
            dataset_id="pep",
            year_column="year",
        )

        assert "reference_date" not in out.columns
        assert set(out["year"]) == {2018, 2019, 2020}
        # 2018: no prior year → raw value 1000
        assert out.loc[out["year"] == 2018, "population"].iloc[0] == pytest.approx(1000.0)
        # 2019: 0.5 * 1000 + 0.5 * 1100 = 1050
        assert out.loc[out["year"] == 2019, "population"].iloc[0] == pytest.approx(1050.0)
        # 2020: 0.5 * 1100 + 0.5 * 1200 = 1150
        assert out.loc[out["year"] == 2020, "population"].iloc[0] == pytest.approx(1150.0)

    def test_interpolate_to_month_multiple_geos(self):
        """Interpolation groups by geography independently."""
        df = pd.DataFrame(
            {
                "county_fips": ["A", "A", "B", "B"],
                "year": [2019, 2020, 2019, 2020],
                "reference_date": pd.to_datetime(
                    ["2019-07-01", "2020-07-01", "2019-07-01", "2020-07-01"]
                ),
                "population": [100.0, 200.0, 500.0, 600.0],
            }
        )

        out = _apply_temporal_filter(
            df,
            filt=TemporalFilter(
                type="temporal",
                column="reference_date",
                method="interpolate_to_month",
                month=1,
            ),
            year=2020,
            dataset_id="pep",
            year_column="year",
        )

        a_2020 = out.loc[(out["county_fips"] == "A") & (out["year"] == 2020), "population"].iloc[0]
        b_2020 = out.loc[(out["county_fips"] == "B") & (out["year"] == 2020), "population"].iloc[0]
        assert a_2020 == pytest.approx(150.0)  # 0.5*100 + 0.5*200
        assert b_2020 == pytest.approx(550.0)  # 0.5*500 + 0.5*600

    def test_interpolate_to_month_same_source_target(self):
        """When source month == target month, no interpolation needed."""
        df = pd.DataFrame(
            {
                "geo_id": ["A", "A"],
                "year": [2019, 2020],
                "reference_date": pd.to_datetime(["2019-07-01", "2020-07-01"]),
                "value": [10.0, 20.0],
            }
        )

        out = _apply_temporal_filter(
            df,
            filt=TemporalFilter(
                type="temporal",
                column="reference_date",
                method="interpolate_to_month",
                month=7,
            ),
            year=2020,
            dataset_id="demo",
            year_column="year",
        )

        assert "reference_date" not in out.columns
        assert set(out["year"]) == {2019, 2020}
        assert out.loc[out["year"] == 2020, "value"].iloc[0] == pytest.approx(20.0)

    def test_interpolate_to_month_string_year_column(self):
        """String-typed year column must not crash arithmetic."""
        df = pd.DataFrame(
            {
                "county_fips": ["01001"] * 3,
                "year": ["2018", "2019", "2020"],
                "reference_date": pd.to_datetime(
                    [
                        "2018-07-01",
                        "2019-07-01",
                        "2020-07-01",
                    ]
                ),
                "population": [1000.0, 1100.0, 1200.0],
            }
        )

        out = _apply_temporal_filter(
            df,
            filt=TemporalFilter(
                type="temporal",
                column="reference_date",
                method="interpolate_to_month",
                month=1,
            ),
            year=2019,
            dataset_id="pep",
            year_column="year",
        )

        assert out["year"].dtype == object  # preserved as string
        # jan(2019) = 0.5*jul(2018) + 0.5*jul(2019) = 0.5*1000 + 0.5*1100 = 1050
        row_2019 = out.loc[out["year"] == "2019"]
        assert len(row_2019) == 1
        assert row_2019["population"].iloc[0] == pytest.approx(1050.0)

    def test_interpolate_to_month_string_year_target_after_source(self):
        """String year column with target_month > source_month."""
        df = pd.DataFrame(
            {
                "geo_id": ["A", "A", "A"],
                "year": ["2019", "2020", "2021"],
                "reference_date": pd.to_datetime(
                    [
                        "2019-01-01",
                        "2020-01-01",
                        "2021-01-01",
                    ]
                ),
                "value": [100.0, 200.0, 300.0],
            }
        )

        out = _apply_temporal_filter(
            df,
            filt=TemporalFilter(
                type="temporal",
                column="reference_date",
                method="interpolate_to_month",
                month=7,
            ),
            year=2020,
            dataset_id="demo",
            year_column="year",
        )

        assert out["year"].dtype == object  # preserved as string
        # jul(2020) between jan(2020) and jan(2021): fraction=6/12=0.5
        # 0.5*200 + 0.5*300 = 250
        row_2020 = out.loc[out["year"] == "2020"]
        assert len(row_2020) == 1
        assert row_2020["value"].iloc[0] == pytest.approx(250.0)

    def test_interpolate_to_month_no_year_col_raises(self):
        """Missing year column raises ExecutorError."""
        df = pd.DataFrame(
            {
                "geo_id": ["A"],
                "reference_date": pd.to_datetime(["2020-07-01"]),
                "value": [10.0],
            }
        )

        with pytest.raises(ExecutorError, match="requires a year column"):
            _apply_temporal_filter(
                df,
                filt=TemporalFilter(
                    type="temporal",
                    column="reference_date",
                    method="interpolate_to_month",
                    month=1,
                ),
                year=2020,
                dataset_id="demo",
            )

    def test_interpolate_to_month_non_datetime_raises(self):
        """Non-datetime column raises ExecutorError."""
        df = pd.DataFrame(
            {
                "geo_id": ["A"],
                "year": [2020],
                "reference_date": [7],
                "value": [10.0],
            }
        )

        with pytest.raises(ExecutorError, match="requires a datetime column"):
            _apply_temporal_filter(
                df,
                filt=TemporalFilter(
                    type="temporal",
                    column="reference_date",
                    method="interpolate_to_month",
                    month=1,
                ),
                year=2020,
                dataset_id="demo",
                year_column="year",
            )


# ===========================================================================
# Column resolution safety tests
# ===========================================================================


class TestColumnResolution:
    def test_declared_geo_column_used(self, tmp_path: Path):
        """When geo_column is declared, the executor uses it."""
        ds_path = tmp_path / "data" / "ds.parquet"
        ds_path.parent.mkdir(parents=True)
        pd.DataFrame(
            {
                "my_geo": ["T1", "T2"],
                "year": [2020, 2020],
                "val": [10, 20],
            }
        ).to_parquet(ds_path)

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
        pd.DataFrame(
            {
                "GEOID": ["T1"],
                "year": [2020],
                "val": [10],
            }
        ).to_parquet(ds_path)

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
        pd.DataFrame(
            {
                "coc_id": ["COC1", "COC1"],
                "pit_year": [2020, 2021],
                "val": [10, 20],
            }
        ).to_parquet(ds_path)

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

    def test_declared_year_column_string_values_used(self, tmp_path: Path):
        """String-typed year columns should still match the requested year."""
        ds_path = tmp_path / "data" / "ds.parquet"
        ds_path.parent.mkdir(parents=True)
        pd.DataFrame(
            {
                "metro_id": ["GF01", "GF01"],
                "acs1_vintage": ["2023", "2024"],
                "val": [10, 20],
            }
        ).to_parquet(ds_path)

        data = _recipe_with_pipeline()
        data["datasets"]["pit"]["year_column"] = "acs1_vintage"
        data["datasets"]["pit"]["geo_column"] = "metro_id"
        recipe = load_recipe(data)
        ctx = ExecutionContext(project_root=tmp_path, recipe=recipe)

        task = ResampleTask(
            dataset_id="pit",
            year=2023,
            input_path="data/ds.parquet",
            effective_geometry=GeometryRef(type="metro"),
            method="identity",
            transform_id=None,
            to_geometry=GeometryRef(type="metro", source="glynn_fox_v1"),
            measures=["val"],
            year_column="acs1_vintage",
            geo_column="metro_id",
        )
        result = _execute_resample(task, ctx)
        assert result.success
        df = ctx.intermediates[("pit", 2023)]
        assert len(df) == 1
        assert df["year"].iloc[0] == 2023
        assert df["val"].iloc[0] == 10

    def test_lagged_acs1_vintage_executes_with_analysis_year(self, tmp_path: Path):
        """Lagged ACS1 vintages should survive execution without breaking joins."""
        ds_path = tmp_path / "data" / "ds.parquet"
        ds_path.parent.mkdir(parents=True)
        pd.DataFrame(
            {
                "metro_id": ["GF01", "GF02"],
                "acs1_vintage": [2022, 2022],
                "val": [10, 20],
            }
        ).to_parquet(ds_path)

        data = _recipe_with_pipeline()
        data["datasets"]["pit"]["provider"] = "census"
        data["datasets"]["pit"]["product"] = "acs1"
        data["datasets"]["pit"]["year_column"] = "acs1_vintage"
        data["datasets"]["pit"]["geo_column"] = "metro_id"
        recipe = load_recipe(data)
        ctx = ExecutionContext(project_root=tmp_path, recipe=recipe)

        task = ResampleTask(
            dataset_id="pit",
            year=2023,
            input_path="data/ds.parquet",
            effective_geometry=GeometryRef(type="metro"),
            method="identity",
            transform_id=None,
            to_geometry=GeometryRef(type="metro", source="glynn_fox_v1"),
            measures=["val"],
            year_column="acs1_vintage",
            geo_column="metro_id",
        )
        result = _execute_resample(task, ctx)
        assert result.success
        assert any("lagged ACS1 vintage 2022" in note for note in result.notes)
        df = ctx.intermediates[("pit", 2023)]
        assert set(df["year"]) == {2023}
        assert set(df["acs1_vintage"]) == {2022}
        assert list(df["val"]) == [10, 20]

    def test_multi_vintage_acs1_source_errors(self, tmp_path: Path):
        """ACS1 source with multiple distinct vintages (none matching) should error."""
        ds_path = tmp_path / "data" / "ds.parquet"
        ds_path.parent.mkdir(parents=True)
        pd.DataFrame(
            {
                "metro_id": ["GF01", "GF02"],
                "acs1_vintage": [2021, 2022],
                "val": [10, 20],
            }
        ).to_parquet(ds_path)

        data = _recipe_with_pipeline()
        data["datasets"]["pit"]["provider"] = "census"
        data["datasets"]["pit"]["product"] = "acs1"
        data["datasets"]["pit"]["year_column"] = "acs1_vintage"
        data["datasets"]["pit"]["geo_column"] = "metro_id"
        recipe = load_recipe(data)
        ctx = ExecutionContext(project_root=tmp_path, recipe=recipe)

        task = ResampleTask(
            dataset_id="pit",
            year=2023,
            input_path="data/ds.parquet",
            effective_geometry=GeometryRef(type="metro"),
            method="identity",
            transform_id=None,
            to_geometry=GeometryRef(type="metro", source="glynn_fox_v1"),
            measures=["val"],
            year_column="acs1_vintage",
            geo_column="metro_id",
        )
        result = _execute_resample(task, ctx)
        assert not result.success
        assert "no rows after filtering acs1_vintage==2023" in result.error

    def test_non_acs1_dataset_no_lagged_tolerance(self, tmp_path: Path):
        """Non-ACS1 dataset with acs1_vintage column should not get lagged tolerance."""
        ds_path = tmp_path / "data" / "ds.parquet"
        ds_path.parent.mkdir(parents=True)
        pd.DataFrame(
            {
                "coc_id": ["NY-600"],
                "acs1_vintage": [2022],
                "val": [10],
            }
        ).to_parquet(ds_path)

        data = _recipe_with_pipeline()
        data["datasets"]["pit"]["year_column"] = "acs1_vintage"
        data["datasets"]["pit"]["geo_column"] = "coc_id"
        recipe = load_recipe(data)
        ctx = ExecutionContext(project_root=tmp_path, recipe=recipe)

        task = ResampleTask(
            dataset_id="pit",
            year=2023,
            input_path="data/ds.parquet",
            effective_geometry=GeometryRef(type="coc"),
            method="identity",
            transform_id=None,
            to_geometry=GeometryRef(type="coc", vintage=2025),
            measures=["val"],
            year_column="acs1_vintage",
            geo_column="coc_id",
        )
        result = _execute_resample(task, ctx)
        assert not result.success
        assert "no rows after filtering acs1_vintage==2023" in result.error

    def test_ambiguous_geo_column_errors(self, tmp_path: Path):
        """Multiple geo-ID candidate columns without declaration should error."""
        ds_path = tmp_path / "data" / "ds.parquet"
        ds_path.parent.mkdir(parents=True)
        pd.DataFrame(
            {
                "geo_id": ["T1"],
                "GEOID": ["T1"],
                "year": [2020],
                "val": [10],
            }
        ).to_parquet(ds_path)

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
        panel_file = (
            _default_recipe_output_dir(tmp_path, "executor-test")
            / "panel__Y2020-2021@B2025.parquet"
        )
        assert panel_file.exists()

    def test_panel_contains_all_years(self, tmp_path: Path):
        _setup_pipeline_fixtures(tmp_path)
        data = _recipe_with_pipeline()
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        data["datasets"]["acs"]["path"] = "data/acs.parquet"
        recipe = load_recipe(data)
        execute_recipe(recipe, project_root=tmp_path)
        panel_file = (
            _default_recipe_output_dir(tmp_path, "executor-test")
            / "panel__Y2020-2021@B2025.parquet"
        )
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
        panel_file = (
            _default_recipe_output_dir(tmp_path, "executor-test")
            / "panel__Y2020-2021@B2025.parquet"
        )
        table = pq.read_table(panel_file)
        metadata = table.schema.metadata
        assert b"hhplab_provenance" in metadata
        prov = json_mod.loads(metadata[b"hhplab_provenance"])
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
            s for s in data["pipelines"][0]["steps"] if not (isinstance(s, dict) and "join" in s)
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
        result = runner.invoke(
            app,
            [
                "build",
                "recipe",
                "--recipe",
                str(recipe_file),
            ],
        )
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

    def test_same_object_on_repeated_read(self, tmp_path: Path):
        """read_parquet() returns the *same* internal object (identity) for
        the cached DataFrame, though returned copies differ."""
        f = tmp_path / "data.parquet"
        pd.DataFrame({"a": [10, 20]}).to_parquet(f)
        cache = RecipeCache(enabled=True)
        df1 = cache.read_parquet(f)
        df2 = cache.read_parquet(f)
        # Returned copies are equal but not the same Python object
        assert df1.equals(df2)
        assert df1 is not df2
        # Underlying cached frame is the single stored object
        key = str(f.resolve())
        assert cache._frames[key] is cache._frames[key]

    def test_stale_read_after_file_changes(self, tmp_path: Path):
        """Cache returns the *original* content when the underlying file
        changes on disk — documenting the lack of TTL / invalidation."""
        f = tmp_path / "data.parquet"
        pd.DataFrame({"v": [1]}).to_parquet(f)
        cache = RecipeCache(enabled=True)
        df_original = cache.read_parquet(f)

        # Overwrite with new content
        pd.DataFrame({"v": [999]}).to_parquet(f)
        df_after = cache.read_parquet(f)

        # Cache still returns the stale (original) data
        assert list(df_after["v"]) == [1]
        assert df_original.equals(df_after)
        # A fresh, uncached read would see the new data
        assert list(pd.read_parquet(f)["v"]) == [999]

    def test_cross_pipeline_cache_sharing(self, tmp_path: Path):
        """A single RecipeCache shared across two pipeline executions serves
        cached frames to the second pipeline without re-reading disk."""
        f = tmp_path / "shared.parquet"
        pd.DataFrame({"z": [5, 6, 7]}).to_parquet(f)

        cache = RecipeCache(enabled=True)

        # Simulate pipeline-1 reading the file
        df_p1 = cache.read_parquet(f)
        assert cache.cached_count == 1

        # Simulate pipeline-2 reading the same file through the same cache
        df_p2 = cache.read_parquet(f)
        assert cache.cached_count == 1  # no new entry — same file reused

        # Both pipelines see identical data
        assert df_p1.equals(df_p2)
        # But they received independent copies (mutation-safe)
        df_p1["z"] = 0
        df_p3 = cache.read_parquet(f)
        assert list(df_p3["z"]) == [5, 6, 7]


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

    def test_export_skips_missing_files_with_warning(self, tmp_path: Path, caplog):
        """Regression coclab-kbk1: missing assets emit a warning."""
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
        import logging

        with caplog.at_level(logging.WARNING, logger="hhplab.recipe.manifest"):
            export_bundle(m, tmp_path, out)
        assert (out / "manifest.json").exists()
        assert "skipping missing asset" in caplog.text

    def test_export_bundle_deleted_asset_skipped(self, tmp_path: Path, caplog):
        """Asset present during execution then deleted before export is skipped."""
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

        # Delete the source file after manifest was built
        src.unlink()

        out = tmp_path / "bundle"
        import logging

        with caplog.at_level(logging.WARNING, logger="hhplab.recipe.manifest"):
            result = export_bundle(m, tmp_path, out)

        assert result == out
        assert (out / "manifest.json").exists()
        assert not (out / "assets" / "data" / "pit.parquet").exists()
        assert "skipping missing asset" in caplog.text

    def test_export_bundle_completeness_with_partial_missing(
        self,
        tmp_path: Path,
        caplog,
    ):
        """Bundle copies present assets and skips deleted ones."""
        (tmp_path / "data").mkdir()
        present = tmp_path / "data" / "acs.parquet"
        pd.DataFrame({"y": [2]}).to_parquet(present)
        deleted = tmp_path / "data" / "pit.parquet"
        pd.DataFrame({"x": [1]}).to_parquet(deleted)

        m = RecipeManifest(
            recipe_name="test",
            recipe_version=1,
            pipeline_id="main",
            assets=[
                AssetRecord(
                    role="dataset",
                    path="data/acs.parquet",
                    sha256="def",
                    size=200,
                ),
                AssetRecord(
                    role="dataset",
                    path="data/pit.parquet",
                    sha256="abc",
                    size=100,
                ),
            ],
        )

        # Remove one file to simulate post-execution deletion
        deleted.unlink()

        out = tmp_path / "bundle"
        import logging

        with caplog.at_level(logging.WARNING, logger="hhplab.recipe.manifest"):
            export_bundle(m, tmp_path, out)

        # Present asset copied
        assert (out / "assets" / "data" / "acs.parquet").exists()
        # Deleted asset silently skipped
        assert not (out / "assets" / "data" / "pit.parquet").exists()
        # Manifest still records both assets (provenance is preserved)
        manifest = read_manifest(out / "manifest.json")
        assert len(manifest.assets) == 2
        assert {a.path for a in manifest.assets} == {
            "data/acs.parquet",
            "data/pit.parquet",
        }
        # Warning emitted only for the missing file
        assert caplog.text.count("skipping missing asset") == 1

    def test_export_rejects_absolute_path(self, tmp_path: Path):
        m = RecipeManifest(
            recipe_name="test",
            recipe_version=1,
            pipeline_id="main",
            assets=[
                AssetRecord(
                    role="dataset",
                    path="/etc/passwd",
                    sha256="abc",
                    size=0,
                ),
            ],
        )
        with pytest.raises(ValueError, match="Absolute asset path rejected"):
            export_bundle(m, tmp_path, tmp_path / "bundle")

    def test_export_rejects_path_traversal_source(self, tmp_path: Path):
        m = RecipeManifest(
            recipe_name="test",
            recipe_version=1,
            pipeline_id="main",
            assets=[
                AssetRecord(
                    role="dataset",
                    path="../../etc/passwd",
                    sha256="abc",
                    size=0,
                ),
            ],
        )
        with pytest.raises(ValueError, match="escapes project root"):
            export_bundle(m, tmp_path, tmp_path / "bundle")

    def test_export_rejects_path_traversal_dest(self, tmp_path: Path):
        """Path that resolves inside project but escapes assets dir."""
        # Create a source file inside the project root
        (tmp_path / "project" / "data").mkdir(parents=True)
        src = tmp_path / "project" / "data" / "ok.parquet"
        src.write_bytes(b"data")

        m = RecipeManifest(
            recipe_name="test",
            recipe_version=1,
            pipeline_id="main",
            assets=[
                AssetRecord(
                    role="dataset",
                    path="data/../../../escape.parquet",
                    sha256="abc",
                    size=0,
                ),
            ],
        )
        with pytest.raises(ValueError, match="escapes"):
            export_bundle(m, tmp_path / "project", tmp_path / "bundle")


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
            _default_recipe_output_dir(tmp_path, "executor-test")
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
            _default_recipe_output_dir(tmp_path, "executor-test")
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
            _default_recipe_output_dir(tmp_path, "executor-test")
            / "panel__Y2020-2021@B2025.parquet"
        )
        table = pq.read_table(panel_file)
        prov = json_mod.loads(table.schema.metadata[b"hhplab_provenance"])
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
            output_path="outputs/demo/out.parquet",
        )
        mf = tmp_path / "test.manifest.json"
        write_manifest(m, mf)

        result = runner.invoke(
            app,
            [
                "build",
                "recipe-provenance",
                "--manifest",
                str(mf),
            ],
        )
        assert result.exit_code == 0
        assert "demo" in result.output
        assert "pit" in result.output
        assert "aaaaaaaaaaaa" in result.output  # sha256 prefix

    def test_provenance_missing_manifest(self, tmp_path: Path):
        result = runner.invoke(
            app,
            [
                "build",
                "recipe-provenance",
                "--manifest",
                str(tmp_path / "nope.json"),
            ],
        )
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

        result = runner.invoke(
            app,
            [
                "build",
                "recipe-export",
                "--manifest",
                str(mf),
                "--destination",
                str(out),
            ],
        )
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
        result = runner.invoke(
            app,
            [
                "build",
                "recipe",
                "--recipe",
                str(recipe_file),
                "--no-cache",
            ],
        )
        assert result.exit_code == 0
        assert "executed" in result.output.lower()


# ===========================================================================
# --json output mode tests
# ===========================================================================


def _make_project_root(tmp_path: Path) -> None:
    """Create marker files so _check_working_directory() doesn't warn."""
    (tmp_path / "pyproject.toml").write_text("[project]\nname='test'\n")
    (tmp_path / "hhplab").mkdir(exist_ok=True)
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
        result = runner.invoke(
            app,
            [
                "build",
                "recipe",
                "--recipe",
                str(rf),
                "--dry-run",
                "--json",
                "--skip-preflight",
            ],
        )
        assert result.exit_code == 0
        out = json.loads(result.output)
        assert out["status"] == "ok"
        assert out["recipe_name"] == "executor-test"
        assert out["dry_run"] is True
        assert "validation" in out

    def test_json_full_execution(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ):
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        _setup_pipeline_fixtures(tmp_path)
        data = _recipe_with_pipeline()
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        data["datasets"]["acs"]["path"] = "data/acs.parquet"
        rf = self._write_recipe(tmp_path, data)
        result = runner.invoke(
            app,
            [
                "build",
                "recipe",
                "--recipe",
                str(rf),
                "--json",
            ],
        )
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

    def test_json_full_execution_reports_written_artifact_paths(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ):
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        _setup_pipeline_fixtures(tmp_path)
        data = _recipe_with_pipeline()
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        data["datasets"]["acs"]["path"] = "data/acs.parquet"
        data["targets"][0]["outputs"] = ["panel", "diagnostics"]
        rf = self._write_recipe(tmp_path, data)
        result = runner.invoke(
            app,
            [
                "build",
                "recipe",
                "--recipe",
                str(rf),
                "--json",
            ],
        )
        assert result.exit_code == 0
        out = json.loads(result.output)
        expected_artifacts = {
            "panel_path": "outputs/executor-test/panel__Y2020-2021@B2025.parquet",
            "manifest_path": "outputs/executor-test/panel__Y2020-2021@B2025.manifest.json",
            "diagnostics_path": "outputs/executor-test/panel__Y2020-2021@B2025__diagnostics.json",
        }
        assert out["artifacts"] == expected_artifacts
        assert out["pipelines"][0]["artifacts"] == expected_artifacts
        for rel_path in expected_artifacts.values():
            assert (tmp_path / rel_path).exists()

    def test_json_map_execution_reports_map_artifact_path(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ):
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        _setup_pipeline_fixtures(tmp_path)
        data = _recipe_with_pipeline()
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        data["datasets"]["acs"]["path"] = "data/acs.parquet"
        data["targets"][0]["outputs"] = ["map"]
        data["targets"][0]["map_spec"] = {
            "layers": [
                {
                    "geometry": {"type": "coc", "vintage": 2025},
                    "selector_ids": ["COC1"],
                }
            ]
        }
        rf = self._write_recipe(tmp_path, data)
        result = runner.invoke(
            app,
            [
                "build",
                "recipe",
                "--recipe",
                str(rf),
                "--json",
            ],
        )
        assert result.exit_code == 0
        out = json.loads(result.output)
        assert out["status"] == "ok"
        assert out["artifacts"] == {
            "map_path": "outputs/executor-test/map__Y2020-2021@B2025.html",
        }
        assert out["pipelines"][0]["success"] is True
        steps = out["pipelines"][0]["steps"]
        assert any(step["step_kind"] == "persist_map" for step in steps)
        assert (tmp_path / out["artifacts"]["map_path"]).exists()

    def test_json_suppresses_progress(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ):
        """JSON mode should not include human-readable progress lines."""
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        _setup_pipeline_fixtures(tmp_path)
        data = _recipe_with_pipeline()
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        data["datasets"]["acs"]["path"] = "data/acs.parquet"
        rf = self._write_recipe(tmp_path, data)
        result = runner.invoke(
            app,
            [
                "build",
                "recipe",
                "--recipe",
                str(rf),
                "--json",
            ],
        )
        # Output should be valid JSON (no interleaved echo lines)
        out = json.loads(result.output)
        assert isinstance(out, dict)

    def test_json_validation_error(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ):
        """Missing datasets now route through preflight as blocked status."""
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        data = _recipe_with_pipeline()
        # Introduce a missing required dataset path
        data["datasets"]["pit"]["path"] = "missing.parquet"
        data["datasets"]["acs"]["path"] = "also_missing.parquet"
        data["validation"] = {"missing_dataset": {"default": "fail"}}
        rf = self._write_recipe(tmp_path, data)
        result = runner.invoke(
            app,
            [
                "build",
                "recipe",
                "--recipe",
                str(rf),
                "--json",
            ],
        )
        assert result.exit_code == 1
        out = json.loads(result.output)
        assert out["status"] == "blocked"
        assert "preflight" in out
        assert out["preflight"]["blocking_count"] >= 2

    def test_json_preflight_reports_ct_alignment_warning(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ):
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        _patch_ct_recipe_alignment(monkeypatch)
        rf = self._write_recipe(tmp_path, _setup_ct_alignment_recipe(tmp_path))

        result = runner.invoke(
            app,
            [
                "build",
                "recipe-preflight",
                "--recipe",
                str(rf),
                "--json",
            ],
        )

        assert result.exit_code == 0
        out = json.loads(result.output)
        assert out["status"] == "ok"
        assert out["warning_count"] == 2
        ct_findings = [f for f in out["findings"] if f["kind"] == "ct_county_alignment"]
        assert len(ct_findings) == 2
        assert any("planning-region dataset 'pep_county'" in f["message"] for f in ct_findings)
        assert any("population_source 'weights'" in f["message"] for f in ct_findings)

    def test_json_preflight_blocks_when_ct_bridge_missing(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ):
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        _patch_ct_recipe_alignment_failure(monkeypatch)
        rf = self._write_recipe(tmp_path, _setup_ct_alignment_recipe(tmp_path))

        result = runner.invoke(
            app,
            [
                "build",
                "recipe-preflight",
                "--recipe",
                str(rf),
                "--json",
            ],
        )

        assert result.exit_code == 1
        out = json.loads(result.output)
        assert out["status"] == "blocked"
        ct_findings = [f for f in out["findings"] if f["kind"] == "ct_county_alignment"]
        assert ct_findings
        assert all(f["severity"] == "error" for f in ct_findings)
        assert all(
            f["remediation"]["command"] == "hhplab ingest tiger --year 2023 --type counties"
            for f in ct_findings
        )

    def test_json_build_includes_ct_alignment_notes(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ):
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        _patch_ct_recipe_alignment(monkeypatch)
        rf = self._write_recipe(tmp_path, _setup_ct_alignment_recipe(tmp_path))

        result = runner.invoke(
            app,
            [
                "build",
                "recipe",
                "--recipe",
                str(rf),
                "--json",
            ],
        )

        assert result.exit_code == 0
        out = json.loads(result.output)
        resample_steps = [
            step
            for pipeline in out["pipelines"]
            for step in pipeline["steps"]
            if step["step_kind"] == "resample"
        ]
        step_notes = [note for step in resample_steps for note in step["notes"]]
        assert any("planning-region dataset 'pep_county'" in note for note in step_notes)
        assert any("population_source 'weights'" in note for note in step_notes)

    def test_human_build_prints_ct_alignment_note(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ):
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        _patch_ct_recipe_alignment(monkeypatch)
        rf = self._write_recipe(tmp_path, _setup_ct_alignment_recipe(tmp_path))

        result = runner.invoke(
            app,
            [
                "build",
                "recipe",
                "--recipe",
                str(rf),
            ],
        )

        assert result.exit_code == 0
        assert "Connecticut special-case alignment applied" in result.output

    def test_json_provenance(self, tmp_path: Path):
        m = RecipeManifest(
            recipe_name="demo",
            recipe_version=1,
            pipeline_id="main",
            assets=[
                AssetRecord(
                    role="dataset",
                    path="data/x.parquet",
                    sha256="a" * 64,
                    size=100,
                    dataset_id="x",
                ),
            ],
        )
        mf = tmp_path / "test.manifest.json"
        write_manifest(m, mf)
        result = runner.invoke(
            app,
            [
                "build",
                "recipe-provenance",
                "--manifest",
                str(mf),
                "--json",
            ],
        )
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
                    role="dataset",
                    path="data/pit.parquet",
                    sha256="a" * 64,
                    size=100,
                ),
            ],
        )
        mf = tmp_path / "test.manifest.json"
        write_manifest(m, mf)
        out_dir = tmp_path / "bundle"
        result = runner.invoke(
            app,
            [
                "build",
                "recipe-export",
                "--manifest",
                str(mf),
                "--destination",
                str(out_dir),
                "--json",
            ],
        )
        assert result.exit_code == 0
        out = json.loads(result.output)
        assert out["status"] == "ok"
        assert out["assets_copied"] == 1

    def test_json_provenance_missing_manifest(self, tmp_path: Path):
        result = runner.invoke(
            app,
            [
                "build",
                "recipe-provenance",
                "--manifest",
                str(tmp_path / "nope.json"),
                "--json",
            ],
        )
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
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ):
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        data = _recipe_with_pipeline()
        rf = self._write_recipe(tmp_path, data)
        result = runner.invoke(
            app,
            [
                "build",
                "recipe-plan",
                "--recipe",
                str(rf),
            ],
        )
        assert result.exit_code == 0
        assert "Pipeline 'main'" in result.output
        assert "[materialize]" in result.output
        assert "[resample]" in result.output
        assert "[join]" in result.output

    def test_plan_human_output_shows_metro_definition_version(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ):
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        _setup_metro_pipeline_fixtures(tmp_path)
        data = _recipe_with_metro_pipeline()
        rf = self._write_recipe(tmp_path, data)
        result = runner.invoke(
            app,
            [
                "build",
                "recipe-plan",
                "--recipe",
                str(rf),
            ],
        )
        assert result.exit_code == 0
        assert "geometry=metro@glynn_fox_v1" in result.output
        assert "to=metro@glynn_fox_v1" in result.output

    def test_plan_json_output(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ):
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        data = _recipe_with_pipeline()
        rf = self._write_recipe(tmp_path, data)
        result = runner.invoke(
            app,
            [
                "build",
                "recipe-plan",
                "--recipe",
                str(rf),
                "--json",
            ],
        )
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
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ):
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        # Create the dataset file so validation passes
        pit_path = tmp_path / "data" / "pit.parquet"
        pd.DataFrame({"coc_id": ["C1"], "year": [2020], "pit_total": [1]}).to_parquet(pit_path)
        data = _recipe_with_pipeline()
        data["datasets"]["pit"]["path"] = "data/pit.parquet"
        rf = self._write_recipe(tmp_path, data)
        result = runner.invoke(
            app,
            [
                "build",
                "recipe-plan",
                "--recipe",
                str(rf),
                "--json",
            ],
        )
        out = json.loads(result.output)
        resample_tasks = out["pipelines"][0]["resample_tasks"]
        pit_tasks = [t for t in resample_tasks if t["dataset_id"] == "pit"]
        assert pit_tasks[0]["input_path"] == "data/pit.parquet"

    def test_plan_json_shows_geometry(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ):
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        data = _recipe_with_pipeline()
        rf = self._write_recipe(tmp_path, data)
        result = runner.invoke(
            app,
            [
                "build",
                "recipe-plan",
                "--recipe",
                str(rf),
                "--json",
            ],
        )
        out = json.loads(result.output)
        resample_tasks = out["pipelines"][0]["resample_tasks"]
        acs_task = next(t for t in resample_tasks if t["dataset_id"] == "acs")
        assert acs_task["effective_geometry"]["type"] == "tract"
        assert acs_task["effective_geometry"]["vintage"] == 2020
        assert acs_task["to_geometry"]["type"] == "coc"

    def test_plan_json_shows_transform_selection(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ):
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        data = _recipe_with_pipeline()
        rf = self._write_recipe(tmp_path, data)
        result = runner.invoke(
            app,
            [
                "build",
                "recipe-plan",
                "--recipe",
                str(rf),
                "--json",
            ],
        )
        out = json.loads(result.output)
        resample_tasks = out["pipelines"][0]["resample_tasks"]
        acs_task = next(t for t in resample_tasks if t["dataset_id"] == "acs")
        assert acs_task["transform_id"] == "tract_to_coc"

    def test_plan_planner_error_json(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ):
        _make_project_root(tmp_path)
        monkeypatch.chdir(tmp_path)
        data = _recipe_with_pipeline()
        # Add an unresolvable auto transform
        data["pipelines"][0]["steps"].insert(
            1,
            {
                "resample": {
                    "dataset": "acs",
                    "to_geometry": {"type": "county"},
                    "method": "aggregate",
                    "via": "auto",
                    "measures": ["total_population"],
                },
            },
        )
        rf = self._write_recipe(tmp_path, data)
        result = runner.invoke(
            app,
            [
                "build",
                "recipe-plan",
                "--recipe",
                str(rf),
                "--json",
            ],
        )
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
            len(d["materialize_tasks"]) + len(d["resample_tasks"]) + len(d["join_tasks"])
        )

    def test_plan_to_dict_geometry_fields(self):
        recipe = load_recipe(_recipe_with_pipeline())
        plan = resolve_plan(recipe, "main")
        d = plan.to_dict()
        rt = d["resample_tasks"][0]
        assert "type" in rt["effective_geometry"]
        assert "type" in rt["to_geometry"]


# ===========================================================================
# Cohort selector tests
# ===========================================================================


class TestCohortSelectorSchema:
    """Test CohortSelector validation in the recipe schema."""

    def test_target_with_cohort_parses(self):
        data = _minimal_recipe()
        data["targets"][0]["cohort"] = {
            "rank_by": "total_population",
            "method": "top_n",
            "n": 50,
            "reference_year": 2021,
        }
        recipe = load_recipe(data)
        assert recipe.targets[0].cohort is not None
        assert recipe.targets[0].cohort.method == "top_n"
        assert recipe.targets[0].cohort.n == 50

    def test_target_without_cohort(self):
        recipe = load_recipe(_minimal_recipe())
        assert recipe.targets[0].cohort is None

    def test_cohort_reference_year_outside_universe_rejected(self):
        data = _minimal_recipe()
        data["targets"][0]["cohort"] = {
            "rank_by": "total_population",
            "method": "top_n",
            "n": 10,
            "reference_year": 2000,
        }
        with pytest.raises(RecipeLoadError, match="reference_year"):
            load_recipe(data)

    def test_top_n_requires_n(self):
        from pydantic import ValidationError

        from hhplab.recipe.recipe_schema import CohortSelector

        with pytest.raises(ValidationError, match="requires 'n'"):
            CohortSelector(
                rank_by="total_population",
                method="top_n",
                reference_year=2021,
            )

    def test_percentile_requires_threshold(self):
        from pydantic import ValidationError

        from hhplab.recipe.recipe_schema import CohortSelector

        with pytest.raises(ValidationError, match="requires 'threshold'"):
            CohortSelector(
                rank_by="total_population",
                method="percentile",
                reference_year=2021,
            )

    def test_bottom_n_parses(self):
        data = _minimal_recipe()
        data["targets"][0]["cohort"] = {
            "rank_by": "total_population",
            "method": "bottom_n",
            "n": 10,
            "reference_year": 2021,
        }
        recipe = load_recipe(data)
        assert recipe.targets[0].cohort.method == "bottom_n"

    def test_percentile_parses(self):
        data = _minimal_recipe()
        data["targets"][0]["cohort"] = {
            "rank_by": "total_population",
            "method": "percentile",
            "threshold": 0.75,
            "reference_year": 2021,
        }
        recipe = load_recipe(data)
        assert recipe.targets[0].cohort.threshold == 0.75


class TestApplyCohortSelector:
    """Test the _apply_cohort_selector executor function."""

    def _make_panel(self) -> pd.DataFrame:
        """Panel with 5 geos across 2 years."""
        rows = []
        pops = {"G1": 100, "G2": 500, "G3": 300, "G4": 200, "G5": 400}
        for year in [2020, 2021]:
            for geo, pop in pops.items():
                rows.append({"geo_id": geo, "year": year, "total_population": pop})
        return pd.DataFrame(rows)

    def test_top_n(self):
        from hhplab.recipe.executor import _apply_cohort_selector
        from hhplab.recipe.recipe_schema import CohortSelector

        panel = self._make_panel()
        cohort = CohortSelector(
            rank_by="total_population",
            method="top_n",
            n=3,
            reference_year=2021,
        )
        result = _apply_cohort_selector(panel, cohort)
        selected_geos = set(result["geo_id"].unique())
        assert selected_geos == {"G2", "G5", "G3"}
        # All years included for selected geos
        assert len(result) == 6

    def test_bottom_n(self):
        from hhplab.recipe.executor import _apply_cohort_selector
        from hhplab.recipe.recipe_schema import CohortSelector

        panel = self._make_panel()
        cohort = CohortSelector(
            rank_by="total_population",
            method="bottom_n",
            n=2,
            reference_year=2021,
        )
        result = _apply_cohort_selector(panel, cohort)
        selected_geos = set(result["geo_id"].unique())
        assert selected_geos == {"G1", "G4"}
        assert len(result) == 4

    def test_percentile(self):
        from hhplab.recipe.executor import _apply_cohort_selector
        from hhplab.recipe.recipe_schema import CohortSelector

        panel = self._make_panel()
        # 0.5 threshold keeps geos >= median (300): G2=500, G5=400, G3=300
        cohort = CohortSelector(
            rank_by="total_population",
            method="percentile",
            threshold=0.5,
            reference_year=2021,
        )
        result = _apply_cohort_selector(panel, cohort)
        selected_geos = set(result["geo_id"].unique())
        assert "G2" in selected_geos
        assert "G5" in selected_geos
        assert "G1" not in selected_geos

    def test_missing_rank_column_raises(self):
        from hhplab.recipe.executor import ExecutorError, _apply_cohort_selector
        from hhplab.recipe.recipe_schema import CohortSelector

        panel = self._make_panel()
        cohort = CohortSelector(
            rank_by="nonexistent_col",
            method="top_n",
            n=3,
            reference_year=2021,
        )
        with pytest.raises(ExecutorError, match="rank_by column"):
            _apply_cohort_selector(panel, cohort)

    def test_empty_reference_year_raises(self):
        from hhplab.recipe.executor import ExecutorError, _apply_cohort_selector
        from hhplab.recipe.recipe_schema import CohortSelector

        panel = self._make_panel()
        cohort = CohortSelector(
            rank_by="total_population",
            method="top_n",
            n=3,
            reference_year=2099,
        )
        with pytest.raises(ExecutorError, match="reference_year"):
            _apply_cohort_selector(panel, cohort)


class TestCanonicalizeMetroNameBackfill:
    """Regression tests for coclab-vwa5: metro_name backfill with null values."""

    def _metro_geometry(self):
        return GeometryRef(type="metro", source="glynn_fox_v1")

    def test_backfills_when_column_absent(self):
        df = pd.DataFrame({"geo_id": ["GF01", "GF02"], "year": [2020, 2020]})
        result = _canonicalize_panel_for_target(df, self._metro_geometry())
        assert "metro_name" in result.columns
        assert result["metro_name"].notna().all()

    def test_backfills_when_column_has_nulls(self):
        df = pd.DataFrame(
            {
                "geo_id": ["GF01", "GF02"],
                "year": [2020, 2020],
                "metro_name": [None, None],
            }
        )
        result = _canonicalize_panel_for_target(df, self._metro_geometry())
        assert result["metro_name"].notna().all()

    def test_backfills_partial_nulls(self):
        df = pd.DataFrame(
            {
                "geo_id": ["GF01", "GF02"],
                "year": [2020, 2020],
                "metro_name": ["New York", None],
            }
        )
        result = _canonicalize_panel_for_target(df, self._metro_geometry())
        assert result["metro_name"].notna().all()
