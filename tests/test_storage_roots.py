"""Regression tests for configurable storage roots.

Covers the key integration scenarios for the storage roots feature:
1. Export bundles with root-aware manifest records
2. Export bundles with mixed root-aware and legacy records
3. Custom asset_store_root / output_root for export resolution
4. Backward compatibility with legacy (root=None) manifests
5. _classify_path in the recipe executor
"""

from __future__ import annotations

from pathlib import Path

import pytest

from hhplab.recipe.loader import load_recipe
from hhplab.recipe.manifest import (
    ROOT_ASSET_STORE,
    ROOT_OUTPUT,
    AssetRecord,
    RecipeManifest,
    export_bundle,
)
from hhplab.storage.config import StorageConfig, load_config

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_file(path: Path, content: bytes = b"data") -> Path:
    """Create a file at the given path and return it."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


# ---------------------------------------------------------------------------
# Root-aware export
# ---------------------------------------------------------------------------


class TestRootAwareExport:
    """Export bundles with root-aware manifest records."""

    def test_asset_store_record_resolves_from_config(self, tmp_path: Path):
        """Asset with root=asset_store resolves from asset_store_root."""
        asset_root = tmp_path / "assets"
        _make_file(asset_root / "curated" / "xwalks" / "xwalk.parquet")

        cfg = StorageConfig(
            asset_store_root=asset_root,
            output_root=tmp_path / "outputs",
        )

        m = RecipeManifest(
            recipe_name="test",
            recipe_version=1,
            pipeline_id="main",
            assets=[
                AssetRecord(
                    role="crosswalk",
                    path="curated/xwalks/xwalk.parquet",
                    sha256="abc",
                    size=4,
                    root=ROOT_ASSET_STORE,
                ),
            ],
        )
        bundle = tmp_path / "bundle"
        export_bundle(m, tmp_path, bundle, storage_config=cfg)
        assert (bundle / "asset_store" / "curated" / "xwalks" / "xwalk.parquet").exists()

    def test_output_record_resolves_from_config(self, tmp_path: Path):
        """Asset with root=output resolves from output_root."""
        output_root = tmp_path / "outputs"
        _make_file(output_root / "panel.parquet")

        cfg = StorageConfig(
            asset_store_root=tmp_path / "assets",
            output_root=output_root,
        )

        m = RecipeManifest(
            recipe_name="test",
            recipe_version=1,
            pipeline_id="main",
            assets=[
                AssetRecord(
                    role="dataset",
                    path="panel.parquet",
                    sha256="def",
                    size=4,
                    root=ROOT_OUTPUT,
                ),
            ],
        )
        bundle = tmp_path / "bundle"
        export_bundle(m, tmp_path, bundle, storage_config=cfg)
        assert (bundle / "output" / "panel.parquet").exists()

    def test_mixed_roots_and_legacy(self, tmp_path: Path):
        """Bundle handles records from different roots plus legacy."""
        asset_root = tmp_path / "assets"
        output_root = tmp_path / "outputs"
        project_root = tmp_path / "project"

        _make_file(asset_root / "curated" / "pit.parquet")
        _make_file(output_root / "panel.parquet")
        _make_file(project_root / "data" / "legacy.parquet")

        cfg = StorageConfig(
            asset_store_root=asset_root,
            output_root=output_root,
        )

        m = RecipeManifest(
            recipe_name="test",
            recipe_version=1,
            pipeline_id="main",
            assets=[
                AssetRecord(
                    role="dataset",
                    path="curated/pit.parquet",
                    sha256="a",
                    size=4,
                    root=ROOT_ASSET_STORE,
                ),
                AssetRecord(
                    role="dataset",
                    path="panel.parquet",
                    sha256="b",
                    size=4,
                    root=ROOT_OUTPUT,
                ),
                AssetRecord(
                    role="dataset",
                    path="data/legacy.parquet",
                    sha256="c",
                    size=4,
                    root=None,
                ),
            ],
        )
        bundle = tmp_path / "bundle"
        export_bundle(m, project_root, bundle, storage_config=cfg)

        assert (bundle / "asset_store" / "curated" / "pit.parquet").exists()
        assert (bundle / "output" / "panel.parquet").exists()
        assert (bundle / "assets" / "data" / "legacy.parquet").exists()
        assert (bundle / "manifest.json").exists()


# ---------------------------------------------------------------------------
# Legacy backward compatibility
# ---------------------------------------------------------------------------


class TestLegacyExport:
    """Legacy (root=None) manifests continue to work."""

    def test_legacy_manifest_export(self, tmp_path: Path):
        """Legacy AssetRecord without root field exports correctly."""
        project_root = tmp_path / "project"
        _make_file(project_root / "data" / "pit.parquet")

        m = RecipeManifest(
            recipe_name="test",
            recipe_version=1,
            pipeline_id="main",
            assets=[
                AssetRecord(
                    role="dataset",
                    path="data/pit.parquet",
                    sha256="abc",
                    size=4,
                ),
            ],
        )
        bundle = tmp_path / "bundle"
        export_bundle(m, project_root, bundle)

        # Legacy exports go under assets/ (not asset_store/ or output/)
        assert (bundle / "assets" / "data" / "pit.parquet").exists()

    def test_legacy_manifest_serialization_roundtrip(self):
        """AssetRecord without root serializes/deserializes with root=None."""
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
        d = m.to_dict()
        restored = RecipeManifest.from_dict(d)
        assert restored.assets[0].root is None
        assert restored.assets[0].path == "data/pit.parquet"

    def test_root_aware_manifest_roundtrip(self):
        """AssetRecord with root survives serialization roundtrip."""
        m = RecipeManifest(
            recipe_name="test",
            recipe_version=1,
            pipeline_id="main",
            assets=[
                AssetRecord(
                    role="crosswalk",
                    path="curated/xwalks/xwalk.parquet",
                    sha256="def",
                    size=200,
                    root=ROOT_ASSET_STORE,
                ),
            ],
        )
        json_str = m.to_json()
        restored = RecipeManifest.from_json(json_str)
        assert restored.assets[0].root == ROOT_ASSET_STORE
        assert restored.assets[0].path == "curated/xwalks/xwalk.parquet"


# ---------------------------------------------------------------------------
# Config integration with path helpers
# ---------------------------------------------------------------------------


class TestConfigIntegrationWithPaths:
    """Config-driven path helpers produce correct results."""

    def test_custom_asset_store_root(self, tmp_path: Path):
        """Custom asset_store_root is used by path helpers."""
        from hhplab.storage.paths import curated_dir, raw_root

        cfg = StorageConfig(
            asset_store_root=tmp_path / "my-assets",
            output_root=tmp_path / "my-outputs",
        )
        assert raw_root(cfg) == tmp_path / "my-assets" / "raw"
        assert curated_dir("acs", cfg) == tmp_path / "my-assets" / "curated" / "acs"

    def test_custom_output_root(self, tmp_path: Path):
        """Custom output_root is used by path helpers."""
        from hhplab.storage.paths import output_root

        cfg = StorageConfig(
            asset_store_root=tmp_path / "my-assets",
            output_root=tmp_path / "my-outputs",
        )
        assert output_root(cfg) == tmp_path / "my-outputs"

    def test_env_vars_override_defaults(self, tmp_path: Path, monkeypatch):
        """Environment variables take precedence over defaults."""
        monkeypatch.setenv("HHPLAB_ASSET_STORE_ROOT", str(tmp_path / "env-assets"))
        cfg = load_config(project_root=tmp_path)
        assert cfg.asset_store_root == Path(tmp_path / "env-assets")

    def test_cli_overrides_env_and_defaults(self, tmp_path: Path, monkeypatch):
        """CLI flags take precedence over everything."""
        monkeypatch.setenv("HHPLAB_OUTPUT_ROOT", "/env/outputs")
        cfg = load_config(
            output_root=tmp_path / "cli-outputs",
            project_root=tmp_path,
        )
        assert cfg.output_root == tmp_path / "cli-outputs"

    def test_recipe_output_root_precedence_and_separate_asset_root(
        self, tmp_path: Path, monkeypatch
    ):
        project_root = tmp_path / "project"
        project_root.mkdir()
        (project_root / "hhplab.yaml").write_text("output_root: repo-outputs\n")
        recipe_dir = tmp_path / "recipes"
        recipe_dir.mkdir()

        monkeypatch.setenv("HHPLAB_OUTPUT_ROOT", "env-outputs")
        env_config = load_config(
            recipe_output_root="recipe-outputs",
            recipe_base_dir=recipe_dir,
            project_root=project_root,
        )
        assert env_config.output_root == Path.cwd() / "env-outputs"

        monkeypatch.delenv("HHPLAB_OUTPUT_ROOT")
        recipe_config = load_config(
            recipe_output_root="recipe-outputs",
            recipe_base_dir=recipe_dir,
            project_root=project_root,
        )
        assert recipe_config.output_root == recipe_dir / "recipe-outputs"

        cli_config = load_config(
            output_root=tmp_path / "cli-outputs",
            recipe_output_root="recipe-outputs",
            recipe_base_dir=recipe_dir,
            project_root=project_root,
        )
        assert cli_config.output_root == tmp_path / "cli-outputs"
        assert cli_config.asset_store_root == project_root / "data"

    def test_project_config_is_discovered_from_nested_working_directory(
        self, tmp_path: Path, monkeypatch
    ):
        project_root = tmp_path / "project"
        nested = project_root / "nested" / "command"
        nested.mkdir(parents=True)
        (project_root / "hhplab.yaml").write_text("output_root: configured-output\n")
        monkeypatch.chdir(nested)

        cfg = load_config()

        assert cfg.output_root == project_root / "configured-output"

    def test_relative_recipe_root_requires_source_base_unless_overridden(
        self, tmp_path: Path, monkeypatch
    ):
        with pytest.raises(ValueError, match="explicit source_base_dir"):
            load_config(recipe_output_root="relative-output", project_root=tmp_path)

        monkeypatch.setenv("HHPLAB_OUTPUT_ROOT", str(tmp_path / "env-output"))
        cfg = load_config(recipe_output_root="relative-output", project_root=tmp_path)
        assert cfg.output_root == tmp_path / "env-output"

    def test_recipe_relative_root_uses_loaded_recipe_origin_and_is_private(
        self, tmp_path: Path, monkeypatch
    ):
        recipe_path = tmp_path / "recipes" / "example.yaml"
        recipe_path.parent.mkdir()
        recipe_path.write_text(
            "version: 1\n"
            "name: root-test\n"
            "output_root: products\n"
            "universe: {years: [2020]}\n"
            "targets: []\n"
            "datasets: {}\n"
            "pipelines: []\n"
        )
        monkeypatch.chdir(tmp_path)
        recipe = load_recipe(recipe_path)
        cfg = load_config(
            recipe_output_root=recipe.output_root,
            recipe_base_dir=recipe.source_base_dir,
            project_root=tmp_path,
        )
        assert recipe.source_base_dir == recipe_path.parent.resolve()
        assert cfg.output_root == recipe_path.parent / "products"
        assert "source_base_dir" not in recipe.model_dump()
        assert "_source_base_dir" not in recipe.model_dump()

    def test_in_memory_recipe_source_base_is_explicit(self, tmp_path: Path):
        data = {
            "version": 1,
            "name": "memory-root-test",
            "output_root": "products",
            "universe": {"years": [2020]},
            "targets": [],
            "datasets": {},
            "pipelines": [],
        }
        recipe = load_recipe(data, source_base_dir=tmp_path)
        cfg = load_config(
            recipe_output_root=recipe.output_root,
            recipe_base_dir=recipe.source_base_dir,
            project_root=tmp_path / "project",
        )
        assert recipe.source_base_dir == tmp_path.resolve()
        assert cfg.output_root == tmp_path / "products"

    def test_tilde_paths_expand_for_recipe_source_and_output_root(
        self, tmp_path: Path, monkeypatch
    ):
        monkeypatch.setenv("HOME", str(tmp_path))
        recipe_path = tmp_path / "recipes" / "example.yaml"
        recipe_path.parent.mkdir()
        recipe_path.write_text(
            "version: 1\n"
            "name: tilde-root-test\n"
            "output_root: ~/published\n"
            "universe: {years: [2020]}\n"
            "targets: []\n"
            "datasets: {}\n"
            "pipelines: []\n"
        )

        recipe = load_recipe("~/recipes/example.yaml")
        cfg = load_config(
            recipe_output_root=recipe.output_root,
            recipe_base_dir=recipe.source_base_dir,
            project_root=tmp_path / "project",
        )
        assert recipe.source_base_dir == recipe_path.parent.resolve()
        assert cfg.output_root == tmp_path / "published"


# ---------------------------------------------------------------------------
# Classify path helper
# ---------------------------------------------------------------------------


class TestClassifyPath:
    """Tests for _classify_path in the executor."""

    def test_classifies_asset_store_path(self, tmp_path: Path):
        from hhplab.recipe.executor import ExecutionContext, _classify_path

        cfg = StorageConfig(
            asset_store_root=tmp_path / "assets",
            output_root=tmp_path / "outputs",
        )
        ctx = ExecutionContext(
            project_root=tmp_path,
            recipe=None,  # type: ignore[arg-type]
            storage_config=cfg,
        )
        root, rel = _classify_path(tmp_path / "assets" / "curated" / "pit.parquet", ctx)
        assert root == ROOT_ASSET_STORE
        assert rel == "curated/pit.parquet"

    def test_classifies_output_path(self, tmp_path: Path):
        from hhplab.recipe.executor import ExecutionContext, _classify_path

        cfg = StorageConfig(
            asset_store_root=tmp_path / "assets",
            output_root=tmp_path / "outputs",
        )
        ctx = ExecutionContext(
            project_root=tmp_path,
            recipe=None,  # type: ignore[arg-type]
            storage_config=cfg,
        )
        root, rel = _classify_path(tmp_path / "outputs" / "panel.parquet", ctx)
        assert root == ROOT_OUTPUT
        assert rel == "panel.parquet"

    def test_classifies_project_relative_fallback(self, tmp_path: Path):
        from hhplab.recipe.executor import ExecutionContext, _classify_path

        cfg = StorageConfig(
            asset_store_root=tmp_path / "assets",
            output_root=tmp_path / "outputs",
        )
        ctx = ExecutionContext(
            project_root=tmp_path,
            recipe=None,  # type: ignore[arg-type]
            storage_config=cfg,
        )
        root, rel = _classify_path(tmp_path / "other" / "file.txt", ctx)
        assert root is None
        assert rel == "other/file.txt"
