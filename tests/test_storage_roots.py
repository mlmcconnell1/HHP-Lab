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

from coclab.config import StorageConfig, load_config
from coclab.recipe.manifest import (
    ROOT_ASSET_STORE,
    ROOT_OUTPUT,
    AssetRecord,
    RecipeManifest,
    export_bundle,
)

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
        from coclab.paths import curated_dir, raw_root

        cfg = StorageConfig(
            asset_store_root=tmp_path / "my-assets",
            output_root=tmp_path / "my-outputs",
        )
        assert raw_root(cfg) == tmp_path / "my-assets" / "raw"
        assert curated_dir("acs", cfg) == tmp_path / "my-assets" / "curated" / "acs"

    def test_custom_output_root(self, tmp_path: Path):
        """Custom output_root is used by path helpers."""
        from coclab.paths import output_root

        cfg = StorageConfig(
            asset_store_root=tmp_path / "my-assets",
            output_root=tmp_path / "my-outputs",
        )
        assert output_root(cfg) == tmp_path / "my-outputs"

    def test_env_vars_override_defaults(self, tmp_path: Path, monkeypatch):
        """Environment variables take precedence over defaults."""
        monkeypatch.setenv("COCLAB_ASSET_STORE_ROOT", str(tmp_path / "env-assets"))
        cfg = load_config(project_root=tmp_path)
        assert cfg.asset_store_root == Path(tmp_path / "env-assets")

    def test_cli_overrides_env_and_defaults(self, tmp_path: Path, monkeypatch):
        """CLI flags take precedence over everything."""
        monkeypatch.setenv("COCLAB_OUTPUT_ROOT", "/env/outputs")
        cfg = load_config(
            output_root=tmp_path / "cli-outputs",
            project_root=tmp_path,
        )
        assert cfg.output_root == tmp_path / "cli-outputs"


# ---------------------------------------------------------------------------
# Classify path helper
# ---------------------------------------------------------------------------


class TestClassifyPath:
    """Tests for _classify_path in the executor."""

    def test_classifies_asset_store_path(self, tmp_path: Path):
        from coclab.recipe.executor import ExecutionContext, _classify_path

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
        from coclab.recipe.executor import ExecutionContext, _classify_path

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
        from coclab.recipe.executor import ExecutionContext, _classify_path

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
