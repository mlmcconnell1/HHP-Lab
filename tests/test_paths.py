"""Tests for hhplab.storage.paths — centralized path resolution helpers."""

from __future__ import annotations

from pathlib import Path

from hhplab.storage.config import StorageConfig
from hhplab.storage.paths import (
    asset_store_root,
    curated_dir,
    curated_root,
    output_dir,
    output_root,
    raw_root,
)

# Fixture config with custom roots for deterministic tests.
ASSET_ROOT = Path("/test/assets")
OUTPUT_ROOT = Path("/test/outputs")
CFG = StorageConfig(asset_store_root=ASSET_ROOT, output_root=OUTPUT_ROOT)


# ---------------------------------------------------------------------------
# Asset store helpers
# ---------------------------------------------------------------------------


class TestAssetStoreHelpers:
    def test_asset_store_root(self) -> None:
        assert asset_store_root(CFG) == ASSET_ROOT

    def test_raw_root(self) -> None:
        assert raw_root(CFG) == ASSET_ROOT / "raw"

    def test_curated_root(self) -> None:
        assert curated_root(CFG) == ASSET_ROOT / "curated"

    def test_curated_dir_acs(self) -> None:
        assert curated_dir("acs", CFG) == ASSET_ROOT / "curated" / "acs"

    def test_curated_dir_xwalks(self) -> None:
        assert curated_dir("xwalks", CFG) == ASSET_ROOT / "curated" / "xwalks"

    def test_curated_dir_pit(self) -> None:
        assert curated_dir("pit", CFG) == ASSET_ROOT / "curated" / "pit"

    def test_curated_dir_tiger(self) -> None:
        assert curated_dir("tiger", CFG) == ASSET_ROOT / "curated" / "tiger"

    def test_curated_dir_zori(self) -> None:
        assert curated_dir("zori", CFG) == ASSET_ROOT / "curated" / "zori"

    def test_curated_dir_pep(self) -> None:
        assert curated_dir("pep", CFG) == ASSET_ROOT / "curated" / "pep"

    def test_curated_dir_measures(self) -> None:
        assert curated_dir("measures", CFG) == ASSET_ROOT / "curated" / "measures"

    def test_curated_dir_maps(self) -> None:
        assert curated_dir("maps", CFG) == ASSET_ROOT / "curated" / "maps"


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------


class TestOutputHelpers:
    def test_output_root(self) -> None:
        assert output_root(CFG) == OUTPUT_ROOT

    def test_output_dir_panel(self) -> None:
        assert output_dir("panel", CFG) == OUTPUT_ROOT / "panel"

    def test_output_dir_diagnostics(self) -> None:
        assert output_dir("diagnostics", CFG) == OUTPUT_ROOT / "diagnostics"


# ---------------------------------------------------------------------------
# Default config fallback
# ---------------------------------------------------------------------------


class TestDefaultFallback:
    """When no config is passed, helpers fall back to load_config() defaults."""

    def test_default_raw_root_is_under_data(self, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.chdir(tmp_path)
        result = raw_root()
        assert result == tmp_path / "data" / "raw"

    def test_default_curated_root_is_under_data(self, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.chdir(tmp_path)
        result = curated_root()
        assert result == tmp_path / "data" / "curated"

    def test_default_output_root(self, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.chdir(tmp_path)
        result = output_root()
        assert result == tmp_path / "outputs"
