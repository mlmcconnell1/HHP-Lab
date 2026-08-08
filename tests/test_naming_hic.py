"""Tests for HIC naming helpers."""

from pathlib import Path

from hhplab.artifacts.naming.naming import hic_filename, hic_path


def test_hic_filename_uses_inventory_year_token() -> None:
    assert hic_filename(2024) == "hic__H2024.parquet"


def test_hic_path_uses_curated_hic_subdir(tmp_path: Path) -> None:
    assert hic_path(2024, base_dir=tmp_path) == (
        tmp_path / "curated" / "hic" / "hic__H2024.parquet"
    )
