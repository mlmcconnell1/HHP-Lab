"""Tests for coclab list curated CLI command."""

import json
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from coclab.cli.main import app

runner = CliRunner()


@pytest.fixture()
def curated_tree(tmp_path: Path) -> Path:
    """Create a minimal curated directory with sample parquet files."""
    curated = tmp_path / "data" / "curated"

    pit_dir = curated / "pit"
    pit_dir.mkdir(parents=True)
    pd.DataFrame({"coc_id": ["XX-500"], "pit_year": [2024], "pit_total": [10]}).to_parquet(
        pit_dir / "pit_vintage__P2024.parquet"
    )

    acs_dir = curated / "acs"
    acs_dir.mkdir(parents=True)
    pd.DataFrame({"tract_geoid": ["01001000100"], "total_population": [500]}).to_parquet(
        acs_dir / "acs5_tracts__A2024xT2020.parquet"
    )

    return curated


class TestListCurated:

    def test_human_output(self, curated_tree: Path):
        result = runner.invoke(app, ["list", "curated", "--dir", str(curated_tree)])
        assert result.exit_code == 0
        assert "pit_vintage__P2024.parquet" in result.output
        assert "acs5_tracts__A2024xT2020.parquet" in result.output
        assert "Total: 2 file(s)" in result.output

    def test_json_output(self, curated_tree: Path):
        result = runner.invoke(app, ["list", "curated", "--dir", str(curated_tree), "--json"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["status"] == "ok"
        assert data["count"] == 2
        filenames = {a["filename"] for a in data["artifacts"]}
        assert "pit_vintage__P2024.parquet" in filenames
        assert "acs5_tracts__A2024xT2020.parquet" in filenames

    def test_json_includes_columns(self, curated_tree: Path):
        result = runner.invoke(app, ["list", "curated", "--dir", str(curated_tree), "--json"])
        data = json.loads(result.output)
        pit = next(a for a in data["artifacts"] if a["subdir"] == "pit")
        assert "pit_year" in pit["columns"]
        assert pit["rows"] == 1

    def test_subdir_filter(self, curated_tree: Path):
        result = runner.invoke(
            app, ["list", "curated", "--dir", str(curated_tree), "--subdir", "pit"]
        )
        assert result.exit_code == 0
        assert "pit_vintage__P2024.parquet" in result.output
        assert "acs5_tracts" not in result.output

    def test_subdir_filter_json(self, curated_tree: Path):
        result = runner.invoke(
            app, ["list", "curated", "--dir", str(curated_tree), "--subdir", "acs", "--json"]
        )
        data = json.loads(result.output)
        assert data["count"] == 1
        assert data["artifacts"][0]["subdir"] == "acs"

    def test_invalid_subdir(self, curated_tree: Path):
        result = runner.invoke(
            app, ["list", "curated", "--dir", str(curated_tree), "--subdir", "bogus"]
        )
        assert result.exit_code == 1
        assert "Unknown subdirectory" in result.output

    def test_empty_directory(self, tmp_path: Path):
        empty = tmp_path / "data" / "curated"
        empty.mkdir(parents=True)
        result = runner.invoke(app, ["list", "curated", "--dir", str(empty)])
        assert result.exit_code == 0
        assert "No curated parquet files found" in result.output

    def test_missing_directory_json(self, tmp_path: Path):
        result = runner.invoke(
            app, ["list", "curated", "--dir", str(tmp_path / "nonexistent"), "--json"]
        )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["count"] == 0
