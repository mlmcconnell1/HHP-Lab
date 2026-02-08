"""Tests for named build directory CLI support."""

import json
from pathlib import Path

from typer.testing import CliRunner

from coclab.cli.main import app

runner = CliRunner()


def _create_boundary_files(base: Path, years: list[int]) -> None:
    """Create stub boundary files so build create can pin them."""
    boundaries_dir = base / "data" / "curated" / "coc_boundaries"
    boundaries_dir.mkdir(parents=True, exist_ok=True)
    for year in years:
        (boundaries_dir / f"coc__B{year}.parquet").write_bytes(
            b"PAR1stub" + year.to_bytes(2, "big")
        )


def test_build_create_and_list():
    with runner.isolated_filesystem():
        years = list(range(2018, 2025))
        _create_boundary_files(Path("."), years)

        result = runner.invoke(
            app,
            [
                "build", "create",
                "--name", "demo",
                "--years", "2018-2024",
                "--data-dir", "data",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "Created build: demo" in result.output
        assert "Base assets pinned: 7" in result.output

        build_root = Path("builds") / "demo"
        assert (build_root / "data" / "curated").exists()
        assert (build_root / "data" / "raw").exists()
        assert (build_root / "base").exists()
        assert (build_root / "manifest.json").exists()

        # Verify manifest content
        manifest = json.loads((build_root / "manifest.json").read_text())
        assert manifest["schema_version"] == 1
        assert manifest["build"]["name"] == "demo"
        assert manifest["build"]["years"] == years
        assert len(manifest["base_assets"]) == 7
        assert manifest["aggregate_runs"] == []

        # Verify base asset files are pinned
        for year in years:
            pinned = build_root / "base" / f"coc__B{year}.parquet"
            assert pinned.exists()

        # Each asset should have sha256
        for asset in manifest["base_assets"]:
            assert asset["asset_type"] == "coc_boundary"
            assert len(asset["sha256"]) == 64

        list_result = runner.invoke(app, ["build", "list"])
        assert list_result.exit_code == 0
        assert "demo" in list_result.output


def test_build_create_missing_boundary_errors():
    """build create should fail if boundary files are missing for requested years."""
    with runner.isolated_filesystem():
        # Create boundaries for 2018-2020 only
        _create_boundary_files(Path("."), [2018, 2019, 2020])

        result = runner.invoke(
            app,
            [
                "build", "create",
                "--name", "demo",
                "--years", "2018-2024",
                "--data-dir", "data",
            ],
        )
        assert result.exit_code == 1
        assert "not found" in result.output.lower() or "Error" in result.output


def test_build_list_empty():
    with runner.isolated_filesystem():
        result = runner.invoke(app, ["build", "list"])
        assert result.exit_code == 0
        assert "No builds found" in result.output


def test_export_with_missing_build_errors():
    with runner.isolated_filesystem():
        result = runner.invoke(app, ["build", "export", "--name", "demo", "--build", "missing"])
        assert result.exit_code == 2
        assert "Build 'missing' not found" in result.output
        assert "coclab build create" in result.output


def test_build_xwalks_with_missing_build_errors():
    with runner.isolated_filesystem():
        result = runner.invoke(
            app,
            [
                "build",
                "xwalks",
                "--build",
                "missing",
                "--boundary",
                "2025",
                "--tracts",
                "2023",
            ],
        )
        assert result.exit_code == 2
        assert "Build 'missing' not found" in result.output
        assert "coclab build create" in result.output


def test_build_panel_with_missing_build_errors():
    with runner.isolated_filesystem():
        result = runner.invoke(
            app,
            [
                "build",
                "panel",
                "--build",
                "missing",
                "--start",
                "2020",
                "--end",
                "2020",
            ],
        )
        assert result.exit_code == 2
        assert "Build 'missing' not found" in result.output
        assert "coclab build create" in result.output
