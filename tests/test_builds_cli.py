"""Tests for named build directory CLI support."""

from pathlib import Path

from typer.testing import CliRunner

from coclab.cli.main import app

runner = CliRunner()


def test_build_create_and_list():
    with runner.isolated_filesystem():
        result = runner.invoke(app, ["build", "create", "--name", "demo"])
        assert result.exit_code == 0
        assert "Created build: demo" in result.output

        build_root = Path("builds") / "demo"
        assert (build_root / "data" / "curated").exists()
        assert (build_root / "data" / "raw").exists()
        assert (build_root / "hub").exists()

        list_result = runner.invoke(app, ["build", "list"])
        assert list_result.exit_code == 0
        assert "demo" in list_result.output


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


def test_build_measures_with_missing_build_errors():
    with runner.isolated_filesystem():
        result = runner.invoke(
            app,
            [
                "build",
                "measures",
                "--build",
                "missing",
                "--boundary",
                "2025",
                "--acs",
                "2019-2023",
            ],
        )
        assert result.exit_code == 2
        assert "Build 'missing' not found" in result.output
        assert "coclab build create" in result.output


def test_build_pep_with_missing_build_errors():
    with runner.isolated_filesystem():
        pep_path = Path("pep.parquet")
        pep_path.touch()
        result = runner.invoke(
            app,
            [
                "build",
                "pep",
                "--build",
                "missing",
                "--boundary",
                "2024",
                "--counties",
                "2024",
                "--pep-path",
                str(pep_path),
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
