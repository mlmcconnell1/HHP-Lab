"""Tests for the coclab status command."""

import json
from pathlib import Path

from typer.testing import CliRunner

from coclab.cli.main import app

runner = CliRunner()


def _scaffold_curated(tmp_path: Path) -> Path:
    """Create a minimal curated directory structure with test parquet files."""
    import pandas as pd

    data_dir = tmp_path / "data"
    curated = data_dir / "curated"

    # Boundaries
    bdir = curated / "coc_boundaries"
    bdir.mkdir(parents=True)
    pd.DataFrame({"coc_id": ["CO-500"]}).to_parquet(bdir / "coc__B2024.parquet")
    pd.DataFrame({"coc_id": ["CO-500"]}).to_parquet(bdir / "coc__B2025.parquet")

    # TIGER census
    tdir = curated / "tiger"
    tdir.mkdir(parents=True)
    pd.DataFrame({"geoid": ["08001"]}).to_parquet(tdir / "tracts__T2023.parquet")
    pd.DataFrame({"geoid": ["08001"]}).to_parquet(tdir / "counties__C2023.parquet")

    # Crosswalks
    xdir = curated / "xwalks"
    xdir.mkdir(parents=True)
    pd.DataFrame({"coc_id": ["CO-500"]}).to_parquet(xdir / "xwalk__B2025xT2023.parquet")
    pd.DataFrame({"coc_id": ["CO-500"]}).to_parquet(xdir / "xwalk__B2025xC2023.parquet")

    # PIT
    pdir = curated / "pit"
    pdir.mkdir(parents=True)
    pd.DataFrame({"coc_id": ["CO-500"]}).to_parquet(pdir / "pit__P2024.parquet")

    # ACS
    adir = curated / "acs"
    adir.mkdir(parents=True)
    pd.DataFrame({"geoid": ["08001"]}).to_parquet(adir / "acs5_tracts__A2023xT2023.parquet")

    # Measures
    mdir = curated / "measures"
    mdir.mkdir(parents=True)
    pd.DataFrame({"coc_id": ["CO-500"]}).to_parquet(mdir / "measures__A2023@B2025.parquet")

    # ZORI
    zdir = curated / "zori"
    zdir.mkdir(parents=True)
    pd.DataFrame({"county": ["08001"]}).to_parquet(zdir / "zori__monthly.parquet")

    return data_dir


def _scaffold_build(tmp_path: Path) -> Path:
    """Create a minimal build directory."""
    builds_dir = tmp_path / "builds"
    build = builds_dir / "test_build"
    build.mkdir(parents=True)
    manifest = {
        "schema_version": 1,
        "build": {"name": "test_build", "years": [2024, 2025]},
        "base_assets": [{"year": 2024}, {"year": 2025}],
        "aggregate_runs": [{"dataset": "acs"}],
    }
    (build / "manifest.json").write_text(json.dumps(manifest))
    return builds_dir


class TestStatusHuman:
    """Tests for human-readable status output."""

    def test_status_full_environment(self, tmp_path):
        """Status with all assets present shows healthy report."""
        data_dir = _scaffold_curated(tmp_path)
        builds_dir = _scaffold_build(tmp_path)

        result = runner.invoke(
            app,
            ["status", "--data-dir", str(data_dir), "--builds-dir", str(builds_dir)],
        )

        assert result.exit_code == 0
        assert "CoC Lab Status Report" in result.output
        assert "2 vintage(s)" in result.output
        assert "2024" in result.output
        assert "2025" in result.output
        assert "Tracts:   1 vintage(s)" in result.output
        assert "Counties: 1 vintage(s)" in result.output
        assert "B2025xT2023" in result.output
        assert "B2025xC2023" in result.output
        assert "PIT Counts: 1 year(s)" in result.output
        assert "ACS Tracts: 1 file(s)" in result.output
        assert "Measures:   1 file(s)" in result.output
        assert "ZORI:       1 file(s)" in result.output
        assert "test_build: OK" in result.output
        assert "No issues found" in result.output

    def test_status_empty_environment(self, tmp_path):
        """Status with no assets shows errors and non-zero exit."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        builds_dir = tmp_path / "builds"

        result = runner.invoke(
            app,
            ["status", "--data-dir", str(data_dir), "--builds-dir", str(builds_dir)],
        )

        assert result.exit_code == 1
        assert "0 vintage(s)" in result.output
        assert "[ERROR]" in result.output
        assert "No curated boundary files" in result.output
        assert "No TIGER census geometry" in result.output

    def test_status_partial_environment(self, tmp_path):
        """Status with some assets shows warnings but exits 0."""
        import pandas as pd

        data_dir = tmp_path / "data"
        curated = data_dir / "curated"

        # Only boundaries and census
        bdir = curated / "coc_boundaries"
        bdir.mkdir(parents=True)
        pd.DataFrame({"coc_id": ["CO-500"]}).to_parquet(bdir / "coc__B2025.parquet")

        tdir = curated / "tiger"
        tdir.mkdir(parents=True)
        pd.DataFrame({"geoid": ["08001"]}).to_parquet(tdir / "tracts__T2023.parquet")

        builds_dir = tmp_path / "builds"

        result = runner.invoke(
            app,
            ["status", "--data-dir", str(data_dir), "--builds-dir", str(builds_dir)],
        )

        assert result.exit_code == 0
        assert "1 vintage(s)" in result.output
        assert "[WARN]" in result.output
        assert "No crosswalk files" in result.output


class TestStatusJSON:
    """Tests for JSON status output."""

    def test_status_json_full(self, tmp_path):
        """JSON output with all assets has correct structure."""
        data_dir = _scaffold_curated(tmp_path)
        builds_dir = _scaffold_build(tmp_path)

        result = runner.invoke(
            app,
            ["status", "--json", "--data-dir", str(data_dir), "--builds-dir", str(builds_dir)],
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["status"] == "healthy"
        assert payload["assets"]["boundaries"]["count"] == 2
        assert payload["assets"]["boundaries"]["vintages"] == [2024, 2025]
        assert payload["assets"]["census"]["tracts"] == [2023]
        assert payload["assets"]["census"]["counties"] == [2023]
        assert len(payload["assets"]["crosswalks"]["tract"]) == 1
        assert len(payload["assets"]["crosswalks"]["county"]) == 1
        assert payload["assets"]["pit"]["count"] == 1
        assert payload["assets"]["acs"]["count"] == 1
        assert payload["assets"]["measures"]["count"] == 1
        assert payload["assets"]["zori"]["count"] == 1
        assert len(payload["builds"]) == 1
        assert payload["builds"][0]["name"] == "test_build"
        assert payload["builds"][0]["healthy"] is True
        assert payload["issues"] == []

    def test_status_json_empty(self, tmp_path):
        """JSON output with no assets returns error status."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        builds_dir = tmp_path / "builds"

        result = runner.invoke(
            app,
            ["status", "--json", "--data-dir", str(data_dir), "--builds-dir", str(builds_dir)],
        )

        assert result.exit_code == 1
        payload = json.loads(result.output)
        assert payload["status"] == "degraded"
        assert len(payload["issues"]) >= 2
        assert any(i["severity"] == "error" for i in payload["issues"])

    def test_status_json_stable_schema(self, tmp_path):
        """JSON output has all expected top-level keys."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        builds_dir = tmp_path / "builds"

        result = runner.invoke(
            app,
            ["status", "--json", "--data-dir", str(data_dir), "--builds-dir", str(builds_dir)],
        )

        payload = json.loads(result.output)
        assert set(payload.keys()) == {"status", "assets", "builds", "issues"}
        assert set(payload["assets"].keys()) == {
            "boundaries", "census", "crosswalks", "pit", "measures", "acs", "zori",
        }


class TestStatusBuilds:
    """Tests for build scanning in status."""

    def test_unhealthy_build(self, tmp_path):
        """Build with missing manifest is flagged as unhealthy."""
        data_dir = tmp_path / "data"
        curated = data_dir / "curated"
        curated.mkdir(parents=True)
        builds_dir = tmp_path / "builds"
        bad_build = builds_dir / "broken"
        bad_build.mkdir(parents=True)
        # No manifest.json

        result = runner.invoke(
            app,
            ["status", "--json", "--data-dir", str(data_dir), "--builds-dir", str(builds_dir)],
        )

        payload = json.loads(result.output)
        assert len(payload["builds"]) == 1
        assert payload["builds"][0]["name"] == "broken"
        assert payload["builds"][0]["healthy"] is False


class TestStatusHelp:
    """Test status command appears in help."""

    def test_status_in_main_help(self):
        result = runner.invoke(app, ["--help"])
        assert "status" in result.output

    def test_status_help(self):
        result = runner.invoke(app, ["status", "--help"])
        assert result.exit_code == 0
        assert "--json" in result.output
        assert "--data-dir" in result.output
        assert "--builds-dir" in result.output
