"""Tests for the hhplab status command."""

import json
from pathlib import Path

from typer.testing import CliRunner

from hhplab.cli.main import app

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
    pd.DataFrame({"coc_id": ["CO-500"], "msa_id": ["35620"]}).to_parquet(
        xdir / "msa_coc_xwalk__B2025xMcensus_msa_2023xC2023.parquet"
    )

    # PIT
    pdir = curated / "pit"
    pdir.mkdir(parents=True)
    pd.DataFrame({"coc_id": ["CO-500"]}).to_parquet(pdir / "pit__P2024.parquet")
    pd.DataFrame({"msa_id": ["35620"]}).to_parquet(
        pdir / "pit__msa__P2024@Mcensus_msa_2023xB2025xC2023.parquet"
    )

    # MSA
    msadir = curated / "msa"
    msadir.mkdir(parents=True)
    pd.DataFrame({"msa_id": ["35620"]}).to_parquet(
        msadir / "msa_definitions__census_msa_2023.parquet"
    )
    pd.DataFrame({"msa_id": ["35620"], "county_fips": ["36061"]}).to_parquet(
        msadir / "msa_county_membership__census_msa_2023.parquet"
    )

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

    # LAUS (canonical naming: laus_metro__A<year>@D<defn>.parquet)
    ldir = curated / "laus"
    ldir.mkdir(parents=True)
    pd.DataFrame({"metro_id": ["GF01"]}).to_parquet(
        ldir / "laus_metro__A2022@Dglynnfoxv1.parquet"
    )
    pd.DataFrame({"metro_id": ["GF01"]}).to_parquet(
        ldir / "laus_metro__A2023@Dglynnfoxv1.parquet"
    )

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
        assert "HHP-Lab Status Report" in result.output
        assert "2 vintage(s)" in result.output
        assert "2024" in result.output
        assert "2025" in result.output
        assert "Tracts:   1 vintage(s)" in result.output
        assert "Counties: 1 vintage(s)" in result.output
        assert "B2025xT2023" in result.output
        assert "B2025xC2023" in result.output
        assert "B2025xMcensus_msa_2023xC2023" in result.output
        assert "PIT Counts: 1 year(s)" in result.output
        assert "MSA PIT:    1 file(s)" in result.output
        assert "MSA Artifacts: 1 complete version(s)" in result.output
        assert "ACS Tracts: 1 file(s)" in result.output
        assert "Measures:   1 file(s)" in result.output
        assert "ZORI:       1 file(s)" in result.output
        assert "LAUS:       2 file(s)" in result.output
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
        assert payload["assets"]["crosswalks"]["msa"] == ["B2025xMcensus_msa_2023xC2023"]
        assert payload["assets"]["pit"]["count"] == 1
        assert payload["assets"]["pit"]["msa_count"] == 1
        assert payload["assets"]["pit"]["msa_items"] == [{
            "year": 2024,
            "definition_version": "census_msa_2023",
            "boundary_vintage": 2025,
            "county_vintage": 2023,
        }]
        assert payload["assets"]["msa"]["definitions"] == ["census_msa_2023"]
        assert payload["assets"]["msa"]["county_memberships"] == ["census_msa_2023"]
        assert payload["assets"]["msa"]["complete_versions"] == ["census_msa_2023"]
        assert payload["assets"]["acs"]["count"] == 1
        assert payload["assets"]["measures"]["count"] == 1
        assert payload["assets"]["zori"]["count"] == 1
        assert payload["assets"]["laus"]["count"] == 2
        assert payload["assets"]["laus"]["years"] == [2022, 2023]
        assert payload["assets"]["laus"]["items"] == [
            {"year": 2022, "definition_version": "glynnfoxv1"},
            {"year": 2023, "definition_version": "glynnfoxv1"},
        ]
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
            "boundaries", "census", "crosswalks", "pit", "msa", "measures", "acs", "zori", "laus",
        }

    def test_status_json_warns_on_partial_msa_artifacts(self, tmp_path):
        data_dir = tmp_path / "data"
        curated = data_dir / "curated"
        msadir = curated / "msa"
        msadir.mkdir(parents=True)

        import pandas as pd

        pd.DataFrame({"msa_id": ["35620"]}).to_parquet(
            msadir / "msa_definitions__census_msa_2023.parquet"
        )

        result = runner.invoke(
            app,
            ["status", "--json", "--data-dir", str(data_dir), "--builds-dir", str(tmp_path / "builds")],
        )

        payload = json.loads(result.output)
        msa_issues = [issue for issue in payload["issues"] if issue["area"] == "msa"]
        assert msa_issues
        assert "missing county membership" in msa_issues[0]["message"].lower()
        assert (
            "Run: hhplab generate msa --definition-version census_msa_2023 --force"
            == msa_issues[0]["hint"]
        )


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


class TestPitDeduplication:
    """coclab-rhix: PIT scan should deduplicate years across base and boundary-scoped files."""

    def test_pit_count_deduplicates_years(self, tmp_path):
        """When both pit__P2024.parquet and pit__P2024@B2024.parquet exist, count=1."""
        import pandas as pd

        data_dir = tmp_path / "data"
        curated = data_dir / "curated"

        # Boundaries (needed to avoid error exit)
        bdir = curated / "coc_boundaries"
        bdir.mkdir(parents=True)
        pd.DataFrame({"coc_id": ["CO-500"]}).to_parquet(bdir / "coc__B2024.parquet")
        tdir = curated / "tiger"
        tdir.mkdir(parents=True)
        pd.DataFrame({"geoid": ["08001"]}).to_parquet(tdir / "tracts__T2023.parquet")

        # PIT: both base and boundary-scoped for same year
        pdir = curated / "pit"
        pdir.mkdir(parents=True)
        pd.DataFrame({"coc_id": ["CO-500"]}).to_parquet(pdir / "pit__P2024.parquet")
        pd.DataFrame({"coc_id": ["CO-500"]}).to_parquet(pdir / "pit__P2024@B2024.parquet")
        pd.DataFrame({"coc_id": ["CO-500"]}).to_parquet(pdir / "pit__P2024@B2025.parquet")

        result = runner.invoke(
            app,
            [
                "status", "--json",
                "--data-dir", str(data_dir),
                "--builds-dir", str(tmp_path / "builds"),
            ],
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["assets"]["pit"]["count"] == 1
        assert payload["assets"]["pit"]["years"] == [2024]


class TestLausScan:
    """coclab-ii45: status output must include curated LAUS metro assets."""

    def _scaffold_with_laus(self, tmp_path: Path, filenames: list[str]) -> Path:
        import pandas as pd

        data_dir = tmp_path / "data"
        curated = data_dir / "curated"

        # Boundaries + census so we don't trip the error gate
        bdir = curated / "coc_boundaries"
        bdir.mkdir(parents=True)
        pd.DataFrame({"coc_id": ["CO-500"]}).to_parquet(bdir / "coc__B2025.parquet")
        tdir = curated / "tiger"
        tdir.mkdir(parents=True)
        pd.DataFrame({"geoid": ["08001"]}).to_parquet(tdir / "tracts__T2023.parquet")

        ldir = curated / "laus"
        ldir.mkdir(parents=True)
        for fname in filenames:
            pd.DataFrame({"metro_id": ["GF01"]}).to_parquet(ldir / fname)
        return data_dir

    def test_laus_count_zero_when_directory_missing(self, tmp_path):
        """Status must not crash when data/curated/laus does not exist."""
        import pandas as pd

        data_dir = tmp_path / "data"
        curated = data_dir / "curated"
        bdir = curated / "coc_boundaries"
        bdir.mkdir(parents=True)
        pd.DataFrame({"coc_id": ["CO-500"]}).to_parquet(bdir / "coc__B2025.parquet")
        tdir = curated / "tiger"
        tdir.mkdir(parents=True)
        pd.DataFrame({"geoid": ["08001"]}).to_parquet(tdir / "tracts__T2023.parquet")

        result = runner.invoke(
            app,
            ["status", "--json", "--data-dir", str(data_dir),
             "--builds-dir", str(tmp_path / "builds")],
        )

        payload = json.loads(result.output)
        assert payload["assets"]["laus"]["count"] == 0
        assert payload["assets"]["laus"]["items"] == []
        assert payload["assets"]["laus"]["years"] == []

    def test_laus_lists_each_canonical_file(self, tmp_path):
        """Multiple curated LAUS years must each appear in items."""
        data_dir = self._scaffold_with_laus(tmp_path, [
            "laus_metro__A2015@Dglynnfoxv1.parquet",
            "laus_metro__A2018@Dglynnfoxv1.parquet",
            "laus_metro__A2023@Dglynnfoxv1.parquet",
        ])

        result = runner.invoke(
            app,
            ["status", "--json", "--data-dir", str(data_dir),
             "--builds-dir", str(tmp_path / "builds")],
        )

        payload = json.loads(result.output)
        assert payload["assets"]["laus"]["count"] == 3
        assert payload["assets"]["laus"]["years"] == [2015, 2018, 2023]
        defns = {item["definition_version"] for item in payload["assets"]["laus"]["items"]}
        assert defns == {"glynnfoxv1"}

    def test_laus_items_sorted_by_year_then_definition(self, tmp_path):
        """Items must be deterministically sorted so the JSON is reproducible."""
        data_dir = self._scaffold_with_laus(tmp_path, [
            "laus_metro__A2023@Dglynnfoxv1.parquet",
            "laus_metro__A2015@Dglynnfoxv1.parquet",
            "laus_metro__A2020@Dotherdefv1.parquet",
        ])

        result = runner.invoke(
            app,
            ["status", "--json", "--data-dir", str(data_dir),
             "--builds-dir", str(tmp_path / "builds")],
        )

        payload = json.loads(result.output)
        years_in_order = [item["year"] for item in payload["assets"]["laus"]["items"]]
        assert years_in_order == sorted(years_in_order)
        assert payload["assets"]["laus"]["years"] == [2015, 2020, 2023]

    def test_laus_ignores_unrelated_files(self, tmp_path):
        """Files in data/curated/laus that don't match the canonical pattern
        must be ignored, not crash or pollute the output."""
        import pandas as pd

        data_dir = self._scaffold_with_laus(tmp_path, [
            "laus_metro__A2023@Dglynnfoxv1.parquet",
        ])
        ldir = data_dir / "curated" / "laus"
        # noise: random parquet that doesn't match the canonical pattern
        pd.DataFrame({"x": [1]}).to_parquet(ldir / "scratch_pad.parquet")

        result = runner.invoke(
            app,
            ["status", "--json", "--data-dir", str(data_dir),
             "--builds-dir", str(tmp_path / "builds")],
        )

        payload = json.loads(result.output)
        assert payload["assets"]["laus"]["count"] == 1
        assert payload["assets"]["laus"]["years"] == [2023]

    def test_laus_appears_in_human_output(self, tmp_path):
        data_dir = self._scaffold_with_laus(tmp_path, [
            "laus_metro__A2022@Dglynnfoxv1.parquet",
            "laus_metro__A2023@Dglynnfoxv1.parquet",
        ])

        result = runner.invoke(
            app,
            ["status", "--data-dir", str(data_dir), "--builds-dir", str(tmp_path / "builds")],
        )

        assert result.exit_code == 0
        assert "LAUS:" in result.output
        assert "2 file(s)" in result.output
        # Year listing should mention each curated year
        assert "2022" in result.output
        assert "2023" in result.output


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
