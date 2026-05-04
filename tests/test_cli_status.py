"""Tests for the hhplab status command."""

import json
from pathlib import Path

from typer.testing import CliRunner

from hhplab.cli.main import app

runner = CliRunner()

RECIPE_PREFLIGHT_CMD = "hhplab build recipe-preflight --recipe <file> --json"
RECIPE_EXECUTE_CMD = "hhplab build recipe --recipe <file> --json"


def _scaffold_curated(tmp_path: Path) -> Path:
    """Create a minimal curated directory structure with test parquet files."""
    import pandas as pd

    data_dir = tmp_path / "data"
    curated = data_dir / "curated"

    bdir = curated / "coc_boundaries"
    bdir.mkdir(parents=True)
    pd.DataFrame({"coc_id": ["CO-500"]}).to_parquet(bdir / "coc__B2024.parquet")
    pd.DataFrame({"coc_id": ["CO-500"]}).to_parquet(bdir / "coc__B2025.parquet")

    tdir = curated / "tiger"
    tdir.mkdir(parents=True)
    pd.DataFrame({"geoid": ["08001"]}).to_parquet(tdir / "tracts__T2023.parquet")
    pd.DataFrame({"geoid": ["08001"]}).to_parquet(tdir / "counties__C2023.parquet")

    xdir = curated / "xwalks"
    xdir.mkdir(parents=True)
    pd.DataFrame({"coc_id": ["CO-500"]}).to_parquet(xdir / "xwalk__B2025xT2023.parquet")
    pd.DataFrame({"coc_id": ["CO-500"]}).to_parquet(xdir / "xwalk__B2025xC2023.parquet")
    pd.DataFrame({"coc_id": ["CO-500"], "msa_id": ["35620"]}).to_parquet(
        xdir / "msa_coc_xwalk__B2025xMcensus_msa_2023xC2023.parquet"
    )

    pdir = curated / "pit"
    pdir.mkdir(parents=True)
    pd.DataFrame({"coc_id": ["CO-500"]}).to_parquet(pdir / "pit__P2024.parquet")
    pd.DataFrame({"msa_id": ["35620"]}).to_parquet(
        pdir / "pit__msa__P2024@Mcensus_msa_2023xB2025xC2023.parquet"
    )

    msadir = curated / "msa"
    msadir.mkdir(parents=True)
    pd.DataFrame({"msa_id": ["35620"]}).to_parquet(
        msadir / "msa_definitions__census_msa_2023.parquet"
    )
    pd.DataFrame({"msa_id": ["35620"], "county_fips": ["36061"]}).to_parquet(
        msadir / "msa_county_membership__census_msa_2023.parquet"
    )
    pd.DataFrame({"msa_id": ["35620"]}).to_parquet(
        msadir / "msa_boundaries__census_msa_2023.parquet"
    )

    metrodir = curated / "metro"
    metrodir.mkdir(parents=True)
    pd.DataFrame({"metro_id": ["GF21"]}).to_parquet(
        metrodir / "metro_definitions__glynn_fox_v1.parquet"
    )
    pd.DataFrame({"metro_id": ["GF21"], "coc_id": ["CO-503"]}).to_parquet(
        metrodir / "metro_coc_membership__glynn_fox_v1.parquet"
    )
    pd.DataFrame({"metro_id": ["GF21"], "county_fips": ["08001"]}).to_parquet(
        metrodir / "metro_county_membership__glynn_fox_v1.parquet"
    )
    pd.DataFrame({"metro_id": ["GF21"]}).to_parquet(
        metrodir / "metro_boundaries__glynn_fox_v1xC2023.parquet"
    )

    adir = curated / "acs"
    adir.mkdir(parents=True)
    pd.DataFrame({"geoid": ["08001"]}).to_parquet(adir / "acs5_tracts__A2023xT2023.parquet")

    mdir = curated / "measures"
    mdir.mkdir(parents=True)
    pd.DataFrame({"coc_id": ["CO-500"]}).to_parquet(mdir / "measures__A2023@B2025.parquet")

    zdir = curated / "zori"
    zdir.mkdir(parents=True)
    pd.DataFrame({"county": ["08001"]}).to_parquet(zdir / "zori__monthly.parquet")

    ldir = curated / "laus"
    ldir.mkdir(parents=True)
    pd.DataFrame({"metro_id": ["GF01"]}).to_parquet(
        ldir / "laus_metro__A2022@Dglynnfoxv1.parquet"
    )
    pd.DataFrame({"metro_id": ["GF01"]}).to_parquet(
        ldir / "laus_metro__A2023@Dglynnfoxv1.parquet"
    )

    return data_dir


def _scaffold_recipe_outputs(tmp_path: Path) -> Path:
    """Create a minimal recipe output namespace."""
    output_root = tmp_path / "outputs"
    recipe_dir = output_root / "demo-recipe"
    recipe_dir.mkdir(parents=True)
    (recipe_dir / "panel__Y2020-2021@B2025.parquet").write_bytes(b"PAR1")
    (recipe_dir / "panel__Y2020-2021@B2025.manifest.json").write_text("{}\n")
    (recipe_dir / "panel__Y2020-2021@B2025__diagnostics.json").write_text("{}\n")
    (recipe_dir / "map__Y2020-2021@B2025.html").write_text("<html></html>\n")
    return output_root


class TestStatusHuman:
    """Tests for human-readable status output."""

    def test_status_full_environment(self, tmp_path):
        data_dir = _scaffold_curated(tmp_path)
        output_root = _scaffold_recipe_outputs(tmp_path)

        result = runner.invoke(
            app,
            ["status", "--data-dir", str(data_dir), "--output-root", str(output_root)],
        )

        assert result.exit_code == 0
        assert "HHP-Lab Status Report" in result.output
        assert "2 vintage(s)" in result.output
        assert "B2025xT2023" in result.output
        assert "PIT Counts: 1 year(s)" in result.output
        assert "Metro Artifacts: 1 complete version(s)" in result.output
        assert "MSA Artifacts: 1 complete version(s)" in result.output
        assert "LAUS:       2 file(s)" in result.output
        assert "Recipe Outputs: 1 namespace(s)" in result.output
        assert (
            "demo-recipe: 1 panel(s), 1 manifest(s), 1 diagnostics file(s), 1 map(s)"
            in result.output
        )
        assert "Recipe Workflow:" in result.output
        assert RECIPE_PREFLIGHT_CMD in result.output
        assert RECIPE_EXECUTE_CMD in result.output
        assert "No issues found" in result.output

    def test_status_empty_environment(self, tmp_path):
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        result = runner.invoke(
            app,
            ["status", "--data-dir", str(data_dir), "--output-root", str(tmp_path / "outputs")],
        )

        assert result.exit_code == 1
        assert "0 vintage(s)" in result.output
        assert "[ERROR]" in result.output
        assert "No curated boundary files" in result.output
        assert "No TIGER census geometry" in result.output

    def test_status_partial_environment(self, tmp_path):
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
            ["status", "--data-dir", str(data_dir), "--output-root", str(tmp_path / "outputs")],
        )

        assert result.exit_code == 0
        assert "[WARN]" in result.output
        assert "No crosswalk files" in result.output
        assert "generate xwalks --boundary <YEAR> --tracts <YEAR>" in result.output

    def test_status_no_recipe_outputs_shows_recipe_guidance(self, tmp_path):
        data_dir = _scaffold_curated(tmp_path)

        result = runner.invoke(
            app,
            ["status", "--data-dir", str(data_dir), "--output-root", str(tmp_path / "outputs")],
        )

        assert result.exit_code == 0
        assert "Recipe Outputs: 0 namespace(s)" in result.output
        assert "No recipe outputs found" in result.output
        assert RECIPE_PREFLIGHT_CMD in result.output
        assert RECIPE_EXECUTE_CMD in result.output


class TestStatusJSON:
    """Tests for JSON status output."""

    def test_status_json_full(self, tmp_path):
        data_dir = _scaffold_curated(tmp_path)
        output_root = _scaffold_recipe_outputs(tmp_path)

        result = runner.invoke(
            app,
            ["status", "--json", "--data-dir", str(data_dir), "--output-root", str(output_root)],
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["status"] == "healthy"
        assert payload["assets"]["boundaries"]["count"] == 2
        assert payload["assets"]["crosswalks"]["msa"] == ["B2025xMcensus_msa_2023xC2023"]
        assert payload["assets"]["pit"]["msa_count"] == 1
        assert payload["assets"]["metro"]["complete_versions"] == ["glynn_fox_v1"]
        assert payload["assets"]["msa"]["fully_materialized_versions"] == ["census_msa_2023"]
        assert payload["assets"]["laus"]["years"] == [2022, 2023]
        assert payload["recipe_outputs"]["count"] == 1
        assert payload["recipe_outputs"]["panel_count"] == 1
        assert payload["recipe_outputs"]["manifest_count"] == 1
        assert payload["recipe_outputs"]["diagnostics_count"] == 1
        assert payload["recipe_outputs"]["map_count"] == 1
        assert payload["recipe_outputs"]["recipes"][0]["name"] == "demo-recipe"
        assert payload["guidance"]["recipe_preflight"] == RECIPE_PREFLIGHT_CMD
        assert payload["guidance"]["recipe_execute"] == RECIPE_EXECUTE_CMD
        assert payload["issues"] == []

    def test_status_json_empty(self, tmp_path):
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        result = runner.invoke(
            app,
            [
                "status",
                "--json",
                "--data-dir",
                str(data_dir),
                "--output-root",
                str(tmp_path / "outputs"),
            ],
        )

        assert result.exit_code == 1
        payload = json.loads(result.output)
        assert payload["status"] == "degraded"
        assert any(i["severity"] == "error" for i in payload["issues"])
        assert payload["recipe_outputs"]["count"] == 0

    def test_status_json_stable_schema(self, tmp_path):
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        result = runner.invoke(
            app,
            [
                "status",
                "--json",
                "--data-dir",
                str(data_dir),
                "--output-root",
                str(tmp_path / "outputs"),
            ],
        )

        payload = json.loads(result.output)
        assert set(payload.keys()) == {"status", "assets", "recipe_outputs", "guidance", "issues"}
        assert set(payload["assets"].keys()) == {
            "boundaries", "census", "crosswalks", "pit", "metro",
            "msa", "measures", "acs", "zori", "laus",
        }

    def test_status_json_warns_on_partial_msa_artifacts(self, tmp_path):
        import pandas as pd

        data_dir = tmp_path / "data"
        curated = data_dir / "curated"
        msadir = curated / "msa"
        msadir.mkdir(parents=True)
        pd.DataFrame({"msa_id": ["35620"]}).to_parquet(
            msadir / "msa_definitions__census_msa_2023.parquet"
        )

        result = runner.invoke(
            app,
            [
                "status",
                "--json",
                "--data-dir",
                str(data_dir),
                "--output-root",
                str(tmp_path / "outputs"),
            ],
        )

        payload = json.loads(result.output)
        msa_issues = [issue for issue in payload["issues"] if issue["area"] == "msa"]
        assert msa_issues
        assert any("missing county membership" in issue["message"].lower() for issue in msa_issues)
        assert any("missing boundary polygon" in issue["message"].lower() for issue in msa_issues)

    def test_status_json_warns_on_partial_metro_artifacts(self, tmp_path):
        import pandas as pd

        data_dir = tmp_path / "data"
        curated = data_dir / "curated"
        metrodir = curated / "metro"
        metrodir.mkdir(parents=True)
        pd.DataFrame({"metro_id": ["GF21"]}).to_parquet(
            metrodir / "metro_definitions__glynn_fox_v1.parquet"
        )

        result = runner.invoke(
            app,
            [
                "status",
                "--json",
                "--data-dir",
                str(data_dir),
                "--output-root",
                str(tmp_path / "outputs"),
            ],
        )

        payload = json.loads(result.output)
        metro_issues = [issue for issue in payload["issues"] if issue["area"] == "metro"]
        assert metro_issues
        assert any("missing coc membership" in issue["message"].lower() for issue in metro_issues)
        assert any(
            "missing county membership" in issue["message"].lower()
            for issue in metro_issues
        )
        assert any("missing boundary polygon" in issue["message"].lower() for issue in metro_issues)

    def test_status_json_ignores_empty_output_namespaces(self, tmp_path):
        data_dir = _scaffold_curated(tmp_path)
        output_root = tmp_path / "outputs"
        (output_root / "empty-recipe").mkdir(parents=True)
        _scaffold_recipe_outputs(tmp_path)

        result = runner.invoke(
            app,
            ["status", "--json", "--data-dir", str(data_dir), "--output-root", str(output_root)],
        )

        payload = json.loads(result.output)
        assert payload["recipe_outputs"]["count"] == 1
        assert payload["recipe_outputs"]["recipes"][0]["name"] == "demo-recipe"


class TestPitDeduplication:
    """PIT scan should deduplicate years across base and boundary-scoped files."""

    def test_pit_count_deduplicates_years(self, tmp_path):
        import pandas as pd

        data_dir = tmp_path / "data"
        curated = data_dir / "curated"

        bdir = curated / "coc_boundaries"
        bdir.mkdir(parents=True)
        pd.DataFrame({"coc_id": ["CO-500"]}).to_parquet(bdir / "coc__B2024.parquet")
        tdir = curated / "tiger"
        tdir.mkdir(parents=True)
        pd.DataFrame({"geoid": ["08001"]}).to_parquet(tdir / "tracts__T2023.parquet")

        pdir = curated / "pit"
        pdir.mkdir(parents=True)
        pd.DataFrame({"coc_id": ["CO-500"]}).to_parquet(pdir / "pit__P2024.parquet")
        pd.DataFrame({"coc_id": ["CO-500"]}).to_parquet(pdir / "pit__P2024@B2024.parquet")
        pd.DataFrame({"coc_id": ["CO-500"]}).to_parquet(pdir / "pit__P2024@B2025.parquet")

        result = runner.invoke(
            app,
            [
                "status",
                "--json",
                "--data-dir",
                str(data_dir),
                "--output-root",
                str(tmp_path / "outputs"),
            ],
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["assets"]["pit"]["count"] == 1
        assert payload["assets"]["pit"]["years"] == [2024]


class TestLausScan:
    """Status output must include curated LAUS metro assets."""

    def _scaffold_with_laus(self, tmp_path: Path, filenames: list[str]) -> Path:
        import pandas as pd

        data_dir = tmp_path / "data"
        curated = data_dir / "curated"

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
            [
                "status",
                "--json",
                "--data-dir",
                str(data_dir),
                "--output-root",
                str(tmp_path / "outputs"),
            ],
        )

        payload = json.loads(result.output)
        assert payload["assets"]["laus"]["count"] == 0
        assert payload["assets"]["laus"]["items"] == []
        assert payload["assets"]["laus"]["years"] == []

    def test_laus_lists_each_canonical_file(self, tmp_path):
        data_dir = self._scaffold_with_laus(tmp_path, [
            "laus_metro__A2015@Dglynnfoxv1.parquet",
            "laus_metro__A2018@Dglynnfoxv1.parquet",
            "laus_metro__A2023@Dglynnfoxv1.parquet",
        ])

        result = runner.invoke(
            app,
            [
                "status",
                "--json",
                "--data-dir",
                str(data_dir),
                "--output-root",
                str(tmp_path / "outputs"),
            ],
        )

        payload = json.loads(result.output)
        assert payload["assets"]["laus"]["count"] == 3
        assert payload["assets"]["laus"]["years"] == [2015, 2018, 2023]

    def test_laus_items_sorted_by_year_then_definition(self, tmp_path):
        data_dir = self._scaffold_with_laus(tmp_path, [
            "laus_metro__A2023@Dglynnfoxv1.parquet",
            "laus_metro__A2015@Dglynnfoxv1.parquet",
            "laus_metro__A2020@Dotherdefv1.parquet",
        ])

        result = runner.invoke(
            app,
            [
                "status",
                "--json",
                "--data-dir",
                str(data_dir),
                "--output-root",
                str(tmp_path / "outputs"),
            ],
        )

        payload = json.loads(result.output)
        years_in_order = [item["year"] for item in payload["assets"]["laus"]["items"]]
        assert years_in_order == sorted(years_in_order)
        assert payload["assets"]["laus"]["years"] == [2015, 2020, 2023]

    def test_laus_ignores_unrelated_files(self, tmp_path):
        import pandas as pd

        data_dir = self._scaffold_with_laus(tmp_path, [
            "laus_metro__A2023@Dglynnfoxv1.parquet",
        ])
        ldir = data_dir / "curated" / "laus"
        pd.DataFrame({"x": [1]}).to_parquet(ldir / "scratch_pad.parquet")

        result = runner.invoke(
            app,
            [
                "status",
                "--json",
                "--data-dir",
                str(data_dir),
                "--output-root",
                str(tmp_path / "outputs"),
            ],
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
            ["status", "--data-dir", str(data_dir), "--output-root", str(tmp_path / "outputs")],
        )

        assert result.exit_code == 0
        assert "LAUS:" in result.output
        assert "2 file(s)" in result.output
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
        assert "--output-root" in result.output
        assert "--builds-dir" not in result.output
