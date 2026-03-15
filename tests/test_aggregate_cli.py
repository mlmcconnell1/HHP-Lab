"""Tests for the ``coclab aggregate`` CLI command group."""

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from coclab.cli.aggregate import _build_lagged_pep_series
from coclab.cli.main import app

runner = CliRunner()


# ---------------------------------------------------------------------------
# Help output
# ---------------------------------------------------------------------------


def test_aggregate_help_shows_subcommands():
    result = runner.invoke(app, ["aggregate", "--help"])
    assert result.exit_code == 0
    assert "build-scoped analysis inputs" in result.output
    for name in ("acs", "zori", "pep", "pit"):
        assert name in result.output


# ---------------------------------------------------------------------------
# Build validation
# ---------------------------------------------------------------------------


def test_aggregate_pep_missing_build():
    with runner.isolated_filesystem():
        result = runner.invoke(app, ["aggregate", "pep", "--build", "nonexistent"])
        assert result.exit_code == 2
        assert "Build 'nonexistent' not found" in result.output


def test_aggregate_pit_missing_build():
    with runner.isolated_filesystem():
        result = runner.invoke(app, ["aggregate", "pit", "--build", "nonexistent"])
        assert result.exit_code == 2
        assert "Build 'nonexistent' not found" in result.output


def test_aggregate_acs_missing_build():
    with runner.isolated_filesystem():
        result = runner.invoke(app, ["aggregate", "acs", "--build", "nonexistent"])
        assert result.exit_code == 2
        assert "Build 'nonexistent' not found" in result.output


def test_aggregate_zori_missing_build():
    with runner.isolated_filesystem():
        result = runner.invoke(app, ["aggregate", "zori", "--build", "nonexistent"])
        assert result.exit_code == 2
        assert "Build 'nonexistent' not found" in result.output


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _create_build(name: str = "demo", *, years: list[int] | None = None) -> None:
    """Create a build directory with a proper manifest for testing."""
    build_dir = Path("builds") / name
    (build_dir / "data" / "curated").mkdir(parents=True)
    (build_dir / "data" / "raw").mkdir(parents=True)
    (build_dir / "base").mkdir(parents=True)

    if years is not None:
        assets = [
            {
                "asset_type": "coc_boundary",
                "year": y,
                "source": "test",
                "relative_path": f"base/coc__B{y}.parquet",
                "sha256": "a" * 64,
            }
            for y in years
        ]
        manifest = {
            "schema_version": 1,
            "build": {
                "name": name,
                "created_at": "2026-01-01T00:00:00Z",
                "years": years,
            },
            "base_assets": assets,
            "aggregate_runs": [],
        }
    else:
        manifest = {"schema_version": 1}

    (build_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


# ---------------------------------------------------------------------------
# Alignment validation
# ---------------------------------------------------------------------------


def test_aggregate_pep_invalid_align():
    with runner.isolated_filesystem():
        _create_build(years=[2020, 2021])
        result = runner.invoke(
            app, ["aggregate", "pep", "--build", "demo", "--align", "bad_mode"]
        )
        assert result.exit_code == 2
        assert "Invalid alignment mode 'bad_mode' for pep" in result.output


def test_aggregate_pit_invalid_align():
    with runner.isolated_filesystem():
        _create_build(years=[2020, 2021])
        result = runner.invoke(
            app, ["aggregate", "pit", "--build", "demo", "--align", "bad_mode"]
        )
        assert result.exit_code == 2
        assert "Invalid alignment mode 'bad_mode' for pit" in result.output


def test_aggregate_acs_invalid_align():
    with runner.isolated_filesystem():
        _create_build(years=[2020, 2021])
        result = runner.invoke(
            app, ["aggregate", "acs", "--build", "demo", "--align", "bad_mode"]
        )
        assert result.exit_code == 2
        assert "Invalid alignment mode 'bad_mode' for acs" in result.output


def test_aggregate_zori_invalid_align():
    with runner.isolated_filesystem():
        _create_build(years=[2020, 2021])
        result = runner.invoke(
            app, ["aggregate", "zori", "--build", "demo", "--align", "bad_mode"]
        )
        assert result.exit_code == 2
        assert "Invalid alignment mode 'bad_mode' for zori" in result.output


# ---------------------------------------------------------------------------
# Missing manifest data (no years / no base assets)
# ---------------------------------------------------------------------------


def test_aggregate_pep_no_manifest_years():
    """Commands should report an error when manifest has no years."""
    with runner.isolated_filesystem():
        _create_build()  # no years
        result = runner.invoke(app, ["aggregate", "pep", "--build", "demo"])
        assert result.exit_code == 2
        output_lower = result.output.lower()
        assert (
            "no years" in output_lower
            or "no pinned base assets" in output_lower
            or "error" in output_lower
        )


def test_aggregate_acs_no_base_assets():
    """ACS aggregate should fail if manifest has years but no base assets."""
    with runner.isolated_filesystem():
        build_dir = Path("builds") / "demo"
        (build_dir / "data" / "curated").mkdir(parents=True)
        (build_dir / "data" / "raw").mkdir(parents=True)
        (build_dir / "base").mkdir(parents=True)
        manifest = {
            "schema_version": 1,
            "build": {"name": "demo", "created_at": "2026-01-01T00:00:00Z", "years": [2020]},
            "base_assets": [],
            "aggregate_runs": [],
        }
        (build_dir / "manifest.json").write_text(json.dumps(manifest) + "\n")

        result = runner.invoke(app, ["aggregate", "acs", "--build", "demo"])
        assert result.exit_code == 2
        assert "No coc_boundary base assets" in result.output


# ---------------------------------------------------------------------------
# --years parsing
# ---------------------------------------------------------------------------


def test_aggregate_pep_with_invalid_years():
    with runner.isolated_filesystem():
        _create_build(years=[2020])
        result = runner.invoke(
            app, ["aggregate", "pep", "--build", "demo", "--years", "bad"]
        )
        assert result.exit_code == 2


# ---------------------------------------------------------------------------
# Dataset-specific options
# ---------------------------------------------------------------------------


def test_aggregate_pep_lagged_rejects_lag_months_out_of_range():
    with runner.isolated_filesystem():
        _create_build(years=[2020])
        result = runner.invoke(
            app,
            [
                "aggregate",
                "pep",
                "--build",
                "demo",
                "--align",
                "lagged",
                "--lag-months",
                "13",
            ],
        )
        assert result.exit_code == 2
        assert "--lag-months must be between 0 and 12" in result.output


def test_aggregate_pep_rejects_lag_months_without_lagged_align():
    with runner.isolated_filesystem():
        _create_build(years=[2020])
        result = runner.invoke(
            app,
            [
                "aggregate",
                "pep",
                "--build",
                "demo",
                "--lag-months",
                "1",
            ],
        )
        assert result.exit_code == 2
        assert "--lag-months is only valid when --align=lagged" in result.output


def test_build_lagged_pep_series_zero_months_matches_current_year():
    import pandas as pd

    pep_df = pd.DataFrame({
        "county_fips": ["01001", "01003", "01001", "01003"],
        "year": [2019, 2019, 2020, 2020],
        "population": [90000, 120000, 100000, 130000],
    })

    result = _build_lagged_pep_series(pep_df, target_year=2020, lag_months=0)
    result = result.sort_values("county_fips").reset_index(drop=True)
    assert list(result["population"]) == [100000, 130000]


def test_build_lagged_pep_series_twelve_months_matches_previous_year():
    import pandas as pd

    pep_df = pd.DataFrame({
        "county_fips": ["01001", "01003", "01001", "01003"],
        "year": [2019, 2019, 2020, 2020],
        "population": [90000, 120000, 100000, 130000],
    })

    result = _build_lagged_pep_series(pep_df, target_year=2020, lag_months=12)
    result = result.sort_values("county_fips").reset_index(drop=True)
    assert list(result["population"]) == [90000, 120000]


def test_build_lagged_pep_series_interpolates_for_partial_month_lag():
    import pandas as pd

    pep_df = pd.DataFrame({
        "county_fips": ["01001", "01003", "01001", "01003"],
        "year": [2019, 2019, 2020, 2020],
        "population": [90000, 120000, 100000, 130000],
    })

    result = _build_lagged_pep_series(pep_df, target_year=2020, lag_months=6)
    result = result.sort_values("county_fips").reset_index(drop=True)
    assert list(result["population"]) == [95000, 125000]


def test_build_lagged_pep_series_rejects_invalid_lag_months():
    import pandas as pd

    pep_df = pd.DataFrame({
        "county_fips": ["01001"],
        "year": [2020],
        "population": [100000],
    })

    with pytest.raises(ValueError, match="--lag-months must be between 0 and 12"):
        _build_lagged_pep_series(pep_df, target_year=2020, lag_months=-1)


def _create_fake_acs_cache(acs_vintage: str, tract_vintage: str | int) -> None:
    """Create a minimal fake ACS cache file so aggregate reaches crosswalk check."""
    import pandas as pd

    from coclab.acs.ingest.tract_population import get_output_path

    cache_path = get_output_path(acs_vintage, str(tract_vintage))
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({
        "tract_geoid": ["01001020100"],
        "total_population": [100],
        "adult_population": [80],
        "median_household_income": [50000.0],
        "median_gross_rent": [1200.0],
        "poverty_universe": [95],
        "below_50pct_poverty": [5],
        "50_to_99pct_poverty": [10],
        "population_below_poverty": [15],
    }).to_parquet(cache_path)


def test_aggregate_acs_missing_crosswalk_suggests_decennial():
    with runner.isolated_filesystem():
        _create_build(years=[2015])
        _create_fake_acs_cache("2011-2015", 2010)
        result = runner.invoke(app, ["aggregate", "acs", "--build", "demo"])
        assert result.exit_code == 1
        assert "Crosswalk not found" in result.output
        assert "Did you mean to request" not in result.output
        assert "Run: coclab generate xwalks --boundary 2015 --tracts 2010" in result.output


def test_aggregate_acs_missing_crosswalk_no_decennial_hint():
    with runner.isolated_filesystem():
        _create_build(years=[2020])
        _create_fake_acs_cache("2016-2020", 2020)
        result = runner.invoke(app, ["aggregate", "acs", "--build", "demo"])
        assert result.exit_code == 1
        assert "Crosswalk not found" in result.output
        assert "Did you mean to request" not in result.output
        assert "Run: coclab generate xwalks --boundary 2020 --tracts 2020" in result.output


# ---------------------------------------------------------------------------
# PIT aggregate with real data
# ---------------------------------------------------------------------------


def _create_build_at(tmp_path, name="test_build", years=None):
    """Create a build directory with manifest at a given tmp_path root."""
    if years is None:
        years = [2020, 2021]
    build_dir = tmp_path / "builds" / name
    (build_dir / "data" / "curated").mkdir(parents=True)
    (build_dir / "data" / "raw").mkdir(parents=True)
    (build_dir / "base").mkdir(parents=True)
    manifest = {
        "schema_version": 1,
        "build": {
            "name": name,
            "created_at": "2026-01-01T00:00:00Z",
            "years": years,
        },
        "base_assets": [
            {
                "asset_type": "coc_boundary",
                "year": y,
                "source": "test",
                "relative_path": f"base/coc__B{y}.parquet",
                "sha256": "a" * 64,
            }
            for y in years
        ],
        "aggregate_runs": [],
    }
    (build_dir / "manifest.json").write_text(json.dumps(manifest) + "\n")
    return build_dir


def test_aggregate_pit_collects_data(tmp_path):
    """PIT aggregate should collect and write per-year PIT files for build years."""
    import os

    import pandas as pd

    _create_build_at(tmp_path, years=[2020, 2021])

    # Create stub PIT individual year files
    pit_dir = tmp_path / "data" / "curated" / "pit"
    pit_dir.mkdir(parents=True)
    for year in [2020, 2021]:
        df = pd.DataFrame({
            "coc_id": [f"XX-{i:03d}" for i in range(3)],
            "pit_year": [year] * 3,
            "total_homeless": [100, 200, 300],
        })
        df.to_parquet(pit_dir / f"pit__P{year}.parquet", index=False)

    old_cwd = os.getcwd()
    try:
        os.chdir(tmp_path)
        result = runner.invoke(
            app,
            ["aggregate", "pit", "--build", "test_build"],
            catch_exceptions=False,
        )
    finally:
        os.chdir(old_cwd)

    assert result.exit_code == 0
    assert "Wrote PIT aggregate" in result.output

    # Verify per-year output files
    out_dir = tmp_path / "builds" / "test_build" / "data" / "curated" / "pit"
    for year in [2020, 2021]:
        assert (out_dir / f"pit__P{year}@B{year}.parquet").exists()


def test_aggregate_pit_falls_back_to_vintage(tmp_path):
    """PIT aggregate should discover vintage files when individual years are missing."""
    import os

    import pandas as pd

    _create_build_at(tmp_path, years=[2019, 2020, 2021])

    # Create NO individual year files — only a vintage file
    pit_dir = tmp_path / "data" / "curated" / "pit"
    pit_dir.mkdir(parents=True)

    rows = []
    for year in range(2015, 2025):
        for i in range(3):
            rows.append({
                "coc_id": f"XX-{i:03d}",
                "pit_year": year,
                "total_homeless": 100 * (i + 1),
            })
    vintage_df = pd.DataFrame(rows)
    vintage_df.to_parquet(pit_dir / "pit_vintage__P2024.parquet", index=False)

    old_cwd = os.getcwd()
    try:
        os.chdir(tmp_path)
        result = runner.invoke(
            app,
            ["aggregate", "pit", "--build", "test_build"],
            catch_exceptions=False,
        )
    finally:
        os.chdir(old_cwd)

    assert result.exit_code == 0
    assert "Using vintage P2024" in result.output
    assert "Wrote PIT aggregate" in result.output

    # Verify per-year output files with correct data
    out_dir = tmp_path / "builds" / "test_build" / "data" / "curated" / "pit"
    for year in [2019, 2020, 2021]:
        out_file = out_dir / f"pit__P{year}@B{year}.parquet"
        assert out_file.exists()
        result_df = pd.read_parquet(out_file)
        assert sorted(result_df["pit_year"].unique()) == [year]


def test_aggregate_pit_vintage_partial_coverage(tmp_path):
    """Vintage file covers some years; missing years still reported."""
    import os

    import pandas as pd

    _create_build_at(tmp_path, years=[2020, 2021, 2025])

    pit_dir = tmp_path / "data" / "curated" / "pit"
    pit_dir.mkdir(parents=True)

    # Vintage only has 2020 and 2021, not 2025
    rows = []
    for year in [2020, 2021]:
        for i in range(3):
            rows.append({
                "coc_id": f"XX-{i:03d}",
                "pit_year": year,
                "total_homeless": 100 * (i + 1),
            })
    vintage_df = pd.DataFrame(rows)
    vintage_df.to_parquet(pit_dir / "pit_vintage__P2021.parquet", index=False)

    old_cwd = os.getcwd()
    try:
        os.chdir(tmp_path)
        result = runner.invoke(
            app,
            ["aggregate", "pit", "--build", "test_build"],
            catch_exceptions=False,
        )
    finally:
        os.chdir(old_cwd)

    assert result.exit_code == 0
    assert "Using vintage P2021" in result.output
    assert "PIT data missing for years: [2025]" in result.output
