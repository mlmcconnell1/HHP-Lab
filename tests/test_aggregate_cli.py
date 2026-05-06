"""Tests for the ``hhplab aggregate`` CLI command group."""

from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from hhplab.cli.aggregate_cli import _build_lagged_pep_series
from hhplab.cli.main import app

runner = CliRunner()

RETIRED_BUILD_SURFACES = ("pep", "pit", "acs", "zori")


def test_aggregate_help_shows_subcommands():
    result = runner.invoke(app, ["aggregate", "--help"])
    assert result.exit_code == 0
    assert "standalone CoC analysis inputs" in result.output
    for name in RETIRED_BUILD_SURFACES:
        assert name in result.output


@pytest.mark.parametrize("subcommand", RETIRED_BUILD_SURFACES)
def test_aggregate_commands_reject_retired_build_flag(subcommand: str):
    result = runner.invoke(app, ["aggregate", subcommand, "--build", "demo"])
    assert result.exit_code != 0
    assert "No such option: --build" in result.output


@pytest.mark.parametrize(
    ("subcommand", "dataset"),
    [
        ("pep", "pep"),
        ("pit", "pit"),
        ("acs", "acs"),
        ("zori", "zori"),
    ],
)
def test_aggregate_commands_reject_invalid_align(subcommand: str, dataset: str):
    result = runner.invoke(
        app,
        ["aggregate", subcommand, "--align", "bad_mode", "--years", "2020"],
    )
    assert result.exit_code == 2
    assert f"Invalid alignment mode 'bad_mode' for {dataset}" in result.output


def test_aggregate_pep_requires_years():
    result = runner.invoke(app, ["aggregate", "pep"])
    assert result.exit_code == 2
    assert "--years is required" in result.output


def test_aggregate_pep_with_invalid_years():
    result = runner.invoke(app, ["aggregate", "pep", "--years", "bad"])
    assert result.exit_code == 2


@patch("hhplab.pep.pep_aggregate.aggregate_pep_to_coc_many")
def test_aggregate_pep_accepts_repeated_weighting(mock_aggregate, tmp_path):
    """PEP CLI passes repeated weighting requests to one multi-output workflow."""
    mock_aggregate.return_value = {
        "area_share": tmp_path / "area.parquet",
        "population_weight": tmp_path / "population.parquet",
    }

    result = runner.invoke(
        app,
        [
            "aggregate",
            "pep",
            "--years",
            "2020",
            "--weighting",
            "area_share",
            "--weighting",
            "population_weight",
        ],
    )

    assert result.exit_code == 0
    mock_aggregate.assert_called_once()
    assert mock_aggregate.call_args.kwargs["weightings"] == [
        "area_share",
        "population_weight",
    ]
    assert "deprecated direct county/CoC area-overlap method" in result.output
    assert "Wrote (area_share)" in result.output
    assert "Wrote (population_weight)" in result.output


def test_aggregate_pep_lagged_rejects_lag_months_out_of_range():
    result = runner.invoke(
        app,
        [
            "aggregate",
            "pep",
            "--years",
            "2020",
            "--align",
            "lagged",
            "--lag-months",
            "13",
        ],
    )
    assert result.exit_code == 2
    assert "--lag-months must be between 0 and 12" in result.output


def test_aggregate_pep_rejects_lag_months_without_lagged_align():
    result = runner.invoke(
        app,
        [
            "aggregate",
            "pep",
            "--years",
            "2020",
            "--lag-months",
            "1",
        ],
    )
    assert result.exit_code == 2
    assert "--lag-months is only valid when --align=lagged" in result.output


def test_build_lagged_pep_series_zero_months_matches_current_year():
    import pandas as pd

    pep_df = pd.DataFrame(
        {
            "county_fips": ["01001", "01003", "01001", "01003"],
            "year": [2019, 2019, 2020, 2020],
            "population": [90000, 120000, 100000, 130000],
        }
    )

    result = _build_lagged_pep_series(pep_df, target_year=2020, lag_months=0)
    result = result.sort_values("county_fips").reset_index(drop=True)
    assert list(result["population"]) == [100000, 130000]


def test_build_lagged_pep_series_twelve_months_matches_previous_year():
    import pandas as pd

    pep_df = pd.DataFrame(
        {
            "county_fips": ["01001", "01003", "01001", "01003"],
            "year": [2019, 2019, 2020, 2020],
            "population": [90000, 120000, 100000, 130000],
        }
    )

    result = _build_lagged_pep_series(pep_df, target_year=2020, lag_months=12)
    result = result.sort_values("county_fips").reset_index(drop=True)
    assert list(result["population"]) == [90000, 120000]


def test_build_lagged_pep_series_interpolates_for_partial_month_lag():
    import pandas as pd

    pep_df = pd.DataFrame(
        {
            "county_fips": ["01001", "01003", "01001", "01003"],
            "year": [2019, 2019, 2020, 2020],
            "population": [90000, 120000, 100000, 130000],
        }
    )

    result = _build_lagged_pep_series(pep_df, target_year=2020, lag_months=6)
    result = result.sort_values("county_fips").reset_index(drop=True)
    assert list(result["population"]) == [95000, 125000]


def test_build_lagged_pep_series_rejects_invalid_lag_months():
    import pandas as pd

    pep_df = pd.DataFrame(
        {
            "county_fips": ["01001"],
            "year": [2020],
            "population": [100000],
        }
    )

    with pytest.raises(ValueError, match="--lag-months must be between 0 and 12"):
        _build_lagged_pep_series(pep_df, target_year=2020, lag_months=-1)


def _create_fake_acs_cache(acs_vintage: str, tract_vintage: str | int) -> None:
    """Create a minimal fake ACS cache file so aggregate reaches crosswalk check."""
    import pandas as pd

    from hhplab.acs.ingest.tract_population import get_output_path

    cache_path = get_output_path(acs_vintage, str(tract_vintage))
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "tract_geoid": ["01001020100"],
            "total_population": [100],
            "adult_population": [80],
            "median_household_income": [50000.0],
            "median_gross_rent": [1200.0],
            "poverty_universe": [95],
            "below_50pct_poverty": [5],
            "50_to_99pct_poverty": [10],
            "population_below_poverty": [15],
        }
    ).to_parquet(cache_path)


def test_aggregate_acs_missing_crosswalk_suggests_decennial():
    with runner.isolated_filesystem():
        _create_fake_acs_cache("2011-2015", 2010)
        result = runner.invoke(app, ["aggregate", "acs", "--years", "2015"])
        assert result.exit_code == 1
        assert "Crosswalk not found" in result.output
        assert "Did you mean to request" not in result.output
        assert "Run: hhplab generate xwalks --boundary 2015 --tracts 2010" in result.output


def test_aggregate_acs_missing_crosswalk_no_decennial_hint():
    with runner.isolated_filesystem():
        _create_fake_acs_cache("2016-2020", 2020)
        result = runner.invoke(app, ["aggregate", "acs", "--years", "2020"])
        assert result.exit_code == 1
        assert "Crosswalk not found" in result.output
        assert "Did you mean to request" not in result.output
        assert "Run: hhplab generate xwalks --boundary 2020 --tracts 2020" in result.output


def test_aggregate_pit_collects_data(tmp_path):
    """PIT aggregate should collect and write per-year PIT files for requested years."""
    import os

    import pandas as pd

    pit_dir = tmp_path / "data" / "curated" / "pit"
    pit_dir.mkdir(parents=True)
    for year in [2020, 2021]:
        df = pd.DataFrame(
            {
                "coc_id": [f"XX-{i:03d}" for i in range(3)],
                "pit_year": [year] * 3,
                "total_homeless": [100, 200, 300],
            }
        )
        df.to_parquet(pit_dir / f"pit__P{year}.parquet", index=False)

    old_cwd = os.getcwd()
    try:
        os.chdir(tmp_path)
        result = runner.invoke(
            app,
            ["aggregate", "pit", "--years", "2020-2021"],
            catch_exceptions=False,
        )
    finally:
        os.chdir(old_cwd)

    assert result.exit_code == 0
    assert "Wrote PIT aggregate" in result.output

    for year in [2020, 2021]:
        assert (pit_dir / f"pit__P{year}@B{year}.parquet").exists()


def test_aggregate_pit_falls_back_to_vintage(tmp_path):
    """PIT aggregate should discover vintage files when individual years are missing."""
    import os

    import pandas as pd

    pit_dir = tmp_path / "data" / "curated" / "pit"
    pit_dir.mkdir(parents=True)

    rows = []
    for year in range(2015, 2025):
        for i in range(3):
            rows.append(
                {
                    "coc_id": f"XX-{i:03d}",
                    "pit_year": year,
                    "total_homeless": 100 * (i + 1),
                }
            )
    vintage_df = pd.DataFrame(rows)
    vintage_df.to_parquet(pit_dir / "pit_vintage__P2024.parquet", index=False)

    old_cwd = os.getcwd()
    try:
        os.chdir(tmp_path)
        result = runner.invoke(
            app,
            ["aggregate", "pit", "--years", "2019-2021"],
            catch_exceptions=False,
        )
    finally:
        os.chdir(old_cwd)

    assert result.exit_code == 0
    assert "Using vintage P2024" in result.output
    assert "Wrote PIT aggregate" in result.output

    for year in [2019, 2020, 2021]:
        out_file = pit_dir / f"pit__P{year}@B{year}.parquet"
        assert out_file.exists()
        result_df = pd.read_parquet(out_file)
        assert sorted(result_df["pit_year"].unique()) == [year]


def test_aggregate_pit_vintage_partial_coverage(tmp_path):
    """Vintage file covers some years; missing years still reported."""
    import os

    import pandas as pd

    pit_dir = tmp_path / "data" / "curated" / "pit"
    pit_dir.mkdir(parents=True)

    rows = []
    for year in [2020, 2021]:
        for i in range(3):
            rows.append(
                {
                    "coc_id": f"XX-{i:03d}",
                    "pit_year": year,
                    "total_homeless": 100 * (i + 1),
                }
            )
    vintage_df = pd.DataFrame(rows)
    vintage_df.to_parquet(pit_dir / "pit_vintage__P2021.parquet", index=False)

    old_cwd = os.getcwd()
    try:
        os.chdir(tmp_path)
        result = runner.invoke(
            app,
            ["aggregate", "pit", "--years", "2020-2021,2025"],
            catch_exceptions=False,
        )
    finally:
        os.chdir(old_cwd)

    assert result.exit_code == 0
    assert "Using vintage P2021" in result.output
    assert "PIT data missing for years: [2025]" in result.output


def test_aggregate_pit_to_msa_materializes_weighted_outputs(tmp_path):
    """MSA PIT aggregate should use the stored CoC-to-MSA crosswalk."""
    import os

    import pandas as pd

    from hhplab.naming import msa_coc_xwalk_filename, msa_pit_filename

    pit_dir = tmp_path / "data" / "curated" / "pit"
    xwalk_dir = tmp_path / "data" / "curated" / "xwalks"
    pit_dir.mkdir(parents=True)
    xwalk_dir.mkdir(parents=True)

    pd.DataFrame(
        {
            "coc_id": ["CO-100", "CO-200", "CO-300"],
            "pit_year": [2020, 2020, 2020],
            "pit_total": [100.0, 80.0, 60.0],
            "pit_sheltered": [60.0, 40.0, 30.0],
            "pit_unsheltered": [40.0, 40.0, 30.0],
        }
    ).to_parquet(pit_dir / "pit__P2020.parquet", index=False)

    pd.DataFrame(
        {
            "coc_id": ["CO-100", "CO-200", "CO-200", "CO-300"],
            "msa_id": ["35620", "35620", "41180", "41180"],
            "cbsa_code": ["35620", "35620", "41180", "41180"],
            "boundary_vintage": ["2020"] * 4,
            "county_vintage": ["2020"] * 4,
            "definition_version": ["census_msa_2023"] * 4,
            "allocation_method": ["area"] * 4,
            "share_column": ["allocation_share"] * 4,
            "allocation_share": [1.0, 0.5, 0.5, 1.0],
        }
    ).to_parquet(
        xwalk_dir / msa_coc_xwalk_filename("2020", "census_msa_2023", 2020),
        index=False,
    )

    old_cwd = os.getcwd()
    try:
        os.chdir(tmp_path)
        result = runner.invoke(
            app,
            [
                "aggregate",
                "pit",
                "--years",
                "2020",
                "--geo-type",
                "msa",
                "--definition-version",
                "census_msa_2023",
                "--counties",
                "2020",
            ],
            catch_exceptions=False,
        )
    finally:
        os.chdir(old_cwd)

    assert result.exit_code == 0
    assert "Aggregating PIT to MSA" in result.output
    out_file = pit_dir / msa_pit_filename(2020, "census_msa_2023", 2020, 2020)
    assert out_file.exists()

    msa_df = pd.read_parquet(out_file).sort_values("msa_id").reset_index(drop=True)
    assert list(msa_df["msa_id"]) == ["35620", "41180"]
    assert list(msa_df["pit_total"].astype(float)) == pytest.approx([140.0, 100.0])


def test_aggregate_pit_to_msa_missing_crosswalk_is_actionable(tmp_path):
    """MSA PIT aggregate should report the exact missing crosswalk command."""
    import os

    import pandas as pd

    pit_dir = tmp_path / "data" / "curated" / "pit"
    pit_dir.mkdir(parents=True)
    pd.DataFrame(
        {
            "coc_id": ["CO-100"],
            "pit_year": [2020],
            "pit_total": [100.0],
        }
    ).to_parquet(pit_dir / "pit__P2020.parquet", index=False)

    old_cwd = os.getcwd()
    try:
        os.chdir(tmp_path)
        result = runner.invoke(
            app,
            [
                "aggregate",
                "pit",
                "--years",
                "2020",
                "--geo-type",
                "msa",
                "--definition-version",
                "census_msa_2023",
                "--counties",
                "2020",
            ],
            catch_exceptions=False,
        )
    finally:
        os.chdir(old_cwd)

    assert result.exit_code == 1
    assert (
        "generate msa-xwalk --boundary 2020 --definition-version census_msa_2023 --counties 2020"
        in result.output
    )
