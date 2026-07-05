"""Tests for the ``hhplab aggregate`` CLI command group."""

import json
from unittest.mock import patch

import pandas as pd
import pytest
from typer.testing import CliRunner

from hhplab.acs.acs_aggregate import (
    _available_contract_rent_bins,
    _derive_acs5_covariates,
)
from hhplab.cli.aggregate_cli import _resolve_boundary_vintage_map
from hhplab.cli.main import app
from hhplab.pep.pep_aggregate import build_lagged_pep_series

runner = CliRunner()

RETIRED_BUILD_SURFACES = ("pep", "pit", "acs", "zori", "cdc-overdose")


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
        ("cdc-overdose", "cdc-overdose"),
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


def test_boundary_vintage_map_supports_ranges_and_singletons():
    result = _resolve_boundary_vintage_map(
        [2015, 2016, 2020, 2022],
        "2015-2016:2019,2020,2022:2020",
    )

    assert result == {2015: "2019", 2016: "2019", 2020: "2020", 2022: "2020"}


def test_boundary_vintage_map_requires_complete_coverage():
    with pytest.raises(ValueError, match="Missing years: \\[2022\\]"):
        _resolve_boundary_vintage_map([2020, 2022], "2020:2020")


@patch("hhplab.cdc.overdose.ingest_and_aggregate_overdose_to_msa")
def test_aggregate_cdc_overdose_json_summary(mock_aggregate, tmp_path):
    county_df = pd.DataFrame({"county_fips": ["01001"], "year": [2024]})
    msa_df = pd.DataFrame({"msa_id": ["12345"], "year": [2024]})
    county_path = tmp_path / "county.parquet"
    msa_path = tmp_path / "msa.parquet"
    mock_aggregate.return_value = (county_df, msa_df, county_path, msa_path)

    result = runner.invoke(
        app,
        [
            "aggregate",
            "cdc-overdose",
            "--years",
            "2024",
            "--definition-version",
            "census_msa_2023",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["status"] == "ok"
    assert payload["dataset"] == "cdc-overdose"
    assert payload["geo_type"] == "msa"
    assert payload["row_counts"] == {"county": 1, "msa": 1}
    assert payload["outputs"] == {"county": str(county_path), "msa": str(msa_path)}
    mock_aggregate.assert_called_once()


@patch("hhplab.cdc.overdose.ingest_county_overdose")
def test_aggregate_cdc_overdose_county_json_summary(mock_ingest, tmp_path):
    county_df = pd.DataFrame({"county_fips": ["01001", "01003"], "year": [2024, 2024]})
    county_path = tmp_path / "county.parquet"
    mock_ingest.return_value = (county_df, county_path)

    result = runner.invoke(
        app,
        [
            "aggregate",
            "cdc-overdose",
            "--years",
            "2024",
            "--geo-type",
            "county",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["status"] == "ok"
    assert payload["geo_type"] == "county"
    assert payload["definition_version"] is None
    assert payload["min_coverage"] is None
    assert payload["row_counts"] == {"county": 2}
    assert payload["outputs"] == {"county": str(county_path)}
    mock_ingest.assert_called_once()
    assert mock_ingest.call_args.kwargs["years"] == [2024]


def test_aggregate_cdc_overdose_json_rejects_invalid_geo_type():
    result = runner.invoke(
        app,
        [
            "aggregate",
            "cdc-overdose",
            "--years",
            "2024",
            "--geo-type",
            "tract",
            "--json",
        ],
    )

    assert result.exit_code == 2
    payload = json.loads(result.output)
    assert payload["status"] == "error"
    assert "Use one of: county, msa" in payload["message"]


def test_aggregate_cdc_overdose_json_rejects_invalid_min_coverage():
    result = runner.invoke(
        app,
        [
            "aggregate",
            "cdc-overdose",
            "--years",
            "2024",
            "--min-coverage",
            "1.5",
            "--json",
        ],
    )

    assert result.exit_code == 2
    payload = json.loads(result.output)
    assert payload["status"] == "error"
    assert "--min-coverage must be between 0 and 1" in payload["message"]


@patch("hhplab.cdc.overdose.ingest_and_aggregate_overdose_to_msa")
def test_aggregate_cdc_overdose_json_reports_source_errors(mock_aggregate):
    mock_aggregate.side_effect = FileNotFoundError(
        "CDC overdose CSV not found: data/raw/cdc/missing.csv"
    )

    result = runner.invoke(
        app,
        ["aggregate", "cdc-overdose", "--years", "2024", "--json"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["status"] == "error"
    assert "CDC overdose CSV not found" in payload["message"]


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

    result = build_lagged_pep_series(pep_df, target_year=2020, lag_months=0)
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

    result = build_lagged_pep_series(pep_df, target_year=2020, lag_months=12)
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

    result = build_lagged_pep_series(pep_df, target_year=2020, lag_months=6)
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
        build_lagged_pep_series(pep_df, target_year=2020, lag_months=-1)


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


def _create_fake_expanded_acs_cache(acs_vintage: str, tract_vintage: str | int) -> None:
    from hhplab.acs.ingest.tract_population import get_output_path

    cache_path = get_output_path(acs_vintage, str(tract_vintage))
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "tract_geoid": ["01001020100", "01003010100"],
            "total_population": [100.0, 300.0],
            "adult_population": [80.0, 240.0],
            "citizenship_total": [100.0, 300.0],
            "naturalized_citizen": [10.0, 30.0],
            "not_us_citizen": [5.0, 15.0],
            "median_household_income": [50000.0, 60000.0],
            "median_gross_rent": [1200.0, 1400.0],
            "contract_rent_distribution_with_cash_rent": [40.0, 60.0],
            "contract_rent_distribution_cash_rent_lt_100": [0.0, 0.0],
            "contract_rent_distribution_cash_rent_100_to_149": [0.0, 0.0],
            "contract_rent_distribution_cash_rent_150_to_199": [0.0, 0.0],
            "contract_rent_distribution_cash_rent_200_to_249": [0.0, 0.0],
            "contract_rent_distribution_cash_rent_250_to_299": [40.0, 60.0],
        }
    ).to_parquet(cache_path)


def _create_fake_msa_membership(definition_version: str) -> None:
    from hhplab.naming import msa_county_membership_path

    path = msa_county_membership_path(definition_version)
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "msa_id": ["12345", "12345"],
            "cbsa_code": ["12345", "12345"],
            "county_fips": ["01001", "01003"],
            "definition_version": [definition_version, definition_version],
        }
    ).to_parquet(path)


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


def test_aggregate_acs_to_msa_writes_expanded_panel_ready_output():
    with runner.isolated_filesystem():
        _create_fake_expanded_acs_cache("2019-2023", 2020)
        _create_fake_msa_membership("census_msa_2023")

        result = runner.invoke(
            app,
            [
                "aggregate",
                "acs",
                "--years",
                "2023",
                "--tracts",
                "2020",
                "--target-geo",
                "msa",
                "--json",
            ],
        )

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output.splitlines()[-1])
        assert payload["status"] == "ok"
        assert payload["target_geo"] == "msa"
        assert payload["geo_count"] == 1

        out_path = payload["outputs"][0]
        assert out_path.endswith("measures__msa__A2023@Mcensusmsa2023xT2020.parquet")
        output = pd.read_parquet(out_path)
        assert output.loc[0, "msa_id"] == "12345"
        assert output.loc[0, "non_native_share"] == pytest.approx(0.15)
        assert output.loc[0, "contract_rent_p10"] == pytest.approx(255.0)
        assert output.loc[0, "contract_rent_p25"] == pytest.approx(262.5)
        assert output.loc[0, "contract_rent_p50"] == pytest.approx(275.0)
        assert "msa_contract_rent_p10" in output.columns
        assert "msa_contract_rent_p25" in output.columns
        assert "msa_contract_rent_p50" in output.columns


def test_derive_acs5_covariates_propagates_missing_non_native_component():
    df = pd.DataFrame(
        {
            "citizenship_total": [100.0, 100.0],
            "naturalized_citizen": [10.0, None],
            "not_us_citizen": [5.0, 5.0],
        }
    )

    _derive_acs5_covariates(df)

    assert df.loc[0, "non_native_share"] == pytest.approx(0.15)
    assert pd.isna(df.loc[1, "non_native_share"])


def test_available_contract_rent_bins_uses_early_schema_without_modern_split():
    df = pd.DataFrame(
        {
            "contract_rent_distribution_cash_rent_1500_to_1999": [10.0],
            "contract_rent_distribution_cash_rent_2000_plus": [5.0],
        }
    )

    bins = _available_contract_rent_bins(df)

    assert [column for column, _, _ in bins] == [
        "contract_rent_distribution_cash_rent_1500_to_1999",
        "contract_rent_distribution_cash_rent_2000_plus",
    ]


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


def test_aggregate_pit_to_msa_uses_boundary_vintage_map(tmp_path):
    """MSA PIT aggregate can use an era boundary different from PIT year."""
    import os

    from hhplab.naming import msa_coc_xwalk_filename, msa_pit_filename

    pit_dir = tmp_path / "data" / "curated" / "pit"
    xwalk_dir = tmp_path / "data" / "curated" / "xwalks"
    pit_dir.mkdir(parents=True)
    xwalk_dir.mkdir(parents=True)

    pd.DataFrame(
        {
            "coc_id": ["CO-100"],
            "pit_year": [2018],
            "pit_total": [100.0],
            "pit_sheltered": [60.0],
            "pit_unsheltered": [40.0],
        }
    ).to_parquet(pit_dir / "pit__P2018.parquet", index=False)
    pd.DataFrame(
        {
            "coc_id": ["CO-100"],
            "msa_id": ["35620"],
            "cbsa_code": ["35620"],
            "boundary_vintage": ["2020"],
            "county_vintage": ["2023"],
            "definition_version": ["census_msa_2023"],
            "allocation_method": ["area"],
            "share_column": ["allocation_share"],
            "allocation_share": [1.0],
        }
    ).to_parquet(
        xwalk_dir / msa_coc_xwalk_filename("2020", "census_msa_2023", 2023),
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
                "2018",
                "--geo-type",
                "msa",
                "--definition-version",
                "census_msa_2023",
                "--counties",
                "2023",
                "--boundary-vintages",
                "2018:2020",
            ],
            catch_exceptions=False,
        )
    finally:
        os.chdir(old_cwd)

    assert result.exit_code == 0, result.output
    out_file = pit_dir / msa_pit_filename(2018, "census_msa_2023", 2020, 2023)
    assert out_file.exists()
    msa_df = pd.read_parquet(out_file)
    assert msa_df.loc[0, "boundary_vintage"] == "2020"
    assert msa_df.loc[0, "pit_total"] == pytest.approx(100.0)


def test_aggregate_pit_to_msa_uses_block_population_crosswalk(tmp_path):
    """MSA PIT aggregate should consume generated block-population crosswalks."""
    import os

    import pandas as pd

    from hhplab.naming import msa_coc_block_population_xwalk_filename, msa_pit_filename
    from hhplab.provenance import read_provenance

    pit_dir = tmp_path / "data" / "curated" / "pit"
    xwalk_dir = tmp_path / "data" / "curated" / "xwalks"
    pit_dir.mkdir(parents=True)
    xwalk_dir.mkdir(parents=True)

    pd.DataFrame(
        {
            "coc_id": ["CO-100", "CO-200"],
            "pit_year": [2020, 2020],
            "pit_total": [100.0, 80.0],
            "pit_sheltered": [60.0, 40.0],
            "pit_unsheltered": [40.0, 40.0],
        }
    ).to_parquet(pit_dir / "pit__P2020.parquet", index=False)

    pd.DataFrame(
        {
            "coc_id": ["CO-100", "CO-200", "CO-200"],
            "msa_id": ["35620", "35620", "41180"],
            "cbsa_code": ["35620", "35620", "41180"],
            "boundary_vintage": ["2020"] * 3,
            "county_vintage": ["2020"] * 3,
            "definition_version": ["census_msa_2023"] * 3,
            "allocation_method": ["block_population"] * 3,
            "share_column": ["allocation_share"] * 3,
            "allocation_share": [1.0, 0.25, 0.75],
            "allocation_basis": ["block_population"] * 3,
            "denominator_source": ["pl_94_171_block_population"] * 3,
        }
    ).to_parquet(
        xwalk_dir
        / msa_coc_block_population_xwalk_filename(
            2020,
            "census_msa_2023",
            2020,
            2020,
            2020,
        ),
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
                "--allocation-basis",
                "block_population",
                "--blocks",
                "2020",
                "--decennial",
                "2020",
            ],
            catch_exceptions=False,
        )
    finally:
        os.chdir(old_cwd)

    assert result.exit_code == 0
    out_file = pit_dir / msa_pit_filename(2020, "census_msa_2023", 2020, 2020)
    msa_df = pd.read_parquet(out_file).sort_values("msa_id").reset_index(drop=True)
    assert list(msa_df["pit_total"].astype(float)) == pytest.approx([120.0, 60.0])
    assert msa_df["allocation_method"].unique().tolist() == ["block_population"]
    provenance = read_provenance(out_file)
    assert provenance is not None
    assert provenance.weighting == "block_population"
    assert provenance.extra["allocation_method"] == "block_population"


def test_aggregate_coc_measure_to_msa_materializes_hic_rollup(tmp_path):
    """Generic CoC measure rollup should support HIC-style additive columns."""
    import os

    import pandas as pd

    from hhplab.naming import msa_coc_xwalk_filename, msa_fractional_rollup_filename
    from hhplab.provenance import read_provenance

    hic_dir = tmp_path / "data" / "curated" / "hic"
    xwalk_dir = tmp_path / "data" / "curated" / "xwalks"
    hic_dir.mkdir(parents=True)
    xwalk_dir.mkdir(parents=True)
    source_path = hic_dir / "hic__H2020.parquet"

    pd.DataFrame(
        {
            "coc_id": ["CO-100", "CO-200", "CO-300"],
            "hic_year": [2020, 2020, 2020],
            "hic_shelter_year_round_beds": [10.0, 20.0, 30.0],
            "hic_total_units": [1.0, 2.0, 3.0],
        }
    ).to_parquet(source_path, index=False)

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
                "coc-measure",
                "--source",
                str(source_path),
                "--columns",
                "hic_shelter_year_round_beds,hic_total_units",
                "--geo-type",
                "msa",
                "--definition-version",
                "census_msa_2023",
                "--boundary-vintage",
                "2020",
                "--counties",
                "2020",
                "--json",
            ],
            catch_exceptions=False,
        )
    finally:
        os.chdir(old_cwd)

    assert result.exit_code == 0
    payload = json.loads(result.output.strip().splitlines()[-1])
    expected_path = hic_dir / msa_fractional_rollup_filename(
        2020,
        2020,
        "hic",
        "area",
        "2020",
        "census_msa_2023",
        "2020",
    )
    assert payload["output_path"] == str(expected_path)
    assert payload["additive_measure_columns"] == [
        "hic_shelter_year_round_beds",
        "hic_total_units",
    ]
    assert expected_path.exists()

    rollup = pd.read_parquet(expected_path).sort_values("msa_id").reset_index(drop=True)
    assert rollup["hic_shelter_year_round_beds"].astype(float).tolist() == pytest.approx(
        [20.0, 40.0]
    )
    assert rollup["hic_total_units"].astype(float).tolist() == pytest.approx([2.0, 4.0])
    assert rollup["source_dataset_id"].unique().tolist() == ["hic"]

    provenance = read_provenance(expected_path)
    assert provenance is not None
    assert provenance.extra["dataset_type"] == "msa_fractional_rollup"
    assert provenance.extra["source_dataset_id"] == "hic"
    assert provenance.extra["source_additive_measure_columns"] == [
        "hic_shelter_year_round_beds",
        "hic_total_units",
    ]


def test_aggregate_coc_measure_missing_crosswalk_json_is_actionable(tmp_path):
    import os

    import pandas as pd

    hic_dir = tmp_path / "data" / "curated" / "hic"
    hic_dir.mkdir(parents=True)
    source_path = hic_dir / "hic__H2020.parquet"
    pd.DataFrame(
        {
            "coc_id": ["CO-100"],
            "hic_year": [2020],
            "hic_shelter_year_round_beds": [10.0],
        }
    ).to_parquet(source_path, index=False)

    old_cwd = os.getcwd()
    try:
        os.chdir(tmp_path)
        result = runner.invoke(
            app,
            [
                "aggregate",
                "coc-measure",
                "--source",
                str(source_path),
                "--columns",
                "hic_shelter_year_round_beds",
                "--geo-type",
                "msa",
                "--definition-version",
                "census_msa_2023",
                "--boundary-vintage",
                "2020",
                "--counties",
                "2020",
                "--json",
            ],
            catch_exceptions=False,
        )
    finally:
        os.chdir(old_cwd)

    assert result.exit_code == 1
    payload = json.loads(result.output.strip().splitlines()[-1])
    assert payload["status"] == "error"
    assert (
        "generate msa-xwalk --boundary 2020 --definition-version census_msa_2023 --counties 2020"
        in payload["message"]
    )


@pytest.mark.parametrize(
    ("rows", "extra_args", "expected_message"),
    [
        pytest.param(
            {
                "coc_id": ["CO-100"],
                "hic_year": ["not-a-year"],
                "hic_shelter_year_round_beds": [10.0],
            },
            [],
            "Source year column 'hic_year' must contain non-null integer years",
            id="fallback-hic-year-string",
        ),
        pytest.param(
            {
                "coc_id": ["CO-100"],
                "source_year": ["bad"],
                "hic_shelter_year_round_beds": [10.0],
            },
            ["--year-column", "source_year"],
            "Source year column 'source_year' must contain non-null integer years",
            id="explicit-year-column-string",
        ),
        pytest.param(
            {
                "coc_id": ["CO-100"],
                "hic_year": [None],
                "hic_shelter_year_round_beds": [10.0],
            },
            [],
            "Invalid values: <null>",
            id="null-year",
        ),
    ],
)
def test_aggregate_coc_measure_bad_year_json_is_actionable(
    tmp_path,
    rows: dict[str, list[object]],
    extra_args: list[str],
    expected_message: str,
):
    import os

    hic_dir = tmp_path / "data" / "curated" / "hic"
    hic_dir.mkdir(parents=True)
    source_path = hic_dir / "hic__Hbad.parquet"
    pd.DataFrame(rows).to_parquet(source_path, index=False)

    old_cwd = os.getcwd()
    try:
        os.chdir(tmp_path)
        result = runner.invoke(
            app,
            [
                "aggregate",
                "coc-measure",
                "--source",
                str(source_path),
                "--columns",
                "hic_shelter_year_round_beds",
                "--geo-type",
                "msa",
                "--json",
                *extra_args,
            ],
            catch_exceptions=False,
        )
    finally:
        os.chdir(old_cwd)

    assert result.exit_code == 1
    payload = json.loads(result.output.strip().splitlines()[-1])
    assert payload["status"] == "error"
    assert expected_message in payload["message"]


def test_aggregate_coc_measure_multiyear_without_boundary_json_is_actionable(tmp_path):
    import os

    hic_dir = tmp_path / "data" / "curated" / "hic"
    hic_dir.mkdir(parents=True)
    source_path = hic_dir / "hic__Hmulti.parquet"
    pd.DataFrame(
        {
            "coc_id": ["CO-100", "CO-100"],
            "hic_year": [2020, 2021],
            "hic_shelter_year_round_beds": [10.0, 11.0],
        }
    ).to_parquet(source_path, index=False)

    old_cwd = os.getcwd()
    try:
        os.chdir(tmp_path)
        result = runner.invoke(
            app,
            [
                "aggregate",
                "coc-measure",
                "--source",
                str(source_path),
                "--columns",
                "hic_shelter_year_round_beds",
                "--geo-type",
                "msa",
                "--json",
            ],
            catch_exceptions=False,
        )
    finally:
        os.chdir(old_cwd)

    assert result.exit_code == 2
    payload = json.loads(result.output.strip().splitlines()[-1])
    assert payload["status"] == "error"
    assert "--boundary-vintage is required" in payload["message"]


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


def test_aggregate_pit_to_msa_missing_block_population_crosswalk_is_actionable(tmp_path):
    """Missing block-population PIT MSA crosswalk should include matching generate flags."""
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
                "--allocation-basis",
                "block_population",
                "--blocks",
                "2020",
                "--decennial",
                "2020",
            ],
            catch_exceptions=False,
        )
    finally:
        os.chdir(old_cwd)

    assert result.exit_code == 1
    assert "--allocation-basis block_population --boundary 2020" in result.output
    assert "--blocks 2020 --decennial 2020" in result.output


def test_aggregate_pit_json_summary_for_coc_output(tmp_path):
    """PIT aggregate should support machine-readable JSON output."""
    import os

    import pandas as pd

    pit_dir = tmp_path / "data" / "curated" / "pit"
    pit_dir.mkdir(parents=True)
    pd.DataFrame(
        {
            "coc_id": ["CO-100", "CO-101"],
            "pit_year": [2020, 2020],
            "pit_total": [100.0, 200.0],
        }
    ).to_parquet(pit_dir / "pit__P2020.parquet", index=False)

    old_cwd = os.getcwd()
    try:
        os.chdir(tmp_path)
        result = runner.invoke(
            app,
            ["aggregate", "pit", "--years", "2020", "--geo-type", "coc", "--json"],
            catch_exceptions=False,
        )
    finally:
        os.chdir(old_cwd)

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output.strip().splitlines()[-1])
    assert payload["status"] == "ok"
    assert payload["geo_type"] == "coc"
    assert payload["years_requested"] == [2020]
    assert payload["years_materialized"] == [2020]
    assert payload["source_coc_count"] == 2
    assert payload["source_record_count"] == 2
    assert payload["file_count"] == 1
