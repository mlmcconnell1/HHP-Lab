"""Tests for DOJ sanctuary jurisdiction MSA matching."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from hhplab.cli.main import app
from hhplab.sources.doj.sanctuary import (
    SANCTUARY_MSA_MATCH_COLUMNS,
    SANCTUARY_MSA_PANEL_COLUMNS,
    build_sanctuary_msa_matches,
    build_sanctuary_msa_panel_covariate,
    write_sanctuary_msa_panel_covariate,
)

runner = CliRunner()

DEFINITIONS = pd.DataFrame(
    {
        "msa_id": ["11111", "22222", "33333", "44444"],
        "cbsa_code": ["11111", "22222", "33333", "44444"],
        "msa_name": [
            "California-Texas Test MSA",
            "Cook County Test MSA",
            "New York City Test MSA",
            "No Match Test MSA",
        ],
    }
)

MEMBERSHIP = pd.DataFrame(
    {
        "msa_id": ["11111", "11111", "22222", "33333", "33333", "44444"],
        "cbsa_code": ["11111", "11111", "22222", "33333", "33333", "44444"],
        "county_fips": ["06001", "48059", "17031", "36061", "36047", "48441"],
        "county_name": [
            "Alameda County",
            "Callahan County",
            "Cook County",
            "New York County",
            "Kings County",
            "Taylor County",
        ],
        "state_name": ["California", "Texas", "Illinois", "New York", "New York", "Texas"],
    }
)

COUNTY_POPULATION = pd.DataFrame(
    {
        "county_fips": ["06001", "48059", "17031", "36061", "36047", "48441"],
        "year": [2020, 2020, 2020, 2020, 2020, 2020],
        "population": [100.0, 300.0, 500.0, 200.0, 800.0, 700.0],
    }
)


def test_build_sanctuary_msa_matches_is_conservative_and_transparent() -> None:
    result = build_sanctuary_msa_matches(DEFINITIONS, MEMBERSHIP)

    assert tuple(result.columns) == SANCTUARY_MSA_MATCH_COLUMNS
    assert result["cbsa_code"].tolist() == ["11111", "22222", "33333"]

    multistate = result.loc[result["cbsa_code"] == "11111"].iloc[0]
    assert bool(multistate["state_match"])
    assert not bool(multistate["county_match"])
    assert bool(multistate["city_match"])
    assert multistate["matched_states"] == "California"
    assert multistate["matched_cities"] == "Berkeley, CA"
    assert multistate["match_basis"] == "state+city"

    cook = result.loc[result["cbsa_code"] == "22222"].iloc[0]
    assert cook["matched_counties"] == "Cook County, IL"
    assert cook["matched_cities"] == "Chicago, IL"
    assert cook["match_basis"] == "state+county+city"

    nyc = result.loc[result["cbsa_code"] == "33333"].iloc[0]
    assert nyc["matched_states"] == "New York"
    assert nyc["matched_cities"] == "New York City, NY"
    assert nyc["match_basis"] == "state+city"


def test_build_sanctuary_msa_panel_covariate_has_binary_indicator_for_all_msas() -> None:
    matches = build_sanctuary_msa_matches(DEFINITIONS, MEMBERSHIP)
    result = build_sanctuary_msa_panel_covariate(
        DEFINITIONS,
        MEMBERSHIP,
        COUNTY_POPULATION,
        matches,
        source_date="2025-08-05",
    )

    assert tuple(result.columns) == SANCTUARY_MSA_PANEL_COLUMNS
    assert result["msa_id"].tolist() == ["11111", "22222", "33333", "44444"]
    assert result["doj_sanctuary_msa"].tolist() == [1, 1, 1, 0]
    assert result["doj_sanctuary_population_share"].tolist() == [0.25, 1.0, 1.0, 0.0]
    assert result["doj_sanctuary_population"].tolist() == [100.0, 500.0, 1000.0, 0.0]
    assert result["doj_sanctuary_population_denominator"].tolist() == [
        400.0,
        500.0,
        1000.0,
        700.0,
    ]
    assert result["doj_sanctuary_population_year"].tolist() == [2020, 2020, 2020, 2020]
    assert result["doj_sanctuary_source_date"].unique().tolist() == ["2025-08-05"]

    no_match = result.loc[result["msa_id"] == "44444"].iloc[0]
    assert no_match["match_basis"] == ""
    assert not bool(no_match["state_match"])
    assert not bool(no_match["county_match"])
    assert not bool(no_match["city_match"])


def test_write_sanctuary_msa_panel_discovers_pep_county_vintage(
    monkeypatch, tmp_path: Path
) -> None:
    pep_dir = tmp_path / "curated" / "pep"
    pep_dir.mkdir(parents=True)
    COUNTY_POPULATION.to_parquet(
        pep_dir / "pep_county__v2024__y2020-2024.parquet",
        index=False,
    )

    monkeypatch.setattr(
        "hhplab.sources.doj.sanctuary.read_msa_definitions",
        lambda msa_definition_version, base_dir=None: DEFINITIONS,
    )
    monkeypatch.setattr(
        "hhplab.sources.doj.sanctuary.read_msa_county_membership",
        lambda msa_definition_version, base_dir=None: MEMBERSHIP,
    )

    result, output_path = write_sanctuary_msa_panel_covariate(base_dir=tmp_path)

    assert output_path.exists()
    assert result["doj_sanctuary_population_share"].tolist() == [0.25, 1.0, 1.0, 0.0]
    assert result["doj_sanctuary_population_year"].tolist() == [2020, 2020, 2020, 2020]


def test_write_sanctuary_msa_panel_missing_pep_county_error_is_actionable(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        "hhplab.sources.doj.sanctuary.read_msa_definitions",
        lambda msa_definition_version, base_dir=None: DEFINITIONS,
    )
    monkeypatch.setattr(
        "hhplab.sources.doj.sanctuary.read_msa_county_membership",
        lambda msa_definition_version, base_dir=None: MEMBERSHIP,
    )

    with pytest.raises(FileNotFoundError) as excinfo:
        write_sanctuary_msa_panel_covariate(base_dir=tmp_path)

    message = str(excinfo.value)
    assert "Run: hhplab ingest pep" in message
    assert "--geography county" not in message


def test_generate_sanctuary_msa_json(monkeypatch, tmp_path: Path) -> None:
    output = (
        tmp_path
        / "data"
        / "curated"
        / "sanctuary"
        / "sanctuary_msa_matches__D20250805xMcensus_msa_2023.parquet"
    )

    def fake_write_sanctuary_msa_matches(**kwargs):
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text("artifact", encoding="utf-8")
        return pd.DataFrame({"cbsa_code": ["11111"]}), output, None

    monkeypatch.setattr(
        "hhplab.sources.doj.sanctuary.write_sanctuary_msa_matches",
        fake_write_sanctuary_msa_matches,
    )

    with runner.isolated_filesystem(temp_dir=tmp_path):
        result = runner.invoke(
            app,
            ["generate", "sanctuary-msa", "--json", "--skip-raw-download"],
            catch_exceptions=False,
        )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["row_count"] == 1
    assert payload["artifact"].endswith(
        "sanctuary_msa_matches__D20250805xMcensus_msa_2023.parquet"
    )


def test_generate_sanctuary_msa_panel_json(monkeypatch, tmp_path: Path) -> None:
    output = (
        tmp_path
        / "data"
        / "curated"
        / "sanctuary"
        / "sanctuary_msa_panel__D20250805xMcensus_msa_2023.parquet"
    )

    def fake_write_sanctuary_msa_panel_covariate(**kwargs):
        output.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            {
                "msa_id": ["11111", "22222"],
                "doj_sanctuary_msa": [1, 0],
                "doj_sanctuary_population_share": [0.4, 0.0],
                "match_basis": ["state", ""],
            }
        ).to_parquet(output, index=False)
        return pd.read_parquet(output), output

    monkeypatch.setattr(
        "hhplab.sources.doj.sanctuary.write_sanctuary_msa_panel_covariate",
        fake_write_sanctuary_msa_panel_covariate,
    )

    with runner.isolated_filesystem(temp_dir=tmp_path):
        result = runner.invoke(
            app,
            ["generate", "sanctuary-msa-panel", "--json"],
            catch_exceptions=False,
        )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["row_count"] == 2
    assert payload["matched_msa_count"] == 1
    assert payload["indicator_column"] == "doj_sanctuary_msa"
    assert payload["intensity_column"] == "doj_sanctuary_population_share"
    assert payload["match_basis_column"] == "match_basis"
    assert payload["artifact"].endswith(
        "sanctuary_msa_panel__D20250805xMcensus_msa_2023.parquet"
    )
