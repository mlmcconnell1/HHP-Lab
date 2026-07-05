"""Tests for expanded hidden-cause covariate sources."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from hhplab.cli.main import app
from hhplab.covariates.aggregate import (
    EMERGENCY_SHELTER_ACTIVATION_C,
    aggregate_covariate_source,
    derive_prism_temperature_basis,
)
from hhplab.covariates.catalog import COVARIATE_SOURCE_SPECS
from hhplab.covariates.ingest import ingest_covariate_source
from hhplab.covariates.mpi_contract import (
    MPI_COUNTY_HEADERS,
    MPI_COUNTY_SHEET,
    MPI_CURATED_COUNTY_COLUMNS,
    MPI_ESTIMATE_YEAR,
    MPI_GEOGRAPHY_RULES,
    MPI_MEASURE_COLUMNS,
    MPI_RAW_COUNTY_COLUMNS,
    MPI_RAW_STATE_COLUMNS,
    MPI_REQUIRED_SHEETS,
    MPI_SOURCE_ID,
    MPI_STATE_HEADERS,
    MPI_STATE_SHEET,
    MPI_WORKBOOK_GLOB,
    validate_mpi_workbook_contract,
)
from hhplab.curated_policy import validate_curated_layout
from hhplab.provenance import read_provenance

runner = CliRunner()

EXPECTED_COVARIATE_SOURCES = {
    "eviction_lab": ("county", "eviction_filings"),
    "census_bps": ("county", "permitted_units"),
    "hud_fmr": ("county", "fmr_2br"),
    "hud_psh": ("county", "subsidized_households"),
    "hud_spm": ("coc", "spm_first_time_homeless"),
    "kff_medicaid_expansion": ("state", "medicaid_expansion_adopted"),
    "prism_tmin_january": ("county", "tmin_c"),
    MPI_SOURCE_ID: ("county", "unauthorized_immigrant_population"),
}

BRANCH_ROUNDTRIP_CASES = [
    pytest.param(
        "census_bps",
        {
            "county_fips": ["01001"],
            "year": [2020],
            "permitted_units": [100],
            "permitted_buildings": [8],
        },
        "county",
        ["01001"],
        id="census-bps-county",
    ),
    pytest.param(
        "hud_psh",
        {
            "county_fips": ["01001"],
            "year": [2023],
            "subsidized_households": [500],
            "housing_choice_vouchers": [200],
        },
        "county",
        ["01001"],
        id="hud-psh-county",
    ),
    pytest.param(
        "hud_spm",
        {
            "coc_number": ["CA-600"],
            "year": [2023],
            "spm_first_time_homeless": [120],
            "spm_returns_to_homelessness": [25],
            "spm_successful_exits": [80],
        },
        "coc",
        ["CA-600"],
        id="hud-spm-coc",
    ),
    pytest.param(
        "kff_medicaid_expansion",
        {
            "state": ["Alabama", "CA"],
            "year": [2020, 2021],
            "medicaid_expansion_adopted": [0, 1],
        },
        "state",
        ["AL", "CA"],
        id="kff-state",
    ),
]


def _write_mpi_contract_workbook(
    tmp_path: Path,
    *,
    state_headers: tuple[str, ...] = MPI_STATE_HEADERS,
    county_headers: tuple[str, ...] = MPI_COUNTY_HEADERS,
) -> Path:
    from openpyxl import Workbook

    workbook = Workbook()
    state_sheet = workbook.active
    state_sheet.title = MPI_STATE_SHEET
    county_sheet = workbook.create_sheet(MPI_COUNTY_SHEET)

    state_sheet.append([])
    state_sheet.append(["National and State Estimates of the Unauthorized Immigrant Population"])
    state_sheet.append(list(state_headers))
    state_sheet.append(["United States", 13_738_000, 1.0])

    county_sheet.append([])
    county_sheet.append(["National and County Estimates of the Unauthorized Immigrant Population"])
    county_sheet.append(list(county_headers))
    county_sheet.append([None, "United States", 13_738_000, 1.0])

    workbook_path = tmp_path / (
        "MPI-2023_Unauthorized_Profiles-State-County-Topline_Estimates-FINAL.xlsx"
    )
    workbook.save(workbook_path)
    return workbook_path


def test_covariate_catalog_declares_hidden_cause_sources() -> None:
    """The expanded source catalog should cover all bead-requested families."""
    assert set(COVARIATE_SOURCE_SPECS) == set(EXPECTED_COVARIATE_SOURCES)
    for source_id, (native_geo, measure) in EXPECTED_COVARIATE_SOURCES.items():
        spec = COVARIATE_SOURCE_SPECS[source_id]
        assert spec.native_geo == native_geo
        assert measure in spec.measure_columns
        assert spec.source_page.startswith("https://")
        assert spec.recommended_align


def test_mpi_contract_declares_workbook_schema_and_geography_rules() -> None:
    """MPI ingest has a written contract before parser implementation."""
    assert MPI_WORKBOOK_GLOB.endswith("*.xlsx")
    assert MPI_REQUIRED_SHEETS == (MPI_STATE_SHEET, MPI_COUNTY_SHEET)
    assert MPI_MEASURE_COLUMNS == (
        "unauthorized_immigrant_population",
        "unauthorized_immigrant_share_of_us_total",
    )
    assert "county_fips" in MPI_CURATED_COUNTY_COLUMNS
    assert "exclusion_reason" in MPI_RAW_COUNTY_COLUMNS
    assert "is_us_total" in MPI_RAW_STATE_COLUMNS
    assert any("MSAs" in rule for rule in MPI_GEOGRAPHY_RULES)
    assert any("independent city" in rule for rule in MPI_GEOGRAPHY_RULES)
    assert any("Alaska municipality" in rule for rule in MPI_GEOGRAPHY_RULES)
    spec = COVARIATE_SOURCE_SPECS[MPI_SOURCE_ID]
    assert spec.first_year == MPI_ESTIMATE_YEAR
    assert spec.last_year == MPI_ESTIMATE_YEAR
    assert set(MPI_MEASURE_COLUMNS) <= set(spec.measure_columns)


def test_mpi_workbook_contract_accepts_expected_layout(tmp_path: Path) -> None:
    """Workbook validation checks the stable MPI sheet and header contract."""
    workbook_path = _write_mpi_contract_workbook(tmp_path)

    contract = validate_mpi_workbook_contract(workbook_path)

    assert contract.source_id == MPI_SOURCE_ID
    assert contract.required_sheets == MPI_REQUIRED_SHEETS
    assert contract.curated_county_columns == MPI_CURATED_COUNTY_COLUMNS


def test_mpi_workbook_contract_rejects_unsupported_layout(tmp_path: Path) -> None:
    """Workbook drift should fail with an actionable contract update message."""
    workbook_path = _write_mpi_contract_workbook(
        tmp_path,
        county_headers=("State", "County", "Unexpected Estimate", "Share"),
    )

    with pytest.raises(ValueError, match="Update the MPI source contract"):
        validate_mpi_workbook_contract(workbook_path)


@pytest.mark.parametrize(
    "source_id,raw_data,target_geo,expected_geo_ids",
    BRANCH_ROUNDTRIP_CASES,
)
def test_covariate_ingest_and_native_aggregate_branches(
    source_id: str,
    raw_data: dict[str, list],
    target_geo: str,
    expected_geo_ids: list[str],
    tmp_path: Path,
    monkeypatch,
) -> None:
    """All expanded covariate geography branches should normalize and aggregate natively."""
    monkeypatch.setattr("hhplab.covariates.ingest.register_source", lambda **_: None)
    raw = tmp_path / f"{source_id}.csv"
    pd.DataFrame(raw_data).to_csv(raw, index=False)

    curated = ingest_covariate_source(source_id, raw, output_dir=tmp_path, force=True)
    curated_df = pd.read_parquet(curated)
    assert curated_df["geo_type"].unique().tolist() == [target_geo]
    assert curated_df["geo_id"].tolist() == expected_geo_ids
    if source_id == "kff_medicaid_expansion":
        assert curated_df["state"].tolist() == ["AL", "CA"]
        assert curated_df["state_fips"].tolist() == ["01", "06"]

    panel = aggregate_covariate_source(
        source_id,
        curated_path=curated,
        output_dir=tmp_path,
        target_geo=target_geo,
        force=True,
    )
    panel_df = pd.read_parquet(panel)
    assert panel_df["geo_id"].tolist() == expected_geo_ids


def test_state_covariate_rejects_unknown_state(tmp_path: Path, monkeypatch) -> None:
    """Unrecognized state labels should fail before incompatible geo_ids are written."""
    monkeypatch.setattr("hhplab.covariates.ingest.register_source", lambda **_: None)
    raw = tmp_path / "kff.csv"
    pd.DataFrame(
        {
            "state": ["Atlantis"],
            "year": [2020],
            "medicaid_expansion_adopted": [0],
        }
    ).to_csv(raw, index=False)

    with pytest.raises(ValueError, match="unrecognized state value 'Atlantis'"):
        ingest_covariate_source(
            "kff_medicaid_expansion",
            raw,
            output_dir=tmp_path,
            force=True,
        )


def test_ingest_and_aggregate_county_covariate_roundtrip(tmp_path: Path, monkeypatch) -> None:
    """Staged provider CSVs become curated and panel-ready Parquet."""
    monkeypatch.setattr("hhplab.covariates.ingest.register_source", lambda **_: None)
    raw = tmp_path / "evictions.csv"
    pd.DataFrame(
        {
            "fips": ["1001", "01003"],
            "year": [2020, 2021],
            "eviction_filings": [120, 140],
            "eviction_rate": [2.5, 2.7],
        }
    ).to_csv(raw, index=False)

    curated = ingest_covariate_source(
        "eviction_lab",
        raw,
        output_dir=tmp_path,
        force=True,
    )
    curated_df = pd.read_parquet(curated)

    assert curated.name == "covariate__eviction_lab__Y2000-ongoing.parquet"
    assert curated_df["geo_type"].tolist() == ["county", "county"]
    assert curated_df["county_fips"].tolist() == ["01001", "01003"]
    assert curated_df["eviction_filings"].tolist() == [120, 140]
    provenance = read_provenance(curated)
    assert provenance is not None
    assert provenance.extra["dataset_type"] == "expanded_covariate"
    assert provenance.extra["source_id"] == "eviction_lab"

    with pytest.raises(ValueError, match="cannot be emitted as coc panel-ready data"):
        aggregate_covariate_source(
            "eviction_lab",
            curated_path=curated,
            output_dir=tmp_path,
            years=[2021],
            force=True,
        )

    panel = aggregate_covariate_source(
        "eviction_lab",
        curated_path=curated,
        output_dir=tmp_path,
        years=[2021],
        target_geo="county",
        force=True,
    )
    panel_df = pd.read_parquet(panel)
    assert panel.name == "covariate_panel__eviction_lab__Y2000-ongoing.parquet"
    assert panel_df["geo_id"].tolist() == ["01003"]
    assert panel_df["year"].tolist() == [2021]
    panel_provenance = read_provenance(panel)
    assert panel_provenance is not None
    assert panel_provenance.extra["dataset_type"] == "expanded_covariate_panel"
    assert panel_provenance.extra["target_geo"] == "county"


def test_covariate_outputs_pass_curated_layout_policy(tmp_path: Path, monkeypatch) -> None:
    """Covariate artifacts should use canonical names in a registered curated subdir."""
    monkeypatch.setattr("hhplab.covariates.ingest.register_source", lambda **_: None)
    raw = tmp_path / "fmr.csv"
    pd.DataFrame(
        {
            "county_fips": ["01001"],
            "year": [2024],
            "fmr_0br": [750],
            "fmr_1br": [850],
            "fmr_2br": [1000],
            "fmr_3br": [1250],
            "fmr_4br": [1500],
        }
    ).to_csv(raw, index=False)
    curated_root = tmp_path / "curated"
    covariate_dir = curated_root / "covariates"

    curated = ingest_covariate_source(
        "hud_fmr",
        raw,
        output_dir=covariate_dir,
        force=True,
    )
    panel = aggregate_covariate_source(
        "hud_fmr",
        curated_path=curated,
        output_dir=covariate_dir,
        years=[2024],
        target_geo="county",
        force=True,
    )

    assert curated.name == "covariate__hud_fmr__Y2000-ongoing.parquet"
    assert panel.name == "covariate_panel__hud_fmr__Y2000-ongoing.parquet"
    assert validate_curated_layout(curated_root) == []


def test_prism_tmin_basis_columns_sum_to_temperature() -> None:
    """PRISM tmin basis columns should be policy-threshold pieces of tmin_c."""
    df = pd.DataFrame({"tmin_c": [-3.0, 2.0, 7.0]})

    result = derive_prism_temperature_basis(df)

    assert result["tmin_below_freezing"].tolist() == [-3.0, 0.0, 0.0]
    assert result["tmin_code_blue_band"].tolist() == [0.0, 2.0, EMERGENCY_SHELTER_ACTIVATION_C]
    assert result["tmin_above_code_blue"].tolist() == pytest.approx([0.0, 0.0, 2.6])
    total = (
        result["tmin_below_freezing"]
        + result["tmin_code_blue_band"]
        + result["tmin_above_code_blue"]
    )
    assert total.tolist() == pytest.approx(result["tmin_c"].tolist())


def test_prism_county_covariate_aggregates_to_msa_with_population_weights(
    tmp_path: Path,
) -> None:
    """County-native PRISM tmin rolls up to MSA-year rows with PEP population weights."""
    curated = tmp_path / "prism_county_monthly__tmin__Y2024M01@C2023.parquet"
    pd.DataFrame(
        {
            "geo_type": ["county", "county", "county"],
            "geo_id": ["01001", "01003", "02001"],
            "county_fips": ["01001", "01003", "02001"],
            "year": [2024, 2024, 2024],
            "month": [1, 1, 1],
            "tmin_c": [0.0, 10.0, -2.0],
        }
    ).to_parquet(curated)
    population = tmp_path / "pep_county__v2024.parquet"
    pd.DataFrame(
        {
            "county_fips": ["01001", "01003", "02001"],
            "year": [2024, 2024, 2024],
            "population": [100.0, 300.0, 500.0],
        }
    ).to_parquet(population)
    data_root = tmp_path / "data"
    msa_dir = data_root / "curated" / "msa"
    msa_dir.mkdir(parents=True)
    pd.DataFrame(
        {
            "msa_id": ["11111", "11111", "22222"],
            "cbsa_code": ["11111", "11111", "22222"],
            "county_fips": ["01001", "01003", "02001"],
            "definition_version": ["test_msa_v1", "test_msa_v1", "test_msa_v1"],
        }
    ).to_parquet(msa_dir / "msa_county_membership__test_msa_v1.parquet")

    panel = aggregate_covariate_source(
        "prism_tmin_january",
        curated_path=curated,
        output_dir=tmp_path,
        years=[2024],
        target_geo="msa",
        msa_definition_version="test_msa_v1",
        county_population_path=population,
        data_root=data_root,
        force=True,
    )

    result = pd.read_parquet(panel)
    by_msa = result.set_index("msa_id")
    assert by_msa.loc["11111", "tmin_c"] == pytest.approx(7.5)
    assert by_msa.loc["22222", "tmin_c"] == pytest.approx(-2.0)
    assert by_msa.loc["11111", "tmin_code_blue_band"] == pytest.approx(3.3)
    assert by_msa.loc["11111", "population_weight_denominator"] == pytest.approx(400.0)
    provenance = read_provenance(panel)
    assert provenance is not None
    assert provenance.geo_type == "msa"
    assert provenance.extra["target_geo"] == "msa"
    assert provenance.extra["msa_definition_version"] == "test_msa_v1"


def test_cli_lists_covariate_sources_as_json() -> None:
    """Agents can discover expanded covariate support without scraping text."""
    result = runner.invoke(app, ["list", "covariates", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["status"] == "ok"
    assert payload["source_count"] == len(EXPECTED_COVARIATE_SOURCES)
    source_ids = {source["source_id"] for source in payload["sources"]}
    assert source_ids == set(EXPECTED_COVARIATE_SOURCES)


def test_cli_ingest_and_aggregate_covariate(tmp_path: Path, monkeypatch) -> None:
    """CLI commands expose ingest and aggregate with structured output."""
    monkeypatch.setattr("hhplab.covariates.ingest.register_source", lambda **_: None)
    raw = tmp_path / "fmr.csv"
    pd.DataFrame(
        {
            "county_fips": ["01001"],
            "year": [2024],
            "fmr_0br": [750],
            "fmr_1br": [850],
            "fmr_2br": [1000],
            "fmr_3br": [1250],
            "fmr_4br": [1500],
        }
    ).to_csv(raw, index=False)

    ingest_result = runner.invoke(
        app,
        [
            "ingest",
            "covariate",
            "--source",
            "hud_fmr",
            "--raw-path",
            str(raw),
            "--output-dir",
            str(tmp_path),
            "--force",
            "--json",
        ],
    )
    assert ingest_result.exit_code == 0
    ingest_payload = json.loads(ingest_result.output)
    assert ingest_payload["status"] == "ok"
    assert ingest_payload["source_id"] == "hud_fmr"

    aggregate_result = runner.invoke(
        app,
        [
            "aggregate",
            "covariate",
            "--source",
            "hud_fmr",
            "--curated-path",
            ingest_payload["output_path"],
            "--output-dir",
            str(tmp_path),
            "--years",
            "2024",
            "--target-geo",
            "county",
            "--force",
            "--json",
        ],
    )
    assert aggregate_result.exit_code == 0
    aggregate_payload = json.loads(aggregate_result.output)
    assert aggregate_payload["status"] == "ok"
    assert aggregate_payload["target_geo"] == "county"
    assert aggregate_payload["row_count"] == 1


def test_cli_aggregate_county_covariate_rejects_default_coc_target(
    tmp_path: Path, monkeypatch
) -> None:
    """County-native sources must not silently masquerade as CoC panel data."""
    monkeypatch.setattr("hhplab.covariates.ingest.register_source", lambda **_: None)
    raw = tmp_path / "fmr.csv"
    pd.DataFrame(
        {
            "county_fips": ["01001"],
            "year": [2024],
            "fmr_0br": [750],
            "fmr_1br": [850],
            "fmr_2br": [1000],
            "fmr_3br": [1250],
            "fmr_4br": [1500],
        }
    ).to_csv(raw, index=False)
    curated = ingest_covariate_source(
        "hud_fmr",
        raw,
        output_dir=tmp_path,
        force=True,
    )

    result = runner.invoke(
        app,
        [
            "aggregate",
            "covariate",
            "--source",
            "hud_fmr",
            "--curated-path",
            str(curated),
            "--output-dir",
            str(tmp_path),
            "--years",
            "2024",
            "--json",
        ],
    )

    assert result.exit_code == 2
    payload = json.loads(result.output)
    assert payload["status"] == "error"
    assert "native to county geography" in payload["message"]
    assert "--target-geo county" in payload["message"]


def test_cli_aggregate_prism_covariate_to_msa(tmp_path: Path) -> None:
    """CLI exposes population-weighted MSA output for county-native PRISM covariates."""
    curated = tmp_path / "prism.parquet"
    pd.DataFrame(
        {
            "geo_type": ["county", "county"],
            "geo_id": ["01001", "01003"],
            "county_fips": ["01001", "01003"],
            "year": [2024, 2024],
            "tmin_c": [1.0, 5.0],
        }
    ).to_parquet(curated)
    population = tmp_path / "pep.parquet"
    pd.DataFrame(
        {
            "county_fips": ["01001", "01003"],
            "year": [2024, 2024],
            "population": [25.0, 75.0],
        }
    ).to_parquet(population)
    data_root = tmp_path / "data"
    msa_dir = data_root / "curated" / "msa"
    msa_dir.mkdir(parents=True)
    pd.DataFrame(
        {
            "msa_id": ["11111", "11111"],
            "county_fips": ["01001", "01003"],
        }
    ).to_parquet(msa_dir / "msa_county_membership__test_msa_v1.parquet")

    result = runner.invoke(
        app,
        [
            "aggregate",
            "covariate",
            "--source",
            "prism_tmin_january",
            "--curated-path",
            str(curated),
            "--output-dir",
            str(tmp_path),
            "--years",
            "2024",
            "--target-geo",
            "msa",
            "--msa-definition-version",
            "test_msa_v1",
            "--county-population-path",
            str(population),
            "--data-root",
            str(data_root),
            "--force",
            "--json",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["status"] == "ok"
    assert payload["target_geo"] == "msa"
    assert payload["msa_definition_version"] == "test_msa_v1"
    panel = pd.read_parquet(payload["output_path"])
    assert panel.loc[0, "tmin_c"] == pytest.approx(4.0)
