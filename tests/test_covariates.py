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
    aggregate_county_covariate_to_msa,
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
from hhplab.provenance import ProvenanceBlock, read_provenance, write_parquet_with_provenance

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

MPI_XLSX_ROW_TRUTH_TABLE = {
    "us_total": {
        "row": [None, "United States", 13_738_000, 1.0],
        "expected": "skipped:us_total",
    },
    "resolved_county": {
        "row": ["California", "Los Angeles County, California", 1_101_000, 0.080176],
        "expected": "written:06037",
    },
    "multi_county": {
        "row": ["Florida", "Miami-Dade-Monroe Counties, Florida", 356_000, 0.025903],
        "expected": "skipped:multi_county_row",
    },
    "hyphenated_county": {
        "row": ["Florida", "Miami-Dade County, Florida", 135_000, 0.009827],
        "expected": "written:12086",
    },
    "unmapped_county": {
        "row": ["Atlantis", "Poseidon County, Atlantis", 5_000, 0.0001],
        "expected": "error:no_resolved_county_fips",
    },
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
    county_rows: list[list[object]] | None = None,
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
    for row in county_rows or [[None, "United States", 13_738_000, 1.0]]:
        county_sheet.append(row)

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


def test_mpi_xlsx_ingest_writes_resolved_counties_with_provenance(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """MPI XLSX ingest writes canonical county rows and records skipped rows."""
    monkeypatch.setattr("hhplab.covariates.ingest.register_source", lambda **_: None)
    workbook_path = _write_mpi_contract_workbook(
        tmp_path,
        county_rows=[
            MPI_XLSX_ROW_TRUTH_TABLE["us_total"]["row"],
            MPI_XLSX_ROW_TRUTH_TABLE["resolved_county"]["row"],
            MPI_XLSX_ROW_TRUTH_TABLE["hyphenated_county"]["row"],
            MPI_XLSX_ROW_TRUTH_TABLE["multi_county"]["row"],
        ],
    )
    county_reference = tmp_path / "pep_county_reference.csv"
    pd.DataFrame(
        {
            "STNAME": ["California", "Florida"],
            "CTYNAME": ["Los Angeles County", "Miami-Dade County"],
            "STATE": [6, 12],
            "COUNTY": [37, 86],
        }
    ).to_csv(county_reference, index=False)

    curated = ingest_covariate_source(
        MPI_SOURCE_ID,
        workbook_path,
        output_dir=tmp_path,
        county_reference_path=county_reference,
        force=True,
    )

    result = pd.read_parquet(curated)
    assert curated.name == "covariate__mpi_unauthorized_immigrants__Y2023-2023.parquet"
    assert result["county_fips"].tolist() == ["06037", "12086"]
    assert result["geo_id"].tolist() == ["06037", "12086"]
    assert result["state_fips"].tolist() == ["06", "12"]
    assert result["unauthorized_immigrant_population"].tolist() == [1_101_000, 135_000]
    assert result["source_sheet"].tolist() == [MPI_COUNTY_SHEET, MPI_COUNTY_SHEET]
    provenance = read_provenance(curated)
    assert provenance is not None
    assert provenance.extra["source_id"] == MPI_SOURCE_ID
    assert provenance.extra["rows_written"] == 2
    assert provenance.extra["skipped_rows"] == 2
    assert provenance.extra["skipped_reasons"] == {"us_total": 1, "multi_county_row": 1}


def test_mpi_xlsx_ingest_rejects_unmapped_counties(tmp_path: Path, monkeypatch) -> None:
    """Unmapped source county labels fail before writing empty curated artifacts."""
    monkeypatch.setattr("hhplab.covariates.ingest.register_source", lambda **_: None)
    workbook_path = _write_mpi_contract_workbook(
        tmp_path,
        county_rows=[MPI_XLSX_ROW_TRUTH_TABLE["unmapped_county"]["row"]],
    )
    county_reference = tmp_path / "pep_county_reference.csv"
    pd.DataFrame(
        {
            "STNAME": ["California"],
            "CTYNAME": ["Los Angeles County"],
            "STATE": [6],
            "COUNTY": [37],
        }
    ).to_csv(county_reference, index=False)

    with pytest.raises(ValueError, match="no county rows with resolved county_fips"):
        ingest_covariate_source(
            MPI_SOURCE_ID,
            workbook_path,
            output_dir=tmp_path,
            county_reference_path=county_reference,
            force=True,
        )


def test_mpi_default_county_reference_uses_configured_raw_root(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Default MPI county references should not depend on the process CWD."""
    monkeypatch.setattr("hhplab.covariates.ingest.register_source", lambda **_: None)
    asset_root = tmp_path / "assets"
    pep_raw = asset_root / "raw" / "pep"
    pep_raw.mkdir(parents=True)
    pd.DataFrame(
        {
            "STNAME": ["California"],
            "CTYNAME": ["Los Angeles County"],
            "STATE": [6],
            "COUNTY": [37],
        }
    ).to_csv(pep_raw / "pep_county__v2020__fixture.csv", index=False)
    workbook_path = _write_mpi_contract_workbook(
        tmp_path,
        county_rows=[MPI_XLSX_ROW_TRUTH_TABLE["resolved_county"]["row"]],
    )
    cwd = tmp_path / "elsewhere"
    cwd.mkdir()
    monkeypatch.setenv("HHPLAB_ASSET_STORE_ROOT", str(asset_root))
    monkeypatch.chdir(cwd)

    curated = ingest_covariate_source(
        MPI_SOURCE_ID,
        workbook_path,
        output_dir=tmp_path,
        force=True,
    )

    result = pd.read_parquet(curated)
    assert result["county_fips"].tolist() == ["06037"]
    provenance = read_provenance(curated)
    assert provenance is not None
    assert provenance.extra["county_reference_path"] == [
        str(pep_raw / "pep_county__v2020__fixture.csv")
    ]


def test_cli_ingests_mpi_xlsx_with_json_warnings(tmp_path: Path, monkeypatch) -> None:
    """CLI reports MPI output path, measures, and skipped workbook rows as JSON."""
    monkeypatch.setattr("hhplab.covariates.ingest.register_source", lambda **_: None)
    workbook_path = _write_mpi_contract_workbook(
        tmp_path,
        county_rows=[
            MPI_XLSX_ROW_TRUTH_TABLE["us_total"]["row"],
            MPI_XLSX_ROW_TRUTH_TABLE["resolved_county"]["row"],
        ],
    )
    county_reference = tmp_path / "pep_county_reference.csv"
    pd.DataFrame(
        {
            "STNAME": ["California"],
            "CTYNAME": ["Los Angeles County"],
            "STATE": [6],
            "COUNTY": [37],
        }
    ).to_csv(county_reference, index=False)

    result = runner.invoke(
        app,
        [
            "ingest",
            "covariate",
            "--source",
            MPI_SOURCE_ID,
            "--raw-path",
            str(workbook_path),
            "--county-reference-path",
            str(county_reference),
            "--output-dir",
            str(tmp_path),
            "--force",
            "--json",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["status"] == "ok"
    assert payload["source_id"] == MPI_SOURCE_ID
    assert payload["row_count"] == 1
    assert payload["measure_columns"] == list(MPI_MEASURE_COLUMNS)
    assert payload["skipped_rows"] == 1
    assert payload["warnings"] == {"us_total": 1}


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


def test_mpi_static_county_covariate_aggregates_to_msa_with_coverage_diagnostics(
    tmp_path: Path,
) -> None:
    """MPI county estimates can be carried to panel years and rolled up to MSA."""
    curated = tmp_path / "covariate__mpi_unauthorized_immigrants__Y2023-2023.parquet"
    pd.DataFrame(
        {
            "geo_type": ["county", "county"],
            "geo_id": ["01001", "01003"],
            "county_fips": ["01001", "01003"],
            "year": [2023, 2023],
            "unauthorized_immigrant_population": [1_000, 300],
            "unauthorized_immigrant_share_of_us_total": [0.01, 0.003],
        }
    ).to_parquet(curated)
    population = tmp_path / "pep_county__v2024.parquet"
    pd.DataFrame(
        {
            "county_fips": ["01001", "01003", "01005"],
            "year": [2024, 2024, 2024],
            "population": [100.0, 300.0, 600.0],
        }
    ).to_parquet(population)
    data_root = tmp_path / "data"
    msa_dir = data_root / "curated" / "msa"
    msa_dir.mkdir(parents=True)
    pd.DataFrame(
        {
            "msa_id": ["11111", "11111", "11111"],
            "county_fips": ["01001", "01003", "01005"],
        }
    ).to_parquet(msa_dir / "msa_county_membership__test_msa_v1.parquet")

    panel = aggregate_covariate_source(
        MPI_SOURCE_ID,
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
    assert result["year"].tolist() == [2024]
    assert result["unauthorized_immigrant_population"].tolist() == pytest.approx([1_300.0])
    assert result["unauthorized_immigrant_share_of_us_total"].tolist() == pytest.approx([0.013])
    assert result["source_estimate_year"].tolist() == [2023]
    assert result["coverage_ratio"].tolist() == pytest.approx([2 / 3])
    assert result["population_weight_denominator"].tolist() == pytest.approx([400.0])
    assert result["county_count"].tolist() == [2]
    assert result["membership_county_count"].tolist() == [3]
    provenance = read_provenance(panel)
    assert provenance is not None
    assert provenance.extra["measure_aggregations"] == {
        "unauthorized_immigrant_population": "extensive_sum",
        "unauthorized_immigrant_share_of_us_total": "extensive_sum",
    }
    assert provenance.extra["static_year_policy"] == {
        "policy": "carry_forward_static_estimate_to_requested_years",
        "source_year": 2023,
        "target_years": [2024],
    }
    assert provenance.extra["coverage_diagnostics"]["partial_target_count"] == 1

    filtered_panel = aggregate_covariate_source(
        MPI_SOURCE_ID,
        curated_path=curated,
        output_dir=tmp_path,
        years=[2024],
        target_geo="msa",
        msa_definition_version="test_msa_v1",
        county_population_path=population,
        data_root=data_root,
        min_coverage_ratio=0.75,
        drop_below_min_coverage=True,
        force=True,
    )
    assert pd.read_parquet(filtered_panel).empty
    filtered_provenance = read_provenance(filtered_panel)
    assert filtered_provenance is not None
    assert filtered_provenance.extra["coverage_policy"]["dropped_row_count"] == 1


def test_mpi_same_msa_multi_county_rows_are_allocated_to_msa(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Recoverable MPI multi-county rows can contribute to MSA aggregation."""
    monkeypatch.setattr("hhplab.covariates.ingest.register_source", lambda **_: None)
    workbook_path = _write_mpi_contract_workbook(
        tmp_path,
        county_rows=[
            ["California", "Los Angeles County, California", 1_101_000, 0.080176],
            ["Colorado", "El Paso-Teller Counties, Colorado", 20_000, 0.001456],
        ],
    )
    county_reference = tmp_path / "pep_county_reference.csv"
    pd.DataFrame(
        {
            "STNAME": ["California", "Colorado", "Colorado"],
            "CTYNAME": ["Los Angeles County", "El Paso County", "Teller County"],
            "STATE": [6, 8, 8],
            "COUNTY": [37, 41, 119],
        }
    ).to_csv(county_reference, index=False)
    curated = ingest_covariate_source(
        MPI_SOURCE_ID,
        workbook_path,
        output_dir=tmp_path,
        county_reference_path=county_reference,
        force=True,
    )
    ingest_provenance = read_provenance(curated)
    assert ingest_provenance is not None
    assert ingest_provenance.extra["skipped_reasons"] == {"multi_county_row": 1}
    assert ingest_provenance.extra["multi_county_rows"][0]["member_county_fips"] == [
        "08041",
        "08119",
    ]

    population = tmp_path / "pep_county__v2024.parquet"
    pd.DataFrame(
        {
            "county_fips": ["06037", "08041", "08119"],
            "year": [2024, 2024, 2024],
            "population": [9_700_000.0, 750_000.0, 25_000.0],
        }
    ).to_parquet(population)
    data_root = tmp_path / "data"
    msa_dir = data_root / "curated" / "msa"
    msa_dir.mkdir(parents=True)
    pd.DataFrame(
        {
            "msa_id": ["31080", "17820", "17820"],
            "county_fips": ["06037", "08041", "08119"],
        }
    ).to_parquet(msa_dir / "msa_county_membership__test_msa_v1.parquet")

    panel = aggregate_covariate_source(
        MPI_SOURCE_ID,
        curated_path=curated,
        output_dir=tmp_path,
        years=[2024],
        target_geo="msa",
        msa_definition_version="test_msa_v1",
        county_population_path=population,
        data_root=data_root,
        force=True,
    )

    result = pd.read_parquet(panel).set_index("msa_id")
    assert set(result.index) == {"17820", "31080"}
    assert result.loc["17820", "unauthorized_immigrant_population"] == pytest.approx(20_000)
    assert result.loc["17820", "unauthorized_immigrant_share_of_us_total"] == pytest.approx(
        0.001456
    )
    assert result.loc["17820", "county_count"] == 2
    assert result.loc["17820", "coverage_ratio"] == pytest.approx(1.0)
    assert result.loc["17820", "mpi_multi_county_source_row_count"] == 1


def test_mpi_multi_county_rows_count_distinct_covered_msa_counties() -> None:
    """MPI multi-county diagnostics count distinct member counties, not source-row width."""
    county = pd.DataFrame(
        {
            "county_fips": ["01001"],
            "year": [2024],
            "unauthorized_immigrant_population": [1_000],
        }
    )
    membership = pd.DataFrame(
        {
            "msa_id": ["11111", "11111"],
            "county_fips": ["01001", "01003"],
        }
    )
    population = pd.DataFrame(
        {
            "county_fips": ["01001", "01003"],
            "year": [2024, 2024],
            "population": [100.0, 300.0],
        }
    )

    result = aggregate_county_covariate_to_msa(
        county,
        measure_columns=["unauthorized_immigrant_population"],
        measure_aggregations={"unauthorized_immigrant_population": "extensive_sum"},
        msa_definition_version="test_msa_v1",
        msa_county_membership=membership,
        county_population=population,
        mpi_multi_county_rows=[
            {
                "year": 2024,
                "member_county_fips": ["01003", "01003"],
                "unauthorized_immigrant_population": 500,
            }
        ],
    )

    row = result.set_index("msa_id").loc["11111"]
    assert row["unauthorized_immigrant_population"] == pytest.approx(1_500)
    assert row["population_weight_denominator"] == pytest.approx(400.0)
    assert row["county_count"] == 2
    assert row["membership_county_count"] == 2
    assert row["coverage_ratio"] == pytest.approx(1.0)
    assert row["mpi_multi_county_source_row_count"] == 1
    assert "_covered_county_fips" not in result.columns


def test_mpi_native_msa_rows_fill_missing_msa_covariate_rows() -> None:
    """Recover MPI native MSA rows when they uniquely match the MSA definition name."""
    county = pd.DataFrame(
        {
            "county_fips": ["06037"],
            "year": [2024],
            "unauthorized_immigrant_population": [1_101_000],
        }
    )
    membership = pd.DataFrame(
        {
            "msa_id": ["31080", "14460", "14460"],
            "county_fips": ["06037", "25025", "25017"],
        }
    )
    definitions = pd.DataFrame(
        {
            "msa_id": ["31080", "14460"],
            "msa_name": [
                "Los Angeles-Long Beach-Anaheim, CA",
                "Boston-Cambridge-Newton, MA-NH",
            ],
        }
    )
    population = pd.DataFrame(
        {
            "county_fips": ["06037", "25025", "25017"],
            "year": [2024, 2024, 2024],
            "population": [9_700_000.0, 700_000.0, 1_600_000.0],
        }
    )

    result = aggregate_county_covariate_to_msa(
        county,
        measure_columns=["unauthorized_immigrant_population"],
        measure_aggregations={"unauthorized_immigrant_population": "extensive_sum"},
        msa_definition_version="test_msa_v1",
        msa_county_membership=membership,
        msa_definitions=definitions,
        county_population=population,
        mpi_msa_rows=[
            {
                "year": 2024,
                "county_label": "Boston-Cambridge-Newton MSA,++ Massachusetts-New Hampshire",
                "unauthorized_immigrant_population": 301_000,
                "source_estimate_year": 2023,
                "static_year_policy": "carry_forward",
            }
        ],
    )

    by_msa = result.set_index("msa_id")
    assert set(by_msa.index) == {"14460", "31080"}
    assert by_msa.loc["14460", "unauthorized_immigrant_population"] == pytest.approx(301_000)
    assert by_msa.loc["14460", "county_count"] == 2
    assert by_msa.loc["14460", "membership_county_count"] == 2
    assert by_msa.loc["14460", "coverage_ratio"] == pytest.approx(1.0)
    assert by_msa.loc["14460", "population_weight_denominator"] == pytest.approx(2_300_000)
    assert by_msa.loc["14460", "mpi_msa_source_row_count"] == 1
    assert by_msa.loc["14460", "source_estimate_year"] == 2023
    assert by_msa.loc["14460", "static_year_policy"] == "carry_forward"


def test_mpi_native_msa_rows_use_state_qualifier_for_duplicate_names() -> None:
    """MPI state-qualified native MSA labels should disambiguate duplicate MSA names."""
    county = pd.DataFrame(
        {
            "county_fips": ["06037"],
            "year": [2024],
            "unauthorized_immigrant_population": [1_101_000],
        }
    )
    membership = pd.DataFrame(
        {
            "msa_id": ["31080", "44140", "44100"],
            "county_fips": ["06037", "25013", "17167"],
        }
    )
    definitions = pd.DataFrame(
        {
            "msa_id": ["31080", "44140", "44100"],
            "msa_name": [
                "Los Angeles-Long Beach-Anaheim, CA",
                "Springfield, MA",
                "Springfield, IL",
            ],
        }
    )
    population = pd.DataFrame(
        {
            "county_fips": ["06037", "25013", "17167"],
            "year": [2024, 2024, 2024],
            "population": [9_700_000.0, 465_000.0, 190_000.0],
        }
    )

    result = aggregate_county_covariate_to_msa(
        county,
        measure_columns=["unauthorized_immigrant_population"],
        measure_aggregations={"unauthorized_immigrant_population": "extensive_sum"},
        msa_definition_version="test_msa_v1",
        msa_county_membership=membership,
        msa_definitions=definitions,
        county_population=population,
        mpi_msa_rows=[
            {
                "year": 2024,
                "county_label": "Springfield MSA,++ Massachusetts",
                "unauthorized_immigrant_population": 11_000,
            }
        ],
    )

    by_msa = result.set_index("msa_id")
    assert set(by_msa.index) == {"31080", "44140"}
    assert "44100" not in by_msa.index
    assert by_msa.loc["44140", "unauthorized_immigrant_population"] == pytest.approx(11_000)
    assert by_msa.loc["44140", "population_weight_denominator"] == pytest.approx(465_000)
    assert by_msa.loc["44140", "coverage_ratio"] == pytest.approx(1.0)
    assert by_msa.loc["44140", "mpi_msa_source_row_count"] == 1


def test_mpi_msa_aggregate_rejects_stale_preview_only_msa_provenance(
    tmp_path: Path,
) -> None:
    """Stale MPI artifacts must not recover native MSA rows from capped previews."""
    curated = tmp_path / "covariate__mpi_unauthorized_immigrants__Y2023-2023.parquet"
    write_parquet_with_provenance(
        pd.DataFrame(
            {
                "geo_type": ["county"],
                "geo_id": ["06037"],
                "county_fips": ["06037"],
                "year": [MPI_ESTIMATE_YEAR],
                **{column: [1.0] for column in MPI_MEASURE_COLUMNS},
            }
        ),
        curated,
        ProvenanceBlock(
            geo_type="county",
            extra={
                "dataset_type": "expanded_covariate",
                "source_id": MPI_SOURCE_ID,
                "native_geo": "county",
                "measure_columns": list(MPI_MEASURE_COLUMNS),
                "skipped_reasons": {"msa_row": 5},
                "skipped_preview": [
                    {
                        "county_label": "Boston-Cambridge-Newton MSA,++ Massachusetts-New Hampshire",
                        "exclusion_reason": "msa_row",
                    }
                ],
            },
        ),
    )

    with pytest.raises(ValueError, match="Re-ingest the MPI covariate"):
        aggregate_covariate_source(
            MPI_SOURCE_ID,
            curated_path=curated,
            output_path=tmp_path / "mpi_msa.parquet",
            target_geo="msa",
            force=True,
        )


def test_mpi_county_covariate_rejects_unsupported_coc_target(tmp_path: Path) -> None:
    """MPI county-native estimates require explicit crosswalk support for CoC output."""
    curated = tmp_path / "covariate__mpi_unauthorized_immigrants__Y2023-2023.parquet"
    pd.DataFrame(
        {
            "geo_type": ["county"],
            "geo_id": ["01001"],
            "county_fips": ["01001"],
            "year": [2023],
            "unauthorized_immigrant_population": [1_000],
            "unauthorized_immigrant_share_of_us_total": [0.01],
        }
    ).to_parquet(curated)

    with pytest.raises(ValueError, match="cannot be emitted as coc panel-ready data"):
        aggregate_covariate_source(
            MPI_SOURCE_ID,
            curated_path=curated,
            output_dir=tmp_path,
            target_geo="coc",
            force=True,
        )


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
    assert aggregate_payload["rebuilt"] is True
    assert aggregate_payload["skipped_existing_output"] is False
    assert aggregate_payload["row_count"] == 1

    cached_result = runner.invoke(
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
            "--json",
        ],
    )
    assert cached_result.exit_code == 0
    cached_payload = json.loads(cached_result.output)
    assert cached_payload["status"] == "ok"
    assert cached_payload["rebuilt"] is False
    assert cached_payload["skipped_existing_output"] is True
    assert cached_payload["output_path"] == aggregate_payload["output_path"]
    assert cached_payload["row_count"] == 1


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
            "county_fips": ["01001", "01003", "01005"],
            "year": [2024, 2024, 2024],
            "population": [25.0, 75.0, 50.0],
        }
    ).to_parquet(population)
    data_root = tmp_path / "data"
    msa_dir = data_root / "curated" / "msa"
    msa_dir.mkdir(parents=True)
    pd.DataFrame(
        {
            "msa_id": ["11111", "11111", "11111"],
            "county_fips": ["01001", "01003", "01005"],
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
    assert payload["coverage_policy"]["below_threshold_count"] == 1
    assert payload["coverage_diagnostics"]["partial_target_count"] == 1
    assert payload["warnings"][0]["code"] == "covariate_msa_partial_coverage"
    panel = pd.read_parquet(payload["output_path"])
    assert panel.loc[0, "tmin_c"] == pytest.approx(4.0)
    assert panel.loc[0, "coverage_ratio"] == pytest.approx(2 / 3)
