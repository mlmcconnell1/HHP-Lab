"""Tests for canonical schema contracts and population lineage."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pandas as pd
import pytest
from typer.testing import CliRunner

from hhplab.acs.variables import (
    COUNT_COLUMNS,
    DERIVED_COLUMNS,
    MEDIAN_COLUMNS,
    MOE_COLUMNS,
    TRACT_OUTPUT_COLUMNS,
)
from hhplab.bls.laus_series import (
    LAUS_MEASURE_CODES as BLS_LAUS_MEASURE_CODES,
)
from hhplab.bls.laus_series import (
    LAUS_METRO_OUTPUT_COLUMNS as BLS_LAUS_METRO_OUTPUT_COLUMNS,
)
from hhplab.cli.main import app
from hhplab.panel.conformance import PanelRequest, run_conformance
from hhplab.pep.pep_ingest import PEP_COUNTY_OUTPUT_COLUMNS as PEP_INGEST_COLUMNS
from hhplab.pit.ingest.parser import CANONICAL_COLUMNS as PIT_PARSER_COLUMNS
from hhplab.recipe.executor import _normalize_recipe_population_measure
from hhplab.recipe.planner import ResampleTask
from hhplab.recipe.schema_common import GeometryRef
from hhplab.rents.zori_ingest import ZORI_INGEST_OUTPUT_COLUMNS as ZORI_INGEST_COLUMNS
from hhplab.schema import SAE_OUTPUT_CONTRACT, validate_artifact_contract
from hhplab.schema import columns as schema_columns
from hhplab.schema.lineage import (
    PopulationLineage,
    PopulationMethod,
    PopulationSource,
    normalize_population_measure,
)
from hhplab.xwalks.tract_mediated import (
    DENOMINATOR_COLUMNS as XWALK_DENOMINATOR_COLUMNS,
)
from hhplab.xwalks.tract_mediated import (
    WEIGHT_COLUMNS as XWALK_WEIGHT_COLUMNS,
)

runner = CliRunner()

SCHEMA_ALIAS_CASES = {
    "acs_count_columns": (COUNT_COLUMNS, schema_columns.ACS5_COUNT_COLUMNS),
    "acs_median_columns": (MEDIAN_COLUMNS, schema_columns.ACS5_MEDIAN_COLUMNS),
    "acs_moe_columns": (MOE_COLUMNS, schema_columns.ACS5_MOE_COLUMNS),
    "acs_derived_columns": (DERIVED_COLUMNS, schema_columns.ACS5_DERIVED_COLUMNS),
    "acs_tract_output": (TRACT_OUTPUT_COLUMNS, schema_columns.ACS_TRACT_OUTPUT_COLUMNS),
    "pep_county_output": (PEP_INGEST_COLUMNS, schema_columns.PEP_COUNTY_OUTPUT_COLUMNS),
    "pit_canonical_output": (PIT_PARSER_COLUMNS, schema_columns.PIT_CANONICAL_COLUMNS),
    "zori_ingest_output": (ZORI_INGEST_COLUMNS, schema_columns.ZORI_INGEST_OUTPUT_COLUMNS),
    "laus_measure_codes": (BLS_LAUS_MEASURE_CODES, schema_columns.LAUS_MEASURE_CODES),
    "laus_metro_output": (
        BLS_LAUS_METRO_OUTPUT_COLUMNS,
        schema_columns.LAUS_METRO_OUTPUT_COLUMNS,
    ),
    "tract_mediated_denominators": (
        XWALK_DENOMINATOR_COLUMNS,
        schema_columns.TRACT_MEDIATED_DENOMINATOR_COLUMNS,
    ),
    "tract_mediated_weights": (
        XWALK_WEIGHT_COLUMNS,
        schema_columns.TRACT_MEDIATED_WEIGHT_COLUMNS,
    ),
}


@pytest.mark.parametrize("case_name", list(SCHEMA_ALIAS_CASES), ids=list(SCHEMA_ALIAS_CASES))
def test_source_schema_aliases_use_canonical_schema_constants(case_name: str) -> None:
    source_constant, schema_constant = SCHEMA_ALIAS_CASES[case_name]

    assert source_constant is schema_constant


def test_sae_schema_constants_define_outputs_lineage_and_diagnostics() -> None:
    assert "sae_rent_burden_30_plus" in schema_columns.SAE_MEASURE_COLUMNS
    assert "sae_household_income_median" in schema_columns.SAE_DERIVED_MEASURE_COLUMNS
    assert "sae_crosswalk_id" in schema_columns.SAE_LINEAGE_COLUMNS
    assert "sae_direct_county_comparable" in schema_columns.SAE_DIAGNOSTIC_COLUMNS
    assert schema_columns.SAE_OUTPUT_COLUMNS == [
        "geo_type",
        "geo_id",
        "year",
        *schema_columns.SAE_LINEAGE_COLUMNS,
        *schema_columns.SAE_MEASURE_COLUMNS,
        *schema_columns.SAE_DIAGNOSTIC_COLUMNS,
    ]


def test_validate_sae_output_contract_passes_for_complete_artifact() -> None:
    row = {column: pd.NA for column in schema_columns.SAE_OUTPUT_COLUMNS}
    row.update(
        {
            "geo_type": "coc",
            "geo_id": "COC-A",
            "year": 2023,
            "acs1_vintage_used": "2023",
            "acs5_vintage_used": "2022",
            "tract_vintage_used": "2020",
            "sae_allocation_method": "tract_share_within_county",
            "sae_denominator_source": "acs5_tract_support",
            "sae_crosswalk_id": "tract2020_to_coc2025",
            "sae_rent_burden_30_plus": 0.4,
            "sae_direct_county_comparable": True,
        }
    )

    findings = validate_artifact_contract(pd.DataFrame([row]), SAE_OUTPUT_CONTRACT)

    assert findings == []


def test_validate_sae_output_contract_reports_missing_lineage_column() -> None:
    row = {column: pd.NA for column in schema_columns.SAE_OUTPUT_COLUMNS}
    del row["sae_crosswalk_id"]

    findings = validate_artifact_contract(pd.DataFrame([row]), SAE_OUTPUT_CONTRACT)

    assert [finding.code for finding in findings] == ["missing_required_column"]
    assert findings[0].column == "sae_crosswalk_id"


def test_validate_schema_contract_json_passes_for_source_artifact(tmp_path) -> None:
    path = tmp_path / "zori.parquet"
    pd.DataFrame(
        {
            "geo_type": ["county"],
            "geo_id": ["08001"],
            "date": [pd.Timestamp("2024-01-01")],
            "year": [2024],
            "month": [1],
            "zori": [1200.0],
            "region_name": ["Adams County"],
            "state": ["CO"],
            "data_source": ["Zillow Economic Research"],
            "metric": ["ZORI"],
            "ingested_at": [pd.Timestamp("2026-01-01T00:00:00Z")],
            "source_ref": ["https://example.com"],
            "raw_sha256": ["abc123"],
        }
    ).to_parquet(path)

    result = runner.invoke(
        app,
        [
            "validate",
            "schema-contract",
            str(path),
            "--artifact-type",
            "zori_ingest",
            "--json",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["status"] == "ok"
    assert payload["findings"] == []
    assert payload["exit_behavior"]["warnings"] == "reported_only"


def test_validate_schema_contract_json_reports_warnings_without_failing(tmp_path) -> None:
    path = tmp_path / "panel.parquet"
    pd.DataFrame(
        {
            "coc_id": ["CO-500"],
            "year": [2024],
            "pit_total": [10],
            "pit_sheltered": [8],
            "pit_unsheltered": [2],
            "boundary_vintage_used": ["2024"],
            "acs5_vintage_used": ["2023"],
            "tract_vintage_used": ["2020"],
            "alignment_type": ["native"],
            "weighting_method": ["population"],
            "total_population": [1000],
            "population_density_per_sq_km": [100.0],
            "adult_population": [800],
            "population_below_poverty": [100],
            "median_household_income": [65000],
            "median_gross_rent": [1400],
            "unemployment_rate": [0.05],
            "coverage_ratio": [1.0],
            "boundary_changed": [False],
            "source": ["fixture"],
            "population": [999],
        }
    ).to_parquet(path)

    result = runner.invoke(
        app,
        [
            "validate",
            "schema-contract",
            str(path),
            "--artifact-type",
            "coc_panel",
            "--json",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["status"] == "ok"
    assert payload["warning_count"] == 2
    assert {finding["code"] for finding in payload["findings"]} == {
        "drift_prone_column",
        "missing_lineage_columns",
    }


def test_validate_schema_contract_can_promote_warnings_to_errors(tmp_path) -> None:
    path = tmp_path / "panel.parquet"
    pd.DataFrame(
        {
            "coc_id": ["CO-500"],
            "year": [2024],
            "pit_total": [10],
            "pit_sheltered": [8],
            "pit_unsheltered": [2],
            "boundary_vintage_used": ["2024"],
            "acs5_vintage_used": ["2023"],
            "tract_vintage_used": ["2020"],
            "alignment_type": ["native"],
            "weighting_method": ["population"],
            "total_population": [1000],
            "population_density_per_sq_km": [100.0],
            "adult_population": [800],
            "population_below_poverty": [100],
            "median_household_income": [65000],
            "median_gross_rent": [1400],
            "unemployment_rate": [0.05],
            "coverage_ratio": [1.0],
            "boundary_changed": [False],
            "source": ["fixture"],
        }
    ).to_parquet(path)

    result = runner.invoke(
        app,
        [
            "validate",
            "schema-contract",
            str(path),
            "--artifact-type",
            "coc_panel",
            "--warnings-as-errors",
            "--json",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["status"] == "error"
    assert payload["error_count"] == 0
    assert payload["warning_count"] == 1
    assert payload["exit_behavior"]["warnings"] == "nonzero_exit"


def test_validate_schema_contract_allows_crosswalk_geo_key_alias(tmp_path) -> None:
    columns = [
        "coc_id",
        *[
            column
            for column in schema_columns.TRACT_MEDIATED_COUNTY_XWALK_COLUMNS
            if column != "geo_id"
        ],
    ]
    path = tmp_path / "xwalk.parquet"
    pd.DataFrame([{column: "value" for column in columns}]).to_parquet(path)

    result = runner.invoke(
        app,
        [
            "validate",
            "schema-contract",
            str(path),
            "--artifact-type",
            "tract_mediated_county_xwalk",
            "--json",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["status"] == "ok"


def test_population_normalization_adds_controlled_lineage_columns() -> None:
    frame = pd.DataFrame({"geo_id": ["COC1"], "population": [12345]})

    result = normalize_population_measure(
        frame,
        source_column="population",
        lineage=PopulationLineage(
            source=PopulationSource.PEP,
            source_year=2024,
            method=PopulationMethod.AREA_CROSSWALK,
            crosswalk_id="county_to_coc",
            crosswalk_geometry="county_to_coc",
            crosswalk_vintage=2025,
        ),
    )

    assert "population" not in result.columns
    assert result.loc[0, "total_population"] == 12345
    assert result.loc[0, "total_population_source"] == "pep"
    assert result.loc[0, "total_population_source_year"] == "2024"
    assert result.loc[0, "total_population_method"] == "area_crosswalk"
    assert result.loc[0, "total_population_crosswalk_id"] == "county_to_coc"
    assert result.loc[0, "total_population_crosswalk_geometry"] == "county_to_coc"
    assert result.loc[0, "total_population_crosswalk_vintage"] == "2025"


def test_population_lineage_tokens_distinguish_supported_sources_and_methods() -> None:
    assert {source.value for source in PopulationSource} >= {
        "acs5",
        "pep",
        "decennial",
        "block",
    }
    assert {method.value for method in PopulationMethod} >= {
        "area_crosswalk",
        "population_crosswalk",
        "tract_mediated_crosswalk",
        "block_mediated_crosswalk",
    }


def test_recipe_pep_county_to_coc_population_becomes_canonical() -> None:
    task = ResampleTask(
        dataset_id="pep_county",
        year=2024,
        input_path="data/pep.parquet",
        effective_geometry=GeometryRef(type="county", vintage=2025),
        method="aggregate",
        transform_id="county_to_coc",
        to_geometry=GeometryRef(type="coc", vintage=2025),
        measures=["population"],
    )
    ctx = SimpleNamespace(
        recipe=SimpleNamespace(
            datasets={
                "pep_county": SimpleNamespace(provider="census", product="pep"),
            }
        )
    )
    result = _normalize_recipe_population_measure(
        pd.DataFrame({"geo_id": ["COC1"], "year": [2024], "population": [1000.0]}),
        task=task,
        ctx=ctx,
    )

    assert list(result.columns) == [
        "geo_id",
        "year",
        "total_population",
        "total_population_source",
        "total_population_source_year",
        "total_population_method",
        "total_population_crosswalk_id",
        "total_population_crosswalk_geometry",
        "total_population_crosswalk_vintage",
    ]
    assert result.loc[0, "total_population"] == 1000.0
    assert result.loc[0, "total_population_source"] == "pep"


def test_schema_contract_reports_ambiguous_population_and_missing_lineage() -> None:
    panel = pd.DataFrame(
        {
            "coc_id": ["COC1"],
            "year": [2024],
            "total_population": [1000],
            "population": [999],
        }
    )
    report = run_conformance(
        panel,
        PanelRequest(
            start_year=2024,
            end_year=2024,
            measure_columns=["total_population"],
            enforce_schema_contract=True,
        ),
    )

    codes = {
        result.details["code"]
        for result in report.results
        if result.check_name == "check_schema_contract"
    }
    assert codes == {"drift_prone_column", "missing_lineage_columns"}
