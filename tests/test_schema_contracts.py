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
from hhplab.recipe.executor import ExecutorError, _normalize_recipe_population_measure
from hhplab.recipe.executor_panel import _resolve_canonical_population
from hhplab.recipe.planner import ResampleTask
from hhplab.recipe.recipe_schema import PanelPolicy
from hhplab.recipe.schema_common import GeometryRef
from hhplab.rents.zori_ingest import ZORI_INGEST_OUTPUT_COLUMNS as ZORI_INGEST_COLUMNS
from hhplab.schema import (
    ACS1_IMPUTATION_MEASURE_SPECS,
    ACS1_IMPUTATION_OUTPUT_COLUMNS,
    ACS1_IMPUTATION_OUTPUT_CONTRACT,
    ACS1_IMPUTED_POVERTY_SPEC,
    ACS1_IMPUTED_TOTAL_HOUSEHOLDS_SPEC,
    SAE_OUTPUT_CONTRACT,
    ACS1ImputationMeasureSpec,
    acs1_imputation_output_columns,
    validate_artifact_contract,
)
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

ACS1_IMPUTATION_SPEC_CASES = {
    "poverty_rate": ACS1_IMPUTED_POVERTY_SPEC,
    "total_households": ACS1_IMPUTED_TOTAL_HOUSEHOLDS_SPEC,
}

ACS1_IMPUTATION_COMPLETE_ROW = {
    column: pd.NA for column in ACS1_IMPUTATION_OUTPUT_COLUMNS
} | {
    "geo_type": "tract",
    "geo_id": "08031001000",
    "year": 2023,
    "target_geo_type": "county",
    "target_geo_id": "08031",
    "county_fips": "08031",
    "tract_geoid": "08031001000",
    "acs1_vintage_used": "2023",
    "acs5_vintage_used": "2022",
    "tract_vintage_used": "2020",
    "acs1_imputation_method": "acs1_controlled_acs5_tract_share",
    "acs1_imputation_denominator_source": "acs5_tract_support",
    "acs1_imputation_crosswalk_id": "county_to_tract_2020",
    "is_modeled": True,
    "is_synthetic": True,
    "acs1_imputed_population_below_poverty": 125.0,
    "acs1_imputed_poverty_universe": 1000.0,
    "acs1_imputed_poverty_rate": 0.125,
    "acs1_imputed_total_households": 450.0,
    "acs1_imputation_source_county_count": 1,
    "acs1_imputation_tract_count": 24,
    "acs1_imputation_zero_denominator_count": 0,
    "acs1_imputation_missing_support_count": 0,
    "acs1_imputation_validation_abs_diff": 0.0,
    "acs1_imputation_validation_rel_diff": 0.0,
}


@pytest.mark.parametrize("case_name", list(SCHEMA_ALIAS_CASES), ids=list(SCHEMA_ALIAS_CASES))
def test_source_schema_aliases_use_canonical_schema_constants(case_name: str) -> None:
    source_constant, schema_constant = SCHEMA_ALIAS_CASES[case_name]

    assert source_constant is schema_constant


@pytest.mark.parametrize(
    "case_name",
    list(ACS1_IMPUTATION_SPEC_CASES),
    ids=list(ACS1_IMPUTATION_SPEC_CASES),
)
def test_acs1_imputation_measure_specs_are_valid(case_name: str) -> None:
    spec = ACS1_IMPUTATION_SPEC_CASES[case_name]

    spec.validate()
    assert spec.target_geo_type == "tract"
    assert spec.provenance_columns == tuple(schema_columns.ACS1_IMPUTATION_LINEAGE_COLUMNS)
    assert spec.modeled_flag_column in schema_columns.ACS1_IMPUTATION_FLAG_COLUMNS
    assert spec.synthetic_flag_column in schema_columns.ACS1_IMPUTATION_FLAG_COLUMNS


def test_acs1_imputation_rate_spec_declares_numerator_denominator_contract() -> None:
    spec = ACS1_IMPUTED_POVERTY_SPEC

    assert spec.value_kind == "rate"
    assert spec.numerator_source_columns == ("population_below_poverty",)
    assert spec.denominator_source_column == "poverty_universe"
    assert spec.output_columns == (
        "acs1_imputed_population_below_poverty",
        "acs1_imputed_poverty_universe",
        "acs1_imputed_poverty_rate",
    )
    assert spec.zero_denominator_policy == "null_rate"


def test_acs1_imputation_count_spec_has_single_output_without_rate_fields() -> None:
    spec = ACS1_IMPUTED_TOTAL_HOUSEHOLDS_SPEC

    assert spec.value_kind == "count"
    assert spec.output_columns == ("acs1_imputed_total_households",)
    assert spec.numerator_source_columns == ()
    assert spec.denominator_source_column is None
    assert spec.zero_denominator_policy == "zero_count"


def test_acs1_imputation_output_columns_are_declared_from_specs() -> None:
    assert acs1_imputation_output_columns(ACS1_IMPUTATION_MEASURE_SPECS) == (
        ACS1_IMPUTATION_OUTPUT_COLUMNS
    )
    assert len(ACS1_IMPUTATION_OUTPUT_COLUMNS) == len(set(ACS1_IMPUTATION_OUTPUT_COLUMNS))


def test_validate_acs1_imputation_output_contract_passes_for_complete_artifact() -> None:
    findings = validate_artifact_contract(
        pd.DataFrame([ACS1_IMPUTATION_COMPLETE_ROW]),
        ACS1_IMPUTATION_OUTPUT_CONTRACT,
    )

    assert findings == []


@pytest.mark.parametrize(
    "missing_column",
    [
        "acs1_imputed_poverty_rate",
        "tract_geoid",
        "target_geo_id",
        "is_synthetic",
        "acs1_imputation_zero_denominator_count",
    ],
)
def test_validate_acs1_imputation_output_contract_reports_missing_columns(
    missing_column: str,
) -> None:
    row = dict(ACS1_IMPUTATION_COMPLETE_ROW)
    del row[missing_column]

    findings = validate_artifact_contract(
        pd.DataFrame([row]),
        ACS1_IMPUTATION_OUTPUT_CONTRACT,
    )

    assert [finding.code for finding in findings] == ["missing_required_column"]
    assert findings[0].column == missing_column


def test_acs1_imputation_rate_spec_requires_denominator_support() -> None:
    spec = ACS1ImputationMeasureSpec(
        name="bad_rate",
        family="labor",
        target_geo_type="tract",
        value_kind="rate",
        acs1_source_columns=("unemployed_count", "civilian_labor_force"),
        acs5_support_columns=("unemployed_count",),
        numerator_source_columns=("unemployed_count",),
        denominator_source_column="civilian_labor_force",
        numerator_output_column="acs1_imputed_unemployed_count",
        denominator_output_column="acs1_imputed_civilian_labor_force",
        output_column="acs1_imputed_unemployment_rate",
    )

    with pytest.raises(ValueError, match="denominator_source_column"):
        spec.validate()


def test_acs1_imputation_rate_spec_rejects_multiple_numerator_columns() -> None:
    spec = ACS1ImputationMeasureSpec(
        name="bad_rate",
        family="labor",
        target_geo_type="tract",
        value_kind="rate",
        acs1_source_columns=("unemployed_count", "employed_count", "civilian_labor_force"),
        acs5_support_columns=("unemployed_count", "employed_count", "civilian_labor_force"),
        numerator_source_columns=("unemployed_count", "employed_count"),
        denominator_source_column="civilian_labor_force",
        numerator_output_column="acs1_imputed_unemployed_count",
        denominator_output_column="acs1_imputed_civilian_labor_force",
        output_column="acs1_imputed_unemployment_rate",
    )

    with pytest.raises(ValueError, match="exactly one numerator_source_columns"):
        spec.validate()


def test_acs1_imputation_rate_spec_requires_numerator_source_subset() -> None:
    spec = ACS1ImputationMeasureSpec(
        name="bad_rate",
        family="labor",
        target_geo_type="tract",
        value_kind="rate",
        acs1_source_columns=("civilian_labor_force",),
        acs5_support_columns=("unemployed_count", "civilian_labor_force"),
        numerator_source_columns=("unemployed_count",),
        denominator_source_column="civilian_labor_force",
        numerator_output_column="acs1_imputed_unemployed_count",
        denominator_output_column="acs1_imputed_civilian_labor_force",
        output_column="acs1_imputed_unemployment_rate",
    )

    with pytest.raises(ValueError, match="numerator_source_columns"):
        spec.validate()


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


def test_acs_population_gets_canonical_lineage_defaults() -> None:
    panel = pd.DataFrame(
        {
            "geo_id": ["COC1"],
            "year": [2024],
            "acs5_vintage_used": ["2023"],
            "total_population": [1200.0],
        }
    )

    result = _resolve_canonical_population(panel, policy=None)

    assert result.loc[0, "total_population"] == 1200.0
    assert result.loc[0, "total_population_source"] == "acs5"
    assert result.loc[0, "total_population_source_year"] == "2023"
    assert result.loc[0, "total_population_method"] == "native"


def test_multiple_population_sources_require_explicit_canonical_source() -> None:
    panel = pd.DataFrame(
        {
            "geo_id": ["COC1"],
            "year": [2024],
            "acs5_vintage_used": ["2023"],
            "total_population_acs5": [1200.0],
            "total_population_pep": [1300.0],
            "total_population_pep_source": ["pep"],
            "total_population_pep_source_year": ["2024"],
            "total_population_pep_method": ["area_crosswalk"],
        }
    )

    with pytest.raises(ExecutorError, match="multiple population sources"):
        _resolve_canonical_population(panel, policy=None)

    result = _resolve_canonical_population(
        panel,
        policy=PanelPolicy(canonical_population_source="pep"),
    )

    assert result.loc[0, "total_population"] == 1300.0
    assert result.loc[0, "total_population_source"] == "pep"
    assert result.loc[0, "total_population_source_year"] == "2024"
    assert result.loc[0, "total_population_method"] == "area_crosswalk"
    assert result.loc[0, "total_population_acs5"] == 1200.0
    assert result.loc[0, "total_population_pep"] == 1300.0


def test_existing_canonical_with_source_specific_population_still_requires_policy() -> None:
    panel = pd.DataFrame(
        {
            "geo_id": ["COC1"],
            "year": [2024],
            "acs5_vintage_used": ["2023"],
            "total_population": [1200.0],
            "total_population_pep": [1300.0],
        }
    )

    with pytest.raises(ExecutorError, match="multiple population sources"):
        _resolve_canonical_population(panel, policy=None)

    acs_result = _resolve_canonical_population(
        panel,
        policy=PanelPolicy(canonical_population_source="acs5"),
    )
    assert acs_result.loc[0, "total_population"] == 1200.0
    assert acs_result.loc[0, "total_population_source"] == "acs5"

    pep_result = _resolve_canonical_population(
        panel,
        policy=PanelPolicy(canonical_population_source="pep"),
    )
    assert pep_result.loc[0, "total_population"] == 1300.0
    assert pep_result.loc[0, "total_population_source"] == "pep"


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
