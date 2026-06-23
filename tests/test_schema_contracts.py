"""Tests for canonical schema contracts and population lineage."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pandas as pd
import pytest
from typer.testing import CliRunner

from hhplab.acs.acs_aggregate import AVERAGE_WEIGHT_DENOMINATORS
from hhplab.acs.variables import (
    ACS5_COVARIATE_REGISTRY,
    ACS5_COVARIATE_REGISTRY_BY_OUTPUT,
    ACS5_EXPANDED_COVARIATE_COLUMNS,
    ACS_TABLES,
    ACS_VARIABLES,
    COUNT_COLUMNS,
    DERIVED_COLUMNS,
    MEDIAN_COLUMNS,
    MOE_COLUMNS,
    TRACT_OUTPUT_COLUMNS,
    acs5_covariate_spec_for_output,
    acs_variables_for_year,
)
from hhplab.acs.variables_acs1 import (
    ACS1_TABLE_COLUMN_NAMES,
    ACS1_VARIABLES_BY_TABLE,
    acs1_variable_names_for_vintage,
    acs1_variables_by_table_for_vintage,
)
from hhplab.bls.laus_series import (
    LAUS_MEASURE_CODES as BLS_LAUS_MEASURE_CODES,
)
from hhplab.bls.laus_series import (
    LAUS_METRO_OUTPUT_COLUMNS as BLS_LAUS_METRO_OUTPUT_COLUMNS,
)
from hhplab.cli.main import app
from hhplab.hic.parser import CANONICAL_COLUMNS as HIC_PARSER_COLUMNS
from hhplab.panel.conformance import PanelRequest, run_conformance
from hhplab.pep.pep_ingest import PEP_COUNTY_OUTPUT_COLUMNS as PEP_INGEST_COLUMNS
from hhplab.pit.ingest.parser import CANONICAL_COLUMNS as PIT_PARSER_COLUMNS
from hhplab.recipe.executor import ExecutorError, _normalize_recipe_population_measure
from hhplab.recipe.executor.panel import _resolve_canonical_population
from hhplab.recipe.planner import ResampleTask
from hhplab.recipe.recipe_schema import (
    ACS5_RECIPE_DEFAULT_MEASURES,
    ACS5_RECIPE_SELECTABLE_MEASURES,
    PanelPolicy,
)
from hhplab.recipe.schema_common import GeometryRef
from hhplab.rents.zori_ingest import ZORI_INGEST_OUTPUT_COLUMNS as ZORI_INGEST_COLUMNS
from hhplab.schema import (
    ACS1_IMPUTATION_MEASURE_SPECS,
    ACS1_IMPUTATION_OUTPUT_COLUMNS,
    ACS1_IMPUTATION_OUTPUT_CONTRACT,
    ACS1_IMPUTED_POVERTY_SPEC,
    ACS1_IMPUTED_TOTAL_HOUSEHOLDS_SPEC,
    ARTIFACT_CONTRACTS,
    COC_URBAN_AREA_DETAIL_COLUMNS,
    COC_URBAN_AREA_DETAIL_CONTRACT,
    COC_URBAN_FRACTION_COLUMNS,
    COC_URBAN_FRACTION_CONTRACT,
    MSA_COC_COVERAGE_COLUMNS,
    MSA_COC_COVERAGE_CONTRACT,
    MSA_COC_PANEL_COLUMNS,
    MSA_COC_PANEL_CONTRACT,
    MSA_FRACTIONAL_ROLLUP_COLUMNS,
    MSA_FRACTIONAL_ROLLUP_CONTRACT,
    PL_BLOCK_POPULATION_COLUMNS,
    PL_BLOCK_POPULATION_CONTRACT,
    SAE_OUTPUT_CONTRACT,
    URBAN_AREA_GEOMETRY_COLUMNS,
    URBAN_AREA_GEOMETRY_CONTRACT,
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
    "hic_canonical_output": (HIC_PARSER_COLUMNS, schema_columns.HIC_CANONICAL_COLUMNS),
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

ACS5_REGISTRY_REQUIRED_FIELDS = (
    "name",
    "table",
    "source_variables",
    "output_columns",
    "measure_kind",
    "weight_column",
    "rollup_method",
    "caveats",
)

ACS1_IMPUTATION_SPEC_CASES = {
    "poverty_rate": ACS1_IMPUTED_POVERTY_SPEC,
    "total_households": ACS1_IMPUTED_TOTAL_HOUSEHOLDS_SPEC,
}

ACS1_IMPUTATION_COMPLETE_ROW = {column: pd.NA for column in ACS1_IMPUTATION_OUTPUT_COLUMNS} | {
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

HIC_EXPANDED_SCHEMA_TRUTH_TABLE = (
    {
        "case": "2010-2012 aggregate shelter plus psh",
        "year": 2010,
        "source_headers": (
            "total_year_round_beds_es_th_sh",
            "total_units_for_households_with_children_es_th",
            "total_year_round_psh_beds",
            "total_psh_units_for_households_with_children",
        ),
        "present_project_types": ("psh",),
        "historically_absent_project_types": ("es", "th", "sh", "rrh", "oph"),
        "shelter_total_source": "published_aggregate",
        "all_program_total_source": "derived",
    },
    {
        "case": "2013 aggregate shelter plus rrh and psh",
        "year": 2013,
        "source_headers": (
            "total_year_round_beds_es_th_rrh_sh",
            "total_units_for_households_with_children_es_th_rrh",
            "total_year_round_rrh_beds",
            "total_rrh_units_for_households_with_children",
            "total_year_round_psh_beds",
            "total_psh_units_for_households_with_children",
        ),
        "present_project_types": ("rrh", "psh"),
        "historically_absent_project_types": ("es", "th", "sh", "oph"),
        "shelter_total_source": "published_aggregate_without_rrh",
        "all_program_total_source": "derived",
    },
    {
        "case": "2014+ full project type split",
        "year": 2014,
        "source_headers": (
            "total_year_round_beds_es",
            "total_year_round_beds_th",
            "total_year_round_beds_sh",
            "total_year_round_beds_rrh",
            "total_year_round_beds_psh",
            "total_year_round_beds_oph",
            "total_units_for_households_with_children_es",
            "total_units_for_households_with_children_th",
            "total_units_for_households_with_children_sh",
            "total_units_for_households_with_children_rrh",
            "total_units_for_households_with_children_psh",
            "total_units_for_households_with_children_oph",
        ),
        "present_project_types": ("es", "th", "sh", "rrh", "psh", "oph"),
        "historically_absent_project_types": (),
        "shelter_total_source": "derived",
        "all_program_total_source": "derived",
    },
)

MSA_COC_PANEL_COMPLETE_ROW = {
    "msa_id": "35620",
    "coc_id": "NY-600",
    "year": 2024,
    "msa_population": 19_498_249,
    "msa_median_rent": 1732.0,
    "msa_vacancy_rate": 0.054,
    "msa_poverty_rate": 0.128,
    "msa_unemployment": 0.041,
    "msa_income": 92_153.0,
    "msa_rent_burden": 0.462,
    "pit_total": 88_025,
    "pit_sheltered": 83_169,
    "pit_unsheltered": 4_856,
    "coc_population": 8_258_035,
    "ranking_population_source": "pep",
    "ranking_reference_year": 2024,
    "containment_min_share": 0.99,
    "containment_denominator": "candidate_area",
    "coc_boundary_vintage_used": 2025,
    "msa_definition_version_used": "census_msa_2023",
    "msa_population_source": "acs5",
    "unemployment_source": "laus",
    "source": "msa_coc_panel",
}

MSA_COC_COVERAGE_COMPLETE_ROW = {
    "msa_id": "35620",
    "msa_name": "New York-Newark-Jersey City, NY-NJ-PA",
    "year": 2024,
    "coc_id": "NY-600",
    "coc_name": "New York City CoC",
    "overlap_basis": "population",
    "denominator_source": "acs5",
    "denominator_vintage": "2023",
    "denominator_column": "total_population",
    "msa_covered_by_coc_percent": 41.8,
    "coc_contained_in_msa_percent": 99.2,
    "intersection_value": 8_250_000.0,
    "msa_denominator": 19_750_000.0,
    "coc_denominator": 8_316_000.0,
    "boundary_vintage": "2025",
    "county_vintage": "2023",
    "definition_version": "census_msa_2023",
    "top_n": 100,
    "ranking_population_source": "pep",
    "ranking_reference_year": 2024,
}

MSA_FRACTIONAL_ROLLUP_COMPLETE_ROW = {
    "msa_id": "35620",
    "year": 2024,
    "source_dataset_id": "pit_coc",
    "source_geo_type": "coc",
    "source_additive_measure_columns": "pit_total,pit_sheltered,pit_unsheltered",
    "native_msa_covariate_columns": "msa_population",
    "allocation_basis": "block_population",
    "denominator_source": "pl_94_171_block_population",
    "boundary_vintage": "2025",
    "county_vintage": "2023",
    "block_vintage": "2020",
    "decennial_vintage": "2020",
    "msa_definition_version": "census_msa_2023",
    "coc_count": 12,
    "covered_coc_count": 11,
    "missing_coc_count": 1,
    "missing_cocs": "CO-200",
    "unmapped_source_coc_count": 1,
    "unmapped_source_cocs": "CO-999",
    "unmapped_source_additive_measure_totals": (
        '{"pit_sheltered":100.0,"pit_total":125.0,"pit_unsheltered":25.0}'
    ),
    "zero_population_coc_count": 0,
    "missing_population_block_count": 3,
    "min_coc_population_containment_share": 0.5,
    "min_msa_population_coverage_share": 0.9,
    "min_allocation_share": 0.01,
    "coc_population_coverage_ratio": 0.98,
    "msa_population_coverage_ratio": 0.95,
    "allocation_share_sum": 1.0,
    "expected_allocation_share_sum": 1.0,
    "source": "msa_fractional_rollup",
    "pit_total": 88025.0,
    "pit_sheltered": 83169.0,
    "pit_unsheltered": 4856.0,
}

URBAN_CONTRACT_CASES = {
    "urban_area_geometry": (
        URBAN_AREA_GEOMETRY_CONTRACT,
        URBAN_AREA_GEOMETRY_COLUMNS,
        {
            "urban_area_geoid": "78904",
            "urban_area_name": "Denver-Aurora, CO",
            "urban_area_type": "urbanized_area",
            "urban_area_vintage": 2020,
            "data_source": "census_tiger",
            "source_ref": "tl_2020_us_uac20.zip",
            "ingested_at": "2026-05-31T00:00:00Z",
            "geometry": None,
        },
        "urban_area_geoid",
    ),
    "pl_block_population": (
        PL_BLOCK_POPULATION_CONTRACT,
        PL_BLOCK_POPULATION_COLUMNS,
        {
            "block_geoid": "080310001001000",
            "state_fips": "08",
            "county_fips": "08031",
            "tract_geoid": "08031000100",
            "block_vintage": 2020,
            "decennial_vintage": 2020,
            "total_population": 42,
            "data_source": "census_pl_94_171",
            "source_ref": "2020-pl-blocks",
            "ingested_at": "2026-05-31T00:00:00Z",
        },
        "block_geoid",
    ),
    "coc_urban_fraction": (
        COC_URBAN_FRACTION_CONTRACT,
        COC_URBAN_FRACTION_COLUMNS,
        {
            "coc_id": "CO-503",
            "boundary_vintage": 2025,
            "urban_area_vintage": 2020,
            "block_vintage": 2020,
            "decennial_vintage": 2020,
            "denominator_source": "pl_94_171_block_population",
            "allocation_method": "block_centroid_with_area_fallback",
            "classification_method": "census_urban_area_intersection",
            "coc_total_population": 1000.0,
            "coc_urban_population": 850.0,
            "coc_rural_population": 150.0,
            "urban_population_fraction": 0.85,
            "block_count": 120,
            "urban_block_count": 100,
            "rural_block_count": 20,
            "missing_denominator_block_count": 0,
            "population_coverage_ratio": 1.0,
            "source": "coc_urban_fraction",
        },
        "classification_method",
    ),
    "coc_urban_area_detail": (
        COC_URBAN_AREA_DETAIL_CONTRACT,
        COC_URBAN_AREA_DETAIL_COLUMNS,
        {
            "coc_id": "CO-503",
            "urban_area_geoid": "78904",
            "urban_area_name": "Denver-Aurora, CO",
            "boundary_vintage": 2025,
            "urban_area_vintage": 2020,
            "block_vintage": 2020,
            "decennial_vintage": 2020,
            "denominator_source": "pl_94_171_block_population",
            "allocation_method": "block_centroid_with_area_fallback",
            "classification_method": "census_urban_area_intersection",
            "total_population": 850.0,
            "urban_population": 850.0,
            "urban_population_fraction": 1.0,
            "block_count": 100,
            "source": "coc_urban_area_detail",
        },
        "urban_area_geoid",
    ),
}


@pytest.mark.parametrize("case_name", list(SCHEMA_ALIAS_CASES), ids=list(SCHEMA_ALIAS_CASES))
def test_source_schema_aliases_use_canonical_schema_constants(case_name: str) -> None:
    source_constant, schema_constant = SCHEMA_ALIAS_CASES[case_name]

    assert source_constant is schema_constant


def test_hic_expanded_schema_defines_canonical_project_type_columns() -> None:
    assert schema_columns.HIC_PROJECT_TYPES == ("es", "th", "sh", "rrh", "psh", "oph")
    assert schema_columns.HIC_EXPANDED_BED_MEASURE_COLUMNS == (
        "hic_es_year_round_beds",
        "hic_th_year_round_beds",
        "hic_sh_year_round_beds",
        "hic_rrh_year_round_beds",
        "hic_psh_year_round_beds",
        "hic_oph_year_round_beds",
    )
    assert schema_columns.HIC_EXPANDED_UNIT_MEASURE_COLUMNS == (
        "hic_es_family_units",
        "hic_th_family_units",
        "hic_sh_family_units",
        "hic_rrh_family_units",
        "hic_psh_family_units",
        "hic_oph_family_units",
    )
    assert schema_columns.HIC_DERIVED_MEASURE_COLUMNS == (
        "hic_shelter_year_round_beds",
        "hic_shelter_family_units",
        "hic_total_beds",
        "hic_total_units",
    )
    assert schema_columns.HIC_EXPANDED_MEASURE_COLUMNS == (
        *schema_columns.HIC_EXPANDED_BED_MEASURE_COLUMNS,
        *schema_columns.HIC_EXPANDED_UNIT_MEASURE_COLUMNS,
        *schema_columns.HIC_DERIVED_MEASURE_COLUMNS,
    )


def test_hic_expanded_schema_documents_total_semantics_and_missing_values() -> None:
    by_column = {
        str(spec["column"]): spec for spec in schema_columns.HIC_EXPANDED_MEASURE_SCHEMA
    }

    assert by_column["hic_shelter_year_round_beds"]["project_type"] == "es_th_sh"
    assert "ES + TH + SH" in str(by_column["hic_shelter_year_round_beds"]["semantics"])
    assert by_column["hic_total_beds"]["project_type"] == "all_programs"
    assert "ES + TH + SH + RRH + PSH + OPH" in str(
        by_column["hic_total_beds"]["semantics"]
    )
    for column in schema_columns.HIC_EXPANDED_MEASURE_COLUMNS:
        assert "historical_missing" in by_column[column]
        assert by_column[column]["aggregation"] == "sum"
    for project_type in ("es", "th", "sh", "rrh", "oph"):
        spec = by_column[f"hic_{project_type}_year_round_beds"]
        assert "filled as 0" in str(spec["historical_missing"]).lower()


@pytest.mark.parametrize(
    "case",
    HIC_EXPANDED_SCHEMA_TRUTH_TABLE,
    ids=[str(case["case"]) for case in HIC_EXPANDED_SCHEMA_TRUTH_TABLE],
)
def test_hic_expanded_schema_truth_table_covers_historical_header_shapes(
    case: dict[str, object],
) -> None:
    by_column = {
        str(spec["column"]): spec for spec in schema_columns.HIC_EXPANDED_MEASURE_SCHEMA
    }
    year = int(case["year"])
    present = set(case["present_project_types"])
    absent = set(case["historically_absent_project_types"])

    assert present | absent == set(schema_columns.HIC_PROJECT_TYPES)
    assert present.isdisjoint(absent)
    assert case["all_program_total_source"] == "derived"
    assert case["source_headers"]

    for project_type in present:
        first_year = by_column[f"hic_{project_type}_year_round_beds"][
            "first_distinct_hud_year"
        ]
        assert int(first_year) <= year

    for project_type in absent:
        first_year = by_column[f"hic_{project_type}_year_round_beds"][
            "first_distinct_hud_year"
        ]
        assert int(first_year) > year or project_type in {"es", "th", "sh"}


@pytest.mark.parametrize(
    "field_name",
    ACS5_REGISTRY_REQUIRED_FIELDS,
    ids=ACS5_REGISTRY_REQUIRED_FIELDS,
)
def test_acs5_covariate_registry_declares_required_metadata(field_name: str) -> None:
    missing = [spec.name for spec in ACS5_COVARIATE_REGISTRY if not getattr(spec, field_name)]

    assert missing == []


def test_acs5_covariate_registry_owns_supported_outputs_once() -> None:
    registry_outputs = [
        column for spec in ACS5_COVARIATE_REGISTRY for column in spec.output_columns
    ]
    duplicate_outputs = sorted(
        column for column in set(registry_outputs) if registry_outputs.count(column) > 1
    )
    expected_outputs = (
        set(COUNT_COLUMNS) | set(MEDIAN_COLUMNS) | set(MOE_COLUMNS) | set(DERIVED_COLUMNS)
    )

    assert duplicate_outputs == []
    assert expected_outputs <= set(registry_outputs)
    assert set(ACS5_COVARIATE_REGISTRY_BY_OUTPUT) == set(registry_outputs)


def test_acs5_covariate_registry_references_known_tables_and_source_variables() -> None:
    known_source_variables = set(ACS_VARIABLES) | {
        variable
        for spec in ACS5_COVARIATE_REGISTRY
        if spec.table == "B01001"
        for variable in spec.source_variables
    }

    invalid_tables = sorted(
        {spec.table for spec in ACS5_COVARIATE_REGISTRY if spec.table not in ACS_TABLES}
    )
    invalid_sources = sorted(
        {
            variable
            for spec in ACS5_COVARIATE_REGISTRY
            for variable in spec.source_variables
            if variable not in known_source_variables
        }
    )

    assert invalid_tables == []
    assert invalid_sources == []


def test_contract_rent_acs5_variable_mappings_include_current_and_early_bins() -> None:
    current = acs_variables_for_year(2023)
    early = acs_variables_for_year(2014)

    assert "B25056" in ACS_TABLES
    assert "B25058" in ACS_TABLES
    assert current["B25056_001E"] == "contract_rent_distribution_total"
    assert current["B25056_002E"] == "contract_rent_distribution_with_cash_rent"
    assert current["B25056_026E"] == "contract_rent_distribution_cash_rent_3500_plus"
    assert current["B25056_027E"] == "contract_rent_distribution_no_cash_rent"
    assert current["B25058_001E"] == "median_contract_rent"
    assert early["B25056_023E"] == "contract_rent_distribution_cash_rent_2000_plus"
    assert early["B25056_024E"] == "contract_rent_distribution_no_cash_rent"


def test_contract_rent_acs1_variable_mappings_use_acs5_column_names() -> None:
    assert ACS1_TABLE_COLUMN_NAMES["B25058"]["B25058_001E"] == "median_contract_rent"
    assert (
        ACS1_TABLE_COLUMN_NAMES["B25056"]["B25056_001E"]
        == "contract_rent_distribution_total"
    )
    assert (
        ACS1_TABLE_COLUMN_NAMES["B25056"]["B25056_026E"]
        == "contract_rent_distribution_cash_rent_3500_plus"
    )
    assert ACS1_TABLE_COLUMN_NAMES["B25056"]["B25056_027E"] == (
        "contract_rent_distribution_no_cash_rent"
    )
    assert ACS1_VARIABLES_BY_TABLE["B25056"] == list(ACS1_TABLE_COLUMN_NAMES["B25056"])


def test_contract_rent_acs1_early_vintage_uses_top_bin_override() -> None:
    early_variables = acs1_variables_by_table_for_vintage(2014)
    modern_variables = acs1_variables_by_table_for_vintage(2015)
    early_names = acs1_variable_names_for_vintage(2014)
    modern_names = acs1_variable_names_for_vintage(2015)

    assert "B25056_025E" not in early_variables["B25056"]
    assert "B25056_026E" not in early_variables["B25056"]
    assert "B25056_027E" not in early_variables["B25056"]
    assert "B25063_025E" not in early_variables["B25063"]
    assert "B25056_025E" in modern_variables["B25056"]
    assert "B25063_025E" in modern_variables["B25063"]

    assert early_names["B25056_023E"] == "contract_rent_distribution_cash_rent_2000_plus"
    assert early_names["B25063_023E"] == "gross_rent_distribution_cash_rent_2000_plus"
    assert modern_names["B25056_023E"] == "contract_rent_distribution_cash_rent_2000_to_2499"
    assert modern_names["B25063_023E"] == "gross_rent_distribution_cash_rent_2000_to_2499"


def test_acs5_covariate_registry_matches_aggregation_column_contracts() -> None:
    count_columns = set(COUNT_COLUMNS)
    median_columns = set(MEDIAN_COLUMNS)
    moe_columns = set(MOE_COLUMNS)
    derived_columns = set(DERIVED_COLUMNS)

    for spec in ACS5_COVARIATE_REGISTRY:
        if spec.rollup_method == "area_weighted_sum":
            assert set(spec.output_columns) <= count_columns, spec.name
            assert spec.weight_column == "area_share"
            continue

        if spec.rollup_method == "root_sum_squared_moe":
            assert set(spec.output_columns) <= moe_columns, spec.name
            assert spec.weight_column == "area_share"
            continue

        if spec.rollup_method in {"population_weighted_mean", "denominator_weighted_mean"}:
            assert set(spec.output_columns) <= median_columns, spec.name
            assert spec.denominator_column is not None, spec.name
            assert spec.denominator_column in AVERAGE_WEIGHT_DENOMINATORS[spec.output_columns[0]]
            continue

        if spec.rollup_method == "ratio_of_area_weighted_sums":
            assert spec.denominator_column in count_columns, spec.name
            assert spec.weight_column == "area_share"
            assert set(spec.output_columns) <= count_columns | derived_columns, spec.name
            assert set(spec.output_columns) - count_columns <= derived_columns, spec.name
            continue

        pytest.fail(f"Unhandled ACS5 rollup method {spec.rollup_method!r} for {spec.name}")


def test_acs5_expanded_covariates_remain_outside_default_canonical_measures() -> None:
    assert ACS5_EXPANDED_COVARIATE_COLUMNS
    assert set(ACS5_EXPANDED_COVARIATE_COLUMNS).isdisjoint(schema_columns.ACS_MEASURE_COLUMNS)
    assert "household_income_total" in ACS5_EXPANDED_COVARIATE_COLUMNS
    assert "contract_rent_distribution_total" in ACS5_EXPANDED_COVARIATE_COLUMNS
    assert "median_contract_rent" in ACS5_EXPANDED_COVARIATE_COLUMNS
    assert "owner_costs_pct_income_total" in ACS5_EXPANDED_COVARIATE_COLUMNS
    assert "median_household_income" not in ACS5_EXPANDED_COVARIATE_COLUMNS


def test_recipe_selectable_acs5_measures_include_expanded_without_changing_defaults() -> None:
    assert ACS5_RECIPE_DEFAULT_MEASURES == tuple(schema_columns.ACS_MEASURE_COLUMNS)
    assert "per_capita_income" in ACS5_RECIPE_SELECTABLE_MEASURES
    assert "gini_index" in ACS5_RECIPE_SELECTABLE_MEASURES
    assert "contract_rent_distribution_total" in ACS5_RECIPE_SELECTABLE_MEASURES
    assert "median_contract_rent" in ACS5_RECIPE_SELECTABLE_MEASURES
    assert "per_capita_income" not in ACS5_RECIPE_DEFAULT_MEASURES
    assert set(ACS5_RECIPE_DEFAULT_MEASURES) < set(ACS5_RECIPE_SELECTABLE_MEASURES)


def test_acs5_covariate_output_lookup_returns_owning_registry_entry() -> None:
    spec = acs5_covariate_spec_for_output("rent_burden_30_plus")

    assert spec.name == "rent_burden"
    assert spec.table == "B25070"
    assert spec.denominator_column == "gross_rent_pct_income_total"
    assert spec.rollup_method == "ratio_of_area_weighted_sums"


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


def test_msa_coc_panel_contract_declares_initial_output_columns() -> None:
    assert tuple(MSA_COC_PANEL_COLUMNS) == MSA_COC_PANEL_CONTRACT.required_columns
    assert MSA_COC_PANEL_CONTRACT.name == "msa_coc_panel"
    assert ARTIFACT_CONTRACTS["msa_coc_panel"] is MSA_COC_PANEL_CONTRACT


def test_msa_coc_coverage_contract_declares_output_columns() -> None:
    assert tuple(MSA_COC_COVERAGE_COLUMNS) == MSA_COC_COVERAGE_CONTRACT.required_columns
    assert MSA_COC_COVERAGE_CONTRACT.name == "msa_coc_coverage"
    assert ARTIFACT_CONTRACTS["msa_coc_coverage"] is MSA_COC_COVERAGE_CONTRACT


def test_msa_fractional_rollup_contract_declares_output_columns() -> None:
    assert tuple(MSA_FRACTIONAL_ROLLUP_COLUMNS) == (
        MSA_FRACTIONAL_ROLLUP_CONTRACT.required_columns
    )
    assert MSA_FRACTIONAL_ROLLUP_CONTRACT.name == "msa_fractional_rollup"
    assert ARTIFACT_CONTRACTS["msa_fractional_rollup"] is MSA_FRACTIONAL_ROLLUP_CONTRACT


@pytest.mark.parametrize("case_name", list(URBAN_CONTRACT_CASES), ids=list(URBAN_CONTRACT_CASES))
def test_urban_contracts_declare_output_columns(case_name: str) -> None:
    contract, columns, _row, _missing_column = URBAN_CONTRACT_CASES[case_name]

    assert tuple(columns) == contract.required_columns
    assert ARTIFACT_CONTRACTS[contract.name] is contract


@pytest.mark.parametrize("case_name", list(URBAN_CONTRACT_CASES), ids=list(URBAN_CONTRACT_CASES))
def test_validate_urban_contracts_pass_for_complete_artifact(case_name: str) -> None:
    contract, _columns, row, _missing_column = URBAN_CONTRACT_CASES[case_name]

    findings = validate_artifact_contract(pd.DataFrame([row]), contract)

    assert findings == []


@pytest.mark.parametrize("case_name", list(URBAN_CONTRACT_CASES), ids=list(URBAN_CONTRACT_CASES))
def test_validate_urban_contracts_report_missing_columns(case_name: str) -> None:
    contract, _columns, row, missing_column = URBAN_CONTRACT_CASES[case_name]
    incomplete = dict(row)
    del incomplete[missing_column]

    findings = validate_artifact_contract(pd.DataFrame([incomplete]), contract)

    assert [finding.code for finding in findings] == ["missing_required_column"]
    assert findings[0].column == missing_column


def test_validate_msa_coc_coverage_contract_passes_for_complete_artifact() -> None:
    findings = validate_artifact_contract(
        pd.DataFrame([MSA_COC_COVERAGE_COMPLETE_ROW]),
        MSA_COC_COVERAGE_CONTRACT,
    )

    assert findings == []


def test_validate_msa_fractional_rollup_contract_passes_for_complete_artifact() -> None:
    findings = validate_artifact_contract(
        pd.DataFrame([MSA_FRACTIONAL_ROLLUP_COMPLETE_ROW]),
        MSA_FRACTIONAL_ROLLUP_CONTRACT,
    )

    assert findings == []


@pytest.mark.parametrize(
    "missing_column",
    [
        "source_additive_measure_columns",
        "native_msa_covariate_columns",
        "allocation_basis",
        "denominator_source",
        "boundary_vintage",
        "county_vintage",
        "block_vintage",
        "decennial_vintage",
        "msa_definition_version",
        "coc_population_coverage_ratio",
        "msa_population_coverage_ratio",
        "unmapped_source_coc_count",
        "unmapped_source_additive_measure_totals",
        "min_coc_population_containment_share",
    ],
)
def test_validate_msa_fractional_rollup_contract_reports_missing_columns(
    missing_column: str,
) -> None:
    row = dict(MSA_FRACTIONAL_ROLLUP_COMPLETE_ROW)
    del row[missing_column]

    findings = validate_artifact_contract(
        pd.DataFrame([row]),
        MSA_FRACTIONAL_ROLLUP_CONTRACT,
    )

    assert [finding.code for finding in findings] == ["missing_required_column"]
    assert findings[0].column == missing_column


@pytest.mark.parametrize(
    "missing_column",
    [
        "overlap_basis",
        "denominator_source",
        "denominator_vintage",
        "denominator_column",
        "msa_covered_by_coc_percent",
        "coc_contained_in_msa_percent",
    ],
)
def test_validate_msa_coc_coverage_contract_reports_missing_columns(
    missing_column: str,
) -> None:
    row = dict(MSA_COC_COVERAGE_COMPLETE_ROW)
    del row[missing_column]

    findings = validate_artifact_contract(
        pd.DataFrame([row]),
        MSA_COC_COVERAGE_CONTRACT,
    )

    assert [finding.code for finding in findings] == ["missing_required_column"]
    assert findings[0].column == missing_column


def test_validate_msa_coc_panel_contract_passes_for_complete_artifact() -> None:
    findings = validate_artifact_contract(
        pd.DataFrame([MSA_COC_PANEL_COMPLETE_ROW]),
        MSA_COC_PANEL_CONTRACT,
    )

    assert findings == []


@pytest.mark.parametrize(
    "missing_column",
    [
        "msa_id",
        "coc_id",
        "msa_population",
        "msa_unemployment",
        "coc_population",
        "msa_definition_version_used",
    ],
)
def test_validate_msa_coc_panel_contract_reports_missing_columns(missing_column: str) -> None:
    row = dict(MSA_COC_PANEL_COMPLETE_ROW)
    del row[missing_column]

    findings = validate_artifact_contract(
        pd.DataFrame([row]),
        MSA_COC_PANEL_CONTRACT,
    )

    assert [finding.code for finding in findings] == ["missing_required_column"]
    assert findings[0].column == missing_column


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
    assert "sae_contract_rent_median" in schema_columns.SAE_DERIVED_MEASURE_COLUMNS
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
