"""Tests for canonical schema contracts and population lineage."""

from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest

from hhplab.acs.variables import (
    COUNT_COLUMNS,
    DERIVED_COLUMNS,
    MEDIAN_COLUMNS,
    MOE_COLUMNS,
    TRACT_OUTPUT_COLUMNS,
)
from hhplab.bls.laus_series import (
    LAUS_MEASURE_CODES as BLS_LAUS_MEASURE_CODES,
    LAUS_METRO_OUTPUT_COLUMNS as BLS_LAUS_METRO_OUTPUT_COLUMNS,
)
from hhplab.panel.conformance import PanelRequest, run_conformance
from hhplab.pep.pep_ingest import PEP_COUNTY_OUTPUT_COLUMNS as PEP_INGEST_COLUMNS
from hhplab.pit.ingest.parser import CANONICAL_COLUMNS as PIT_PARSER_COLUMNS
from hhplab.recipe.executor import _normalize_recipe_population_measure
from hhplab.recipe.planner import ResampleTask
from hhplab.recipe.schema_common import GeometryRef
from hhplab.rents.zori_ingest import ZORI_INGEST_OUTPUT_COLUMNS as ZORI_INGEST_COLUMNS
from hhplab.schema import columns as schema_columns
from hhplab.schema.lineage import (
    PopulationLineage,
    PopulationMethod,
    PopulationSource,
    normalize_population_measure,
)
from hhplab.xwalks.tract_mediated import (
    DENOMINATOR_COLUMNS as XWALK_DENOMINATOR_COLUMNS,
    WEIGHT_COLUMNS as XWALK_WEIGHT_COLUMNS,
)

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
