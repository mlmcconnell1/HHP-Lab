"""Tests for canonical schema contracts and population lineage."""

from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

from hhplab.panel.conformance import PanelRequest, run_conformance
from hhplab.recipe.executor import _normalize_recipe_population_measure
from hhplab.recipe.planner import ResampleTask
from hhplab.recipe.schema_common import GeometryRef
from hhplab.schema.lineage import (
    PopulationLineage,
    PopulationMethod,
    PopulationSource,
    normalize_population_measure,
)


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
