"""Regression coverage for committed example recipe files.

These tests intentionally stop at recipe load/plan resolution so they remain
portable in CI without depending on local curated data artifacts.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest
import yaml

from coclab.recipe.loader import load_recipe
from coclab.recipe.planner import resolve_plan

REPO_ROOT = Path(__file__).resolve().parent.parent
EXAMPLES_DIR = REPO_ROOT / "recipes" / "examples"


@dataclass(frozen=True)
class ExampleRecipeCase:
    path: str
    pipeline_id: str
    recipe_name: str
    target_type: str
    years: tuple[int, ...]
    datasets: tuple[str, ...]


EXAMPLE_RECIPE_CASES: tuple[ExampleRecipeCase, ...] = (
    ExampleRecipeCase(
        path="coc-base-pit-acs-zori-2016-2021.yaml",
        pipeline_id="build_coc_panel",
        recipe_name="coc_base_pit_acs_zori_2016_2021",
        target_type="coc",
        years=(2016, 2017, 2018, 2019, 2020, 2021),
        datasets=("pit", "pep_county", "acs_tract", "zori_county"),
    ),
    ExampleRecipeCase(
        path="coc-pep-zori-calendar-2020-2024.yaml",
        pipeline_id="build_coc_panel",
        recipe_name="coc_pep_zori_calendar_2020_2024",
        target_type="coc",
        years=(2020, 2021, 2022, 2023, 2024),
        datasets=("pep_county", "zori_county"),
    ),
    ExampleRecipeCase(
        path="metro-glynnfox-acs-income-2019-2025.yaml",
        pipeline_id="build_metro_panel",
        recipe_name="metro_glynnfox_acs_income_2019_2025",
        target_type="metro",
        years=(2019, 2020, 2021, 2022, 2023, 2024, 2025),
        datasets=("acs_tract",),
    ),
    ExampleRecipeCase(
        path="metro-glynnfox-pit-acs-pep-zori-2016-2024.yaml",
        pipeline_id="build_metro_panel",
        recipe_name="metro_glynnfox_pit_acs_pep_zori_2016_2024",
        target_type="metro",
        years=(2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024),
        datasets=("pit", "pep_county", "acs_tract", "zori_county"),
    ),
    ExampleRecipeCase(
        path="metro-glynnfox-pit-pep-2011-2014.yaml",
        pipeline_id="build_metro_panel",
        recipe_name="metro_glynnfox_pit_pep_2011_2014",
        target_type="metro",
        years=(2011, 2012, 2013, 2014),
        datasets=("pit", "pep_county"),
    ),
    ExampleRecipeCase(
        path="metro-glynnfox-pit-pep-acs1-2023.yaml",
        pipeline_id="build_metro_panel",
        recipe_name="metro_glynnfox_pit_pep_acs1_2023",
        target_type="metro",
        years=(2023,),
        datasets=("pit", "pep_county", "acs1_metro"),
    ),
)


AUTO_TRANSFORM_EXPECTATIONS: tuple[tuple[str, str, str, dict[int, str]], ...] = (
    (
        "coc-base-pit-acs-zori-2016-2021.yaml",
        "build_coc_panel",
        "acs_tract",
        {2016: "tract_to_coc_2010", 2020: "tract_to_coc_2010", 2021: "tract_to_coc_2020"},
    ),
    (
        "metro-glynnfox-acs-income-2019-2025.yaml",
        "build_metro_panel",
        "acs_tract",
        {2019: "tract_to_metro_2010", 2020: "tract_to_metro_2010", 2021: "tract_to_metro_2020", 2025: "tract_to_metro_2020"},
    ),
    (
        "metro-glynnfox-pit-acs-pep-zori-2016-2024.yaml",
        "build_metro_panel",
        "acs_tract",
        {2016: "tract_to_metro_2010", 2020: "tract_to_metro_2010", 2021: "tract_to_metro_2020", 2024: "tract_to_metro_2020"},
    ),
)


def _load_example(relative_path: str):
    path = EXAMPLES_DIR / relative_path
    with path.open(encoding="utf-8") as handle:
        return load_recipe(yaml.safe_load(handle))


@pytest.mark.parametrize("case", EXAMPLE_RECIPE_CASES, ids=lambda case: case.path)
def test_example_recipe_loads_and_resolves(case: ExampleRecipeCase):
    recipe = _load_example(case.path)
    plan = resolve_plan(recipe, case.pipeline_id)

    assert recipe.name == case.recipe_name
    assert recipe.targets[0].geometry.type == case.target_type
    assert [task.year for task in plan.join_tasks] == list(case.years)
    assert tuple(plan.join_tasks[0].datasets) == case.datasets
    assert len(plan.resample_tasks) == len(case.years) * len(case.datasets)


@pytest.mark.parametrize(
    ("path", "pipeline_id", "dataset_id", "expected_by_year"),
    AUTO_TRANSFORM_EXPECTATIONS,
    ids=[item[0] for item in AUTO_TRANSFORM_EXPECTATIONS],
)
def test_example_recipe_auto_transform_selection(
    path: str,
    pipeline_id: str,
    dataset_id: str,
    expected_by_year: dict[int, str],
):
    recipe = _load_example(path)
    plan = resolve_plan(recipe, pipeline_id)

    tasks = [task for task in plan.resample_tasks if task.dataset_id == dataset_id]
    transform_by_year = {task.year: task.transform_id for task in tasks}

    for year, transform_id in expected_by_year.items():
        assert transform_by_year[year] == transform_id
