"""Regression coverage for the committed Glynn/Fox metro recipes."""

from pathlib import Path

import yaml

from coclab.recipe.loader import load_recipe
from coclab.recipe.planner import resolve_plan

REPO_ROOT = Path(__file__).resolve().parent.parent
RECIPES_DIR = REPO_ROOT / "recipes"

ZORI_RECIPE_PATH = RECIPES_DIR / "metro25-glynnfox.yaml"


def _load_recipe(path: Path):
    with path.open(encoding="utf-8") as handle:
        return load_recipe(yaml.safe_load(handle))


def test_glynn_fox_zori_recipe_covers_2015_2020():
    with ZORI_RECIPE_PATH.open(encoding="utf-8") as handle:
        raw_recipe = yaml.safe_load(handle)
    recipe = _load_recipe(ZORI_RECIPE_PATH)
    plan = resolve_plan(recipe, "build_metro_panel")

    assert recipe.name == "glynn_fox_metro_panel_2015_2020_zori"
    assert [task.year for task in plan.join_tasks] == [2015, 2016, 2017, 2018, 2019, 2020]
    assert set(plan.join_tasks[0].datasets) == {"pit", "pep_county", "acs_tract", "zori_county"}
    assert raw_recipe["datasets"]["zori_county"]["path"] == "data/curated/zori/zori__county__Z2026.parquet"
    assert raw_recipe["transforms"][0]["from"]["vintage"] == 2020


def test_glynn_fox_zori_recipe_uses_lagged_acs_paths():
    recipe = _load_recipe(ZORI_RECIPE_PATH)
    plan = resolve_plan(recipe, "build_metro_panel")

    acs_tasks = {
        task.year: task
        for task in plan.resample_tasks
        if task.dataset_id == "acs_tract"
    }

    assert acs_tasks[2015].input_path == "data/curated/acs/acs5_tracts__A2014xT2010.parquet"
    assert acs_tasks[2020].input_path == "data/curated/acs/acs5_tracts__A2019xT2020.parquet"
