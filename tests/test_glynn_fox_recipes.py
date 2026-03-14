"""Regression coverage for the committed Glynn/Fox metro recipes."""

from pathlib import Path

import yaml

from coclab.recipe.loader import load_recipe
from coclab.recipe.planner import resolve_plan


REPO_ROOT = Path(__file__).resolve().parent.parent
RECIPES_DIR = REPO_ROOT / "recipes"

ZORI_RECIPE_PATH = RECIPES_DIR / "glynn_fox_metro_panel.yaml"
NO_ZORI_RECIPE_PATH = RECIPES_DIR / "glynn_fox_metro_panel_no_zori.yaml"


def _load_recipe(path: Path):
    with path.open(encoding="utf-8") as handle:
        return load_recipe(yaml.safe_load(handle))


def test_glynn_fox_zori_recipe_stays_on_available_zori_window():
    recipe = _load_recipe(ZORI_RECIPE_PATH)
    plan = resolve_plan(recipe, "build_metro_panel")

    assert recipe.name == "glynn_fox_metro_panel_2015_2016_zori"
    assert [task.year for task in plan.join_tasks] == [2015, 2016]
    assert set(plan.join_tasks[0].datasets) == {"pit", "pep_county", "zori_county"}


def test_glynn_fox_no_zori_recipe_covers_full_paper_window():
    recipe = _load_recipe(NO_ZORI_RECIPE_PATH)
    plan = resolve_plan(recipe, "build_metro_panel")

    assert recipe.name == "glynn_fox_metro_panel_2011_2016_no_zori"
    assert "zori_county" not in recipe.datasets
    assert [task.year for task in plan.join_tasks] == [2011, 2012, 2013, 2014, 2015, 2016]
    assert set(plan.join_tasks[0].datasets) == {"pit", "pep_county"}
