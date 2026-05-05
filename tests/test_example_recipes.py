"""Regression coverage for committed example recipe files.

These tests intentionally stop at recipe load/plan resolution so they remain
portable in CI without depending on local curated data artifacts.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from hhplab.cli.main import app
from hhplab.recipe.loader import load_recipe
from hhplab.recipe.planner import resolve_plan

REPO_ROOT = Path(__file__).resolve().parent.parent
EXAMPLES_DIR = REPO_ROOT / "recipes" / "examples"
runner = CliRunner()


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
        path="coc-pit-density-2015-2024.yaml",
        pipeline_id="build_coc_panel",
        recipe_name="coc_pit_density_2015_2024",
        target_type="coc",
        years=(2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024),
        datasets=("pit", "acs_tract"),
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
    ExampleRecipeCase(
        path="msa-census-pit-acs-pep-2020-2021.yaml",
        pipeline_id="build_msa_panel",
        recipe_name="msa_census_pit_acs_pep_2020_2021",
        target_type="msa",
        years=(2020, 2021),
        datasets=("pit", "pep_county", "acs_tract"),
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
        "coc-pit-density-2015-2024.yaml",
        "build_coc_panel",
        "acs_tract",
        {
            2015: "tract_to_coc_2010",
            2020: "tract_to_coc_2010",
            2021: "tract_to_coc_2020",
            2024: "tract_to_coc_2020",
        },
    ),
    (
        "metro-glynnfox-acs-income-2019-2025.yaml",
        "build_metro_panel",
        "acs_tract",
        {
            2019: "tract_to_metro_2010", 2020: "tract_to_metro_2010",
            2021: "tract_to_metro_2020", 2025: "tract_to_metro_2020",
        },
    ),
    (
        "metro-glynnfox-pit-acs-pep-zori-2016-2024.yaml",
        "build_metro_panel",
        "acs_tract",
        {
            2016: "tract_to_metro_2010", 2020: "tract_to_metro_2010",
            2021: "tract_to_metro_2020", 2024: "tract_to_metro_2020",
        },
    ),
    (
        "msa-census-pit-acs-pep-2020-2021.yaml",
        "build_msa_panel",
        "acs_tract",
        {
            2020: "tract_to_msa_2010",
            2021: "tract_to_msa_2020",
        },
    ),
)


@dataclass(frozen=True)
class MapRecipeCase:
    path: str
    pipeline_id: str
    recipe_name: str
    target_id: str


@dataclass(frozen=True)
class ContainmentRecipeCase:
    path: str
    pipeline_id: str
    recipe_name: str
    target_id: str
    container_type: str
    candidate_type: str
    selector_ids: tuple[str, ...]


MAP_RECIPE_CASES: tuple[MapRecipeCase, ...] = (
    MapRecipeCase(
        path="recipes/florida-cocs-orlando-msa-map-2025.yaml",
        pipeline_id="florida_overlay_map_pipeline",
        recipe_name="florida_cocs_orlando_msa_map_2025",
        target_id="florida_overlay_map",
    ),
    MapRecipeCase(
        path="recipes/colorado-cocs-denver-msa-map-2025.yaml",
        pipeline_id="colorado_overlay_map_pipeline",
        recipe_name="colorado_cocs_denver_msa_map_2025",
        target_id="colorado_overlay_map",
    ),
)


CONTAINMENT_RECIPE_CASES: tuple[ContainmentRecipeCase, ...] = (
    ContainmentRecipeCase(
        path="msa-coc-containment-denver-2025.yaml",
        pipeline_id="build_denver_msa_coc_candidates",
        recipe_name="msa_coc_containment_denver_2025",
        target_id="denver_msa_coc_candidates",
        container_type="msa",
        candidate_type="coc",
        selector_ids=("19740",),
    ),
    ContainmentRecipeCase(
        path="coc-county-containment-los-angeles-2025.yaml",
        pipeline_id="build_los_angeles_coc_county_candidates",
        recipe_name="coc_county_containment_los_angeles_2025",
        target_id="los_angeles_coc_county_candidates",
        container_type="coc",
        candidate_type="county",
        selector_ids=("CA-600",),
    ),
)


def _load_example(relative_path: str):
    path = EXAMPLES_DIR / relative_path
    with path.open(encoding="utf-8") as handle:
        return load_recipe(yaml.safe_load(handle))


def _load_repo_recipe(relative_path: str):
    path = REPO_ROOT / relative_path
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
    "case",
    CONTAINMENT_RECIPE_CASES,
    ids=lambda case: case.path,
)
def test_containment_example_recipe_loads_and_resolves(case: ContainmentRecipeCase):
    recipe = _load_example(case.path)
    plan = resolve_plan(recipe, case.pipeline_id)
    target = recipe.targets[0]

    assert recipe.name == case.recipe_name
    assert target.id == case.target_id
    assert target.outputs == ["containment"]
    assert target.containment_spec is not None
    assert target.containment_spec.container.type == case.container_type
    assert target.containment_spec.candidate.type == case.candidate_type
    assert tuple(target.containment_spec.selector_ids or ()) == case.selector_ids
    assert recipe.datasets == {}
    assert plan.materialize_tasks == []
    assert plan.resample_tasks == []
    assert plan.join_tasks == []


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


@pytest.mark.parametrize("case", MAP_RECIPE_CASES, ids=lambda case: Path(case.path).name)
def test_map_recipe_loads_and_resolves_without_datasets(case: MapRecipeCase):
    recipe = _load_repo_recipe(case.path)
    plan = resolve_plan(recipe, case.pipeline_id)

    assert recipe.name == case.recipe_name
    assert recipe.targets[0].id == case.target_id
    assert recipe.targets[0].outputs == ["map"]
    assert recipe.datasets == {}
    assert plan.materialize_tasks == []
    assert plan.resample_tasks == []
    assert plan.join_tasks == []


@pytest.mark.parametrize("case", MAP_RECIPE_CASES, ids=lambda case: Path(case.path).name)
def test_map_recipe_build_executes_map_only_pipeline(
    case: MapRecipeCase,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    def _fake_render_recipe_map(target, *, project_root: Path, out_html: Path) -> Path:
        del target, project_root
        out_html.parent.mkdir(parents=True, exist_ok=True)
        out_html.write_text("<html>map</html>", encoding="utf-8")
        return out_html

    monkeypatch.chdir(REPO_ROOT)
    monkeypatch.setattr(
        "hhplab.viz.map_folium.render_recipe_map",
        _fake_render_recipe_map,
    )

    result = runner.invoke(
        app,
        [
            "build",
            "recipe",
            "--recipe",
            case.path,
            "--output-root",
            str(tmp_path / "outputs"),
            "--json",
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    payload = yaml.safe_load(result.stdout)
    assert payload["status"] == "ok"
    assert payload["pipelines"][0]["pipeline_id"] == case.pipeline_id
    assert payload["pipelines"][0]["success"] is True
    assert [step["step_kind"] for step in payload["pipelines"][0]["steps"]] == ["persist_map"]
    map_path = Path(payload["artifacts"]["map_path"])
    assert map_path.is_absolute()
    assert map_path.exists()
    assert map_path.parent.name == case.recipe_name
    assert map_path.name == "map__Y2025-2025@B2025.html"
