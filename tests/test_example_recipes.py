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


@dataclass(frozen=True)
class CanonicalUrbanRecipeCase:
    path: str
    pipeline_id: str
    recipe_name: str
    target_id: str
    threshold: float
    primary_msa_overlap_basis: str
    primary_msa_population_source: str | None = None


CANONICAL_URBAN_OUTPUT_COLUMNS: tuple[str, ...] = (
    "coc_id",
    "coc_name",
    "year",
    "urban_population_fraction",
    "coc_population",
    "population_density_per_sq_km",
    "primary_msa_id",
    "primary_msa_name",
    "primary_msa_population",
    "primary_msa_overlap_basis",
    "primary_msa_coc_contained_percent",
    "primary_msa_covered_by_coc_percent",
)


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
        path="coc-pit-hic-2010-2024.yaml",
        pipeline_id="build_coc_panel",
        recipe_name="coc_pit_hic_2010_2024",
        target_type="coc",
        years=(
            2010,
            2011,
            2012,
            2013,
            2014,
            2015,
            2016,
            2017,
            2018,
            2019,
            2020,
            2021,
            2022,
            2023,
            2024,
        ),
        datasets=("pit", "hic"),
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
        path="county-medsl-pep-2024.yaml",
        pipeline_id="build_county_panel",
        recipe_name="county_medsl_pep_2024",
        target_type="county",
        years=(2024,),
        datasets=("medsl_president", "pep_county"),
    ),
    ExampleRecipeCase(
        path="coc-msa-prism-tmin-january-2024.yaml",
        pipeline_id="build_coc_panel",
        recipe_name="coc_msa_prism_tmin_january_2024",
        target_type="coc",
        years=(2024,),
        datasets=("prism_tmin_county",),
    ),
    ExampleRecipeCase(
        path="coc-msa-prism-tmin-january-2024.yaml",
        pipeline_id="build_msa_panel",
        recipe_name="coc_msa_prism_tmin_january_2024",
        target_type="msa",
        years=(2024,),
        datasets=("prism_tmin_county",),
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
    ExampleRecipeCase(
        path="msa-census-pit-pep-medsl-2024.yaml",
        pipeline_id="build_msa_panel",
        recipe_name="msa_census_pit_pep_medsl_2024",
        target_type="msa",
        years=(2024,),
        datasets=("pit", "pep_county", "medsl_president"),
    ),
)


CANONICAL_URBAN_RECIPE_CASES: tuple[CanonicalUrbanRecipeCase, ...] = (
    CanonicalUrbanRecipeCase(
        path="recipes/coc-urban-fraction-gte-95-2020.yaml",
        pipeline_id="build_urban_fraction_gte_95",
        recipe_name="coc_urban_fraction_gte_95_2020",
        target_id="urban_fraction_gte_95",
        threshold=0.95,
        primary_msa_overlap_basis="population",
        primary_msa_population_source="decennial",
    ),
    CanonicalUrbanRecipeCase(
        path="recipes/coc-urban-fraction-gte-99-2020.yaml",
        pipeline_id="build_urban_fraction_gte_99",
        recipe_name="coc_urban_fraction_gte_99_2020",
        target_id="urban_fraction_gte_99",
        threshold=0.99,
        primary_msa_overlap_basis="area",
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
            2019: "tract_to_metro_2010",
            2020: "tract_to_metro_2010",
            2021: "tract_to_metro_2020",
            2025: "tract_to_metro_2020",
        },
    ),
    (
        "metro-glynnfox-pit-acs-pep-zori-2016-2024.yaml",
        "build_metro_panel",
        "acs_tract",
        {
            2016: "tract_to_metro_2010",
            2020: "tract_to_metro_2010",
            2021: "tract_to_metro_2020",
            2024: "tract_to_metro_2020",
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


@dataclass(frozen=True)
class SaeRecipeCase:
    path: str
    pipeline_id: str
    recipe_name: str
    output_dataset: str
    source_dataset: str
    support_dataset: str
    measure_families: tuple[str, ...]


@dataclass(frozen=True)
class MsaCocCoverageRecipeCase:
    path: str
    pipeline_id: str
    recipe_name: str
    target_id: str
    overlap_bases: tuple[str, ...]
    ranking_population_source: str
    acs5_population_vintage: int


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


SAE_RECIPE_CASES: tuple[SaeRecipeCase, ...] = (
    SaeRecipeCase(
        path="coc-sae-acs1-2023.yaml",
        pipeline_id="build_coc_sae_panel",
        recipe_name="coc_sae_acs1_2023",
        output_dataset="acs_sae_coc",
        source_dataset="acs1_county_sae",
        support_dataset="acs5_tract_sae_support",
        measure_families=(
            "labor_force",
            "rent_burden",
            "owner_cost_burden",
            "household_income_bins",
            "gross_rent_bins",
        ),
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


MSA_COC_COVERAGE_RECIPE_CASES: tuple[MsaCocCoverageRecipeCase, ...] = (
    MsaCocCoverageRecipeCase(
        path="msa-coc-coverage.yaml",
        pipeline_id="build_msa_coc_coverage",
        recipe_name="msa_coc_coverage_2024",
        target_id="top100_msa_coc_coverage",
        overlap_bases=("area", "population"),
        ranking_population_source="pep",
        acs5_population_vintage=2023,
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
    pipeline = next(p for p in recipe.pipelines if p.id == case.pipeline_id)
    target = next(t for t in recipe.targets if t.id == pipeline.target)

    assert recipe.name == case.recipe_name
    assert target.geometry.type == case.target_type
    assert [task.year for task in plan.join_tasks] == list(case.years)
    assert tuple(plan.join_tasks[0].datasets) == case.datasets
    assert len(plan.resample_tasks) == len(case.years) * len(case.datasets)


def test_coc_pit_hic_example_requests_expanded_hic_measures() -> None:
    recipe = _load_example("coc-pit-hic-2010-2024.yaml")
    plan = resolve_plan(recipe, "build_coc_panel")
    target = recipe.targets[0]
    expected_hic_measures = [
        "hic_es_year_round_beds",
        "hic_th_year_round_beds",
        "hic_sh_year_round_beds",
        "hic_rrh_year_round_beds",
        "hic_psh_year_round_beds",
        "hic_oph_year_round_beds",
        "hic_shelter_year_round_beds",
        "hic_total_beds",
        "hic_total_units",
    ]

    hic_tasks = [task for task in plan.resample_tasks if task.dataset_id == "hic"]

    assert target.panel_policy is not None
    assert all(column in target.panel_policy.output_columns for column in expected_hic_measures)
    assert hic_tasks
    assert all(list(task.measures) == expected_hic_measures for task in hic_tasks)


def test_top50_msa_coc_pit_acs5_poverty_2020_recipe_loads_and_resolves() -> None:
    recipe = _load_repo_recipe("recipes/top50-msa-coc-pit-acs5-poverty-2020.yaml")
    plan = resolve_plan(recipe, "build_msa_panel")
    target = recipe.targets[0]
    cohort = target.cohort

    assert recipe.name == "top50_msa_coc_pit_acs5_poverty_2020"
    assert target.geometry.type == "msa"
    assert cohort is not None
    assert cohort.method == "top_n"
    assert cohort.n == 50
    assert cohort.reference_year == 2020
    assert [task.year for task in plan.join_tasks] == [2020]
    assert tuple(plan.join_tasks[0].datasets) == ("pit", "pep_county", "acs_tract")

    acs_tasks = [task for task in plan.resample_tasks if task.dataset_id == "acs_tract"]
    assert len(acs_tasks) == 1
    assert "population_below_poverty" in acs_tasks[0].measures
    assert acs_tasks[0].to_geometry.type == "msa"


def test_top50_msa_coc_pit_acs5_poverty_2015_2025_recipe_loads_and_resolves() -> None:
    recipe = _load_repo_recipe("recipes/top50-msa-coc-pit-acs5-poverty-2015-2025.yaml")
    plan = resolve_plan(recipe, "build_msa_panel")
    target = recipe.targets[0]
    cohort = target.cohort

    assert recipe.name == "top50_msa_coc_pit_acs5_poverty_2015_2025"
    assert target.geometry.type == "msa"
    assert cohort is not None
    assert cohort.method == "top_n"
    assert cohort.n == 50
    assert cohort.reference_year == 2025
    assert [task.year for task in plan.join_tasks] == [2015, 2025]
    assert all(tuple(task.datasets) == ("pit", "pep_county", "acs_tract") for task in plan.join_tasks)

    acs_tasks_by_year = {
        task.year: task for task in plan.resample_tasks if task.dataset_id == "acs_tract"
    }
    assert set(acs_tasks_by_year) == {2015, 2025}
    assert acs_tasks_by_year[2015].input_path.endswith("acs5_tracts__A2014xT2010.parquet")
    assert acs_tasks_by_year[2025].input_path.endswith("acs5_tracts__A2024xT2020.parquet")
    assert all("population_below_poverty" in task.measures for task in acs_tasks_by_year.values())
    assert all(task.to_geometry.type == "msa" for task in acs_tasks_by_year.values())


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
    "case",
    MSA_COC_COVERAGE_RECIPE_CASES,
    ids=lambda case: case.path,
)
def test_msa_coc_coverage_example_recipe_loads_and_resolves(
    case: MsaCocCoverageRecipeCase,
):
    recipe = _load_example(case.path)
    plan = resolve_plan(recipe, case.pipeline_id)
    target = recipe.targets[0]
    spec = target.msa_coc_coverage

    assert recipe.name == case.recipe_name
    assert target.id == case.target_id
    assert target.outputs == ["msa_coc_coverage"]
    assert spec is not None
    assert tuple(spec.overlap_bases) == case.overlap_bases
    assert spec.ranking_population_source == case.ranking_population_source
    assert spec.acs5_population_vintage == case.acs5_population_vintage
    assert spec.tract_vintage == 2020
    assert len(plan.resample_tasks) == 1
    assert plan.resample_tasks[0].dataset_id == "pep_msa"
    assert plan.join_tasks == []


@pytest.mark.parametrize(
    "case",
    CANONICAL_URBAN_RECIPE_CASES,
    ids=lambda case: Path(case.path).name,
)
def test_canonical_urban_recipes_request_primary_msa_annotations(
    case: CanonicalUrbanRecipeCase,
):
    recipe = _load_repo_recipe(case.path)
    plan = resolve_plan(recipe, case.pipeline_id)
    target = recipe.targets[0]
    policy = target.panel_policy

    assert recipe.name == case.recipe_name
    assert target.id == case.target_id
    assert target.outputs == ["panel"]
    assert target.cohort.value == case.threshold
    assert policy is not None
    assert policy.primary_msa is not None
    assert policy.primary_msa.definition_version == "census_msa_2023"
    assert policy.primary_msa.county_vintage == 2023
    assert policy.primary_msa.overlap_basis == case.primary_msa_overlap_basis
    if case.primary_msa_overlap_basis == "population":
        assert policy.primary_msa.population_source == case.primary_msa_population_source
        assert policy.primary_msa.decennial_population_vintage == 2020
        assert policy.primary_msa.tract_vintage == 2020
    assert policy.output_columns == list(CANONICAL_URBAN_OUTPUT_COLUMNS)
    assert [task.year for task in plan.join_tasks] == [2020]
    assert tuple(plan.join_tasks[0].datasets) == ("urban_fraction",)


def test_all_coc_pit_sheltered_unsheltered_recipe_loads_and_resolves():
    recipe = _load_repo_recipe("recipes/cocs-pit-sheltered-unsheltered-2010-2020.yaml")
    plan = resolve_plan(recipe, "build_coc_pit_panel")
    target = recipe.targets[0]

    assert recipe.name == "cocs_pit_sheltered_unsheltered_2010_2020"
    assert target.id == "coc_pit_panel"
    assert target.geometry.type == "coc"
    assert target.geometry.vintage == 2025
    assert target.outputs == ["panel"]
    assert target.panel_policy is not None
    assert target.panel_policy.output_columns == [
        "coc_id",
        "coc_name",
        "year",
        "pit_total",
        "pit_sheltered",
        "pit_unsheltered",
    ]
    assert [task.year for task in plan.join_tasks] == list(range(2010, 2021))
    assert all(tuple(task.datasets) == ("pit",) for task in plan.join_tasks)
    assert len(plan.resample_tasks) == 11
    assert {tuple(task.measures) for task in plan.resample_tasks} == {
        ("pit_total", "pit_sheltered", "pit_unsheltered")
    }


@pytest.mark.parametrize("case", SAE_RECIPE_CASES, ids=lambda case: case.path)
def test_sae_example_recipe_loads_and_resolves(case: SaeRecipeCase):
    recipe = _load_example(case.path)
    plan = resolve_plan(recipe, case.pipeline_id)

    assert recipe.name == case.recipe_name
    assert len(plan.small_area_estimate_tasks) == 1
    task = plan.small_area_estimate_tasks[0]
    assert task.output_dataset == case.output_dataset
    assert task.source_dataset == case.source_dataset
    assert task.support_dataset == case.support_dataset
    assert tuple(task.measure_families) == case.measure_families
    assert task.terminal_acs5_vintage == "2022"
    assert task.tract_vintage == "2020"
    assert plan.join_tasks[0].datasets == [case.output_dataset]


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


@pytest.mark.parametrize(
    ("path", "pipeline_id"),
    [
        ("county-medsl-pep-2024.yaml", "build_county_panel"),
        ("msa-census-pit-pep-medsl-2024.yaml", "build_msa_panel"),
    ],
)
def test_medsl_examples_derive_vote_rates_from_summed_counts(
    path: str,
    pipeline_id: str,
):
    recipe = _load_example(path)
    plan = resolve_plan(recipe, pipeline_id)

    task = next(task for task in plan.resample_tasks if task.dataset_id == "medsl_president")

    assert task.measure_aggregations == {
        "democratic_votes": "sum",
        "republican_votes": "sum",
        "two_party_votes": "sum",
        "totalvotes": "sum",
    }
    assert sorted((task.derived_measures or {}).keys()) == [
        "democratic_republican_vote_ratio",
        "democratic_vote_share",
        "republican_vote_share",
    ]
    assert task.derived_measures["democratic_vote_share"]["source_numerator_column"] == (
        "democratic_votes"
    )
    assert task.derived_measures["democratic_vote_share"]["denominator_column"] == "totalvotes"


@pytest.mark.parametrize(
    "path",
    [
        "coc-base-pit-acs-zori-2016-2021.yaml",
        "coc-pep-zori-calendar-2020-2024.yaml",
    ],
)
def test_coc_pep_population_uses_tract_mediated_crosswalk(path: str):
    recipe = _load_example(path)
    plan = resolve_plan(recipe, "build_coc_panel")

    transform = next(t for t in recipe.transforms if t.id == "county_to_coc_population")
    assert transform.spec.weighting.scheme == "tract_mediated"
    assert transform.spec.weighting.variety == "population"

    pep_tasks = [task for task in plan.resample_tasks if task.dataset_id == "pep_county"]
    assert pep_tasks
    assert {task.transform_id for task in pep_tasks} == {"county_to_coc_population"}
    assert {task.weight_column for task in pep_tasks} == {"population_weight"}


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
