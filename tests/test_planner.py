"""Tests for the recipe resolution planner."""

from __future__ import annotations

import pytest

from coclab.recipe.loader import load_recipe
from coclab.recipe.planner import (
    ExecutionPlan,
    PlannerError,
    resolve_plan,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _segmented_recipe() -> dict:
    """Recipe with ACS segmented file_set and two crosswalk transforms."""
    return {
        "version": 1,
        "name": "planner-test",
        "universe": {"range": "2017-2021"},
        "targets": [
            {"id": "coc_panel", "geometry": {"type": "coc", "vintage": 2025}},
        ],
        "datasets": {
            "acs": {
                "provider": "census",
                "product": "acs",
                "version": 1,
                "native_geometry": {"type": "tract"},
                "file_set": {
                    "path_template": "data/acs/acs_{year}.parquet",
                    "segments": [
                        {
                            "years": {"range": "2015-2019"},
                            "geometry": {"type": "tract", "vintage": 2010},
                            "overrides": {
                                2017: "data/acs/acs_special_2017.parquet",
                            },
                        },
                        {
                            "years": {"range": "2020-2024"},
                            "geometry": {"type": "tract", "vintage": 2020},
                        },
                    ],
                },
            },
            "pit": {
                "provider": "hud",
                "product": "pit",
                "version": 1,
                "native_geometry": {"type": "coc"},
                "path": "data/pit/pit.parquet",
            },
        },
        "transforms": [
            {
                "id": "coc_to_tract_2010",
                "type": "crosswalk",
                "from": {"type": "coc", "vintage": 2025},
                "to": {"type": "tract", "vintage": 2010},
                "spec": {"weighting": {"scheme": "area"}},
            },
            {
                "id": "coc_to_tract_2020",
                "type": "crosswalk",
                "from": {"type": "coc", "vintage": 2025},
                "to": {"type": "tract", "vintage": 2020},
                "spec": {"weighting": {"scheme": "area"}},
            },
        ],
        "pipelines": [
            {
                "id": "main",
                "target": "coc_panel",
                "steps": [
                    {
                        "materialize": {
                            "transforms": ["coc_to_tract_2010", "coc_to_tract_2020"],
                        },
                    },
                    {
                        "resample": {
                            "dataset": "acs",
                            "to_geometry": {"type": "coc", "vintage": 2025},
                            "method": "aggregate",
                            "via": "auto",
                            "measures": ["total_population", "median_household_income"],
                        },
                    },
                    {
                        "resample": {
                            "dataset": "pit",
                            "to_geometry": {"type": "coc", "vintage": 2025},
                            "method": "identity",
                            "measures": ["pit_total"],
                        },
                    },
                    {
                        "join": {
                            "datasets": ["acs", "pit"],
                            "join_on": ["geo_id", "year"],
                        },
                    },
                ],
            },
        ],
    }


# ===========================================================================
# Planner: dataset-year resolution
# ===========================================================================


class TestPlannerResolution:

    def test_resolves_correct_geometry_per_year(self):
        recipe = load_recipe(_segmented_recipe())
        plan = resolve_plan(recipe, "main")
        assert isinstance(plan, ExecutionPlan)

        # ACS tasks: years 2017-2021 = 5 tasks; PIT tasks: 5 tasks
        acs_tasks = [t for t in plan.resample_tasks if t.dataset_id == "acs"]
        assert len(acs_tasks) == 5

        # 2017 → tract@2010
        t2017 = next(t for t in acs_tasks if t.year == 2017)
        assert t2017.effective_geometry.type == "tract"
        assert t2017.effective_geometry.vintage == 2010

        # 2021 → tract@2020
        t2021 = next(t for t in acs_tasks if t.year == 2021)
        assert t2021.effective_geometry.type == "tract"
        assert t2021.effective_geometry.vintage == 2020

    def test_path_override_used(self):
        recipe = load_recipe(_segmented_recipe())
        plan = resolve_plan(recipe, "main")
        acs_tasks = [t for t in plan.resample_tasks if t.dataset_id == "acs"]
        t2017 = next(t for t in acs_tasks if t.year == 2017)
        assert t2017.input_path == "data/acs/acs_special_2017.parquet"

    def test_path_template_used_when_no_override(self):
        recipe = load_recipe(_segmented_recipe())
        plan = resolve_plan(recipe, "main")
        acs_tasks = [t for t in plan.resample_tasks if t.dataset_id == "acs"]
        t2018 = next(t for t in acs_tasks if t.year == 2018)
        assert t2018.input_path == "data/acs/acs_2018.parquet"

    def test_static_path_dataset(self):
        recipe = load_recipe(_segmented_recipe())
        plan = resolve_plan(recipe, "main")
        pit_tasks = [t for t in plan.resample_tasks if t.dataset_id == "pit"]
        assert len(pit_tasks) == 5
        for t in pit_tasks:
            assert t.input_path == "data/pit/pit.parquet"


# ===========================================================================
# Planner: via:auto transform selection
# ===========================================================================


class TestPlannerAutoTransform:

    def test_auto_selects_2010_crosswalk_for_2017(self):
        recipe = load_recipe(_segmented_recipe())
        plan = resolve_plan(recipe, "main")
        acs_tasks = [t for t in plan.resample_tasks if t.dataset_id == "acs"]
        t2017 = next(t for t in acs_tasks if t.year == 2017)
        assert t2017.transform_id == "coc_to_tract_2010"

    def test_auto_selects_2020_crosswalk_for_2021(self):
        recipe = load_recipe(_segmented_recipe())
        plan = resolve_plan(recipe, "main")
        acs_tasks = [t for t in plan.resample_tasks if t.dataset_id == "acs"]
        t2021 = next(t for t in acs_tasks if t.year == 2021)
        assert t2021.transform_id == "coc_to_tract_2020"

    def test_identity_has_no_transform(self):
        recipe = load_recipe(_segmented_recipe())
        plan = resolve_plan(recipe, "main")
        pit_tasks = [t for t in plan.resample_tasks if t.dataset_id == "pit"]
        for t in pit_tasks:
            assert t.transform_id is None


# ===========================================================================
# Planner: error cases
# ===========================================================================


class TestPlannerErrors:

    def test_missing_segment_coverage(self):
        """Year outside all segments should fail."""
        data = _segmented_recipe()
        # Universe includes 2014 but no segment covers it
        data["universe"] = {"range": "2014-2016"}
        recipe = load_recipe(data)
        with pytest.raises(PlannerError, match="no file_set segment covering year 2014"):
            resolve_plan(recipe, "main")

    def test_ambiguous_auto_transform(self):
        """Two transforms matching the same endpoints should fail."""
        data = _segmented_recipe()
        # Add a duplicate transform for tract@2010
        data["transforms"].append({
            "id": "coc_to_tract_2010_alt",
            "type": "crosswalk",
            "from": {"type": "coc", "vintage": 2025},
            "to": {"type": "tract", "vintage": 2010},
            "spec": {"weighting": {"scheme": "population"}},
        })
        recipe = load_recipe(data)
        with pytest.raises(PlannerError, match="multiple compatible transforms"):
            resolve_plan(recipe, "main")

    def test_no_matching_transform(self):
        """No transform matching endpoints should fail."""
        data = _segmented_recipe()
        # Remove the 2010 transform but keep the 2010 segment
        data["transforms"] = [
            t for t in data["transforms"] if t["id"] != "coc_to_tract_2010"
        ]
        # Also update the materialize step
        data["pipelines"][0]["steps"][0] = {
            "materialize": {"transforms": ["coc_to_tract_2020"]},
        }
        recipe = load_recipe(data)
        with pytest.raises(PlannerError, match="no compatible transform found"):
            resolve_plan(recipe, "main")

    def test_dataset_years_coverage_violation(self):
        """Year outside declared dataset.years should fail."""
        data = _segmented_recipe()
        # PIT declares years 2017-2021 but universe asks for 2017-2021, fine.
        # Narrow PIT years so 2017 is excluded.
        data["datasets"]["pit"]["years"] = "2018-2021"
        recipe = load_recipe(data)
        with pytest.raises(PlannerError, match="year 2017 is not covered"):
            resolve_plan(recipe, "main")

    def test_dataset_years_coverage_passes(self):
        """Dataset with years covering the full universe should resolve OK."""
        data = _segmented_recipe()
        data["datasets"]["pit"]["years"] = "2017-2021"
        recipe = load_recipe(data)
        plan = resolve_plan(recipe, "main")
        pit_tasks = [t for t in plan.resample_tasks if t.dataset_id == "pit"]
        assert len(pit_tasks) == 5

    def test_unknown_pipeline_id(self):
        recipe = load_recipe(_segmented_recipe())
        with pytest.raises(PlannerError, match="Pipeline 'nonexistent' not found"):
            resolve_plan(recipe, "nonexistent")


# ===========================================================================
# Planner: plan structure
# ===========================================================================


class TestPlanStructure:

    def test_materialize_tasks(self):
        recipe = load_recipe(_segmented_recipe())
        plan = resolve_plan(recipe, "main")
        assert len(plan.materialize_tasks) == 1
        assert set(plan.materialize_tasks[0].transform_ids) == {
            "coc_to_tract_2010",
            "coc_to_tract_2020",
        }

    def test_join_tasks_per_year(self):
        recipe = load_recipe(_segmented_recipe())
        plan = resolve_plan(recipe, "main")
        assert len(plan.join_tasks) == 5  # one per year 2017-2021
        years = sorted(t.year for t in plan.join_tasks)
        assert years == [2017, 2018, 2019, 2020, 2021]
        for t in plan.join_tasks:
            assert t.datasets == ["acs", "pit"]
            assert t.join_on == ["geo_id", "year"]

    def test_resample_measures_carried(self):
        recipe = load_recipe(_segmented_recipe())
        plan = resolve_plan(recipe, "main")
        acs_tasks = [t for t in plan.resample_tasks if t.dataset_id == "acs"]
        for t in acs_tasks:
            assert t.measures == ["total_population", "median_household_income"]
