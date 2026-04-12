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
                "years": "2015-2024",
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

    def test_path_template_supports_segment_constants_and_offsets(self):
        data = _segmented_recipe()
        data["datasets"]["acs"]["file_set"]["path_template"] = (
            "data/curated/measures/measures__A{acs_end}@B{boundary}xT{tract}.parquet"
        )
        data["datasets"]["acs"]["file_set"]["segments"][0]["constants"] = {"tract": 2010}
        data["datasets"]["acs"]["file_set"]["segments"][0]["year_offsets"] = {
            "acs_end": -1,
            "boundary": 0,
        }
        data["datasets"]["acs"]["file_set"]["segments"][1]["constants"] = {"tract": 2020}
        data["datasets"]["acs"]["file_set"]["segments"][1]["year_offsets"] = {
            "acs_end": -1,
            "boundary": 0,
        }

        recipe = load_recipe(data)
        plan = resolve_plan(recipe, "main")
        acs_tasks = [t for t in plan.resample_tasks if t.dataset_id == "acs"]

        t2018 = next(t for t in acs_tasks if t.year == 2018)
        assert (
            t2018.input_path
            == "data/curated/measures/measures__A2017@B2018xT2010.parquet"
        )

        t2021 = next(t for t in acs_tasks if t.year == 2021)
        assert (
            t2021.input_path
            == "data/curated/measures/measures__A2020@B2021xT2020.parquet"
        )


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

    def test_method_parameter_forwarded(self):
        """Regression coclab-im99: method parameter is forwarded to
        _resolve_auto_transform for future direction validation."""
        data = _segmented_recipe()
        # Changing method to allocate should still resolve because
        # crosswalks are symmetric — both directions match.
        for step in data["pipelines"][0]["steps"]:
            if "resample" in step and step["resample"].get("dataset") == "acs":
                step["resample"]["method"] = "allocate"
                break
        recipe = load_recipe(data)
        plan = resolve_plan(recipe, "main")
        alloc_tasks = [
            t for t in plan.resample_tasks
            if t.dataset_id == "acs" and t.method == "allocate"
        ]
        assert len(alloc_tasks) > 0
        assert alloc_tasks[0].transform_id is not None


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

    def test_missing_template_variable_raises_planner_error(self):
        recipe = load_recipe(_segmented_recipe())
        recipe.datasets["acs"].file_set.path_template = "data/acs/acs_{missing_var}.parquet"
        with pytest.raises(PlannerError, match="missing variable 'missing_var'"):
            resolve_plan(recipe, "main")


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


# ===========================================================================
# Planner: vintage sets in file_set paths (integration)
# ===========================================================================


def _vintage_set_recipe() -> dict:
    """Recipe with vintage_sets driving file_set segment constants/year_offsets.

    The vintage set ``acs_measures`` defines four dimensions across two
    year bands (2017-2019 using tract@2010, 2020-2022 using tract@2020).
    The file_set segments mirror these values so the planner resolves
    paths like ``data/curated/measures__A2016@B2017xT2010.parquet``.
    """
    return {
        "version": 1,
        "name": "vintage-set-integration",
        "universe": {"range": "2017-2022"},
        "targets": [
            {"id": "coc_panel", "geometry": {"type": "coc", "vintage": 2025}},
        ],
        "vintage_sets": {
            "acs_measures": {
                "dimensions": ["analysis_year", "acs_end", "boundary", "tract"],
                "rules": [
                    {
                        "years": "2015-2019",
                        "constants": {"tract": 2010},
                        "year_offsets": {
                            "analysis_year": 0,
                            "acs_end": -1,
                            "boundary": 0,
                        },
                    },
                    {
                        "years": "2020-2024",
                        "constants": {"tract": 2020},
                        "year_offsets": {
                            "analysis_year": 0,
                            "acs_end": -1,
                            "boundary": 0,
                        },
                    },
                ],
            },
        },
        "datasets": {
            "acs": {
                "provider": "census",
                "product": "acs",
                "version": 1,
                "native_geometry": {"type": "tract"},
                "file_set": {
                    "path_template": (
                        "data/curated/measures__A{acs_end}@B{boundary}xT{tract}.parquet"
                    ),
                    "segments": [
                        {
                            "years": {"range": "2015-2019"},
                            "geometry": {"type": "tract", "vintage": 2010},
                            "constants": {"tract": 2010},
                            "year_offsets": {
                                "acs_end": -1,
                                "boundary": 0,
                            },
                        },
                        {
                            "years": {"range": "2020-2024"},
                            "geometry": {"type": "tract", "vintage": 2020},
                            "constants": {"tract": 2020},
                            "year_offsets": {
                                "acs_end": -1,
                                "boundary": 0,
                            },
                        },
                    ],
                },
            },
            "pit": {
                "provider": "hud",
                "product": "pit",
                "version": 1,
                "native_geometry": {"type": "coc"},
                "years": "2017-2022",
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
                            "transforms": [
                                "coc_to_tract_2010",
                                "coc_to_tract_2020",
                            ],
                        },
                    },
                    {
                        "resample": {
                            "dataset": "acs",
                            "to_geometry": {"type": "coc", "vintage": 2025},
                            "method": "aggregate",
                            "via": "auto",
                            "measures": ["total_population"],
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


class TestPlannerVintageSetIntegration:
    """Integration tests: full pipeline plan with vintage-set-derived file_set paths."""

    def test_full_plan_resolves_all_years(self):
        """Plan resolves one resample task per dataset per universe year."""
        recipe = load_recipe(_vintage_set_recipe())
        plan = resolve_plan(recipe, "main")
        assert isinstance(plan, ExecutionPlan)

        acs_tasks = [t for t in plan.resample_tasks if t.dataset_id == "acs"]
        pit_tasks = [t for t in plan.resample_tasks if t.dataset_id == "pit"]
        assert len(acs_tasks) == 6  # 2017-2022
        assert len(pit_tasks) == 6

    def test_vintage_set_constants_produce_correct_paths(self):
        """Segment constants from vintage set yield correctly rendered paths."""
        recipe = load_recipe(_vintage_set_recipe())
        plan = resolve_plan(recipe, "main")
        acs_tasks = {
            t.year: t
            for t in plan.resample_tasks
            if t.dataset_id == "acs"
        }

        # Pre-2020 band: tract=2010, acs_end=year-1, boundary=year
        assert (
            acs_tasks[2017].input_path
            == "data/curated/measures__A2016@B2017xT2010.parquet"
        )
        assert (
            acs_tasks[2018].input_path
            == "data/curated/measures__A2017@B2018xT2010.parquet"
        )
        assert (
            acs_tasks[2019].input_path
            == "data/curated/measures__A2018@B2019xT2010.parquet"
        )

        # Post-2020 band: tract=2020, acs_end=year-1, boundary=year
        assert (
            acs_tasks[2020].input_path
            == "data/curated/measures__A2019@B2020xT2020.parquet"
        )
        assert (
            acs_tasks[2021].input_path
            == "data/curated/measures__A2020@B2021xT2020.parquet"
        )
        assert (
            acs_tasks[2022].input_path
            == "data/curated/measures__A2021@B2022xT2020.parquet"
        )

    def test_vintage_set_geometry_drives_transform_selection(self):
        """Auto-selected transform matches the segment geometry vintage."""
        recipe = load_recipe(_vintage_set_recipe())
        plan = resolve_plan(recipe, "main")
        acs_tasks = {
            t.year: t
            for t in plan.resample_tasks
            if t.dataset_id == "acs"
        }

        # 2017-2019 -> tract@2010 -> coc_to_tract_2010
        for yr in (2017, 2018, 2019):
            assert acs_tasks[yr].effective_geometry.vintage == 2010
            assert acs_tasks[yr].transform_id == "coc_to_tract_2010"

        # 2020-2022 -> tract@2020 -> coc_to_tract_2020
        for yr in (2020, 2021, 2022):
            assert acs_tasks[yr].effective_geometry.vintage == 2020
            assert acs_tasks[yr].transform_id == "coc_to_tract_2020"

    def test_plan_to_dict_includes_all_tasks(self):
        """Serialized plan has correct task count and structure."""
        recipe = load_recipe(_vintage_set_recipe())
        plan = resolve_plan(recipe, "main")
        d = plan.to_dict()

        assert d["pipeline_id"] == "main"
        # 1 materialize + 12 resample (6 acs + 6 pit) + 6 join = 19
        assert d["task_count"] == 19
        assert len(d["materialize_tasks"]) == 1
        assert len(d["resample_tasks"]) == 12
        assert len(d["join_tasks"]) == 6

    def test_join_tasks_span_full_universe(self):
        """A join task is emitted for every universe year."""
        recipe = load_recipe(_vintage_set_recipe())
        plan = resolve_plan(recipe, "main")
        join_years = sorted(t.year for t in plan.join_tasks)
        assert join_years == [2017, 2018, 2019, 2020, 2021, 2022]

    def test_vintage_set_declared_but_not_referenced_still_valid(self):
        """A vintage_set that exists in the recipe but is not wired into any
        file_set does not cause errors; the plan resolves normally."""
        data = _vintage_set_recipe()
        data["vintage_sets"]["unused_set"] = {
            "dimensions": ["x"],
            "rules": [{"years": "2020-2024", "year_offsets": {"x": 0}}],
        }
        recipe = load_recipe(data)
        plan = resolve_plan(recipe, "main")
        acs_tasks = [t for t in plan.resample_tasks if t.dataset_id == "acs"]
        assert len(acs_tasks) == 6

    def test_segment_with_override_bypasses_vintage_set_path(self):
        """A per-year override in a segment takes precedence even when
        vintage-set-derived constants are present."""
        data = _vintage_set_recipe()
        data["datasets"]["acs"]["file_set"]["segments"][0]["overrides"] = {
            2018: "data/acs/special_2018.parquet",
        }
        recipe = load_recipe(data)
        plan = resolve_plan(recipe, "main")
        acs_tasks = {
            t.year: t
            for t in plan.resample_tasks
            if t.dataset_id == "acs"
        }
        # Override year gets the literal path
        assert acs_tasks[2018].input_path == "data/acs/special_2018.parquet"
        # Adjacent years still render via template
        assert (
            acs_tasks[2017].input_path
            == "data/curated/measures__A2016@B2017xT2010.parquet"
        )
        assert (
            acs_tasks[2019].input_path
            == "data/curated/measures__A2018@B2019xT2010.parquet"
        )
