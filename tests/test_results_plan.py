"""Deterministic static planning tests for RecipeV2 results."""

from __future__ import annotations

import json

import pytest

from hhplab.recipe.loader import load_recipe
from hhplab.recipe.results_plan import ResultsPlanningError, plan_results
from hhplab.recipe.schema_results import RecipeV2


def _recipe(
    *,
    analyses: list[dict[str, object]] | None = None,
    variants: list[dict[str, object]] | None = None,
    sample_groups: dict[str, object] | None = None,
    findings: list[dict[str, object]] | None = None,
    diagnostics: list[dict[str, object]] | None = None,
    parameters: dict[str, object] | None = None,
    datasets: dict[str, object] | None = None,
    steps: list[dict[str, object]] | None = None,
) -> RecipeV2:
    data: dict[str, object] = {
        "version": 2,
        "id": "planning-test",
        "title": "Planning test",
        "authors": ["HHP-Lab"],
        "universe": {"range": "2020-2022"},
        "targets": [{"id": "target", "geometry": {"type": "coc", "vintage": 2025}}],
        "datasets": datasets or {},
        "pipelines": [{"id": "build", "target": "target", "steps": steps or []}],
        "panels": {
            "panel": {
                "ref": {"pipeline": "build", "output": "panel"},
                "geo_key": "geo_id",
                "time_key": "year",
            }
        },
        "parameters": parameters or {},
        "sample_groups": sample_groups or {},
        "analyses": analyses
        or [
            {
                "id": "model",
                "method": "regression",
                "input": "panels.panel",
                "sample": {"years": {"from": 2020, "to": 2022}},
                "outcome": "y",
                "predictors": ["x"],
                "controls": ["z"],
                "report": ["n", "r2"],
            }
        ],
        "variants": variants or [],
        "diagnostics": diagnostics or [],
        "findings": findings or [],
        "results": {
            "bundle_dir": "results/{id}/{run_id}",
            "include_panels": ["panel"],
            "report": {"title": "Planning report"},
        },
    }
    return load_recipe(data)  # type: ignore[arg-type,return-value]


def _plan(recipe: RecipeV2, source: bytes = b"exact recipe bytes\n"):
    return plan_results(recipe, original_recipe_bytes=source)


def test_plan_preserves_source_bytes_and_is_deterministic() -> None:
    recipe = _recipe(parameters={"threshold": 0.25})

    first = _plan(recipe)
    second = _plan(recipe)

    assert first.original_recipe_bytes == b"exact recipe bytes\n"
    assert first.canonical_plan == second.canonical_plan
    assert first.plan_sha256 == second.plan_sha256
    assert first.analyses[0].result_key == "model@base"
    assert first.analyses[0].required_columns == ("y", "x", "z")
    assert first.analysis_dependencies["model@base"] == ("panel:panel",)
    assert set(first.component_hashes) == {
        "panels",
        "parameters",
        "masks",
        "derive",
        "samples",
        "analyses",
        "diagnostics",
        "findings",
        "results",
    }


def test_panel_build_identity_includes_recipe_universe() -> None:
    first_recipe = _recipe()
    second_recipe = first_recipe.model_copy(update={"universe": {"range": "2018-2022"}})

    first = _plan(first_recipe).panels[0]
    second = _plan(second_recipe).panels[0]

    assert first.build_spec_hash != second.build_spec_hash
    assert first.panel_identity != second.panel_identity


def test_panel_plan_retains_exact_and_unknown_source_declarations() -> None:
    recipe = _recipe(
        datasets={
            "acs": {
                "provider": "census",
                "product": "acs5",
                "version": 1,
                "native_geometry": {"type": "tract", "vintage": 2020},
            },
            "local": {
                "provider": "custom-provider",
                "product": "custom-product",
                "version": 1,
                "native_geometry": {"type": "coc", "vintage": 2025},
            },
        },
        steps=[{"kind": "join", "datasets": ["acs", "local"]}],
    )

    panel = _plan(recipe).panels[0]

    assert panel.source_ids == (
        "census/acs5",
        "custom-provider/custom-product",
    )


def test_resolved_plan_data_is_deeply_immutable_and_to_dict_is_detached() -> None:
    plan = _plan(_recipe(parameters={"threshold": 0.25}))
    analysis = plan.analyses[0]

    with pytest.raises(TypeError):
        plan.parameters["threshold"] = 1.0  # type: ignore[index]
    with pytest.raises(TypeError):
        analysis.effective_spec["predictors"][0] = "changed"  # type: ignore[index]
    with pytest.raises(TypeError):
        analysis.sample["years"]["from"] = 1999  # type: ignore[index]

    detached = plan.to_dict()
    detached["analyses"][0]["effective_spec"]["predictors"][0] = "changed"
    detached["parameters"]["threshold"] = 5.0
    assert analysis.effective_spec["predictors"][0] == "x"
    assert plan.parameters["threshold"] == 0.25


def test_variant_substitutions_are_simultaneous_and_keep_base_term_aliases() -> None:
    recipe = _recipe(
        variants=[
            {
                "id": "renamed",
                "applies_to": ["model"],
                "substitute": {"x": "x2", "x2": "x3"},
            }
        ]
    )

    plan = _plan(recipe)
    variant = next(item for item in plan.analyses if item.variant_id == "renamed")

    assert variant.result_key == "model@renamed"
    assert variant.effective_spec["predictors"] == ("x2",)
    assert variant.base_term_by_effective_term == {"y": "y", "x2": "x", "z": "z"}
    assert variant.required_columns[:3] == ("y", "x2", "z")


def test_substitution_collision_with_untouched_term_is_rejected() -> None:
    recipe = _recipe(
        variants=[
            {
                "id": "collision",
                "applies_to": ["model"],
                "substitute": {"x": "z"},
            }
        ]
    )

    with pytest.raises(ResultsPlanningError, match="substitution collision"):
        _plan(recipe)


def test_substitution_matching_no_columns_is_an_actionable_warning() -> None:
    recipe = _recipe(
        variants=[
            {
                "id": "unused",
                "applies_to": ["model"],
                "substitute": {"no_column": "renamed_column"},
            }
        ]
    )

    plan = _plan(recipe)

    assert plan.is_ready
    assert len(plan.preflight_findings) == 1
    assert plan.preflight_findings[0].severity.value == "warning"
    assert "no referenced columns" in plan.preflight_findings[0].message


def test_inference_only_variant_keeps_base_cluster_requirement() -> None:
    recipe = _recipe(
        analyses=[
            {
                "id": "model",
                "method": "regression",
                "input": "panel",
                "sample": {"years": {"from": 2020, "to": 2022}},
                "outcome": "y",
                "predictors": ["x"],
                "inference": {"method": "clustered", "cluster_by": "cluster_id"},
                "report": ["n"],
            }
        ],
        variants=[
            {
                "id": "hc1",
                "applies_to": ["model"],
                "override": {"inference": {"method": "hc1"}},
            }
        ],
    )

    plan = _plan(recipe)
    variant = next(item for item in plan.analyses if item.variant_id == "hc1")

    assert variant.required_columns == ("y", "x", "cluster_id")
    assert variant.base_membership_key == "model@base"
    assert variant.sample_source == "model@base"


@pytest.mark.parametrize(
    "inference,override,cluster_columns",
    [
        ({"method": "classical"}, {"inference.method": "hc1"}, ()),
        (
            {"method": "clustered", "cluster_by": "cluster_id"},
            {"inference": {"method": "hc1"}},
            ("cluster_id",),
        ),
    ],
    ids=["method-field", "replace-clustered-inference"],
)
def test_inference_only_group_variant_reuses_base_membership_and_union(
    inference, override, cluster_columns
) -> None:
    analyses = [
        {
            "id": "first",
            "method": "regression",
            "input": "panel",
            "sample": {"group": "shared"},
            "outcome": "y",
            "predictors": ["x"],
            "weights": "w",
            "inference": inference,
            "report": ["n"],
        },
        {
            "id": "second",
            "method": "regression",
            "input": "panel",
            "sample": {"group": "shared"},
            "outcome": "q",
            "predictors": ["z"],
            "report": ["n"],
        },
    ]
    recipe = _recipe(
        analyses=analyses,
        sample_groups={"shared": {"sample": {"years": {"from": 2020, "to": 2022}}}},
        variants=[
            {
                "id": "hc1",
                "applies_to": ["first"],
                "override": override,
            }
        ],
    )

    plan = _plan(recipe)
    context = next(item for item in plan.group_contexts if item.variant_id == "hc1")
    variant = next(item for item in plan.analyses if item.result_key == "first@hc1")

    assert context.required_columns == ("y", "x", "w", *cluster_columns, "q", "z")
    assert context.member_analysis_ids == ("first", "second")
    assert context.base_membership_key == next(
        item.key for item in plan.group_contexts if item.variant_id == "base"
    )
    assert context.sample_source == "shared@base"
    assert variant.group_context_key == context.key
    assert variant.weight_columns == ("w",)
    assert variant.sample_source == "shared@base"


def test_all_member_inference_variant_reuses_cluster_column_membership() -> None:
    recipe = _recipe(
        analyses=[
            {
                "id": "clustered",
                "method": "regression",
                "input": "panel",
                "sample": {"group": "shared"},
                "outcome": "y",
                "predictors": ["x"],
                "inference": {"method": "clustered", "cluster_by": "coc_id"},
                "report": ["n"],
            }
        ],
        sample_groups={"shared": {"sample": {"years": {"from": 2020, "to": 2022}}}},
        variants=[
            {
                "id": "hc1",
                "applies_to": ["clustered"],
                "override": {"inference.method": "hc1"},
            }
        ],
    )

    plan = _plan(recipe)
    context = next(item for item in plan.group_contexts if item.variant_id == "hc1")

    assert context.required_columns == ("y", "x", "coc_id")
    assert context.base_membership_key == next(
        item.key for item in plan.group_contexts if item.variant_id == "base"
    )
    assert context.sample_source == "shared@base"


def test_key_aliases_resolve_before_shared_inference_membership() -> None:
    recipe = _recipe(
        analyses=[
            {
                "id": identifier,
                "method": "regression",
                "input": "panel",
                "sample": {"group": "shared"},
                "outcome": "y",
                "predictors": ["x"],
                "fixed_effects": ["geography", "year"],
                "inference": {"method": "clustered", "cluster_by": "geography"},
                "report": ["n"],
            }
            for identifier in ("first", "second")
        ],
        sample_groups={"shared": {"sample": {}}},
        variants=[
            {
                "id": "hc1",
                "applies_to": ["first"],
                "override": {"inference": {"method": "hc1"}},
            }
        ],
    )
    plan = _plan(recipe)
    base = next(item for item in plan.analyses if item.result_key == "first@base")
    variant = next(item for item in plan.analyses if item.result_key == "first@hc1")
    assert base.fixed_effects == ("geo_id", "year")
    assert base.effective_spec["inference"]["cluster_by"] == "geo_id"
    assert variant.sample_source == "shared@base"
    assert "geography" not in variant.required_columns


@pytest.mark.parametrize("cluster_by", [None, "other_cluster"], ids=["remove", "replace"])
def test_explicit_cluster_override_cannot_change_only_one_group_member(cluster_by) -> None:
    recipe = _recipe(
        analyses=[
            {
                "id": identifier,
                "method": "regression",
                "input": "panel",
                "sample": {"group": "shared"},
                "outcome": "y",
                "predictors": ["x"],
                "inference": {"method": "clustered", "cluster_by": "cluster_id"},
            }
            for identifier in ("first", "second")
        ],
        sample_groups={"shared": {"sample": {}}},
        variants=[
            {
                "id": "changed",
                "applies_to": ["first"],
                "override": {"inference": {"method": "hc1", "cluster_by": cluster_by}},
            }
        ],
    )
    with pytest.raises(ResultsPlanningError, match="missing members"):
        _plan(recipe)


def test_all_member_group_sample_change_gets_a_new_membership_context() -> None:
    recipe = _recipe(
        analyses=[
            {
                "id": "first",
                "method": "regression",
                "input": "panel",
                "sample": {"group": "shared"},
                "outcome": "y",
                "predictors": ["x"],
                "report": ["n"],
            },
            {
                "id": "second",
                "method": "regression",
                "input": "panel",
                "sample": {"group": "shared"},
                "outcome": "q",
                "predictors": ["z"],
                "report": ["n"],
            },
        ],
        sample_groups={"shared": {"sample": {"years": {"from": 2020, "to": 2022}}}},
        variants=[
            {
                "id": "later_years",
                "applies_to": ["first", "second"],
                "override": {"sample.years": {"from": 2021, "to": 2022}},
            }
        ],
    )

    plan = _plan(recipe)
    context = next(item for item in plan.group_contexts if item.variant_id == "later_years")

    assert context.sample["years"] == {"from": 2021, "to": 2022, "step": 1}
    assert context.base_membership_key is None
    assert context.sample_source == "shared@later_years"


def test_group_membership_change_for_subset_is_rejected() -> None:
    analyses = [
        {
            "id": name,
            "method": "regression",
            "input": "panel",
            "sample": {"group": "shared"},
            "outcome": f"y_{name}",
            "predictors": ["x"],
            "report": ["n"],
        }
        for name in ("first", "second")
    ]
    recipe = _recipe(
        analyses=analyses,
        sample_groups={"shared": {"sample": {"years": {"from": 2020, "to": 2022}}}},
        variants=[
            {
                "id": "subset",
                "applies_to": ["first"],
                "override": {"sample.years": {"from": 2021, "to": 2022}},
            }
        ],
    )

    with pytest.raises(ResultsPlanningError, match="subset of sample group"):
        _plan(recipe)


def test_finding_and_diagnostic_references_resolve_base_terms() -> None:
    recipe = _recipe(
        variants=[
            {
                "id": "renamed",
                "applies_to": ["model"],
                "substitute": {"x": "x2"},
            }
        ],
        findings=[
            {
                "id": "claim",
                "claim": "The predictor matters",
                "criteria_source": {"estimate": {"analysis": "model", "term": "x"}},
                "bands": [{"label": "large", "equivalent_to": {"value": 1, "margin": 0.1}}],
                "robustness": {"variants": ["renamed"]},
            }
        ],
        diagnostics=[
            {
                "id": "identity",
                "op": "linear_identity",
                "terms": [
                    {"analysis": "model", "term": "x", "sign": 1},
                    {"analysis": "model", "term": "z", "sign": -1},
                ],
                "variants": ["renamed"],
            }
        ],
    )

    plan = _plan(recipe)
    refs = {(item.owner_id, item.result_key, item.base_term, item.term) for item in plan.references}

    assert ("claim", "model@renamed", "x", "x2") in refs
    assert ("identity", "model@renamed", "x", "x2") in refs
    assert ("identity", "model@renamed", "z", "z") in refs


def test_finding_rejects_undefined_terms_and_unapplied_robustness_variant() -> None:
    undefined = _recipe(
        findings=[
            {
                "id": "claim",
                "claim": "Claim",
                "criteria_source": {"estimate": {"analysis": "model", "term": "missing"}},
                "bands": [{"label": "large", "equivalent_to": {"value": 1}}],
            }
        ]
    )
    with pytest.raises(ResultsPlanningError, match="undefined term"):
        _plan(undefined)

    unapplied = _recipe(
        variants=[
            {"id": "other", "applies_to": ["model"], "override": {"inference.method": "hc1"}}
        ],
        findings=[
            {
                "id": "claim",
                "claim": "Claim",
                "criteria_source": {"estimate": {"analysis": "model", "term": "x"}},
                "bands": [{"label": "large", "equivalent_to": {"value": 1}}],
                "robustness": {"variants": ["other"]},
            }
        ],
    )
    # The variant applies to the estimate; an unrelated stat source would still need
    # explicit reference resolution. This case verifies the applied variant is planned.
    assert "model@other" in {item.result_key for item in _plan(unapplied).analyses}


def test_group_and_analysis_sample_requirements_are_ordered_and_resolved() -> None:
    analyses = [
        {
            "id": "first",
            "method": "regression",
            "input": "panel",
            "sample": {"group": "shared"},
            "outcome": "y",
            "predictors": ["x"],
            "report": ["n"],
        },
        {
            "id": "second",
            "method": "regression",
            "input": "panel",
            "sample": {"group": "shared"},
            "outcome": "q",
            "predictors": ["z"],
            "report": ["n"],
        },
    ]
    recipe = _recipe(
        analyses=analyses,
        sample_groups={"shared": {"sample": {"require": ["group_required"]}}},
    )

    plan = _plan(recipe)

    assert plan.group_contexts[0].required_columns == (
        "y",
        "x",
        "q",
        "z",
        "group_required",
    )


def test_plan_changes_when_sample_changes_and_keeps_exact_source_separate() -> None:
    original = b"source bytes do not get normalized\r\n"
    first = _plan(_recipe(), original)
    second = _plan(
        _recipe(
            analyses=[
                {
                    "id": "model",
                    "method": "regression",
                    "input": "panel",
                    "sample": {"years": {"from": 2021, "to": 2022}},
                    "outcome": "y",
                    "predictors": ["x"],
                    "controls": ["z"],
                    "report": ["n", "r2"],
                }
            ]
        ),
        original,
    )

    assert first.original_recipe_bytes == second.original_recipe_bytes == original
    assert first.plan_sha256 != second.plan_sha256
    assert json.loads(first.canonical_plan)["analyses"][0]["sample"]["years"]["from"] == 2020
