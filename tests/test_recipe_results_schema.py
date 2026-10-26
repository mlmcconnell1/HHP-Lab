"""RecipeV2 schema dispatch and closed-contract validation tests."""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

from hhplab.recipe.loader import RecipeLoadError, load_recipe
from hhplab.recipe.recipe_schema import RecipeV1
from hhplab.recipe.schema_results import RecipeV2

V2_BUILD_BASE_FIELDS = {
    "universe": {"range": "2010-2024"},
    "targets": [
        {"id": "cbsa_target", "geometry": {"type": "cbsa", "vintage": 2020}},
        {"id": "coc_target", "geometry": {"type": "coc", "vintage": 2025}},
    ],
    "datasets": {},
}


def _minimal_v1() -> dict:
    return {
        "version": 1,
        "name": "legacy-recipe",
        "universe": {"range": "2020-2022"},
        "targets": [{"id": "coc_panel", "geometry": {"type": "coc", "vintage": 2025}}],
        "datasets": {
            "acs": {
                "provider": "census",
                "product": "acs5",
                "version": 1,
                "native_geometry": {"type": "tract", "vintage": 2020},
            }
        },
    }


def _minimal_v2() -> dict:
    """Small V2 recipe with an intentionally unresolved panel for preflight."""
    recipe = _minimal_v1()
    recipe.pop("name")
    recipe.update(
        {
            "version": 2,
            "id": "results-recipe",
            "title": "Results recipe",
            "authors": ["HHP-Lab"],
            "panels": {"analysis_panel": {"ref": {"unresolved": "connect build output"}}},
            "analyses": [
                {
                    "id": "summary",
                    "method": "descriptive",
                    "input": "panels.analysis_panel",
                    "sample": {"years": {"from": 2020, "to": 2022}},
                    "columns": ["population"],
                    "stats": ["n", "mean"],
                }
            ],
            "results": {
                "bundle_dir": "results/{id}/{run_id}",
                "include_panels": ["analysis_panel"],
                "report": {"title": "Results report"},
            },
        }
    )
    return recipe


def test_recipe_v1_still_loads_as_v1_with_legacy_name() -> None:
    recipe = load_recipe(_minimal_v1())

    assert isinstance(recipe, RecipeV1)
    assert not isinstance(recipe, RecipeV2)
    assert recipe.name == "legacy-recipe"
    assert recipe.version == 1


def test_recipe_v2_dispatch_preserves_id_and_legacy_name_property() -> None:
    recipe = load_recipe(_minimal_v2())

    assert isinstance(recipe, RecipeV2)
    assert recipe.id == "results-recipe"
    assert recipe.name == "results-recipe"
    assert recipe.model_dump(by_alias=True)["id"] == "results-recipe"
    assert recipe.unresolved_values() == (
        ("recipe.panels.analysis_panel.ref", "connect build output"),
    )


def test_phase_one_spec_example_loads_with_unresolved_items_available_to_preflight() -> None:
    spec_path = Path(__file__).parents[1] / "docs" / "results-recipe-spec_2.md"
    source = spec_path.read_text(encoding="utf-8")
    match = re.search(
        r"```yaml\n(version: 2\nid: shelter-capacity-substitution.*?\n)```",
        source,
        re.DOTALL,
    )
    assert match is not None
    data = yaml.safe_load(match.group(1))
    recipe = load_recipe(data)

    assert isinstance(recipe, RecipeV2)
    assert recipe.id == "shelter-capacity-substitution"
    assert len(recipe.analyses) == 6
    assert len(recipe.derive) == 23
    assert len(recipe.unresolved_values()) == 5


@pytest.mark.parametrize("version", [True, False])
def test_boolean_versions_are_not_accepted_as_integers(version: bool) -> None:
    recipe = _minimal_v1()
    recipe["version"] = version

    with pytest.raises(RecipeLoadError, match="version.*must be an integer"):
        load_recipe(recipe)


@pytest.mark.parametrize(
    ("path", "value", "message"),
    [
        (("unexpected",), True, "Extra inputs are not permitted"),
        (("analyses", 0, "unknown"), True, "Extra inputs are not permitted"),
        (("analyses", 0, "ci_level"), 1.0, "less than 1"),
        (("analyses", 0, "ci_level"), 0.0, "greater than 0"),
    ],
    ids=["unknown-top-level", "unknown-analysis-key", "ci-level-one", "ci-level-zero"],
)
def test_v2_rejects_unknown_keys_and_invalid_confidence(
    path: tuple[str | int, ...], value: object, message: str
) -> None:
    recipe = _minimal_v2()
    if path[-1] == "ci_level":
        recipe["analyses"][0] = {
            "id": "summary",
            "method": "regression",
            "input": "analysis_panel",
            "sample": {"years": {"from": 2020, "to": 2022}},
            "outcome": "y",
            "predictors": ["x"],
            "ci_level": 0.95,
        }
    target = recipe
    for part in path[:-1]:
        target = target[part]  # type: ignore[index]
    target[path[-1]] = value  # type: ignore[index]

    with pytest.raises(RecipeLoadError, match=message):
        load_recipe(recipe)


def test_v2_rejects_unknown_nested_variant_override_path() -> None:
    recipe = _minimal_v2()
    recipe["variants"] = [
        {
            "id": "broken",
            "applies_to": ["summary"],
            "override": {"sample.yeers": {"from": 2020, "to": 2022}},
        }
    ]

    with pytest.raises(RecipeLoadError, match="unsupported variant override path"):
        load_recipe(recipe)


@pytest.mark.parametrize(
    ("kind", "value", "message"),
    [
        ("analysis", "   ", "analysis id must contain a non-whitespace character"),
        ("analysis", "fit@base", "analysis id may not contain the reserved"),
        ("variant", "   ", "variant id must contain a non-whitespace character"),
        ("variant", "base", "variant id 'base' is reserved"),
        ("variant", "alt@run", "variant id may not contain the reserved"),
    ],
    ids=[
        "blank-analysis",
        "analysis-delimiter",
        "blank-variant",
        "base-variant",
        "variant-delimiter",
    ],
)
def test_result_key_ids_reject_reserved_or_empty_values(
    kind: str, value: str, message: str
) -> None:
    recipe = _minimal_v2()
    if kind == "analysis":
        recipe["analyses"][0]["id"] = value
    else:
        recipe["variants"] = [{"id": value, "applies_to": ["summary"]}]

    with pytest.raises(RecipeLoadError, match=message):
        load_recipe(recipe)


def test_v2_rejects_two_names_for_the_same_panel_output() -> None:
    recipe = _minimal_v2()
    recipe["pipelines"] = [{"id": "build", "target": "coc_panel", "steps": []}]
    recipe["panels"] = {
        "first": {
            "ref": {"pipeline": "build", "output": "panel"},
            "geo_key": "geo",
            "time_key": "year",
        },
        "second": {
            "ref": {"pipeline": "build", "output": "panel"},
            "geo_key": "geo",
            "time_key": "year",
        },
    }
    recipe["analyses"][0]["input"] = "panels.first"
    recipe["results"]["include_panels"] = ["first"]

    with pytest.raises(RecipeLoadError, match="assigned to more than one name"):
        load_recipe(recipe)


@pytest.mark.parametrize(
    ("method", "fields", "message"),
    [
        (
            "regression",
            {
                "outcome": "y",
                "predictors": ["x"],
                "estimator": "2sls",
                "ci_level": 0.95,
            },
            "capability_unavailable: 2sls",
        ),
        (
            "lagged_association",
            {"outcome": "y", "predictors": ["x"]},
            "capability_unavailable: lagged_association",
        ),
    ],
    ids=["two-stage-least-squares", "lagged-association"],
)
def test_phase_two_analysis_methods_fail_with_capability_error(
    method: str, fields: dict, message: str
) -> None:
    recipe = _minimal_v2()
    analysis = recipe["analyses"][0]
    analysis.clear()
    analysis.update(
        {
            "id": "unsupported",
            "method": method,
            "input": "analysis_panel",
            "sample": {"years": {"from": 2020, "to": 2022}},
            **fields,
        }
    )

    with pytest.raises(RecipeLoadError, match=message):
        load_recipe(recipe)


def test_unresolved_parameter_reference_is_parseable_and_unknown_parameter_is_rejected() -> None:
    recipe = _minimal_v2()
    recipe["parameters"] = {"margin": {"unresolved": "choose margin"}}
    recipe["findings"] = [
        {
            "id": "claim",
            "claim": "claim text",
            "criteria_source": {"estimate": {"analysis": "summary", "term": "x"}},
            "bands": [
                {
                    "label": "supported",
                    "equivalent_to": {
                        "value": 0,
                        "margin": {"param": "margin"},
                    },
                }
            ],
        }
    ]

    parsed = load_recipe(recipe)
    assert isinstance(parsed, RecipeV2)
    assert any(description == "choose margin" for _, description in parsed.unresolved_values())

    recipe["findings"][0]["bands"][0]["equivalent_to"]["margin"] = {"param": "missing"}
    with pytest.raises(RecipeLoadError, match="unknown parameter 'missing'"):
        load_recipe(recipe)


def test_cyclic_derived_columns_are_rejected() -> None:
    recipe = _minimal_v2()
    recipe["derive"] = [
        {"id": "first", "op": "ratio", "num": "second", "den": "base"},
        {"id": "second", "op": "ratio", "num": "first", "den": "base"},
    ]

    with pytest.raises(RecipeLoadError, match="derive dependency cycle"):
        load_recipe(recipe)


def test_nonpositive_sample_interval_and_negative_margin_are_rejected() -> None:
    bad_interval = _minimal_v2()
    bad_interval["analyses"][0]["sample"]["years"] = {
        "from": 2020,
        "to": 2022,
        "step": 0,
    }
    with pytest.raises(RecipeLoadError, match="greater than or equal to 1"):
        load_recipe(bad_interval)

    bad_margin = _minimal_v2()
    bad_margin["parameters"] = {"margin": -0.1}
    bad_margin["findings"] = [
        {
            "id": "claim",
            "claim": "claim text",
            "criteria_source": {"estimate": {"analysis": "summary", "term": "x"}},
            "bands": [
                {
                    "label": "supported",
                    "equivalent_to": {"value": 0, "margin": {"param": "margin"}},
                }
            ],
        }
    ]
    with pytest.raises(RecipeLoadError, match="margin must be nonnegative"):
        load_recipe(bad_margin)


def test_finding_criteria_must_reference_a_known_analysis() -> None:
    recipe = _minimal_v2()
    recipe["findings"] = [
        {
            "id": "claim",
            "claim": "claim text",
            "criteria_source": {"estimate": {"analysis": "missing", "term": "x"}},
            "bands": [{"label": "defined", "ci_excludes": 0}],
        }
    ]

    with pytest.raises(RecipeLoadError, match="references unknown analysis 'missing'"):
        load_recipe(recipe)
