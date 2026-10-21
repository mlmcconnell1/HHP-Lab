"""Truth tables for exact sample membership and its exclusion audit."""

from __future__ import annotations

import pandas as pd
import pytest

from hhplab.analysis.sample import (
    SampleResolutionError,
    group_required_columns,
    resolve_sample,
)
from hhplab.recipe.loader import load_recipe
from hhplab.recipe.results_plan import plan_results

KEYS = ("geo_id", "year")
SAMPLE_ROWS = [
    {"geo_id": "A", "year": 2020, "outcome": 1.0, "weight": 1.0, "policy": 0},
    {"geo_id": "A", "year": 2021, "outcome": 2.0, "weight": 1.0, "policy": 1},
    {"geo_id": "B", "year": 2020, "outcome": 3.0, "weight": 2.0, "policy": 0},
    {"geo_id": "B", "year": 2021, "outcome": 4.0, "weight": 2.0, "policy": 0},
]
SAMPLE_FRAME = pd.DataFrame(SAMPLE_ROWS)


def plan(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "analysis_id": "synthetic",
        "variant_id": "base",
        "result_key": "synthetic@base",
        "panel_name": "cbsa",
        "panel_identity": "panel-identity-1",
        "geo_key": "geo_id",
        "time_key": "year",
        "required_columns": ["outcome", "weight"],
        "numeric_columns": ["outcome", "weight"],
        "weight_columns": [],
        "fixed_effects": [],
        "predicate_columns": [],
        "sample": {},
        "sample_source": "synthetic@base",
    }
    values.update(overrides)
    if "required_columns" in overrides and "numeric_columns" not in overrides:
        values["numeric_columns"] = [
            column for column in ("outcome", "weight") if column in values["required_columns"]
        ]
    return values


def test_group_required_columns_keep_declaration_and_inference_order() -> None:
    assert group_required_columns(
        [["y", "x", "weight"], ["y", "control", "cluster"]], ["extra", "x"]
    ) == ("y", "x", "weight", "control", "cluster", "extra")


def test_planned_weighted_regression_resolves_panel_key_aliases_and_weight() -> None:
    recipe = load_recipe(
        {
            "version": 2,
            "id": "weighted-sample-plan",
            "title": "Weighted sample planning fixture",
            "authors": ["test"],
            "universe": {"range": "2020-2021"},
            "targets": [{"id": "target", "geometry": {"type": "coc", "vintage": 2025}}],
            "datasets": {},
            "pipelines": [{"id": "build", "target": "target", "steps": []}],
            "panels": {
                "panel": {
                    "ref": {"pipeline": "build", "output": "panel"},
                    "geo_key": "geo_id",
                    "time_key": "year",
                }
            },
            "analyses": [
                {
                    "id": "fit",
                    "method": "regression",
                    "input": "panels.panel",
                    "outcome": "outcome",
                    "predictors": ["predictor"],
                    "weights": "weight",
                    "fixed_effects": ["geography"],
                    "inference": {"method": "clustered", "cluster_by": "geography"},
                    "sample": {},
                }
            ],
            "results": {
                "bundle_dir": "results/{id}/{run_id}",
                "include_panels": ["panel"],
                "report": {"title": "Weighted sample planning fixture"},
            },
        }
    )
    plan = plan_results(recipe, original_recipe_bytes=b"weighted sample plan fixture")
    analysis = next(item for item in plan.analyses if item.result_key == "fit@base")
    frame = pd.DataFrame(
        {
            "geo_id": ["A", "A", "B", "B", "C", "C", "D", "D"],
            "year": [2020, 2021] * 4,
            "outcome": [1.0, 2.0, 2.0, 4.0, 3.0, 5.0, 4.0, 8.0],
            "predictor": [1.0, 2.0, 1.5, 3.0, 2.0, 3.0, 2.5, 5.0],
            "weight": [1.0, 2.0, 1.0, 2.0, 1.0, 2.0, 1.0, 2.0],
        }
    )

    resolved = resolve_sample(frame, analysis.to_dict())

    assert analysis.fixed_effects == ("geo_id",)
    assert analysis.weight_columns == ("weight",)
    assert "weight" in analysis.numeric_columns
    assert "geo_id" in analysis.required_columns
    assert resolved["resolved"].row_keys == tuple(
        (row.geo_id, row.year) for row in frame.itertuples(index=False)
    )


def test_sample_year_geography_and_required_column_order() -> None:
    frame = SAMPLE_FRAME.copy()
    frame["optional"] = [1, 1, 1, 1]
    result = resolve_sample(
        frame,
        plan(
            required_columns=["outcome", "policy", "weight", "optional"],
            numeric_columns=["outcome", "weight", "optional"],
            sample={
                "years": {"from": 2020, "to": 2021, "step": 2},
                "exclude_years": [2020],
                "geographies": {
                    "include": ["A", "B"],
                    "exclude": {"column": "policy", "equals": 1},
                },
            },
        ),
    )

    # 2020 is in-range but explicitly excluded; 2021 is outside the stepped interval.
    assert result["membership"]["reason"].tolist() == [
        "year_excluded",
        "year_out_of_range",
        "year_excluded",
        "year_out_of_range",
    ]
    assert result["sample_source"] == "synthetic@base"


def test_first_exclusion_reason_order_and_nonfinite_weight_mapping() -> None:
    frame = pd.DataFrame(
        {
            "geo_id": ["A"] * 8,
            "year": list(range(2000, 2008)),
            "outcome": pd.Series([None, 1.0, 1.0, 1.0, 1.0, float("inf"), 1.0, 1.0], dtype=object),
            "weight": pd.Series(
                [float("nan"), 1.0, 1.0, 1.0, 1.0, float("inf"), float("nan"), 0.0],
                dtype=object,
            ),
            "policy": [0, 1, 0, 0, 0, 0, 0, 0],
            "extra": pd.Series([1, 1, 1, None, None, 1, 1, 1], dtype=object),
        }
    )
    masks = pd.DataFrame({column: [()] * len(frame) for column in frame.columns})
    masks.loc[1, "outcome"] = ("masked-outcome",)
    masks.loc[1, "extra"] = ("masked-extra",)
    masks.loc[2, "outcome"] = ("masked-outcome",)
    masks.loc[2, "extra"] = ("masked-extra",)
    masks.loc[3, "extra"] = ("masked-extra",)
    result = resolve_sample(
        frame,
        plan(
            required_columns=["outcome", "extra", "weight"],
            numeric_columns=["outcome", "extra", "weight"],
            weight_columns=["weight"],
            predicate_columns=["policy"],
            sample={"geographies": {"exclude": {"column": "policy", "equals": 1}}},
        ),
        mask_provenance=masks,
    )

    assert result["membership"]["reason"].tolist() == [
        "missing:outcome",  # storage null precedes the nonfinite weight
        "geography_excluded",  # geography takes precedence over mask and missing
        "masked:outcome",
        "masked:extra",  # mask pass precedes the null pass
        "missing:extra",
        "nonfinite:outcome",  # required numeric order precedes weight
        "invalid_weight",  # nonfinite weight has its closed reason
        "invalid_weight",  # finite nonpositive weight is checked afterward
    ]
    assert result["exclusions"] == {
        "geography_excluded": 1,
        "invalid_weight": 2,
        "masked:extra": 1,
        "masked:outcome": 1,
        "missing:extra": 1,
        "missing:outcome": 1,
        "nonfinite:outcome": 1,
    }


def test_masked_derived_null_precedes_domain_or_storage_missing() -> None:
    frame = pd.DataFrame(
        {
            "geo_id": ["A", "A"],
            "year": [2020, 2021],
            "derived": pd.Series([pd.NA, pd.NA], dtype=object),
        }
    )
    masks = pd.DataFrame({column: [()] * len(frame) for column in frame.columns})
    masks.loc[0, "derived"] = ("masked-input",)
    result = resolve_sample(
        frame,
        plan(
            required_columns=["derived"],
            sample_source="group@base",
        ),
        mask_provenance=masks,
    )

    assert result["membership"]["reason"].tolist() == ["masked:derived", "missing:derived"]


def test_nan_remains_nonfinite_and_is_not_storage_null() -> None:
    frame = pd.DataFrame(
        {
            "geo_id": ["A", "B"],
            "year": [2020, 2020],
            "outcome": pd.Series([float("nan"), None], dtype=object),
        }
    )
    result = resolve_sample(
        frame,
        plan(
            required_columns=["outcome"],
            numeric_columns=["outcome"],
            weight_columns=[],
        ),
    )

    assert result["membership"]["reason"].tolist() == ["nonfinite:outcome", "missing:outcome"]


def test_nonnumeric_required_numeric_value_is_readiness_error() -> None:
    frame = pd.DataFrame({"geo_id": ["A"], "year": [2020], "outcome": ["bad"]})
    with pytest.raises(SampleResolutionError, match="nonnumeric value") as error:
        resolve_sample(
            frame,
            plan(required_columns=["outcome"], numeric_columns=["outcome"], weight_columns=[]),
        )
    assert error.value.code == "readiness_error"


@pytest.mark.parametrize(
    ("column", "nonfinite"),
    [
        pytest.param("extra", float("nan"), id="extra-require-nan"),
        pytest.param("cluster", float("inf"), id="cluster-infinity"),
    ],
)
def test_nonfinite_values_in_nonnumeric_required_roles_are_exclusions(
    column: str, nonfinite: float
) -> None:
    frame = pd.DataFrame(
        {
            "geo_id": ["A", "B"],
            "year": [2020, 2020],
            "outcome": [1.0, 2.0],
            column: pd.Series(["category", nonfinite], dtype=object),
        }
    )
    result = resolve_sample(
        frame,
        plan(
            required_columns=["outcome", column],
            numeric_columns=["outcome"],
            weight_columns=[],
        ),
    )

    assert result["membership"]["reason"].tolist() == [None, f"nonfinite:{column}"]
    assert result["resolved"].row_keys == (("A", 2020),)


def test_iterative_insufficient_observations_then_singletons_keep_first_reason() -> None:
    frame = pd.DataFrame(
        {
            "geo_id": ["A", "A", "A", "B", "B"],
            "year": [2020, 2021, 2022, 2020, 2021],
            "outcome": [1, 2, 3, 4, 5],
            "fe": ["shared", "shared", "rare", "b1", "b2"],
        }
    )
    result = resolve_sample(
        frame,
        plan(
            required_columns=["outcome", "fe"],
            numeric_columns=["outcome"],
            weight_columns=[],
            fixed_effects=["fe"],
            sample={"min_obs_per_geography": 3, "drop_singletons": True},
        ),
    )

    assert result["membership"]["reason"].tolist() == [
        "insufficient_obs",
        "insufficient_obs",
        "singleton_fe",  # singleton pass runs after the initial min-observation pass
        "insufficient_obs",  # too few rows wins over its singleton levels in that iteration
        "insufficient_obs",
    ]
    assert result["n_included"] == 0


def test_singleton_cascade_repeats_until_membership_stabilizes() -> None:
    frame = pd.DataFrame(
        {
            "geo_id": ["A", "A", "A", "B"],
            "year": [2020, 2021, 2022, 2020],
            "outcome": [1, 2, 3, 4],
            "fe_one": ["x", "x", "y", "z"],
            "fe_two": ["a", "b", "b", "a"],
        }
    )
    result = resolve_sample(
        frame,
        plan(
            required_columns=["outcome", "fe_one", "fe_two"],
            numeric_columns=["outcome"],
            weight_columns=[],
            fixed_effects=["fe_one", "fe_two"],
            sample={"min_obs_per_geography": 1, "drop_singletons": True},
        ),
    )

    assert result["membership"]["reason"].tolist() == [
        "singleton_fe",
        "singleton_fe",
        "singleton_fe",
        "singleton_fe",
    ]
    assert result["resolved"].row_keys == ()


def test_group_union_membership_is_shared_by_ordered_required_columns() -> None:
    frame = pd.DataFrame(
        {
            "geo_id": ["A", "A", "B"],
            "year": [2020, 2021, 2020],
            "outcome": [1.0, 2.0, 3.0],
            "predictor": pd.Series([1.0, 1.0, None], dtype=object),
            "control": [0.0, 1.0, 0.0],
        }
    )
    union = group_required_columns([["outcome", "predictor"], ["outcome", "control"]], ["extra"])
    frame["extra"] = [1, 1, 1]
    result = resolve_sample(
        frame,
        plan(
            required_columns=list(union),
            numeric_columns=["outcome", "predictor", "control"],
            weight_columns=[],
            sample_group_id="fd_main",
            group_context_key="fd_main|base|panel-identity-1",
            sample_source="fd_main@base",
        ),
    )

    assert result["required_columns"] == (
        "outcome",
        "predictor",
        "control",
        "extra",
    )
    assert result["membership"]["reason"].tolist() == [None, None, "missing:predictor"]
    assert set(result["membership"]["sample_source"]) == {"fd_main@base"}


def test_inference_only_variant_reuses_base_decisions_by_key_after_reordering() -> None:
    frame = SAMPLE_FRAME.copy()
    base = resolve_sample(
        frame,
        plan(
            required_columns=["outcome", "weight", "policy"],
            numeric_columns=["outcome", "weight"],
            predicate_columns=["policy"],
            group_context_key="fd_main|base|panel-identity-1",
            sample_source="fd_main@base",
            sample={"geographies": {"exclude": {"column": "policy", "equals": 1}}},
        ),
    )
    reordered = frame.iloc[[3, 1, 0, 2]].copy()
    variant = resolve_sample(
        reordered,
        plan(
            variant_id="hc1",
            result_key="fit@hc1",
            required_columns=["outcome", "weight", "policy"],
            numeric_columns=["outcome", "weight"],
            predicate_columns=["policy"],
            group_context_key="fd_main|hc1|panel-identity-1",
            base_membership_key="fd_main|base|panel-identity-1",
            sample_source="fd_main@base",
            sample={"geographies": {"exclude": {"column": "policy", "equals": 1}}},
        ),
        base_membership=base,
    )

    assert variant["membership"]["reason"].tolist() == [
        None,
        "geography_excluded",
        None,
        None,
    ]
    assert variant["resolved"].row_keys == (
        ("B", 2021),
        ("A", 2020),
        ("B", 2020),
    )
    assert variant["sample_source"] == "fd_main@base"


def test_base_reuse_rejects_different_identity_or_panel_key_set() -> None:
    base = resolve_sample(SAMPLE_FRAME, plan())
    with pytest.raises(SampleResolutionError, match="same panel identity"):
        resolve_sample(
            SAMPLE_FRAME,
            plan(
                panel_identity="different",
                base_membership_key="synthetic@base",
            ),
            base_membership=base,
        )
    with pytest.raises(SampleResolutionError, match="identical full panel key set"):
        resolve_sample(
            SAMPLE_FRAME.iloc[:-1].copy(),
            plan(base_membership_key="synthetic@base"),
            base_membership=base,
        )
    with pytest.raises(SampleResolutionError, match="explicit base_membership_key"):
        resolve_sample(SAMPLE_FRAME, plan(), base_membership=base)


def test_membership_keeps_every_key_and_exact_retained_frame_identity() -> None:
    frame = SAMPLE_FRAME.iloc[[2, 0, 3, 1]].copy()
    result = resolve_sample(
        frame,
        plan(sample={"years": [2020]}),
    )

    assert result["membership"][list(KEYS)].values.tolist() == frame[list(KEYS)].values.tolist()
    assert result["membership"]["included"].tolist() == [True, True, False, False]
    assert result["resolved"].row_keys == tuple(
        (row.geo_id, row.year) for row in frame.iloc[:2].itertuples(index=False)
    )
    assert result["n_included"] == 2


@pytest.mark.parametrize(
    ("frame", "message"),
    [
        pytest.param(
            pd.DataFrame({"geo_id": ["A", "A"], "year": [2020, 2020], "outcome": [1, 2]}),
            "duplicate keys",
            id="duplicate-panel-key",
        ),
        pytest.param(
            pd.DataFrame({"geo_id": [None], "year": [2020], "outcome": [1]}),
            "geography key is invalid",
            id="null-geography-key",
        ),
    ],
)
def test_panel_key_contract_is_validated_before_membership(
    frame: pd.DataFrame, message: str
) -> None:
    with pytest.raises(SampleResolutionError, match=message):
        resolve_sample(frame, plan(required_columns=["outcome"], numeric_columns=["outcome"]))
