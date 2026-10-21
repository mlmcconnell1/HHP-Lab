"""Truth tables for full-panel masks, derivation, and cell provenance."""

from __future__ import annotations

import math

import pandas as pd
import pytest

from hhplab.recipe.derive import DerivationError, apply_masks, derive_panel

PANEL_KEYS = {"panel_name": "cbsa", "geo_key": "geo_id", "time_key": "year"}
BASE_ROWS = [
    {"geo_id": "A", "year": 2022, "count": 5, "other": 3, "population": 10},
    {"geo_id": "B", "year": 2021, "count": 2, "other": 1, "population": 0},
    {"geo_id": "A", "year": 2020, "count": 1, "other": 1, "population": 5},
    {"geo_id": "A", "year": 2021, "count": 3, "other": 2, "population": 5},
]
BASE_FRAME = pd.DataFrame(BASE_ROWS)
SOURCE_MEASURES = {
    "count": {
        "units": "people",
        "aggregation_semantics": "extensive",
        "source_lineage": ["pit_count"],
    },
    "other": {
        "units": "people",
        "aggregation_semantics": "extensive",
        "source_lineage": ["acs_population"],
    },
    "population": {
        "units": "people",
        "aggregation_semantics": "extensive",
        "source_lineage": ["acs_population"],
    },
    "rate": {"units": "1", "aggregation_semantics": "rate", "source_lineage": ["rate"]},
}


def derive(
    frame: pd.DataFrame, specs: list[dict[str, object]], **kwargs: object
) -> dict[str, object]:
    return derive_panel(
        frame,
        specs,
        **PANEL_KEYS,
        input_measures=SOURCE_MEASURES,
        **kwargs,
    )


@pytest.mark.parametrize(
    ("op", "spec_fields", "expected"),
    [
        pytest.param("sum", {"of": ["count", "other"]}, [8, 3, 2, 5], id="sum"),
        pytest.param(
            "ratio", {"num": "count", "den": "population"}, [0.5, pd.NA, 0.2, 0.6], id="ratio"
        ),
        pytest.param(
            "per_capita",
            {"of": "count", "pop": "population", "scale": 100},
            [50, pd.NA, 20, 60],
            id="per-capita",
        ),
        pytest.param("diff", {"of": "count", "years": 2}, [4, pd.NA, pd.NA, pd.NA], id="diff"),
        pytest.param("lag", {"of": "count", "years": 2}, [1, pd.NA, pd.NA, pd.NA], id="lag"),
        pytest.param(
            "growth",
            {"of": "count", "years": 2, "kind": "pct"},
            [4, pd.NA, pd.NA, pd.NA],
            id="growth-pct",
        ),
        pytest.param(
            "growth",
            {"of": "count", "years": 2, "kind": "log"},
            [math.log(5), pd.NA, pd.NA, pd.NA],
            id="growth-log",
        ),
        pytest.param(
            "log",
            {"of": "count", "offset": 1},
            [math.log(6), math.log(3), math.log(2), math.log(4)],
            id="log",
        ),
        pytest.param("indicator", {"of": "count", "gte": 3}, [1, 0, 0, 1], id="indicator-gte"),
        pytest.param("indicator", {"of": "count", "lte": 2}, [0, 1, 1, 0], id="indicator-lte"),
        pytest.param(
            "share", {"of": "count", "total": "population"}, [0.5, pd.NA, 0.2, 0.6], id="share"
        ),
    ],
)
def test_closed_operation_set_preserves_row_order(
    op: str, spec_fields: dict[str, object], expected: list[object]
) -> None:
    result = derive(BASE_FRAME, [{"id": "value", "op": op, **spec_fields}])

    assert _null_normalized(result["frame"]["value"].tolist()) == _null_normalized(expected)
    assert result["frame"][["geo_id", "year"]].equals(BASE_FRAME[["geo_id", "year"]])
    expected_lineage = (
        ["pit_count", "acs_population"]
        if op in {"ratio", "per_capita", "share"}
        else ["pit_count", "acs_population"]
        if op == "sum"
        else ["pit_count"]
    )
    assert result["measures"]["value"]["source_lineage"] == expected_lineage


def test_masks_preserve_null_nan_and_carry_mask_ids_through_chains() -> None:
    frame = pd.DataFrame(
        {
            "geo_id": ["A", "A", "A"],
            "year": [2020, 2021, 2022],
            "count": pd.Series([1.0, float("nan"), 5.0], dtype=object),
            "other": pd.Series([None, float("nan"), 3.0], dtype=object),
        }
    )
    masked = apply_masks(
        frame,
        [
            {
                "id": "mask-2021",
                "columns": ["count"],
                "years": [2021],
                "reason": "count unavailable",
                "geographies": {"include": ["A"]},
            }
        ],
        **PANEL_KEYS,
    )
    result = derive_panel(
        masked,
        [
            {"id": "lagged", "op": "lag", "of": "count", "years": 1},
            {"id": "chain", "op": "sum", "of": ["lagged", "other"], "require_all": True},
        ],
        **PANEL_KEYS,
        input_measures={
            "count": SOURCE_MEASURES["count"],
            "other": SOURCE_MEASURES["other"],
        },
    )

    output = result["frame"]
    assert output["count"].iloc[1] is pd.NA
    assert output["count"].iloc[0] == 1.0
    assert output["other"].iloc[0] is None
    assert math.isnan(output["other"].iloc[1])
    assert _null_normalized(output["lagged"].tolist()) == [None, 1.0, None]
    assert result["mask_provenance"]["lagged"].tolist() == [(), (), ("mask-2021",)]
    assert result["mask_provenance"]["chain"].tolist() == [(), (), ("mask-2021",)]
    assert result["value_reasons"]["lagged"].tolist() == [
        "missing_baseline:count",
        None,
        "masked:lagged",
    ]
    assert result["value_reasons"]["chain"].tolist() == [
        "missing_baseline:count",
        None,
        "masked:chain",
    ]


def test_lag_reads_prior_calendar_cell_even_when_current_cell_is_null() -> None:
    frame = pd.DataFrame(
        {
            "geo_id": ["A", "A"],
            "year": [2020, 2021],
            "count": pd.Series([4.0, None], dtype=object),
        }
    )
    result = derive(
        frame,
        [{"id": "lagged", "op": "lag", "of": "count", "years": 1}],
    )

    assert _null_normalized(result["frame"]["lagged"].tolist()) == [None, 4.0]
    assert result["value_reasons"]["lagged"].tolist() == ["missing_baseline:count", None]


def test_biennial_diff_uses_exact_calendar_year_and_not_previous_row() -> None:
    frame = pd.DataFrame(
        {
            "geo_id": ["A", "A", "A"],
            "year": [2022, 2018, 2020],
            "count": [7, 1, 4],
        }
    )
    result = derive(frame, [{"id": "delta", "op": "diff", "of": "count", "years": 2}])

    assert _null_normalized(result["frame"]["delta"].tolist()) == [3, None, 3]
    assert result["frame"][["geo_id", "year"]].equals(frame[["geo_id", "year"]])


def test_missing_exact_baseline_year_is_reported_even_when_another_year_exists() -> None:
    frame = pd.DataFrame(
        {
            "geo_id": ["A", "A"],
            "year": [2018, 2020],
            "count": [1, 5],
        },
        index=[8, 3],
    )
    result = derive(frame, [{"id": "delta", "op": "diff", "of": "count", "years": 1}])

    assert result["frame"].index.tolist() == [8, 3]
    assert _null_normalized(result["frame"]["delta"].tolist()) == [None, None]
    assert result["value_reasons"]["delta"].tolist() == [
        "missing_baseline:count",
        "missing_baseline:count",
    ]


def test_masks_apply_by_year_geography_and_panel_without_reordering() -> None:
    masks = [
        {
            "id": "only-a",
            "panels": ["cbsa"],
            "columns": ["count"],
            "years": [2021],
            "reason": "A-only invalidation",
            "geographies": {"include": ["A"]},
        },
        {
            "id": "other-panel",
            "panels": ["coc"],
            "columns": ["other"],
            "years": [2022],
            "reason": "not active for this panel",
        },
    ]

    result = apply_masks(BASE_FRAME, masks, **PANEL_KEYS)

    assert result["frame"][["geo_id", "year"]].equals(BASE_FRAME[["geo_id", "year"]])
    selected = result["frame"].query("geo_id == 'A' and year == 2021").iloc[0]
    assert selected["count"] is pd.NA
    assert result["mask_provenance"]["count"].tolist() == [(), (), (), ("only-a",)]
    assert set(result["mask_definitions"]) == {"only-a"}


@pytest.mark.parametrize(
    ("spec", "expected_reason"),
    [
        pytest.param(
            {"id": "ratio", "op": "ratio", "num": "count", "den": "population"},
            "domain:zero_denominator",
            id="ratio-zero-denominator",
        ),
        pytest.param(
            {"id": "share", "op": "share", "of": "count", "total": "population"},
            "domain:zero_denominator",
            id="share-zero-denominator",
        ),
        pytest.param(
            {"id": "pc", "op": "per_capita", "of": "count", "pop": "population", "scale": 1},
            "domain:nonpositive_population",
            id="per-capita-zero-population",
        ),
    ],
)
def test_zero_domains_keep_null_reason(spec: dict[str, object], expected_reason: str) -> None:
    result = derive(BASE_FRAME, [spec])

    assert result["frame"][spec["id"]].iloc[1] is pd.NA
    assert result["value_reasons"][spec["id"]].iloc[1] == expected_reason


@pytest.mark.parametrize("population", [0, -2], ids=["zero", "negative"])
def test_per_capita_rejects_nonpositive_population(population: int) -> None:
    frame = pd.DataFrame(
        {"geo_id": ["A"], "year": [2020], "count": [1], "population": [population]}
    )
    result = derive(
        frame,
        [{"id": "pc", "op": "per_capita", "of": "count", "pop": "population", "scale": 1}],
    )

    assert result["frame"]["pc"].iloc[0] is pd.NA
    assert result["value_reasons"]["pc"].iloc[0] == "domain:nonpositive_population"


def test_negative_finite_denominators_are_allowed() -> None:
    frame = pd.DataFrame({"geo_id": ["A"], "year": [2020], "count": [5], "population": [-10]})
    result = derive(
        frame,
        [{"id": "ratio", "op": "ratio", "num": "count", "den": "population"}],
    )

    assert result["frame"]["ratio"].iloc[0] == -0.5


def test_indicator_membership_and_numeric_parameters_are_resolved() -> None:
    frame = pd.DataFrame({"geo_id": ["A", "A"], "year": [2020, 2021], "count": [1, 3]})
    result = derive_panel(
        frame,
        [{"id": "selected", "op": "indicator", "of": "count", "in": [1, {"param": "target"}]}],
        **PANEL_KEYS,
        input_measures={"count": SOURCE_MEASURES["count"]},
        parameters={"target": 3},
    )

    assert result["frame"]["selected"].tolist() == [1, 1]


def test_indicator_membership_is_numeric_only_in_recipe_v2() -> None:
    frame = pd.DataFrame({"geo_id": ["A"], "year": [2020], "category": ["north"]})
    with pytest.raises(DerivationError, match="non-numeric value"):
        derive_panel(
            frame,
            [{"id": "selected", "op": "indicator", "of": "category", "in": ["north"]}],
            **PANEL_KEYS,
            input_measures={"category": {"units": "label", "aggregation_semantics": "intensive"}},
        )


def test_zero_denominator_error_policy_raises() -> None:
    with pytest.raises(DerivationError, match="zero denominator"):
        derive(
            BASE_FRAME,
            [
                {
                    "id": "ratio",
                    "op": "ratio",
                    "num": "count",
                    "den": "population",
                    "zero_den": "error",
                }
            ],
        )


@pytest.mark.parametrize(
    ("spec", "expected_reason"),
    [
        pytest.param(
            {"id": "pct", "op": "growth", "of": "count", "years": 1, "kind": "pct"},
            "domain:zero_growth_baseline",
            id="pct-growth-zero-base",
        ),
        pytest.param(
            {"id": "log_growth", "op": "growth", "of": "count", "years": 1, "kind": "log"},
            "domain:nonpositive_growth_endpoint",
            id="log-growth-negative-endpoint",
        ),
        pytest.param(
            {"id": "logged", "op": "log", "of": "count", "offset": 0},
            "domain:log_nonpositive",
            id="log-zero-domain",
        ),
    ],
)
def test_temporal_and_log_domains_are_named(spec: dict[str, object], expected_reason: str) -> None:
    frame = pd.DataFrame(
        {
            "geo_id": ["A", "A"],
            "year": [2020, 2021],
            "count": [0.0, -1.0],
        }
    )
    result = derive(frame, [spec])

    assert result["frame"][spec["id"]].iloc[-1] is pd.NA
    assert result["value_reasons"][spec["id"]].iloc[-1] == expected_reason


def test_require_all_sum_null_provenance_and_partial_sum_semantics() -> None:
    frame = pd.DataFrame(
        {
            "geo_id": ["A"],
            "year": [2020],
            "count": pd.Series([None], dtype=object),
            "other": [2],
        }
    )
    result = derive(
        frame,
        [
            {"id": "partial", "op": "sum", "of": ["count", "other"]},
            {"id": "complete", "op": "sum", "of": ["count", "other"], "require_all": True},
        ],
    )

    assert result["frame"]["partial"].iloc[0] == 2
    assert result["frame"]["complete"].iloc[0] is pd.NA
    assert result["value_reasons"]["complete"].iloc[0] == "missing_input:count"


def test_finite_overflow_becomes_domain_null_and_nan_input_stays_nonfinite() -> None:
    frame = pd.DataFrame(
        {
            "geo_id": ["A", "A"],
            "year": [2020, 2021],
            "large": pd.Series([1e308, float("nan")], dtype=object),
            "also_large": pd.Series([1e308, 1.0], dtype=object),
        }
    )
    metadata = {
        "large": {"units": "count", "aggregation_semantics": "extensive"},
        "also_large": {"units": "count", "aggregation_semantics": "extensive"},
    }
    result = derive_panel(
        frame,
        [{"id": "total", "op": "sum", "of": ["large", "also_large"]}],
        **PANEL_KEYS,
        input_measures=metadata,
    )

    assert result["frame"]["total"].iloc[0] is pd.NA
    assert result["value_reasons"]["total"].iloc[0] == "domain:nonfinite_arithmetic"
    assert math.isnan(result["frame"]["total"].iloc[1])


def test_operation_order_lineage_units_and_incompatible_sum_are_explicit() -> None:
    result = derive(
        BASE_FRAME,
        [
            {"id": "triple", "op": "sum", "of": ["double", "count"]},
            {"id": "double", "op": "sum", "of": ["count", "other"]},
        ],
    )

    assert result["frame"]["triple"].tolist() == [13, 5, 3, 8]
    assert result["measures"]["double"]["units"] == "people"
    assert result["measures"]["triple"]["source_lineage"] == ["pit_count", "acs_population"]
    assert result["measures"]["triple"]["definition"]["op"] == "sum"
    repeated = derive(
        BASE_FRAME,
        [
            {"id": "triple", "op": "sum", "of": ["double", "count"]},
            {"id": "double", "op": "sum", "of": ["count", "other"]},
        ],
    )
    assert repeated["frame"].equals(result["frame"])
    assert repeated["measures"] == result["measures"]

    frame_with_rate = BASE_FRAME.copy()
    frame_with_rate["rate"] = 0.5
    with pytest.raises(DerivationError, match="requires extensive inputs"):
        derive(
            frame_with_rate,
            [{"id": "invalid_total", "op": "sum", "of": ["count", "rate"]}],
        )


def test_ratio_and_per_capita_units_are_rates_and_sum_requires_matching_units() -> None:
    result = derive(
        BASE_FRAME,
        [
            {"id": "share", "op": "ratio", "num": "count", "den": "population"},
            {
                "id": "per_person",
                "op": "per_capita",
                "of": "count",
                "pop": "population",
                "scale": 100,
            },
        ],
    )

    assert result["measures"]["share"]["units"] == "1"
    assert result["measures"]["share"]["aggregation_semantics"] == "rate"
    assert result["measures"]["per_person"]["units"] == "1"
    assert result["measures"]["per_person"]["aggregation_semantics"] == "rate"

    mismatched_units = dict(SOURCE_MEASURES)
    mismatched_units["other"] = {
        **SOURCE_MEASURES["other"],
        "units": "dollars",
    }
    with pytest.raises(DerivationError, match="matching input units"):
        derive_panel(
            BASE_FRAME,
            [{"id": "total", "op": "sum", "of": ["count", "other"]}],
            **PANEL_KEYS,
            input_measures=mismatched_units,
        )


@pytest.mark.parametrize(
    ("frame", "specs", "message"),
    [
        pytest.param(
            pd.DataFrame({"geo_id": ["A", "A"], "year": [2020, 2020], "count": [1, 2]}),
            [{"id": "lagged", "op": "lag", "of": "count", "years": 1}],
            "must be unique",
            id="duplicate-keys-before-lookup",
        ),
        pytest.param(
            pd.DataFrame({"geo_id": [None], "year": [2020], "count": [1]}),
            [{"id": "lagged", "op": "lag", "of": "count", "years": 1}],
            "keys must not be null",
            id="null-keys-before-lookup",
        ),
        pytest.param(
            BASE_FRAME,
            [{"id": "unknown", "op": "log", "of": "absent"}],
            "unknown columns",
            id="unknown-input",
        ),
        pytest.param(
            BASE_FRAME,
            [
                {"id": "first", "op": "lag", "of": "second", "years": 1},
                {"id": "second", "op": "lag", "of": "first", "years": 1},
            ],
            "dependency cycle",
            id="derive-cycle",
        ),
        pytest.param(
            BASE_FRAME,
            [{"id": "count", "op": "log", "of": "other"}],
            "collide",
            id="derived-name-collision",
        ),
    ],
)
def test_invalid_panels_and_derivation_graphs_fail_clearly(
    frame: pd.DataFrame, specs: list[dict[str, object]], message: str
) -> None:
    with pytest.raises(DerivationError, match=message):
        derive(frame, specs)


def _null_normalized(values: list[object]) -> list[object]:
    return [None if value is pd.NA else value for value in values]


def test_mask_unknown_column_is_rejected() -> None:
    with pytest.raises(DerivationError, match="unknown columns"):
        apply_masks(
            BASE_FRAME,
            [{"id": "bad", "columns": ["missing"], "years": [2021], "reason": "bad"}],
            **PANEL_KEYS,
        )


@pytest.mark.parametrize("key_column", ["geo_id", "year"], ids=["geography", "time"])
def test_mask_cannot_change_panel_key_columns(key_column: str) -> None:
    with pytest.raises(DerivationError, match="cannot mask panel key columns"):
        apply_masks(
            BASE_FRAME,
            [{"id": "bad-key", "columns": [key_column], "years": [2021], "reason": "bad"}],
            **PANEL_KEYS,
        )


@pytest.mark.parametrize(
    ("mask", "message"),
    [
        pytest.param(
            {
                "id": "bad-key",
                "panels": ["coc"],
                "columns": ["count"],
                "years": [2021],
                "reason": "bad",
                "selector": "ignored?",
            },
            "unsupported keys",
            id="unknown-field-even-for-other-panel",
        ),
        pytest.param(
            {"id": "missing-reason", "columns": ["count"], "years": [2021]},
            "reason must be a non-empty string",
            id="required-reason",
        ),
        pytest.param(
            {
                "id": "bad-geography",
                "columns": ["count"],
                "years": [2021],
                "reason": "bad",
                "geographies": {"exclude": ["A"]},
            },
            "unsupported keys",
            id="unknown-geography-selector",
        ),
    ],
)
def test_direct_mask_mappings_reject_unsupported_or_incomplete_contracts(
    mask: dict[str, object], message: str
) -> None:
    with pytest.raises(DerivationError, match=message):
        apply_masks(BASE_FRAME, [mask], **PANEL_KEYS)
