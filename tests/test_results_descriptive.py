"""Truth tables for resolved-frame descriptive and correlation results."""

from __future__ import annotations

import math

import pandas as pd
import pytest

from hhplab.analysis.contracts import AnalysisError, ResolvedSampleFrame
from hhplab.analysis.correlation import correlate_panel, correlate_resolved_sample
from hhplab.analysis.inspection import describe_panel, describe_resolved_sample
from hhplab.schema.results import validate_result_rows


def resolved(rows: list[dict[str, object]]) -> ResolvedSampleFrame:
    frame = pd.DataFrame(rows)
    for column in frame.columns.difference(["geo_id", "year"]):
        frame[column] = pd.Series([row.get(column) for row in rows], dtype=object)
    return ResolvedSampleFrame(
        frame=frame,
        row_keys=tuple(zip(frame["geo_id"], frame["year"], strict=True)),
        geo_key="geo_id",
        time_key="year",
    )


def test_descriptive_type7_sd_shares_and_canonical_group_json() -> None:
    sample = resolved(
        [
            {"geo_id": "A", "year": 2020, "group": "south", "value": 0.0},
            {"geo_id": "B", "year": 2020, "group": "south", "value": 10.0},
            {"geo_id": "C", "year": 2020, "group": "north", "value": 1.0},
        ]
    )
    result = describe_resolved_sample(
        sample,
        analysis_id="summary",
        variant_id="base",
        columns=["value"],
        stats=["n", "sd", "p10", "p25", {"share_gte": {"value": 1}}, {"share_lte": {"value": 1}}],
        by=["group"],
    )

    south = result[result["group"] == '{"group":"south"}'].set_index("stat")
    assert south.loc["n", "value"] == 2
    assert south.loc["sd", "value"] == pytest.approx(math.sqrt(50))
    assert south.loc["p10", "value"] == pytest.approx(1.0)
    assert south.loc["p25", "value"] == pytest.approx(2.5)
    assert south.loc["share_gte", "value"] == pytest.approx(0.5)
    assert south.loc["share_lte", "value"] == pytest.approx(0.5)
    north = result[result["group"] == '{"group":"north"}'].set_index("stat")
    assert north.loc["share_gte", "value"] == 1.0
    assert north.loc["share_lte", "value"] == 1.0
    assert result["group"].drop_duplicates().tolist() == ['{"group":"north"}', '{"group":"south"}']
    assert result.attrs["row_keys"] == sample.row_keys
    assert sample.frame["geo_id"].tolist() == ["A", "B", "C"]
    validate_result_rows("descriptive_stats", result.to_dict(orient="records"))


def test_type7_quantiles_are_stable_with_tied_values() -> None:
    sample = resolved(
        [
            {"geo_id": "A", "year": 2020, "value": 1.0},
            {"geo_id": "B", "year": 2020, "value": 1.0},
            {"geo_id": "C", "year": 2020, "value": 2.0},
            {"geo_id": "D", "year": 2020, "value": 4.0},
        ]
    )
    result = describe_resolved_sample(
        sample,
        analysis_id="ties",
        variant_id="base",
        columns=["value"],
        stats=["p25", "median"],
    ).set_index("stat")

    assert result.loc["p25", "value"] == 1.0
    assert result.loc["median", "value"] == 1.5


def test_descriptive_missing_group_and_singleton_sd_are_explicit() -> None:
    sample = resolved(
        [
            {"geo_id": "A", "year": 2020, "group": None, "value": 2.0},
            {"geo_id": "B", "year": 2020, "group": "only", "value": 4.0},
        ]
    )
    result = describe_resolved_sample(
        sample,
        analysis_id="singletons",
        variant_id="base",
        columns=["value"],
        stats=["n", "mean", "sd"],
        by=["group"],
    )

    missing_group = result[result["group"] == '{"group":null}'].set_index("stat")
    assert missing_group.loc["mean", "value"] == 2.0
    assert missing_group.loc["sd", "value"] is None
    assert missing_group.loc["sd", "status"] == "not_evaluated"
    assert missing_group.loc["sd", "reason"] == "insufficient_observations"
    assert result["group"].drop_duplicates().tolist() == ['{"group":"only"}', '{"group":null}']


def test_descriptive_all_null_values_and_empty_sample_have_distinct_reasons() -> None:
    all_null = resolved(
        [
            {"geo_id": "A", "year": 2020, "value": pd.NA},
            {"geo_id": "B", "year": 2020, "value": None},
        ]
    )
    empty = ResolvedSampleFrame(
        frame=pd.DataFrame(columns=["geo_id", "year", "value"]),
        row_keys=(),
        geo_key="geo_id",
        time_key="year",
    )
    all_null_rows = describe_resolved_sample(
        all_null,
        analysis_id="all-null",
        variant_id="base",
        columns=["value"],
        stats=["n", "mean", "sd"],
    )
    empty_rows = describe_resolved_sample(
        empty,
        analysis_id="empty",
        variant_id="base",
        columns=["value"],
        stats=["n", "mean"],
    )

    assert all_null_rows["value"].tolist() == [0, None, None]
    assert all_null_rows["reason"].tolist() == [
        None,
        "no_valid_observations",
        "no_valid_observations",
    ]
    assert empty_rows["reason"].tolist() == [None, "empty_sample"]
    validate_result_rows("descriptive_stats", all_null_rows.to_dict(orient="records"))
    validate_result_rows("descriptive_stats", empty_rows.to_dict(orient="records"))


def test_descriptive_partial_null_does_not_estimate_on_a_subset() -> None:
    sample = resolved(
        [
            {"geo_id": "A", "year": 2020, "group": "same", "value": 2.0},
            {"geo_id": "B", "year": 2020, "group": "same", "value": pd.NA},
        ]
    )
    result = describe_resolved_sample(
        sample,
        analysis_id="partial-null",
        variant_id="base",
        columns=["value"],
        stats=["n", "mean"],
        by=["group"],
    ).set_index("stat")

    assert result.loc["n", "value"] == 1
    assert result.loc["mean", "value"] is None
    assert result.loc["mean", "status"] == "not_evaluated"
    assert result.loc["mean", "reason"] == "missing_input"
    assert result.attrs["row_keys"] == sample.row_keys
    validate_result_rows("descriptive_stats", result.reset_index().to_dict(orient="records"))


def test_descriptive_rejects_weights_and_nonfinite_without_mutating_sample() -> None:
    sample = resolved(
        [
            {"geo_id": "A", "year": 2020, "value": 1.0},
            {"geo_id": "B", "year": 2020, "value": float("inf")},
        ]
    )
    with pytest.raises(AnalysisError, match="weighted descriptive"):
        describe_resolved_sample(
            sample,
            analysis_id="weighted",
            variant_id="base",
            columns=["value"],
            stats=["mean"],
            weights="weight",
        )
    with pytest.raises(AnalysisError, match="nonfinite"):
        describe_resolved_sample(
            sample,
            analysis_id="nonfinite",
            variant_id="base",
            columns=["value"],
            stats=["mean"],
        )
    assert sample.row_keys == (("A", 2020), ("B", 2020))
    assert sample.frame["value"].iloc[1] == float("inf")


def test_correlation_uses_shared_listwise_membership_and_average_tie_ranks() -> None:
    sample = resolved(
        [
            {"geo_id": "A", "year": 2020, "x": 1.0, "y": 1.0, "z": 3.0},
            {"geo_id": "B", "year": 2020, "x": 2.0, "y": 1.0, "z": 2.0},
            {"geo_id": "C", "year": 2020, "x": 3.0, "y": 2.0, "z": 1.0},
        ]
    )
    result = correlate_resolved_sample(
        sample,
        analysis_id="corr",
        variant_id="base",
        columns=["x", "y", "z"],
        kind="spearman",
    )

    assert result[["col_a", "col_b"]].values.tolist() == [["x", "y"], ["x", "z"], ["y", "z"]]
    assert result["n"].tolist() == [3, 3, 3]
    assert result["r"].tolist() == pytest.approx([math.sqrt(3 / 4), -1.0, -math.sqrt(3 / 4)])
    assert result["status"].tolist() == ["evaluated"] * 3
    assert result.attrs["row_keys"] == sample.row_keys
    assert len(sample.frame) == 3
    validate_result_rows("correlations", result.to_dict(orient="records"))


@pytest.mark.parametrize(
    ("rows", "expected_n", "reason"),
    [
        pytest.param([], 0, "no_valid_observations", id="empty-listwise"),
        pytest.param(
            [{"x": None, "y": 1.0}, {"x": None, "y": 2.0}],
            2,
            "no_valid_observations",
            id="all-null-input",
        ),
        pytest.param(
            [{"x": 1.0, "y": 2.0}],
            1,
            "insufficient_observations",
            id="one-listwise-observation",
        ),
    ],
)
def test_correlation_undefined_listwise_results_are_explicit(
    rows: list[dict[str, object]], expected_n: int, reason: str
) -> None:
    panel_rows = [{"geo_id": f"G{index}", "year": 2020, **row} for index, row in enumerate(rows)]
    frame = pd.DataFrame(panel_rows, columns=["geo_id", "year", "x", "y"])
    frame["x"] = pd.Series([row.get("x") for row in rows], dtype=object)
    frame["y"] = pd.Series([row.get("y") for row in rows], dtype=object)
    sample = ResolvedSampleFrame(
        frame=frame,
        row_keys=tuple(zip(frame["geo_id"], frame["year"], strict=True)),
        geo_key="geo_id",
        time_key="year",
    )
    result = correlate_resolved_sample(
        sample,
        analysis_id="undefined",
        variant_id="base",
        columns=["x", "y"],
    )

    assert result["n"].tolist() == [expected_n]
    assert result["r"].tolist() == [None]
    assert result["reason"].tolist() == [reason]
    assert result.attrs["row_keys"] == sample.row_keys
    validate_result_rows("correlations", result.to_dict(orient="records"))


def test_correlation_direct_partial_null_does_not_shrink_sample() -> None:
    sample = resolved(
        [
            {"geo_id": "A", "year": 2020, "x": 1.0, "y": 2.0},
            {"geo_id": "B", "year": 2020, "x": 2.0, "y": pd.NA},
            {"geo_id": "C", "year": 2020, "x": 3.0, "y": 6.0},
        ]
    )
    result = correlate_resolved_sample(
        sample,
        analysis_id="partial-null",
        variant_id="base",
        columns=["x", "y"],
    )

    assert result.loc[0, "n"] == len(sample.frame)
    assert result.loc[0, "r"] is None
    assert result.loc[0, "status"] == "not_evaluated"
    assert result.loc[0, "reason"] == "missing_input"
    assert result.attrs["row_keys"] == sample.row_keys
    validate_result_rows("correlations", result.to_dict(orient="records"))


def test_constant_correlations_and_weighted_correlations_have_contract_statuses() -> None:
    sample = resolved(
        [
            {"geo_id": "A", "year": 2020, "x": 1.0, "y": 2.0},
            {"geo_id": "B", "year": 2020, "x": 1.0, "y": 3.0},
            {"geo_id": "C", "year": 2020, "x": 1.0, "y": 4.0},
        ]
    )
    result = correlate_resolved_sample(
        sample,
        analysis_id="constant",
        variant_id="base",
        columns=["x", "y"],
    )
    assert result.loc[0, "status"] == "not_evaluated"
    assert result.loc[0, "reason"] == "constant_input"
    assert result.loc[0, "r"] is None
    with pytest.raises(AnalysisError, match="weighted correlation"):
        correlate_resolved_sample(
            sample,
            analysis_id="weighted",
            variant_id="base",
            columns=["x", "y"],
            weights="weight",
        )
    validate_result_rows("correlations", result.to_dict(orient="records"))


def test_legacy_path_apis_keep_their_existing_result_shape(tmp_path) -> None:
    panel_path = tmp_path / "panel.parquet"
    pd.DataFrame(
        {
            "geo_id": ["A", "B", "C"],
            "year": [2020, 2020, 2020],
            "x": [1.0, 2.0, 3.0],
            "y": [2.0, 4.0, 6.0],
        }
    ).to_parquet(panel_path)

    described = describe_panel(panel_path, columns=["x"], output_path=tmp_path / "describe.parquet")
    correlated = correlate_panel(
        panel_path,
        columns=["x", "y"],
        output_path=tmp_path / "correlate.parquet",
    )

    assert {"column", "mean", "std", "p25", "median", "p75"}.issubset(described.table.columns)
    assert {"left", "right", "correlation", "partial_correlation"}.issubset(
        correlated.table.columns
    )
    assert correlated.table.loc[0, "correlation"] == pytest.approx(1.0)
