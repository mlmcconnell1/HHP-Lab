"""Shared helpers for applying crosswalk weights to tabular source data."""

from __future__ import annotations

from collections.abc import Sequence

import pandas as pd


def apply_crosswalk(
    data: pd.DataFrame,
    crosswalk: pd.DataFrame,
    *,
    value_cols: Sequence[str],
    weight_col: str,
    geo_id_col: str,
    source_id_col: str,
    data_source_id_col: str | None = None,
    group_cols: Sequence[str] = (),
    normalize: bool = False,
    output_cols: Sequence[str] | None = None,
) -> pd.DataFrame:
    """Apply crosswalk weights and aggregate source values to target geography.

    The helper owns the common merge → row contribution → groupby skeleton used by
    source-specific aggregators.  Callers still decide which weight column to use,
    whether values are normalized by available coverage, and how to interpret the
    returned diagnostics.
    """
    if not value_cols:
        raise ValueError("apply_crosswalk requires at least one value column.")
    if output_cols is None:
        output_cols = value_cols
    if len(output_cols) != len(value_cols):
        raise ValueError("output_cols must have the same length as value_cols.")

    data_source_id_col = data_source_id_col or source_id_col
    crosswalk_required = {geo_id_col, source_id_col, weight_col}
    data_required = {data_source_id_col, *group_cols, *value_cols}
    _require_columns(crosswalk, crosswalk_required, label="crosswalk")
    _require_columns(data, data_required, label="data")

    xwalk_cols = [geo_id_col, source_id_col, weight_col]
    data_cols = [data_source_id_col, *group_cols, *value_cols]
    source = data[data_cols].copy()
    if data_source_id_col != source_id_col:
        source = source.rename(columns={data_source_id_col: source_id_col})

    merged = crosswalk[xwalk_cols].merge(source, on=source_id_col, how="inner")
    group_keys = [geo_id_col, *group_cols]
    if merged.empty:
        return pd.DataFrame(
            columns=[
                *group_keys,
                *output_cols,
                *(f"max_weighted_{output_col}" for output_col in output_cols),
                "covered_weight",
                "source_count",
                "max_source_weight",
            ]
        )

    weights = pd.to_numeric(merged[weight_col], errors="coerce").fillna(0.0)
    weighted_cols: list[str] = []
    for source_col, output_col in zip(value_cols, output_cols, strict=True):
        weighted_col = f"__weighted_{output_col}"
        merged[weighted_col] = pd.to_numeric(merged[source_col], errors="coerce") * weights
        weighted_cols.append(weighted_col)
    merged["__covered_weight"] = weights

    grouped = merged.groupby(group_keys, dropna=False)
    aggregations = {
        **{
            output_col: (weighted_col, _sum_with_missing_preserved)
            for output_col, weighted_col in zip(output_cols, weighted_cols, strict=True)
        },
        **{
            f"max_weighted_{output_col}": (weighted_col, "max")
            for output_col, weighted_col in zip(output_cols, weighted_cols, strict=True)
        },
        "covered_weight": ("__covered_weight", "sum"),
        "source_count": (source_id_col, "size"),
        "max_source_weight": ("__covered_weight", "max"),
    }
    result = grouped.agg(**aggregations).reset_index()

    if normalize:
        coverage = result["covered_weight"].where(result["covered_weight"] > 0)
        for output_col in output_cols:
            result[output_col] = result[output_col] / coverage
        result["max_source_weight"] = result["max_source_weight"] / coverage
        result.loc[result["covered_weight"] <= 0, [*output_cols, "max_source_weight"]] = pd.NA

    return result


def _require_columns(df: pd.DataFrame, required: set[str], *, label: str) -> None:
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"{label} missing required column(s): {', '.join(missing)}")


def _sum_with_missing_preserved(values: pd.Series) -> float:
    return values.sum(min_count=1)
