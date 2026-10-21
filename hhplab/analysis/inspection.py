"""Panel inspection and measure-semantic summaries."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from numbers import Real
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from hhplab.schema.measures import resolve_panel_measure_entry
from hhplab.schema.results import canonical_json

from .contracts import AnalysisError, AnalysisResult, ResolvedSampleFrame
from .persistence import _persist_result, _read_panel, _require_columns


def _numeric_columns(df: pd.DataFrame, requested: list[str] | None) -> list[str]:
    if requested:
        _require_columns(df, requested, context="analysis")
        return requested
    return [
        column
        for column in df.columns
        if pd.api.types.is_numeric_dtype(df[column]) and column != "year"
    ]


def _measure_semantics(column: str, *, panel_columns: list[str]) -> dict[str, Any]:
    entry = resolve_panel_measure_entry(column, panel_columns=panel_columns)
    if entry is None:
        return {}
    return {
        "definition": entry.definition,
        "units": entry.units,
        "source_provider": entry.source_provider,
        "source_product": entry.source_product,
        "native_geometry": entry.native_geometry,
        "role_hint": entry.role_hint,
    }


_DESCRIPTIVE_STATISTICS = {
    "n",
    "mean",
    "sd",
    "min",
    "p10",
    "p25",
    "median",
    "p75",
    "p90",
    "max",
}
_QUANTILES = {"p10": 0.10, "p25": 0.25, "median": 0.50, "p75": 0.75, "p90": 0.90}
_DESCRIPTIVE_RESULT_COLUMNS = (
    "analysis",
    "variant",
    "column",
    "group",
    "stat",
    "value",
    "status",
    "reason",
)


def describe_resolved_sample(
    sample: ResolvedSampleFrame,
    *,
    analysis_id: str,
    variant_id: str,
    columns: Sequence[str],
    stats: Sequence[str | Mapping[str, Any]],
    by: Sequence[str] = (),
    weights: str | None = None,
) -> pd.DataFrame:
    """Compute schema-ready descriptive rows over an exact resolved sample.

    The frame and its key order are never filtered or mutated. A null in a
    column makes that group's statistics undefined instead of shrinking the
    resolved sample. The count statistic reports observed values; all-null and
    empty groups have explicit reasons. Group identities use canonical JSON,
    including a JSON ``null`` for missing grouping values.
    """
    _validate_resolved_request(
        sample,
        analysis_id=analysis_id,
        variant_id=variant_id,
        columns=columns,
        weights=weights,
    )
    grouping_columns = _column_list(by, "by", allow_empty=True)
    missing_groups = [column for column in grouping_columns if column not in sample.frame]
    if missing_groups:
        raise AnalysisError(f"descriptive grouping columns are missing: {missing_groups}")
    selected_stats = _normalize_descriptive_stats(stats, selected_columns=list(columns))

    group_positions: dict[str, list[int]] = {}
    for position in range(len(sample.frame)):
        group = {
            column: _json_group_value(sample.frame[column].iat[position])
            for column in grouping_columns
        }
        group_json = canonical_json(group)
        group_positions.setdefault(group_json, []).append(position)
    if not group_positions and not grouping_columns:
        group_positions[canonical_json({})] = []

    rows: list[dict[str, Any]] = []
    for group_json in sorted(group_positions):
        positions = group_positions[group_json]
        for statistic, target_column, threshold in selected_stats:
            target_columns = [target_column] if target_column is not None else list(columns)
            for column in target_columns:
                values = _descriptive_values(sample.frame[column].iloc[positions], column=column)
                has_null = len(values) != len(positions)
                if statistic == "n":
                    value, status, reason = len(values), "evaluated", None
                elif not values:
                    value, status, reason = None, "not_evaluated", "no_valid_observations"
                elif has_null:
                    value, status, reason = None, "not_evaluated", "missing_input"
                else:
                    value, status, reason = _descriptive_value(
                        statistic, values, threshold=threshold
                    )
                if not len(sample.frame) and statistic != "n":
                    value, status, reason = None, "not_evaluated", "empty_sample"
                rows.append(
                    {
                        "analysis": analysis_id,
                        "variant": variant_id,
                        "column": column,
                        "group": group_json,
                        "stat": statistic,
                        "value": value,
                        "status": status,
                        "reason": reason,
                    }
                )
    sample.validate_frame_identity()
    result = pd.DataFrame(rows, columns=_DESCRIPTIVE_RESULT_COLUMNS)
    result["value"] = pd.Series([row["value"] for row in rows], dtype=object)
    result.attrs["row_keys"] = sample.row_keys
    return result


def _validate_resolved_request(
    sample: ResolvedSampleFrame,
    *,
    analysis_id: str,
    variant_id: str,
    columns: Sequence[str],
    weights: str | None,
) -> None:
    if not isinstance(sample, ResolvedSampleFrame):
        raise AnalysisError("analysis requires a ResolvedSampleFrame")
    sample.validate_frame_identity()
    if not isinstance(analysis_id, str) or not analysis_id.strip():
        raise AnalysisError("analysis_id must be a non-empty string")
    if not isinstance(variant_id, str) or not variant_id.strip():
        raise AnalysisError("variant_id must be a non-empty string")
    if weights not in (None, "none"):
        raise AnalysisError("capability_unavailable: weighted descriptive analyses are unsupported")
    selected = _column_list(columns, "columns")
    missing = [column for column in selected if column not in sample.frame]
    if missing:
        raise AnalysisError(f"analysis columns are missing from the resolved sample: {missing}")


def _column_list(values: Sequence[str], context: str, *, allow_empty: bool = False) -> list[str]:
    if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
        raise AnalysisError(f"{context} must be a list of column names")
    if not allow_empty and not values:
        raise AnalysisError(f"{context} must not be empty")
    if any(not isinstance(value, str) or not value.strip() for value in values):
        raise AnalysisError(f"{context} must contain non-empty column names")
    if len(set(values)) != len(values):
        raise AnalysisError(f"{context} column names must be unique")
    return list(values)


def _normalize_descriptive_stats(
    stats: Sequence[str | Mapping[str, Any]], *, selected_columns: list[str]
) -> list[tuple[str, str | None, float | None]]:
    if isinstance(stats, (str, bytes)) or not isinstance(stats, Sequence) or not stats:
        raise AnalysisError("stats must be a non-empty sequence")
    normalized: list[tuple[str, str | None, float | None]] = []
    seen: set[tuple[str, str | None]] = set()
    for item in stats:
        if isinstance(item, str):
            if item not in _DESCRIPTIVE_STATISTICS:
                raise AnalysisError(f"unsupported descriptive statistic: {item!r}")
            key = (item, None)
            statistic, target, threshold = item, None, None
        elif isinstance(item, Mapping) and len(item) == 1:
            statistic, targets = next(iter(item.items()))
            if statistic not in {"share_gte", "share_lte"}:
                raise AnalysisError(f"unsupported descriptive statistic: {statistic!r}")
            if not isinstance(targets, Mapping) or len(targets) != 1:
                raise AnalysisError(f"{statistic} requires exactly one column and threshold")
            target, raw_threshold = next(iter(targets.items()))
            if target not in selected_columns:
                raise AnalysisError(f"{statistic} column {target!r} is not in columns")
            threshold = _finite_number(raw_threshold, context=f"{statistic}.{target}")
            key = (statistic, target)
        else:
            raise AnalysisError("each descriptive statistic must be a name or one share rule")
        if key in seen:
            raise AnalysisError(f"duplicate descriptive statistic: {key!r}")
        seen.add(key)
        normalized.append((statistic, target, threshold))
    return normalized


def _descriptive_values(series: pd.Series, *, column: str) -> list[float]:
    values: list[float] = []
    for position, raw_value in enumerate(series.array):
        value = raw_value.item() if isinstance(raw_value, np.generic) else raw_value
        if value is None or value is pd.NA or value is pd.NaT:
            continue
        if isinstance(value, bool) or not isinstance(value, Real):
            raise AnalysisError(
                f"descriptive column {column!r} contains a nonnumeric value "
                f"at sample row position {position}"
            )
        value = float(value)
        if not np.isfinite(value):
            raise AnalysisError(
                f"resolved sample contains a nonfinite value in descriptive column {column!r}"
            )
        values.append(value)
    return values


def _descriptive_value(
    statistic: str, values: list[float], *, threshold: float | None
) -> tuple[int | float | None, str, str | None]:
    if statistic == "n":
        return len(values), "evaluated", None
    if not values:
        return None, "not_evaluated", "no_valid_observations"
    if statistic == "mean":
        result = float(np.mean(values))
    elif statistic == "sd":
        if len(values) < 2:
            return None, "not_evaluated", "insufficient_observations"
        result = float(np.std(values, ddof=1))
    elif statistic == "min":
        result = min(values)
    elif statistic == "max":
        result = max(values)
    elif statistic in _QUANTILES:
        result = float(np.quantile(values, _QUANTILES[statistic], method="linear"))
    elif statistic == "share_gte":
        assert threshold is not None
        result = sum(value >= threshold for value in values) / len(values)
    elif statistic == "share_lte":
        assert threshold is not None
        result = sum(value <= threshold for value in values) / len(values)
    else:
        raise AnalysisError(f"unsupported descriptive statistic: {statistic!r}")
    if not np.isfinite(result):
        return None, "not_evaluated", "nonfinite_arithmetic"
    return result, "evaluated", None


def _json_group_value(value: Any) -> Any:
    if isinstance(value, np.generic):
        value = value.item()
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, float) and np.isnan(value):
        return None
    if isinstance(value, bool | str | int | float):
        return value
    raise AnalysisError(f"grouping value {value!r} cannot be represented in canonical JSON")


def _finite_number(value: Any, *, context: str) -> float:
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, bool) or not isinstance(value, Real):
        raise AnalysisError(f"{context} must be a finite number")
    result = float(value)
    if not np.isfinite(result):
        raise AnalysisError(f"{context} must be a finite number")
    return result


def describe_panel(
    panel_path: Path,
    *,
    columns: list[str] | None = None,
    output_path: Path | None = None,
) -> AnalysisResult:
    """Summarize numeric panel columns with semantics and missingness."""
    df = _read_panel(panel_path)
    measure_columns = _numeric_columns(df, columns)
    rows: list[dict[str, Any]] = []
    for column in measure_columns:
        series = pd.to_numeric(df[column], errors="coerce")
        non_null = series.dropna()
        row = {
            "column": column,
            "n": int(non_null.shape[0]),
            "missing": int(series.isna().sum()),
            "missing_rate": float(series.isna().mean()) if len(series) else 0.0,
            "mean": float(non_null.mean()) if not non_null.empty else np.nan,
            "std": float(non_null.std(ddof=1)) if len(non_null) > 1 else np.nan,
            "min": float(non_null.min()) if not non_null.empty else np.nan,
            "p25": float(non_null.quantile(0.25)) if not non_null.empty else np.nan,
            "median": float(non_null.median()) if not non_null.empty else np.nan,
            "p75": float(non_null.quantile(0.75)) if not non_null.empty else np.nan,
            "max": float(non_null.max()) if not non_null.empty else np.nan,
        }
        row.update(_measure_semantics(column, panel_columns=df.columns.tolist()))
        rows.append(row)
    table = pd.DataFrame(rows)
    return _persist_result(
        table,
        panel_path=panel_path,
        output_path=output_path,
        analysis_type="describe",
        parameters={"columns": measure_columns},
        metadata={"analysis_type": "describe", "column_count": len(measure_columns)},
    )
