"""Pairwise and partial panel correlations."""

from __future__ import annotations

import math
from collections.abc import Sequence
from numbers import Real
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .contracts import AnalysisError, AnalysisResult, ResolvedSampleFrame
from .persistence import _persist_result, _read_panel, _require_columns

_CORRELATION_RESULT_COLUMNS = (
    "analysis",
    "variant",
    "col_a",
    "col_b",
    "kind",
    "r",
    "n",
    "status",
    "reason",
)


def correlate_resolved_sample(
    sample: ResolvedSampleFrame,
    *,
    analysis_id: str,
    variant_id: str,
    columns: Sequence[str],
    kind: str = "pearson",
    weights: str | None = None,
) -> pd.DataFrame:
    """Compute schema-ready listwise Pearson or Spearman rows.

    The resolved sample is never mutated, reordered, or reduced. If a direct
    caller supplies nulls, every pair is explicitly undefined; only the sample
    resolver may exclude rows. ``attrs['row_keys']`` records the exact resolved
    keys consumed by the analysis contract.
    """
    if not isinstance(sample, ResolvedSampleFrame):
        raise AnalysisError("correlation requires a ResolvedSampleFrame")
    sample.validate_frame_identity()
    if not isinstance(analysis_id, str) or not analysis_id.strip():
        raise AnalysisError("analysis_id must be a non-empty string")
    if not isinstance(variant_id, str) or not variant_id.strip():
        raise AnalysisError("variant_id must be a non-empty string")
    if weights not in (None, "none"):
        raise AnalysisError("capability_unavailable: weighted correlation is unsupported")
    selected = _correlation_columns(columns)
    missing = [column for column in selected if column not in sample.frame]
    if missing:
        raise AnalysisError(f"correlation columns are missing from the resolved sample: {missing}")
    if kind not in {"pearson", "spearman"}:
        raise AnalysisError(f"unsupported correlation kind: {kind!r}")

    numeric_rows: list[list[float | None]] = []
    has_null = False
    for position in range(len(sample.frame)):
        raw_values = [sample.frame[column].iat[position] for column in selected]
        numeric_values: list[float | None] = []
        for column, raw_value in zip(selected, raw_values, strict=True):
            if _is_storage_null(raw_value):
                has_null = True
                numeric_values.append(None)
                continue
            value = raw_value.item() if isinstance(raw_value, np.generic) else raw_value
            if isinstance(value, bool) or not isinstance(value, Real):
                raise AnalysisError(
                    f"correlation column {column!r} contains a nonnumeric value "
                    f"at sample row position {position}"
                )
            numeric_value = float(value)
            if not math.isfinite(numeric_value):
                raise AnalysisError(
                    f"resolved sample contains a nonfinite value in correlation column {column!r}"
                )
            numeric_values.append(numeric_value)
        numeric_rows.append(numeric_values)

    n = len(sample.frame)
    complete_count = sum(all(value is not None for value in row) for row in numeric_rows)
    matrix = (
        np.asarray(numeric_rows, dtype=float)
        if not has_null
        else np.empty((0, len(selected)), dtype=float)
    )
    if kind == "spearman" and n and not has_null:
        ranked = np.column_stack(
            [
                pd.Series(matrix[:, column]).rank(method="average").to_numpy()
                for column in range(len(selected))
            ]
        )
    elif not has_null:
        ranked = matrix
    else:
        ranked = np.empty((0, len(selected)), dtype=float)

    rows: list[dict[str, Any]] = []
    for left_index, left in enumerate(selected):
        for right_index in range(left_index + 1, len(selected)):
            right = selected[right_index]
            if n == 0:
                coefficient, status, reason = None, "not_evaluated", "no_valid_observations"
            elif has_null:
                coefficient, status = None, "not_evaluated"
                reason = "no_valid_observations" if complete_count == 0 else "missing_input"
            elif n < 2:
                coefficient, status, reason = None, "not_evaluated", "insufficient_observations"
            elif _is_constant(ranked[:, left_index]) or _is_constant(ranked[:, right_index]):
                coefficient, status, reason = None, "not_evaluated", "constant_input"
            else:
                coefficient = _stable_pearson(ranked[:, left_index], ranked[:, right_index])
                if coefficient is None:
                    status, reason = "not_evaluated", "nonfinite_arithmetic"
                else:
                    status, reason = "evaluated", None
            rows.append(
                {
                    "analysis": analysis_id,
                    "variant": variant_id,
                    "col_a": left,
                    "col_b": right,
                    "kind": kind,
                    "r": coefficient,
                    "n": n,
                    "status": status,
                    "reason": reason,
                }
            )
    sample.validate_frame_identity()
    result = pd.DataFrame(rows, columns=_CORRELATION_RESULT_COLUMNS)
    result["r"] = pd.Series([row["r"] for row in rows], dtype=object)
    result.attrs["row_keys"] = sample.row_keys
    return result


def _correlation_columns(columns: Sequence[str]) -> list[str]:
    if isinstance(columns, (str, bytes)) or not isinstance(columns, Sequence) or len(columns) < 2:
        raise AnalysisError("correlation needs at least two columns")
    if any(not isinstance(column, str) or not column.strip() for column in columns):
        raise AnalysisError("correlation columns must be non-empty names")
    if len(set(columns)) != len(columns):
        raise AnalysisError("correlation columns must be unique")
    return list(columns)


def _is_storage_null(value: Any) -> bool:
    if isinstance(value, np.generic):
        value = value.item()
    return value is None or value is pd.NA or value is pd.NaT


def _is_constant(values: np.ndarray) -> bool:
    return len(values) < 2 or bool(np.all(values == values[0]))


def _stable_pearson(left: np.ndarray, right: np.ndarray) -> float | None:
    left_scale = float(np.max(np.abs(left)))
    right_scale = float(np.max(np.abs(right)))
    if left_scale == 0 or right_scale == 0:
        return None
    left_centered = left / left_scale
    right_centered = right / right_scale
    left_centered -= left_centered.mean()
    right_centered -= right_centered.mean()
    left_norm = float(np.dot(left_centered, left_centered))
    right_norm = float(np.dot(right_centered, right_centered))
    denominator = math.sqrt(left_norm * right_norm)
    if denominator == 0 or not math.isfinite(denominator):
        return None
    coefficient = float(np.dot(left_centered, right_centered) / denominator)
    if not math.isfinite(coefficient):
        return None
    return min(1.0, max(-1.0, coefficient))


def _residualize(values: pd.Series, controls: pd.DataFrame) -> pd.Series:
    design = controls.apply(pd.to_numeric, errors="coerce")
    design = design.assign(__intercept=1.0)
    combined = pd.concat([values.rename("__value"), design], axis=1).dropna()
    if combined.empty:
        return pd.Series(dtype=float)
    y = combined["__value"].to_numpy(dtype=float)
    x = combined.drop(columns=["__value"]).to_numpy(dtype=float)
    beta, *_ = np.linalg.lstsq(x, y, rcond=None)
    resid = y - x @ beta
    return pd.Series(resid, index=combined.index)


def correlate_panel(
    panel_path: Path,
    *,
    columns: list[str],
    partial_controls: list[str] | None = None,
    output_path: Path | None = None,
) -> AnalysisResult:
    """Compute pairwise Pearson and optional partial correlations."""
    df = _read_panel(panel_path)
    controls = partial_controls or []
    _require_columns(df, [*columns, *controls], context="correlate")
    rows: list[dict[str, Any]] = []
    for left_index, left in enumerate(columns):
        for right in columns[left_index + 1 :]:
            pair = df[[left, right, *controls]].apply(pd.to_numeric, errors="coerce").dropna()
            if pair.empty:
                corr = np.nan
                partial_corr = np.nan
            else:
                corr = float(pair[left].corr(pair[right]))
                if controls:
                    left_resid = _residualize(pair[left], pair[controls])
                    right_resid = _residualize(pair[right], pair[controls])
                    aligned = pd.concat([left_resid, right_resid], axis=1).dropna()
                    partial_corr = (
                        float(aligned.iloc[:, 0].corr(aligned.iloc[:, 1]))
                        if len(aligned) > 1
                        else np.nan
                    )
                else:
                    partial_corr = np.nan
            rows.append(
                {
                    "left": left,
                    "right": right,
                    "n": int(len(pair)),
                    "correlation": corr,
                    "partial_correlation": partial_corr,
                    "partial_controls": ",".join(controls),
                }
            )
    table = pd.DataFrame(rows)
    return _persist_result(
        table,
        panel_path=panel_path,
        output_path=output_path,
        analysis_type="correlate",
        parameters={"columns": columns, "partial_controls": controls},
        metadata={
            "analysis_type": "correlate",
            "pair_count": len(rows),
            "partial_controls": controls,
        },
    )
