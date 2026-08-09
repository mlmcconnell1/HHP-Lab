"""Panel inspection and measure-semantic summaries."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from hhplab.schema.measures import resolve_panel_measure_entry

from .contracts import AnalysisResult
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
