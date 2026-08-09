"""Pairwise and partial panel correlations."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .contracts import AnalysisResult
from .persistence import _persist_result, _read_panel, _require_columns


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
