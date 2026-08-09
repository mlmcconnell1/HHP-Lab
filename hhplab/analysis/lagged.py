"""Lagged panel associations."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .contracts import AnalysisError, AnalysisResult
from .persistence import _persist_result, _read_panel, _require_columns


def lagged_associations_panel(
    panel_path: Path,
    *,
    outcome: str,
    predictors: list[str],
    lags: list[int],
    entity_column: str = "geo_id",
    year_column: str = "year",
    output_path: Path | None = None,
) -> AnalysisResult:
    """Correlate an outcome with lagged predictor values by entity-year."""
    if not lags or any(lag < 1 for lag in lags):
        raise AnalysisError("lagged associations require one or more positive lags.")
    df = _read_panel(panel_path)
    _require_columns(
        df,
        [outcome, *predictors, entity_column, year_column],
        context="lagged",
    )
    work = df[[outcome, *predictors, entity_column, year_column]].copy()
    for column in [outcome, *predictors]:
        work[column] = pd.to_numeric(work[column], errors="coerce")
    work = work.sort_values([entity_column, year_column])

    rows: list[dict[str, Any]] = []
    for predictor in predictors:
        for lag in sorted(set(lags)):
            lagged_column = f"__{predictor}_lag_{lag}"
            work[lagged_column] = work.groupby(entity_column, dropna=False)[predictor].shift(lag)
            pair = work[[outcome, lagged_column]].dropna()
            rows.append(
                {
                    "outcome": outcome,
                    "predictor": predictor,
                    "lag": lag,
                    "n": int(len(pair)),
                    "correlation": (
                        float(pair[outcome].corr(pair[lagged_column])) if len(pair) > 1 else np.nan
                    ),
                }
            )
    table = pd.DataFrame(rows)
    return _persist_result(
        table,
        panel_path=panel_path,
        output_path=output_path,
        analysis_type="lagged",
        parameters={
            "outcome": outcome,
            "predictors": predictors,
            "lags": sorted(set(lags)),
            "entity_column": entity_column,
            "year_column": year_column,
        },
        metadata={
            "analysis_type": "lagged",
            "outcome": outcome,
            "predictors": predictors,
            "lags": sorted(set(lags)),
            "association_count": len(rows),
        },
    )
