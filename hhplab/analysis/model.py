"""Regression model-frame and design construction."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import pandas as pd

from .contracts import AnalysisError
from .persistence import _read_panel, _require_columns


def _fixed_effect_dummies(df: pd.DataFrame, column: str, prefix: str) -> pd.DataFrame:
    return pd.get_dummies(df[column].astype("string"), prefix=prefix, drop_first=True, dtype=float)

def _is_binary_indicator(series: pd.Series) -> bool:
    values = set(pd.to_numeric(series.dropna(), errors="coerce").dropna().unique().tolist())
    return bool(values) and values <= {0, 1}


def _standardize_model_columns(
    model_df: pd.DataFrame,
    columns: list[str],
) -> dict[str, dict[str, Any]]:
    metadata: dict[str, dict[str, Any]] = {}
    for column in columns:
        values = pd.to_numeric(model_df[column], errors="coerce")
        mean = float(values.mean())
        std = float(values.std(ddof=0))
        if not math.isfinite(std) or std <= 0:
            raise AnalysisError(
                f"Cannot standardize column '{column}' because its model-sample "
                "standard deviation is zero or undefined."
            )
        if _is_binary_indicator(values):
            metadata[column] = {
                "standardized": False,
                "mean": mean,
                "std": std,
                "note": "binary_indicator_not_standardized",
            }
            continue
        model_df[column] = (values - mean) / std
        metadata[column] = {
            "standardized": True,
            "mean": mean,
            "std": std,
            "note": "",
        }
    return metadata


def _model_frame_for_regression(
    panel_path: Path,
    *,
    outcome: str,
    predictors: list[str],
    instruments: list[str],
    entity_column: str,
    year_column: str,
    entity_fe: bool,
    year_fe: bool,
    cluster_by: str | None,
) -> pd.DataFrame:
    df = _read_panel(panel_path)
    needed = [outcome, *predictors, *instruments]
    if entity_fe:
        needed.append(entity_column)
    if year_fe:
        needed.append(year_column)
    if cluster_by is not None:
        needed.append(cluster_by)
    _require_columns(df, list(dict.fromkeys(needed)), context="regress")

    model_df = df[list(dict.fromkeys(needed))].copy()
    numeric_cols = [outcome, *predictors, *instruments]
    for column in numeric_cols:
        model_df[column] = pd.to_numeric(model_df[column], errors="coerce")
    drop_subset = list(numeric_cols)
    if cluster_by is not None:
        drop_subset.append(cluster_by)
    return model_df.dropna(subset=drop_subset)


def _regression_design(
    model_df: pd.DataFrame,
    *,
    predictors: list[str],
    entity_column: str,
    year_column: str,
    entity_fe: bool,
    year_fe: bool,
) -> pd.DataFrame:
    x_parts = [pd.Series(1.0, index=model_df.index, name="Intercept"), model_df[predictors]]
    if entity_fe:
        x_parts.append(_fixed_effect_dummies(model_df, entity_column, "entity"))
    if year_fe:
        x_parts.append(_fixed_effect_dummies(model_df, year_column, "year"))
    return pd.concat(x_parts, axis=1).astype(float)
