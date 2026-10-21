"""Regression model-frame and design construction."""

from __future__ import annotations

import math
from dataclasses import dataclass
from numbers import Real
from pathlib import Path
from typing import Any

import numpy as np
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


@dataclass(frozen=True)
class AbsorbedDesign:
    """Weighted, FE-residualized arrays and the full LSDV rank accounting."""

    outcome: np.ndarray
    regressors: np.ndarray
    regressor_norms: np.ndarray
    terms: tuple[str, ...]
    normalized_weights: np.ndarray
    outcome_mean: float
    regressor_means: np.ndarray
    weighted_intercept: np.ndarray | None
    total_sum_squares: float
    within_sum_squares: float
    design_rank: int
    k_dof: int
    df_resid: int
    fixed_effects: tuple[str, ...]
    nested_fixed_effects: tuple[str, ...]
    iterations: int


def build_absorbed_design(
    frame: pd.DataFrame,
    *,
    outcome: str,
    terms: list[str] | tuple[str, ...],
    fixed_effects: list[str] | tuple[str, ...] = (),
    weights: str | None = None,
    cluster_by: str | None = None,
    tolerance: float = 1e-12,
    max_iterations: int = 100_000,
) -> AbsorbedDesign:
    """Build a WLS design after converged weighted multi-way FE absorption.

    The returned outcome and regressors are multiplied by sqrt(normalized
    weights), so ordinary least squares on them equals the requested WLS.
    Weights are divided by their mean to make the numeric path invariant to a
    common rescaling. The legacy standalone regression design remains separate.
    """
    if not isinstance(frame, pd.DataFrame):
        raise AnalysisError("regression readiness requires a pandas DataFrame")
    terms = tuple(terms)
    fixed_effects = tuple(fixed_effects)
    if not outcome or len(set(terms)) != len(terms):
        raise AnalysisError("outcome and requested regressor terms must be non-empty and unique")
    if len(set(fixed_effects)) != len(fixed_effects):
        raise AnalysisError("fixed-effect columns must be unique")
    if tolerance <= 0 or tolerance > 1e-12 or not math.isfinite(tolerance):
        raise AnalysisError("absorption tolerance must be finite and in (0, 1e-12]")
    if max_iterations < 1:
        raise AnalysisError("absorption max_iterations must be positive")

    required = [outcome, *terms, *fixed_effects]
    if weights is not None:
        required.append(weights)
    if cluster_by is not None:
        required.append(cluster_by)
    missing = [column for column in dict.fromkeys(required) if column not in frame.columns]
    if missing:
        raise AnalysisError(f"regression readiness is missing columns: {missing}")
    if len(frame) == 0:
        raise AnalysisError("regression readiness requires at least one resolved row")

    role_columns = [outcome, *terms]
    numeric = np.empty((len(frame), len(role_columns)), dtype=float)
    for index, column in enumerate(role_columns):
        numeric[:, index] = _strict_numeric_column(frame[column], name=column, role="numeric role")
    y = numeric[:, 0]
    x = numeric[:, 1:]

    if weights is None:
        original_weights = np.ones(len(frame), dtype=float)
    else:
        original_weights = _strict_numeric_column(
            frame[weights], name=weights, role="weight column"
        )
        if (original_weights <= 0).any():
            raise AnalysisError(f"weight column {weights!r} must contain only positive values")
    max_weight = float(np.max(original_weights))
    scaled_weights = original_weights / max_weight
    mean_scaled_weight = float(np.mean(scaled_weights))
    if not math.isfinite(mean_scaled_weight) or mean_scaled_weight <= 0:
        raise AnalysisError("weights must have a finite positive mean")
    normalized_weights = scaled_weights / mean_scaled_weight

    for column in fixed_effects:
        if frame[column].isna().any():
            raise AnalysisError(f"fixed-effect column {column!r} contains missing values")
    if cluster_by is not None and frame[cluster_by].isna().any():
        raise AnalysisError(f"cluster column {cluster_by!r} contains missing values")

    root_weights = np.sqrt(normalized_weights)
    weighted_y = root_weights * y
    weighted_x = root_weights[:, None] * x
    regressor_norms = np.linalg.norm(weighted_x, axis=0)
    outcome_mean = float(np.average(y, weights=normalized_weights))
    regressor_means = np.average(x, axis=0, weights=normalized_weights)
    centered_y = root_weights * (y - outcome_mean)
    total_sum_squares = float(centered_y @ centered_y)
    # With no FE dimensions, absorb the always-present intercept by weighted
    # demeaning. With FEs, their span already contains the intercept.
    projection_columns = fixed_effects
    if not projection_columns:
        weighted_y, weighted_x, iterations = _weighted_demean(
            weighted_y, weighted_x, frame, (), normalized_weights, tolerance
        )
    else:
        weighted_y, weighted_x, iterations = _weighted_demean(
            weighted_y,
            weighted_x,
            frame,
            projection_columns,
            normalized_weights,
            tolerance,
            max_iterations=max_iterations,
        )
    within_sum_squares = float(weighted_y @ weighted_y)
    weighted_intercept = root_weights if not fixed_effects else None

    full_design = _full_lsdv_design(frame, x, fixed_effects)
    weighted_full_design = root_weights[:, None] * full_design
    design_rank = _matrix_rank(weighted_full_design)
    nested_fixed_effects: tuple[str, ...] = ()
    reduced_fe = fixed_effects
    if cluster_by is not None and fixed_effects:
        nested_fixed_effects = tuple(
            column for column in fixed_effects if _is_nested_within(frame, column, cluster_by)
        )
        reduced_fe = tuple(column for column in fixed_effects if column not in nested_fixed_effects)
    if nested_fixed_effects:
        reduced_design = _full_lsdv_design(frame, x, reduced_fe)
        k_dof = _matrix_rank(root_weights[:, None] * reduced_design)
    else:
        k_dof = design_rank
    df_resid = len(frame) - design_rank
    return AbsorbedDesign(
        outcome=weighted_y,
        regressors=weighted_x,
        regressor_norms=regressor_norms,
        terms=terms,
        normalized_weights=normalized_weights,
        outcome_mean=outcome_mean,
        regressor_means=regressor_means,
        weighted_intercept=weighted_intercept,
        total_sum_squares=total_sum_squares,
        within_sum_squares=within_sum_squares,
        design_rank=design_rank,
        k_dof=k_dof,
        df_resid=df_resid,
        fixed_effects=fixed_effects,
        nested_fixed_effects=nested_fixed_effects,
        iterations=iterations,
    )


def _weighted_demean(
    outcome: np.ndarray,
    regressors: np.ndarray,
    frame: pd.DataFrame,
    fixed_effects: tuple[str, ...],
    weights: np.ndarray,
    tolerance: float,
    *,
    max_iterations: int = 100_000,
) -> tuple[np.ndarray, np.ndarray, int]:
    values = np.column_stack((outcome, regressors))
    if not fixed_effects:
        root_weights = np.sqrt(weights)
        raw = values / root_weights[:, None]
        mean = np.average(raw, axis=0, weights=weights)
        transformed = (raw - mean) * root_weights[:, None]
        return transformed[:, 0], transformed[:, 1:], 1
    # Input values already have sqrt(w) applied. Convert to original scale for
    # weighted group-mean projections, then transform back after convergence.
    root_weights = np.sqrt(weights)
    raw = values / root_weights[:, None]
    scale = max(1.0, float(np.max(np.abs(raw))))
    for iteration in range(1, max_iterations + 1):
        largest_change = 0.0
        for column in fixed_effects:
            codes, uniques = pd.factorize(frame[column], sort=False)
            for code in range(len(uniques)):
                mask = codes == code
                group_weights = weights[mask]
                mean = np.average(raw[mask], axis=0, weights=group_weights)
                raw[mask] -= mean
                largest_change = max(largest_change, float(np.max(np.abs(mean))))
        if largest_change <= tolerance * scale:
            transformed = raw * root_weights[:, None]
            return transformed[:, 0], transformed[:, 1:], iteration
    raise AnalysisError(
        f"weighted fixed-effect absorption did not converge after {max_iterations} iterations"
    )


def _full_lsdv_design(
    frame: pd.DataFrame,
    regressors: np.ndarray,
    fixed_effects: tuple[str, ...],
) -> np.ndarray:
    parts = [np.ones((len(frame), 1), dtype=float), regressors]
    for column in fixed_effects:
        codes, categories = pd.factorize(frame[column], sort=False)
        if (codes < 0).any():
            raise AnalysisError(f"fixed-effect column {column!r} contains missing values")
        indicators = np.zeros((len(frame), len(categories)), dtype=float)
        indicators[np.arange(len(frame)), codes] = 1.0
        parts.append(indicators)
    return np.column_stack(parts)


def _matrix_rank(matrix: np.ndarray) -> int:
    if matrix.size == 0:
        return 0
    return int(np.linalg.matrix_rank(matrix))


def _is_nested_within(frame: pd.DataFrame, fixed_effect: str, cluster_by: str) -> bool:
    return bool(frame.groupby(fixed_effect, dropna=False)[cluster_by].nunique().le(1).all())


def _strict_numeric_column(series: pd.Series, *, name: str, role: str) -> np.ndarray:
    values = np.empty(len(series), dtype=float)
    for index, raw_value in enumerate(series.array):
        if raw_value is None or raw_value is pd.NA or raw_value is pd.NaT:
            raise AnalysisError(f"{role} {name!r} contains a missing value")
        if isinstance(raw_value, np.generic):
            raw_value = raw_value.item()
        if isinstance(raw_value, bool) or not isinstance(raw_value, Real):
            raise AnalysisError(f"{role} {name!r} contains a nonnumeric value")
        value = float(raw_value)
        if not math.isfinite(value):
            raise AnalysisError(f"{role} {name!r} contains a nonfinite value")
        values[index] = value
    return values
