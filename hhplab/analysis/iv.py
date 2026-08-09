"""Instrumental-variable estimation and Anderson–Rubin inference."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import numpy as np

from .contracts import AnalysisError, _json_safe
from .estimation import _first_stage_f_statistic, _fit_2sls, _fit_ols
from .model import _model_frame_for_regression, _regression_design


def _accepted_grid_intervals(
    accepted: list[float],
    *,
    grid_step: float,
) -> list[dict[str, float]]:
    if not accepted:
        return []
    values = sorted(accepted)
    intervals: list[dict[str, float]] = []
    start = prev = values[0]
    tolerance = abs(grid_step) * 1.5
    for value in values[1:]:
        if value - prev <= tolerance:
            prev = value
            continue
        intervals.append({"lower": float(start), "upper": float(prev)})
        start = prev = value
    intervals.append({"lower": float(start), "upper": float(prev)})
    return intervals


def anderson_rubin_confidence_set(
    panel_path: Path,
    *,
    outcome: str,
    predictors: list[str],
    endogenous: str,
    instruments: list[str],
    entity_column: str = "geo_id",
    year_column: str = "year",
    entity_fe: bool = True,
    year_fe: bool = True,
    cluster_by: str | None = "geo_id",
    alpha: float = 0.05,
    grid_min: float = -10.0,
    grid_max: float = 10.0,
    grid_step: float = 0.05,
) -> dict[str, Any]:
    """Invert Anderson-Rubin tests over a beta grid for one endogenous predictor."""
    if endogenous not in predictors:
        raise AnalysisError(
            f"--endogenous '{endogenous}' must be one of the model predictors: {predictors}."
        )
    if not instruments:
        raise AnalysisError("Anderson-Rubin inference requires at least one instrument.")
    overlapping = sorted(set(instruments) & {outcome, *predictors})
    if overlapping:
        raise AnalysisError(
            f"--instruments must be excluded from the structural equation; remove "
            f"{overlapping} from the outcome/predictors or choose different instruments."
        )
    if not 0 < alpha < 1:
        raise AnalysisError("--alpha must be between 0 and 1.")
    if grid_step <= 0:
        raise AnalysisError("--grid-step must be positive.")
    if grid_max < grid_min:
        raise AnalysisError("--grid-max must be greater than or equal to --grid-min.")

    model_df = _model_frame_for_regression(
        panel_path,
        outcome=outcome,
        predictors=predictors,
        instruments=instruments,
        entity_column=entity_column,
        year_column=year_column,
        entity_fe=entity_fe,
        year_fe=year_fe,
        cluster_by=cluster_by,
    )
    if len(model_df) <= len(predictors):
        raise AnalysisError("Anderson-Rubin has too few complete rows for the requested model.")

    design = _regression_design(
        model_df,
        predictors=predictors,
        entity_column=entity_column,
        year_column=year_column,
        entity_fe=entity_fe,
        year_fe=year_fe,
    )
    x = design.to_numpy(dtype=float)
    y = model_df[outcome].to_numpy(dtype=float)
    structural_rank = int(np.linalg.matrix_rank(x))
    structural_dof = int(len(y) - structural_rank)
    if structural_dof < 1:
        raise AnalysisError(
            "Anderson-Rubin structural model is saturated or rank-deficient: "
            f"n={len(y)}, rank={structural_rank}."
        )

    controls = [predictor for predictor in predictors if predictor != endogenous]
    reduced_design = _regression_design(
        model_df,
        predictors=controls,
        entity_column=entity_column,
        year_column=year_column,
        entity_fe=entity_fe,
        year_fe=year_fe,
    )
    z_design = reduced_design.copy()
    for instrument in instruments:
        z_design[instrument] = model_df[instrument].astype(float)
    z = z_design.to_numpy(dtype=float)
    z_rank = int(np.linalg.matrix_rank(z))
    dof = int(len(y) - z_rank)
    if dof < 1:
        raise AnalysisError(
            "Anderson-Rubin reduced-form test is saturated or rank-deficient: "
            f"n={len(y)}, instrument_design_rank={z_rank}."
        )
    clusters = model_df[cluster_by].rename(cluster_by) if cluster_by is not None else None
    instrument_indices = [int(z_design.columns.get_loc(name)) for name in instruments]
    endogenous_values = model_df[endogenous].to_numpy(dtype=float)

    iv_fit = _fit_2sls(x=x, z=z, y=y, dof=structural_dof, clusters=clusters)
    endogenous_index = int(design.columns.get_loc(endogenous))
    grid = np.arange(grid_min, grid_max + grid_step / 2.0, grid_step)
    rows: list[dict[str, float | bool]] = []
    accepted: list[float] = []
    for beta in grid:
        transformed = y - float(beta) * endogenous_values
        fit = _fit_ols(x=z, y=transformed, dof=dof, clusters=clusters)
        f_stat, p_value = _first_stage_f_statistic(
            fit=fit,
            z=z,
            instrument_indices=instrument_indices,
            dof=dof,
            clusters=clusters,
        )
        accept = bool(math.isfinite(p_value) and p_value >= alpha)
        if accept:
            accepted.append(float(beta))
        rows.append(
            {
                "beta": float(beta),
                "f_statistic": float(f_stat),
                "p_value": float(p_value),
                "accepted": accept,
            }
        )

    return _json_safe(
        {
            "status": "ok",
            "analysis_type": "anderson_rubin",
            "panel_path": str(panel_path),
            "outcome": outcome,
            "predictors": predictors,
            "endogenous": endogenous,
            "instruments": instruments,
            "entity_column": entity_column,
            "year_column": year_column,
            "entity_fe": entity_fe,
            "year_fe": year_fe,
            "cluster_by": cluster_by,
            "alpha": alpha,
            "grid_min": grid_min,
            "grid_max": grid_max,
            "grid_step": grid_step,
            "n": int(len(y)),
            "design_rank": structural_rank,
            "dof": structural_dof,
            "iv_estimate": float(iv_fit.beta[endogenous_index]),
            "confidence_set": _accepted_grid_intervals(accepted, grid_step=grid_step),
            "grid": rows,
        }
    )
