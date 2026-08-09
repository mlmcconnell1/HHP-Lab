"""Clustered and resampling-based inference methods."""

from __future__ import annotations

import math
import warnings

import numpy as np
import pandas as pd

from .contracts import AnalysisError, _RegressionFit
from .estimation import (
    _fit_2sls,
    _fit_ols,
    _restricted_iv_fitted_and_residuals,
    _restricted_ols_fitted_and_residuals,
)


def _wild_cluster_bootstrap_p_values(
    *,
    x: np.ndarray,
    y: np.ndarray,
    z: np.ndarray | None = None,
    fit: _RegressionFit,
    dof: int,
    clusters: pd.Series,
    terms: list[str],
    design_columns: pd.Index,
    reps: int,
    seed: int,
) -> dict[str, float]:
    if reps < 1:
        raise AnalysisError("--inference-reps must be positive for wild-cluster bootstrap.")
    cluster_values = clusters.dropna().unique().tolist()
    if len(cluster_values) < 2:
        raise AnalysisError("wild-cluster bootstrap requires at least two non-null clusters.")
    rng = np.random.default_rng(seed)
    term_indices = {term: int(design_columns.get_loc(term)) for term in terms}
    exceed = {term: 0 for term in terms}
    observed = {
        term: abs(float(fit.t_stats[index])) for term, index in term_indices.items()
    }
    restricted_sources: dict[str, tuple[np.ndarray, np.ndarray]] = {}
    for term, index in term_indices.items():
        if z is not None:
            restricted_sources[term] = _restricted_iv_fitted_and_residuals(
                x=x,
                z=z,
                y=y,
                restricted_index=index,
            )
        else:
            restricted_sources[term] = _restricted_ols_fitted_and_residuals(
                x=x,
                y=y,
                restricted_index=index,
            )
    cluster_array = clusters.to_numpy()
    for _ in range(reps):
        weights_by_cluster = {
            cluster: rng.choice(np.array([-1.0, 1.0])) for cluster in cluster_values
        }
        weights = np.array([weights_by_cluster.get(cluster, 0.0) for cluster in cluster_array])
        for term, index in term_indices.items():
            restricted_fitted, restricted_residuals = restricted_sources[term]
            y_star = restricted_fitted + restricted_residuals * weights
            if z is not None:
                boot_fit = _fit_2sls(x=x, z=z, y=y_star, dof=dof, clusters=clusters)
            else:
                boot_fit = _fit_ols(x=x, y=y_star, dof=dof, clusters=clusters)
            boot_se = boot_fit.std_errors[index]
            boot_t = (
                abs(float(boot_fit.beta[index] / boot_se))
                if boot_se > 0
                else np.nan
            )
            if math.isfinite(boot_t) and boot_t >= observed[term]:
                exceed[term] += 1
    return {term: float((exceed[term] + 1) / (reps + 1)) for term in terms}


def _permutation_p_values(
    *,
    model_df: pd.DataFrame,
    y: np.ndarray,
    design: pd.DataFrame,
    dof: int,
    clusters: pd.Series | None,
    terms: list[str],
    fit: _RegressionFit,
    reps: int,
    seed: int,
) -> dict[str, float]:
    if reps < 1:
        raise AnalysisError("--inference-reps must be positive for permutation inference.")
    rng = np.random.default_rng(seed)
    exceed = {term: 0 for term in terms}
    term_indices = {term: int(design.columns.get_loc(term)) for term in terms}
    observed = {term: abs(float(fit.beta[index])) for term, index in term_indices.items()}
    for _ in range(reps):
        permuted_design = design.copy()
        for term in terms:
            permuted_design[term] = rng.permutation(model_df[term].to_numpy(dtype=float))
        permuted_fit = _fit_ols(
            x=permuted_design.to_numpy(dtype=float),
            y=y,
            dof=dof,
            clusters=clusters,
        )
        for term, index in term_indices.items():
            statistic = abs(float(permuted_fit.beta[index]))
            if math.isfinite(statistic) and statistic >= observed[term]:
                exceed[term] += 1
    return {term: float((exceed[term] + 1) / (reps + 1)) for term in terms}


def _warn_if_permutation_model_has_correlated_controls(
    *,
    predictors: list[str],
    selected_inference_terms: list[str],
) -> None:
    predictor_terms = set(predictors)
    tested_terms = set(selected_inference_terms)
    if len(tested_terms) <= 1 and not (predictor_terms - tested_terms):
        return
    warnings.warn(
        "Permutation inference is calibrated for single-predictor or literal-randomization "
        "designs. Testing multiple terms, or testing one term while retaining additional "
        "correlated predictor controls, can be anti-conservative; simulations documented "
        "false-positive rates as high as roughly 30% at a nominal 5%. Prefer wild-cluster "
        "inference for observational multi-predictor regressions.",
        RuntimeWarning,
        stacklevel=2,
    )
