"""Shared regression covariance and small-sample statistics."""

from __future__ import annotations

import math
from statistics import NormalDist

import numpy as np
import pandas as pd

from .contracts import AnalysisError


def _clustered_covariance(
    x: np.ndarray,
    residuals: np.ndarray,
    clusters: pd.Series,
) -> np.ndarray:
    _require_clustered_inference_clusters(clusters)
    xtx_inv = np.linalg.pinv(x.T @ x)
    meat = np.zeros((x.shape[1], x.shape[1]))
    for cluster in clusters.dropna().unique():
        mask = clusters == cluster
        xg = x[mask.to_numpy()]
        eg = residuals[mask.to_numpy()]
        score = xg.T @ eg
        meat += np.outer(score, score)
    return xtx_inv @ meat @ xtx_inv


def _clustered_standard_errors(
    x: np.ndarray,
    residuals: np.ndarray,
    clusters: pd.Series,
) -> np.ndarray:
    variance = _clustered_covariance(x, residuals, clusters)
    return np.sqrt(np.clip(np.diag(variance), 0, None))


def _two_sided_p_value(t_stat: float, dof: int) -> float:
    if not math.isfinite(t_stat):
        return np.nan
    try:
        from scipy import stats  # type: ignore[import-not-found]

        return float(2.0 * stats.t.sf(abs(t_stat), dof))
    except Exception:
        return float(2.0 * (1.0 - NormalDist().cdf(abs(t_stat))))


def _require_clustered_inference_clusters(clusters: pd.Series) -> int:
    cluster_count = int(clusters.dropna().nunique())
    if cluster_count < 2:
        cluster_column = clusters.name or "cluster column"
        raise AnalysisError(
            "clustered standard errors require at least two non-null clusters "
            f"in {cluster_column}; found {cluster_count}. "
            "Use --cluster-by '' for non-clustered standard errors or broaden "
            "the analysis sample."
        )
    return cluster_count


def _cluster_denominator_dof(clusters: pd.Series | None, fallback_dof: int) -> int:
    if clusters is None:
        return fallback_dof
    return _require_clustered_inference_clusters(clusters) - 1


def _classical_covariance(
    x: np.ndarray,
    residuals: np.ndarray,
    *,
    df_resid: int,
) -> np.ndarray:
    """Analytic-weight classical covariance in whitened design coordinates."""
    if df_resid < 1:
        raise AnalysisError(f"classical covariance requires positive df_resid; found {df_resid}")
    sigma_squared = float(residuals @ residuals) / df_resid
    bread = np.linalg.pinv(x.T @ x)
    covariance = bread * sigma_squared
    _require_finite_covariance(covariance, context="classical covariance")
    return covariance


def _hc1_covariance(
    x: np.ndarray,
    residuals: np.ndarray,
    *,
    df_resid: int,
) -> np.ndarray:
    """HC1 covariance with the Phase 1 N / df_resid correction."""
    n = len(residuals)
    if df_resid < 1 or n <= 0:
        raise AnalysisError(
            "HC1 covariance requires positive sample and residual degrees of freedom"
        )
    bread = np.linalg.pinv(x.T @ x)
    scores = x * residuals[:, None]
    covariance = (n / df_resid) * (bread @ (scores.T @ scores) @ bread)
    _require_finite_covariance(covariance, context="HC1 covariance")
    return covariance


def _cr1_covariance(
    x: np.ndarray,
    residuals: np.ndarray,
    clusters: pd.Series,
    *,
    k_dof: int,
) -> np.ndarray:
    """Clustered sandwich with G/(G−1) and (N−1)/(N−k_dof) corrections."""
    if len(clusters) != len(residuals):
        raise AnalysisError("cluster vector length must equal the resolved sample size")
    if clusters.isna().any():
        raise AnalysisError("cluster vector cannot contain missing values")
    n = len(residuals)
    cluster_count = int(clusters.nunique(dropna=False))
    if cluster_count < 2:
        raise AnalysisError(
            f"clustered inference requires at least two clusters; found {cluster_count}"
        )
    if n <= 1 or n <= k_dof:
        raise AnalysisError(
            f"CR1 finite-sample correction requires N > k_dof; N={n}, k_dof={k_dof}"
        )
    bread = np.linalg.pinv(x.T @ x)
    meat = np.zeros((x.shape[1], x.shape[1]), dtype=float)
    cluster_values = clusters.to_numpy()
    for cluster in pd.unique(cluster_values):
        mask = cluster_values == cluster
        score = x[mask].T @ residuals[mask]
        meat += np.outer(score, score)
    correction = (cluster_count / (cluster_count - 1)) * ((n - 1) / (n - k_dof))
    covariance = correction * (bread @ meat @ bread)
    _require_finite_covariance(covariance, context="CR1 covariance")
    return covariance


def _require_finite_covariance(covariance: np.ndarray, *, context: str) -> None:
    if not np.isfinite(covariance).all():
        raise AnalysisError(f"{context} contains nonfinite entries")
