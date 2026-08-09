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
