"""OLS, 2SLS, and restricted-fit estimation primitives."""

from __future__ import annotations

import numpy as np
import pandas as pd

from .contracts import AnalysisError, _RegressionFit
from .stats import (
    _cluster_denominator_dof,
    _clustered_covariance,
    _clustered_standard_errors,
    _two_sided_p_value,
)


def _fit_ols(
    *,
    x: np.ndarray,
    y: np.ndarray,
    dof: int,
    clusters: pd.Series | None,
) -> _RegressionFit:
    beta, *_ = np.linalg.lstsq(x, y, rcond=None)
    fitted = x @ beta
    residuals = y - fitted
    sigma2 = float((residuals @ residuals) / dof)
    naive_se = np.sqrt(np.clip(np.diag(np.linalg.pinv(x.T @ x)) * sigma2, 0, None))
    if clusters is not None:
        std_errors = _clustered_standard_errors(x, residuals, clusters)
        std_error_type = f"clustered:{clusters.name}"
    else:
        std_errors = naive_se
        std_error_type = "ols"
    t_stats = beta / pd.Series(std_errors).replace(0, np.nan).to_numpy(dtype=float)
    p_value_dof = _cluster_denominator_dof(clusters, dof)
    p_values = np.array(
        [_two_sided_p_value(float(t_stat), p_value_dof) for t_stat in t_stats]
    )
    denom = np.sum((y - y.mean()) ** 2)
    r_squared = float(1 - (residuals @ residuals) / denom) if denom > 0 else np.nan
    return _RegressionFit(
        beta=beta,
        fitted=fitted,
        residuals=residuals,
        std_errors=std_errors,
        std_error_type=std_error_type,
        t_stats=t_stats,
        p_values=p_values,
        r_squared=r_squared,
    )


def _fit_2sls(
    *,
    x: np.ndarray,
    z: np.ndarray,
    y: np.ndarray,
    dof: int,
    clusters: pd.Series | None,
) -> _RegressionFit:
    """Two-stage least squares via the projected design X-hat = P_Z X.

    Standard errors use the structural residuals y - X @ beta (not the
    second-stage OLS residuals) with the projected design as the score matrix,
    which is the conventional 2SLS sandwich.
    """
    zz_inv = np.linalg.pinv(z.T @ z)
    x_hat = z @ (zz_inv @ (z.T @ x))
    beta = np.linalg.pinv(x_hat.T @ x) @ (x_hat.T @ y)
    fitted = x @ beta
    residuals = y - fitted
    sigma2 = float((residuals @ residuals) / dof)
    bread = np.linalg.pinv(x_hat.T @ x_hat)
    naive_se = np.sqrt(np.clip(np.diag(bread) * sigma2, 0, None))
    if clusters is not None:
        std_errors = _clustered_standard_errors(x_hat, residuals, clusters)
        std_error_type = f"clustered:{clusters.name}"
    else:
        std_errors = naive_se
        std_error_type = "iv_homoskedastic"
    t_stats = beta / pd.Series(std_errors).replace(0, np.nan).to_numpy(dtype=float)
    p_value_dof = _cluster_denominator_dof(clusters, dof)
    p_values = np.array(
        [_two_sided_p_value(float(t_stat), p_value_dof) for t_stat in t_stats]
    )
    denom = np.sum((y - y.mean()) ** 2)
    r_squared = float(1 - (residuals @ residuals) / denom) if denom > 0 else np.nan
    return _RegressionFit(
        beta=beta,
        fitted=fitted,
        residuals=residuals,
        std_errors=std_errors,
        std_error_type=std_error_type,
        t_stats=t_stats,
        p_values=p_values,
        r_squared=r_squared,
    )


def _restricted_iv_fitted_and_residuals(
    *,
    x: np.ndarray,
    z: np.ndarray,
    y: np.ndarray,
    restricted_index: int,
    null_value: float = 0.0,
) -> tuple[np.ndarray, np.ndarray]:
    """Constrained 2SLS fit under H0: beta[restricted_index] == null_value."""
    keep = [index for index in range(x.shape[1]) if index != restricted_index]
    restricted_beta = np.zeros(x.shape[1], dtype=float)
    restricted_beta[restricted_index] = null_value
    adjusted_y = y - x[:, restricted_index] * null_value
    if keep:
        x_keep = x[:, keep]
        ztz_inv = np.linalg.pinv(z.T @ z)
        projected_x_keep = z @ (ztz_inv @ (z.T @ x_keep))
        restricted_beta[keep] = np.linalg.pinv(projected_x_keep.T @ x_keep) @ (
            projected_x_keep.T @ adjusted_y
        )
    fitted = x @ restricted_beta
    return fitted, y - fitted


def _restricted_ols_fitted_and_residuals(
    *,
    x: np.ndarray,
    y: np.ndarray,
    restricted_index: int,
    null_value: float = 0.0,
) -> tuple[np.ndarray, np.ndarray]:
    """Constrained OLS fit under H0: beta[restricted_index] == null_value."""
    keep = [index for index in range(x.shape[1]) if index != restricted_index]
    restricted_beta = np.zeros(x.shape[1], dtype=float)
    restricted_beta[restricted_index] = null_value
    adjusted_y = y - x[:, restricted_index] * null_value
    if keep:
        x_keep = x[:, keep]
        restricted_beta[keep] = np.linalg.pinv(x_keep.T @ x_keep) @ (x_keep.T @ adjusted_y)
    fitted = x @ restricted_beta
    return fitted, y - fitted


def _first_stage_f_statistic(
    *,
    fit: _RegressionFit,
    z: np.ndarray,
    instrument_indices: list[int],
    dof: int,
    clusters: pd.Series | None,
) -> tuple[float, float]:
    """Wald F on the excluded instruments in the first-stage regression."""
    denominator_dof = dof
    if clusters is not None:
        covariance = _clustered_covariance(z, fit.residuals, clusters)
        denominator_dof = _cluster_denominator_dof(clusters, dof)
    else:
        sigma2 = float((fit.residuals @ fit.residuals) / dof)
        covariance = np.linalg.pinv(z.T @ z) * sigma2
    subset = np.ix_(instrument_indices, instrument_indices)
    beta_sub = fit.beta[instrument_indices]
    cov_sub = covariance[subset]
    try:
        wald = float(beta_sub @ np.linalg.solve(cov_sub, beta_sub))
    except np.linalg.LinAlgError:
        return np.nan, np.nan
    k = len(instrument_indices)
    f_stat = wald / k
    if denominator_dof < 1:
        return f_stat, np.nan
    try:
        from scipy import stats  # type: ignore[import-not-found]

        p_value = float(stats.f.sf(f_stat, k, denominator_dof))
    except Exception:
        p_value = np.nan
    return f_stat, p_value


def _parse_inference_terms(terms: list[str] | None, design_columns: pd.Index) -> list[str]:
    requested = terms or []
    if not requested:
        return []
    missing = [term for term in requested if term not in set(design_columns)]
    if missing:
        raise AnalysisError(
            f"--inference-terms references model terms not in the regression design: {missing}. "
            f"Available terms: {list(design_columns)}"
        )
    return requested
