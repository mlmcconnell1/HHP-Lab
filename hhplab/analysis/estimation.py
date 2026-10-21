"""OLS, 2SLS, and restricted-fit estimation primitives."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Literal

import numpy as np
import pandas as pd
from scipy import stats

from .contracts import AnalysisError, _RegressionFit
from .model import AbsorbedDesign
from .stats import (
    _classical_covariance,
    _cluster_denominator_dof,
    _clustered_covariance,
    _clustered_standard_errors,
    _cr1_covariance,
    _hc1_covariance,
    _two_sided_p_value,
)


@dataclass(frozen=True)
class _WeightedRegressionFit:
    """Phase 1 WLS inference and finite fit-statistic results."""

    terms: tuple[str, ...]
    estimates: np.ndarray
    covariance: np.ndarray
    standard_errors: np.ndarray
    ci_lo: np.ndarray
    ci_hi: np.ndarray
    p_values: tuple[float | None, ...]
    p_status: tuple[str, ...]
    p_reasons: tuple[str | None, ...]
    inference_method: str
    inference_df: int
    residuals: np.ndarray
    ssr: float
    r2: float | None
    r2_status: str
    r2_reason: str | None
    r2_within: float | None
    r2_within_status: str
    r2_within_reason: str | None


def _fit_wls(
    *,
    design: AbsorbedDesign,
    inference_method: Literal["classical", "hc1", "clustered"],
    clusters: pd.Series | None = None,
    ci_level: float = 0.95,
) -> _WeightedRegressionFit:
    """Fit weighted absorbed design and compute Phase 1 finite-sample inference."""
    if inference_method not in {"classical", "hc1", "clustered"}:
        raise AnalysisError(f"unsupported WLS inference method: {inference_method!r}")
    if isinstance(ci_level, bool) or not isinstance(ci_level, (int, float)):
        raise AnalysisError("ci_level must be numeric and strictly between 0 and 1")
    if not math.isfinite(ci_level) or not 0 < ci_level < 1:
        raise AnalysisError("ci_level must be numeric and strictly between 0 and 1")
    n = len(design.outcome)
    if n == 0 or design.df_resid < 1:
        raise AnalysisError(
            f"WLS requires positive residual degrees of freedom; n={n}, df_resid={design.df_resid}"
        )

    if design.weighted_intercept is None:
        fit_x = design.regressors
        fit_y = design.outcome
        output_terms = design.terms
        transform = np.eye(len(output_terms), dtype=float)
        estimate_offset = np.zeros(len(output_terms), dtype=float)
        inference_df = design.df_resid
    else:
        intercept = design.weighted_intercept
        fit_x = np.column_stack((intercept, design.regressors))
        fit_y = design.outcome
        output_terms = ("(intercept)", *design.terms)
        transform = np.eye(len(output_terms), dtype=float)
        transform[0, 1:] = -design.regressor_means
        estimate_offset = np.zeros(len(output_terms), dtype=float)
        estimate_offset[0] = design.outcome_mean
        inference_df = design.df_resid
    if fit_x.shape[1] == 0:
        raise AnalysisError("WLS requires at least one requested slope or an intercept")
    if np.linalg.matrix_rank(fit_x) < fit_x.shape[1]:
        raise AnalysisError("rank_deficient: WLS design is not full rank")

    centered_coefficients, *_ = np.linalg.lstsq(fit_x, fit_y, rcond=None)
    residuals = fit_y - fit_x @ centered_coefficients
    ssr = float(residuals @ residuals)
    if not math.isfinite(ssr):
        raise AnalysisError("WLS residual sum of squares is nonfinite")
    if inference_method == "classical":
        centered_covariance = _classical_covariance(fit_x, residuals, df_resid=design.df_resid)
    elif inference_method == "hc1":
        centered_covariance = _hc1_covariance(fit_x, residuals, df_resid=design.df_resid)
    else:
        if clusters is None:
            raise AnalysisError("clustered inference requires a cluster vector")
        if design.k_dof >= n:
            raise AnalysisError(f"CR1 correction requires N > k_dof; N={n}, k_dof={design.k_dof}")
        centered_covariance = _cr1_covariance(fit_x, residuals, clusters, k_dof=design.k_dof)
        inference_df = _cluster_denominator_dof(clusters, design.df_resid)

    estimates = transform @ centered_coefficients + estimate_offset
    covariance = transform @ centered_covariance @ transform.T
    covariance = (covariance + covariance.T) / 2
    if not np.isfinite(estimates).all() or not np.isfinite(covariance).all():
        raise AnalysisError("WLS produced nonfinite estimates or covariance")
    variances = np.diag(covariance)
    variance_tolerance = 1e-12 * max(1.0, float(np.max(np.abs(variances))))
    if (variances < -variance_tolerance).any():
        raise AnalysisError("WLS covariance has a materially negative variance")
    standard_errors = np.sqrt(np.maximum(variances, 0.0))
    critical_value = float(stats.t.ppf((1.0 + ci_level) / 2.0, inference_df))
    ci_lo = estimates - critical_value * standard_errors
    ci_hi = estimates + critical_value * standard_errors
    if not np.isfinite(ci_lo).all() or not np.isfinite(ci_hi).all():
        raise AnalysisError("WLS confidence intervals contain nonfinite endpoints")

    p_values: list[float | None] = []
    p_status: list[str] = []
    p_reasons: list[str | None] = []
    for estimate, standard_error in zip(estimates, standard_errors, strict=True):
        if standard_error == 0 and estimate == 0:
            p_values.append(None)
            p_status.append("not_evaluated")
            p_reasons.append("zero_estimate_zero_se")
        elif standard_error == 0:
            p_values.append(0.0)
            p_status.append("evaluated")
            p_reasons.append(None)
        else:
            p_value = float(2.0 * stats.t.sf(abs(float(estimate / standard_error)), inference_df))
            if not math.isfinite(p_value):
                raise AnalysisError("WLS produced a nonfinite p-value")
            p_values.append(min(1.0, max(0.0, p_value)))
            p_status.append("evaluated")
            p_reasons.append(None)

    r2, r2_status, r2_reason = _fit_statistic(
        ssr, design.total_sum_squares, "zero_total_sum_squares"
    )
    r2_within, r2_within_status, r2_within_reason = _fit_statistic(
        ssr, design.within_sum_squares, "zero_within_sum_squares"
    )
    return _WeightedRegressionFit(
        terms=output_terms,
        estimates=estimates,
        covariance=covariance,
        standard_errors=standard_errors,
        ci_lo=ci_lo,
        ci_hi=ci_hi,
        p_values=tuple(p_values),
        p_status=tuple(p_status),
        p_reasons=tuple(p_reasons),
        inference_method=inference_method,
        inference_df=inference_df,
        residuals=residuals,
        ssr=ssr,
        r2=r2,
        r2_status=r2_status,
        r2_reason=r2_reason,
        r2_within=r2_within,
        r2_within_status=r2_within_status,
        r2_within_reason=r2_within_reason,
    )


def _fit_statistic(
    ssr: float,
    denominator: float,
    zero_reason: str,
) -> tuple[float | None, str, str | None]:
    if denominator == 0:
        return None, "not_evaluated", zero_reason
    value = 1.0 - ssr / denominator
    if not math.isfinite(value):
        return None, "not_evaluated", "nonfinite_fit_statistic"
    return value, "evaluated", None


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
    p_values = np.array([_two_sided_p_value(float(t_stat), p_value_dof) for t_stat in t_stats])
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
    p_values = np.array([_two_sided_p_value(float(t_stat), p_value_dof) for t_stat in t_stats])
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
