"""Data-dependent regression readiness over resolved analysis samples."""

from __future__ import annotations

from dataclasses import dataclass
from numbers import Real
from typing import Literal

import numpy as np
import pandas as pd

from .contracts import AnalysisError, ResolvedSampleFrame
from .model import AbsorbedDesign, build_absorbed_design


@dataclass(frozen=True)
class WeightValidityCounts:
    """Exclusive weight classifications for the already-resolved rows."""

    valid: int
    missing: int
    nonnumeric: int
    nonfinite: int
    nonpositive: int

    def to_dict(self) -> dict[str, int]:
        return {
            "valid": self.valid,
            "missing": self.missing,
            "nonnumeric": self.nonnumeric,
            "nonfinite": self.nonfinite,
            "nonpositive": self.nonpositive,
        }


@dataclass(frozen=True)
class RegressionReadiness:
    """Successful readiness evidence for one exact resolved sample."""

    n: int
    design_rank: int
    k_dof: int
    df_resid: int
    cluster_count: int | None
    warnings: tuple[str, ...]
    weight_validity: WeightValidityCounts
    absorbed: tuple[str, ...]
    nested_fixed_effects: tuple[str, ...]
    absorption_iterations: int
    design: AbsorbedDesign


def check_regression_readiness(
    sample: ResolvedSampleFrame,
    *,
    outcome: str,
    predictors: tuple[str, ...] | list[str],
    controls: tuple[str, ...] | list[str] = (),
    fixed_effects: tuple[str, ...] | list[str] = (),
    weights: str | None = None,
    cluster_by: str | None = None,
    inference_method: Literal["classical", "hc1", "clustered"] = "classical",
) -> RegressionReadiness:
    """Validate roles, weights, clusters and requested slope identifiability.

    This function inspects every row in the resolved frame and never filters or
    reorders it. Sample exclusions belong to resolution, before readiness.
    """
    if not isinstance(sample, ResolvedSampleFrame):
        raise AnalysisError("regression readiness requires a ResolvedSampleFrame")
    sample.validate_frame_identity()
    terms = (*tuple(predictors), *tuple(controls))
    fixed_effects = tuple(fixed_effects)
    if not outcome.strip():
        raise AnalysisError("outcome column name must be non-empty")
    if len(set(terms)) != len(terms):
        raise AnalysisError("predictor and control columns must be unique")
    if set(terms) & set(fixed_effects):
        repeated = sorted(set(terms) & set(fixed_effects))
        raise AnalysisError(f"numeric slope columns cannot also be fixed effects: {repeated}")
    if inference_method not in {"classical", "hc1", "clustered"}:
        raise AnalysisError(f"unsupported inference method for readiness: {inference_method!r}")

    weight_validity = _weight_validity(sample.frame, weights)
    invalid_weight_count = (
        weight_validity.missing
        + weight_validity.nonnumeric
        + weight_validity.nonfinite
        + weight_validity.nonpositive
    )
    if invalid_weight_count:
        raise AnalysisError(
            f"weight column {weights!r} has invalid included rows: {weight_validity.to_dict()}"
        )

    if cluster_by is not None and cluster_by not in sample.frame.columns:
        raise AnalysisError(f"cluster column {cluster_by!r} is missing from the resolved frame")
    cluster_count = (
        int(sample.frame[cluster_by].nunique(dropna=False)) if cluster_by is not None else None
    )
    if inference_method == "clustered" and (cluster_count is None or cluster_count < 2):
        raise AnalysisError(
            f"clustered inference requires at least two clusters; observed {cluster_count}"
        )

    design = build_absorbed_design(
        sample.frame,
        outcome=outcome,
        terms=terms,
        fixed_effects=fixed_effects,
        weights=weights,
        cluster_by=cluster_by,
    )
    if terms:
        unidentified: list[str] = []
        accepted = np.empty((len(sample.frame), 0), dtype=float)
        for index, term in enumerate(terms):
            candidate = design.regressors[:, index]
            residual = candidate.copy()
            if accepted.shape[1]:
                coefficients, *_ = np.linalg.lstsq(accepted, candidate, rcond=None)
                residual -= accepted @ coefficients
            threshold = 1e-12 * max(float(design.regressor_norms[index]), np.finfo(float).tiny)
            if np.linalg.norm(residual) <= threshold:
                unidentified.append(term)
            else:
                accepted = np.column_stack((accepted, candidate))
        if unidentified:
            raise AnalysisError(
                "rank_deficient: requested slopes are unidentified after fixed-effect "
                f"absorption: {unidentified}"
            )
    if design.df_resid < 1:
        raise AnalysisError(
            "insufficient_degrees_of_freedom: "
            f"n={len(sample.frame)}, design_rank={design.design_rank}, "
            f"df_resid={design.df_resid}"
        )
    if inference_method == "clustered" and design.k_dof >= len(sample.frame):
        raise AnalysisError(
            "insufficient_degrees_of_freedom: clustered correction requires "
            f"n > k_dof; n={len(sample.frame)}, k_dof={design.k_dof}"
        )

    warnings = ()
    if cluster_count is not None and cluster_count < 30:
        warnings = (
            f"low_cluster_count: observed {cluster_count}; "
            "consider a bootstrap method when available",
        )
    return RegressionReadiness(
        n=len(sample.frame),
        design_rank=design.design_rank,
        k_dof=design.k_dof,
        df_resid=design.df_resid,
        cluster_count=cluster_count,
        warnings=warnings,
        weight_validity=weight_validity,
        absorbed=fixed_effects,
        nested_fixed_effects=design.nested_fixed_effects,
        absorption_iterations=design.iterations,
        design=design,
    )


def _weight_validity(frame: pd.DataFrame, column: str | None) -> WeightValidityCounts:
    if column is None:
        return WeightValidityCounts(
            valid=len(frame), missing=0, nonnumeric=0, nonfinite=0, nonpositive=0
        )
    if column not in frame.columns:
        raise AnalysisError(f"weight column {column!r} is missing from the resolved frame")
    missing = 0
    nonnumeric = 0
    nonfinite = 0
    nonpositive = 0
    for raw_value in frame[column].array:
        if raw_value is None or raw_value is pd.NA or raw_value is pd.NaT:
            missing += 1
            continue
        if isinstance(raw_value, np.generic):
            raw_value = raw_value.item()
        if isinstance(raw_value, bool) or not isinstance(raw_value, Real):
            nonnumeric += 1
            continue
        value = float(raw_value)
        if not np.isfinite(value):
            nonfinite += 1
        elif value <= 0:
            nonpositive += 1
    invalid = missing + nonnumeric + nonfinite + nonpositive
    return WeightValidityCounts(
        valid=len(frame) - invalid,
        missing=missing,
        nonnumeric=nonnumeric,
        nonfinite=nonfinite,
        nonpositive=nonpositive,
    )
