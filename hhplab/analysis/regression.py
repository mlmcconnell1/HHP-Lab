"""Panel regression orchestration for OLS and 2SLS."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import numpy as np
import pandas as pd

from .contracts import (
    AnalysisError,
    AnalysisExecutionResult,
    AnalysisResult,
    InferenceMethod,
    ResolvedRegressionResult,
    ResolvedSampleFrame,
)
from .estimation import (
    _first_stage_f_statistic,
    _fit_2sls,
    _fit_ols,
    _fit_wls,
    _parse_inference_terms,
    _WeightedRegressionFit,
)
from .model import (
    _fixed_effect_dummies,
    _standardize_model_columns,
)
from .persistence import _persist_result, _read_panel, _require_columns
from .readiness import check_regression_readiness
from .resampling import (
    _permutation_p_values,
    _warn_if_permutation_model_has_correlated_controls,
    _wild_cluster_bootstrap_p_values,
)


def regress_resolved_frame(
    sample: ResolvedSampleFrame,
    *,
    analysis: str,
    variant: str = "base",
    sample_group: str | None = None,
    outcome: str,
    predictors: list[str] | tuple[str, ...],
    controls: list[str] | tuple[str, ...] = (),
    fixed_effects: list[str] | tuple[str, ...] = (),
    weights: str | None = None,
    inference_method: Literal["classical", "hc1", "clustered"] = "classical",
    cluster_by: str | None = None,
    ci_level: float = 0.95,
    base_term_by_effective_term: dict[str, str] | None = None,
) -> ResolvedRegressionResult:
    """Estimate regression on an exact resolved sample without filtering rows."""
    if not isinstance(sample, ResolvedSampleFrame):
        raise AnalysisError("regression requires a ResolvedSampleFrame")
    if not isinstance(analysis, str) or not analysis.strip():
        raise AnalysisError("analysis id must be a non-empty string")
    if not isinstance(variant, str) or not variant.strip():
        raise AnalysisError("variant id must be a non-empty string")
    sample_key = f"{analysis}@{variant}"
    sample_source = sample_group or sample_key
    if not isinstance(sample_source, str) or not sample_source.strip():
        raise AnalysisError("sample_group must be a non-empty string when supplied")
    predictors = tuple(predictors)
    controls = tuple(controls)
    fixed_effects = tuple(fixed_effects)
    terms = (*predictors, *controls)
    if "(intercept)" in terms:
        raise AnalysisError("'(intercept)' is reserved for the fitted intercept")
    if base_term_by_effective_term is None:
        base_terms = {term: term for term in terms}
    else:
        base_terms = dict(base_term_by_effective_term)
        if set(base_terms) != set(terms):
            raise AnalysisError(
                "base_term_by_effective_term must map every effective predictor/control exactly"
            )
        if any(not isinstance(value, str) or not value for value in base_terms.values()):
            raise AnalysisError("base terms must be non-empty strings")

    try:
        sample.validate_frame_identity()
    except (TypeError, ValueError) as exc:
        raise AnalysisError(f"sample_contract_violation: {exc}") from exc
    expected_keys = sample.row_keys
    try:
        readiness = check_regression_readiness(
            sample,
            outcome=outcome,
            predictors=predictors,
            controls=controls,
            fixed_effects=fixed_effects,
            weights=weights,
            cluster_by=cluster_by,
            inference_method=inference_method,
        )
    except ValueError as exc:
        if "row_keys must exactly match" in str(exc) or "missing key column" in str(exc):
            raise AnalysisError(f"sample_contract_violation: {exc}") from exc
        raise
    _validate_regression_row_alignment(sample, readiness.design, expected_keys)
    fit = _fit_wls(
        design=readiness.design,
        inference_method=inference_method,
        clusters=sample.frame[cluster_by].rename(cluster_by) if cluster_by else None,
        ci_level=ci_level,
    )
    _validate_fit_row_alignment(sample, fit, expected_keys)

    estimate_rows = _regression_estimate_rows(
        fit,
        analysis=analysis,
        variant=variant,
        predictor_terms=set(predictors),
        control_terms=set(controls),
        base_terms=base_terms,
        ci_level=ci_level,
        inference_method=inference_method,
    )
    fit_row = {
        "analysis": analysis,
        "variant": variant,
        "sample_source": sample_source,
        "n": readiness.n,
        "n_clusters": readiness.cluster_count,
        "design_rank": readiness.design_rank,
        "k_dof": readiness.k_dof,
        "df_resid": readiness.df_resid,
        "r2": fit.r2,
        "r2_status": fit.r2_status,
        "r2_reason": fit.r2_reason,
        "r2_within": fit.r2_within,
        "r2_within_status": fit.r2_within_status,
        "r2_within_reason": fit.r2_within_reason,
        "absorbed": list(fixed_effects),
    }
    design_evidence = _regression_design_evidence(
        sample,
        row_keys=expected_keys,
        regressors=terms,
        fixed_effects=fixed_effects,
        normalized_weights=readiness.design.normalized_weights,
        sample_group=sample_source,
    )
    execution = AnalysisExecutionResult(
        status="complete",
        consumed_row_keys=expected_keys,
    )
    try:
        execution.validate_sample(sample)
    except (TypeError, ValueError) as exc:
        raise AnalysisError(f"sample_contract_violation: {exc}") from exc
    return ResolvedRegressionResult(
        regression_estimates=tuple(estimate_rows),
        regression_fit=fit_row,
        execution=execution,
        warnings=readiness.warnings,
        design_evidence={sample_key: design_evidence},
    )


def _validate_regression_row_alignment(sample, design, expected_keys) -> None:
    try:
        sample.validate_frame_identity()
    except (TypeError, ValueError) as exc:
        raise AnalysisError(f"sample_contract_violation: {exc}") from exc
    n = len(expected_keys)
    arrays = (design.outcome, design.regressors, design.normalized_weights)
    if any(len(array) != n for array in arrays):
        raise AnalysisError(
            "sample_contract_violation: estimator design row count differs from resolved sample"
        )
    if sample.row_keys != expected_keys:
        raise AnalysisError(
            "sample_contract_violation: estimator changed resolved sample row identity or order"
        )


def _validate_fit_row_alignment(sample, fit: _WeightedRegressionFit, expected_keys) -> None:
    try:
        sample.validate_frame_identity()
    except (TypeError, ValueError) as exc:
        raise AnalysisError(f"sample_contract_violation: {exc}") from exc
    if len(fit.residuals) != len(expected_keys):
        raise AnalysisError(
            "sample_contract_violation: estimator result row count differs from resolved sample"
        )
    if sample.row_keys != expected_keys:
        raise AnalysisError(
            "sample_contract_violation: estimator changed resolved sample row identity or order"
        )


def _regression_estimate_rows(
    fit: _WeightedRegressionFit,
    *,
    analysis: str,
    variant: str,
    predictor_terms: set[str],
    control_terms: set[str],
    base_terms: dict[str, str],
    ci_level: float,
    inference_method: str,
) -> list[dict[str, Any]]:
    rows = []
    for index, term in enumerate(fit.terms):
        if term == "(intercept)":
            role = "intercept"
            base_term = "(intercept)"
        elif term in predictor_terms:
            role = "predictor"
            base_term = base_terms[term]
        elif term in control_terms:
            role = "control"
            base_term = base_terms[term]
        else:
            raise AnalysisError(
                f"sample_contract_violation: estimator emitted unknown term {term!r}"
            )
        rows.append(
            {
                "analysis": analysis,
                "variant": variant,
                "term": term,
                "base_term": base_term,
                "role": role,
                "estimate": float(fit.estimates[index]),
                "se": float(fit.standard_errors[index]),
                "df": fit.inference_df,
                "ci_lo": float(fit.ci_lo[index]),
                "ci_hi": float(fit.ci_hi[index]),
                "ci_level": float(ci_level),
                "ci_method": "wald",
                "p": fit.p_values[index],
                "p_status": fit.p_status[index],
                "p_reason": fit.p_reasons[index],
                "inference_method": inference_method,
            }
        )
    return rows


def _regression_design_evidence(
    sample: ResolvedSampleFrame,
    *,
    row_keys,
    regressors: tuple[str, ...],
    fixed_effects: tuple[str, ...],
    normalized_weights: np.ndarray,
    sample_group: str,
) -> dict[str, Any]:
    try:
        sample.validate_frame_identity()
    except (TypeError, ValueError) as exc:
        raise AnalysisError(f"sample_contract_violation: {exc}") from exc
    if len(normalized_weights) != len(row_keys):
        raise AnalysisError(
            "sample_contract_violation: design evidence row count differs from resolved sample"
        )
    frame = sample.frame
    rows = []
    for position, (geo, time) in enumerate(row_keys):
        rows.append(
            {
                "key": [_json_scalar(geo), _json_scalar(time)],
                "regressors": [_json_scalar(frame[name].iloc[position]) for name in regressors],
                "fixed_effects": [
                    _json_scalar(frame[name].iloc[position]) for name in fixed_effects
                ],
                "weight": float(normalized_weights[position]),
            }
        )
    return {
        "sample_group": sample_group,
        "regressors": list(regressors),
        "fixed_effects": list(fixed_effects),
        "rows": rows,
    }


def _json_scalar(value: Any) -> Any:
    if isinstance(value, np.generic):
        value = value.item()
    if value is pd.NA or value is pd.NaT:
        raise AnalysisError("sample_contract_violation: design evidence contains a null value")
    if isinstance(value, float) and not np.isfinite(value):
        raise AnalysisError("sample_contract_violation: design evidence contains a nonfinite value")
    if not isinstance(value, (str, int, float, bool)) and value is not None:
        raise AnalysisError(
            f"sample_contract_violation: design evidence contains unsupported value {value!r}"
        )
    return value


def regress_panel(
    panel_path: Path,
    *,
    outcome: str,
    predictors: list[str],
    entity_column: str = "geo_id",
    year_column: str = "year",
    entity_fe: bool = True,
    year_fe: bool = True,
    cluster_by: str | None = "geo_id",
    standardize: str = "none",
    inference: InferenceMethod = "none",
    inference_reps: int = 999,
    inference_seed: int = 0,
    inference_terms: list[str] | None = None,
    endogenous: str | None = None,
    instruments: list[str] | None = None,
    output_path: Path | None = None,
) -> AnalysisResult:
    """Run OLS (or 2SLS with --endogenous/--instruments) with optional fixed effects."""
    if standardize not in {"none", "predictors", "all"}:
        raise AnalysisError("--standardize must be one of: none, predictors, all.")
    if inference not in {"none", "wild-cluster", "permutation"}:
        raise AnalysisError("--inference must be one of: none, wild-cluster, permutation.")
    if inference_reps < 1:
        raise AnalysisError("--inference-reps must be positive.")
    instruments = instruments or []
    if (endogenous is None) != (len(instruments) == 0):
        raise AnalysisError(
            "2SLS requires both --endogenous and --instruments. Provide the endogenous "
            "predictor plus at least one excluded instrument column, or neither for OLS."
        )
    if endogenous is not None:
        if endogenous not in predictors:
            raise AnalysisError(
                f"--endogenous '{endogenous}' must be one of the model predictors: {predictors}."
            )
        overlapping = sorted(set(instruments) & {outcome, *predictors})
        if overlapping:
            raise AnalysisError(
                f"--instruments must be excluded from the structural equation; remove "
                f"{overlapping} from the outcome/predictors or choose different instruments."
            )
        if inference == "permutation":
            raise AnalysisError(
                "Permutation --inference is not supported with 2SLS. Use wild-cluster "
                "inference with --cluster-by, or run the Anderson-Rubin helper."
            )
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
    model_df = model_df.dropna(subset=drop_subset)
    if len(model_df) <= len(predictors):
        raise AnalysisError("regress has too few complete rows for the requested model.")

    standardize_columns: list[str] = []
    if standardize in {"predictors", "all"}:
        standardize_columns.extend(predictors)
    if standardize == "all":
        standardize_columns.append(outcome)
    standardization = _standardize_model_columns(model_df, standardize_columns)

    x_parts = [pd.Series(1.0, index=model_df.index, name="Intercept"), model_df[predictors]]
    if entity_fe:
        x_parts.append(_fixed_effect_dummies(model_df, entity_column, "entity"))
    if year_fe:
        x_parts.append(_fixed_effect_dummies(model_df, year_column, "year"))
    design = pd.concat(x_parts, axis=1).astype(float)
    y = model_df[outcome].to_numpy(dtype=float)
    x = design.to_numpy(dtype=float)
    rank = int(np.linalg.matrix_rank(x))
    dof = int(len(y) - rank)
    if dof < 1:
        raise AnalysisError(
            "regress model is saturated or rank-deficient after fixed effects: "
            f"n={len(y)}, design_columns={x.shape[1]}, rank={rank}, residual_dof={dof}. "
            "Use fewer predictors/fixed effects or a larger panel."
        )
    clusters = model_df[cluster_by].rename(cluster_by) if cluster_by is not None else None
    first_stage: dict[str, Any] | None = None
    first_stage_table: pd.DataFrame | None = None
    if endogenous is not None:
        z_design = design.drop(columns=[endogenous]).copy()
        for instrument in instruments:
            z_design[instrument] = model_df[instrument].astype(float)
        z = z_design.to_numpy(dtype=float)
        z_rank = int(np.linalg.matrix_rank(z))
        if z_rank < rank:
            raise AnalysisError(
                "2SLS instrument matrix has lower rank than the structural design "
                f"(rank {z_rank} < {rank}); the model is underidentified. Add "
                "instruments or drop collinear columns."
            )
        first_stage_dof = int(len(y) - z_rank)
        if first_stage_dof < 1:
            raise AnalysisError(
                "2SLS first stage is saturated or rank-deficient: "
                f"n={len(y)}, instrument_design_rank={z_rank}."
            )
        fit = _fit_2sls(x=x, z=z, y=y, dof=dof, clusters=clusters)
        endog_values = model_df[endogenous].to_numpy(dtype=float)
        first_stage_fit = _fit_ols(x=z, y=endog_values, dof=first_stage_dof, clusters=clusters)
        instrument_indices = [int(z_design.columns.get_loc(name)) for name in instruments]
        f_stat, f_p_value = _first_stage_f_statistic(
            fit=first_stage_fit,
            z=z,
            instrument_indices=instrument_indices,
            dof=first_stage_dof,
            clusters=clusters,
        )
        first_stage = {
            "endogenous": endogenous,
            "instruments": list(instruments),
            "f_statistic": f_stat,
            "f_p_value": f_p_value,
            "r_squared": first_stage_fit.r_squared,
            "dof": first_stage_dof,
        }
        first_stage_table = pd.DataFrame(
            {
                "term": z_design.columns,
                "estimate": first_stage_fit.beta,
                "std_error": first_stage_fit.std_errors,
                "t_stat": first_stage_fit.t_stats,
                "asymptotic_p_value": first_stage_fit.p_values,
            }
        )
        first_stage_table = first_stage_table[
            first_stage_table["term"].isin(instruments)
        ].reset_index(drop=True)
        first_stage_table["p_value"] = first_stage_table["asymptotic_p_value"]
        first_stage_table["stage"] = "first_stage"
        first_stage_table["outcome"] = endogenous
        first_stage_table["r_squared"] = first_stage_fit.r_squared
        first_stage_table["dof"] = first_stage_dof
        first_stage_table["std_error_type"] = first_stage_fit.std_error_type
    else:
        fit = _fit_ols(x=x, y=y, dof=dof, clusters=clusters)
    selected_inference_terms = _parse_inference_terms(inference_terms, design.columns)
    if inference != "none" and not selected_inference_terms:
        selected_inference_terms = [term for term in predictors if term in set(design.columns)]
    inference_p_values: dict[str, float] = {}
    if inference == "wild-cluster":
        if clusters is None:
            raise AnalysisError("wild-cluster inference requires --cluster-by.")
        inference_p_values = _wild_cluster_bootstrap_p_values(
            x=x,
            y=y,
            z=z if endogenous is not None else None,
            fit=fit,
            dof=dof,
            clusters=clusters,
            terms=selected_inference_terms,
            design_columns=design.columns,
            reps=inference_reps,
            seed=inference_seed,
        )
    elif inference == "permutation":
        if entity_fe or year_fe:
            raise AnalysisError(
                "permutation inference is currently supported for cross-sectional models "
                "without fixed effects."
            )
        _warn_if_permutation_model_has_correlated_controls(
            predictors=predictors,
            selected_inference_terms=selected_inference_terms,
        )
        inference_p_values = _permutation_p_values(
            model_df=model_df,
            y=y,
            design=design,
            dof=dof,
            clusters=clusters,
            terms=selected_inference_terms,
            fit=fit,
            reps=inference_reps,
            seed=inference_seed,
        )
    coef = pd.DataFrame(
        {
            "term": design.columns,
            "estimate": fit.beta,
            "std_error": fit.std_errors,
        }
    )
    coef["t_stat"] = fit.t_stats
    coef["asymptotic_p_value"] = fit.p_values
    if inference_p_values:
        coef["p_value"] = coef["term"].map(lambda term: inference_p_values.get(str(term), pd.NA))
        coef["p_value"] = pd.to_numeric(coef["p_value"], errors="coerce").fillna(
            coef["asymptotic_p_value"]
        )
    else:
        coef["p_value"] = coef["asymptotic_p_value"]
    coef["inference_method"] = inference
    coef["inference_reps"] = inference_reps if inference != "none" else 0
    coef["inference_seed"] = inference_seed if inference != "none" else pd.NA
    coef["inference_term"] = coef["term"].isin(selected_inference_terms)
    coef["stage"] = "structural"
    coef["estimator"] = "2sls" if endogenous is not None else "ols"
    coef["outcome"] = outcome
    coef["n"] = int(len(y))
    coef["design_rank"] = rank
    coef["dof"] = dof
    coef["r_squared"] = fit.r_squared
    coef["std_error_type"] = fit.std_error_type
    if first_stage_table is not None:
        first_stage_table["estimator"] = "2sls"
        first_stage_table["n"] = int(len(y))
        first_stage_table["inference_method"] = "none"
        first_stage_table["inference_reps"] = 0
        first_stage_table["inference_term"] = False
        coef = pd.concat([coef, first_stage_table], ignore_index=True)
    coef["standardization"] = standardize
    coef["standardized"] = coef["term"].map(
        lambda term: bool(standardization.get(str(term), {}).get("standardized", False))
    )
    coef["standardization_mean"] = coef["term"].map(
        lambda term: standardization.get(str(term), {}).get("mean", pd.NA)
    )
    coef["standardization_std"] = coef["term"].map(
        lambda term: standardization.get(str(term), {}).get("std", pd.NA)
    )
    coef["standardization_note"] = coef["term"].map(
        lambda term: standardization.get(str(term), {}).get("note", "")
    )
    return _persist_result(
        coef,
        panel_path=panel_path,
        output_path=output_path,
        analysis_type="regress",
        parameters={
            "outcome": outcome,
            "predictors": predictors,
            "entity_column": entity_column,
            "year_column": year_column,
            "entity_fe": entity_fe,
            "year_fe": year_fe,
            "cluster_by": cluster_by,
            "standardize": standardize,
            "standardization": standardization,
            "inference": inference,
            "inference_reps": inference_reps,
            "inference_seed": inference_seed,
            "inference_terms": selected_inference_terms,
            "endogenous": endogenous,
            "instruments": list(instruments),
        },
        metadata={
            "analysis_type": "regress",
            "outcome": outcome,
            "predictors": predictors,
            "estimator": "2sls" if endogenous is not None else "ols",
            "first_stage": first_stage,
            "n": int(len(y)),
            "design_rank": rank,
            "dof": dof,
            "r_squared": float(coef["r_squared"].iloc[0]),
            "std_error_type": fit.std_error_type,
            "standardize": standardize,
            "inference": inference,
            "inference_reps": inference_reps if inference != "none" else 0,
            "inference_seed": inference_seed if inference != "none" else None,
            "inference_terms": selected_inference_terms,
            "standardized_terms": [
                term for term, spec in standardization.items() if spec["standardized"]
            ],
            "unstandardized_terms": [
                term for term, spec in standardization.items() if not spec["standardized"]
            ],
        },
    )
