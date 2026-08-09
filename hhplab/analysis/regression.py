"""Panel regression orchestration for OLS and 2SLS."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .contracts import AnalysisError, AnalysisResult, InferenceMethod
from .estimation import _first_stage_f_statistic, _fit_2sls, _fit_ols, _parse_inference_terms
from .model import (
    _fixed_effect_dummies,
    _standardize_model_columns,
)
from .persistence import _persist_result, _read_panel, _require_columns
from .resampling import (
    _permutation_p_values,
    _warn_if_permutation_model_has_correlated_controls,
    _wild_cluster_bootstrap_p_values,
)


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
        first_stage_fit = _fit_ols(
            x=z, y=endog_values, dof=first_stage_dof, clusters=clusters
        )
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
        coef["p_value"] = coef["term"].map(
            lambda term: inference_p_values.get(str(term), pd.NA)
        )
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
