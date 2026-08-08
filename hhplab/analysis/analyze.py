"""Agent-facing analysis helpers for panel Parquet artifacts."""

from __future__ import annotations

import hashlib
import json
import math
import warnings
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from statistics import NormalDist
from typing import Any, Literal

import numpy as np
import pandas as pd

from hhplab.artifacts.naming.naming import analysis_manifest_path, analysis_output_path
from hhplab.schema.measures import resolve_panel_measure_entry
from hhplab.storage.provenance import (
    ProvenanceBlock,
    read_provenance,
    write_parquet_with_provenance,
)


class AnalysisError(ValueError):
    """Raised when an analysis request is invalid for the input panel."""


@dataclass(frozen=True)
class AnalysisResult:
    """A persisted analysis result plus JSON-ready summary metadata."""

    table: pd.DataFrame
    output_path: Path
    manifest_path: Path
    metadata: dict[str, Any]

    def to_payload(self) -> dict[str, Any]:
        return _json_safe(
            {
                "status": "ok",
                "output_path": str(self.output_path),
                "manifest_path": str(self.manifest_path),
                **self.metadata,
                "records": self.table.to_dict(orient="records"),
            }
        )


InferenceMethod = Literal["none", "wild-cluster", "permutation"]


@dataclass(frozen=True)
class _RegressionFit:
    beta: np.ndarray
    fitted: np.ndarray
    residuals: np.ndarray
    std_errors: np.ndarray
    std_error_type: str
    t_stats: np.ndarray
    p_values: np.ndarray
    r_squared: float


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_panel(panel_path: Path) -> pd.DataFrame:
    if not panel_path.exists():
        raise AnalysisError(f"Panel parquet not found: {panel_path}")
    return pd.read_parquet(panel_path)


def _require_columns(df: pd.DataFrame, columns: list[str], *, context: str) -> None:
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise AnalysisError(
            f"{context} references missing panel columns {missing}. "
            f"Available columns: {sorted(df.columns.tolist())}"
        )


def _numeric_columns(df: pd.DataFrame, requested: list[str] | None) -> list[str]:
    if requested:
        _require_columns(df, requested, context="analysis")
        return requested
    return [
        column
        for column in df.columns
        if pd.api.types.is_numeric_dtype(df[column]) and column != "year"
    ]


def _measure_semantics(column: str, *, panel_columns: list[str]) -> dict[str, Any]:
    entry = resolve_panel_measure_entry(column, panel_columns=panel_columns)
    if entry is None:
        return {}
    return {
        "definition": entry.definition,
        "units": entry.units,
        "source_provider": entry.source_provider,
        "source_product": entry.source_product,
        "native_geometry": entry.native_geometry,
        "role_hint": entry.role_hint,
    }


def _analysis_provenance(
    *,
    analysis_type: str,
    panel_path: Path,
    parameters: dict[str, Any],
) -> ProvenanceBlock:
    input_provenance = read_provenance(panel_path)
    return ProvenanceBlock(
        extra={
            "dataset_type": "analysis_result",
            "analysis_type": analysis_type,
            "input_panel": str(panel_path),
            "input_panel_provenance": (
                input_provenance.to_dict() if input_provenance is not None else None
            ),
            "parameters": parameters,
        }
    )


def _json_safe(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if hasattr(value, "item"):
        return _json_safe(value.item())
    if pd.isna(value) and not isinstance(value, (list, tuple, dict)):
        return None
    return value


def _result_summary(table: pd.DataFrame, metadata: dict[str, Any]) -> dict[str, Any]:
    return {
        "row_count": int(len(table)),
        "columns": [str(column) for column in table.columns],
        "metadata": _json_safe(metadata),
    }


def _write_analysis_manifest(
    *,
    panel_path: Path,
    output_path: Path,
    analysis_type: str,
    parameters: dict[str, Any],
    metadata: dict[str, Any],
    table: pd.DataFrame,
) -> Path:
    input_provenance = read_provenance(panel_path)
    manifest_path = analysis_manifest_path(output_path)
    manifest = {
        "manifest_version": 1,
        "created_at": datetime.now(UTC).isoformat(),
        "analysis_type": analysis_type,
        "specification": {
            "analysis_type": analysis_type,
            "parameters": _json_safe(parameters),
        },
        "panel": {
            "path": str(panel_path),
            "name": panel_path.stem,
            "sha256": _sha256(panel_path),
            "provenance": input_provenance.to_dict() if input_provenance else None,
        },
        "output": {
            "path": str(output_path),
            "manifest_path": str(manifest_path),
            "sha256": _sha256(output_path),
        },
        "result_summary": _result_summary(table, metadata),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    return manifest_path


def _persist_result(
    table: pd.DataFrame,
    *,
    panel_path: Path,
    output_path: Path | None,
    analysis_type: str,
    parameters: dict[str, Any],
    metadata: dict[str, Any],
) -> AnalysisResult:
    resolved_output = output_path or analysis_output_path(panel_path, analysis_type)
    provenance = _analysis_provenance(
        analysis_type=analysis_type,
        panel_path=panel_path,
        parameters=parameters,
    )
    write_parquet_with_provenance(table, resolved_output, provenance)
    manifest_path = _write_analysis_manifest(
        panel_path=panel_path,
        output_path=resolved_output,
        analysis_type=analysis_type,
        parameters=parameters,
        metadata=metadata,
        table=table,
    )
    return AnalysisResult(
        table=table,
        output_path=resolved_output,
        manifest_path=manifest_path,
        metadata=metadata,
    )


def read_analysis_manifest(path: Path) -> dict[str, Any]:
    """Read an analysis manifest sidecar."""
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except OSError as exc:
        raise AnalysisError(f"Analysis manifest not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise AnalysisError(f"Invalid analysis manifest JSON: {path}") from exc


def list_analysis_manifests(
    directory: Path,
    *,
    analysis_type: str | None = None,
    panel: str | None = None,
) -> list[dict[str, Any]]:
    """List analysis manifest summaries under a directory."""
    if not directory.exists():
        raise AnalysisError(f"Analysis manifest directory not found: {directory}")
    rows: list[dict[str, Any]] = []
    for manifest_path in sorted(directory.rglob("*.manifest.json")):
        try:
            manifest = read_analysis_manifest(manifest_path)
        except AnalysisError:
            continue
        if manifest.get("manifest_version") != 1 or "analysis_type" not in manifest:
            continue
        manifest_type = str(manifest.get("analysis_type"))
        panel_path = str(manifest.get("panel", {}).get("path", ""))
        panel_name = str(manifest.get("panel", {}).get("name", ""))
        if analysis_type is not None and manifest_type != analysis_type:
            continue
        if panel is not None and panel not in {panel_path, panel_name}:
            continue
        rows.append(
            {
                "manifest_path": str(manifest_path),
                "created_at": manifest.get("created_at"),
                "analysis_type": manifest_type,
                "panel_path": panel_path,
                "panel_name": panel_name,
                "output_path": manifest.get("output", {}).get("path"),
                "parameters": manifest.get("specification", {}).get("parameters", {}),
                "result_summary": manifest.get("result_summary", {}),
            }
        )
    return rows


def describe_panel(
    panel_path: Path,
    *,
    columns: list[str] | None = None,
    output_path: Path | None = None,
) -> AnalysisResult:
    """Summarize numeric panel columns with semantics and missingness."""
    df = _read_panel(panel_path)
    measure_columns = _numeric_columns(df, columns)
    rows: list[dict[str, Any]] = []
    for column in measure_columns:
        series = pd.to_numeric(df[column], errors="coerce")
        non_null = series.dropna()
        row = {
            "column": column,
            "n": int(non_null.shape[0]),
            "missing": int(series.isna().sum()),
            "missing_rate": float(series.isna().mean()) if len(series) else 0.0,
            "mean": float(non_null.mean()) if not non_null.empty else np.nan,
            "std": float(non_null.std(ddof=1)) if len(non_null) > 1 else np.nan,
            "min": float(non_null.min()) if not non_null.empty else np.nan,
            "p25": float(non_null.quantile(0.25)) if not non_null.empty else np.nan,
            "median": float(non_null.median()) if not non_null.empty else np.nan,
            "p75": float(non_null.quantile(0.75)) if not non_null.empty else np.nan,
            "max": float(non_null.max()) if not non_null.empty else np.nan,
        }
        row.update(_measure_semantics(column, panel_columns=df.columns.tolist()))
        rows.append(row)
    table = pd.DataFrame(rows)
    return _persist_result(
        table,
        panel_path=panel_path,
        output_path=output_path,
        analysis_type="describe",
        parameters={"columns": measure_columns},
        metadata={"analysis_type": "describe", "column_count": len(measure_columns)},
    )


def _residualize(values: pd.Series, controls: pd.DataFrame) -> pd.Series:
    design = controls.apply(pd.to_numeric, errors="coerce")
    design = design.assign(__intercept=1.0)
    combined = pd.concat([values.rename("__value"), design], axis=1).dropna()
    if combined.empty:
        return pd.Series(dtype=float)
    y = combined["__value"].to_numpy(dtype=float)
    x = combined.drop(columns=["__value"]).to_numpy(dtype=float)
    beta, *_ = np.linalg.lstsq(x, y, rcond=None)
    resid = y - x @ beta
    return pd.Series(resid, index=combined.index)


def correlate_panel(
    panel_path: Path,
    *,
    columns: list[str],
    partial_controls: list[str] | None = None,
    output_path: Path | None = None,
) -> AnalysisResult:
    """Compute pairwise Pearson and optional partial correlations."""
    df = _read_panel(panel_path)
    controls = partial_controls or []
    _require_columns(df, [*columns, *controls], context="correlate")
    rows: list[dict[str, Any]] = []
    for left_index, left in enumerate(columns):
        for right in columns[left_index + 1 :]:
            pair = df[[left, right, *controls]].apply(pd.to_numeric, errors="coerce").dropna()
            if pair.empty:
                corr = np.nan
                partial_corr = np.nan
            else:
                corr = float(pair[left].corr(pair[right]))
                if controls:
                    left_resid = _residualize(pair[left], pair[controls])
                    right_resid = _residualize(pair[right], pair[controls])
                    aligned = pd.concat([left_resid, right_resid], axis=1).dropna()
                    partial_corr = (
                        float(aligned.iloc[:, 0].corr(aligned.iloc[:, 1]))
                        if len(aligned) > 1
                        else np.nan
                    )
                else:
                    partial_corr = np.nan
            rows.append(
                {
                    "left": left,
                    "right": right,
                    "n": int(len(pair)),
                    "correlation": corr,
                    "partial_correlation": partial_corr,
                    "partial_controls": ",".join(controls),
                }
            )
    table = pd.DataFrame(rows)
    return _persist_result(
        table,
        panel_path=panel_path,
        output_path=output_path,
        analysis_type="correlate",
        parameters={"columns": columns, "partial_controls": controls},
        metadata={
            "analysis_type": "correlate",
            "pair_count": len(rows),
            "partial_controls": controls,
        },
    )


def _fixed_effect_dummies(df: pd.DataFrame, column: str, prefix: str) -> pd.DataFrame:
    return pd.get_dummies(df[column].astype("string"), prefix=prefix, drop_first=True, dtype=float)


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


def lagged_associations_panel(
    panel_path: Path,
    *,
    outcome: str,
    predictors: list[str],
    lags: list[int],
    entity_column: str = "geo_id",
    year_column: str = "year",
    output_path: Path | None = None,
) -> AnalysisResult:
    """Correlate an outcome with lagged predictor values by entity-year."""
    if not lags or any(lag < 1 for lag in lags):
        raise AnalysisError("lagged associations require one or more positive lags.")
    df = _read_panel(panel_path)
    _require_columns(
        df,
        [outcome, *predictors, entity_column, year_column],
        context="lagged",
    )
    work = df[[outcome, *predictors, entity_column, year_column]].copy()
    for column in [outcome, *predictors]:
        work[column] = pd.to_numeric(work[column], errors="coerce")
    work = work.sort_values([entity_column, year_column])

    rows: list[dict[str, Any]] = []
    for predictor in predictors:
        for lag in sorted(set(lags)):
            lagged_column = f"__{predictor}_lag_{lag}"
            work[lagged_column] = work.groupby(entity_column, dropna=False)[predictor].shift(lag)
            pair = work[[outcome, lagged_column]].dropna()
            rows.append(
                {
                    "outcome": outcome,
                    "predictor": predictor,
                    "lag": lag,
                    "n": int(len(pair)),
                    "correlation": (
                        float(pair[outcome].corr(pair[lagged_column])) if len(pair) > 1 else np.nan
                    ),
                }
            )
    table = pd.DataFrame(rows)
    return _persist_result(
        table,
        panel_path=panel_path,
        output_path=output_path,
        analysis_type="lagged",
        parameters={
            "outcome": outcome,
            "predictors": predictors,
            "lags": sorted(set(lags)),
            "entity_column": entity_column,
            "year_column": year_column,
        },
        metadata={
            "analysis_type": "lagged",
            "outcome": outcome,
            "predictors": predictors,
            "lags": sorted(set(lags)),
            "association_count": len(rows),
        },
    )
