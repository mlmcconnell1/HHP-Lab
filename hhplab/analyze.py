"""Agent-facing analysis helpers for panel Parquet artifacts."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from statistics import NormalDist
from typing import Any

import numpy as np
import pandas as pd

from hhplab.provenance import ProvenanceBlock, read_provenance, write_parquet_with_provenance
from hhplab.schema.measures import resolve_panel_measure_entry


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


def _default_output_path(panel_path: Path, analysis_type: str) -> Path:
    return panel_path.with_name(f"{panel_path.stem}__analysis_{analysis_type}.parquet")


def _manifest_path_for_output(output_path: Path) -> Path:
    return output_path.with_suffix(".manifest.json")


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
    manifest_path = _manifest_path_for_output(output_path)
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
    resolved_output = output_path or _default_output_path(panel_path, analysis_type)
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


def _clustered_standard_errors(
    x: np.ndarray,
    residuals: np.ndarray,
    clusters: pd.Series,
) -> np.ndarray:
    xtx_inv = np.linalg.pinv(x.T @ x)
    meat = np.zeros((x.shape[1], x.shape[1]))
    for cluster in clusters.dropna().unique():
        mask = clusters == cluster
        xg = x[mask.to_numpy()]
        eg = residuals[mask.to_numpy()]
        score = xg.T @ eg
        meat += np.outer(score, score)
    variance = xtx_inv @ meat @ xtx_inv
    return np.sqrt(np.clip(np.diag(variance), 0, None))


def _two_sided_p_value(t_stat: float, dof: int) -> float:
    if not math.isfinite(t_stat):
        return np.nan
    try:
        from scipy import stats  # type: ignore[import-not-found]

        return float(2.0 * stats.t.sf(abs(t_stat), dof))
    except Exception:
        return float(2.0 * (1.0 - NormalDist().cdf(abs(t_stat))))


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
        if _is_binary_indicator(values):
            metadata[column] = {
                "standardized": False,
                "mean": float(values.mean()),
                "std": float(values.std(ddof=0)),
                "note": "binary_indicator_not_standardized",
            }
            continue
        mean = float(values.mean())
        std = float(values.std(ddof=0))
        if not math.isfinite(std) or std <= 0:
            raise AnalysisError(
                f"Cannot standardize column '{column}' because its model-sample "
                "standard deviation is zero or undefined."
            )
        model_df[column] = (values - mean) / std
        metadata[column] = {
            "standardized": True,
            "mean": mean,
            "std": std,
            "note": "",
        }
    return metadata


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
    output_path: Path | None = None,
) -> AnalysisResult:
    """Run OLS with optional entity/year fixed effects and clustered standard errors."""
    if standardize not in {"none", "predictors", "all"}:
        raise AnalysisError("--standardize must be one of: none, predictors, all.")
    df = _read_panel(panel_path)
    needed = [outcome, *predictors]
    if entity_fe:
        needed.append(entity_column)
    if year_fe:
        needed.append(year_column)
    if cluster_by is not None:
        needed.append(cluster_by)
    _require_columns(df, list(dict.fromkeys(needed)), context="regress")

    model_df = df[list(dict.fromkeys(needed))].copy()
    numeric_cols = [outcome, *predictors]
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
    beta, *_ = np.linalg.lstsq(x, y, rcond=None)
    fitted = x @ beta
    residuals = y - fitted
    sigma2 = float((residuals @ residuals) / dof)
    naive_se = np.sqrt(np.clip(np.diag(np.linalg.pinv(x.T @ x)) * sigma2, 0, None))
    if cluster_by is not None:
        std_errors = _clustered_standard_errors(x, residuals, model_df[cluster_by])
        std_error_type = f"clustered:{cluster_by}"
    else:
        std_errors = naive_se
        std_error_type = "ols"
    coef = pd.DataFrame(
        {
            "term": design.columns,
            "estimate": beta,
            "std_error": std_errors,
        }
    )
    coef["t_stat"] = coef["estimate"] / coef["std_error"].replace(0, np.nan)
    coef["p_value"] = [
        _two_sided_p_value(float(t_stat), dof) for t_stat in coef["t_stat"].tolist()
    ]
    coef["outcome"] = outcome
    coef["n"] = int(len(y))
    coef["design_rank"] = rank
    coef["dof"] = dof
    coef["r_squared"] = float(1 - (residuals @ residuals) / np.sum((y - y.mean()) ** 2))
    coef["std_error_type"] = std_error_type
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
        },
        metadata={
            "analysis_type": "regress",
            "outcome": outcome,
            "predictors": predictors,
            "n": int(len(y)),
            "design_rank": rank,
            "dof": dof,
            "r_squared": float(coef["r_squared"].iloc[0]),
            "std_error_type": std_error_type,
            "standardize": standardize,
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
                        float(pair[outcome].corr(pair[lagged_column]))
                        if len(pair) > 1
                        else np.nan
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
