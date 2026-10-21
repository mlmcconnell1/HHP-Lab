"""Closed dispatch from planned analyses to row-preserving numerical APIs."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from hhplab.analysis.contracts import AnalysisExecutionResult, ResolvedSampleFrame
from hhplab.analysis.correlation import correlate_resolved_sample
from hhplab.analysis.inspection import describe_resolved_sample
from hhplab.analysis.regression import regress_resolved_frame
from hhplab.schema.results import AnalysisStatus, validate_result_rows

SUPPORTED_METHODS = frozenset({"regression", "descriptive", "correlation"})


def execute_analysis(plan: Mapping[str, Any], sample: ResolvedSampleFrame) -> dict[str, Any]:
    """Execute one result key; a numerical failure cannot suppress other results.

    The caller owns lifecycle, samples, lineage, reports and persistence.
    Dispatch never filters rows or performs estimation itself.
    """
    identity = {
        "analysis": plan["analysis_id"],
        "variant": plan["variant_id"],
        "result_key": plan["result_key"],
        "sample_source": plan["sample_source"],
    }
    try:
        _check_sample(sample)
        spec = plan["effective_spec"]
        method = spec["method"]
        if method not in SUPPORTED_METHODS:
            raise ValueError(f"capability_unavailable: Phase 1 does not support {method!r}")
        if method == "regression":
            inference = spec.get("inference", {})
            if (
                spec.get("estimator", "ols") != "ols"
                or inference.get("method") == "wild_cluster_bootstrap"
            ):
                raise ValueError(
                    "capability_unavailable: Phase 1 supports OLS with classical, HC1 or CR1"
                )
            result = regress_resolved_frame(
                sample,
                outcome=spec["outcome"],
                predictors=spec["predictors"],
                controls=spec.get("controls", ()),
                fixed_effects=spec.get("fixed_effects", ()),
                weights=spec.get("weights"),
                inference_method=inference.get("method", "classical"),
                cluster_by=inference.get("cluster_by"),
                ci_level=spec.get("ci_level", 0.95),
                analysis=identity["analysis"],
                variant=identity["variant"],
                sample_group=plan["sample_source"],
                base_term_by_effective_term={
                    term: plan["base_term_by_effective_term"][term]
                    for term in (*spec["predictors"], *spec.get("controls", ()))
                },
            )
            result.execution.validate_sample(sample)
            payload = result.to_payload()
        else:
            common = {
                "analysis_id": identity["analysis"],
                "variant_id": identity["variant"],
                "columns": spec["columns"],
                "weights": spec.get("weights"),
            }
            if method == "descriptive":
                table = describe_resolved_sample(
                    sample,
                    **common,
                    stats=spec["stats"],
                    by=spec.get("by", ()),
                )
                table_name = "descriptive_stats"
            else:
                table = correlate_resolved_sample(
                    sample, **common, kind=spec.get("kind", "pearson")
                )
                table_name = "correlations"
            if "row_keys" not in table.attrs:
                raise ValueError("sample_contract_violation: analysis omitted consumed row keys")
            try:
                AnalysisExecutionResult(
                    AnalysisStatus.COMPLETE, tuple(table.attrs["row_keys"])
                ).validate_sample(sample)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"sample_contract_violation: {exc}") from exc
            rows = table.to_dict(orient="records")
            validate_result_rows(table_name, rows)
            payload = {"status": "complete", table_name: rows, "warnings": []}
        _check_sample(sample)
        return {**identity, **payload}
    except Exception as exc:
        message = str(exc)
        code = getattr(exc, "code", None)
        if code is None:
            code = (
                "sample_contract_violation"
                if "sample_contract_violation" in message
                else (
                    "capability_unavailable"
                    if "capability_unavailable" in message
                    else "analysis_failed"
                )
            )
        return {**identity, "status": "failed", "error": {"code": code, "message": message}}


def _check_sample(sample: ResolvedSampleFrame) -> None:
    try:
        sample.validate_frame_identity()
    except (TypeError, ValueError) as exc:
        raise ValueError(f"sample_contract_violation: {exc}") from exc
