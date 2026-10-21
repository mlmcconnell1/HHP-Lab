"""RecipeV2 finding evaluation against exact analysis result keys."""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any

from hhplab.results.bands import evaluate_bands
from hhplab.schema.results import (
    BUNDLE_FORMAT_VERSION,
    FindingRecord,
    FindingsPayload,
    FindingStatus,
    RobustnessStatus,
)


def _record_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if hasattr(value, "to_dict"):
        result = value.to_dict()
    elif hasattr(value, "model_dump"):
        result = value.model_dump(mode="json", by_alias=True, exclude_none=True)
    else:
        raise TypeError("finding plans and references must be mappings or serializable records")
    if not isinstance(result, dict):
        raise TypeError("finding plan record did not serialize to an object")
    return result


def _result_key_set(variant_plans: Any) -> set[str]:
    if isinstance(variant_plans, dict):
        return {str(key) for key in variant_plans}
    keys: set[str] = set()
    for raw in variant_plans or ():
        result_key = _record_dict(raw).get("result_key")
        if isinstance(result_key, str):
            keys.add(result_key)
    return keys


def _finding_references(references: Any, finding_id: str) -> list[dict[str, Any]]:
    return [
        reference
        for raw in references or ()
        if (reference := _record_dict(raw)).get("owner_id") == finding_id
        and reference.get("owner_kind") == "finding"
    ]


def _source_result_key(
    finding_id: str,
    source: Mapping[str, Any],
    *,
    evaluation_variant: str,
    estimand_analysis: str,
    references: list[dict[str, Any]],
    planned_result_keys: set[str],
) -> str:
    analysis_id = source["analysis"]
    target_variant = (
        evaluation_variant
        if evaluation_variant != "base" and analysis_id == estimand_analysis
        else "base"
    )
    candidates = [
        reference
        for reference in references
        if reference.get("analysis_id") == analysis_id
        and reference.get("base_term") == source.get("term")
        and reference.get("stat") == source.get("stat")
        and reference.get("result_key", "").endswith(f"@{target_variant}")
    ]
    if candidates:
        return candidates[0]["result_key"]
    result_key = f"{analysis_id}@{target_variant}"
    if not planned_result_keys or result_key in planned_result_keys:
        return result_key
    base_key = f"{analysis_id}@base"
    if target_variant != "base" and base_key in planned_result_keys:
        return base_key
    raise KeyError(f"finding {finding_id!r} has no planned result for {result_key!r}")


def _finite_or_none(value: Any) -> float | int | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _result_failure(result_key: str, results_by_key: Mapping[str, Any]) -> str | None:
    result = results_by_key.get(result_key)
    if not isinstance(result, Mapping):
        return f"missing_result:{result_key}"
    if result.get("status") != "complete":
        error = result.get("error")
        code = error.get("code") if isinstance(error, Mapping) else None
        return f"analysis_failed:{result_key}:{code or 'unknown'}"
    return None


def _find_estimate(
    result: Mapping[str, Any], term: str
) -> tuple[Mapping[str, Any] | None, str | None]:
    rows = result.get("regression_estimates")
    if not isinstance(rows, list):
        return None, "missing_regression_estimates"
    matches = [
        row for row in rows if isinstance(row, Mapping) and row.get("base_term") == term
    ]
    if len(matches) != 1:
        return None, "missing_coefficient" if not matches else "ambiguous_coefficient"
    return matches[0], None


def _read_stat(result: Mapping[str, Any], stat: str) -> tuple[Any, str | None]:
    fit_rows = result.get("regression_fit")
    if isinstance(fit_rows, Mapping):
        row: Mapping[str, Any] | None = fit_rows
    elif isinstance(fit_rows, list):
        candidates = [item for item in fit_rows if isinstance(item, Mapping)]
        row = candidates[0] if len(candidates) == 1 else None
    else:
        row = None
    if row is not None and stat in row:
        if row.get(f"{stat}_status") == "not_evaluated":
            return None, str(row.get(f"{stat}_reason") or f"not_evaluated:{stat}")
        value = _finite_or_none(row.get(stat))
        return (value, None) if value is not None else (None, f"missing_statistic:{stat}")

    for collection, value_key in (
        (result.get("descriptive_stats"), "value"),
        (result.get("correlations"), "r" if stat == "r" else stat),
    ):
        if not isinstance(collection, list):
            continue
        if collection is result.get("descriptive_stats"):
            matches = [
                item for item in collection
                if isinstance(item, Mapping) and item.get("stat") == stat
            ]
        else:
            matches = [item for item in collection if isinstance(item, Mapping)]
        if len(matches) > 1:
            return None, f"ambiguous_statistic:{stat}"
        if len(matches) == 1:
            item = matches[0]
            if item.get("status") == "not_evaluated":
                return None, str(item.get("reason") or f"not_evaluated:{stat}")
            value = _finite_or_none(item.get(value_key))
            return (value, None) if value is not None else (None, f"missing_statistic:{stat}")
    return None, f"missing_statistic:{stat}"


def _criteria_for_instance(
    declaration: Mapping[str, Any],
    results_by_key: Mapping[str, Any],
    *,
    evaluation_variant: str,
    estimand_analysis: str,
    references: list[dict[str, Any]],
    planned_result_keys: set[str],
) -> tuple[dict[str, Any], str | None]:
    criteria: dict[str, Any] = {}
    failures: list[str] = []
    sources = declaration["criteria_source"]
    estimate_source = sources.get("estimate")
    if isinstance(estimate_source, Mapping):
        criteria.update({"estimate": None, "ci_lo": None, "ci_hi": None})
        try:
            result_key = _source_result_key(
                declaration["id"], estimate_source,
                evaluation_variant=evaluation_variant,
                estimand_analysis=estimand_analysis,
                references=references,
                planned_result_keys=planned_result_keys,
            )
        except KeyError as exc:
            result_key = f"{estimate_source['analysis']}@{evaluation_variant}"
            failures.append(str(exc))
        result = results_by_key.get(result_key)
        failure = _result_failure(result_key, results_by_key)
        if failure:
            failures.append(failure)
        elif isinstance(result, Mapping):
            row, reason = _find_estimate(result, estimate_source["term"])
            if reason:
                failures.append(f"{result_key}:{reason}")
            if row is not None:
                for key in ("estimate", "ci_lo", "ci_hi"):
                    criteria[key] = _finite_or_none(row.get(key))
                    if criteria[key] is None:
                        failures.append(f"{result_key}:undefined_{key}")

    for name, source in sources.get("stats", {}).items():
        if not isinstance(source, Mapping):
            criteria[name] = None
            failures.append(f"invalid_stat_source:{name}")
            continue
        try:
            result_key = _source_result_key(
                declaration["id"], source,
                evaluation_variant=evaluation_variant,
                estimand_analysis=estimand_analysis,
                references=references,
                planned_result_keys=planned_result_keys,
            )
        except KeyError as exc:
            result_key = f"{source['analysis']}@{evaluation_variant}"
            failures.append(str(exc))
        failure = _result_failure(result_key, results_by_key)
        if failure:
            criteria[name] = None
            failures.append(failure)
            continue
        value, reason = _read_stat(results_by_key[result_key], source["stat"])
        criteria[name] = value
        if reason:
            failures.append(f"{result_key}:{reason}")
    return criteria, (failures[0] if failures else None)


def _evaluate_instance(
    declaration: Mapping[str, Any],
    results_by_key: Mapping[str, Any],
    *,
    evaluation_variant: str,
    estimand_analysis: str,
    references: list[dict[str, Any]],
    planned_result_keys: set[str],
    parameters: Mapping[str, Any],
) -> dict[str, Any]:
    criteria, failure_reason = _criteria_for_instance(
        declaration,
        results_by_key,
        evaluation_variant=evaluation_variant,
        estimand_analysis=estimand_analysis,
        references=references,
        planned_result_keys=planned_result_keys,
    )
    return evaluate_bands(
        declaration["bands"],
        criteria,
        reference_values=declaration.get("reference_values", ()),
        otherwise=declaration.get("otherwise", "inconclusive"),
        parameters=parameters,
        failure_reason=failure_reason,
    )


def _robustness_status(
    base: Mapping[str, Any], variants: Mapping[str, Mapping[str, Any]], labels: set[str]
) -> RobustnessStatus:
    if base.get("status") == FindingStatus.NOT_EVALUATED.value:
        return RobustnessStatus.NOT_EVALUATED
    if base.get("status") not in labels:
        return RobustnessStatus.UNINFORMATIVE
    if any(item.get("status") == FindingStatus.NOT_EVALUATED.value for item in variants.values()):
        return RobustnessStatus.INCOMPLETE
    if any(item.get("status") != base.get("status") for item in variants.values()):
        return RobustnessStatus.FRAGILE
    return RobustnessStatus.ROBUST if variants else RobustnessStatus.UNINFORMATIVE


def evaluate_finding(
    declaration: Mapping[str, Any],
    results_by_key: Mapping[str, Mapping[str, Any]],
    *,
    parameters: Mapping[str, Any] | None = None,
    references: Any = (),
    variant_plans: Any = (),
) -> FindingRecord:
    """Evaluate one RecipeV2 finding against exact ``analysis@variant`` results."""
    declaration = _record_dict(declaration)
    if not isinstance(results_by_key, Mapping):
        raise TypeError("results_by_key must map exact result keys to result payloads")
    sources = declaration.get("criteria_source", {})
    estimate = sources.get("estimate") if isinstance(sources, Mapping) else None
    source_specs = [estimate] if isinstance(estimate, Mapping) else []
    stats = sources.get("stats", {}) if isinstance(sources, Mapping) else {}
    if isinstance(stats, Mapping):
        source_specs.extend(item for item in stats.values() if isinstance(item, Mapping))
    estimand = estimate.get("analysis") if isinstance(estimate, Mapping) else (
        source_specs[0].get("analysis") if source_specs else ""
    )
    planned_keys = _result_key_set(variant_plans)
    finding_refs = _finding_references(references, declaration["id"])
    resolved_parameters = {} if parameters is None else parameters
    base = _evaluate_instance(
        declaration, results_by_key,
        evaluation_variant="base", estimand_analysis=estimand,
        references=finding_refs, planned_result_keys=planned_keys,
        parameters=resolved_parameters,
    )
    variants: dict[str, Mapping[str, Any]] = {}
    robustness = declaration.get("robustness")
    if isinstance(robustness, Mapping):
        for variant_id in robustness.get("variants", ()):
            variants[variant_id] = _evaluate_instance(
                declaration, results_by_key,
                evaluation_variant=variant_id, estimand_analysis=estimand,
                references=finding_refs, planned_result_keys=planned_keys,
                parameters=resolved_parameters,
            )
    labels = {
        band["label"] for band in declaration.get("bands", ())
        if isinstance(band, Mapping)
    }
    return FindingRecord(
        id=declaration["id"],
        base=base,
        variants=variants,
        robustness_status=_robustness_status(base, variants, labels),
    )


def evaluate_findings(
    declarations: Any,
    results_by_key: Mapping[str, Mapping[str, Any]],
    *,
    parameters: Mapping[str, Any] | None = None,
    references: Any = (),
    variant_plans: Any = (),
) -> FindingsPayload:
    """Evaluate declarations into the versioned results bundle contract."""
    records = tuple(
        evaluate_finding(
            declaration, results_by_key, parameters=parameters,
            references=references, variant_plans=variant_plans,
        )
        for declaration in declarations
    )
    return FindingsPayload(BUNDLE_FORMAT_VERSION, records)
