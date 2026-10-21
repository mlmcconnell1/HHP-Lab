"""RecipeV2 finding evaluation and robustness precedence tests."""

from __future__ import annotations

import math

import pytest

from hhplab.results.findings import finding_from_result
from hhplab.results.recipe_findings import evaluate_finding, evaluate_findings
from hhplab.schema.results import FindingRecord, FindingsPayload, RobustnessStatus

BASE_ESTIMATE = {
    "analysis": "model",
    "variant": "base",
    "term": "x_eff",
    "base_term": "x",
    "estimate": 0.8,
    "ci_lo": 0.6,
    "ci_hi": 1.0,
}
VARIANT_ESTIMATE = {
    **BASE_ESTIMATE,
    "variant": "robust",
    "estimate": 0.75,
    "ci_lo": 0.55,
    "ci_hi": 0.95,
}
FINDING = {
    "id": "effect",
    "claim": "The treatment raises the outcome.",
    "criteria_source": {"estimate": {"analysis": "model", "term": "x"}},
    "reference_values": [0, 0.5],
    "bands": [
        {"label": "positive", "ci_above": {"value": 0}},
        {"label": "null", "equivalent_to": {"value": 0, "margin": 0.1}},
    ],
    "otherwise": "inconclusive",
    "robustness": {"variants": ["robust"]},
    "limitations": ["Synthetic test declaration"],
}


def _result(estimate: dict[str, object] | None = BASE_ESTIMATE) -> dict[str, object]:
    if estimate is None:
        return {"status": "failed", "error": {"code": "readiness_error", "message": "failed"}}
    return {"status": "complete", "regression_estimates": [estimate]}


def test_finding_evaluates_base_and_variant_by_exact_result_key() -> None:
    references = [
        {
            "owner_id": "effect",
            "owner_kind": "finding",
            "analysis_id": "model",
            "result_key": "model@base",
            "base_term": "x",
            "term": "x_eff",
            "stat": None,
        },
        {
            "owner_id": "effect",
            "owner_kind": "finding",
            "analysis_id": "model",
            "result_key": "model@robust",
            "base_term": "x",
            "term": "x_eff",
            "stat": None,
        },
    ]
    results = {"model@base": _result(), "model@robust": _result(VARIANT_ESTIMATE)}

    record = evaluate_finding(
        FINDING,
        results,
        references=references,
        variant_plans=[{"result_key": "model@base"}, {"result_key": "model@robust"}],
    )

    assert isinstance(record, FindingRecord)
    assert record.base["status"] == "positive"
    assert record.base["criteria"]["ci_lo"] == {"value": 0.6, "reason": None}
    assert record.base["compatible_with"] == []
    assert record.variants["robust"]["status"] == "positive"
    assert record.robustness_status is RobustnessStatus.ROBUST


def test_findings_payload_is_versioned_and_preserves_independent_instances() -> None:
    payload = evaluate_findings(
        [FINDING],
        {"model@base": _result(), "model@robust": _result(VARIANT_ESTIMATE)},
        variant_plans=[{"result_key": "model@base"}, {"result_key": "model@robust"}],
    )

    assert isinstance(payload, FindingsPayload)
    data = payload.to_dict()
    assert data["bundle_format_version"] == "1.0"
    assert data["findings"][0]["base"]["label"] == "positive"
    assert data["findings"][0]["variants"]["robust"]["label"] == "positive"


@pytest.mark.parametrize(
    ("base", "variant", "expected"),
    [
        pytest.param(None, VARIANT_ESTIMATE, "not_evaluated", id="failed-base-wins"),
        pytest.param(BASE_ESTIMATE, None, "incomplete", id="failed-variant"),
        pytest.param(
            BASE_ESTIMATE, {**VARIANT_ESTIMATE, "ci_lo": -0.2}, "fragile", id="different-label"
        ),
    ],
)
def test_robustness_precedence(
    base: dict[str, object] | None,
    variant: dict[str, object] | None,
    expected: str,
) -> None:
    record = evaluate_finding(
        FINDING,
        {"model@base": _result(base), "model@robust": _result(variant)},
    )

    assert record.robustness_status.value == expected


def test_undecisive_base_is_uninformative_even_when_variant_is_decisive() -> None:
    base = {**BASE_ESTIMATE, "ci_lo": -0.2, "ci_hi": 0.2}
    record = evaluate_finding(
        FINDING,
        {"model@base": _result(base), "model@robust": _result(VARIANT_ESTIMATE)},
    )

    assert record.base["status"] == "inconclusive"
    assert record.robustness_status is RobustnessStatus.UNINFORMATIVE


def test_overlapping_runtime_bands_are_reported_ambiguous_rule() -> None:
    declaration = {
        **FINDING,
        "bands": [
            {"label": "first", "ci_above": {"value": 0}},
            {"label": "second", "ci_above": {"value": -1}},
        ],
        "robustness": None,
    }

    record = evaluate_finding(declaration, {"model@base": _result()})

    assert record.base["status"] == "ambiguous_rule"
    assert record.base["label"] is None
    assert record.robustness_status is RobustnessStatus.UNINFORMATIVE


def test_failed_result_has_null_sanitized_criteria_and_reason() -> None:
    record = evaluate_finding(FINDING, {"model@base": _result(None)})

    assert record.base["status"] == "not_evaluated"
    assert record.base["label"] is None
    assert "analysis_failed:model@base" in record.base["reason"]
    assert record.base["criteria"]["ci_lo"] == {
        "value": None,
        "reason": "missing_criterion:ci_lo",
    }
    assert record.robustness_status is RobustnessStatus.NOT_EVALUATED


def test_estimate_resolution_uses_base_term_and_rejects_ambiguous_matches() -> None:
    wrong_term = {**BASE_ESTIMATE, "base_term": "other"}
    missing = evaluate_finding(FINDING, {"model@base": _result(wrong_term)})
    ambiguous = evaluate_finding(
        FINDING,
        {"model@base": {"status": "complete", "regression_estimates": [BASE_ESTIMATE] * 2}},
    )

    assert missing.base["status"] == "not_evaluated"
    assert "missing_coefficient" in missing.base["reason"]
    assert ambiguous.base["status"] == "not_evaluated"
    assert "ambiguous_coefficient" in ambiguous.base["reason"]


def test_fit_statistics_are_resolved_by_declared_stat_and_nonfinite_values_are_null() -> None:
    declaration = {
        "id": "fit",
        "claim": "Fit is high.",
        "criteria_source": {"stats": {"within": {"analysis": "model", "stat": "r2_within"}}},
        "bands": [{"label": "high", "stat_gte": {"stat": "within", "value": 0.5}}],
        "otherwise": "inconclusive",
    }
    result = {
        "status": "complete",
        "regression_fit": [{"r2_within": math.nan}],
    }

    record = evaluate_finding(declaration, {"model@base": result})

    assert record.base["status"] == "not_evaluated"
    assert record.base["criteria"]["within"] == {
        "value": None,
        "reason": "missing_criterion:within",
    }


def test_legacy_covariate_finding_factory_remains_available() -> None:
    result = {
        "regressions": [
            {
                "family": "legacy",
                "model": "old_model",
                "term": "covariate_x",
                "estimate": 1.0,
                "p_value": 0.01,
            }
        ]
    }

    finding = finding_from_result(
        workflow_id="legacy-workflow",
        module_name="build_irs_migration_pooled_panel",
        result=result,
    )

    assert finding is not None
    assert finding.key_terms[0].term == "covariate_x"
