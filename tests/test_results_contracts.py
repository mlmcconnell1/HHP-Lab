"""Contract tests for versioned recipe-results artifacts and analysis frames.

The payload fixtures below are synthetic contract values, not research
thresholds. They cover valid and invalid manifest, result, diagnostic,
finding, ledger-event, redistribution, and resolved-sample shapes.
"""

from __future__ import annotations

import ast
import math
from pathlib import Path

import pandas as pd
import pytest

from hhplab.analysis.contracts import (
    AnalysisErrorRecord,
    AnalysisExecutionResult,
    ResolvedSampleFrame,
)
from hhplab.schema.redistribution import (
    ArtifactScope,
    RedistributionStatus,
    SourceRedistribution,
    artifact_disposition,
    combine_source_terms,
)
from hhplab.schema.results import (
    BUNDLE_FORMAT_VERSION,
    AnalysisStatus,
    DiagnosticRecord,
    FindingRecord,
    ResultContractError,
    ResultsManifest,
    canonical_json,
    component_hashes,
    validate_ledger_event,
    validate_reserved_ids,
    validate_result_rows,
)

SHA256_A = "a" * 64
SHA256_B = "b" * 64
UTC_TIMESTAMP = "2026-10-20T12:00:00Z"
BASE_ROW_KEYS = (("01001", 2020), ("01001", 2021))
BASE_PANEL_ROWS = (
    {"geo_id": "01001", "year": 2020, "value": 4.0},
    {"geo_id": "01001", "year": 2021, "value": 5.0},
)


def manifest_payload(
    *,
    execution_status: str = "complete",
    diagnostics_status: str = "pass",
    findings_status: str = "all_evaluated",
    distribution_status: str = "full",
    private: bool = False,
) -> dict[str, object]:
    """Return a minimal valid manifest with named synthetic identity values."""
    return {
        "bundle_format_version": BUNDLE_FORMAT_VERSION,
        "recipe_id": "synthetic-recipe",
        "run_id": "synthetic-run",
        "created_at": UTC_TIMESTAMP,
        "execution_status": execution_status,
        "diagnostics_status": diagnostics_status,
        "findings_status": findings_status,
        "distribution_status": distribution_status,
        "analysis_statuses": {
            "fit@base": {
                "status": (
                    "pending"
                    if execution_status == "failed"
                    else "skipped"
                    if execution_status == "partial"
                    else "complete"
                ),
                "errors": [],
            },
        },
        "spec_hash": SHA256_A,
        "component_hashes": {"analyses": SHA256_B},
        "lock_status": "unlocked",
        "input_identities": [
            {
                "panel": "synthetic-panel",
                "build_spec_hash": SHA256_A,
                "declared_vintages": {"pit": "2024"},
                "content_sha256": SHA256_B,
            },
        ],
        "private": private,
        "files": [],
    }


RESULT_TABLE_CASES = (
    (
        "regression_estimates",
        {
            "analysis": "fit",
            "variant": "base",
            "term": "beds",
            "base_term": "beds",
            "role": "predictor",
            "estimate": 0.5,
            "se": 0.1,
            "df": 9.0,
            "ci_lo": 0.2,
            "ci_hi": 0.8,
            "ci_level": 0.95,
            "ci_method": "wald",
            "p": 0.01,
            "p_status": "evaluated",
            "p_reason": None,
            "inference_method": "classical",
        },
    ),
    (
        "regression_fit",
        {
            "analysis": "fit",
            "variant": "base",
            "sample_source": "fd_main@base",
            "n": 12,
            "n_clusters": 4,
            "design_rank": 3,
            "k_dof": 3,
            "df_resid": 9,
            "r2": 0.4,
            "r2_status": "evaluated",
            "r2_reason": None,
            "r2_within": 0.2,
            "r2_within_status": "evaluated",
            "r2_within_reason": None,
            "absorbed": ["year"],
        },
    ),
    (
        "descriptive_stats",
        {
            "analysis": "summary",
            "variant": "base",
            "column": "value",
            "group": "{}",
            "stat": "mean",
            "value": 4.5,
            "status": "evaluated",
            "reason": None,
        },
    ),
    (
        "correlations",
        {
            "analysis": "correlation",
            "variant": "base",
            "col_a": "x",
            "col_b": "y",
            "kind": "pearson",
            "r": 0.0,
            "n": 12,
            "status": "evaluated",
            "reason": None,
        },
    ),
)


@pytest.mark.parametrize(("table_name", "record"), RESULT_TABLE_CASES)
def test_result_table_accepts_canonical_success_rows(table_name, record):
    validate_result_rows(table_name, [record])


@pytest.mark.parametrize(
    ("table_name", "record"),
    [
        (
            "regression_fit",
            {
                "analysis": "constant-outcome",
                "variant": "base",
                "sample_source": "base",
                "n": 8,
                "n_clusters": 4,
                "design_rank": 2,
                "k_dof": 2,
                "df_resid": 6,
                "r2": None,
                "r2_status": "not_evaluated",
                "r2_reason": "zero_total_sum_squares",
                "r2_within": None,
                "r2_within_status": "not_evaluated",
                "r2_within_reason": "zero_within_sum_squares",
                "absorbed": [],
            },
        ),
        (
            "descriptive_stats",
            {
                "analysis": "empty-group",
                "variant": "base",
                "column": "value",
                "group": "{}",
                "stat": "mean",
                "value": None,
                "status": "not_evaluated",
                "reason": "empty_sample",
            },
        ),
        (
            "correlations",
            {
                "analysis": "constant-correlation",
                "variant": "base",
                "col_a": "x",
                "col_b": "y",
                "kind": "pearson",
                "r": None,
                "n": 5,
                "status": "not_evaluated",
                "reason": "constant_input",
            },
        ),
    ],
)
def test_undefined_result_values_are_null_with_reasons(table_name, record):
    validate_result_rows(table_name, [record])


def test_undefined_zero_over_zero_p_value_is_explicit():
    record = dict(dict(RESULT_TABLE_CASES)["regression_estimates"])
    record.update(
        {
            "estimate": 0.0,
            "se": 0.0,
            "p": None,
            "p_status": "not_evaluated",
            "p_reason": "zero_estimate_zero_se",
        }
    )

    validate_result_rows("regression_estimates", [record])


@pytest.mark.parametrize("table_name", [name for name, _ in RESULT_TABLE_CASES])
def test_empty_result_tables_are_valid(table_name):
    validate_result_rows(table_name, [])


@pytest.mark.parametrize(
    ("table_name", "record_update", "reason"),
    [
        ("regression_estimates", {"estimate": math.nan}, "nonfinite"),
        ("correlations", {"r": math.inf}, "nonfinite"),
        (
            "descriptive_stats",
            {"value": None, "status": "evaluated", "reason": None},
            "null",
        ),
    ],
)
def test_result_rows_reject_nonfinite_and_inconsistent_nulls(table_name, record_update, reason):
    base_record = dict(dict(RESULT_TABLE_CASES)[table_name])
    record = base_record | record_update

    with pytest.raises(ResultContractError, match=reason):
        validate_result_rows(table_name, [record])


@pytest.mark.parametrize(
    ("table_name", "record_update", "error_pattern"),
    [
        ("regression_estimates", {"role": "fixed_effect"}, "role"),
        ("regression_estimates", {"inference_method": "made_up"}, "inference_method"),
        ("regression_estimates", {"se": -0.1}, "nonnegative"),
        ("regression_estimates", {"ci_level": 1.0}, "ci_level"),
        ("regression_estimates", {"p": 1.1}, "p must"),
        ("regression_estimates", {"ci_lo": 1.0, "ci_hi": 0.0}, "ci_lo"),
        ("correlations", {"kind": "kendall"}, "kind"),
    ],
)
def test_result_rows_reject_invalid_vocab_and_numeric_bounds(
    table_name, record_update, error_pattern
):
    base_record = dict(dict(RESULT_TABLE_CASES)[table_name])
    record = base_record | record_update

    with pytest.raises(ResultContractError, match=error_pattern):
        validate_result_rows(table_name, [record])


@pytest.mark.parametrize(
    ("statuses", "expected"),
    [
        pytest.param(["pass"], "pass", id="all-pass"),
        pytest.param(["not_applicable"], "not_applicable", id="all-not-applicable"),
        pytest.param(["pass", "not_applicable"], "pass", id="pass-with-not-applicable"),
        pytest.param(["not_evaluated", "not_applicable"], "incomplete", id="incomplete"),
        pytest.param(["fail", "not_evaluated"], "fail", id="failure-takes-precedence"),
        pytest.param([], "none", id="no-diagnostics"),
    ],
)
def test_diagnostics_status_precedence(statuses, expected):
    from hhplab.schema.results import aggregate_diagnostics_status

    assert aggregate_diagnostics_status(statuses) == expected


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param(manifest_payload(), id="complete-success"),
        pytest.param(
            manifest_payload(
                execution_status="partial",
                diagnostics_status="incomplete",
                findings_status="some_not_evaluated",
            ),
            id="partial-run",
        ),
        pytest.param(
            manifest_payload(
                execution_status="failed",
                diagnostics_status="none",
                findings_status="all_evaluated",
            ),
            id="fatal-startup",
        ),
        pytest.param(
            manifest_payload(findings_status="ambiguous"),
            id="ambiguous-finding",
        ),
        pytest.param(
            manifest_payload(distribution_status="withheld"),
            id="withheld-artifacts",
        ),
        pytest.param(
            manifest_payload(distribution_status="private", private=True),
            id="private-bundle",
        ),
    ],
)
def test_manifest_round_trip_for_run_outcomes(payload):
    manifest = ResultsManifest.from_dict(payload)
    assert ResultsManifest.from_dict(manifest.to_dict()) == manifest


def test_manifest_tracks_pending_and_skipped_analysis_statuses():
    payload = manifest_payload(execution_status="partial")
    payload["analysis_statuses"] = {
        "fit@base": {"status": "complete", "errors": []},
        "fit@odd-years": {"status": "skipped", "errors": []},
        "summary@base": {"status": "pending", "errors": []},
    }

    manifest = ResultsManifest.from_dict(payload)

    assert manifest.analysis_statuses["fit@odd-years"].status is AnalysisStatus.SKIPPED
    assert manifest.analysis_statuses["summary@base"].status is AnalysisStatus.PENDING


def test_complete_manifest_rejects_pending_analysis_status():
    payload = manifest_payload()
    payload["analysis_statuses"] = {
        "fit@base": {"status": "pending", "errors": []},
    }

    with pytest.raises(ResultContractError, match="requires every analysis"):
        ResultsManifest.from_dict(payload)


@pytest.mark.parametrize(
    "file_records",
    [
        pytest.param(
            [{"path": "manifest.json", "sha256": SHA256_A, "bytes": 1, "role": "manifest"}],
            id="manifest-self-reference",
        ),
        pytest.param(
            [{"path": "manifest.sha256", "sha256": SHA256_A, "bytes": 1, "role": "manifest-hash"}],
            id="manifest-hash-self-reference",
        ),
        pytest.param(
            [{"path": "../outside.json", "sha256": SHA256_A, "bytes": 1, "role": "external"}],
            id="parent-traversal",
        ),
        pytest.param(
            [
                {"path": "results/a.json", "sha256": SHA256_A, "bytes": 1, "role": "result"},
                {"path": "results/a.json", "sha256": SHA256_B, "bytes": 2, "role": "result"},
            ],
            id="duplicate-path",
        ),
    ],
)
def test_manifest_rejects_self_inventory_unsafe_and_duplicate_paths(file_records):
    payload = manifest_payload()
    payload["files"] = file_records

    with pytest.raises(ResultContractError):
        ResultsManifest.from_dict(payload)


@pytest.mark.parametrize(
    ("payload_update", "error_pattern"),
    [
        ({"bundle_format_version": "99"}, "version"),
        ({"private": True, "distribution_status": "withheld"}, "private"),
        ({"execution_status": "succeeded"}, "execution_status"),
        ({"spec_hash": "not-a-sha256"}, "spec_hash"),
    ],
)
def test_manifest_rejects_invalid_version_status_and_identity(payload_update, error_pattern):
    payload = manifest_payload() | payload_update
    with pytest.raises(ResultContractError, match=error_pattern):
        ResultsManifest.from_dict(payload)


def test_diagnostic_round_trip_preserves_not_applicable_status():
    record = DiagnosticRecord.from_dict(
        {
            "id": "identity",
            "variant": "hc1",
            "status": "not_applicable",
            "value": None,
            "tolerance": None,
            "reason": "not_all_analyses_included",
            "details": {},
        }
    )

    assert DiagnosticRecord.from_dict(record.to_dict()) == record


@pytest.mark.parametrize(
    ("payload", "reason"),
    [
        (
            {
                "id": "identity",
                "variant": "base",
                "status": "not_evaluated",
                "value": math.nan,
                "tolerance": 1e-9,
                "reason": "nonfinite_input",
                "details": {},
            },
            "nonfinite",
        ),
        (
            {
                "id": "identity",
                "variant": "base",
                "status": "pass",
                "value": None,
                "tolerance": 1e-9,
                "reason": None,
                "details": {},
            },
            "null",
        ),
    ],
)
def test_diagnostic_rejects_invalid_nonfinite_or_null_contract(payload, reason):
    with pytest.raises(ResultContractError, match=reason):
        DiagnosticRecord.from_dict(payload)


def test_finding_round_trip_carries_ambiguous_base_and_variant_results():
    finding = FindingRecord.from_dict(
        {
            "id": "synthetic-finding",
            "base": {
                "status": "ambiguous_rule",
                "label": None,
                "compatible_with": [0],
                "reason": "multiple_bands_match",
                "criteria": {"ci_lo": {"value": -1.0, "reason": None}},
            },
            "variants": {
                "hc1": {
                    "status": "not_evaluated",
                    "label": None,
                    "compatible_with": [],
                    "reason": "analysis_failed",
                    "criteria": {"ci_lo": {"value": None, "reason": "analysis_failed"}},
                },
            },
            "robustness_status": "not_evaluated",
        }
    )

    assert FindingRecord.from_dict(finding.to_dict()) == finding


@pytest.mark.parametrize(
    "criterion",
    [
        pytest.param({"value": None, "reason": None}, id="null-without-reason"),
        pytest.param({"value": math.inf, "reason": None}, id="infinite-value"),
        pytest.param({"value": 1.0, "reason": "stale-reason"}, id="value-with-reason"),
    ],
)
def test_finding_rejects_invalid_criterion_contract(criterion):
    evaluation = {
        "status": "inconclusive",
        "label": "inconclusive",
        "compatible_with": [],
        "reason": None,
        "criteria": {"fit": criterion},
    }
    with pytest.raises(ResultContractError, match="criteria"):
        FindingRecord.from_dict(
            {
                "id": "synthetic-finding",
                "base": evaluation,
                "variants": {},
                "robustness_status": "uninformative",
            }
        )


LEDGER_EVENT_CASES = (
    (
        "run_started",
        {
            "run_id": "synthetic-run",
            "recipe_id": "synthetic-recipe",
            "recipe_sha256": SHA256_A,
            "spec_hash": SHA256_B,
            "component_hashes": {"analyses": SHA256_A},
            "lock_status": "unlocked",
            "lock_id": None,
            "host": "test-host",
            "pid": 123,
            "process_start_time": "synthetic-start",
            "environment": {},
        },
    ),
    (
        "run_finished",
        {
            "run_id": "synthetic-run",
            "execution_status": "complete",
            "diagnostics_status": "pass",
            "findings_status": "all_evaluated",
            "distribution_status": "full",
            "bundle_path": "results/synthetic/run",
            "manifest_sha256": SHA256_A,
        },
    ),
    (
        "run_failed",
        {
            "run_id": "synthetic-run",
            "failure_scope": "run",
            "error_class": "OSError",
            "message": "synthetic startup failure",
            "failure_bundle_path": None,
        },
    ),
    (
        "lock_created",
        {
            "lock_id": "synthetic-lock",
            "recipe_id": "synthetic-recipe",
            "recipe_sha256": SHA256_A,
            "spec_hash": SHA256_B,
            "component_hashes": {"analyses": SHA256_A},
            "export_path": "locks/synthetic.lock.json",
        },
    ),
    (
        "ledger_repair",
        {
            "byte_offset": 12,
            "byte_length": 24,
            "ledger_path": "results/run-ledger.jsonl",
        },
    ),
)


@pytest.mark.parametrize(("event", "payload"), LEDGER_EVENT_CASES)
def test_ledger_event_round_trip(event, payload):
    envelope = {
        "event": event,
        "schema_version": "1.0",
        "ts": UTC_TIMESTAMP,
        **payload,
    }
    assert validate_ledger_event(validate_ledger_event(envelope)) == envelope


def test_ledger_event_requires_terminal_status_fields_and_utc_timestamp():
    envelope = {
        "event": "run_finished",
        "schema_version": "1.0",
        "ts": UTC_TIMESTAMP,
        "run_id": "synthetic-run",
    }

    with pytest.raises(ResultContractError, match="diagnostics_status"):
        validate_ledger_event(envelope)


def test_started_event_requires_identity_and_lock_contract():
    payload = dict(LEDGER_EVENT_CASES[0][1])
    payload["recipe_id"] = None
    event = {
        "event": "run_started",
        "schema_version": "1.0",
        "ts": UTC_TIMESTAMP,
        **payload,
    }

    with pytest.raises(ResultContractError, match="recipe_id"):
        validate_ledger_event(event)


@pytest.mark.parametrize(
    ("field", "invalid_value", "error_pattern"),
    [
        pytest.param("lock_status", "invented", "lock status", id="invalid-lock-status"),
        pytest.param("process_start_time", None, "process_start_time", id="missing-start-time"),
        pytest.param("pid", True, "pid", id="boolean-pid"),
    ],
)
def test_started_event_rejects_malformed_process_metadata(field, invalid_value, error_pattern):
    payload = dict(LEDGER_EVENT_CASES[0][1])
    payload[field] = invalid_value
    event = {
        "event": "run_started",
        "schema_version": "1.0",
        "ts": UTC_TIMESTAMP,
        **payload,
    }

    with pytest.raises(ResultContractError, match=error_pattern):
        validate_ledger_event(event)


def test_finished_event_rejects_failed_status_and_missing_terminal_identity():
    payload = dict(LEDGER_EVENT_CASES[1][1])
    payload["execution_status"] = "failed"
    event = {
        "event": "run_finished",
        "schema_version": "1.0",
        "ts": UTC_TIMESTAMP,
        **payload,
    }

    with pytest.raises(ResultContractError, match="run_id|failed execution"):
        validate_ledger_event(event)


def test_canonical_json_is_stable_for_mapping_insertion_order():
    left = {"b": 2, "a": {"y": 4, "x": 3}}
    right = {"a": {"x": 3, "y": 4}, "b": 2}

    assert canonical_json(left) == canonical_json(right)


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf])
def test_canonical_json_rejects_nonfinite_numbers(value):
    with pytest.raises(ResultContractError, match="nonfinite"):
        canonical_json({"value": value})


def test_component_hashes_are_stable_and_change_with_semantic_content():
    first = component_hashes({"analyses": {"outcome": "y", "predictors": ["x"]}})
    reordered = component_hashes({"analyses": {"predictors": ["x"], "outcome": "y"}})
    changed = component_hashes({"analyses": {"outcome": "y", "predictors": ["z"]}})

    assert first == reordered
    assert first["analyses"] != changed["analyses"]


def test_reserved_identifier_collisions_are_rejected():
    with pytest.raises(ResultContractError, match="reserved"):
        validate_reserved_ids(["base", "manifest", "user-result"], reserved=["base", "manifest"])


def test_resolved_sample_frame_preserves_integer_year_keys_and_order():
    panel_frame = pd.DataFrame(BASE_PANEL_ROWS)

    resolved = ResolvedSampleFrame(
        frame=panel_frame,
        row_keys=BASE_ROW_KEYS,
        geo_key="geo_id",
        time_key="year",
    )

    assert resolved.row_keys == BASE_ROW_KEYS
    assert isinstance(resolved.row_keys[0][1], int)
    resolved.validate_result_keys(BASE_ROW_KEYS)


def test_resolved_sample_rechecks_mutable_frame_keys_before_consumption():
    panel_frame = pd.DataFrame(BASE_PANEL_ROWS)
    resolved = ResolvedSampleFrame(
        frame=panel_frame,
        row_keys=BASE_ROW_KEYS,
        geo_key="geo_id",
        time_key="year",
    )
    panel_frame.loc[0, "year"] = 2022

    with pytest.raises(ValueError, match="ordered frame key columns"):
        resolved.validate_result_keys(BASE_ROW_KEYS)


def test_analysis_execution_result_normalizes_status_and_requires_failure_record():
    complete = AnalysisExecutionResult(status="complete", consumed_row_keys=BASE_ROW_KEYS)
    assert complete.status is AnalysisStatus.COMPLETE

    with pytest.raises(ValueError, match="requires an error"):
        AnalysisExecutionResult(status="failed")

    failed = AnalysisExecutionResult(
        status=AnalysisStatus.FAILED,
        error=AnalysisErrorRecord(code="estimation_failed", message="failed"),
    )
    assert failed.status is AnalysisStatus.FAILED


@pytest.mark.parametrize(
    "invalid_row_keys",
    [
        pytest.param((("01001", 2020), ("01001", 2020)), id="duplicate-key"),
        pytest.param((("01001", 2021), ("01001", 2020)), id="wrong-order"),
        pytest.param((("01001", 2020),), id="missing-row"),
        pytest.param((("01001", 2020), ("01001", 2021), ("01001", 2022)), id="extra-row"),
    ],
)
def test_resolved_sample_frame_rejects_duplicate_or_mismatched_frame_keys(invalid_row_keys):
    with pytest.raises(ValueError):
        ResolvedSampleFrame(
            frame=pd.DataFrame(BASE_PANEL_ROWS),
            row_keys=invalid_row_keys,
            geo_key="geo_id",
            time_key="year",
        )


def test_redistribution_lineage_uses_most_restrictive_source_term():
    allowed = SourceRedistribution(
        row_level=RedistributionStatus.ALLOWED,
        aggregate=RedistributionStatus.ALLOWED,
    )
    restricted = SourceRedistribution(
        row_level=RedistributionStatus.RESTRICTED,
        aggregate=RedistributionStatus.UNKNOWN,
    )

    combined = combine_source_terms({"pit": allowed, "survey": restricted})

    assert combined.row_level is RedistributionStatus.RESTRICTED
    assert combined.aggregate is RedistributionStatus.UNKNOWN
    assert artifact_disposition(combined, scope=ArtifactScope.ROW_LEVEL) == "withheld"
    assert artifact_disposition(combined, scope=ArtifactScope.AGGREGATE) == "withheld"


@pytest.mark.parametrize(
    ("row_level", "aggregate"),
    [
        pytest.param("unsupported", RedistributionStatus.ALLOWED, id="bad-row-term"),
        pytest.param(RedistributionStatus.ALLOWED, "unsupported", id="bad-aggregate-term"),
    ],
)
def test_source_redistribution_direct_constructor_rejects_unknown_statuses(row_level, aggregate):
    with pytest.raises(ResultContractError, match="invalid source redistribution"):
        SourceRedistribution(row_level=row_level, aggregate=aggregate)


def test_private_and_unknown_aggregate_overrides_are_explicit():
    unknown = SourceRedistribution(
        row_level=RedistributionStatus.ALLOWED,
        aggregate=RedistributionStatus.UNKNOWN,
    )

    assert artifact_disposition(unknown, scope=ArtifactScope.AGGREGATE) == "withheld"
    assert (
        artifact_disposition(
            unknown,
            scope=ArtifactScope.AGGREGATE,
            allow_unknown_aggregates=True,
        )
        == "included"
    )
    assert (
        artifact_disposition(
            unknown,
            scope=ArtifactScope.ROW_LEVEL,
            private=True,
        )
        == "included_private"
    )


def test_schema_contract_modules_import_no_application_or_provider_modules():
    repository_root = Path(__file__).resolve().parents[1]
    module_paths = (
        repository_root / "hhplab" / "schema" / "results.py",
        repository_root / "hhplab" / "schema" / "redistribution.py",
    )
    forbidden_prefixes = ("hhplab.analysis", "hhplab.recipe", "hhplab.results", "hhplab.sources")

    imported_modules: list[str] = []
    for module_path in module_paths:
        syntax_tree = ast.parse(module_path.read_text(encoding="utf-8"))
        for node in ast.walk(syntax_tree):
            if isinstance(node, ast.Import):
                imported_modules.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module is not None:
                imported_modules.append(node.module)

    assert not [
        module
        for module in imported_modules
        if any(module == prefix or module.startswith(prefix + ".") for prefix in forbidden_prefixes)
    ]
