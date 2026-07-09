"""Covariate finding sidecars for result workflows."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from hhplab.results.workflows._paths import OUTPUTS_ROOT, REPO_ROOT

FINDINGS_DIR = OUTPUTS_ROOT / "result_findings"
CONTROL_TERMS = {"const", "d_log_pop", "log_pop", "population", "year"}

SOURCE_BY_WORKFLOW = {
    "build_bps_valuation_rent_channel": "census_bps",
    "build_eviction_rate_timing_panel": "eviction_lab_national",
    "build_irs_migration_pooled_panel": "irs_soi_migration",
    "build_qcew_labor_market_panel": "qcew",
    "build_subsidized_housing_stock_panel": "hud_psh",
}

DEVDOC_BY_WORKFLOW = {
    "build_bps_valuation_rent_channel": "devdocs/bps_valuation_rent_channel_findings.md",
    "build_eviction_rate_timing_panel": "devdocs/eviction_filings_mechanism_test.md",
    "build_irs_migration_pooled_panel": "devdocs/irs_migration_pooled_direct_income_findings.md",
    "build_qcew_labor_market_panel": "devdocs/qcew_labor_market_findings.md",
    "build_subsidized_housing_stock_panel": "devdocs/composition_rent_population_findings.md",
}


@dataclass(frozen=True)
class FindingTerm:
    term: str
    model: str
    outcome: str | None
    fixed_effects: str | None
    estimate: float | None
    std_error: float | None
    p_value: float | None
    n: int | None


@dataclass(frozen=True)
class CovariateFinding:
    workflow_id: str
    module_name: str
    covariate_family: str
    source_id: str | None
    question: str
    headline_result: str
    direction: str
    robustness_status: str
    primary_spec: dict[str, Any]
    key_terms: list[FindingTerm]
    sample_window: dict[str, Any]
    coverage_notes: dict[str, Any]
    artifact_paths: dict[str, str]
    devdoc_path: str | None
    retraction_status: str = "active"
    supersedes: list[str] = field(default_factory=list)
    next_checks: list[str] = field(default_factory=list)

    def to_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["key_terms"] = [asdict(term) for term in self.key_terms]
        return payload


def _relative_path(value: object) -> str:
    path = Path(str(value))
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def _candidate_rows(result: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key in ("regressions", "outflow_robustness", "models"):
        value = result.get(key)
        if isinstance(value, list):
            rows.extend(row for row in value if isinstance(row, dict))
    return rows


def _focal_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    focal = [
        row
        for row in rows
        if str(row.get("term", "")) not in CONTROL_TERMS and row.get("estimate") is not None
    ]
    return sorted(
        focal,
        key=lambda row: (
            float(row["p_value"]) if row.get("p_value") is not None else float("inf"),
            str(row.get("model", "")),
            str(row.get("term", "")),
        ),
    )


def _direction(rows: list[dict[str, Any]]) -> str:
    significant = [
        row
        for row in rows
        if row.get("p_value") is not None and float(row["p_value"]) < 0.05
    ]
    if not significant:
        return "null"
    signs = {1 if float(row["estimate"]) > 0 else -1 for row in significant}
    if signs == {1}:
        return "positive"
    if signs == {-1}:
        return "negative"
    return "mixed"


def _headline(direction: str, primary: dict[str, Any] | None) -> str:
    if primary is None:
        return "No structured covariate result rows were available."
    term = primary.get("term", "covariate")
    estimate = primary.get("estimate")
    p_value = primary.get("p_value")
    if direction == "null":
        return f"No focal covariate term is significant at p < 0.05; strongest term is {term}."
    return f"{term} is the strongest {direction} focal term (estimate={estimate}, p={p_value})."


def _sample_window(summary: dict[str, Any]) -> dict[str, Any]:
    years = summary.get("years")
    if isinstance(years, list) and years:
        return {"start_year": min(years), "end_year": max(years), "years": years}
    return {}


def finding_from_result(
    *,
    workflow_id: str,
    module_name: str,
    result: dict[str, Any],
) -> CovariateFinding | None:
    rows = _focal_rows(_candidate_rows(result))
    if not rows:
        return None
    primary = rows[0]
    summary = result.get("summary") if isinstance(result.get("summary"), dict) else {}
    outputs = result.get("outputs") if isinstance(result.get("outputs"), dict) else {}
    direction = _direction(rows)
    key_terms = [
        FindingTerm(
            term=str(row.get("term", "")),
            model=str(row.get("model", "")),
            outcome=row.get("outcome"),
            fixed_effects=row.get("fixed_effects"),
            estimate=row.get("estimate"),
            std_error=row.get("std_error"),
            p_value=row.get("p_value"),
            n=row.get("nobs") or row.get("n"),
        )
        for row in rows[:8]
    ]
    return CovariateFinding(
        workflow_id=workflow_id,
        module_name=module_name,
        covariate_family=str(primary.get("family") or summary.get("primary_model_family") or ""),
        source_id=SOURCE_BY_WORKFLOW.get(module_name),
        question=f"What did {workflow_id} learn from the structured covariate screen?",
        headline_result=_headline(direction, primary),
        direction=direction,
        robustness_status="structured_result_sidecar",
        primary_spec={
            "model": primary.get("model"),
            "outcome": primary.get("outcome"),
            "fixed_effects": primary.get("fixed_effects"),
            "std_error_type": primary.get("std_error_type"),
        },
        key_terms=key_terms,
        sample_window=_sample_window(summary),
        coverage_notes={
            key: summary[key]
            for key in (
                "levels_rows",
                "fd_rows_year_gap_1",
                "msa_count",
                "complete_fd_rows_for_inflow_agi_per_return_k",
            )
            if key in summary
        },
        artifact_paths={str(key): _relative_path(value) for key, value in outputs.items()},
        devdoc_path=DEVDOC_BY_WORKFLOW.get(module_name),
    )


def write_finding_sidecar(finding: CovariateFinding, directory: Path | None = None) -> Path:
    directory = FINDINGS_DIR if directory is None else directory
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{finding.workflow_id}__{finding.module_name}.finding.json"
    path.write_text(json.dumps(finding.to_payload(), indent=2, sort_keys=True) + "\n")
    return path


def write_finding_sidecar_from_result(
    *,
    workflow_id: str,
    module_name: str,
    result: dict[str, Any],
    directory: Path | None = None,
) -> Path | None:
    finding = finding_from_result(workflow_id=workflow_id, module_name=module_name, result=result)
    if finding is None:
        return None
    return write_finding_sidecar(finding, directory=directory)


def read_finding(path: Path | str) -> dict[str, Any]:
    with Path(path).open(encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Finding sidecar must contain a JSON object: {path}")
    return payload


def list_findings(directory: Path | None = None) -> list[dict[str, Any]]:
    directory = FINDINGS_DIR if directory is None else directory
    if not directory.exists():
        return []
    findings = []
    for path in sorted(directory.glob("*.finding.json")):
        try:
            payload = read_finding(path)
        except (OSError, ValueError, json.JSONDecodeError):
            continue
        payload["finding_path"] = str(path)
        findings.append(payload)
    return findings
