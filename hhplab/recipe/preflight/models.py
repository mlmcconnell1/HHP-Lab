"""Preflight report and finding models."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field

from hhplab.recipe.planner import ExecutionPlan


class Severity(str, enum.Enum):
    """Severity of a preflight finding."""

    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


class FindingKind(str, enum.Enum):
    """Classification of preflight findings."""

    MISSING_DATASET = "missing_dataset"
    MISSING_TRANSFORM = "missing_transform"
    MISSING_COLUMN = "missing_column"
    AMBIGUOUS_COLUMN = "ambiguous_column"
    UNCOVERED_YEARS = "uncovered_years"
    STATIC_BROADCAST = "static_broadcast"
    PLANNER_ERROR = "planner_error"
    ADAPTER_ERROR = "adapter_error"
    SCHEMA_UNREADABLE = "schema_unreadable"
    MISSING_MEASURE = "missing_measure"
    TEMPORAL_FILTER = "temporal_filter"
    TEMPORAL_ALIGNMENT = "temporal_alignment"
    DATASET_PROVENANCE = "dataset_provenance"
    MISSING_SUPPORT_DATASET = "missing_support_dataset"
    CT_COUNTY_ALIGNMENT = "ct_county_alignment"
    MISSING_MAP_ARTIFACT = "missing_map_artifact"
    MISSING_CONTAINMENT_ARTIFACT = "missing_containment_artifact"
    MISSING_MSA_COC_COVERAGE_ARTIFACT = "missing_msa_coc_coverage_artifact"
    MISSING_MSA_FRACTIONAL_ROLLUP_ARTIFACT = "missing_msa_fractional_rollup_artifact"
    MISSING_PRIMARY_MSA_ARTIFACT = "missing_primary_msa_artifact"
    CONTAINMENT_SELECTOR = "containment_selector"
    TARGET_SELECTOR = "target_selector"


@dataclass
class Remediation:
    """Actionable remediation for a finding."""

    hint: str
    command: str | None = None

    def to_dict(self) -> dict:
        d: dict = {"hint": self.hint}
        if self.command is not None:
            d["command"] = self.command
        return d


@dataclass
class PreflightFinding:
    """A single preflight finding."""

    severity: Severity
    kind: FindingKind
    message: str
    dataset_id: str | None = None
    transform_id: str | None = None
    pipeline_id: str | None = None
    years: list[int] | None = None
    geometry: str | None = None
    remediation: Remediation | None = None

    @property
    def is_blocking(self) -> bool:
        return self.severity == Severity.ERROR

    def to_dict(self) -> dict:
        d: dict = {
            "severity": self.severity.value,
            "kind": self.kind.value,
            "message": self.message,
        }
        if self.dataset_id is not None:
            d["dataset_id"] = self.dataset_id
        if self.transform_id is not None:
            d["transform_id"] = self.transform_id
        if self.pipeline_id is not None:
            d["pipeline_id"] = self.pipeline_id
        if self.years is not None:
            d["years"] = self.years
        if self.geometry is not None:
            d["geometry"] = self.geometry
        if self.remediation is not None:
            d["remediation"] = self.remediation.to_dict()
        return d


# ---------------------------------------------------------------------------
# Report model
# ---------------------------------------------------------------------------


@dataclass
class PipelineSummary:
    """Summary of preflight results for one pipeline."""

    pipeline_id: str
    plan: ExecutionPlan | None = None
    plan_error: str | None = None
    task_count: int = 0

    def to_dict(self) -> dict:
        d: dict = {"pipeline_id": self.pipeline_id}
        if self.plan is not None:
            d["task_count"] = self.task_count
        if self.plan_error is not None:
            d["plan_error"] = self.plan_error
        return d


@dataclass
class PreflightReport:
    """Structured preflight report for a recipe."""

    recipe_name: str
    recipe_version: int
    universe_years: list[int]
    findings: list[PreflightFinding] = field(default_factory=list)
    pipelines: list[PipelineSummary] = field(default_factory=list)

    @property
    def blocking_count(self) -> int:
        return sum(1 for f in self.findings if f.is_blocking)

    @property
    def warning_count(self) -> int:
        return sum(1 for f in self.findings if f.severity == Severity.WARNING)

    @property
    def is_ready(self) -> bool:
        return self.blocking_count == 0

    def blocking_findings(self) -> list[PreflightFinding]:
        return [f for f in self.findings if f.is_blocking]

    def gaps_manifest(self) -> dict:
        """Return a structured data-gaps manifest suitable for automation.

        Groups findings by kind and includes per-gap metadata with
        remediation hints.  Only includes gap-type findings (missing
        datasets, transforms, columns, measures, etc.), not informational
        items.
        """
        gap_kinds = {
            FindingKind.MISSING_DATASET,
            FindingKind.MISSING_TRANSFORM,
            FindingKind.MISSING_COLUMN,
            FindingKind.AMBIGUOUS_COLUMN,
            FindingKind.UNCOVERED_YEARS,
            FindingKind.STATIC_BROADCAST,
            FindingKind.MISSING_MEASURE,
            FindingKind.TEMPORAL_FILTER,
            FindingKind.DATASET_PROVENANCE,
            FindingKind.MISSING_SUPPORT_DATASET,
            FindingKind.CT_COUNTY_ALIGNMENT,
            FindingKind.MISSING_MAP_ARTIFACT,
            FindingKind.MISSING_CONTAINMENT_ARTIFACT,
            FindingKind.CONTAINMENT_SELECTOR,
            FindingKind.TARGET_SELECTOR,
        }
        gaps = [f for f in self.findings if f.kind in gap_kinds]
        by_kind: dict[str, list[dict]] = {}
        for g in gaps:
            by_kind.setdefault(g.kind.value, []).append(g.to_dict())
        return {
            "recipe_name": self.recipe_name,
            "total_gaps": len(gaps),
            "blocking_gaps": sum(1 for g in gaps if g.is_blocking),
            "gaps_by_kind": by_kind,
        }

    def to_dict(self) -> dict:
        return {
            "recipe_name": self.recipe_name,
            "recipe_version": self.recipe_version,
            "universe_years": self.universe_years,
            "ready": self.is_ready,
            "blocking_count": self.blocking_count,
            "warning_count": self.warning_count,
            "pipelines": [p.to_dict() for p in self.pipelines],
            "findings": [f.to_dict() for f in self.findings],
        }


# ---------------------------------------------------------------------------
# Analyzer implementation
# ---------------------------------------------------------------------------

