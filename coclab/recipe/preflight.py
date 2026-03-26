"""Recipe preflight analyzer: validate readiness without executing.

Resolves each pipeline plan, enumerates required dataset-year inputs and
transforms, inspects schemas where files exist, and returns a structured
report.  The report is consumable by CLI commands (human and JSON) and
by downstream automation.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from coclab.recipe.adapters import (
    ValidationDiagnostic,
    dataset_registry,
    geometry_registry,
    validate_recipe_adapters,
)
from coclab.recipe.default_adapters import register_defaults
from coclab.recipe.planner import (
    ExecutionPlan,
    PlannerError,
    ResampleTask,
    resolve_plan,
)
from coclab.recipe.probes import (
    probe_dataset_schema,
    probe_geo_column,
    probe_measures,
    probe_static_broadcast,
    probe_temporal_filter,
    probe_transform_path,
    probe_year_column,
)
from coclab.recipe.recipe_schema import (
    RecipeV1,
    TemporalFilter,
    expand_year_spec,
)


# ---------------------------------------------------------------------------
# Finding model
# ---------------------------------------------------------------------------

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
        return sum(
            1 for f in self.findings if f.severity == Severity.WARNING
        )

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

def _check_dataset_paths(
    recipe: RecipeV1,
    project_root: Path,
) -> list[PreflightFinding]:
    """Check that all referenced dataset files exist on disk."""
    findings: list[PreflightFinding] = []
    policy = recipe.validation.missing_dataset
    policy_extra: dict[str, str] = policy.model_extra or {}

    for ds_id, ds in recipe.datasets.items():
        is_optional = ds.optional
        per_ds_policy = policy_extra.get(ds_id)
        severity = Severity.WARNING if (
            per_ds_policy == "warn" or (per_ds_policy is None and is_optional)
            or (per_ds_policy is None and not is_optional and policy.default == "warn")
        ) else Severity.ERROR

        if ds.path is not None:
            resolved = project_root / ds.path
            if not resolved.exists():
                findings.append(PreflightFinding(
                    severity=severity,
                    kind=FindingKind.MISSING_DATASET,
                    message=f"Dataset '{ds_id}' path not found: {ds.path}",
                    dataset_id=ds_id,
                    remediation=_dataset_remediation(ds_id, ds),
                ))

        if ds.file_set is not None:
            for seg in ds.file_set.segments:
                seg_years = expand_year_spec(seg.years)
                missing_years: list[int] = []
                for year in seg_years:
                    if year in seg.overrides:
                        p = seg.overrides[year]
                    else:
                        render_ctx: dict[str, object] = {"year": year}
                        render_ctx.update(seg.constants)
                        render_ctx.update(
                            {k: year + offset
                             for k, offset in seg.year_offsets.items()}
                        )
                        try:
                            p = ds.file_set.path_template.format(**render_ctx)
                        except KeyError:
                            continue  # planner will catch template errors
                    if not (project_root / p).exists():
                        missing_years.append(year)

                if missing_years:
                    findings.append(PreflightFinding(
                        severity=severity,
                        kind=FindingKind.MISSING_DATASET,
                        message=(
                            f"Dataset '{ds_id}': {len(missing_years)} "
                            f"file(s) missing for years {missing_years}"
                        ),
                        dataset_id=ds_id,
                        years=missing_years,
                        remediation=_dataset_remediation(ds_id, ds),
                    ))

    return findings


def _dataset_remediation(ds_id: str, ds) -> Remediation:
    """Build a remediation hint for a missing dataset."""
    provider = ds.provider
    product = ds.product
    return Remediation(
        hint=(
            f"Ingest {provider}/{product} data for dataset '{ds_id}'."
        ),
        command=f"coclab ingest {product}" if product else None,
    )


def _check_adapter_validation(
    recipe: RecipeV1,
) -> list[PreflightFinding]:
    """Run adapter registry validation and convert to findings."""
    register_defaults()
    diagnostics = validate_recipe_adapters(
        recipe, geometry_registry, dataset_registry,
    )
    findings: list[PreflightFinding] = []
    for d in diagnostics:
        findings.append(PreflightFinding(
            severity=(
                Severity.ERROR if d.level == "error" else Severity.WARNING
            ),
            kind=FindingKind.ADAPTER_ERROR,
            message=d.message,
        ))
    return findings


def _check_transforms(
    recipe: RecipeV1,
    project_root: Path,
    needed_transforms: set[str],
) -> list[PreflightFinding]:
    """Check that required transform artifacts exist."""
    findings: list[PreflightFinding] = []
    for tid in sorted(needed_transforms):
        result = probe_transform_path(tid, recipe, project_root)
        if not result.ok:
            can_generate = (
                result.detail.get("can_generate", False)
                if result.detail else False
            )
            transform = None
            for t in recipe.transforms:
                if t.id == tid:
                    transform = t
                    break
            is_metro = (
                transform is not None
                and (transform.from_.type == "metro"
                     or transform.to.type == "metro")
            )
            if is_metro:
                cmd = "coclab generate metro"
                hint = (
                    f"Metro transform '{tid}' can be generated. "
                    f"Ensure metro definition artifacts exist."
                )
            else:
                cmd = "coclab generate xwalks"
                hint = (
                    f"Generate crosswalk artifacts for transform '{tid}'."
                )
            findings.append(PreflightFinding(
                severity=Severity.ERROR,
                kind=FindingKind.MISSING_TRANSFORM,
                message=result.message,
                transform_id=tid,
                remediation=Remediation(hint=hint, command=cmd),
            ))
    return findings


def _check_dataset_schemas(
    recipe: RecipeV1,
    project_root: Path,
    resample_tasks: list[ResampleTask],
) -> list[PreflightFinding]:
    """Inspect dataset schemas for column issues without loading data."""
    findings: list[PreflightFinding] = []
    universe_years = expand_year_spec(recipe.universe)

    # Deduplicate: check each (dataset_id, path) once
    checked: set[tuple[str, str]] = set()

    for task in resample_tasks:
        if task.input_path is None:
            continue
        key = (task.dataset_id, task.input_path)
        if key in checked:
            continue
        checked.add(key)

        full_path = project_root / task.input_path
        schema_result = probe_dataset_schema(full_path)
        if not schema_result.ok:
            # File missing or unreadable — already covered by path checks
            continue

        columns = schema_result.detail["columns"]
        ds = recipe.datasets.get(task.dataset_id)
        if ds is None:
            continue

        # Year column
        year_result = probe_year_column(columns, ds.year_column)
        if not year_result.ok:
            sev = Severity.ERROR
            if "Ambiguous" in (year_result.message or ""):
                kind = FindingKind.AMBIGUOUS_COLUMN
            else:
                kind = FindingKind.MISSING_COLUMN
            findings.append(PreflightFinding(
                severity=sev,
                kind=kind,
                message=(
                    f"Dataset '{task.dataset_id}' ({task.input_path}): "
                    f"{year_result.message}"
                ),
                dataset_id=task.dataset_id,
            ))

        # Geo column
        geo_result = probe_geo_column(columns, ds.geo_column)
        if not geo_result.ok:
            if "Ambiguous" in (geo_result.message or ""):
                kind = FindingKind.AMBIGUOUS_COLUMN
            else:
                kind = FindingKind.MISSING_COLUMN
            findings.append(PreflightFinding(
                severity=Severity.ERROR,
                kind=kind,
                message=(
                    f"Dataset '{task.dataset_id}' ({task.input_path}): "
                    f"{geo_result.message}"
                ),
                dataset_id=task.dataset_id,
            ))

        # Measures
        measure_result = probe_measures(
            columns, task.measures, task.dataset_id,
        )
        if not measure_result.ok:
            findings.append(PreflightFinding(
                severity=Severity.ERROR,
                kind=FindingKind.MISSING_MEASURE,
                message=(
                    f"Dataset '{task.dataset_id}' ({task.input_path}): "
                    f"{measure_result.message}"
                ),
                dataset_id=task.dataset_id,
            ))

        # Temporal filter
        filt = recipe.filters.get(task.dataset_id)
        if filt is not None and isinstance(filt, TemporalFilter):
            tf_result = probe_temporal_filter(
                columns, filt, task.dataset_id,
            )
            if not tf_result.ok:
                findings.append(PreflightFinding(
                    severity=Severity.ERROR,
                    kind=FindingKind.TEMPORAL_FILTER,
                    message=(
                        f"Dataset '{task.dataset_id}' ({task.input_path}): "
                        f"{tf_result.message}"
                    ),
                    dataset_id=task.dataset_id,
                ))

        # Static broadcast
        year_col_found = (
            year_result.ok
            and year_result.detail is not None
            and year_result.detail.get("year_column") is not None
        )
        broadcast_result = probe_static_broadcast(
            ds,
            task.dataset_id,
            year_column_found=year_col_found,
            universe_year_count=len(universe_years),
        )
        if not broadcast_result.ok:
            findings.append(PreflightFinding(
                severity=Severity.ERROR,
                kind=FindingKind.STATIC_BROADCAST,
                message=(
                    f"Dataset '{task.dataset_id}': "
                    f"{broadcast_result.message}"
                ),
                dataset_id=task.dataset_id,
                years=universe_years,
                remediation=Remediation(
                    hint=(
                        "Add year_column, use file_set for per-year files, "
                        "or set params.broadcast_static=true."
                    ),
                ),
            ))

    return findings


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_preflight(
    recipe: RecipeV1,
    project_root: Path | None = None,
) -> PreflightReport:
    """Run a complete preflight analysis on a recipe.

    Resolves each pipeline plan, checks dataset paths, transform
    artifacts, and dataset schemas.  Returns a structured report
    without executing any build steps.

    Parameters
    ----------
    recipe : RecipeV1
        A structurally valid recipe.
    project_root : Path | None
        Project root for resolving paths.  Defaults to cwd.

    Returns
    -------
    PreflightReport
        Structured report with all findings.
    """
    if project_root is None:
        project_root = Path.cwd()

    register_defaults()

    universe_years = expand_year_spec(recipe.universe)
    report = PreflightReport(
        recipe_name=recipe.name,
        recipe_version=recipe.version,
        universe_years=universe_years,
    )

    # 1. Adapter validation
    report.findings.extend(_check_adapter_validation(recipe))

    # 2. Dataset path checks
    report.findings.extend(_check_dataset_paths(recipe, project_root))

    # 3. Resolve plans and collect tasks
    all_resample_tasks: list[ResampleTask] = []
    needed_transforms: set[str] = set()

    for pipeline in recipe.pipelines:
        try:
            plan = resolve_plan(recipe, pipeline.id)
            summary = PipelineSummary(
                pipeline_id=pipeline.id,
                plan=plan,
                task_count=(
                    len(plan.materialize_tasks)
                    + len(plan.resample_tasks)
                    + len(plan.join_tasks)
                ),
            )
            report.pipelines.append(summary)

            # Collect transforms and resample tasks
            for mt in plan.materialize_tasks:
                needed_transforms.update(mt.transform_ids)
            for rt in plan.resample_tasks:
                if rt.transform_id:
                    needed_transforms.add(rt.transform_id)
            all_resample_tasks.extend(plan.resample_tasks)

        except PlannerError as exc:
            summary = PipelineSummary(
                pipeline_id=pipeline.id,
                plan_error=str(exc),
            )
            report.pipelines.append(summary)
            report.findings.append(PreflightFinding(
                severity=Severity.ERROR,
                kind=FindingKind.PLANNER_ERROR,
                message=f"Pipeline '{pipeline.id}': {exc}",
                pipeline_id=pipeline.id,
            ))

    # 4. Transform artifact checks
    report.findings.extend(
        _check_transforms(recipe, project_root, needed_transforms),
    )

    # 5. Dataset schema probes
    report.findings.extend(
        _check_dataset_schemas(recipe, project_root, all_resample_tasks),
    )

    return report
