"""Recipe preflight analyzer: validate readiness without executing.

Resolves each pipeline plan, enumerates required dataset-year inputs and
transforms, inspects schemas where files exist, and returns a structured
report.  The report is consumable by CLI commands (human and JSON) and
by downstream automation.
"""

from __future__ import annotations

import enum
import re
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from hhplab.geo.ct_planning_regions import (
    CT_LEGACY_COUNTY_VINTAGE,
    CT_PLANNING_REGION_VINTAGE,
    build_ct_county_planning_region_crosswalk,
    is_ct_legacy_county_fips,
    is_ct_planning_region_fips,
)
from hhplab.geo.io import resolve_curated_boundary_path
from hhplab.naming import metro_boundaries_path, msa_boundaries_path
from hhplab.naming import county_path
from hhplab.recipe.adapters import (
    dataset_registry,
    geometry_registry,
    validate_recipe_adapters,
)
from hhplab.recipe.default_adapters import register_defaults
from hhplab.recipe.planner import (
    ExecutionPlan,
    PlannerError,
    ResampleTask,
    _resolve_dataset_year,
    resolve_plan,
)
from hhplab.recipe.probes import (
    get_weighted_transform_requirements,
    probe_acs5_tract_translation_provenance,
    probe_dataset_schema,
    probe_geo_column,
    probe_interpolate_to_month_data,
    probe_measures,
    probe_static_broadcast,
    probe_support_dataset,
    probe_temporal_filter,
    probe_transform_path,
    probe_year_column,
)
from hhplab.recipe.recipe_schema import (
    JoinStep,
    RecipeV1,
    ResampleStep,
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
    TEMPORAL_ALIGNMENT = "temporal_alignment"
    DATASET_PROVENANCE = "dataset_provenance"
    MISSING_SUPPORT_DATASET = "missing_support_dataset"
    CT_COUNTY_ALIGNMENT = "ct_county_alignment"
    MISSING_MAP_ARTIFACT = "missing_map_artifact"


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
            FindingKind.DATASET_PROVENANCE,
            FindingKind.MISSING_SUPPORT_DATASET,
            FindingKind.CT_COUNTY_ALIGNMENT,
            FindingKind.MISSING_MAP_ARTIFACT,
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
    resample_tasks: list[ResampleTask],
) -> list[PreflightFinding]:
    """Check that plan-required dataset files exist on disk.

    Only checks paths for (dataset_id, year) pairs that appear in the
    resolved execution plan, so years outside the recipe universe or
    datasets not referenced by any pipeline are ignored.
    """
    findings: list[PreflightFinding] = []
    policy = recipe.validation.missing_dataset
    policy_extra: dict[str, str] = policy.model_extra or {}

    # Group tasks by dataset to aggregate missing years
    by_dataset: dict[str, list[ResampleTask]] = {}
    for task in resample_tasks:
        by_dataset.setdefault(task.dataset_id, []).append(task)

    for ds_id, tasks in by_dataset.items():
        ds = recipe.datasets.get(ds_id)
        if ds is None:
            continue

        is_optional = ds.optional
        per_ds_policy = policy_extra.get(ds_id)
        severity = Severity.WARNING if (
            per_ds_policy == "warn" or (per_ds_policy is None and is_optional)
            or (per_ds_policy is None and not is_optional
                and policy.default == "warn")
        ) else Severity.ERROR

        # Deduplicate: check each resolved path once
        checked_paths: set[str] = set()
        missing_years: list[int] = []
        missing_paths: set[str] = set()

        for task in tasks:
            path = task.input_path
            if path is None:
                # Static dataset with no file_set — use ds.path
                if ds.path is not None and ds.path not in checked_paths:
                    checked_paths.add(ds.path)
                    if not (project_root / ds.path).exists():
                        findings.append(PreflightFinding(
                            severity=severity,
                            kind=FindingKind.MISSING_DATASET,
                            message=(
                                f"Dataset '{ds_id}' path not found: "
                                f"{ds.path}"
                            ),
                            dataset_id=ds_id,
                            remediation=_dataset_remediation(ds_id, ds),
                        ))
                continue

            if path in checked_paths:
                continue
            checked_paths.add(path)

            if not (project_root / path).exists():
                missing_years.append(task.year)
                missing_paths.add(path)

        if missing_years:
            if len(missing_paths) == 1:
                msg = (
                    f"Dataset '{ds_id}' path not found: "
                    f"{next(iter(missing_paths))}"
                )
            else:
                msg = (
                    f"Dataset '{ds_id}': {len(missing_years)} "
                    f"file(s) missing for years {missing_years}"
                )
            findings.append(PreflightFinding(
                severity=severity,
                kind=FindingKind.MISSING_DATASET,
                message=msg,
                dataset_id=ds_id,
                years=missing_years,
                remediation=_dataset_remediation(ds_id, ds, years=missing_years),
            ))

    return findings


def _check_dataset_provenance(
    recipe: RecipeV1,
    project_root: Path,
    resample_tasks: list[ResampleTask],
) -> list[PreflightFinding]:
    """Block known-stale translated ACS tract caches via provenance checks."""
    findings: list[PreflightFinding] = []
    tasks_by_path: dict[tuple[str, str], list[ResampleTask]] = {}

    for task in resample_tasks:
        if task.input_path is None:
            continue
        tasks_by_path.setdefault((task.dataset_id, task.input_path), []).append(task)

    for (dataset_id, input_path), tasks in tasks_by_path.items():
        ds = recipe.datasets.get(dataset_id)
        if ds is None:
            continue

        full_path = project_root / input_path
        if not full_path.exists():
            continue

        result = probe_acs5_tract_translation_provenance(
            dataset_id=dataset_id,
            dataset_spec=ds,
            effective_geometry=tasks[0].effective_geometry,
            path=full_path,
            path_label=input_path,
        )
        if result.ok:
            continue

        remediation_command = (
            result.detail.get("remediation_command")
            if result.detail is not None
            else None
        )
        findings.append(PreflightFinding(
            severity=Severity.ERROR,
            kind=FindingKind.DATASET_PROVENANCE,
            message=result.message or "Dataset provenance validation failed.",
            dataset_id=dataset_id,
            years=sorted(task.year for task in tasks),
            remediation=Remediation(
                hint=(
                    "Rebuild the translated ACS tract cache so its provenance "
                    "records the source and target tract vintages."
                ),
                command=(
                    str(remediation_command)
                    if remediation_command is not None
                    else None
                ),
            ),
        ))

    return findings


def _dataset_remediation(ds_id: str, ds, *, years: list[int] | None = None) -> Remediation:
    """Build a remediation hint for a missing dataset."""
    from hhplab.acs.variables_acs1 import ACS1_UNAVAILABLE_VINTAGES

    provider = ds.provider
    product = ds.product

    # Detect ACS 1-year datasets that span unavailable vintages (e.g. 2020).
    if product == "acs1" and years:
        unavailable = sorted(set(years) & ACS1_UNAVAILABLE_VINTAGES)
        if unavailable:
            years_str = ", ".join(str(y) for y in unavailable)
            return Remediation(
                hint=(
                    f"ACS 1-year data for vintage(s) {years_str} is not "
                    f"available from Census (data collection was disrupted by "
                    f"COVID-19 in 2020). No ingest command can succeed for "
                    f"these vintages. Consider BLS LAUS data for labor-market "
                    f"measures, or exclude {years_str} from the recipe universe."
                ),
                command=None,
            )

    return Remediation(
        hint=(
            f"Ingest {provider}/{product} data for dataset '{ds_id}'."
        ),
        command=f"hhplab ingest {product}" if product else None,
    )


def _msa_transform_remediation(transform_id: str, transform, missing_inputs: list[str]) -> Remediation:
    """Build an actionable remediation hint for an MSA transform artifact."""
    from pathlib import Path

    from hhplab.recipe.executor_transforms import _identify_msa_and_base

    msa_ref, base_ref = _identify_msa_and_base(transform.from_, transform.to)
    definition_version = msa_ref.source if msa_ref is not None else None

    if base_ref.type == "coc" and base_ref.vintage is not None and definition_version is not None:
        crosswalk_command = (
            "hhplab generate msa-xwalk "
            f"--boundary {base_ref.vintage} "
            f"--definition-version {definition_version} "
            f"--counties {base_ref.vintage}"
        )
    else:
        crosswalk_command = None

    if not missing_inputs:
        return Remediation(
            hint=(
                f"Generate the cached CoC-to-MSA crosswalk artifact for transform "
                f"'{transform_id}'."
            ),
            command=crosswalk_command,
        )

    details: list[str] = []
    commands: list[str] = []
    for missing in missing_inputs:
        name = Path(missing).name
        if name.startswith("msa_county_membership__") and definition_version is not None:
            details.append(f"missing MSA county membership artifact '{missing}'")
            commands.append(f"hhplab generate msa --definition-version {definition_version}")
            continue
        if name.startswith("msa_definitions__") and definition_version is not None:
            details.append(f"missing MSA definitions artifact '{missing}'")
            commands.append(f"hhplab generate msa --definition-version {definition_version}")
            continue
        if name.startswith("coc__B") and base_ref.type == "coc" and base_ref.vintage is not None:
            details.append(f"missing CoC boundary artifact '{missing}'")
            commands.append(
                f"hhplab ingest boundaries --source hud_exchange --vintage {base_ref.vintage}"
            )
            continue
        if name.startswith("counties__C") and base_ref.type == "coc" and base_ref.vintage is not None:
            details.append(f"missing county geometry artifact '{missing}'")
            commands.append(f"hhplab ingest tiger --year {base_ref.vintage} --type counties")
            continue
        if name.startswith("tracts__T") and base_ref.type == "tract" and base_ref.vintage is not None:
            details.append(f"missing tract geometry artifact '{missing}'")
            commands.append(f"hhplab ingest tiger --year {base_ref.vintage} --type tracts")
            continue
        details.append(f"missing prerequisite artifact '{missing}'")

    hint = (
        f"MSA transform '{transform_id}' can be generated once its prerequisites exist: "
        + "; ".join(details)
        + "."
    )
    return Remediation(
        hint=hint,
        command=commands[0] if commands else crosswalk_command,
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


def _check_temporal_alignment_guidance(
    recipe: RecipeV1,
) -> list[PreflightFinding]:
    """Warn on nonstandard recipe lag choices that need review."""
    findings: list[PreflightFinding] = []
    for ds_id, ds in recipe.datasets.items():
        if ds.provider != "census":
            continue

        # ACS5 lag check: warn on nonstandard acs_end offset.
        if ds.product in {"acs", "acs5"} and ds.file_set is not None:
            for seg in ds.file_set.segments:
                acs_end_offset = seg.year_offsets.get("acs_end")
                if acs_end_offset is None or acs_end_offset == -1:
                    continue
                years = expand_year_spec(seg.years)
                year_label = seg.years.range or seg.years.years
                findings.append(PreflightFinding(
                    severity=Severity.WARNING,
                    kind=FindingKind.TEMPORAL_ALIGNMENT,
                    message=(
                        f"Dataset '{ds_id}' segment {year_label} uses "
                        f"acs_end offset {acs_end_offset}. HHP-Lab's standard "
                        "ACS lag for PIT/January-aligned panels is acs_end=-1; "
                        "review this offset against the ACS temporal guidance "
                        "before building."
                    ),
                    dataset_id=ds_id,
                    years=years,
                    remediation=Remediation(
                        hint=(
                            "Use year_offsets: { acs_end: -1 } for the standard "
                            "PIT/January-aligned ACS lag, or document why a "
                            "different offset is intentional."
                        ),
                    ),
                ))

    return findings


def _map_artifact_remediation(geo_type: str, *, source: str | None, vintage: int | None) -> Remediation:
    """Build actionable remediation for missing map boundary artifacts."""
    if geo_type == "coc":
        return Remediation(
            hint=(
                f"Ingest curated CoC boundary polygons for vintage {vintage} "
                "before rendering this map layer."
            ),
            command=(
                f"hhplab ingest boundaries --source hud_exchange --vintage {vintage}"
                if vintage is not None
                else None
            ),
        )

    if geo_type == "msa":
        return Remediation(
            hint=(
                f"Ingest official MSA boundary polygons for definition version "
                f"'{source}' aligned to county geometry vintage {vintage}."
            ),
            command=(
                f"hhplab ingest msa-boundaries --definition-version {source} --year {vintage}"
                if source is not None and vintage is not None
                else None
            ),
        )

    if geo_type == "metro":
        return Remediation(
            hint=(
                f"Generate metro boundary polygons for definition version "
                f"'{source}' using county geometry vintage {vintage}."
            ),
            command=(
                f"hhplab generate metro-boundaries --definition-version {source} --counties {vintage}"
                if source is not None and vintage is not None
                else None
            ),
        )

    return Remediation(hint="Create the missing boundary artifact required by the map layer.")


def _check_map_artifacts(
    recipe: RecipeV1,
    project_root: Path,
) -> list[PreflightFinding]:
    """Check recipe-native map targets for required boundary artifacts."""
    findings: list[PreflightFinding] = []
    base_dir = project_root / "data"

    for target in recipe.targets:
        if "map" not in target.outputs or target.map_spec is None:
            continue

        for index, layer in enumerate(target.map_spec.layers, start=1):
            layer_name = layer.label or layer.group or f"layer {index}"
            geometry = layer.geometry
            geo_type = geometry.type
            vintage = geometry.vintage
            source = geometry.source

            artifact_path: Path | None = None
            if geo_type == "coc" and vintage is not None:
                try:
                    artifact_path = resolve_curated_boundary_path(str(vintage), base_dir=base_dir)
                except FileNotFoundError:
                    artifact_path = base_dir / "curated" / "coc_boundaries" / f"coc__B{vintage}.parquet"
            elif geo_type == "msa" and source is not None and vintage is not None:
                artifact_path = msa_boundaries_path(source, base_dir)
            elif geo_type == "metro" and source is not None and vintage is not None:
                artifact_path = metro_boundaries_path(source, vintage, base_dir)
            else:
                continue

            if artifact_path.exists():
                continue

            findings.append(
                PreflightFinding(
                    severity=Severity.ERROR,
                    kind=FindingKind.MISSING_MAP_ARTIFACT,
                    message=(
                        f"Map target '{target.id}' layer '{layer_name}' requires missing "
                        f"{geo_type.upper()} boundary artifact: {artifact_path}"
                    ),
                    geometry=geo_type,
                    remediation=_map_artifact_remediation(
                        geo_type,
                        source=source,
                        vintage=vintage,
                    ),
                )
            )

    return findings


def _pipeline_dataset_ids(pipeline) -> set[str]:
    """Return dataset ids referenced by a pipeline."""
    dataset_ids: set[str] = set()
    for step in pipeline.steps:
        if isinstance(step, ResampleStep):
            dataset_ids.add(step.dataset)
        elif isinstance(step, JoinStep):
            dataset_ids.update(step.datasets)
    return dataset_ids


def _dataset_is_january_aligned(recipe: RecipeV1, dataset_id: str) -> bool:
    """Return True when a dataset is used with January/PIT temporal alignment."""
    ds = recipe.datasets[dataset_id]
    filt = recipe.filters.get(dataset_id)

    if ds.provider == "hud" and ds.product == "pit":
        return ds.params.get("align", "point_in_time_jan") == "point_in_time_jan"

    if ds.params.get("align") == "point_in_time_jan":
        return True

    if (
        filt is not None
        and filt.method in {"point_in_time", "interpolate_to_month"}
        and filt.month == 1
    ):
        return True

    return False


def _january_aligned_pipeline_ids(recipe: RecipeV1) -> set[str]:
    """Return pipeline ids that include January/PIT-aligned inputs."""
    pipeline_ids: set[str] = set()
    for pipeline in recipe.pipelines:
        dataset_ids = _pipeline_dataset_ids(pipeline)
        if any(_dataset_is_january_aligned(recipe, ds_id) for ds_id in dataset_ids):
            pipeline_ids.add(pipeline.id)
    return pipeline_ids


def _detect_year_column(path: Path, declared_year_column: str | None) -> str | None:
    """Resolve the effective year column for a parquet input."""
    if declared_year_column is not None:
        return declared_year_column

    schema_probe = probe_dataset_schema(path)
    if not schema_probe.ok or schema_probe.detail is None:
        return None

    year_probe = probe_year_column(schema_probe.detail["columns"], None)
    if not year_probe.ok or year_probe.detail is None:
        return None
    return year_probe.detail["year_column"]


def _path_contains_same_year_acs1(
    *,
    path: Path,
    analysis_year: int,
    declared_year_column: str | None,
) -> bool:
    """Return True when a parquet input contains rows for the analysis year."""
    if not path.exists():
        return False

    year_column = _detect_year_column(path, declared_year_column)
    if year_column is None:
        return False

    try:
        df = pd.read_parquet(path, columns=[year_column])
    except (FileNotFoundError, OSError, ValueError, KeyError):
        return False

    if year_column not in df.columns:
        return False

    years = pd.to_numeric(df[year_column], errors="coerce")
    return bool((years == analysis_year).any())


def _file_set_segment_for_year(ds, year: int):
    """Return the file_set segment that covers a given analysis year."""
    if ds.file_set is None:
        return None
    for seg in ds.file_set.segments:
        if year in expand_year_spec(seg.years):
            return seg
    return None


def _task_uses_same_year_acs1(
    *,
    recipe: RecipeV1,
    task: ResampleTask,
    project_root: Path,
) -> bool:
    """Return True when an ACS1 task resolves to same-year data."""
    ds = recipe.datasets[task.dataset_id]

    if task.input_path is not None:
        match = re.search(r"__A(\d{4})", task.input_path)
        if match and int(match.group(1)) == task.year:
            return True

        full_path = project_root / task.input_path
        if _path_contains_same_year_acs1(
            path=full_path,
            analysis_year=task.year,
            declared_year_column=task.year_column,
        ):
            return True

    seg = _file_set_segment_for_year(ds, task.year)
    if seg is None:
        return False

    acs1_end_offset = seg.year_offsets.get("acs1_end")
    if acs1_end_offset is None:
        acs1_end_offset = seg.year_offsets.get("acs_end")
    if acs1_end_offset == 0:
        return True

    override_path = seg.overrides.get(task.year)
    if override_path is not None:
        match = re.search(r"__A(\d{4})", override_path)
        if match and int(match.group(1)) == task.year:
            return True

    template = ds.file_set.path_template
    if (
        re.search(r"__A\{year(?::[^}]*)?\}", template)
        and "{acs1_end}" not in template
        and "{acs_end}" not in template
    ):
        return True

    return False


def _check_acs1_temporal_alignment_guidance(
    recipe: RecipeV1,
    project_root: Path,
    pipeline_tasks: list[tuple[str, ResampleTask]],
) -> list[PreflightFinding]:
    """Warn when PIT/January-aligned pipelines use same-year ACS1 vintages."""
    findings: list[PreflightFinding] = []
    january_pipeline_ids = _january_aligned_pipeline_ids(recipe)
    same_year_usage: dict[tuple[str, str], set[int]] = {}

    for pipeline_id, task in pipeline_tasks:
        if pipeline_id not in january_pipeline_ids:
            continue

        ds = recipe.datasets[task.dataset_id]
        if ds.provider != "census" or ds.product != "acs1":
            continue

        if _task_uses_same_year_acs1(
            recipe=recipe,
            task=task,
            project_root=project_root,
        ):
            same_year_usage.setdefault((pipeline_id, task.dataset_id), set()).add(
                task.year,
            )

    for (pipeline_id, dataset_id), years in sorted(same_year_usage.items()):
        findings.append(PreflightFinding(
            severity=Severity.WARNING,
            kind=FindingKind.TEMPORAL_ALIGNMENT,
            message=(
                f"Pipeline '{pipeline_id}': dataset '{dataset_id}' uses "
                f"same-year ACS1 vintage for analysis year(s) {sorted(years)}. "
                "ACS1 data for year Y is published in fall of year Y and is "
                "not available at January-aligned observation dates; use "
                "prior-year ACS1 vintages or document why same-year ACS1 is "
                "intentional."
            ),
            pipeline_id=pipeline_id,
            dataset_id=dataset_id,
            years=sorted(years),
            remediation=Remediation(
                hint=(
                    "For PIT/January-aligned pipelines, use lagged ACS1 "
                    "vintages (for example year_offsets: { acs1_end: -1 } or "
                    "a prior-year static artifact path), or document why "
                    "same-year ACS1 is intentional."
                ),
            ),
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
            generation_ready = (
                result.detail.get("generation_ready", False)
                if result.detail else False
            )
            if can_generate and generation_ready:
                continue
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
            is_msa = (
                transform is not None
                and (transform.from_.type == "msa"
                     or transform.to.type == "msa")
            )
            if is_metro:
                missing_inputs = (
                    result.detail.get("missing_inputs", [])
                    if result.detail else []
                )
                cmd = None
                if transform is not None:
                    metro_ref = (
                        transform.to
                        if transform.to.type == "metro"
                        else transform.from_
                    )
                    metro_definition_version = metro_ref.resolved_metro_definition_version()
                    subset_definition_version = (
                        metro_ref.resolved_metro_subset_definition_version()
                    )
                    if any("msa_county_membership__" in item for item in missing_inputs):
                        cmd = (
                            "hhplab generate msa "
                            f"--definition-version {metro_definition_version}"
                            if metro_definition_version is not None
                            else "hhplab generate msa"
                        )
                    elif any("metro_subset_membership__" in item for item in missing_inputs):
                        cmd = (
                            "hhplab generate metro-universe "
                            f"--definition-version {metro_definition_version} "
                            f"--profile-definition-version {subset_definition_version}"
                            if metro_definition_version is not None
                            and subset_definition_version is not None
                            else "hhplab generate metro-universe"
                        )
                    elif any("metro_coc_membership__" in item or "metro_county_membership__" in item for item in missing_inputs):
                        cmd = "hhplab generate metro"
                    elif any("coc__B" in item for item in missing_inputs):
                        coc_ref = (
                            transform.to
                            if transform.to.type == "coc"
                            else transform.from_
                            if transform.from_.type == "coc"
                            else None
                        )
                        vintage = coc_ref.vintage if coc_ref is not None else None
                        cmd = None if vintage is None else (
                            "hhplab ingest boundaries --source hud_exchange "
                            f"--vintage {vintage}"
                        )
                if missing_inputs:
                    hint = (
                        f"Metro transform '{tid}' can be generated once its "
                        f"source artifacts exist. Missing: {missing_inputs}"
                    )
                else:
                    hint = (
                        f"Metro transform '{tid}' can be generated. "
                        f"Ensure metro definition artifacts exist."
                    )
                remediation = Remediation(hint=hint, command=cmd)
            elif is_msa:
                missing_inputs = (
                    result.detail.get("missing_inputs", [])
                    if result.detail else []
                )
                remediation = _msa_transform_remediation(tid, transform, missing_inputs)
            else:
                remediation = Remediation(
                    hint=f"Generate crosswalk artifacts for transform '{tid}'.",
                    command="hhplab generate xwalks",
                )
            findings.append(PreflightFinding(
                severity=Severity.ERROR,
                kind=FindingKind.MISSING_TRANSFORM,
                message=result.message,
                transform_id=tid,
                remediation=remediation,
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
    distinct_paths_by_dataset: dict[str, set[str]] = {}
    for task in resample_tasks:
        if task.input_path is not None:
            distinct_paths_by_dataset.setdefault(task.dataset_id, set()).add(
                task.input_path
            )

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
        column_types = schema_result.detail.get("column_types", {})
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
            resolved_year_column = (
                year_result.detail.get("year_column")
                if year_result.ok and year_result.detail is not None
                else None
            )
            tf_result = probe_temporal_filter(
                columns,
                filt,
                task.dataset_id,
                year_column=resolved_year_column,
                column_types=column_types,
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
            elif filt.method == "interpolate_to_month":
                tf_data_result = probe_interpolate_to_month_data(
                    full_path,
                    filt,
                    task.dataset_id,
                )
                if not tf_data_result.ok:
                    findings.append(PreflightFinding(
                        severity=Severity.ERROR,
                        kind=FindingKind.TEMPORAL_FILTER,
                        message=(
                            f"Dataset '{task.dataset_id}' ({task.input_path}): "
                            f"{tf_data_result.message}"
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
            distinct_paths=(
                len(distinct_paths_by_dataset.get(task.dataset_id, set()))
                if ds.file_set is not None
                else None
            ),
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


def _check_support_datasets(
    recipe: RecipeV1,
    project_root: Path,
    needed_transforms: set[str],
    universe_years: list[int],
) -> list[PreflightFinding]:
    """Check support-dataset prerequisites for weighted transforms.

    When a transform uses population weighting, the referenced
    population_source dataset must exist and contain the declared
    population_field.  These checks run without loading data.
    """
    findings: list[PreflightFinding] = []

    for tid in sorted(needed_transforms):
        transform = None
        for t in recipe.transforms:
            if t.id == tid:
                transform = t
                break
        if transform is None:
            continue

        reqs = get_weighted_transform_requirements(transform)
        if reqs is None:
            continue

        population_source, population_field = reqs
        probe_results = probe_support_dataset(
            population_source=population_source,
            population_field=population_field,
            transform_id=tid,
            recipe=recipe,
            project_root=project_root,
            years=universe_years,
        )
        for r in probe_results:
            finding_kind = FindingKind.MISSING_SUPPORT_DATASET
            remediation_hint = (
                f"Ensure dataset '{population_source}' is "
                f"available with field '{population_field}' "
                f"for the required years."
            )
            remediation_command = (
                f"hhplab ingest {recipe.datasets[population_source].product}"
                if population_source in recipe.datasets
                and recipe.datasets[population_source].product
                else None
            )
            if (
                r.detail is not None
                and r.detail.get("finding_kind") == FindingKind.DATASET_PROVENANCE.value
            ):
                finding_kind = FindingKind.DATASET_PROVENANCE
                remediation_hint = (
                    "Rebuild the translated ACS tract cache so its provenance "
                    "records the source and target tract vintages."
                )
                remediation_command = r.detail.get("remediation_command")
            findings.append(PreflightFinding(
                severity=Severity.ERROR,
                kind=finding_kind,
                message=r.message,
                transform_id=tid,
                dataset_id=population_source,
                years=(
                    r.detail.get("missing_years")
                    if r.detail else None
                ),
                remediation=Remediation(
                    hint=remediation_hint,
                    command=(
                        str(remediation_command)
                        if remediation_command is not None
                        else None
                    ),
                ),
            ))

    return findings


def _filter_to_year(
    df: pd.DataFrame,
    year_col: str,
    year: int,
) -> pd.DataFrame:
    """Filter a DataFrame to a requested year, tolerating string year values."""
    series = df[year_col]
    if pd.api.types.is_numeric_dtype(series):
        mask = series == year
    else:
        coerced = pd.to_numeric(series, errors="coerce")
        if coerced.notna().any():
            mask = coerced == year
        else:
            mask = series.astype(str) == str(year)
    return df[mask].copy()


def _read_geo_values_for_year(
    *,
    path: Path,
    declared_geo_column: str | None,
    declared_year_column: str | None,
    year: int,
) -> pd.Series | None:
    """Load the relevant geo-ID column, filtered to the requested year when possible."""
    try:
        schema_columns = list(pq.read_schema(path).names)
    except (FileNotFoundError, OSError, ValueError):
        return None

    geo_result = probe_geo_column(schema_columns, declared_geo_column)
    if not geo_result.ok or not geo_result.detail:
        return None
    geo_col = geo_result.detail["geo_column"]

    year_result = probe_year_column(schema_columns, declared_year_column)
    year_col = year_result.detail["year_column"] if year_result.ok and year_result.detail else None

    read_columns = [geo_col]
    if year_col is not None and year_col not in read_columns:
        read_columns.append(year_col)
    try:
        df = pd.read_parquet(path, columns=read_columns)
    except (FileNotFoundError, OSError, ValueError):
        return None

    if year_col is not None and year_col in df.columns:
        df = _filter_to_year(df, year_col, year)
    return df[geo_col]


def _needs_ct_planning_to_legacy_alignment(
    *,
    xwalk_values: pd.Series,
    source_values: pd.Series | None,
) -> bool:
    """Return True when CT planning-region inputs need the legacy-county bridge."""
    if source_values is None:
        return False

    xwalk_has_ct_legacy = xwalk_values.dropna().astype(str).map(
        is_ct_legacy_county_fips,
    ).any()
    source_has_ct_planning = source_values.dropna().astype(str).map(
        is_ct_planning_region_fips,
    ).any()
    return bool(xwalk_has_ct_legacy and source_has_ct_planning)


def _check_ct_county_alignment(
    recipe: RecipeV1,
    project_root: Path,
    pipeline_tasks: list[tuple[str, ResampleTask]],
) -> list[PreflightFinding]:
    """Detect CT legacy/planning mismatches and report the special-case path."""
    findings: list[PreflightFinding] = []
    bridge_status: dict[int, str | None] = {}
    source_events: dict[tuple[str, str, int], set[int]] = {}
    support_events: dict[tuple[str, str, str, int], set[int]] = {}

    def ensure_bridge_ready(legacy_vintage: int) -> str | None:
        cached = bridge_status.get(legacy_vintage)
        if legacy_vintage in bridge_status:
            return cached
        try:
            build_ct_county_planning_region_crosswalk(
                legacy_county_vintage=legacy_vintage,
                planning_region_vintage=CT_PLANNING_REGION_VINTAGE,
            )
        except (FileNotFoundError, OSError, ValueError) as exc:
            bridge_status[legacy_vintage] = (
                "Connecticut county alignment needs the authoritative bridge "
                f"derived from {county_path(legacy_vintage)} and "
                f"{county_path(CT_PLANNING_REGION_VINTAGE)}: {exc}"
            )
        else:
            bridge_status[legacy_vintage] = None
        return bridge_status[legacy_vintage]

    for pipeline_id, task in pipeline_tasks:
        if task.method != "aggregate" or task.transform_id is None:
            continue
        if task.effective_geometry.type != "county":
            continue

        transform_probe = probe_transform_path(task.transform_id, recipe, project_root)
        transform_path = (
            project_root / transform_probe.detail["path"]
            if transform_probe.ok and transform_probe.detail
            else None
        )
        if transform_path is None or not transform_path.exists():
            continue

        try:
            xwalk = pd.read_parquet(transform_path, columns=["county_fips"])
        except (FileNotFoundError, OSError, ValueError, KeyError):
            continue

        legacy_vintage = (
            int(task.effective_geometry.vintage)
            if task.effective_geometry.vintage is not None
            else CT_LEGACY_COUNTY_VINTAGE
        )

        if task.input_path is not None:
            source_series = _read_geo_values_for_year(
                path=project_root / task.input_path,
                declared_geo_column=task.geo_column,
                declared_year_column=task.year_column,
                year=task.year,
            )
            if _needs_ct_planning_to_legacy_alignment(
                xwalk_values=xwalk["county_fips"],
                source_values=source_series,
            ):
                bridge_error = ensure_bridge_ready(legacy_vintage)
                if bridge_error is None:
                    source_events.setdefault(
                        (pipeline_id, task.dataset_id, legacy_vintage),
                        set(),
                    ).add(task.year)
                else:
                    findings.append(PreflightFinding(
                        severity=Severity.ERROR,
                        kind=FindingKind.CT_COUNTY_ALIGNMENT,
                        message=(
                            f"Pipeline '{pipeline_id}': dataset '{task.dataset_id}' "
                            "uses Connecticut planning-region county IDs against "
                            f"legacy county crosswalk '{task.transform_id}' for "
                            f"year {task.year}. {bridge_error}"
                        ),
                        pipeline_id=pipeline_id,
                        dataset_id=task.dataset_id,
                        transform_id=task.transform_id,
                        years=[task.year],
                        remediation=Remediation(
                            hint=(
                                "Materialize the CT planning-region county geometry "
                                "before running this recipe so the bridge can be built."
                            ),
                            command="hhplab ingest tiger --year 2023 --type counties",
                        ),
                    ))

        transform = next((t for t in recipe.transforms if t.id == task.transform_id), None)
        reqs = get_weighted_transform_requirements(transform) if transform is not None else None
        if reqs is None:
            continue

        population_source, _population_field = reqs
        resolved = _resolve_dataset_year(population_source, task.year, recipe)
        if resolved.path is None:
            continue
        support_path = project_root / resolved.path
        if not support_path.exists():
            continue

        support_ds = recipe.datasets.get(population_source)
        if support_ds is None:
            continue
        support_series = _read_geo_values_for_year(
            path=support_path,
            declared_geo_column=support_ds.geo_column,
            declared_year_column=support_ds.year_column,
            year=task.year,
        )
        if _needs_ct_planning_to_legacy_alignment(
            xwalk_values=xwalk["county_fips"],
            source_values=support_series,
        ):
            bridge_error = ensure_bridge_ready(legacy_vintage)
            if bridge_error is None:
                support_events.setdefault(
                    (pipeline_id, task.transform_id, population_source, legacy_vintage),
                    set(),
                ).add(task.year)
            else:
                findings.append(PreflightFinding(
                    severity=Severity.ERROR,
                    kind=FindingKind.CT_COUNTY_ALIGNMENT,
                    message=(
                        f"Pipeline '{pipeline_id}': population_source "
                        f"'{population_source}' for transform '{task.transform_id}' "
                        "uses Connecticut planning-region county IDs against a "
                        f"legacy county crosswalk for year {task.year}. {bridge_error}"
                    ),
                    pipeline_id=pipeline_id,
                    dataset_id=population_source,
                    transform_id=task.transform_id,
                    years=[task.year],
                    remediation=Remediation(
                        hint=(
                            "Materialize the CT planning-region county geometry "
                            "before running this recipe so the bridge can be built."
                        ),
                        command="hhplab ingest tiger --year 2023 --type counties",
                    ),
                ))

    for (pipeline_id, dataset_id, legacy_vintage), years in sorted(source_events.items()):
        findings.append(PreflightFinding(
            severity=Severity.WARNING,
            kind=FindingKind.CT_COUNTY_ALIGNMENT,
            message=(
                f"Pipeline '{pipeline_id}': Connecticut special-case alignment "
                f"will translate planning-region dataset '{dataset_id}' to "
                f"legacy counties for years {sorted(years)} using "
                f"{county_path(legacy_vintage)} and "
                f"{county_path(CT_PLANNING_REGION_VINTAGE)}."
            ),
            pipeline_id=pipeline_id,
            dataset_id=dataset_id,
            years=sorted(years),
        ))

    for key, years in sorted(support_events.items()):
        pipeline_id, transform_id, dataset_id, legacy_vintage = key
        findings.append(PreflightFinding(
            severity=Severity.WARNING,
            kind=FindingKind.CT_COUNTY_ALIGNMENT,
            message=(
                f"Pipeline '{pipeline_id}': Connecticut special-case alignment "
                f"will translate planning-region population_source '{dataset_id}' "
                f"for transform '{transform_id}' to legacy counties for years "
                f"{sorted(years)} using {county_path(legacy_vintage)} and "
                f"{county_path(CT_PLANNING_REGION_VINTAGE)}."
            ),
            pipeline_id=pipeline_id,
            dataset_id=dataset_id,
            transform_id=transform_id,
            years=sorted(years),
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
    report.findings.extend(_check_map_artifacts(recipe, project_root))

    # 2. Resolve plans and collect tasks (before path checks so we
    #    can scope path checking to plan-required dataset-years only)
    report.findings.extend(_check_temporal_alignment_guidance(recipe))

    all_resample_tasks: list[ResampleTask] = []
    pipeline_resample_tasks: list[tuple[str, ResampleTask]] = []
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
                pipeline_resample_tasks.append((pipeline.id, rt))
            all_resample_tasks.extend(plan.resample_tasks)

        except PlannerError as exc:
            err_str = str(exc)
            summary = PipelineSummary(
                pipeline_id=pipeline.id,
                plan_error=err_str,
            )
            report.pipelines.append(summary)

            # Surface planner errors as both PLANNER_ERROR and
            # UNCOVERED_YEARS when they indicate year-coverage gaps,
            # so the gaps manifest includes them.
            report.findings.append(PreflightFinding(
                severity=Severity.ERROR,
                kind=FindingKind.PLANNER_ERROR,
                message=f"Pipeline '{pipeline.id}': {exc}",
                pipeline_id=pipeline.id,
            ))
            if "not covered" in err_str or "no file_set segment" in err_str:
                # Extract dataset_id from common planner error patterns
                ds_id_from_err: str | None = None
                if "Dataset '" in err_str:
                    start = err_str.index("Dataset '") + 9
                    end = err_str.index("'", start)
                    ds_id_from_err = err_str[start:end]

                # Extract the specific missing year from the error
                _year_match = re.search(r"year (\d{4})", err_str)
                missing_years = (
                    [int(_year_match.group(1))] if _year_match
                    else universe_years
                )

                report.findings.append(PreflightFinding(
                    severity=Severity.ERROR,
                    kind=FindingKind.UNCOVERED_YEARS,
                    message=f"Pipeline '{pipeline.id}': {exc}",
                    pipeline_id=pipeline.id,
                    dataset_id=ds_id_from_err,
                    years=missing_years,
                    remediation=Remediation(
                        hint=(
                            f"Year(s) {missing_years} not covered by "
                            f"dataset '{ds_id_from_err or '?'}'. "
                            f"Extend dataset year coverage or narrow "
                            f"the recipe universe "
                            f"({min(universe_years)}-{max(universe_years)})."
                        ),
                    ),
                ))

    report.findings.extend(
        _check_acs1_temporal_alignment_guidance(
            recipe,
            project_root,
            pipeline_resample_tasks,
        ),
    )

    # 3. Dataset path checks (plan-scoped: only checks paths required
    #    by the resolved execution plan)
    report.findings.extend(
        _check_dataset_paths(recipe, project_root, all_resample_tasks),
    )

    # 4. Dataset provenance checks for translated ACS tract caches
    report.findings.extend(
        _check_dataset_provenance(recipe, project_root, all_resample_tasks),
    )

    # 5. Transform artifact checks
    report.findings.extend(
        _check_transforms(recipe, project_root, needed_transforms),
    )

    # 6. Dataset schema probes
    report.findings.extend(
        _check_dataset_schemas(recipe, project_root, all_resample_tasks),
    )

    # 7. Support-dataset probes for weighted transforms
    report.findings.extend(
        _check_support_datasets(
            recipe, project_root, needed_transforms, universe_years,
        ),
    )

    # 8. Connecticut county-transition detection and bridge readiness
    report.findings.extend(
        _check_ct_county_alignment(recipe, project_root, pipeline_resample_tasks),
    )

    return report
