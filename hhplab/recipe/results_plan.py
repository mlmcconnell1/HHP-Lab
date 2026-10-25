"""Pure planning and static preflight for RecipeV2 result analyses.

This module does not build panels or inspect data. It resolves the recipe's
declared identities, variants, sample requirements, and static references into
deterministic mappings for the sample resolver and result workflow.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any

from pydantic import TypeAdapter

from hhplab.recipe.executor.core import ExecutorError
from hhplab.recipe.executor.inputs import SiteUniverseResolution, resolve_site_universe_source
from hhplab.recipe.preflight.models import (
    FindingKind,
    PreflightFinding,
    Remediation,
    Severity,
)
from hhplab.recipe.schema_common import expand_year_spec
from hhplab.recipe.schema_results import (
    AnalysisBase,
    AnalysisSpec,
    CorrelationAnalysis,
    DescriptiveAnalysis,
    FindingSpec,
    LinearIdentityDiagnostic,
    RecipeV2,
    RegressionAnalysis,
    SampleGroupReference,
    SampleGroupSpec,
    SampleSpec,
    VariantSpec,
)
from hhplab.schema.results import canonical_bytes, component_hashes

_ANALYSIS_ADAPTER = TypeAdapter(AnalysisSpec)
_SUPPORTED_STATS: Mapping[str, frozenset[str]] = {
    "regression": frozenset({"n", "n_clusters", "r2", "r2_within"}),
    "correlation": frozenset({"n", "r"}),
}


def _deep_freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({key: _deep_freeze(item) for key, item in value.items()})
    if isinstance(value, Sequence) and not isinstance(value, str | bytes):
        return tuple(_deep_freeze(item) for item in value)
    return value


def _deep_thaw(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _deep_thaw(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, str | bytes):
        return [_deep_thaw(item) for item in value]
    return value


@dataclass(frozen=True)
class PanelPlan:
    """A named panel artifact with a deterministic build identity."""

    name: str
    pipeline_id: str | None
    output: str
    geo_key: str | None
    time_key: str | None
    panel_identity: str
    build_spec_hash: str
    declared_vintages: Mapping[str, str]
    source_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "declared_vintages", _deep_freeze(self.declared_vintages))

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "pipeline_id": self.pipeline_id,
            "output": self.output,
            "geo_key": self.geo_key,
            "time_key": self.time_key,
            "panel_identity": self.panel_identity,
            "build_spec_hash": self.build_spec_hash,
            "declared_vintages": _deep_thaw(self.declared_vintages),
            "source_ids": list(self.source_ids),
        }


@dataclass(frozen=True)
class AnalysisVariantPlan:
    """Normalized analysis input for result execution or sample resolution."""

    analysis_id: str
    variant_id: str
    result_key: str
    panel_name: str
    panel_identity: str
    geo_key: str
    time_key: str
    method: str
    effective_spec: Mapping[str, Any]
    base_term_by_effective_term: Mapping[str, str]
    required_columns: tuple[str, ...]
    numeric_columns: tuple[str, ...]
    weight_columns: tuple[str, ...]
    predicate_columns: tuple[str, ...]
    fixed_effects: tuple[str, ...]
    drop_singletons: bool
    min_obs_per_geography: int
    sample: Mapping[str, Any]
    sample_group_id: str | None
    group_context_key: str | None
    sample_source: str
    base_membership_key: str | None
    component_hashes: Mapping[str, str]

    def __post_init__(self) -> None:
        for field_name in (
            "effective_spec",
            "base_term_by_effective_term",
            "sample",
            "component_hashes",
        ):
            object.__setattr__(self, field_name, _deep_freeze(getattr(self, field_name)))

    def to_dict(self) -> dict[str, Any]:
        return {
            "analysis_id": self.analysis_id,
            "variant_id": self.variant_id,
            "result_key": self.result_key,
            "panel_name": self.panel_name,
            "panel_identity": self.panel_identity,
            "geo_key": self.geo_key,
            "time_key": self.time_key,
            "method": self.method,
            "effective_spec": _deep_thaw(self.effective_spec),
            "base_term_by_effective_term": _deep_thaw(self.base_term_by_effective_term),
            "required_columns": list(self.required_columns),
            "numeric_columns": list(self.numeric_columns),
            "weight_columns": list(self.weight_columns),
            "predicate_columns": list(self.predicate_columns),
            "fixed_effects": list(self.fixed_effects),
            "drop_singletons": self.drop_singletons,
            "min_obs_per_geography": self.min_obs_per_geography,
            "sample": _deep_thaw(self.sample),
            "sample_group_id": self.sample_group_id,
            "group_context_key": self.group_context_key,
            "sample_source": self.sample_source,
            "base_membership_key": self.base_membership_key,
            "component_hashes": _deep_thaw(self.component_hashes),
        }


@dataclass(frozen=True)
class GroupContext:
    """One resolved shared-sample context for a group/variant/panel."""

    group_id: str
    variant_id: str
    panel_name: str
    panel_identity: str
    member_analysis_ids: tuple[str, ...]
    sample: Mapping[str, Any]
    required_columns: tuple[str, ...]
    numeric_columns: tuple[str, ...]
    weight_columns: tuple[str, ...]
    predicate_columns: tuple[str, ...]
    fixed_effects: tuple[str, ...]
    drop_singletons: bool
    min_obs_per_geography: int
    sample_source: str
    base_membership_key: str | None

    def __post_init__(self) -> None:
        object.__setattr__(self, "sample", _deep_freeze(self.sample))

    @property
    def key(self) -> str:
        return f"{self.group_id}@{self.variant_id}:{self.panel_identity}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "group_id": self.group_id,
            "variant_id": self.variant_id,
            "panel_name": self.panel_name,
            "panel_identity": self.panel_identity,
            "member_analysis_ids": list(self.member_analysis_ids),
            "sample": _deep_thaw(self.sample),
            "required_columns": list(self.required_columns),
            "numeric_columns": list(self.numeric_columns),
            "weight_columns": list(self.weight_columns),
            "predicate_columns": list(self.predicate_columns),
            "fixed_effects": list(self.fixed_effects),
            "drop_singletons": self.drop_singletons,
            "min_obs_per_geography": self.min_obs_per_geography,
            "sample_source": self.sample_source,
            "base_membership_key": self.base_membership_key,
        }


@dataclass(frozen=True)
class ResolvedReference:
    """An external term/stat reference resolved to a result key."""

    owner_id: str
    owner_kind: str
    analysis_id: str
    result_key: str
    base_term: str | None = None
    term: str | None = None
    stat: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "owner_id": self.owner_id,
            "owner_kind": self.owner_kind,
            "analysis_id": self.analysis_id,
            "result_key": self.result_key,
            "base_term": self.base_term,
            "term": self.term,
            "stat": self.stat,
        }


@dataclass(frozen=True)
class ResultsPlan:
    """Deterministic V2 execution plan plus the original source bytes."""

    recipe_id: str
    original_recipe_bytes: bytes
    canonical_plan: bytes
    plan_sha256: str
    parameters: Mapping[str, Any]
    panels: tuple[PanelPlan, ...]
    analyses: tuple[AnalysisVariantPlan, ...]
    group_contexts: tuple[GroupContext, ...]
    references: tuple[ResolvedReference, ...]
    analysis_dependencies: Mapping[str, tuple[str, ...]]
    component_hashes: Mapping[str, str]
    preflight_findings: tuple[PreflightFinding, ...]
    site_universe: Mapping[str, Any] | None = None
    site_year_boundary_compatibility: Any | None = None
    annotations: tuple[Mapping[str, Any], ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "parameters", _deep_freeze(self.parameters))
        object.__setattr__(self, "analysis_dependencies", _deep_freeze(self.analysis_dependencies))
        object.__setattr__(self, "component_hashes", _deep_freeze(self.component_hashes))
        if self.site_universe is not None:
            object.__setattr__(self, "site_universe", _deep_freeze(self.site_universe))
        object.__setattr__(self, "annotations", _deep_freeze(self.annotations))

    @property
    def is_ready(self) -> bool:
        return not any(finding.is_blocking for finding in self.preflight_findings)

    def to_dict(self) -> dict[str, Any]:
        result = {
            "recipe_id": self.recipe_id,
            "parameters": _deep_thaw(self.parameters),
            "panels": [panel.to_dict() for panel in self.panels],
            "analyses": [analysis.to_dict() for analysis in self.analyses],
            "group_contexts": [context.to_dict() for context in self.group_contexts],
            "references": [reference.to_dict() for reference in self.references],
            "analysis_dependencies": {
                key: list(value) for key, value in sorted(self.analysis_dependencies.items())
            },
            "component_hashes": _deep_thaw(self.component_hashes),
            "annotations": [_deep_thaw(item) for item in self.annotations],
            "preflight_findings": [finding.to_dict() for finding in self.preflight_findings],
        }
        if self.site_universe is not None:
            result["site_universe"] = _deep_thaw(self.site_universe)
        return result


class ResultsPlanningError(ValueError):
    """A static V2 planning error with existing actionable preflight findings."""

    def __init__(self, findings: Sequence[PreflightFinding]):
        self.findings = tuple(findings)
        super().__init__("; ".join(finding.message for finding in self.findings))


def plan_results(
    recipe: RecipeV2,
    *,
    original_recipe_bytes: bytes,
    site_universe_resolution: SiteUniverseResolution | None = None,
    boundary_evidence_payload: Mapping[str, object] | None = None,
) -> ResultsPlan:
    """Resolve a RecipeV2 without reading panel data or performing a build."""
    if not isinstance(recipe, RecipeV2):
        raise TypeError("plan_results requires a validated RecipeV2")
    if not isinstance(original_recipe_bytes, bytes):
        raise TypeError("original_recipe_bytes must be bytes to preserve exact source content")

    unresolved = recipe.unresolved_values()
    if unresolved:
        raise ResultsPlanningError(
            tuple(
                _finding(
                    f"unresolved_value at {path}: {description}",
                    hint="Resolve the value in the recipe before running results preflight.",
                )
                for path, description in unresolved
            )
        )

    universe_resolution = None
    if recipe.site_universe is not None:
        if site_universe_resolution is not None:
            universe_resolution = site_universe_resolution
            expected_membership_identity = hashlib.sha256(
                canonical_bytes(
                    {
                        "schema_version": "msa_coc_sites/1",
                        "artifact_id": universe_resolution.artifact_id,
                        "threshold": universe_resolution.threshold,
                        "site_key": "msa_id",
                        "memberships": [list(pair) for pair in universe_resolution.memberships],
                    }
                )
            ).hexdigest()
            membership_msas = tuple(
                sorted({msa_id for msa_id, _ in universe_resolution.memberships})
            )
            membership_cocs = tuple(coc_id for _, coc_id in universe_resolution.memberships)
            if (
                universe_resolution.artifact_id != recipe.site_universe.source.artifact_id
                or universe_resolution.threshold != float(recipe.site_universe.threshold)
                or universe_resolution.schema_version != "msa_coc_sites/1"
                or universe_resolution.membership_identity != expected_membership_identity
                or universe_resolution.selected_msa_ids != membership_msas
                or len(membership_cocs) != len(set(membership_cocs))
                or universe_resolution.eligible_site_count != len(membership_msas)
                or universe_resolution.excluded_site_count < 0
                or len(universe_resolution.fingerprints) != 3
                or any(
                    len(fingerprint) != 64
                    or any(char not in "0123456789abcdef" for char in fingerprint)
                    for fingerprint in universe_resolution.fingerprints
                )
            ):
                _raise(
                    "Injected site-universe resolution does not match the recipe artifact "
                    "and threshold",
                    hint=(
                        "Supply the immutable validated resolution for this exact "
                        "site_universe reference."
                    ),
                )
        else:
            try:
                universe_resolution = resolve_site_universe_source(
                    source=recipe.site_universe.source,
                    threshold=float(recipe.site_universe.threshold),
                    source_base_dir=recipe.source_base_dir,
                )
            except (ExecutorError, OSError, ValueError) as exc:
                _raise(
                    str(exc),
                    hint="Correct the artifact locator or use matching canonical producer outputs.",
                )
    elif site_universe_resolution is not None or boundary_evidence_payload is not None:
        _raise(
            "Injected site-universe inputs require a recipe site_universe reference",
            hint="Use a V2 recipe that declares the site-universe source and site-year panel.",
        )
    panels = _resolve_panels(recipe, universe_resolution=universe_resolution)
    panel_by_name = {panel.name: panel for panel in panels}
    parameter_values = _resolved_parameters(recipe)
    variants = {variant.id: variant for variant in recipe.variants}
    group_members = _group_members(recipe)
    base_plan_by_analysis: dict[str, AnalysisVariantPlan] = {}

    _validate_group_variants(recipe, group_members, panel_by_name)
    analysis_plans: list[AnalysisVariantPlan] = []
    contexts: list[GroupContext] = []
    plan_by_key: dict[str, AnalysisVariantPlan] = {}

    for analysis in recipe.analyses:
        base_plan = _plan_analysis(
            analysis,
            variant_id="base",
            variant=None,
            recipe=recipe,
            panel_by_name=panel_by_name,
            parameter_values=parameter_values,
            group_members=group_members,
        )
        analysis_plans.append(base_plan)
        plan_by_key[base_plan.result_key] = base_plan
        base_plan_by_analysis[analysis.id] = base_plan

    base_contexts = _build_group_contexts(
        recipe,
        analysis_plans,
        group_members,
        variant_id="base",
        base_contexts={},
    )
    contexts.extend(base_contexts)
    base_group_contexts = {context.group_id: context for context in base_contexts}

    for variant in recipe.variants:
        affected = [analysis for analysis in recipe.analyses if analysis.id in variant.applies_to]
        variant_plans: list[AnalysisVariantPlan] = []
        for analysis in affected:
            planned = _plan_analysis(
                analysis,
                variant_id=variant.id,
                variant=variant,
                recipe=recipe,
                panel_by_name=panel_by_name,
                parameter_values=parameter_values,
                group_members=group_members,
                base_plan=base_plan_by_analysis[analysis.id],
            )
            variant_plans.append(planned)

        variant_contexts = _build_group_contexts(
            recipe,
            variant_plans,
            group_members,
            variant_id=variant.id,
            base_contexts=base_group_contexts,
            all_recipe_analyses=recipe.analyses,
        )
        contexts.extend(variant_contexts)
        group_context_by_id = {context.group_id: context for context in variant_contexts}

        # Attach group-level membership requirements and exact base reuse.
        for planned in variant_plans:
            if planned.sample_group_id is not None:
                context = group_context_by_id[planned.sample_group_id]
                planned = _attach_group_context(planned, context)
            analysis_plans.append(planned)
            plan_by_key[planned.result_key] = planned

    analysis_plans = _attach_base_group_contexts(analysis_plans, base_group_contexts)
    plan_by_key = {plan.result_key: plan for plan in analysis_plans}

    references = _resolve_references(recipe, plan_by_key, variants)
    dependencies = _build_dependencies(recipe, references, plan_by_key)
    components = _component_hashes(recipe, panels, analysis_plans, contexts)
    annotation_records = [
        spec.model_dump(mode="json", by_alias=True, exclude_none=True)
        for spec in recipe.annotations
    ]
    findings = _substitution_warnings(recipe)
    site_universe_data: dict[str, Any] | None = None
    site_year_boundary_compatibility = None
    if universe_resolution is not None:
        if universe_resolution.eligible_site_count == 0:
            site_universe_data = universe_resolution.to_dict()
            site_universe_data["requested_years"] = expand_year_spec(recipe.universe)
        else:
            (
                site_universe_data,
                site_year_boundary_compatibility,
            ) = _resolve_site_year_contract(
                recipe,
                universe_resolution,
                boundary_evidence_payload=boundary_evidence_payload,
            )
        site_universe_hash_data = _site_universe_hash_projection(site_universe_data)
        components["site_universe"] = hashlib.sha256(
            canonical_bytes(site_universe_hash_data)
        ).hexdigest()
        if universe_resolution.eligible_site_count == 0:
            findings.append(
                _finding(
                    "site_universe selects zero eligible MSA sites at the requested threshold",
                    hint=(
                        "Choose a producer artifact and explicit threshold with at least one "
                        "eligible site."
                    ),
                )
            )
        elif site_universe_data is not None:
            for record in site_universe_data["boundary_evidence"]["validation"]["records"]:
                if record["status"] in {"unknown", "incompatible"}:
                    findings.append(
                        _warning(
                            "site-year boundary status "
                            f"{record['status']} for MSA {record['msa_id']}, "
                            f"CoC {record['frozen_coc_id']}, panel "
                            f"{record['source_panel']}, year {record['year']}",
                            hint=(
                                "Add reviewed compatibility evidence for every selected "
                                "site, panel, and requested year."
                            ),
                        )
                    )
    payload = {
        "recipe_id": recipe.id,
        "panels": [panel.to_dict() for panel in panels],
        "parameters": parameter_values,
        "analyses": [analysis.to_dict() for analysis in analysis_plans],
        "group_contexts": [context.to_dict() for context in contexts],
        "references": [reference.to_dict() for reference in references],
        "analysis_dependencies": {key: list(value) for key, value in sorted(dependencies.items())},
        "component_hashes": components,
        "annotations": annotation_records,
    }
    if site_universe_data is not None:
        payload["site_universe"] = site_universe_hash_data
    canonical = canonical_bytes(payload)
    plan_sha256 = hashlib.sha256(canonical).hexdigest()
    return ResultsPlan(
        recipe_id=recipe.id,
        original_recipe_bytes=original_recipe_bytes,
        canonical_plan=canonical,
        plan_sha256=plan_sha256,
        parameters=parameter_values,
        panels=tuple(panels),
        analyses=tuple(analysis_plans),
        group_contexts=tuple(contexts),
        references=tuple(references),
        analysis_dependencies=dependencies,
        component_hashes=components,
        preflight_findings=tuple(findings),
        site_universe=site_universe_data,
        site_year_boundary_compatibility=site_year_boundary_compatibility,
        annotations=tuple(annotation_records),
    )


def _finding(message: str, *, hint: str) -> PreflightFinding:
    return PreflightFinding(
        severity=Severity.ERROR,
        kind=FindingKind.PLANNER_ERROR,
        message=message,
        remediation=Remediation(hint=hint),
    )


def _warning(message: str, *, hint: str) -> PreflightFinding:
    return PreflightFinding(
        severity=Severity.WARNING,
        kind=FindingKind.PLANNER_ERROR,
        message=message,
        remediation=Remediation(hint=hint),
    )


def _substitution_warnings(recipe: RecipeV2) -> list[PreflightFinding]:
    analyses = {analysis.id: analysis for analysis in recipe.analyses}
    warnings: list[PreflightFinding] = []
    for variant in recipe.variants:
        for analysis_id in variant.applies_to:
            analysis = analyses[analysis_id]
            referenced = _all_analysis_columns(analysis)
            if isinstance(analysis.sample, SampleGroupReference):
                referenced.update(
                    _sample_columns(recipe.sample_groups[analysis.sample.group].sample)
                )
        unmatched = sorted(set(variant.substitute) - referenced)
        if unmatched:
            warnings.append(
                _warning(
                    f"variant {variant.id!r} substitutions match no referenced columns "
                    f"in analysis {analysis_id!r}: {unmatched}",
                    hint="Remove unused substitutions or update the targeted analysis columns.",
                )
            )
    return warnings


def _raise(message: str, *, hint: str) -> None:
    raise ResultsPlanningError((_finding(message, hint=hint),))


def _json_model(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json", by_alias=True, exclude_none=True)
    if isinstance(value, Mapping):
        return {str(key): _json_model(item) for key, item in value.items()}
    if isinstance(value, tuple | list):
        return [_json_model(item) for item in value]
    return value


def _resolve_panels(
    recipe: RecipeV2,
    *,
    universe_resolution: SiteUniverseResolution | None = None,
) -> list[PanelPlan]:
    targets = {target.id: target for target in recipe.targets}
    pipelines = {pipeline.id: pipeline for pipeline in recipe.pipelines}
    result: list[PanelPlan] = []
    for name, binding in recipe.panels.items():
        if binding.site_year is not None:
            if universe_resolution is None:
                _raise(
                    f"derived panel {name!r} requires site_universe resolution",
                    hint="Declare a valid top-level site_universe source and threshold.",
                )
            binding_data = _json_model(binding.site_year)
            build_hash = hashlib.sha256(canonical_bytes(binding_data)).hexdigest()
            identity = hashlib.sha256(
                canonical_bytes(
                    {
                        "build_spec_hash": build_hash,
                        "membership_identity": universe_resolution.membership_identity,
                        "site_key": recipe.site_universe.site_key,
                        "boundary_vintage": universe_resolution.boundary_vintage,
                    }
                )
            ).hexdigest()
            result.append(
                PanelPlan(
                    name=name,
                    pipeline_id=None,
                    output="site_year",
                    geo_key=binding.geo_key,
                    time_key=binding.time_key,
                    panel_identity=identity,
                    build_spec_hash=build_hash,
                    declared_vintages={
                        "coc_boundary_vintage": universe_resolution.boundary_vintage
                    },
                    source_ids=(universe_resolution.artifact_id,),
                )
            )
            continue
        ref = binding.ref
        if not hasattr(ref, "pipeline"):
            _raise(
                f"panel {name!r} has an unresolved build reference",
                hint="Set panels.<name>.ref to an existing pipeline panel output.",
            )
        pipeline = pipelines.get(ref.pipeline)
        if pipeline is None:
            _raise(
                f"panel {name!r} references unknown pipeline {ref.pipeline!r}",
                hint="Use a pipeline ID declared in the V1 build sections.",
            )
        target = targets[pipeline.target]
        pipeline_data = _json_model(pipeline)
        target_data = _json_model(target)
        referenced_datasets = _referenced_dataset_ids(pipeline_data)
        transform_by_id = {item.id: item for item in recipe.transforms}
        transform_ids = _referenced_transform_ids(pipeline_data)
        transforms = {
            transform_id: _json_model(transform_by_id[transform_id])
            for transform_id in sorted(transform_ids)
            if transform_id in transform_by_id
        }
        referenced_datasets.update(_referenced_dataset_ids(transforms))
        dataset_data = {
            dataset_id: _json_model(recipe.datasets[dataset_id])
            for dataset_id in sorted(referenced_datasets)
            if dataset_id in recipe.datasets
        }
        build_spec = {
            "universe": _json_model(recipe.universe),
            "pipeline": pipeline_data,
            "target": target_data,
            "datasets": dataset_data,
            "filters": {
                dataset_id: _json_model(recipe.filters[dataset_id])
                for dataset_id in sorted(referenced_datasets)
                if dataset_id in recipe.filters
            },
            "transforms": transforms,
            "vintage_sets": _json_model(recipe.vintage_sets),
            "validation": _json_model(recipe.validation),
        }
        build_hash = hashlib.sha256(canonical_bytes(build_spec)).hexdigest()
        identity = hashlib.sha256(
            canonical_bytes(
                {
                    "build_spec_hash": build_hash,
                    "output": ref.output,
                    "geo_key": binding.geo_key,
                    "time_key": binding.time_key,
                }
            )
        ).hexdigest()
        vintages = {
            dataset_id: _dataset_vintage(recipe.datasets[dataset_id])
            for dataset_id in sorted(referenced_datasets)
            if dataset_id in recipe.datasets
        }
        result.append(
            PanelPlan(
                name=name,
                pipeline_id=ref.pipeline,
                output=ref.output,
                geo_key=binding.geo_key,
                time_key=binding.time_key,
                panel_identity=identity,
                build_spec_hash=build_hash,
                declared_vintages=vintages,
                source_ids=tuple(
                    sorted(
                        {
                            _declared_source_id(recipe.datasets[dataset_id])
                            for dataset_id in referenced_datasets
                            if dataset_id in recipe.datasets
                        }
                    )
                ),
            )
        )
    return result


def _resolve_site_year_contract(
    recipe: RecipeV2,
    resolution: SiteUniverseResolution,
    *,
    boundary_evidence_payload: Mapping[str, object] | None = None,
) -> tuple[dict[str, Any], Any]:
    """Bind the frozen membership identity to explicit source vintages and evidence."""
    from hhplab.geographies.coc.coc_validate import (
        BoundaryCompatibilityError,
        validate_coc_boundary_compatibility,
    )

    derived = next(
        binding.site_year for binding in recipe.panels.values() if binding.site_year is not None
    )
    assert derived is not None
    years = expand_year_spec(recipe.universe)
    source_vintages: dict[tuple[str, int], str | int] = {}
    pipelines = {pipeline.id: pipeline for pipeline in recipe.pipelines}
    targets = {target.id: target for target in recipe.targets}
    for source_binding in (derived.outcomes.pit, derived.outcomes.hic):
        source_panel = recipe.panels[source_binding.panel]
        assert source_panel.ref is not None and hasattr(source_panel.ref, "pipeline")
        pipeline = pipelines[source_panel.ref.pipeline]
        target = targets[pipeline.target]
        vintage = target.geometry.vintage
        if vintage is None:
            _raise(
                f"site_year source panel {source_binding.panel!r} does not declare a "
                "CoC boundary vintage",
                hint=(
                    "Set the source target geometry vintage so boundary evidence can be "
                    "checked against it."
                ),
            )
        for year in years:
            source_vintages[(source_binding.panel, year)] = vintage

    evidence_path_text = derived.boundary_evidence.path
    if boundary_evidence_payload is None:
        evidence_path = Path(evidence_path_text).expanduser()
        if not evidence_path.is_absolute():
            if recipe.source_base_dir is None:
                _raise(
                    "Relative boundary evidence path requires a recipe source directory",
                    hint=(
                        "Load the recipe from a file or supply source_base_dir for an "
                        "in-memory recipe."
                    ),
                )
            evidence_path = recipe.source_base_dir / evidence_path
        evidence_path = evidence_path.resolve()
        try:
            evidence_payload = json.loads(evidence_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            _raise(
                f"Invalid site-year boundary evidence {evidence_path}: {exc}",
                hint="Provide a readable versioned boundary evidence JSON artifact.",
            )
        evidence_path_text = str(evidence_path)
    else:
        evidence_payload = dict(boundary_evidence_payload)
    try:
        validation = validate_coc_boundary_compatibility(
            evidence_payload,
            artifact_id=resolution.artifact_id,
            threshold=resolution.threshold,
            membership_identity=resolution.membership_identity,
            frozen_boundary_vintage=resolution.boundary_vintage,
            frozen_memberships=resolution.memberships,
            requested_years=years,
            source_panel_vintages=source_vintages,
        )
    except BoundaryCompatibilityError as exc:
        _raise(
            f"Invalid site-year boundary evidence {evidence_path_text}: {exc}",
            hint=(
                "Provide versioned compatibility evidence bound to this exact artifact, "
                "threshold, membership, and source vintage."
            ),
        )
    payload = resolution.to_dict()
    payload["boundary_evidence"] = {
        "path": evidence_path_text,
        "fingerprint": validation.evidence_fingerprint,
        "validation": asdict(validation),
    }
    payload["source_panel_vintages"] = {
        f"{panel}:{year}": vintage
        for (panel, year), vintage in sorted(source_vintages.items())
    }
    payload["requested_years"] = years
    return payload, validation


def _site_universe_hash_projection(value: Mapping[str, Any]) -> dict[str, Any]:
    """Keep scientific identity content-based while leaving paths as routing metadata."""
    projected = dict(value)
    projected.pop("paths", None)
    projected.pop("manifest_fingerprint", None)
    boundary_evidence = dict(projected.get("boundary_evidence", {}))
    boundary_evidence.pop("path", None)
    projected["boundary_evidence"] = boundary_evidence
    return projected


def _referenced_dataset_ids(value: Any) -> set[str]:
    found: set[str] = set()
    dataset_fields = {"dataset", "datasets", "source_dataset", "support_dataset"}
    if isinstance(value, Mapping):
        for key, item in value.items():
            if key in dataset_fields and isinstance(item, str):
                found.add(item)
            elif key in dataset_fields and isinstance(item, Sequence):
                found.update(candidate for candidate in item if isinstance(candidate, str))
            else:
                found.update(_referenced_dataset_ids(item))
    elif isinstance(value, Sequence) and not isinstance(value, str | bytes):
        for item in value:
            found.update(_referenced_dataset_ids(item))
    return found


def _referenced_transform_ids(pipeline: Mapping[str, Any]) -> set[str]:
    found: set[str] = set()

    def visit(value: Any, key: str | None = None) -> None:
        if isinstance(value, Mapping):
            for child_key, item in value.items():
                if child_key == "transforms" and isinstance(item, Sequence):
                    found.update(candidate for candidate in item if isinstance(candidate, str))
                elif child_key == "via" and isinstance(item, str) and item != "auto":
                    found.add(item)
                else:
                    visit(item, child_key)
        elif isinstance(value, Sequence) and not isinstance(value, str | bytes):
            for item in value:
                visit(item, key)

    visit(pipeline)
    return found


def _dataset_vintage(dataset: Any) -> str:
    value = _json_model(dataset)
    return canonical_bytes(value).decode("utf-8")


def _declared_source_id(dataset: Any) -> str:
    provider = getattr(dataset, "provider", None)
    product = getattr(dataset, "product", None)
    return f"{provider}/{product}"


def _resolved_parameters(recipe: RecipeV2) -> dict[str, Any]:
    return {name: _json_model(value) for name, value in sorted(recipe.parameters.items())}


def _group_members(recipe: RecipeV2) -> dict[str, tuple[str, ...]]:
    members: dict[str, list[str]] = {group_id: [] for group_id in recipe.sample_groups}
    for analysis in recipe.analyses:
        if isinstance(analysis.sample, SampleGroupReference):
            members[analysis.sample.group].append(analysis.id)
    return {group_id: tuple(ids) for group_id, ids in members.items()}


def _sample_group_id(analysis: AnalysisBase) -> str | None:
    return analysis.sample.group if isinstance(analysis.sample, SampleGroupReference) else None


def _analysis_data(analysis: AnalysisSpec) -> dict[str, Any]:
    return _json_model(analysis)


def _set_path(data: dict[str, Any], path: str, value: Any) -> None:
    parts = path.split(".")
    current = data
    for part in parts[:-1]:
        child = current.get(part)
        if not isinstance(child, dict):
            _raise(
                f"variant override {path!r} does not address an object in the analysis",
                hint="Use an allowed override path whose parent exists in the base analysis.",
            )
        current = child
    current[parts[-1]] = _json_model(value)


def _apply_overrides(
    analysis: AnalysisSpec,
    variant: VariantSpec | None,
    *,
    sample_group_id: str | None,
) -> tuple[AnalysisSpec, dict[str, Any]]:
    data = _analysis_data(analysis)
    if variant is None:
        return analysis, data
    for path, value in sorted(variant.override.items()):
        if sample_group_id is not None and (path == "sample" or path.startswith("sample.")):
            continue
        _set_path(data, path, value)
    try:
        effective = _ANALYSIS_ADAPTER.validate_python(data)
    except Exception as exc:
        _raise(
            f"variant {variant.id!r} makes analysis {analysis.id!r} invalid: {exc}",
            hint="Adjust the override to preserve the analysis method's closed schema.",
        )
    return effective, _analysis_data(effective)


def _apply_substitutions(
    analysis: AnalysisSpec,
    data: dict[str, Any],
    substitutions: Mapping[str, str],
) -> tuple[dict[str, Any], dict[str, str]]:
    base_terms = sorted(_all_analysis_columns(analysis))
    mapped_terms = [substitutions.get(term, term) for term in base_terms]
    collisions: dict[str, list[str]] = {}
    for before, after in zip(base_terms, mapped_terms, strict=True):
        collisions.setdefault(after, []).append(before)
    collided = {after: before for after, before in collisions.items() if len(set(before)) > 1}
    if collided:
        _raise(
            f"substitution collision in analysis {analysis.id!r}: {collided}",
            hint="Use distinct substitution destinations that do not collide with untouched terms.",
        )
    effective_to_base = {
        after: before for before, after in zip(base_terms, mapped_terms, strict=True)
    }

    def substitute(value: Any) -> Any:
        if isinstance(value, str):
            return substitutions.get(value, value)
        if isinstance(value, list):
            return [substitute(item) for item in value]
        if isinstance(value, dict):
            return {key: substitute(item) for key, item in value.items()}
        return value

    # Only declared column-bearing analysis fields are rewritten. IDs, panel
    # names, statistic names, and report labels remain literal.
    fields = {"outcome", "predictors", "controls", "weights", "fixed_effects", "columns", "by"}
    for field in fields:
        if field in data:
            data[field] = substitute(data[field])
    if "inference" in data and isinstance(data["inference"], dict):
        cluster_by = data["inference"].get("cluster_by")
        if isinstance(cluster_by, str):
            data["inference"]["cluster_by"] = substitutions.get(cluster_by, cluster_by)
    sample = data.get("sample")
    if isinstance(sample, dict) and "require" in sample:
        sample["require"] = substitute(sample["require"])
    if isinstance(sample, dict) and isinstance(sample.get("geographies"), dict):
        exclusion = sample["geographies"].get("exclude")
        if isinstance(exclusion, dict) and isinstance(exclusion.get("column"), str):
            exclusion["column"] = substitutions.get(exclusion["column"], exclusion["column"])
    return data, effective_to_base


def _term_columns(analysis: AnalysisSpec) -> list[str]:
    if isinstance(analysis, RegressionAnalysis):
        return [analysis.outcome, *analysis.predictors, *analysis.controls]
    if isinstance(analysis, (DescriptiveAnalysis, CorrelationAnalysis)):
        return list(analysis.columns)
    return []


def _all_analysis_columns(analysis: AnalysisSpec) -> set[str]:
    """Return columns that can affect an analysis result or its sample membership."""
    data = _analysis_data(analysis)
    columns = set(_term_columns(analysis))
    for field in ("weights", "fixed_effects", "by"):
        columns.update(_column_list(data.get(field)))
    inference = data.get("inference")
    if isinstance(inference, dict):
        columns.update(_column_list(inference.get("cluster_by")))
    sample = data.get("sample")
    if isinstance(sample, dict):
        columns.update(_column_list(sample.get("require")))
        geography = sample.get("geographies")
        if isinstance(geography, dict):
            exclusion = geography.get("exclude")
            if isinstance(exclusion, dict):
                columns.update(_column_list(exclusion.get("column")))
    return columns


def _map_terms_to_base(analysis: AnalysisSpec, substitutions: Mapping[str, str]) -> dict[str, str]:
    return {substitutions.get(term, term): term for term in _term_columns(analysis)}


def _sample_group_override(
    variant: VariantSpec | None,
    group_spec: SampleGroupSpec,
) -> SampleSpec:
    sample_data = _json_model(group_spec.sample)
    if variant is None:
        return group_spec.sample
    for path, value in sorted(variant.override.items()):
        if path == "sample":
            replacement = _json_model(value)
            if not isinstance(replacement, dict):
                _raise(
                    f"variant {variant.id!r} sample override must be an object",
                    hint="Set the group sample override to a closed sample object.",
                )
            sample_data = replacement
        elif path.startswith("sample."):
            _set_path(sample_data, path.removeprefix("sample."), value)
    try:
        return SampleSpec.model_validate(sample_data)
    except Exception as exc:
        _raise(
            f"variant {variant.id!r} makes a shared sample invalid: {exc}",
            hint="Adjust the sample override to match the closed sample schema.",
        )


def _plan_analysis(
    analysis: AnalysisSpec,
    *,
    variant_id: str,
    variant: VariantSpec | None,
    recipe: RecipeV2,
    panel_by_name: Mapping[str, PanelPlan],
    parameter_values: Mapping[str, Any],
    group_members: Mapping[str, tuple[str, ...]],
    base_plan: AnalysisVariantPlan | None = None,
) -> AnalysisVariantPlan:
    group_id = _sample_group_id(analysis)
    effective, data = _apply_overrides(analysis, variant, sample_group_id=group_id)
    substitutions = variant.substitute if variant is not None else {}
    data, _ = _apply_substitutions(effective, data, substitutions)
    base_term_map = _map_terms_to_base(analysis, substitutions)
    data = _resolve_parameter_refs(data, parameter_values)
    try:
        effective = _ANALYSIS_ADAPTER.validate_python(data)
    except Exception as exc:
        _raise(
            f"variant {variant_id!r} makes analysis {analysis.id!r} invalid after "
            f"substitution: {exc}",
            hint="Adjust substitutions so the effective analysis still satisfies its schema.",
        )
    panel_name = _panel_name(effective.input)
    panel = panel_by_name.get(panel_name)
    if panel is None:
        _raise(
            f"analysis {analysis.id!r} references unknown panel {effective.input!r}",
            hint="Use a key declared in the recipe's panels section.",
        )
    if panel.geo_key is None or panel.time_key is None:
        _raise(
            f"panel {panel_name!r} has no geography/time key contract",
            hint="Set geo_key and time_key on the panel binding.",
        )
    data = _resolve_key_aliases(data, panel.geo_key, panel.time_key)
    effective = _ANALYSIS_ADAPTER.validate_python(data)
    sample_model, group_id = _effective_sample(effective, recipe, group_id, variant)
    sample = _resolve_parameter_refs(_json_model(sample_model), parameter_values)
    fixed_effects = tuple(_column_list(data.get("fixed_effects", [])))
    default_drop = bool(fixed_effects)
    drop_singletons = _effective_drop_singletons(
        recipe.sample_groups[group_id].sample if group_id is not None else analysis.sample,
        variant,
        default=default_drop,
    )
    sample["drop_singletons"] = drop_singletons
    sample_for_required = dict(sample)
    if group_id is not None:
        # Group-specific requirements follow the union of member requirements.
        sample_for_required.pop("require", None)
    required, numeric, weights, predicates = _required_columns(
        effective,
        sample_for_required,
    )
    min_obs = int(sample.get("min_obs_per_geography", 1))
    component_map = component_hashes(
        {
            "analysis": _json_model(effective),
            "sample": sample,
            "substitute": dict(sorted(substitutions.items())),
            "panel_identity": panel.panel_identity,
        }
    )
    result_key = f"{analysis.id}@{variant_id}"
    sample_source = result_key
    base_membership_key: str | None = None
    if (
        variant is not None
        and base_plan is not None
        and _same_row_membership(base_plan, effective, sample)
    ):
        required = base_plan.required_columns
        numeric = base_plan.numeric_columns
        weights = base_plan.weight_columns
        predicates = base_plan.predicate_columns
        fixed_effects = base_plan.fixed_effects
        drop_singletons = base_plan.drop_singletons
        min_obs = base_plan.min_obs_per_geography
        sample = dict(base_plan.sample)
        sample_source = base_plan.result_key
        base_membership_key = base_plan.result_key
    if group_id is not None:
        group_context_key = f"{group_id}@{variant_id}:{panel.panel_identity}"
    else:
        group_context_key = None
    return AnalysisVariantPlan(
        analysis_id=analysis.id,
        variant_id=variant_id,
        result_key=result_key,
        panel_name=panel_name,
        panel_identity=panel.panel_identity,
        geo_key=panel.geo_key,
        time_key=panel.time_key,
        method=effective.method,
        effective_spec=_json_model(effective),
        base_term_by_effective_term=base_term_map,
        required_columns=required,
        numeric_columns=numeric,
        weight_columns=weights,
        predicate_columns=predicates,
        fixed_effects=fixed_effects,
        drop_singletons=drop_singletons,
        min_obs_per_geography=min_obs,
        sample=sample,
        sample_group_id=group_id,
        group_context_key=group_context_key,
        sample_source=sample_source,
        base_membership_key=base_membership_key,
        component_hashes=component_map,
    )


def _same_row_membership(
    base: AnalysisVariantPlan,
    effective: AnalysisSpec,
    sample: Mapping[str, Any],
) -> bool:
    base_spec = _deep_thaw(base.effective_spec)
    current = _json_model(effective)
    for field in (
        "input",
        "sample",
        "weights",
        "outcome",
        "predictors",
        "controls",
        "fixed_effects",
    ):
        if current.get(field) != base_spec.get(field):
            return False
    inference = current.get("inference", {})
    base_inference = base_spec.get("inference", {})
    if inference.get("method") == "clustered" and inference.get("cluster_by") != base_inference.get(
        "cluster_by"
    ):
        return False
    return _deep_thaw(sample) == _deep_thaw(base.sample)


def _panel_name(reference: str) -> str:
    return reference.removeprefix("panels.")


def _resolve_key_aliases(data: dict[str, Any], geo_key: str, time_key: str) -> dict[str, Any]:
    """Resolve semantic key aliases only in categorical analysis roles."""
    aliases = {"geography": geo_key, "year": time_key}
    resolved = dict(data)
    for field in ("fixed_effects", "by"):
        if field in resolved:
            resolved[field] = [aliases.get(column, column) for column in resolved[field]]
    if isinstance(resolved.get("inference"), dict):
        inference = dict(resolved["inference"])
        if inference.get("cluster_by") is not None:
            inference["cluster_by"] = aliases.get(inference["cluster_by"], inference["cluster_by"])
        resolved["inference"] = inference
    return resolved


def _effective_sample(
    analysis: AnalysisSpec,
    recipe: RecipeV2,
    group_id: str | None,
    variant: VariantSpec | None,
) -> tuple[SampleSpec, str | None]:
    if group_id is not None:
        group_sample = _sample_group_override(variant, recipe.sample_groups[group_id])
        return _substitute_sample(group_sample, variant), group_id
    sample_data = _json_model(analysis.sample)
    if variant is not None:
        for path, value in sorted(variant.override.items()):
            if path == "sample":
                sample_data = _json_model(value)
            elif path.startswith("sample."):
                _set_path(sample_data, path.removeprefix("sample."), value)
    try:
        sample_model = SampleSpec.model_validate(sample_data)
    except Exception as exc:
        _raise(
            f"variant {variant.id if variant else 'base'!r} makes an analysis sample "
            f"invalid: {exc}",
            hint="Adjust the sample override to match the closed sample schema.",
        )
    return _substitute_sample(sample_model, variant), None


def _substitute_sample(sample: SampleSpec, variant: VariantSpec | None) -> SampleSpec:
    if variant is None or not variant.substitute:
        return sample
    data = _json_model(sample)
    substitutions = variant.substitute
    if isinstance(data.get("require"), list):
        data["require"] = [substitutions.get(item, item) for item in data["require"]]
    geographies = data.get("geographies")
    if isinstance(geographies, dict):
        exclusion = geographies.get("exclude")
        if isinstance(exclusion, dict) and isinstance(exclusion.get("column"), str):
            column = exclusion["column"]
            exclusion["column"] = substitutions.get(column, column)
    try:
        return SampleSpec.model_validate(data)
    except Exception as exc:
        _raise(
            f"variant {variant.id!r} makes a substituted sample invalid: {exc}",
            hint="Keep sample column substitutions valid and unambiguous.",
        )


def _resolve_parameter_refs(value: Any, parameters: Mapping[str, Any]) -> Any:
    if isinstance(value, dict):
        if set(value) == {"param"}:
            name = value["param"]
            if name not in parameters:
                _raise(
                    f"unknown parameter reference {name!r}",
                    hint="Declare the parameter in the recipe's parameters section.",
                )
            return parameters[name]
        return {key: _resolve_parameter_refs(item, parameters) for key, item in value.items()}
    if isinstance(value, list):
        return [_resolve_parameter_refs(item, parameters) for item in value]
    return value


def _required_columns(
    analysis: AnalysisSpec,
    sample: Mapping[str, Any],
) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    columns: list[str] = []
    numeric: list[str] = []
    weights: list[str] = []
    if isinstance(analysis, RegressionAnalysis):
        numeric = [analysis.outcome, *analysis.predictors, *analysis.controls]
        if analysis.weights not in (None, "none"):
            weights = [analysis.weights]
        columns.extend(numeric)
        columns.extend(weights)
        columns.extend(analysis.fixed_effects)
        if analysis.inference.cluster_by:
            columns.append(analysis.inference.cluster_by)
    elif isinstance(analysis, DescriptiveAnalysis):
        numeric = list(analysis.columns)
        columns.extend(analysis.columns)
        columns.extend(analysis.by)
    elif isinstance(analysis, CorrelationAnalysis):
        numeric = list(analysis.columns)
        columns.extend(analysis.columns)
    required = list(sample.get("require", []))
    columns.extend(required)
    predicates: list[str] = []
    geographies = sample.get("geographies")
    if isinstance(geographies, dict):
        exclude = geographies.get("exclude")
        if isinstance(exclude, dict) and isinstance(exclude.get("column"), str):
            predicates.append(exclude["column"])
    return (
        tuple(_unique(columns)),
        tuple(_unique([*numeric, *weights])),
        tuple(_unique(weights)),
        tuple(_unique(predicates)),
    )


def _column_list(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, Sequence) and not isinstance(value, str | bytes):
        return [item for item in value if isinstance(item, str)]
    return []


def _unique(values: Sequence[str]) -> list[str]:
    return list(dict.fromkeys(values))


def _drop_singletons(sample: Mapping[str, Any], *, default: bool) -> bool:
    if isinstance(sample, SampleSpec):
        if "drop_singletons" not in sample.model_fields_set:
            return default
        return sample.drop_singletons
    value = sample.get("drop_singletons")
    return default if value is None else bool(value)


def _effective_drop_singletons(
    base_sample: SampleSpec,
    variant: VariantSpec | None,
    *,
    default: bool,
) -> bool:
    if "drop_singletons" in base_sample.model_fields_set:
        value = base_sample.drop_singletons
    else:
        value = default
    if variant is None:
        return value
    if "sample.drop_singletons" in variant.override:
        return bool(variant.override["sample.drop_singletons"])
    replacement = variant.override.get("sample")
    if isinstance(replacement, Mapping) and "drop_singletons" in replacement:
        return bool(replacement["drop_singletons"])
    return value


def _validate_group_variants(
    recipe: RecipeV2,
    group_members: Mapping[str, tuple[str, ...]],
    panel_by_name: Mapping[str, PanelPlan],
) -> None:
    analyses = {analysis.id: analysis for analysis in recipe.analyses}
    for group_id, member_ids in group_members.items():
        if not member_ids:
            _raise(
                f"sample group {group_id!r} has no member analyses",
                hint="Reference each sample group from at least one analysis.",
            )
        base_panels = {_panel_name(analyses[member].input) for member in member_ids}
        if len(base_panels) != 1:
            _raise(
                f"sample group {group_id!r} members use different panels: {sorted(base_panels)}",
                hint="Set every group member to the same panel input.",
            )
        base_panel = next(iter(base_panels))
        if base_panel not in panel_by_name:
            _raise(
                f"sample group {group_id!r} references unknown panel {base_panel!r}",
                hint="Use a declared panel key for every group member.",
            )
        policies = {
            _fixed_effect_policy(analyses[member], recipe.sample_groups[group_id].sample)
            for member in member_ids
        }
        if len(policies) != 1:
            _raise(
                f"sample group {group_id!r} members have different fixed-effect exclusion policies",
                hint=(
                    "Align fixed_effects, drop_singletons, and min_obs_per_geography "
                    "across group members."
                ),
            )
        for variant in recipe.variants:
            targeted = set(variant.applies_to) & set(member_ids)
            if not targeted:
                continue
            membership_change = _variant_changes_group_membership(
                variant,
                [analyses[member] for member in targeted],
                recipe.sample_groups[group_id],
            )
            if membership_change and targeted != set(member_ids):
                missing = sorted(set(member_ids) - targeted)
                _raise(
                    f"variant {variant.id!r} changes membership for a subset of sample group "
                    f"{group_id!r}; missing members: {missing}",
                    hint=(
                        "Add every member analysis to applies_to or make the variant "
                        "inference-only."
                    ),
                )
            effective_panels = {
                _variant_panel_name(analyses[member], variant)
                if member in variant.applies_to
                else _panel_name(analyses[member].input)
                for member in member_ids
            }
            if len(effective_panels) != 1:
                _raise(
                    f"variant {variant.id!r} resolves sample group {group_id!r} to mixed panels",
                    hint="Keep all group members on one panel under each variant.",
                )
            policies = {
                _variant_fixed_effect_policy(
                    analyses[member], variant, recipe.sample_groups[group_id].sample
                )
                if member in variant.applies_to
                else _fixed_effect_policy(analyses[member], recipe.sample_groups[group_id].sample)
                for member in member_ids
            }
            if len(policies) != 1:
                _raise(
                    f"variant {variant.id!r} gives sample group {group_id!r} inconsistent "
                    "fixed-effect policies",
                    hint="Align fixed_effects and sample exclusion settings across group members.",
                )


def _fixed_effect_policy(analysis: AnalysisSpec, sample: SampleSpec) -> tuple[Any, ...]:
    effects = tuple(getattr(analysis, "fixed_effects", []))
    drop = _drop_singletons(sample, default=bool(effects))
    return effects, drop, sample.min_obs_per_geography


def _variant_analysis(analysis: AnalysisSpec, variant: VariantSpec) -> AnalysisSpec:
    effective, data = _apply_overrides(
        analysis,
        variant,
        sample_group_id=_sample_group_id(analysis),
    )
    data, _ = _apply_substitutions(effective, data, variant.substitute)
    try:
        return _ANALYSIS_ADAPTER.validate_python(data)
    except Exception as exc:
        _raise(
            f"variant {variant.id!r} makes analysis {analysis.id!r} invalid after "
            f"substitution: {exc}",
            hint="Adjust substitutions so the effective analysis still satisfies its schema.",
        )


def _variant_fixed_effect_policy(
    analysis: AnalysisSpec,
    variant: VariantSpec,
    sample: SampleSpec,
) -> tuple[Any, ...]:
    effective = _variant_analysis(analysis, variant)
    effects = tuple(getattr(effective, "fixed_effects", []))
    sample_data = _sample_group_override(variant, SampleGroupSpec(sample=sample))
    drop = _effective_drop_singletons(
        sample,
        variant,
        default=bool(effects),
    )
    return effects, drop, sample_data.min_obs_per_geography


def _variant_changes_group_membership(
    variant: VariantSpec,
    analyses: Sequence[AnalysisSpec],
    group_spec: SampleGroupSpec,
) -> bool:
    for analysis in analyses:
        _, base_data = _apply_overrides(analysis, None, sample_group_id=_sample_group_id(analysis))
        effective, variant_data = _apply_overrides(
            analysis,
            variant,
            sample_group_id=_sample_group_id(analysis),
        )
        membership_paths = (
            "input",
            "sample",
            "weights",
            "outcome",
            "predictors",
            "controls",
            "fixed_effects",
        )
        for path in membership_paths:
            if path == "sample":
                # Group sample overrides are shared and always affect membership.
                if any(key == "sample" or key.startswith("sample.") for key in variant.override):
                    return True
                continue
            if base_data.get(path) != variant_data.get(path):
                return True
        base_cluster = base_data.get("inference", {}).get("cluster_by")
        variant_cluster = variant_data.get("inference", {}).get("cluster_by")
        inference_override = variant.override.get("inference", {})
        explicit_cluster = "inference.cluster_by" in variant.override or (
            isinstance(inference_override, Mapping) and "cluster_by" in inference_override
        )
        if base_cluster != variant_cluster and (
            explicit_cluster or variant_data.get("inference", {}).get("method") == "clustered"
        ):
            return True
        if isinstance(analysis.sample, SampleGroupReference):
            base_sample = _json_model(analysis.sample)
            variant_sample = _json_model(effective.sample)
            # The analysis keeps its group reference; inspect the shared override directly.
            if base_sample != variant_sample:
                return True
        if variant.substitute and _all_analysis_columns(analysis) & set(variant.substitute):
            return True
    group_columns = _sample_columns(group_spec.sample)
    if variant.substitute and group_columns & set(variant.substitute):
        return True
    return False


def _sample_columns(sample: SampleSpec) -> set[str]:
    columns = set(sample.require)
    if sample.geographies is not None and sample.geographies.exclude is not None:
        columns.add(sample.geographies.exclude.column)
    return columns


def _variant_panel_name(analysis: AnalysisSpec, variant: VariantSpec) -> str:
    data = _analysis_data(analysis)
    if "input" in variant.override:
        data["input"] = variant.override["input"]
    return _panel_name(data["input"])


def _build_group_contexts(
    recipe: RecipeV2,
    plans: Sequence[AnalysisVariantPlan],
    group_members: Mapping[str, tuple[str, ...]],
    *,
    variant_id: str,
    base_contexts: Mapping[str, GroupContext],
    all_recipe_analyses: Sequence[AnalysisSpec] | None = None,
) -> list[GroupContext]:
    plans_by_id = {plan.analysis_id: plan for plan in plans}
    all_analyses = {analysis.id: analysis for analysis in (all_recipe_analyses or recipe.analyses)}
    contexts: list[GroupContext] = []
    for group_id, member_ids in group_members.items():
        members = [plans_by_id[member_id] for member_id in member_ids if member_id in plans_by_id]
        if not members:
            continue
        panels = {(plan.panel_name, plan.panel_identity) for plan in members}
        if len(panels) != 1:
            _raise(
                f"sample group {group_id!r} has mixed panel identities under {variant_id!r}",
                hint="Make the panel build and keys identical for every group member.",
            )
        panel_name, identity = next(iter(panels))
        base = base_contexts.get(group_id)
        context_variant = next(
            (variant for variant in recipe.variants if variant.id == variant_id),
            None,
        )
        membership_change = context_variant is not None and _variant_changes_group_membership(
            context_variant,
            [all_analyses[member_id] for member_id in member_ids],
            recipe.sample_groups[group_id],
        )
        is_inference_only_subset = (
            variant_id != "base"
            and base is not None
            and not membership_change
            and all(
                _same_membership_fields(all_analyses[member_id], plan)
                for plan in members
                for member_id in (plan.analysis_id,)
            )
        )
        if variant_id != "base" and base is not None and is_inference_only_subset:
            contexts.append(
                GroupContext(
                    group_id=group_id,
                    variant_id=variant_id,
                    panel_name=base.panel_name,
                    panel_identity=base.panel_identity,
                    member_analysis_ids=tuple(member_ids),
                    sample=base.sample,
                    required_columns=base.required_columns,
                    numeric_columns=base.numeric_columns,
                    weight_columns=base.weight_columns,
                    predicate_columns=base.predicate_columns,
                    fixed_effects=base.fixed_effects,
                    drop_singletons=base.drop_singletons,
                    min_obs_per_geography=base.min_obs_per_geography,
                    sample_source=f"{group_id}@base",
                    base_membership_key=base.key,
                )
            )
            continue
        group_sample = _sample_group_override(context_variant, recipe.sample_groups[group_id])
        group_sample = _substitute_sample(group_sample, context_variant)
        sample_data = _json_model(group_sample)
        required = _unique([column for plan in members for column in plan.required_columns])
        required = _unique([*required, *sample_data.get("require", [])])
        numeric = _unique([column for plan in members for column in plan.numeric_columns])
        weights = _unique([column for plan in members for column in plan.weight_columns])
        predicates = _unique([column for plan in members for column in plan.predicate_columns])
        first = members[0]
        drop = _effective_drop_singletons(
            recipe.sample_groups[group_id].sample,
            context_variant,
            default=bool(first.fixed_effects),
        )
        sample_data["drop_singletons"] = drop
        context = GroupContext(
            group_id=group_id,
            variant_id=variant_id,
            panel_name=panel_name,
            panel_identity=identity,
            member_analysis_ids=tuple(member_ids),
            sample=sample_data,
            required_columns=tuple(required),
            numeric_columns=tuple(numeric),
            weight_columns=tuple(weights),
            predicate_columns=tuple(predicates),
            fixed_effects=first.fixed_effects,
            drop_singletons=drop,
            min_obs_per_geography=int(sample_data.get("min_obs_per_geography", 1)),
            sample_source=f"{group_id}@{variant_id}",
            base_membership_key=(
                base.key if is_inference_only_subset and base is not None else None
            ),
        )
        contexts.append(context)
    return contexts


def _same_membership_fields(base: AnalysisSpec, planned: AnalysisVariantPlan) -> bool:
    base_data = _resolve_key_aliases(_analysis_data(base), planned.geo_key, planned.time_key)
    effective = _deep_thaw(planned.effective_spec)
    membership = (
        "input",
        "sample",
        "weights",
        "outcome",
        "predictors",
        "controls",
        "fixed_effects",
    )
    for field in membership:
        if base_data.get(field) != effective.get(field):
            return False
    inference = effective.get("inference", {})
    return inference.get("method") != "clustered" or base_data.get("inference", {}).get(
        "cluster_by"
    ) == inference.get("cluster_by")


def _attach_group_context(plan: AnalysisVariantPlan, context: GroupContext) -> AnalysisVariantPlan:
    return AnalysisVariantPlan(
        **{
            **plan.__dict__,
            "panel_name": context.panel_name,
            "panel_identity": context.panel_identity,
            "geo_key": plan.geo_key,
            "time_key": plan.time_key,
            "required_columns": context.required_columns,
            "numeric_columns": context.numeric_columns,
            "weight_columns": context.weight_columns,
            "predicate_columns": context.predicate_columns,
            "fixed_effects": context.fixed_effects,
            "drop_singletons": context.drop_singletons,
            "min_obs_per_geography": context.min_obs_per_geography,
            "sample": context.sample,
            "group_context_key": context.key,
            "sample_source": context.sample_source,
            "base_membership_key": context.base_membership_key,
        }
    )


def _attach_base_group_contexts(
    plans: Sequence[AnalysisVariantPlan],
    base_contexts: Mapping[str, GroupContext],
) -> list[AnalysisVariantPlan]:
    result: list[AnalysisVariantPlan] = []
    for plan in plans:
        if plan.variant_id == "base" and plan.sample_group_id is not None:
            context = base_contexts[plan.sample_group_id]
            result.append(_attach_group_context(plan, context))
        else:
            result.append(plan)
    return result


def _resolve_references(
    recipe: RecipeV2,
    plans: Mapping[str, AnalysisVariantPlan],
    variants: Mapping[str, VariantSpec],
) -> list[ResolvedReference]:
    analyses = {analysis.id: analysis for analysis in recipe.analyses}
    references: list[ResolvedReference] = []
    for finding in recipe.findings:
        refs = _finding_sources(finding)
        estimand_analysis = next(
            (source.analysis for key, source in refs if key == "estimate"),
            None,
        )
        for source_kind, source in refs:
            analysis = analyses[source.analysis]
            variants_for_reference = ["base"]
            if source_kind == "estimate" and source.term is not None:
                if not isinstance(analysis, RegressionAnalysis):
                    _raise(
                        f"finding {finding.id!r} estimate references non-regression analysis "
                        f"{source.analysis!r}",
                        hint="Reference a regression coefficient in criteria_source.estimate.",
                    )
                if source.term not in _term_columns(analysis)[1:]:
                    _raise(
                        f"finding {finding.id!r} references undefined term "
                        f"{source.term!r} in analysis {source.analysis!r}",
                        hint="Use a predictor or control declared by the analysis.",
                    )
            if source.stat is not None:
                _validate_stat(source.stat, analysis, finding.id)
            if finding.robustness is not None and source.analysis == estimand_analysis:
                missing = [
                    variant_id
                    for variant_id in finding.robustness.variants
                    if source.analysis not in variants[variant_id].applies_to
                ]
                if missing:
                    _raise(
                        f"finding {finding.id!r} robustness variants do not apply to its "
                        f"estimand analysis: {missing}",
                        hint="Target the estimand analysis with each listed robustness variant.",
                    )
                variants_for_reference.extend(finding.robustness.variants)
            for variant_id in variants_for_reference:
                result_key = f"{source.analysis}@{variant_id}"
                plan = plans.get(result_key)
                if plan is None:
                    _raise(
                        f"finding {finding.id!r} references unavailable result {result_key!r}",
                        hint="Ensure each referenced variant applies to the source analysis.",
                    )
                term = source.term
                if term is not None:
                    matches = [
                        effective
                        for effective, base in plan.base_term_by_effective_term.items()
                        if base == term
                    ]
                    if len(matches) != 1:
                        _raise(
                            f"finding {finding.id!r} term {term!r} does not resolve exactly once "
                            f"for {result_key!r}",
                            hint=(
                                "Use a term declared by the base analysis and preserved by "
                                "substitution."
                            ),
                        )
                    resolved_term = matches[0]
                else:
                    resolved_term = None
                references.append(
                    ResolvedReference(
                        owner_id=finding.id,
                        owner_kind="finding",
                        analysis_id=source.analysis,
                        result_key=result_key,
                        base_term=term,
                        term=resolved_term,
                        stat=source.stat,
                    )
                )

    for diagnostic in recipe.diagnostics:
        if isinstance(diagnostic, LinearIdentityDiagnostic):
            analysis_ids = _unique([term.analysis for term in diagnostic.terms])
            eligible_variants = {"base"}
            if diagnostic.variants == "all":
                for variant in recipe.variants:
                    if set(analysis_ids) <= set(variant.applies_to):
                        eligible_variants.add(variant.id)
            else:
                eligible_variants.update(diagnostic.variants)
            missing_variants = sorted(
                variant_id
                for variant_id in eligible_variants
                if variant_id != "base"
                and any(
                    term.analysis not in variants[variant_id].applies_to
                    for term in diagnostic.terms
                )
            )
            if missing_variants:
                _raise(
                    f"diagnostic {diagnostic.id!r} variants do not apply to every referenced "
                    f"analysis: {missing_variants}",
                    hint="Target every analysis used by the diagnostic with each listed variant.",
                )
            for variant_id in sorted(eligible_variants):
                for term in diagnostic.terms:
                    source_analysis = analyses[term.analysis]
                    if not isinstance(source_analysis, RegressionAnalysis) or term.term not in (
                        source_analysis.predictors + source_analysis.controls
                    ):
                        _raise(
                            f"diagnostic {diagnostic.id!r} references undefined coefficient "
                            f"term {term.term!r} in analysis {term.analysis!r}",
                            hint="Use a predictor or control declared by each regression analysis.",
                        )
                    result_key = f"{term.analysis}@{variant_id}"
                    plan = plans.get(result_key)
                    if plan is None:
                        _raise(
                            f"diagnostic {diagnostic.id!r} references unavailable result "
                            f"{result_key!r}",
                            hint=(
                                "Ensure every requested diagnostic variant applies to its analyses."
                            ),
                        )
                    matches = [
                        effective
                        for effective, base in plan.base_term_by_effective_term.items()
                        if base == term.term
                    ]
                    if len(matches) != 1:
                        _raise(
                            f"diagnostic {diagnostic.id!r} term {term.term!r} does not resolve "
                            f"exactly once for {result_key!r}",
                            hint=(
                                "Use a term declared by each referenced analysis and preserved "
                                "by substitution."
                            ),
                        )
                    references.append(
                        ResolvedReference(
                            owner_id=diagnostic.id,
                            owner_kind="diagnostic",
                            analysis_id=term.analysis,
                            result_key=result_key,
                            base_term=term.term,
                            term=matches[0],
                        )
                    )
    return references


def _finding_sources(finding: FindingSpec) -> list[tuple[str, Any]]:
    sources: list[tuple[str, Any]] = []
    if "estimate" in finding.criteria_source:
        from hhplab.recipe.schema_results import CriterionSource

        sources.append(
            ("estimate", CriterionSource.model_validate(finding.criteria_source["estimate"]))
        )
    from hhplab.recipe.schema_results import CriterionSource

    sources.extend(
        (f"stats.{name}", CriterionSource.model_validate(source))
        for name, source in finding.criteria_source.get("stats", {}).items()
    )
    return sources


def _validate_stat(stat: str, analysis: AnalysisSpec, finding_id: str) -> None:
    if isinstance(analysis, DescriptiveAnalysis):
        declared = {
            item if isinstance(item, str) else next(iter(item.model_dump(exclude_none=True)))
            for item in analysis.stats
        }
        if stat not in declared:
            _raise(
                f"finding {finding_id!r} references undefined statistic {stat!r} "
                f"in analysis {analysis.id!r}",
                hint="Declare the statistic in the descriptive analysis stats list.",
            )
        return
    supported = _SUPPORTED_STATS.get(analysis.method, frozenset())
    if stat not in supported or stat not in analysis.report:
        _raise(
            f"finding {finding_id!r} references undefined statistic {stat!r} "
            f"in analysis {analysis.id!r}",
            hint="Use a supported scalar statistic included in the analysis report.",
        )


def _build_dependencies(
    recipe: RecipeV2,
    references: Sequence[ResolvedReference],
    plans: Mapping[str, AnalysisVariantPlan],
) -> dict[str, tuple[str, ...]]:
    dependencies: dict[str, set[str]] = {
        key: {f"panel:{plan.panel_name}"} for key, plan in plans.items()
    }
    for reference in references:
        owner_key = f"{reference.owner_kind}:{reference.owner_id}"
        dependencies.setdefault(owner_key, set()).add(reference.result_key)
    return {key: tuple(sorted(value)) for key, value in sorted(dependencies.items())}


def _component_hashes(
    recipe: RecipeV2,
    panels: Sequence[PanelPlan],
    analyses: Sequence[AnalysisVariantPlan],
    contexts: Sequence[GroupContext],
) -> dict[str, str]:
    panel_data = [panel.to_dict() for panel in panels]
    return component_hashes(
        {
            "panels": panel_data,
            "parameters": _resolved_parameters(recipe),
            "masks": [_json_model(mask) for mask in recipe.masks],
            "derive": [_json_model(spec) for spec in recipe.derive],
            "samples": {
                "analyses": [
                    {"result_key": item.result_key, "sample": _deep_thaw(item.sample)}
                    for item in analyses
                ],
                "groups": [context.to_dict() for context in contexts],
            },
            "analyses": [item.to_dict() for item in analyses],
            "diagnostics": [_json_model(item) for item in recipe.diagnostics],
            "findings": [_json_model(item) for item in recipe.findings],
            "results": _json_model(recipe.results),
            "annotations": [_json_model(item) for item in recipe.annotations],
        }
    )
