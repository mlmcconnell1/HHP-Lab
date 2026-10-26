"""RecipeV2 lifecycle and deterministic post-build replay services."""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
import platform
import re
import socket
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pandas as pd

from hhplab.analysis.inspection import _json_group_value
from hhplab.analysis.sample import resolve_sample
from hhplab.recipe.derive import (
    annotation_identity_key,
    apply_masks,
    derive_panel,
    extract_bindings_from_sidecar,
    serialize_annotation_contract,
)
from hhplab.recipe.executor.analyses import execute_analysis
from hhplab.recipe.executor.inputs import SiteUniverseResolution, resolve_site_universe_files
from hhplab.recipe.executor.msa_coc_panel import (
    SiteYearCovariateBinding,
    SiteYearSourceBinding,
    assemble_site_year_panel,
)
from hhplab.recipe.results_plan import ResultsPlanningError, plan_results
from hhplab.recipe.schema_results import RecipeV2, _validate_annotation_supersedes_graph
from hhplab.results.bundle import BundleArtifact, BundlePayload, write_bundle
from hhplab.results.diagnostics import column_identity, linear_identity
from hhplab.results.lineage import (
    ArtifactLineage,
    declared_source_terms,
    derive_lineage,
    descriptive_scope,
    union_lineage,
)
from hhplab.results.recipe_findings import evaluate_findings
from hhplab.schema.contracts import (
    PUBLIC_SAMPLE_ANNOTATION_NOTICE,
    annotation_source_identity,
    frame_annotation_contract,
    project_annotation_contract,
)
from hhplab.schema.measures import analysis_measure_metadata
from hhplab.schema.redistribution import ArtifactScope
from hhplab.schema.results import (
    BUNDLE_FORMAT_VERSION,
    AnalysisStatus,
    AnalysisStatusRecord,
    DiagnosticRecord,
    DiagnosticsPayload,
    DiagnosticsStatus,
    DiagnosticStatus,
    DistributionStatus,
    ExecutionStatus,
    FindingsPayload,
    FindingsStatus,
    InputIdentity,
    LockStatus,
    ResultError,
    StatusFields,
    Tolerance,
    aggregate_distribution_status,
    canonical_bytes,
    canonical_json,
)
from hhplab.storage.provenance import PROVENANCE_KEY, ProvenanceBlock, read_provenance
from hhplab.storage.run_ledger import RunLedger


class ResultsRunError(RuntimeError):
    """A run-level failure with its lifecycle scope attached."""

    def __init__(self, message: str, *, code: str = "run_failed") -> None:
        super().__init__(message)
        self.code = code
        self.scope = "run"


def check_results(
    recipe: RecipeV2,
    *,
    original_recipe_bytes: bytes,
    readiness: bool = False,
    project_root: Path | None = None,
    outputs_root: Path | None = None,
    panel_frames: Mapping[str, pd.DataFrame] | None = None,
) -> dict[str, Any]:
    """Run static preflight, and optionally frame readiness, without a ledger.

    Readiness may load identity-matching cached panels from canonical output
    paths. This function never builds or writes state.
    """
    try:
        plan = plan_results(recipe, original_recipe_bytes=original_recipe_bytes)
    except ResultsPlanningError as exc:
        return {
            "status": "failed",
            "readiness": readiness,
            "issues": [
                item.to_dict()
                if hasattr(item, "to_dict")
                else {"severity": "error", "message": str(item)}
                for item in exc.findings
            ],
            "warnings": [],
            "plan": None,
        }
    issues = [item.to_dict() for item in plan.preflight_findings if item.is_blocking]
    warnings = [
        item.to_dict() for item in plan.preflight_findings if not item.is_blocking
    ]
    if readiness and not issues:
        if panel_frames is None:
            panel_frames, cache_errors = _load_cached_panels(
                recipe,
                plan,
                project_root=project_root or Path.cwd(),
                outputs_root=outputs_root,
            )
            issues.extend(
                {"severity": "error", "message": message} for message in cache_errors
            )
        if panel_frames and not issues:
            replay = replay_results(
                {"recipe": _recipe_dict(recipe), "plan": plan.to_dict()},
                {"spec_hash": plan.plan_sha256, "input_measures": {}},
                panel_frames,
            )
            for result_key, result in replay["analyses"].items():
                if result["status"] == "failed":
                    issues.append(
                        {
                            "severity": "error",
                            "result_key": result_key,
                            "message": result.get("error", {}).get(
                                "message", "analysis readiness failed"
                            ),
                        }
                    )
    return {
        "status": "failed" if issues else "ready",
        "readiness": readiness,
        "issues": issues,
        "warnings": warnings,
        "plan": plan.to_dict(),
    }


def replay_results(
    resolved_spec: Mapping[str, Any],
    execution: Mapping[str, Any],
    panel_frames: Mapping[str, pd.DataFrame],
    *,
    site_universe_resolution: SiteUniverseResolution | None = None,
    boundary_evidence_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Replay all analysis stages from panel frames, without builds or ledger I/O."""
    if not isinstance(resolved_spec, Mapping) or not isinstance(execution, Mapping):
        raise TypeError("resolved_spec and execution must be mappings")
    recipe_data = resolved_spec.get("recipe")
    expected_plan = resolved_spec.get("plan")
    if not isinstance(recipe_data, Mapping) or not isinstance(expected_plan, Mapping):
        raise ValueError(
            "resolved_spec must contain canonical recipe and plan mappings"
        )
    recipe = RecipeV2.model_validate(recipe_data)
    original = canonical_bytes(
        recipe.model_dump(mode="json", by_alias=True, exclude_none=True)
    )
    if expected_plan.get("site_universe") is not None:
        if site_universe_resolution is None or boundary_evidence_payload is None:
            site_universe_resolution, boundary_evidence_payload = (
                _site_universe_inputs_from_plan(expected_plan["site_universe"])
            )
        else:
            site_universe_resolution = replace(site_universe_resolution, paths=())
    rebuilt = plan_results(
        recipe,
        original_recipe_bytes=original,
        site_universe_resolution=site_universe_resolution,
        boundary_evidence_payload=boundary_evidence_payload,
    )
    plan_hash_payload = {
        key: expected_plan[key]
        for key in (
            "recipe_id",
            "panels",
            "parameters",
            "analyses",
            "group_contexts",
            "references",
            "analysis_dependencies",
            "component_hashes",
            "annotations",
        )
        if key in expected_plan
    }
    if expected_plan.get("site_universe") is not None:
        site_hash_data = dict(expected_plan["site_universe"])
        site_hash_data.pop("paths", None)
        site_hash_data.pop("manifest_fingerprint", None)
        evidence = dict(site_hash_data.get("boundary_evidence", {}))
        evidence.pop("path", None)
        site_hash_data["boundary_evidence"] = evidence
        plan_hash_payload["site_universe"] = site_hash_data
    plan_hash = hashlib.sha256(canonical_bytes(plan_hash_payload)).hexdigest()
    expected_hash_keys = 10 if expected_plan.get("site_universe") is not None else 9
    if len(plan_hash_payload) != expected_hash_keys or execution.get("spec_hash") != plan_hash:
        raise ValueError(
            "execution spec_hash does not match the canonical resolved plan"
        )
    if expected_plan.get("recipe_id") != recipe.id:
        raise ValueError("resolved plan recipe_id does not match RecipeV2")
    expected_panels = expected_plan.get("panels", ())
    actual_panels = [panel.to_dict() for panel in rebuilt.panels]
    if sorted(expected_panels, key=lambda item: item["name"]) != sorted(
        actual_panels, key=lambda item: item["name"]
    ):
        raise ValueError("resolved plan panel identities do not match RecipeV2")
    rebuilt_dict = rebuilt.to_dict()
    if _recipe_plan_projection(expected_plan) != _recipe_plan_projection(rebuilt_dict):
        raise ValueError("resolved analysis plan does not match RecipeV2")
    declared_result_keys = {
        f"{analysis.id}@{variant}"
        for analysis in recipe.analyses
        for variant in [
            "base",
            *[item.id for item in recipe.variants if analysis.id in item.applies_to],
        ]
    }
    if {
        item.get("result_key") for item in expected_plan.get("analyses", ())
    } != declared_result_keys:
        raise ValueError("resolved plan analysis keys do not match RecipeV2")
    plan = _runtime_plan(expected_plan, rebuilt, plan_hash)
    if not isinstance(panel_frames, Mapping):
        raise TypeError("panel_frames must be a mapping of panel name to DataFrame")

    panels = {panel.name: panel for panel in plan.panels}
    states: dict[str, dict[str, Any]] = {}
    derived: dict[str, pd.DataFrame] = {}
    derived_measures: dict[str, dict[str, Any]] = {}
    panel_errors: dict[str, str] = {}
    measures_by_panel = execution.get("input_measures", {})
    if not isinstance(measures_by_panel, Mapping):
        measures_by_panel = {}
    materialized_panel_frames = {
        name: frame.copy(deep=True) if isinstance(frame, pd.DataFrame) else frame
        for name, frame in panel_frames.items()
    }
    site_panel_name = None
    missing_sources: list[str] = []
    source_panel_fingerprints: dict[str, str] = {}
    if rebuilt.site_universe is not None:
        if rebuilt.site_year_boundary_compatibility is None:
            raise ValueError("validated site-year boundary compatibility is unavailable")
        site_panel_name, site_binding = next(
            (name, binding.site_year)
            for name, binding in recipe.panels.items()
            if binding.site_year is not None
        )
        assert site_binding is not None
        required_sources = {
            site_binding.outcomes.pit.panel,
            site_binding.outcomes.hic.panel,
            *(binding.panel for binding in site_binding.covariates),
        }
        missing_sources = sorted(
            name
            for name in required_sources
            if not isinstance(materialized_panel_frames.get(name), pd.DataFrame)
        )
        if not missing_sources:
            assembly, source_panel_fingerprints = _assemble_site_year_panel(
                recipe,
                rebuilt,
                materialized_panel_frames,
            )
            site_panel = assembly.panel
            site_panel.attrs["results_panel_provenance"] = {
                "dataset_type": "site_year_panel",
                **assembly.provenance,
            }
            site_panel.attrs["site_year_provenance"] = assembly.provenance
            site_panel.attrs["results_source_parquet_bytes"] = _parquet_bytes(
                site_panel,
                dataset_type="site_year_panel",
                provenance=site_panel.attrs["results_panel_provenance"],
            )
            site_panel.attrs["results_content_sha256"] = hashlib.sha256(
                site_panel.attrs["results_source_parquet_bytes"]
            ).hexdigest()
            materialized_panel_frames[site_panel_name] = site_panel
        else:
            panel_errors[site_panel_name] = (
                "site-year source panel(s) are unavailable: " + ", ".join(missing_sources)
            )

    declared_annotations = [
        spec.model_dump(mode="json", by_alias=True, exclude_none=True)
        for spec in recipe.annotations
    ]
    effective_annotations: dict[str, dict[str, Any]] = {}
    inherited_by_panel: dict[str, list[dict[str, Any]]] = {}
    bindings_by_panel: dict[str, dict[str, dict[str, Any]]] = {}
    uncertain_by_panel: dict[str, list[str]] = {}
    for panel_name, panel_plan in panels.items():
        frame = materialized_panel_frames.get(panel_name)
        if not isinstance(frame, pd.DataFrame):
            panel_errors[panel_name] = "panel input is missing or is not a DataFrame"
            continue
        try:
            discovered, panel_bindings, panel_uncertain = _collect_origin_annotation_records(
                frame
            )
            merged, origins, origin_sources = _merge_annotation_records(
                declared_annotations, discovered
            )
            uncertain_keys = panel_uncertain
            attachment_bindings = dict(panel_bindings)
            for identity in uncertain_keys:
                if attachment_bindings.get(identity, {}).get("columns"):
                    continue  # A conservative candidate set is narrower than the whole panel.
                attachment_bindings[identity] = {"columns": {
                    column: [
                        [geo.item() if hasattr(geo, "item") else geo, int(year)]
                        for geo, year in zip(
                            frame[panel_plan.geo_key], frame[panel_plan.time_key], strict=True
                        )
                    ]
                    for column in frame.columns
                    if column not in (panel_plan.geo_key, panel_plan.time_key)
                }}
            uncertain_by_panel[panel_name] = sorted(uncertain_keys)
            inherited = [
                record
                for record in merged
                if ANNOTATION_ORIGIN_ARTIFACT in origin_sources[
                    f"{record['id']}#r{int(record.get('revision', 1))}"
                ]
            ]
            if inherited:
                inherited_by_panel[panel_name] = inherited
            state = apply_masks(
                # Applicability is already explicit in the arguments below.
                # Keep the original annotated source for bundling; numerical
                # views must not copy its full provenance into every cell.
                _panel_value_frame(frame),
                recipe.masks,
                panel_name=panel_name,
                geo_key=panel_plan.geo_key,
                time_key=panel_plan.time_key,
                annotations=merged or None,
                annotation_bindings=attachment_bindings or None,
                annotation_origin_flags=origins,
            )
            active_derivations = [
                item
                for item in recipe.derive
                if not item.panels or panel_name in item.panels
            ]
            metadata = measures_by_panel.get(
                panel_name, _input_measures({panel_name: frame})[panel_name]
            )
            if active_derivations:
                state = derive_panel(
                    state,
                    recipe.derive,
                    panel_name=panel_name,
                    geo_key=panel_plan.geo_key,
                    time_key=panel_plan.time_key,
                    input_measures=metadata,
                    parameters=plan.parameters,
                )
            states[panel_name] = state
            derived[panel_name] = state["frame"]
            derived_measures[panel_name] = dict(state.get("measures", {}))
            final_bindings = extract_bindings_from_sidecar(
                state["annotation_provenance"], state["frame"],
                geo_key=panel_plan.geo_key, time_key=panel_plan.time_key,
            )
            uncertain_keys = [identity for identity in uncertain_keys
                              if final_bindings.get(identity, {}).get("columns")]
            uncertain_by_panel[panel_name] = uncertain_keys
            bindings_by_panel[panel_name] = final_bindings
            origin_contract = frame_annotation_contract(frame)
            annotation_sources = {
                identity: list(entries)
                for identity, entries in (origin_contract or {}).get("sources", {}).items()
            }
            if merged:
                source_bytes = _panel_input_bytes(frame, dataset_type="results_input_panel")
                digest = hashlib.sha256(source_bytes).hexdigest()
                claimed = frame.attrs.get("results_content_sha256")
                if claimed is not None and claimed != digest:
                    raise ValueError(
                        f"panel {panel_name!r} source content hash does not match bytes"
                    )
                frame.attrs["results_source_parquet_bytes"] = source_bytes
                frame.attrs["results_content_sha256"] = digest
                source = annotation_source_identity(
                    digest, _parquet_provenance(source_bytes),
                )
                for identity, binding in final_bindings.items():
                    if binding["columns"]:
                        entries = annotation_sources.setdefault(identity, [])
                        if source not in entries:
                            entries.append(source)
            effective_annotations[panel_name] = (
                serialize_annotation_contract(
                    merged, bindings=final_bindings, uncertain_ids=uncertain_keys,
                    sources=annotation_sources,
                ) if merged else None
            )
        except Exception as exc:
            panel_errors[panel_name] = f"{type(exc).__name__}: {exc}"

    global_discovered: list[tuple[str, dict[str, Any]]] = []
    for panel_name, _panel_plan in panels.items():
        frame = materialized_panel_frames.get(panel_name)
        if isinstance(frame, pd.DataFrame):
            discovered, _bindings, _uncertain = _collect_origin_annotation_records(frame)
            global_discovered.extend(discovered)
    global_records, global_origins, global_origin_sources = _merge_annotation_records(
        declared_annotations, global_discovered
    )

    analyses: dict[str, dict[str, Any]] = {}
    samples: dict[str, pd.DataFrame] = {}
    sample_results: dict[str, dict[str, Any]] = {}
    annotations_by_key: dict[str, dict[str, Any]] = {}
    groups = {context.key: context for context in plan.group_contexts}
    for analysis_plan in plan.analyses:
        key = analysis_plan.result_key
        if analysis_plan.panel_name in panel_errors:
            missing_code = (
                "missing_panel"
                if analysis_plan.panel_name not in execution.get("panels", {})
                else "panel_unavailable"
            )
            analyses[key] = {
                "analysis": analysis_plan.analysis_id,
                "variant": analysis_plan.variant_id,
                "result_key": key,
                "sample_source": analysis_plan.sample_source,
                "status": "failed",
                "error": {
                    "code": missing_code,
                    "message": panel_errors[analysis_plan.panel_name],
                },
            }
            continue
        state = states[analysis_plan.panel_name]
        panel_state = state["frame"]
        normalized = analysis_plan.to_dict()
        group_context = None
        if analysis_plan.group_context_key is not None:
            group_context = groups.get(analysis_plan.group_context_key)
            if group_context is None:
                analyses[key] = _failed_analysis(
                    analysis_plan,
                    "missing_group_context",
                    "planned sample group context is missing",
                )
                continue
            normalized.update(group_context.to_dict())
            normalized.update(
                {
                    "panel_name": analysis_plan.panel_name,
                    "panel_identity": analysis_plan.panel_identity,
                    "geo_key": analysis_plan.geo_key,
                    "time_key": analysis_plan.time_key,
                    "sample_source": analysis_plan.sample_source,
                    "context_key": analysis_plan.group_context_key,
                }
            )
        else:
            normalized["context_key"] = analysis_plan.result_key
        normalized.setdefault("context_key", analysis_plan.result_key)
        base = (
            sample_results.get(analysis_plan.base_membership_key)
            if analysis_plan.base_membership_key
            else None
        )
        try:
            resolved = resolve_sample(
                panel_state,
                normalized,
                mask_provenance=state["mask_provenance"],
                annotation_provenance=state.get("annotation_provenance"),
                base_membership=base,
            )
            sample_results[key] = resolved
            annotations_by_key[key] = {
                "keys": list(resolved["annotations"]["keys"]),
                "counts": {
                    f"{annotation_id}#r{revision}": count
                    for (annotation_id, revision), count in resolved["annotations"][
                        "counts"
                    ].items()
                },
            }
            if analysis_plan.group_context_key is not None:
                sample_results[analysis_plan.group_context_key] = resolved
                annotations_by_key[analysis_plan.group_context_key] = annotations_by_key[key]
            samples[key] = resolved["membership"]
            outcome = execute_analysis(analysis_plan.to_dict(), resolved["resolved"])
        except Exception as exc:
            outcome = _failed_analysis(
                analysis_plan, "readiness_error", f"{type(exc).__name__}: {exc}"
            )
        analyses[key] = outcome

    try:
        diagnostics = _evaluate_diagnostics(
            recipe, plan, states, analyses, sample_results
        )
        diagnostics_error = None
    except Exception as exc:
        diagnostics = DiagnosticsPayload(BUNDLE_FORMAT_VERSION, ())
        diagnostics_error = f"{type(exc).__name__}: {exc}"
    try:
        findings = evaluate_findings(
            recipe.model_dump(mode="python", by_alias=True, exclude_none=True).get(
                "findings", ()
            ),
            analyses,
            parameters=plan.parameters,
            references=plan.references,
            variant_plans=plan.analyses,
        )
    except Exception as exc:
        findings = FindingsPayload(BUNDLE_FORMAT_VERSION, ())
        finding_error = f"{type(exc).__name__}: {exc}"
    else:
        finding_error = None
    return {
        "tables": _collect_tables(analyses),
        "analyses": analyses,
        "diagnostics": diagnostics.to_dict(),
        "diagnostics_error": diagnostics_error,
        "findings": findings.to_dict(),
        "findings_error": finding_error,
        "samples": samples,
        "derived": derived,
        "derived_measures": derived_measures,
        "input_measures": dict(measures_by_panel),
        "panel_errors": panel_errors,
        "input_panels": materialized_panel_frames,
        "site_year_source_panel_fingerprints": (
            source_panel_fingerprints if rebuilt.site_universe is not None else {}
        ),
        "sample_results": sample_results,
        "annotations": _annotations_payload(
            analyses,
            annotations_by_key,
            global_records,
            global_origins,
            inherited_by_panel,
            global_origin_sources,
            bindings_by_panel,
            uncertain_by_panel,
            {name: contract["sources"] for name, contract in effective_annotations.items()
             if contract is not None},
        ),
        "effective_annotations": effective_annotations,
        "inherited_annotations": inherited_by_panel,
        "annotation_sidecars": {
            name: state.get("annotation_provenance")
            for name, state in states.items()
            if isinstance(state, Mapping) and state.get("annotation_provenance") is not None
        },
    }


ANNOTATION_ORIGIN_DECLARED = "declared"
ANNOTATION_ORIGIN_ARTIFACT = "artifact"


def _normalize_annotation_record(record: Any) -> dict[str, Any]:
    """Normalize an annotation record to canonical JSON shape, validating strictly."""
    from hhplab.recipe.schema_results import DataAnnotationSpec

    if isinstance(record, DataAnnotationSpec):
        return record.model_dump(mode="json", by_alias=True, exclude_none=True)
    if not isinstance(record, Mapping):
        raise ValueError(
            f"annotation record must be a mapping, got {type(record).__name__}"
        )
    try:
        spec = DataAnnotationSpec.model_validate(dict(record))
    except Exception as exc:
        raise ValueError(f"invalid annotation record: {exc}") from exc
    return spec.model_dump(mode="json", by_alias=True, exclude_none=True)


def _collect_origin_annotation_records(
    frame: pd.DataFrame,
) -> tuple[list[tuple[str, dict[str, Any]]], dict[str, dict[str, Any]], list[str]]:
    """Discover data-origin annotation records persisted in a panel artifact.

    Lookup order: ``results_panel_provenance.extra.annotations`` then
    ``results_panel_provenance.annotations`` (both forms written by bundle
    producers), then a direct ``frame.attrs["annotations"]`` container used by
    injected panel frames. Containers use the versioned serialization contract
    (``list`` legacy record definitions or a v1 contract dict with records,
    concrete bindings, and uncertain identity keys). Present containers of any
    other shape, or simultaneously-present alternate containers, are rejected
    with an actionable error — never silently ignored.
    """
    contract = frame_annotation_contract(frame)
    if contract is None:
        return [], {}, []
    return (
        [(ANNOTATION_ORIGIN_ARTIFACT, record) for record in contract["records"]],
        contract["bindings"], contract["uncertain_ids"],
    )


def _annotation_identity(record: Mapping[str, Any]) -> tuple[str, int]:
    return (str(record.get("id")), int(record.get("revision", 1)))


def _canonical_annotation(record: Mapping[str, Any]) -> str:
    """Deterministic canonical form for identity comparison."""
    return json.dumps(record, sort_keys=True, separators=(",", ":"), default=str)


class AnnotationMergeConflictError(ValueError):
    """Raised when the same annotation identity maps to different content."""


def _merge_annotation_records(
    declared: Sequence[Any],
    discovered: Sequence[tuple[str, dict[str, Any]]],
) -> tuple[list[dict[str, Any]], dict[str, str], dict[str, list[str]]]:
    """Merge declared and inherited annotation records under immutable identity.

    Records sharing an ``id``/``revision`` must have identical canonical
    content; identical content deduplicates with truthful origins. Different
    content for the same identity is an actionable conflict (a changed
    definition requires an explicit new revision) — never a silent in-place
    override. Returns ``(merged, origins, origin_sources)``.
    """
    entries: dict[tuple[str, int], dict[str, Any]] = {}
    for record in declared:
        rec = _normalize_annotation_record(record)
        key = _annotation_identity(rec)
        if key not in entries:
            entries[key] = {"canonical": _canonical_annotation(rec), "origins": set()}
            entries[key]["record"] = rec
        elif entries[key]["canonical"] != _canonical_annotation(rec):
            raise AnnotationMergeConflictError(
                "annotation identity is declared with different content than "
                "already registered; a changed definition requires an explicit "
                "new revision, not an in-place override"
            )
        entries[key]["origins"].add(ANNOTATION_ORIGIN_DECLARED)
    for origin_label, record in discovered:
        rec = _normalize_annotation_record(record)
        key = _annotation_identity(rec)
        if key not in entries:
            entries[key] = {"canonical": _canonical_annotation(rec), "origins": set()}
            entries[key]["record"] = rec
        elif entries[key]["canonical"] != _canonical_annotation(rec):
            raise AnnotationMergeConflictError(
                "annotation identity from artifact origin conflicts with "
                "existing content; a changed definition requires an explicit "
                "new revision, not an in-place override"
            )
        entries[key]["origins"].add(origin_label)

    merged = [dict(entry["record"]) for entry in entries.values()]
    origins: dict[str, str] = {}
    origin_sources: dict[str, list[str]] = {}
    for key, entry in entries.items():
        identity = f"{key[0]}#r{key[1]}"
        sources = sorted(entry["origins"])
        origin_sources[identity] = sources
        origins[identity] = (
            ANNOTATION_ORIGIN_DECLARED
            if ANNOTATION_ORIGIN_DECLARED in sources
            else sources[0]
        )
    merged.sort(key=_annotation_identity)
    _validate_annotation_supersedes_graph(merged)
    return merged, origins, origin_sources


def _iter_annotation_keys(entry: Mapping[str, Any]):
    """Yield (id, revision) pairs from a per-analysis key entry.

    Keys may be ``(id, revision)`` tuples (raw sample resolution) or dicts
    (serialized ``per_analysis`` view).
    """
    for item in entry.get("keys", ()):
        if isinstance(item, Mapping):
            yield str(item.get("id")), int(item.get("revision", 1))
        else:
            yield str(item[0]), int(item[1])


def _annotation_scope_covers(newer, older):
    """Prove selector coverage before hiding an older contributing revision."""
    new_scope, old_scope = newer.get("scope", {}), older.get("scope", {})
    for axis in ("panels", "columns", "years"):
        new_values, old_values = new_scope.get(axis, []), old_scope.get(axis, [])
        if new_values and (not old_values or not set(old_values) <= set(new_values)):
            return False
    new_geo = (new_scope.get("geographies") or {}).get("include")
    old_geo = (old_scope.get("geographies") or {}).get("include")
    if new_geo is not None and (old_geo is None or not set(old_geo) <= set(new_geo)):
        return False
    new_masks, old_masks = newer.get("applies_to_masks", []), older.get("applies_to_masks", [])
    return not new_masks or bool(old_masks and set(old_masks) <= set(new_masks))


def _active_identities_by_key(
    annotations_by_key: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
) -> dict[str, list[tuple[str, int]]]:
    """Resolve supersession per applied analysis key.

    A revision is suppressed in a key only when a *higher* revision of the
    same id that explicitly supersedes it is itself applied in that same key.
    Unrelated superseders never hide contributing inherited evidence.
    """
    record_index = {
        (str(record.get("id")), int(record.get("revision", 1))): record
        for record in records
    }
    by_id: dict[str, dict[int, set[str]]] = {}
    for key, entry in annotations_by_key.items():
        for annotation_id, revision in _iter_annotation_keys(entry):
            by_id.setdefault(str(annotation_id), {}).setdefault(int(revision), set()).add(key)
    active_by_key: dict[str, list[tuple[str, int]]] = {}
    for key, entry in annotations_by_key.items():
        active: list[tuple[str, int]] = []
        for annotation_id, revision in _iter_annotation_keys(entry):
            annotation_id = str(annotation_id)
            revision = int(revision)
            covered = False
            for other_revision in by_id.get(annotation_id, {}):
                if other_revision <= revision or key not in by_id[annotation_id][other_revision]:
                    continue
                superseder = record_index.get((annotation_id, other_revision))
                older = record_index[(annotation_id, revision)]
                if superseder is not None and not _annotation_scope_covers(superseder, older):
                    continue
                while superseder is not None and superseder.get("supersedes") is not None:
                    target = superseder["supersedes"]
                    if target == revision:
                        covered = True
                        break
                    superseder = record_index.get((annotation_id, target))
                if covered:
                    break
            if not covered:
                active.append((annotation_id, revision))
        active.sort(key=lambda item: (item[0], item[1]))
        active_by_key[key] = active
    return active_by_key


def _annotations_payload(
    analyses,
    annotations_by_key,
    records,
    origins,
    inherited_by_panel,
    origin_sources,
    bindings_by_panel,
    uncertain_by_panel,
    sources_by_panel,
):
    """Aggregate annotation records and per-analysis applied keys for a replay."""
    ordered = sorted(records, key=_annotation_identity)
    applied: set[tuple[str, int]] = set()
    for entry in annotations_by_key.values():
        for annotation_id, revision in _iter_annotation_keys(entry):
            applied.add((str(annotation_id), int(revision)))
    active_by_key = _active_identities_by_key(annotations_by_key, ordered)
    conflicts = []
    conflict_seen: dict[tuple[str, tuple[int, ...]], int] = {}
    for key, active in active_by_key.items():
        by_id_active: dict[str, list[int]] = {}
        for annotation_id, revision in active:
            by_id_active.setdefault(annotation_id, []).append(revision)
        for annotation_id, revisions in by_id_active.items():
            if len(revisions) < 2:
                continue
            revisions = sorted(set(revisions))
            seen_key = (annotation_id, tuple(revisions))
            if seen_key in conflict_seen:
                conflicts[conflict_seen[seen_key]]["analyses"].append(key)
                continue
            conflict_seen[seen_key] = len(conflicts)
            conflicts.append(
                {
                    "id": annotation_id,
                    "revisions": revisions,
                    "analyses": [key],
                    "message": (
                        f"annotation {annotation_id!r} has multiple contributing revisions "
                        f"{revisions}; inspect their scopes and supersession coverage"
                    ),
                }
            )
    conflicts.sort(key=lambda item: (item["id"], tuple(item["revisions"])))
    applied_list = sorted(applied, key=lambda item: (item[0], item[1]))
    unused = [
        {"id": annotation_id, "revision": revision}
        for annotation_id, revision in sorted(
            (_annotation_identity(item) for item in ordered),
            key=lambda item: (item[0], item[1]),
        )
        if (str(annotation_id), int(revision)) not in applied
    ]
    inherited = sorted(
        (
            {
                "panel": panel,
                "id": record.get("id"),
                "revision": record.get("revision"),
            }
            for panel, panel_records in inherited_by_panel.items()
            for record in panel_records
        ),
        key=lambda item: (item["panel"], str(item["id"]), int(item["revision"])),
    )
    unapplied_inherited = []
    for panel, panel_records in inherited_by_panel.items():
        for record in panel_records:
            identity = f"{record['id']}#r{int(record.get('revision', 1))}"
            binding = bindings_by_panel.get(panel, {}).get(identity)
            if binding is not None:
                continue
            unapplied_inherited.append(
                {
                    "panel": panel,
                    "id": record.get("id"),
                    "revision": record.get("revision"),
                    "reason": (
                        "uncertain_applicability"
                        if identity in uncertain_by_panel.get(panel, [])
                        else "no_persisted_binding"
                    ),
                }
            )
    unapplied_inherited.sort(
        key=lambda item: (item["panel"], str(item["id"]), int(item["revision"]))
    )
    return {
        "schema_version": 1,
        "records": ordered,
        "per_analysis": {
            key: {
                "keys": [
                    {"id": annotation_id, "revision": revision}
                    for annotation_id, revision in _iter_annotation_keys(entry)
                ],
                "counts": dict(entry["counts"]),
            }
            for key, entry in sorted(annotations_by_key.items())
        },
        "applied": [
            {"id": annotation_id, "revision": revision}
            for annotation_id, revision in applied_list
        ],
        "active_per_analysis": {
            key: [{"id": annotation_id, "revision": revision} for annotation_id, revision in active]
            for key, active in sorted(active_by_key.items())
        },
        "unused": unused,
        "conflicts": conflicts,
        "origins": dict(sorted(origins.items())),
        "origin_sources": {
            key: list(value) for key, value in sorted(origin_sources.items())
        },
        "inherited": inherited,
        "unapplied_inherited": unapplied_inherited,
        "uncertain_by_panel": uncertain_by_panel,
        "bindings_by_panel": bindings_by_panel,
        "sources_by_panel": sources_by_panel,
    }


def _report_notes(records, annotations_by_key, uncertain_ids=()):
    """Build portable note records for active per-key annotation revisions.

    Supersession is resolved per applied analysis key so an unrelated
    superseding revision never hides contributing inherited evidence; the note
    for a revision is kept as long as it is active in at least one key.
    """
    notes = []
    index = {
        (str(record.get("id")), int(record.get("revision", 1))): record
        for record in records
    }
    if isinstance(annotations_by_key, Mapping) and annotations_by_key:
        active_by_key = _active_identities_by_key(annotations_by_key, records)
        active: set[tuple[str, int]] = set()
        for active_list in active_by_key.values():
            active.update(active_list)
        analyses_by_identity: dict[tuple[str, int], list[str]] = {}
        for analysis_key, active_list in active_by_key.items():
            for identity in active_list:
                analyses_by_identity.setdefault(identity, []).append(analysis_key)
    else:
        active = {
            (str(item.get("id")), int(item.get("revision", 1)))
            for item in annotations_by_key or ()
        }
        analyses_by_identity = {}
    for key in sorted(active, key=lambda item: (item[0], item[1])):
        record = index.get(key)
        if record is None:
            continue
        note = {
            "id": record.get("id"),
            "revision": record.get("revision"),
            "category": record.get("category"),
            "summary": record.get("summary"),
            "text": record.get("text"),
            "public_text": record.get("public_text"),
            "treatment": record.get("treatment"),
            "uncertainty": record.get("uncertainty"),
            "supersedes": record.get("supersedes"),
            "evidence": [dict(item) for item in record.get("evidence", ())],
        }
        analyses = sorted(analyses_by_identity.get(key, []))
        if analyses:
            note["analyses"] = analyses
        if annotation_identity_key(*key) in uncertain_ids:
            note["applicability_uncertainty"] = (
                "Applicability is conservative after an aggregation or unsupported scope mapping."
            )
        notes.append(note)
    return notes


def run_results(
    recipe: RecipeV2,
    *,
    original_recipe_bytes: bytes,
    outputs_root: Path,
    project_root: Path | None = None,
    private: bool = False,
    allow_unknown_aggregates: bool = False,
    panel_frames: Mapping[str, pd.DataFrame] | None = None,
    build_callable=None,
) -> dict[str, Any]:
    """Run a V2 recipe, record its durable lifecycle, and write a bundle.

    ``panel_frames`` is a test/offline injection point. When omitted, the V1
    executor builds each source pipeline and the canonical output artifacts
    are loaded from disk. ``build_callable`` may replace the executor while
    retaining that output-loading contract.
    """
    if not isinstance(outputs_root, Path):
        outputs_root = Path(outputs_root)
    checked = check_results(recipe, original_recipe_bytes=original_recipe_bytes)
    if checked["status"] != "ready":
        raise ResultsRunError(
            "results run refused before ledger start: "
            + "; ".join(issue["message"] for issue in checked["issues"]),
            code="static_preflight_failed",
        )
    plan = plan_results(recipe, original_recipe_bytes=original_recipe_bytes)
    run_id = uuid.uuid4().hex
    ledger = RunLedger(outputs_root)
    lease = ledger.start(
        recipe_id=recipe.id,
        recipe_sha256=hashlib.sha256(original_recipe_bytes).hexdigest(),
        spec_hash=plan.plan_sha256,
        components=plan.component_hashes,
        environment={
            "python": os.sys.version.split()[0],
            "platform": os.sys.platform,
            "host": socket.gethostname(),
        },
        run_id=run_id,
    )
    root = project_root or Path.cwd()
    replay = None
    frames = {}
    bundle_dir = None
    terminal_attempted = False
    run_lock_status = LockStatus.from_value("unlocked")
    try:
        start_event = next(
            event
            for event in ledger.read()["events"]
            if event.get("event") == "run_started" and event.get("run_id") == run_id
        )
        run_lock_status = LockStatus.from_value(start_event["lock_status"])
        frames, build_failures = _load_run_panels(
            recipe,
            plan,
            outputs_root=outputs_root,
            project_root=root,
            panel_frames=panel_frames,
            build_callable=build_callable,
        )
        if plan.site_universe is not None:
            # Re-read every pinned source immediately before execution. The plan
            # identity is content based, so a changed source cannot silently
            # produce a bundle under the old lock identity.
            _site_universe_inputs_from_plan(plan.site_universe)
        lease.check_health()
        replay = replay_results(
            {"recipe": _recipe_dict(recipe), "plan": plan.to_dict()},
            {"spec_hash": plan.plan_sha256, "input_measures": _input_measures(frames)},
            frames,
        )
        frames = replay["input_panels"]
        for panel_name, message in build_failures.items():
            replay["panel_errors"][panel_name] = message
            for item in plan.analyses:
                if item.panel_name == panel_name:
                    replay["analyses"][item.result_key] = _failed_analysis(
                        item, "panel_unavailable", message
                    )
        lease.check_health()
        statuses = _status_fields(replay, run_failed=False, private=private)
        bundle_dir = outputs_root / recipe.results.bundle_dir.format(
            id=recipe.id, run_id=run_id
        )
        _ensure_contained_path(outputs_root, bundle_dir)
        payload, content_hashes = _bundle_payload(
            recipe,
            plan,
            replay,
            frames,
            original_recipe_bytes=original_recipe_bytes,
            run_id=run_id,
            statuses=statuses,
            lock_status=run_lock_status,
            private=private,
            allow_unknown_aggregates=allow_unknown_aggregates,
        )
        write_result = write_bundle(bundle_dir, payload)
        statuses = StatusFields(
            statuses.execution_status,
            statuses.diagnostics_status,
            statuses.findings_status,
            aggregate_distribution_status(
                private=private, withheld_count=len(write_result.withheld_paths)
            ),
        )
        terminal_attempted = True
        lease.finish(
            statuses=statuses.to_dict(),
            bundle_path=str(bundle_dir),
            manifest_sha256=write_result.manifest_sha256,
        )
        return {
            "run_id": run_id,
            "bundle_path": str(bundle_dir),
            "statuses": statuses.to_dict(),
            "lock_status": run_lock_status.to_dict(),
            "analysis_statuses": {
                key: value.to_dict() for key, value in payload.analysis_statuses.items()
            },
            "manifest_sha256": write_result.manifest_sha256,
        }
    except Exception as exc:
        failure_bundle_path = _write_failure_bundle(
            recipe,
            plan,
            replay,
            frames,
            outputs_root=outputs_root,
            original_recipe_bytes=original_recipe_bytes,
            run_id=run_id,
            lock_status=run_lock_status,
            private=private,
            allow_unknown_aggregates=allow_unknown_aggregates,
            error=exc,
        )
        if not terminal_attempted:
            lease.fail(exc, failure_bundle_path=failure_bundle_path)
        if isinstance(exc, ResultsRunError):
            raise
        raise ResultsRunError(
            f"results run failed: {type(exc).__name__}: {exc}",
            code=getattr(exc, "code", "run_failed"),
        ) from exc


def _evaluate_diagnostics(recipe, plan, states, analyses, sample_results):
    records = []
    by_variant: dict[str, list[Any]] = {}
    for item in plan.analyses:
        by_variant.setdefault(item.variant_id, []).append(item)
    tolerance = Tolerance()
    for declaration in recipe.diagnostics:
        data = declaration.model_dump(mode="python", by_alias=True, exclude_none=True)
        if data["op"] == "linear_identity":
            variants = (
                ["base", *[variant.id for variant in recipe.variants]]
                if data.get("variants", "all") == "all"
                else data["variants"]
            )
            for variant in variants:
                try:
                    applicable = [
                        item.analysis_id for item in by_variant.get(variant, ())
                    ]
                    results = {
                        key: value
                        for key, value in analyses.items()
                        if key.endswith(f"@{variant}")
                    }
                    designs = {
                        key: value.get("design_evidence", {}).get(key, {})
                        for key, value in results.items()
                        if isinstance(value.get("design_evidence"), Mapping)
                    }
                    records.append(
                        linear_identity(
                            data,
                            variant=variant,
                            applicable_analyses=applicable,
                            results=results,
                            designs=designs,
                            tolerance=tolerance,
                        )
                    )
                except Exception as exc:
                    records.append(_diagnostic_failure(data["id"], variant, exc))
        else:
            panel_name = data["panel"]
            state = states.get(panel_name)
            if state is None:
                panel = next(item for item in plan.panels if item.name == panel_name)
                records.append(
                    column_identity(
                        data,
                        pd.DataFrame(columns=[panel.geo_key, panel.time_key]),
                        geo_key=panel.geo_key,
                        time_key=panel.time_key,
                        tolerance=tolerance,
                    )
                )
                continue
            group_keys = None
            rows = data.get("rows", "unmasked")
            if isinstance(rows, Mapping) and "group" in rows:
                group_id = rows["group"]
                matches = [
                    value
                    for value in sample_results.values()
                    if value["sample_source"].startswith(f"{group_id}@")
                ]
                if matches:
                    membership = matches[0]["membership"]
                    group_keys = list(
                        zip(
                            membership.loc[membership.included, matches[0]["geo_key"]],
                            membership.loc[membership.included, matches[0]["time_key"]],
                            strict=True,
                        )
                    )
            try:
                records.append(
                    column_identity(
                        data,
                        state["frame"],
                        geo_key=next(
                            p.geo_key for p in plan.panels if p.name == panel_name
                        ),
                        time_key=next(
                            p.time_key for p in plan.panels if p.name == panel_name
                        ),
                        mask_provenance=state["mask_provenance"],
                        group_keys=group_keys,
                        tolerance=tolerance,
                    )
                )
            except Exception as exc:
                records.append(_diagnostic_failure(data["id"], "base", exc))
    return DiagnosticsPayload(BUNDLE_FORMAT_VERSION, tuple(records))


def _collect_tables(analyses):
    names = (
        "regression_estimates",
        "regression_fit",
        "descriptive_stats",
        "correlations",
    )
    tables = {name: [] for name in names}
    for result in analyses.values():
        for name in names:
            value = result.get(name)
            if name == "regression_fit" and isinstance(value, Mapping):
                tables[name].append(value)
            elif isinstance(value, list):
                tables[name].extend(value)
    return tables


def _failed_analysis(plan, code, message):
    return {
        "analysis": plan.analysis_id,
        "variant": plan.variant_id,
        "result_key": plan.result_key,
        "sample_source": plan.sample_source,
        "status": "failed",
        "error": {"code": code, "message": message},
    }


def _ensure_contained_path(root, destination):
    resolved_root = Path(root).resolve()
    resolved_destination = Path(destination).resolve()
    try:
        resolved_destination.relative_to(resolved_root)
    except ValueError as exc:
        raise ResultsRunError(
            "results bundle path escapes outputs_root", code="invalid_bundle_path"
        ) from exc


def _write_failure_bundle(
    recipe,
    plan,
    replay,
    frames,
    *,
    outputs_root,
    original_recipe_bytes,
    run_id,
    lock_status,
    private,
    allow_unknown_aggregates,
    error,
):
    """Best-effort separate package preserving partial evidence after failure."""
    failure_dir = outputs_root / "failures" / recipe.id / run_id
    try:
        _ensure_contained_path(outputs_root, failure_dir)
        if replay is None:
            empty = {
                "analyses": {
                    item.result_key: _failed_analysis(
                        item, "run_failed", f"{type(error).__name__}: {error}"
                    )
                    for item in plan.analyses
                },
                "tables": {
                    name: []
                    for name in (
                        "regression_estimates",
                        "regression_fit",
                        "descriptive_stats",
                        "correlations",
                    )
                },
                "diagnostics": DiagnosticsPayload(BUNDLE_FORMAT_VERSION, ()).to_dict(),
                "diagnostics_error": None,
                "findings": FindingsPayload(BUNDLE_FORMAT_VERSION, ()).to_dict(),
                "findings_error": None,
                "samples": {},
                "derived": {},
                "derived_measures": {},
                "panel_errors": {},
                "sample_results": {},
                "annotations": {
                    "records": [],
                    "per_analysis": {},
                    "applied": [],
                    "unused": [],
                    "conflicts": [],
                },
            }
            try:
                empty["diagnostics"] = _evaluate_diagnostics(
                    recipe, plan, {}, empty["analyses"], {}
                ).to_dict()
            except Exception as diagnostic_error:
                empty["diagnostics_error"] = str(diagnostic_error)
            try:
                empty["findings"] = evaluate_findings(
                    recipe.findings,
                    empty["analyses"],
                    parameters=plan.parameters,
                    references=plan.references,
                    variant_plans=plan.analyses,
                ).to_dict()
            except Exception as finding_error:
                empty["findings_error"] = str(finding_error)
        else:
            empty = dict(replay)
        empty["run_error"] = f"{type(error).__name__}: {error}"
        statuses = _status_fields(empty, run_failed=True, private=private)
        payload, _ = _bundle_payload(
            recipe,
            plan,
            empty,
            frames,
            original_recipe_bytes=original_recipe_bytes,
            run_id=run_id,
            statuses=statuses,
            lock_status=lock_status,
            private=private,
            allow_unknown_aggregates=allow_unknown_aggregates,
        )
        write_bundle(failure_dir, payload)
        return str(failure_dir)
    except Exception:
        return None


def _diagnostic_failure(identifier, variant, error):
    return DiagnosticRecord(
        identifier,
        variant,
        DiagnosticStatus.NOT_EVALUATED,
        None,
        None,
        "diagnostic_error",
        {"error": f"{type(error).__name__}: {error}"},
    )


def _recipe_dict(recipe: RecipeV2) -> dict[str, Any]:
    return recipe.model_dump(mode="json", by_alias=True, exclude_none=True)


def _site_universe_inputs_from_plan(site_data):
    """Re-read and fingerprint all local site inputs pinned by a results plan."""
    if not isinstance(site_data, Mapping):
        raise ValueError("resolved plan site_universe must be an object")
    paths = site_data.get("paths")
    fingerprints = site_data.get("fingerprints")
    evidence = site_data.get("boundary_evidence")
    if (
        not isinstance(paths, list | tuple)
        or len(paths) != 3
        or not isinstance(fingerprints, list | tuple)
        or len(fingerprints) != 3
        or not isinstance(evidence, Mapping)
    ):
        raise ValueError("resolved plan is missing pinned site-universe input paths or hashes")
    resolution = resolve_site_universe_files(
        artifact_id=site_data["artifact_id"],
        threshold=float(site_data["threshold"]),
        sites_path=Path(paths[0]),
        memberships_path=Path(paths[1]),
        summary_path=Path(paths[2]),
        expected_fingerprints=tuple(fingerprints),
    )
    evidence_path = Path(str(evidence.get("path", ""))).expanduser()
    try:
        evidence_bytes = evidence_path.read_bytes()
    except OSError as exc:
        raise ValueError(f"boundary evidence is unavailable: {evidence_path}") from exc
    try:
        evidence_payload = json.loads(evidence_bytes)
    except json.JSONDecodeError as exc:
        raise ValueError("pinned boundary evidence is not valid JSON") from exc
    if not isinstance(evidence_payload, Mapping):
        raise ValueError("pinned boundary evidence must contain a JSON object")
    if _boundary_evidence_fingerprint(evidence_payload) != evidence.get("fingerprint"):
        raise ValueError("boundary evidence changed since the results plan was recorded")
    return resolution, evidence_payload


def _site_universe_bundle_artifacts(site_data):
    """Return portable, fingerprint-checked source files and their index."""
    resolution, evidence_payload = _site_universe_inputs_from_plan(site_data)
    del resolution, evidence_payload
    paths = tuple(Path(path) for path in site_data["paths"])
    evidence_path = Path(str(site_data["boundary_evidence"]["path"]))
    artifact_slug = re.sub(
        r"[^A-Za-z0-9._-]+", "-", str(site_data["artifact_id"]).strip().lower()
    )
    slug = re.sub(r"-{2,}", "-", artifact_slug).strip("-.") or "containment"
    names = (
        ("sites", f"msa_coc_sites__{slug}.parquet"),
        ("memberships", f"msa_coc_memberships__{slug}.parquet"),
        ("summary", f"msa_coc_summary__{slug}.json"),
        ("boundary_evidence", f"boundary_evidence__{slug}.json"),
    )
    source_paths = (*paths, evidence_path)
    result_artifacts = []
    index = {"schema_version": str(site_data["schema_version"])}
    for (key, basename), source_path in zip(names, source_paths, strict=True):
        relative = f"site_universes/{basename}"
        contents = source_path.read_bytes()
        if key == "boundary_evidence":
            expected = site_data["boundary_evidence"]["fingerprint"]
            try:
                evidence_payload = json.loads(contents)
            except json.JSONDecodeError as exc:
                raise ValueError(f"boundary evidence is not valid JSON: {source_path}") from exc
            observed = _boundary_evidence_fingerprint(evidence_payload)
        else:
            expected = site_data["fingerprints"][("sites", "memberships", "summary").index(key)]
            observed = hashlib.sha256(contents).hexdigest()
        if observed != expected:
            raise ValueError(f"pinned site-universe input changed before bundling: {source_path}")
        result_artifacts.append(
            BundleArtifact(
                relative,
                contents,
                "site_universe_input",
                ArtifactLineage(
                    relative,
                    (str(site_data["artifact_id"]),),
                    ArtifactScope.ROW_LEVEL,
                ),
            )
        )
        index[key] = relative
    return tuple(result_artifacts), {str(site_data["artifact_id"]): index}


def site_universe_inputs_from_bundle(root: Path, index, expected_site_data):
    """Load exact indexed site/evidence bytes from a verified portable bundle."""
    if not isinstance(index, Mapping) or index.get("withheld", False):
        raise ValueError("required site-universe inputs are withheld from this bundle")
    required = ("sites", "memberships", "summary", "boundary_evidence")
    if any(not isinstance(index.get(key), str) for key in required):
        raise ValueError("bundle site-universe index is missing required input paths")
    root = Path(root).resolve()
    paths = {key: _contained_bundle_input(root, index[key]) for key in required}
    expected_fingerprints = tuple(expected_site_data.get("fingerprints", ()))
    if len(expected_fingerprints) != 3:
        raise ValueError("resolved plan does not pin all site-universe input fingerprints")
    resolution = resolve_site_universe_files(
        artifact_id=expected_site_data["artifact_id"],
        threshold=float(expected_site_data["threshold"]),
        sites_path=paths["sites"],
        memberships_path=paths["memberships"],
        summary_path=paths["summary"],
        expected_fingerprints=expected_fingerprints,
    )
    evidence_bytes = paths["boundary_evidence"].read_bytes()
    expected_evidence_hash = expected_site_data.get("boundary_evidence", {}).get(
        "fingerprint"
    )
    try:
        evidence_payload = json.loads(evidence_bytes)
    except json.JSONDecodeError as exc:
        raise ValueError("bundled boundary evidence is not valid JSON") from exc
    if not isinstance(evidence_payload, Mapping):
        raise ValueError("bundled boundary evidence must contain a JSON object")
    if _boundary_evidence_fingerprint(evidence_payload) != expected_evidence_hash:
        raise ValueError("bundled boundary evidence differs from the resolved plan")
    return replace(resolution, paths=()), evidence_payload


def _boundary_evidence_fingerprint(payload):
    return "sha256:" + hashlib.sha256(canonical_bytes(payload)).hexdigest()


def _contained_bundle_input(root: Path, relative: str) -> Path:
    from pathlib import PurePosixPath

    path = PurePosixPath(relative)
    if (
        not relative
        or path.is_absolute()
        or path.as_posix() != relative
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise ValueError(f"bundle input path is not normalized and relative: {relative!r}")
    candidate = root
    for part in path.parts:
        candidate = candidate / part
        if candidate.is_symlink():
            raise ValueError(f"bundle input path contains a symlink: {relative!r}")
    resolved = candidate.resolve()
    if root not in resolved.parents or not resolved.is_file():
        raise ValueError(f"bundle input is missing or escapes its root: {relative!r}")
    return resolved


def _assemble_site_year_panel(recipe, plan, frames):
    site_panel_name, panel_binding = next(
        (name, binding)
        for name, binding in recipe.panels.items()
        if binding.site_year is not None
    )
    site_spec = panel_binding.site_year
    assert site_spec is not None
    plan_site = plan.site_universe
    if not isinstance(plan_site, Mapping):
        raise ValueError("results plan is missing the resolved site-universe identity")
    source_bindings = {
        "pit": site_spec.outcomes.pit,
        "hic": site_spec.outcomes.hic,
    }
    outcomes = {
        name: SiteYearSourceBinding(panel=binding.panel, measures=tuple(binding.measures))
        for name, binding in source_bindings.items()
    }
    covariates = tuple(
        SiteYearCovariateBinding(
            panel=binding.panel,
            columns=tuple(binding.columns),
            require_match=binding.require_match,
        )
        for binding in site_spec.covariates
    )
    source_names = tuple(
        dict.fromkeys(
            (*[binding.panel for binding in outcomes.values()],
             *[binding.panel for binding in covariates])
        )
    )
    source_panels = {}
    fingerprints = {}
    for name in source_names:
        frame = frames.get(name)
        if not isinstance(frame, pd.DataFrame):
            raise ValueError(f"site-year source panel {name!r} is unavailable")
        if name in {binding.panel for binding in outcomes.values()}:
            _validate_site_source_panel_provenance(name, frame, plan_site)
        else:
            _validate_site_covariate_provenance(name, frame, plan_site)
        source_bytes = _panel_input_bytes(frame, dataset_type="results_input_panel")
        digest = hashlib.sha256(source_bytes).hexdigest()
        claimed = frame.attrs.get("results_content_sha256")
        if claimed is not None and claimed != digest:
            raise ValueError(f"site-year source panel {name!r} content hash does not match bytes")
        frame.attrs["results_source_parquet_bytes"] = source_bytes
        frame.attrs["results_content_sha256"] = digest
        source_panels[name] = frame
        fingerprints[name] = digest
    boundary_validation = plan.site_year_boundary_compatibility
    if boundary_validation is None:
        raise ValueError("validated boundary compatibility evidence is unavailable")
    assembly = assemble_site_year_panel(
        frozen_memberships=tuple(tuple(pair) for pair in plan_site["memberships"]),
        requested_years=tuple(int(year) for year in plan_site["requested_years"]),
        source_panels=source_panels,
        outcome_bindings=outcomes,
        covariate_bindings=covariates,
        boundary_compatibility=boundary_validation,
        artifact_id=str(plan_site["artifact_id"]),
        threshold=float(plan_site["threshold"]),
        membership_identity=str(plan_site["membership_identity"]),
        frozen_boundary_vintage=str(plan_site["boundary_vintage"]),
        source_panel_fingerprints=fingerprints,
    )
    return assembly, fingerprints


def _validate_site_covariate_provenance(panel_name, frame, site_plan):
    provenance = frame.attrs.get("results_panel_provenance")
    if not isinstance(provenance, Mapping):
        raise ValueError(
            f"site-year covariate panel {panel_name!r} lacks embedded HHP-Lab provenance"
        )
    extra = provenance.get("extra", {})
    target = provenance.get("target_geometry")
    if not isinstance(target, Mapping) and isinstance(extra, Mapping):
        target = extra.get("target_geometry")
    target = target if isinstance(target, Mapping) else {}
    geometry_type = target.get("type") or provenance.get("geo_type")
    definition_version = (
        target.get("source")
        or provenance.get("definition_version")
        or (extra.get("msa_definition_version") if isinstance(extra, Mapping) else None)
    )
    subset_profile = target.get("subset_profile")
    subset_profile_version = target.get("subset_profile_definition_version")
    if geometry_type not in {"msa", "metro"}:
        raise ValueError(
            f"site-year covariate panel {panel_name!r} provenance must identify native "
            "MSA geometry"
        )
    if subset_profile is not None or subset_profile_version is not None:
        raise ValueError(
            f"site-year covariate panel {panel_name!r} cannot use an MSA subset profile"
        )
    expected = site_plan.get("msa_definition_version")
    if not isinstance(expected, str) or definition_version != expected:
        raise ValueError(
            f"site-year covariate panel {panel_name!r} MSA definition "
            f"{definition_version!r} does not match frozen site-universe definition "
            f"{expected!r}"
        )


def _validate_site_source_panel_provenance(panel_name, frame, site_plan):
    provenance = frame.attrs.get("results_panel_provenance")
    if not isinstance(provenance, Mapping):
        raise ValueError(
            f"site-year source panel {panel_name!r} lacks embedded HHP-Lab provenance"
        )
    extra = provenance.get("extra", {})
    target = provenance.get("target_geometry")
    if not isinstance(target, Mapping) and isinstance(extra, Mapping):
        target = extra.get("target_geometry")
    target_vintage = target.get("vintage") if isinstance(target, Mapping) else None
    geometry_type = target.get("type") if isinstance(target, Mapping) else None
    if geometry_type is None:
        geometry_type = provenance.get("geo_type")
    if geometry_type is None and isinstance(extra, Mapping):
        geometry_type = extra.get("geo_type")
    if target_vintage is None:
        target_vintage = provenance.get("boundary_vintage")
    if target_vintage is None and isinstance(extra, Mapping):
        target_vintage = extra.get("boundary_vintage")
    if geometry_type != "coc":
        raise ValueError(
            f"site-year source panel {panel_name!r} provenance must identify CoC geometry"
        )
    expected_vintages = {
        str(value)
        for key, value in site_plan.get("source_panel_vintages", {}).items()
        if key.rsplit(":", 1)[0] == panel_name
    }
    if not expected_vintages or str(target_vintage) not in expected_vintages:
        raise ValueError(
            f"site-year source panel {panel_name!r} provenance boundary vintage "
            f"{target_vintage!r} does not match planned vintages {sorted(expected_vintages)}"
        )
    used_column = "boundary_vintage_used"
    if used_column in frame.columns:
        expected = expected_vintages
        observed = {
            str(value) for value in frame[used_column].dropna().unique().tolist()
        }
        if observed - expected:
            raise ValueError(
                f"site-year source panel {panel_name!r} rows use boundary vintages "
                f"{sorted(observed - expected)} outside the planned source vintages"
            )


def _parquet_provenance(contents):
    import pyarrow as pa
    import pyarrow.parquet as pq

    metadata = pq.read_metadata(pa.BufferReader(contents)).metadata or {}
    return json.loads(metadata[PROVENANCE_KEY]) if PROVENANCE_KEY in metadata else {}


def _panel_input_bytes(frame, *, dataset_type):
    existing = frame.attrs.get("results_source_parquet_bytes")
    contract = frame_annotation_contract(frame)
    if isinstance(existing, bytes):
        probe = pd.DataFrame()
        probe.attrs["results_panel_provenance"] = _parquet_provenance(existing)
        if frame_annotation_contract(probe) != contract:
            raise ValueError(
                "panel annotations differ from original bytes; write a new annotated artifact"
            )
        from io import BytesIO

        original = pd.read_parquet(BytesIO(existing))
        matches = _frame_values_equal(frame, original)
        if not matches:
            original = pd.read_parquet(BytesIO(existing), dtype_backend="pyarrow")
            matches = _frame_values_equal(frame, original)
        if not matches:
            raise ValueError(
                "annotated source DataFrame changed after reading its source bytes; "
                "write a new annotated artifact with updated provenance before reuse"
            )
        return existing
    provenance = dict(frame.attrs.get("results_panel_provenance", {}))
    if contract is not None:
        provenance.pop("annotations", None)
        provenance["extra"] = {
            **provenance.get("extra", {}),
            "annotations": contract["records"] if contract["schema_version"] == 0 else contract,
        }
    return _parquet_bytes(frame, dataset_type=dataset_type, provenance=provenance)


def _panel_value_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Preserve physical storage nulls without changing source byte identities."""
    from io import BytesIO

    if isinstance(frame.attrs.get("results_source_parquet_bytes"), bytes):
        source = _panel_input_bytes(frame, dataset_type="results_input_panel")
        values = pd.read_parquet(BytesIO(source), dtype_backend="pyarrow")
        values.attrs.clear()
        return values
    return pd.DataFrame(frame, copy=False)


def _frame_values_equal(left: pd.DataFrame, right: pd.DataFrame) -> bool:
    """Compare logical Arrow types/nulls, then normalized exact cell values."""
    import pyarrow as pa

    if list(left.columns) != list(right.columns) or len(left) != len(right):
        return False
    left_table = pa.Table.from_pandas(pd.DataFrame(left, copy=False), preserve_index=False)
    right_table = pa.Table.from_pandas(pd.DataFrame(right, copy=False), preserve_index=False)
    if not left_table.schema.equals(right_table.schema, check_metadata=False):
        return False
    for column in left.columns:
        if not left_table[column].is_null().equals(right_table[column].is_null()):
            return False
    normalized_left = left_table.to_pandas(ignore_metadata=True)
    normalized_right = right_table.to_pandas(ignore_metadata=True)
    for column in left.columns:
        if normalized_left[column].dtype != object:
            continue
        normalized_left[column] = normalized_left[column].map(_roundtrip_list_value)
        normalized_right[column] = normalized_right[column].map(_roundtrip_list_value)
    return normalized_left.equals(normalized_right)


def _roundtrip_list_value(value):
    """Normalize only list/ndarray values that Parquet reads back differently."""
    if isinstance(value, list):
        return tuple(_roundtrip_list_value(item) for item in value)
    if (
        type(value).__name__ == "ndarray"
        and hasattr(value, "tolist")
        and getattr(value, "ndim", 0) > 0
    ):
        return tuple(_roundtrip_list_value(item) for item in value.tolist())
    return value


def _runtime_plan(value, rebuilt, plan_hash):
    """Rehydrate the hashed normalized plan used by portable replay."""
    panels = [SimpleNamespace(**item) for item in value["panels"]]
    analyses = [
        SimpleNamespace(**item, to_dict=lambda item=item: dict(item))
        for item in value["analyses"]
    ]
    contexts = []
    for item in value.get("group_contexts", ()):
        context = dict(item)
        context["key"] = (
            f"{item['group_id']}@{item['variant_id']}:{item['panel_identity']}"
        )
        context["to_dict"] = lambda item=item: dict(item)
        contexts.append(SimpleNamespace(**context))
    return SimpleNamespace(
        recipe_id=value["recipe_id"],
        panels=panels,
        analyses=analyses,
        group_contexts=contexts,
        references=value.get("references", ()),
        parameters=value.get("parameters", {}),
        analysis_dependencies=value.get("analysis_dependencies", {}),
        component_hashes=value.get("component_hashes", {}),
        plan_sha256=plan_hash,
        preflight_findings=rebuilt.preflight_findings,
        to_dict=lambda: dict(value),
    )


def _recipe_plan_projection(value):
    """Compare plan semantics while ignoring serializer-expanded singleton defaults."""
    projected = dict(value)
    analyses = []
    for raw in value.get("analyses", ()):
        item = dict(raw)
        item.pop("drop_singletons", None)
        sample = dict(item.get("sample", {}))
        sample.pop("drop_singletons", None)
        item["sample"] = sample
        components = dict(item.get("component_hashes", {}))
        components.pop("sample", None)
        item["component_hashes"] = components
        analyses.append(item)
    contexts = []
    for raw in value.get("group_contexts", ()):
        item = dict(raw)
        item.pop("drop_singletons", None)
        sample = dict(item.get("sample", {}))
        sample.pop("drop_singletons", None)
        item["sample"] = sample
        contexts.append(item)
    hashes = dict(value.get("component_hashes", {}))
    hashes.pop("analyses", None)
    hashes.pop("samples", None)
    projected["analyses"] = analyses
    projected["group_contexts"] = contexts
    projected["component_hashes"] = hashes
    if isinstance(projected.get("site_universe"), Mapping):
        site_data = dict(projected["site_universe"])
        site_data.pop("paths", None)
        site_data.pop("manifest_fingerprint", None)
        evidence = dict(site_data.get("boundary_evidence", {}))
        evidence.pop("path", None)
        site_data["boundary_evidence"] = evidence
        projected["site_universe"] = site_data
    return projected


def _load_run_panels(
    recipe, plan, *, outputs_root, project_root, panel_frames, build_callable
):
    if panel_frames is not None:
        return dict(panel_frames), {}
    from hhplab.recipe.executor import (
        ExecutorError,
        _resolve_panel_output_file,
        execute_recipe,
    )
    from hhplab.storage.config import load_config

    storage = load_config(output_root=outputs_root, project_root=project_root)
    failures: dict[str, str] = {}
    pipeline_results = {}
    try:
        if build_callable is None:
            results = execute_recipe(
                recipe, project_root=project_root, quiet=True, storage_config=storage
            )
        else:
            results = build_callable(
                recipe, project_root=project_root, storage_config=storage
            )
        pipeline_results = {
            item.pipeline_id: item
            for item in (results or ())
            if isinstance(getattr(item, "pipeline_id", None), str)
        }
    except ExecutorError as exc:
        pipeline_results = {
            item.pipeline_id: item
            for item in getattr(exc, "partial_results", ())
            if isinstance(getattr(item, "pipeline_id", None), str)
        }
        failed_pipelines = {
            item.pipeline_id
            for item in pipeline_results.values()
            if not item.success
        }
        for item in plan.panels:
            if item.pipeline_id in failed_pipelines:
                failures[item.name] = str(exc)
    frames = {}
    for item in plan.panels:
        if item.pipeline_id is None:
            continue
        if item.name in failures:
            continue
        result = pipeline_results.get(item.pipeline_id)
        persist_completed = result is not None and any(
            step.step_kind == "persist" and step.success for step in result.steps
        )
        if result is None or not result.success or not persist_completed:
            failures[item.name] = (
                f"pipeline {item.pipeline_id!r} has no successful panel persist result; "
                "the existing output was not accepted as a current build"
            )
            continue
        try:
            path = _resolve_panel_output_file(
                recipe, item.pipeline_id, project_root, storage
            )
            if not path.is_file():
                raise FileNotFoundError(f"expected panel output is missing: {path}")
            source_bytes = path.read_bytes()
            frame = pd.read_parquet(path)
            provenance = read_provenance(path)
            if provenance is not None:
                frame.attrs["results_panel_provenance"] = provenance.to_dict()
            frame.attrs["results_content_sha256"] = hashlib.sha256(source_bytes).hexdigest()
            frame.attrs["results_source_parquet_bytes"] = source_bytes
            frames[item.name] = frame
            _write_identity_receipt(path, item.build_spec_hash)
        except Exception as exc:
            failures[item.name] = f"{type(exc).__name__}: {exc}"
    return frames, failures


def _load_cached_panels(recipe, plan, *, project_root, outputs_root=None):
    """Read canonical cached panels only when their embedded identity matches."""
    from hhplab.recipe.executor import _resolve_panel_output_file
    from hhplab.storage.config import load_config
    from hhplab.storage.provenance import read_provenance

    storage = load_config(
        output_root=outputs_root,
        recipe_output_root=recipe.output_root,
        recipe_base_dir=recipe.source_base_dir,
        project_root=project_root,
    )
    frames = {}
    errors = []
    for panel in plan.panels:
        if panel.pipeline_id is None:
            continue
        try:
            path = _resolve_panel_output_file(
                recipe, panel.pipeline_id, project_root, storage
            )
            if not path.is_file():
                raise FileNotFoundError(
                    f"cached panel {panel.name!r} is missing at {path}; build the recipe first"
                )
            provenance = read_provenance(path)
            extra = provenance.extra if provenance is not None else {}
            identity = extra.get(
                "results_build_spec_hash", extra.get("build_spec_hash")
            )
            content_hash = hashlib.sha256(path.read_bytes()).hexdigest()
            receipt = _identity_receipt_path(path, panel.build_spec_hash, content_hash)
            receipt_matches = _receipt_matches(
                receipt,
                panel_path=path,
                build_spec_hash=panel.build_spec_hash,
                content_sha256=content_hash,
            )
            provenance_matches = (
                identity == panel.build_spec_hash
                and extra.get("results_content_sha256") == content_hash
            )
            if not (receipt_matches or provenance_matches):
                raise ValueError(
                    f"cached panel {panel.name!r} has no matching build identity; "
                    "rebuild it with the current recipe before --readiness"
                )
            frame = pd.read_parquet(path)
            if provenance is not None:
                frame.attrs["results_panel_provenance"] = provenance.to_dict()
            source_bytes = path.read_bytes()
            frame.attrs["results_source_parquet_bytes"] = source_bytes
            frame.attrs["results_content_sha256"] = content_hash
            frames[panel.name] = frame
        except Exception as exc:
            errors.append(f"panel {panel.name!r}: {type(exc).__name__}: {exc}")
    return frames, errors


def _identity_receipt_path(panel_path, build_spec_hash, content_sha256):
    return panel_path.with_name(
        f"{panel_path.name}.{build_spec_hash}.{content_sha256}.results-identity.json"
    )


def _write_identity_receipt(panel_path, build_spec_hash):
    content_sha256 = hashlib.sha256(panel_path.read_bytes()).hexdigest()
    receipt = _identity_receipt_path(panel_path, build_spec_hash, content_sha256)
    record = {
        "schema_version": 1,
        "panel_path": panel_path.name,
        "build_spec_hash": build_spec_hash,
        "content_sha256": content_sha256,
    }
    contents = canonical_bytes(record)
    try:
        with receipt.open("xb") as stream:
            stream.write(contents)
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError:
        if receipt.read_bytes() != contents:
            raise ValueError(
                f"cached panel identity receipt conflicts: {receipt.name}"
            ) from None
    directory_fd = os.open(receipt.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _receipt_matches(receipt, *, panel_path, build_spec_hash, content_sha256):
    try:
        value = json.loads(receipt.read_bytes())
    except (OSError, ValueError, TypeError):
        return False
    return value == {
        "schema_version": 1,
        "panel_path": panel_path.name,
        "build_spec_hash": build_spec_hash,
        "content_sha256": content_sha256,
    }


def _input_measures(frames):
    resolved = {}
    for name, frame in frames.items():
        explicit = frame.attrs.get("measures", {})
        metadata = dict(explicit) if isinstance(explicit, Mapping) else {}
        for column in frame.columns:
            if column in metadata:
                continue
            registered = analysis_measure_metadata(column, panel_columns=list(frame.columns))
            if registered is not None:
                metadata[column] = registered
        resolved[name] = metadata
    return resolved


def _status_fields(replay, *, run_failed, private):
    analyses = replay["analyses"]
    execution = (
        ExecutionStatus.FAILED
        if run_failed
        else (
            ExecutionStatus.PARTIAL
            if any(value.get("status") != "complete" for value in analyses.values())
            else ExecutionStatus.COMPLETE
        )
    )
    diagnostic_rows = replay["diagnostics"].get("diagnostics", ())
    if not diagnostic_rows:
        diagnostic_status = (
            DiagnosticsStatus.INCOMPLETE
            if replay.get("diagnostics_error")
            else DiagnosticsStatus.NONE
        )
    elif any(item["status"] == "fail" for item in diagnostic_rows):
        diagnostic_status = DiagnosticsStatus.FAIL
    elif any(item["status"] == "not_evaluated" for item in diagnostic_rows):
        diagnostic_status = DiagnosticsStatus.INCOMPLETE
    elif all(item["status"] == "not_applicable" for item in diagnostic_rows):
        diagnostic_status = DiagnosticsStatus.NOT_APPLICABLE
    else:
        diagnostic_status = DiagnosticsStatus.PASS
    finding_rows = replay["findings"].get("findings", ())
    if replay.get("findings_error") or any(
        outcome.get("status") == "ambiguous_rule"
        for finding in finding_rows
        for outcome in [finding.get("base", {}), *finding.get("variants", {}).values()]
    ):
        finding_status = FindingsStatus.AMBIGUOUS
    elif any(
        outcome.get("status") == "not_evaluated"
        for finding in finding_rows
        for outcome in [finding.get("base", {}), *finding.get("variants", {}).values()]
    ):
        finding_status = FindingsStatus.SOME_NOT_EVALUATED
    else:
        finding_status = FindingsStatus.ALL_EVALUATED
    return StatusFields(
        execution,
        diagnostic_status,
        finding_status,
        DistributionStatus.PRIVATE if private else DistributionStatus.FULL,
    )


def _bundle_payload(
    recipe,
    plan,
    replay,
    frames,
    *,
    original_recipe_bytes,
    run_id,
    statuses,
    lock_status,
    private,
    allow_unknown_aggregates,
):
    from hhplab.results.bundle import RESULT_TABLE_NAMES

    artifacts: list[BundleArtifact] = []
    input_identities = []
    panel_lineage: dict[str, dict[str, tuple[str, ...]]] = {}
    source_ids = {source for panel in plan.panels for source in panel.source_ids}
    if plan.site_universe is not None:
        source_ids.add(str(plan.site_universe["artifact_id"]))
    source_terms = declared_source_terms(source_ids)
    content_hashes = {}
    for panel_plan in plan.panels:
        frame = frames.get(panel_plan.name)
        if frame is None:
            continue
        contents = _panel_input_bytes(frame, dataset_type="results_input_panel")
        digest = hashlib.sha256(contents).hexdigest()
        claimed_digest = frame.attrs.get("results_content_sha256")
        if claimed_digest is not None and claimed_digest != digest:
            raise ValueError(
                f"panel {panel_plan.name!r} source content hash does not match "
                "the bytes being bundled"
            )
        content_hashes[panel_plan.name] = digest
        input_identities.append(
            InputIdentity(
                panel_plan.name,
                panel_plan.build_spec_hash,
                panel_plan.declared_vintages,
                digest,
            )
        )
        sources = panel_plan.source_ids or (f"unknown-panel:{panel_plan.name}",)
        panel_lineage[panel_plan.name] = {
            column: tuple(sources) for column in frame.columns
        }
        site_source_names = {
            source_binding.panel
            for panel_binding in recipe.panels.values()
            if panel_binding.site_year is not None
            for source_binding in (
                panel_binding.site_year.outcomes.pit,
                panel_binding.site_year.outcomes.hic,
            )
        }
        site_source_names.update(
            binding.panel
            for panel_binding in recipe.panels.values()
            if panel_binding.site_year is not None
            for binding in panel_binding.site_year.covariates
        )
        if panel_plan.name in recipe.results.include_panels or panel_plan.name in site_source_names:
            path = f"panels/{panel_plan.name}.parquet"
            artifacts.append(
                BundleArtifact(
                    path,
                    contents,
                    "panel",
                    ArtifactLineage(path, tuple(sources), ArtifactScope.ROW_LEVEL),
                )
            )

    derived_lineage: dict[str, dict[str, tuple[str, ...]]] = {}
    for panel_plan in plan.panels:
        frame = replay["derived"].get(panel_plan.name)
        if frame is None:
            continue
        derived_names = [
            spec.id
            for spec in recipe.derive
            if not spec.panels or panel_plan.name in spec.panels
        ]
        if not derived_names:
            continue
        input_map = panel_lineage.get(panel_plan.name, {})
        definitions = {
            spec.id: _derive_inputs(spec)
            for spec in recipe.derive
            if spec.id in derived_names
        }
        lineage = derive_lineage(input_map, definitions)
        derived_lineage[panel_plan.name] = lineage
        output = frame[[panel_plan.geo_key, panel_plan.time_key, *derived_names]].copy()
        path = f"derived/{panel_plan.name}.parquet"
        sources = union_lineage(derived_names, lineage)
        base_contract = replay.get("effective_annotations", {}).get(panel_plan.name)
        if isinstance(base_contract, Mapping):
            derived_contract = project_annotation_contract(base_contract, output.columns.tolist())
            provenance_extra = {"annotations": derived_contract}
        else:
            provenance_extra = None
        artifacts.append(
            BundleArtifact(
                path,
                _parquet_bytes(
                    output,
                    dataset_type="results_derived_panel",
                    provenance=provenance_extra,
                ),
                "derived",
                ArtifactLineage(path, sources, ArtifactScope.ROW_LEVEL),
            )
        )

    def _sample_annotation_contract(key: str, membership: pd.DataFrame) -> dict[str, Any] | None:
        analysis_plan = next(item for item in plan.analyses if item.result_key == key)
        panel_name = analysis_plan.panel_name
        base = replay.get("effective_annotations", {}).get(panel_name)
        if not isinstance(base, Mapping):
            return None
        identities = {
            annotation_identity_key(item["id"], item["revision"])
            for item in replay["annotations"]["per_analysis"].get(key, {}).get("keys", [])
        }
        # A sample is a membership artifact, not a copy of the measurement
        # columns. Retain the contributing caveats conservatively rather than
        # inventing bindings to measurement columns absent from this artifact.
        return serialize_annotation_contract(
            base["records"], uncertain_ids=sorted(identities), sources=base.get("sources", {}),
        )

    for key, membership in replay["samples"].items():
        path = f"samples/{key}.parquet"
        sample_contract = (
            _sample_annotation_contract(key, membership) if private else None
        )
        sample_provenance = (
            {"annotations": sample_contract} if sample_contract is not None
            else {"annotation_status": PUBLIC_SAMPLE_ANNOTATION_NOTICE} if not private else None
        )
        artifacts.append(
            BundleArtifact(
                path,
                _parquet_bytes(
                    membership,
                    dataset_type="results_sample",
                    provenance=sample_provenance,
                ),
                "sample",
                ArtifactLineage(path, (), ArtifactScope.SAMPLE),
            )
        )

    site_universes = {}
    if plan.site_universe is not None:
        site_artifacts, site_universes = _site_universe_bundle_artifacts(plan.site_universe)
        artifacts.extend(site_artifacts)

    all_lineage = {
        name: {**panel_lineage.get(name, {}), **derived_lineage.get(name, {})}
        for name in panel_lineage
    }
    table_sources: set[str] = set()
    for item in plan.analyses:
        lineage = all_lineage.get(item.panel_name, {})
        table_sources.update(union_lineage(item.required_columns, lineage))
    result_tables = replay["tables"]
    descriptive_result_scope = _descriptive_result_scope(replay, plan)
    result_table_lineage = {
        name: ArtifactLineage(
            f"results/{name}.parquet",
            tuple(sorted(table_sources)),
            (
                descriptive_result_scope
                if name == "descriptive_stats"
                else ArtifactScope.AGGREGATE
            ),
        )
        for name in RESULT_TABLE_NAMES
    }
    diagnostics = replay["diagnostics"]
    findings = replay["findings"]
    result_map = {
        item.result_key: replay["analyses"].get(item.result_key, {})
        for item in plan.analyses
    }
    analysis_statuses = {}
    for key, result in result_map.items():
        if result.get("status") == "complete":
            analysis_statuses[key] = AnalysisStatusRecord(AnalysisStatus.COMPLETE)
        else:
            error = result.get("error", {})
            analysis_statuses[key] = AnalysisStatusRecord(
                AnalysisStatus.FAILED,
                (
                    ResultError(
                        error.get("code", "analysis_failed"),
                        error.get("message", "analysis failed"),
                        "analysis",
                        error_class="AnalysisError",
                    ),
                ),
            )
    report_analyses = {}
    for key, result in result_map.items():
        summary = {
            name: result.get(name, [])
            for name in ("regression_estimates", "descriptive_stats", "correlations")
        }
        summary["status"] = result.get("status")
        summary["regression_fit"] = result.get("regression_fit")
        if "error" in result:
            summary["error"] = result["error"]
        report_analyses[key] = summary
    claim_text = {finding.id: finding.claim for finding in recipe.findings}
    report_payload = {
        "title": recipe.results.report.title,
        "analyses": report_analyses,
        "diagnostics": diagnostics,
        "findings": findings,
        "finding_claims": claim_text,
        "exclusions": [
            {"result_key": key, "counts": result.get("exclusions", {})}
            for key, result in replay["sample_results"].items()
        ],
        "errors": [
            {"result_key": key, **result.get("error", {})}
            for key, result in result_map.items()
            if result.get("status") != "complete"
        ]
        + [
            {"scope": "diagnostics", "message": replay["diagnostics_error"]}
            for _ in [0]
            if replay.get("diagnostics_error")
        ]
        + [
            {"scope": "findings", "message": replay["findings_error"]}
            for _ in [0]
            if replay.get("findings_error")
        ]
        + [
            {"scope": "run", "message": replay["run_error"]}
            for _ in [0]
            if replay.get("run_error")
        ],
        "limitations": [],
        "withheld": [],
        "notes": _report_notes(
            replay.get("annotations", {}).get("records", ()),
            replay.get("annotations", {}).get("per_analysis", {}),
            {
                identity for identities in replay.get("annotations", {})
                .get("uncertain_by_panel", {}).values() for identity in identities
            },
        ),
        "annotation_conflicts": list(
            replay.get("annotations", {}).get("conflicts", ())
        ),
        "declared_annotations": list(
            replay.get("annotations", {}).get("records", ())
        ),
        "inherited_annotations": [
            {
                "panel": panel,
                "id": record.get("id"),
                "revision": record.get("revision"),
            }
            for panel, records in replay.get("inherited_annotations", {}).items()
            for record in records
        ],
    }
    measures = {
        "bundle_format_version": BUNDLE_FORMAT_VERSION,
        "panels": _measure_entries(
            recipe, plan, frames, derived_lineage, replay.get("derived_measures", {})
        ),
        "input_measures": replay.get("input_measures", {}),
    }
    artifacts.append(
        BundleArtifact(
            "environment/summary.json",
            canonical_bytes(environment_summary()),
            "environment",
            ArtifactLineage("environment/summary.json", (), ArtifactScope.SAMPLE),
        )
    )
    payload = BundlePayload(
        recipe_id=recipe.id,
        run_id=run_id,
        created_at=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        original_recipe=original_recipe_bytes,
        recipe=_recipe_dict(recipe),
        plan=plan.to_dict(),
        statuses=statuses,
        analysis_statuses=analysis_statuses,
        spec_hash=plan.plan_sha256,
        component_hashes=dict(plan.component_hashes),
        lock_status=lock_status,
        input_identities=tuple(input_identities),
        artifacts=tuple(artifacts),
        result_tables=result_tables,
        result_table_lineage=result_table_lineage,
        source_terms=source_terms,
        report_payload=report_payload,
        report_tables=result_tables,
        report_lineage=ArtifactLineage(
            "report/index.html", tuple(sorted(table_sources)), ArtifactScope.AGGREGATE
        ),
        measures=measures,
        input_measures=measures["input_measures"],
        tolerance=Tolerance(),
        private=private,
        allow_unknown_aggregates=allow_unknown_aggregates,
        site_universes=site_universes,
        annotations=replay.get("annotations", {}),
    )
    return payload, content_hashes


def _descriptive_result_scope(replay, plan):
    """Classify descriptive output using actual included sample group sizes."""
    rows = replay.get("tables", {}).get("descriptive_stats", ())
    if not rows:
        return ArtifactScope.AGGREGATE
    analyses = {item.result_key: item for item in plan.analyses}
    sample_results = replay.get("sample_results", {})
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        key = f"{row.get('analysis')}@{row.get('variant')}"
        analysis = analyses.get(key)
        if analysis is None or analysis.method != "descriptive":
            # Unknown row provenance is handled conservatively below.
            group_rows = 0
            statistic = row.get("stat", "unknown")
        else:
            statistic = row.get("stat", "unknown")
            group_rows = 0
            sample_result = sample_results.get(key)
            resolved = sample_result.get("resolved") if isinstance(sample_result, Mapping) else None
            frame = getattr(resolved, "frame", None)
            if isinstance(frame, pd.DataFrame):
                grouping = tuple(analysis.effective_spec.get("by", ()))
                if grouping:
                    group_counts: dict[str, int] = {}
                    for values in frame.loc[:, list(grouping)].itertuples(
                        index=False, name=None
                    ):
                        group = {
                            column: _json_group_value(value)
                            for column, value in zip(grouping, values, strict=True)
                        }
                        group_key = canonical_json(group)
                        group_counts[group_key] = group_counts.get(group_key, 0) + 1
                    group_rows = group_counts.get(row.get("group"), 0)
                else:
                    group_rows = len(frame)
        if descriptive_scope(statistic, group_rows) is ArtifactScope.ROW_LEVEL:
            return ArtifactScope.ROW_LEVEL
    return ArtifactScope.AGGREGATE


def environment_summary() -> dict[str, Any]:
    """Return installed runtime versions for portable replay comparison."""
    packages: dict[str, str] = {}
    for name in ("hhplab", "numpy", "pandas", "pyarrow", "scipy", "statsmodels"):
        try:
            packages[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            continue
    return {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "packages": packages,
    }


def _derive_inputs(spec):
    data = spec.model_dump(mode="python", by_alias=True, exclude_none=True)
    if "of" in data:
        values = data["of"]
        return list(values) if isinstance(values, list) else [values]
    return [data[key] for key in ("num", "den", "pop", "total") if key in data]


def _parquet_bytes(frame, *, dataset_type, provenance=None):
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.Table.from_pandas(frame, preserve_index=False)
    metadata = dict(table.schema.metadata or {})
    block = ProvenanceBlock.from_dict(dict(provenance)) if provenance else ProvenanceBlock()
    block.extra.update({"dataset_type": dataset_type, "row_count": len(frame)})
    document = block.to_dict()
    # Results timestamps belong to the manifest. Do not let wall-clock time
    # change the scientific identity of generated/injected panel content.
    if not provenance or "created_at" not in provenance:
        document.pop("created_at", None)
    metadata[PROVENANCE_KEY] = json.dumps(document, sort_keys=True).encode("utf-8")
    stream = pa.BufferOutputStream()
    pq.write_table(
        table.replace_schema_metadata(metadata), stream, compression="snappy"
    )
    return stream.getvalue().to_pybytes()


def _measure_entries(recipe, plan, frames, derived_lineage, derived_measures):
    entries = []
    for panel in plan.panels:
        frame = frames.get(panel.name)
        if frame is None:
            continue
        for column in frame.columns:
            entries.append(
                {
                    "panel": panel.name,
                    "column": column,
                    "source_lineage": list(panel.source_ids),
                }
            )
        for spec in recipe.derive:
            if spec.panels and panel.name not in spec.panels:
                continue
            entries.append(
                {
                    "panel": panel.name,
                    "column": spec.id,
                    "definition": spec.model_dump(
                        mode="json", by_alias=True, exclude_none=True
                    ),
                    "source_lineage": list(
                        derived_lineage.get(panel.name, {}).get(spec.id, ())
                    ),
                    **derived_measures.get(panel.name, {}).get(spec.id, {}),
                }
            )
    return {"version": 1, "measures": entries}
