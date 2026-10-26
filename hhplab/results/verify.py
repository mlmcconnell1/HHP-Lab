"""Integrity and offline reproducibility checks for portable results bundles."""

from __future__ import annotations

import hashlib
import importlib
import json
import math
import numbers
import os
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any

import pandas as pd

from hhplab.recipe.derive import parse_annotation_contract, serialize_annotation_contract
from hhplab.results.bundle import _distribution_safe_annotations
from hhplab.schema.contracts import PUBLIC_SAMPLE_ANNOTATION_NOTICE, project_annotation_contract
from hhplab.schema.results import RESULT_TABLE_SCHEMAS, ResultsManifest, Tolerance
from hhplab.storage.provenance import read_provenance

REPLAYER = Callable[
    [Mapping[str, Any], Mapping[str, Any], Mapping[str, pd.DataFrame]], Mapping[str, Any]
]
_ZERO_TOLERANCE = Tolerance(atol=0.0, rtol=0.0)
_RESULT_TABLES = (
    "regression_estimates",
    "regression_fit",
    "descriptive_stats",
    "correlations",
)


@dataclass(frozen=True)
class VerificationReport:
    """Three independent verification outcomes and their exit precedence."""

    integrity: str
    reproducibility: str
    unavailable: tuple[Mapping[str, Any], ...] = ()
    partial_reasons: tuple[str, ...] = ()
    differences: tuple[Mapping[str, Any], ...] = ()
    errors: tuple[str, ...] = ()

    @property
    def exit_code(self) -> int:
        if self.integrity == "fail":
            return 1
        if self.reproducibility in {"fail", "not_run"}:
            return 2
        if self.reproducibility == "partial" or self.unavailable or self.partial_reasons:
            return 3
        return 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "integrity": self.integrity,
            "reproducibility": self.reproducibility,
            "unavailable": [dict(item) for item in self.unavailable],
            "partial_reasons": list(self.partial_reasons),
            "differences": [dict(item) for item in self.differences],
            "errors": list(self.errors),
            "exit_code": self.exit_code,
        }


@dataclass(frozen=True)
class _DependencyExclusions:
    """Outputs that cannot be replayed because their bundled panels are withheld."""

    panels: frozenset[str]
    analyses: frozenset[str]
    diagnostics: frozenset[tuple[str, str]]
    findings: frozenset[tuple[str, str]]
    records: tuple[Mapping[str, Any], ...]


def verify_bundle(
    bundle_dir: Path | str,
    *,
    atol: float | None = None,
    rtol: float | None = None,
    replay: REPLAYER | None = None,
    current_environment: Mapping[str, Any] | None = None,
) -> VerificationReport:
    """Verify inventory then replay bundled inputs without consulting source paths.

    A custom ``replay`` callable is primarily useful for focused tests. The
    default delegates all recipe stages to the shared executor replay service.
    Integrity failure always skips replay.
    """
    root = Path(bundle_dir)
    integrity_errors, manifest, inventory = _check_integrity(root)
    if integrity_errors:
        return VerificationReport(
            integrity="fail",
            reproducibility="not_run",
            errors=tuple(integrity_errors),
        )

    assert manifest is not None
    partial_reasons: list[str] = []
    try:
        tolerance = _tolerance_from_bundle(root, atol=atol, rtol=rtol)
        unavailable, unavailable_errors = _unavailable_artifacts(root, inventory, manifest)
        if unavailable_errors:
            raise ValueError("; ".join(unavailable_errors))
        resolved = _read_json(root / "recipe" / "resolved.json")
        expected_site_data = _resolved_site_universe(resolved)
        if expected_site_data is not None:
            artifact_id = expected_site_data.get("artifact_id")
            site_index = manifest.site_universes.get(artifact_id)
            if not isinstance(site_index, Mapping):
                unavailable.append(
                    {
                        "path": f"site_universes:{artifact_id}",
                        "artifact_id": artifact_id,
                        "reason": "required_site_universe_index_unavailable",
                    }
                )
                unavailable.extend(
                    _site_universe_dependency_unavailable(
                        resolved, artifact_id, "required_site_universe_index_unavailable"
                    )
                )
                return VerificationReport(
                    integrity="pass",
                    reproducibility="partial",
                    unavailable=tuple(unavailable),
                    partial_reasons=("site_universe_inputs_unavailable", "unavailable_inputs"),
                )
            if site_index.get("withheld", False):
                unavailable.append(
                    {
                        "path": f"site_universes:{artifact_id}",
                        "artifact_id": artifact_id,
                        "reason": "required_site_universe_inputs_withheld",
                    }
                )
                unavailable.extend(
                    _site_universe_dependency_unavailable(
                        resolved, artifact_id, "required_site_universe_inputs_withheld"
                    )
                )
                return VerificationReport(
                    integrity="pass",
                    reproducibility="partial",
                    unavailable=tuple(unavailable),
                    partial_reasons=("site_universe_inputs_withheld", "unavailable_inputs"),
                )
        else:
            site_index = None
        if unavailable_errors:
            raise ValueError("; ".join(unavailable_errors))
        environment_record = _read_optional_json(root / "environment" / "summary.json")
        current = dict(current_environment) if current_environment is not None else _environment()
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
        return VerificationReport(
            integrity="pass",
            reproducibility="fail",
            errors=(f"verification_metadata_invalid: {exc}",),
        )
    if environment_record is None or environment_record != current:
        partial_reasons.append("environment_differs")

    try:
        execution = _execution_with_measures(root)
        if execution.get("spec_hash") != manifest.spec_hash:
            raise ValueError("execution spec_hash does not match manifest spec_hash")
        panels = _load_panels(root, execution, inventory, manifest)
        exclusions = _dependency_exclusions(resolved, unavailable)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return VerificationReport(
            integrity="pass",
            reproducibility="fail",
            unavailable=tuple(unavailable),
            partial_reasons=tuple(partial_reasons),
            errors=(f"replay_inputs_invalid: {exc}",),
        )
    known_unavailable_paths = {item.get("path") for item in unavailable}
    for record in exclusions.records:
        path = record.get("path")
        if path not in known_unavailable_paths:
            unavailable.append(record)
            known_unavailable_paths.add(path)
    if unavailable:
        partial_reasons.append("unavailable_inputs")

    replayer = replay or _default_replay
    try:
        if replay is None and site_index is not None:
            from hhplab.results.runner import site_universe_inputs_from_bundle

            resolution, evidence_payload = site_universe_inputs_from_bundle(
                root, site_index, expected_site_data
            )
            replayed = replayer(
                resolved,
                execution,
                panels,
                site_universe_resolution=resolution,
                boundary_evidence_payload=evidence_payload,
            )
        else:
            replayed = replayer(resolved, execution, panels)
    except (ImportError, ModuleNotFoundError) as exc:
        return VerificationReport(
            integrity="pass",
            reproducibility="not_run",
            unavailable=tuple(unavailable),
            partial_reasons=tuple(partial_reasons),
            errors=(f"replay_service_unavailable: {exc}",),
        )
    except Exception as exc:  # A replay error is a reproducibility failure.
        return VerificationReport(
            integrity="pass",
            reproducibility="fail",
            unavailable=tuple(unavailable),
            partial_reasons=tuple(partial_reasons),
            errors=(f"replay_failed: {type(exc).__name__}: {exc}",),
        )

    try:
        differences = _compare_replay(
            root,
            resolved,
            replayed,
            execution,
            manifest,
            tolerance,
            unavailable=unavailable,
            exclusions=exclusions,
        )
    except (OSError, ValueError, KeyError, TypeError) as exc:
        return VerificationReport(
            integrity="pass",
            reproducibility="fail",
            unavailable=tuple(unavailable),
            partial_reasons=tuple(partial_reasons),
            errors=(f"bundled_outputs_invalid: {exc}",),
        )
    if differences:
        reproducibility = "fail"
    elif partial_reasons:
        reproducibility = "partial"
    else:
        reproducibility = "pass"
    return VerificationReport(
        integrity="pass",
        reproducibility=reproducibility,
        unavailable=tuple(unavailable),
        partial_reasons=tuple(partial_reasons),
        differences=tuple(differences),
    )


def _check_integrity(
    root: Path,
) -> tuple[list[str], ResultsManifest | None, dict[str, Mapping[str, Any]]]:
    errors: list[str] = []
    if root.is_symlink() or not root.is_dir():
        return ["bundle root must be a real directory"], None, {}
    try:
        manifest_bytes = _read_regular_file(root, "manifest.json")
        hash_bytes = _read_regular_file(root, "manifest.sha256")
    except (OSError, ValueError) as exc:
        return [f"manifest_unreadable: {exc}"], None, {}
    expected_manifest_hash = hashlib.sha256(manifest_bytes).hexdigest()
    hash_fields = hash_bytes.decode("ascii", errors="replace").strip().split()
    if hash_fields != [expected_manifest_hash, "manifest.json"]:
        errors.append("manifest.sha256 does not match manifest.json")
    try:
        manifest_payload = json.loads(manifest_bytes)
        manifest = ResultsManifest.from_dict(manifest_payload)
    except (json.JSONDecodeError, TypeError, ValueError) as exc:
        return errors + [f"invalid_manifest: {exc}"], None, {}

    inventory = {str(entry["path"]): entry for entry in manifest.files}
    for relative_path, record in inventory.items():
        try:
            content = _read_regular_file(root, relative_path)
        except (OSError, ValueError) as exc:
            errors.append(f"file_unreadable:{relative_path}: {exc}")
            continue
        if len(content) != record["bytes"]:
            errors.append(f"byte_count_mismatch:{relative_path}")
        if hashlib.sha256(content).hexdigest() != record["sha256"]:
            errors.append(f"sha256_mismatch:{relative_path}")

    observed: set[str] = set()
    for directory, dirnames, filenames in os.walk(root, followlinks=False):
        base = Path(directory)
        for name in [*dirnames, *filenames]:
            path = base / name
            if path.is_symlink():
                errors.append(f"symlink_forbidden:{path.relative_to(root).as_posix()}")
            elif path.is_file():
                relative = path.relative_to(root).as_posix()
                if relative not in {"manifest.json", "manifest.sha256"}:
                    observed.add(relative)
    for relative in sorted(observed - inventory.keys()):
        errors.append(f"unlisted_file:{relative}")
    for relative in sorted(inventory.keys() - observed):
        if not any(error.startswith(f"file_unreadable:{relative}:") for error in errors):
            errors.append(f"missing_file:{relative}")
    return errors, manifest, inventory


def _read_regular_file(root: Path, relative: str) -> bytes:
    _validate_relative_bundle_path(relative)
    posix_path = PurePosixPath(relative)
    candidate = root
    for part in posix_path.parts:
        candidate = candidate / part
        if candidate.is_symlink():
            raise ValueError(f"symlink in bundle path: {relative!r}")
    if not candidate.is_file():
        raise ValueError(f"not a regular file: {relative!r}")
    return candidate.read_bytes()


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected a JSON object in {path}")
    return value


def _read_optional_json(path: Path) -> dict[str, Any] | None:
    return _read_json(path) if path.is_file() else None


def _load_panels(
    root: Path,
    execution: Mapping[str, Any],
    inventory: Mapping[str, Mapping[str, Any]],
    manifest: ResultsManifest,
) -> dict[str, pd.DataFrame]:
    panel_map = execution.get("panels")
    if not isinstance(panel_map, Mapping):
        raise ValueError("recipe/execution.json must contain a panels mapping")
    identities = {identity.panel: identity.content_sha256 for identity in manifest.input_identities}
    frames: dict[str, pd.DataFrame] = {}
    for name, relative_path in panel_map.items():
        if not isinstance(name, str) or not isinstance(relative_path, str):
            raise ValueError("execution panel names and paths must be strings")
        if relative_path != f"panels/{name}.parquet":
            raise ValueError(f"execution panel path does not match panel name: {name!r}")
        if relative_path not in inventory:
            raise ValueError(f"execution panel path is not inventoried: {relative_path!r}")
        content = _read_regular_file(root, relative_path)
        if name not in identities:
            raise ValueError(f"bundled panel has no manifest input identity: {name!r}")
        if hashlib.sha256(content).hexdigest() != identities[name]:
            raise ValueError(f"bundled panel content differs from its input identity: {name!r}")
        from io import BytesIO

        frame = pd.read_parquet(BytesIO(content))
        import pyarrow.parquet as pq

        from hhplab.storage.provenance import PROVENANCE_KEY, ProvenanceBlock

        metadata = pq.read_schema(BytesIO(content)).metadata or {}
        if PROVENANCE_KEY in metadata:
            provenance = ProvenanceBlock.from_json(metadata[PROVENANCE_KEY].decode("utf-8"))
            frame.attrs["results_panel_provenance"] = provenance.to_dict()
        frame.attrs["results_source_parquet_bytes"] = content
        frame.attrs["results_content_sha256"] = hashlib.sha256(content).hexdigest()
        frames[name] = frame
    return frames


def _execution_with_measures(root: Path) -> dict[str, Any]:
    """Restore the source measure metadata recorded beside the replay plan."""
    execution = _read_json(root / "recipe" / "execution.json")
    measures_path = "measures.json"
    if not (root / measures_path).is_file():
        return execution
    measures = _read_regular_file(root, measures_path)
    payload = json.loads(measures)
    if not isinstance(payload, Mapping):
        raise ValueError("measures.json must contain an object")
    recorded = payload.get("input_measures", {})
    if not isinstance(recorded, Mapping):
        raise ValueError("measures.json input_measures must contain an object")
    # The execution copy is derived from in-memory frame attrs and may be
    # incomplete. The measures document is the portable source of truth.
    execution["input_measures"] = dict(recorded)
    return execution


def _unavailable_artifacts(
    root: Path,
    inventory: Mapping[str, Mapping[str, Any]],
    manifest: ResultsManifest,
) -> tuple[list[dict[str, Any]], list[str]]:
    path = root / "withheld.json"
    if not path.is_file():
        return [], []
    payload = _read_json(path)
    records = payload.get("withheld", [])
    if not isinstance(records, Sequence) or isinstance(records, (str, bytes)):
        raise ValueError("withheld.json 'withheld' must be an array")
    unavailable = [dict(item) for item in records if isinstance(item, Mapping)]
    errors: list[str] = []
    if unavailable and manifest.statuses.distribution_status.value != "withheld":
        errors.append("withheld artifacts require distribution_status=withheld")
    for item in unavailable:
        relative = item.get("path")
        if not isinstance(relative, str):
            errors.append("withheld artifact path must be a string")
            continue
        try:
            _validate_relative_bundle_path(relative)
        except ValueError as exc:
            errors.append(f"invalid withheld path {relative!r}: {exc}")
            continue
        if relative in inventory or (root / relative).exists() or (root / relative).is_symlink():
            errors.append(f"withheld artifact is present in bundle: {relative}")
    withheld_paths = {
        item.get("path") for item in unavailable if isinstance(item.get("path"), str)
    }
    for artifact_id, entry in manifest.site_universes.items():
        if entry.get("withheld", False):
            indexed = {
                path
                for key, path in entry.items()
                if key not in {"schema_version", "withheld"}
            }
            missing_records = indexed - withheld_paths
            if missing_records:
                errors.append(
                    f"withheld site-universe {artifact_id!r} lacks withheld.json records "
                    f"for {sorted(missing_records)}"
                )
    return unavailable, errors


def _validate_relative_bundle_path(relative: str) -> None:
    posix_path = PurePosixPath(relative)
    windows_path = PureWindowsPath(relative)
    if (
        not relative
        or posix_path.is_absolute()
        or windows_path.is_absolute()
        or windows_path.drive
        or "\\" in relative
        or posix_path.as_posix() != relative
        or any(part in {"", ".", ".."} for part in posix_path.parts)
    ):
        raise ValueError("path must be normalized, relative, and contained")


def _dependency_exclusions(
    resolved: Mapping[str, Any], withheld: Sequence[Mapping[str, Any]]
) -> _DependencyExclusions:
    """Derive unavailable outputs only from withheld panel records and plan edges."""
    unavailable_panels = set()
    for item in withheld:
        path = item.get("path")
        if isinstance(path, str):
            parts = PurePosixPath(path).parts
            if len(parts) == 2 and parts[0] == "panels" and parts[1].endswith(".parquet"):
                unavailable_panels.add(parts[1].removesuffix(".parquet"))
    plan = resolved.get("plan", {})
    recipe = resolved.get("recipe", {})
    if not isinstance(plan, Mapping) or not isinstance(recipe, Mapping):
        return _DependencyExclusions(
            frozenset(unavailable_panels), frozenset(), frozenset(), frozenset(), ()
        )
    analysis_plans = plan.get("analyses", ())
    if not isinstance(analysis_plans, Sequence):
        analysis_plans = ()
    analysis_panel = {
        item.get("result_key"): item.get("panel_name")
        for item in analysis_plans
        if isinstance(item, Mapping)
        and isinstance(item.get("result_key"), str)
        and isinstance(item.get("panel_name"), str)
    }
    site_source_dependencies: dict[str, set[str]] = {}
    panel_bindings = recipe.get("panels", {})
    if isinstance(panel_bindings, Mapping):
        for panel_name, binding in panel_bindings.items():
            if not isinstance(panel_name, str) or not isinstance(binding, Mapping):
                continue
            site_year = binding.get("site_year")
            outcomes = site_year.get("outcomes") if isinstance(site_year, Mapping) else None
            if not isinstance(outcomes, Mapping):
                continue
            source_names = {
                value.get("panel")
                for value in outcomes.values()
                if isinstance(value, Mapping) and isinstance(value.get("panel"), str)
            }
            covariates = site_year.get("covariates", ())
            if isinstance(covariates, Sequence) and not isinstance(covariates, str | bytes):
                source_names.update(
                    value.get("panel")
                    for value in covariates
                    if isinstance(value, Mapping) and isinstance(value.get("panel"), str)
                )
            site_source_dependencies[panel_name] = source_names
    for panel_name, sources in site_source_dependencies.items():
        if sources & unavailable_panels:
            unavailable_panels.add(panel_name)
    unavailable_analyses = {
        key for key, panel in analysis_panel.items() if panel in unavailable_panels
    }
    records: list[Mapping[str, Any]] = [
        {
            "path": f"panels/{panel}.parquet",
            "panel": panel,
            "reason": "withheld_input",
        }
        for panel in sorted(unavailable_panels - set(site_source_dependencies))
    ]
    for panel_name, sources in site_source_dependencies.items():
        missing_sources = sources & unavailable_panels
        if missing_sources:
            records.append(
                {
                    "path": f"panels/{panel_name}.parquet",
                    "panel": panel_name,
                    "reason": "required_site_source_withheld",
                    "sources": sorted(missing_sources),
                }
            )
    for result_key in sorted(unavailable_analyses):
        records.append(
            {
                "path": f"analysis:{result_key}",
                "result_key": result_key,
                "reason": "required_panel_withheld",
            }
        )
    recipe_derivations = recipe.get("derive", ())
    for panel in sorted(unavailable_panels):
        has_derivation = (
            any(
                isinstance(item, Mapping)
                and (not item.get("panels") or panel in item.get("panels", ()))
                for item in recipe_derivations
            )
            if isinstance(recipe_derivations, Sequence)
            else False
        )
        if has_derivation:
            records.append(
                {
                    "path": f"derived/{panel}.parquet",
                    "panel": panel,
                    "reason": "required_panel_withheld",
                }
            )
    for result_key in sorted(unavailable_analyses):
        records.append(
            {
                "path": f"samples/{result_key}.parquet",
                "result_key": result_key,
                "reason": "required_panel_withheld",
            }
        )

    variants = sorted(
        {
            item.get("variant_id")
            for item in analysis_plans
            if isinstance(item, Mapping) and isinstance(item.get("variant_id"), str)
        }
    )
    unavailable_diagnostics: set[tuple[str, str]] = set()
    diagnostics = recipe.get("diagnostics", ())
    for diagnostic in diagnostics if isinstance(diagnostics, Sequence) else ():
        if not isinstance(diagnostic, Mapping) or not isinstance(diagnostic.get("id"), str):
            continue
        raw_variants = diagnostic.get("variants", "all")
        diagnostic_variants = (
            ["base"]
            if diagnostic.get("op") == "column_identity"
            else variants
            if raw_variants == "all"
            else raw_variants
        )
        if not isinstance(diagnostic_variants, Sequence) or isinstance(
            diagnostic_variants, (str, bytes)
        ):
            continue
        for variant in diagnostic_variants:
            if not isinstance(variant, str):
                continue
            missing_panel = (
                diagnostic.get("op") == "column_identity"
                and diagnostic.get("panel") in unavailable_panels
            )
            terms = diagnostic.get("terms", ())
            missing_analysis = any(
                isinstance(term, Mapping)
                and f"{term.get('analysis')}@{variant}" in unavailable_analyses
                for term in terms
            )
            if missing_panel or missing_analysis:
                identifier = diagnostic["id"]
                unavailable_diagnostics.add((identifier, variant))
                records.append(
                    {
                        "path": f"diagnostics.json#{identifier}@{variant}",
                        "diagnostic": identifier,
                        "variant": variant,
                        "reason": "required_panel_withheld",
                    }
                )

    unavailable_findings: set[tuple[str, str]] = set()
    findings = recipe.get("findings", ())
    for finding in findings if isinstance(findings, Sequence) else ():
        if not isinstance(finding, Mapping) or not isinstance(finding.get("id"), str):
            continue
        sources = finding.get("criteria_source", {})
        criterion_sources = []
        if isinstance(sources, Mapping):
            estimate = sources.get("estimate")
            if isinstance(estimate, Mapping):
                criterion_sources.append(estimate)
            stats = sources.get("stats", {})
            if isinstance(stats, Mapping):
                criterion_sources.extend(
                    source for source in stats.values() if isinstance(source, Mapping)
                )
        source_analyses = {
            source.get("analysis")
            for source in criterion_sources
            if isinstance(source.get("analysis"), str)
        }
        identifier = finding["id"]
        if any(f"{analysis}@base" in unavailable_analyses for analysis in source_analyses):
            unavailable_findings.add((identifier, "base"))
            records.append(
                {
                    "path": f"findings.json#{identifier}@base",
                    "finding": identifier,
                    "evaluation": "base",
                    "reason": "required_panel_withheld",
                }
            )
        robustness = finding.get("robustness", {})
        requested = robustness.get("variants", ()) if isinstance(robustness, Mapping) else ()
        if isinstance(requested, Sequence) and not isinstance(requested, (str, bytes)):
            for variant in requested:
                if isinstance(variant, str) and any(
                    f"{analysis}@{variant}" in unavailable_analyses for analysis in source_analyses
                ):
                    unavailable_findings.add((identifier, variant))
                    records.append(
                        {
                            "path": f"findings.json#{identifier}@{variant}",
                            "finding": identifier,
                            "evaluation": variant,
                            "reason": "required_panel_withheld",
                        }
                    )
    return _DependencyExclusions(
        frozenset(unavailable_panels),
        frozenset(unavailable_analyses),
        frozenset(unavailable_diagnostics),
        frozenset(unavailable_findings),
        tuple(records),
    )


def _tolerance_from_bundle(root: Path, *, atol: float | None, rtol: float | None) -> Tolerance:
    parameters = _read_optional_json(root / "recipe" / "execution.json") or {}
    recorded = parameters.get("tolerance", {"atol": 1e-10, "rtol": 1e-9})
    if not isinstance(recorded, Mapping):
        raise ValueError("execution tolerance must be an object")
    return Tolerance(
        atol=recorded.get("atol", 1e-10) if atol is None else atol,
        rtol=recorded.get("rtol", 1e-9) if rtol is None else rtol,
    )


def _environment() -> dict[str, Any]:
    from hhplab.results.runner import environment_summary

    return environment_summary()


def _default_replay(
    resolved_spec: Mapping[str, Any],
    execution: Mapping[str, Any],
    panel_frames: Mapping[str, pd.DataFrame],
    *,
    site_universe_resolution=None,
    boundary_evidence_payload=None,
) -> Mapping[str, Any]:
    replay_service = importlib.import_module("hhplab.results.runner").replay_results
    return replay_service(
        resolved_spec,
        execution,
        panel_frames,
        site_universe_resolution=site_universe_resolution,
        boundary_evidence_payload=boundary_evidence_payload,
    )


def _resolved_site_universe(resolved):
    plan = resolved.get("plan", {}) if isinstance(resolved, Mapping) else {}
    site_data = plan.get("site_universe") if isinstance(plan, Mapping) else None
    return site_data if isinstance(site_data, Mapping) else None


def _site_universe_dependency_unavailable(resolved, artifact_id, reason):
    """Name site-year panels and results that cannot be replayed without inputs."""
    recipe = resolved.get("recipe", {}) if isinstance(resolved, Mapping) else {}
    plan = resolved.get("plan", {}) if isinstance(resolved, Mapping) else {}
    if not isinstance(recipe, Mapping) or not isinstance(plan, Mapping):
        return []
    bindings = recipe.get("panels", {})
    site_panels = {
        name
        for name, binding in bindings.items()
        if isinstance(name, str)
        and isinstance(binding, Mapping)
        and isinstance(binding.get("site_year"), Mapping)
    } if isinstance(bindings, Mapping) else set()
    records: list[Mapping[str, Any]] = []
    for name in sorted(site_panels):
        records.append(
            {
                "path": f"panels/{name}.parquet",
                "panel": name,
                "artifact_id": artifact_id,
                "reason": reason,
            }
        )
    analyses = plan.get("analyses", ())
    if isinstance(analyses, Sequence) and not isinstance(analyses, (str, bytes)):
        for item in analyses:
            if (
                isinstance(item, Mapping)
                and item.get("panel_name") in site_panels
                and isinstance(item.get("result_key"), str)
            ):
                result_key = item["result_key"]
                records.extend(
                    (
                        {
                            "path": f"analysis:{result_key}",
                            "result_key": result_key,
                            "artifact_id": artifact_id,
                            "reason": reason,
                        },
                        {
                            "path": f"samples/{result_key}.parquet",
                            "result_key": result_key,
                            "artifact_id": artifact_id,
                            "reason": reason,
                        },
                    )
                )
    return records


def _compare_replay(
    root: Path,
    resolved: Mapping[str, Any],
    replayed: Mapping[str, Any],
    execution: Mapping[str, Any],
    manifest: ResultsManifest,
    tolerance: Tolerance,
    *,
    unavailable: Sequence[Mapping[str, Any]],
    exclusions: _DependencyExclusions,
) -> list[dict[str, Any]]:
    """Compare all replayed stage outputs with their bundled copies."""
    differences: list[dict[str, Any]] = []
    tables = replayed.get("tables", {})
    if not isinstance(tables, Mapping):
        return [{"path": "results/results.json", "reason": "replay_tables_missing"}]
    if set(tables) != set(_RESULT_TABLES):
        differences.append(
            {"path": "results/results.json", "reason": "replay_table_inventory_differs"}
        )
    unavailable_paths = {str(item.get("path")) for item in unavailable if item.get("path")}
    panel_errors = replayed.get("panel_errors", {})
    if isinstance(panel_errors, Mapping):
        for panel_name, message in panel_errors.items():
            if panel_name not in exclusions.panels:
                differences.append(
                    {
                        "path": f"panels/{panel_name}",
                        "reason": "unexpected_replay_panel_error",
                        "message": str(message),
                    }
                )
    available_tables = {
        name for name in _RESULT_TABLES if f"results/{name}.parquet" not in unavailable_paths
    }
    results_json_path = "results/results.json"
    results_json_present = (root / results_json_path).is_file()
    results_json = _read_json(root / results_json_path) if results_json_present else {}
    if (
        results_json_present
        and results_json_path not in unavailable_paths
        and set(results_json) != available_tables
    ):
        differences.append({"path": results_json_path, "reason": "table_inventory_differs"})
    if not results_json_present and results_json_path not in unavailable_paths:
        differences.append({"path": results_json_path, "reason": "missing_copy"})
    for table_name in _RESULT_TABLES:
        expected = tables.get(table_name)
        if expected is None:
            differences.append({"path": f"results/{table_name}", "reason": "replay_table_missing"})
            continue
        if results_json_path in unavailable_paths:
            pass
        elif f"results/{table_name}.parquet" in unavailable_paths:
            if table_name in results_json:
                differences.append(
                    {
                        "path": f"results/results.json:{table_name}",
                        "reason": "withheld_table_present_in_json_copy",
                    }
                )
        elif table_name not in results_json:
            differences.append(
                {"path": f"results/results.json:{table_name}", "reason": "missing_copy"}
            )
        else:
            _compare_result_table(
                _without_analyses(expected, exclusions.analyses),
                _without_analyses(results_json[table_name], exclusions.analyses),
                table_name,
                f"results/results.json:{table_name}",
                tolerance,
                differences,
            )
        parquet_path = root / "results" / f"{table_name}.parquet"
        relative_parquet = parquet_path.relative_to(root).as_posix()
        if relative_parquet in unavailable_paths:
            continue
        if parquet_path.is_file():
            bundled = _read_parquet_records(root, parquet_path.relative_to(root).as_posix())
            _compare_result_table(
                _without_analyses(expected, exclusions.analyses),
                _without_analyses(bundled, exclusions.analyses),
                table_name,
                f"results/{table_name}.parquet",
                tolerance,
                differences,
            )
        else:
            differences.append({"path": f"results/{table_name}.parquet", "reason": "missing_copy"})

    _compare_json_artifact(
        root,
        "diagnostics.json",
        replayed.get("diagnostics"),
        tolerance,
        differences,
        unavailable_paths,
        exclusions,
        "diagnostics",
    )
    _compare_json_artifact(
        root,
        "findings.json",
        replayed.get("findings"),
        tolerance,
        differences,
        unavailable_paths,
        exclusions,
        "findings",
    )
    _compare_sample_artifacts(
        root,
        execution,
        replayed.get("samples", {}),
        tolerance,
        differences,
        unavailable_paths,
        exclusions.analyses,
    )
    _compare_analysis_statuses(
        manifest, replayed.get("analyses", {}), exclusions.analyses, differences
    )
    _compare_derived_artifacts(
        root,
        execution,
        replayed.get("derived", {}),
        resolved,
        tolerance,
        differences,
        unavailable_paths,
        exclusions.panels,
    )
    _compare_annotations_artifact(
        root, replayed, manifest, unavailable_paths, differences, exclusions
    )
    _compare_derived_annotation_contracts(
        root, execution, replayed, unavailable_paths, differences
    )
    _compare_sample_annotation_contracts(
        root, resolved, execution, replayed, manifest, unavailable_paths, differences
    )
    plan = resolved.get("plan", {})
    panel_plans = plan.get("panels", ()) if isinstance(plan, Mapping) else ()
    replay_inputs = replayed.get("input_panels", {})
    if _resolved_site_universe(resolved) is not None:
        for panel_plan in panel_plans:
            if not isinstance(panel_plan, Mapping) or panel_plan.get("pipeline_id") is not None:
                continue
            name = panel_plan.get("name")
            if not isinstance(name, str) or name in exclusions.panels:
                continue
            expected_panel = (
                replay_inputs.get(name) if isinstance(replay_inputs, Mapping) else None
            )
            relative_path = execution.get("panels", {}).get(name)
            if not isinstance(relative_path, str) or relative_path in unavailable_paths:
                continue
            actual_panel = _read_parquet_frame(root, relative_path)
            if not isinstance(expected_panel, pd.DataFrame):
                differences.append(
                    {"path": f"panels/{name}.parquet", "reason": "replay_output_missing"}
                )
            else:
                _compare_at(
                    expected_panel,
                    actual_panel,
                    f"panels/{name}.parquet",
                    tolerance,
                    differences,
                )
    return differences


def _read_parquet_provenance(root: Path, relative_path: str) -> dict[str, Any] | None:
    """Read embedded provenance of a bundled parquet artifact."""
    path = root / relative_path
    if not path.is_file():
        return None
    provenance = read_provenance(path)
    return provenance.to_dict() if provenance is not None else None


def _normalize_annotation_contract(value: Any) -> dict[str, Any] | None:
    """Normalize a persisted annotation container to the canonical v1 contract."""
    if value is None:
        return None
    parsed = parse_annotation_contract(value)
    return serialize_annotation_contract(
        parsed["records"],
        bindings=parsed["bindings"],
        uncertain_ids=parsed["uncertain_ids"],
        sources=parsed.get("sources", {}),
    )


def _compare_annotations_artifact(
    root: Path,
    replay: Mapping[str, Any],
    manifest: ResultsManifest,
    unavailable_paths: set[str],
    differences: list[dict[str, Any]],
    exclusions: _DependencyExclusions | None = None,
) -> None:
    """Semantically compare the versioned annotation artifact against replay.

    Metadata-only edits to ``annotations.json`` (or to embedded panel
    contracts) change the bundled payload without touching the manifest; this
    comparison recomputes the expected payload from the replay and fails
    reproducibility when the semantic content differs. Public (safe-filtered)
    bundles are compared against the same distribution-safe view of the
    replayed payload.

    Only the scientifically replayable slice is compared: per-analysis
    membership for analyses whose bundled inputs are unavailable (excluded)
    cannot be reproduced offline, so those ``per_analysis`` entries are
    removed from both sides before comparison — while definitional records,
    provenance, and membership for available analyses stay strict.
    """
    relative_path = "annotations.json"
    if relative_path in unavailable_paths:
        return
    path = root / relative_path
    expected = replay.get("annotations", {})
    if not path.is_file():
        if not expected:
            # Legacy/synthetic bundles with no annotation payload at all have
            # nothing to compare; absence on both sides is not a difference.
            return
        differences.append({"path": relative_path, "reason": "missing_copy"})
        return
    actual = _read_json(path)
    if exclusions is not None:
        expected = _restrict_annotations_to_replayable(expected, exclusions)
        actual = _restrict_annotations_to_replayable(actual, exclusions)
    if not manifest.private:
        withheld = _read_optional_json(root / "withheld.json") or {}
        expected = _distribution_safe_annotations(expected, private=False,
                                                  withheld=withheld.get("withheld", []))
        if exclusions is not None and exclusions.analyses:
            # Source-derived public explanations cannot be replayed if their
            # authoritative inputs are withheld. Availability already marks
            # verification partial. Validate the public wrapper, not hidden lineage.
            if isinstance(actual, Mapping) and actual.get("distribution_status") == "withheld":
                actual = dict(actual)
                expected = dict(expected)
                actual.pop("public_notes", None)
                expected.pop("public_notes", None)
    _compare_at(expected, actual, relative_path, _ZERO_TOLERANCE, differences)


def _restrict_annotations_to_replayable(
    annotations: Any, exclusions: _DependencyExclusions
) -> Any:
    """Drop per-analysis membership for analyses that cannot be replayed.

    ``per_analysis`` entries are keyed by analysis result key (``analysis@variant``).
    Analyses whose inputs are unavailable offline are excluded via
    ``exclusions.analyses``; their membership cannot be reproduced from the
    bundle and is therefore not compared. All other content is compared
    strictly.
    """
    if not isinstance(annotations, Mapping):
        return annotations
    restricted = dict(annotations)
    for field in ("per_analysis", "active_per_analysis"):
        per_analysis = restricted.get(field)
        if isinstance(per_analysis, Mapping) and exclusions.analyses:
            restricted[field] = {
                key: value for key, value in per_analysis.items() if key not in exclusions.analyses
            }
    return restricted


def _compare_derived_annotation_contracts(
    root: Path,
    execution: Mapping[str, Any],
    replay: Mapping[str, Any],
    unavailable_paths: set[str],
    differences: list[dict[str, Any]],
) -> None:
    """Compare embedded derived-panel annotation contracts against replay output."""
    derived_paths = execution.get("derived", {})
    if not isinstance(derived_paths, Mapping):
        return
    effective = replay.get("effective_annotations", {})
    if not isinstance(effective, Mapping):
        return
    for panel_name, relative_path in sorted(derived_paths.items()):
        if not isinstance(relative_path, str) or relative_path in unavailable_paths:
            continue
        expected_contract = effective.get(panel_name)
        if isinstance(expected_contract, Mapping):
            expected_contract = project_annotation_contract(
                expected_contract, pd.read_parquet(root / relative_path).columns.tolist()
            )
        try:
            embedded_provenance = _read_parquet_provenance(root, relative_path)
            embedded_extra = embedded_provenance.get("extra", {}) if embedded_provenance else {}
            actual_contract = _normalize_annotation_contract(embedded_extra.get("annotations"))
            expected_normalized = _normalize_annotation_contract(expected_contract)
        except (ValueError, TypeError) as exc:
            differences.append({
                "path": relative_path, "reason": f"invalid_annotation_contract: {exc}",
            })
            continue
        if actual_contract != expected_normalized:
            differences.append(
                {
                    "path": relative_path,
                    "reason": "annotation_contract_differs",
                    "expected": expected_normalized,
                    "actual": actual_contract,
                }
            )


def _compare_sample_annotation_contracts(
    root, resolved, execution, replay, manifest, unavailable_paths, differences,
):
    panel_by_analysis = {
        item["result_key"]: item["panel_name"]
        for item in resolved.get("plan", {}).get("analyses", [])
    }
    for key, relative in execution.get("samples", {}).items():
        if not (root / relative).is_file():
            continue
        expected = None
        base = replay.get("effective_annotations", {}).get(panel_by_analysis.get(key))
        if manifest.private and isinstance(base, Mapping):
            entry = replay.get("annotations", {}).get("per_analysis", {}).get(key, {})
            expected = serialize_annotation_contract(
                base["records"], uncertain_ids=[
                    f"{item['id']}#r{item['revision']}" for item in entry.get("keys", [])
                ],
                sources=base.get("sources", {}),
            )
        try:
            provenance = _read_parquet_provenance(root, relative) or {}
            actual = _normalize_annotation_contract(provenance.get("extra", {}).get("annotations"))
            if "effective_annotations" in replay:
                expected_notice = PUBLIC_SAMPLE_ANNOTATION_NOTICE if not manifest.private else None
                if provenance.get("extra", {}).get("annotation_status") != expected_notice:
                    differences.append({"path": relative, "reason": "annotation_notice_differs"})
        except (ValueError, TypeError) as exc:
            differences.append({"path": relative, "reason": f"invalid_annotation_contract: {exc}"})
            continue
        if manifest.private and (
            relative in unavailable_paths or key not in replay.get("samples", {})
        ):
            continue
        if actual != expected:
            differences.append({"path": relative, "reason": "sample_annotation_contract_differs"})


def _compare_json_artifact(
    root: Path,
    relative_path: str,
    expected: Any,
    tolerance: Tolerance,
    differences: list[dict[str, Any]],
    unavailable_paths: set[str],
    exclusions: _DependencyExclusions,
    kind: str,
) -> None:
    path = root / relative_path
    if relative_path in unavailable_paths:
        return
    if expected is None:
        differences.append({"path": path.name, "reason": "replay_output_missing"})
    elif not path.is_file():
        differences.append({"path": path.name, "reason": "missing_copy"})
    else:
        actual = _read_json(path)
        if kind == "diagnostics":
            expected = _without_diagnostics(expected, exclusions.diagnostics)
            actual = _without_diagnostics(actual, exclusions.diagnostics)
        else:
            expected = _without_findings(expected, exclusions.findings)
            actual = _without_findings(actual, exclusions.findings)
        _compare_at(expected, actual, path.name, tolerance, differences)


def _without_analyses(rows: Any, unavailable: frozenset[str]) -> Any:
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
        return rows
    return [
        row
        for row in rows
        if not isinstance(row, Mapping)
        or f"{row.get('analysis')}@{row.get('variant')}" not in unavailable
    ]


def _compare_result_table(
    expected: Any,
    actual: Any,
    table_name: str,
    path: str,
    tolerance: Tolerance,
    differences: list[dict[str, Any]],
) -> None:
    if (
        not isinstance(expected, Sequence)
        or isinstance(expected, (str, bytes))
        or not isinstance(actual, Sequence)
        or isinstance(actual, (str, bytes))
    ):
        _compare_at(expected, actual, path, tolerance, differences)
        return
    if len(expected) != len(actual):
        differences.append({"path": path, "reason": "sequence_length_differs"})
        return
    schema = RESULT_TABLE_SCHEMAS[table_name]
    for index, (left, right) in enumerate(zip(expected, actual, strict=True)):
        row_path = f"{path}[{index}]"
        if not isinstance(left, Mapping) or not isinstance(right, Mapping):
            _compare_at(left, right, row_path, tolerance, differences)
            continue
        if left.keys() != right.keys():
            differences.append({"path": row_path, "reason": "mapping_keys_differ"})
            continue
        if set(left) != set(RESULT_TABLE_SCHEMAS[table_name].column_names):
            _compare_at(left, right, row_path, tolerance, differences)
            continue
        for column in schema.columns:
            value_path = f"{row_path}.{column.name}"
            if column.value_type == "number":
                _compare_numeric_value(
                    left[column.name], right[column.name], value_path, tolerance, differences
                )
            else:
                _compare_at(
                    left[column.name], right[column.name], value_path, tolerance, differences
                )


def _compare_numeric_value(
    expected: Any,
    actual: Any,
    path: str,
    tolerance: Tolerance,
    differences: list[dict[str, Any]],
) -> None:
    if expected is None or actual is None:
        if expected is not None or actual is not None:
            differences.append({"path": path, "reason": "nullness_differs"})
        return
    if (
        isinstance(expected, numbers.Real)
        and not isinstance(expected, bool)
        and isinstance(actual, numbers.Real)
        and not isinstance(actual, bool)
    ):
        left, right = float(expected), float(actual)
        if not math.isfinite(left) or not math.isfinite(right):
            differences.append({"path": path, "reason": "nonfinite_value"})
        elif abs(left - right) > tolerance.atol + tolerance.rtol * abs(right):
            differences.append({"path": path, "reason": "numeric_value_differs"})
        return
    _compare_at(expected, actual, path, tolerance, differences)


def _without_diagnostics(payload: Any, unavailable: frozenset[tuple[str, str]]) -> Any:
    if (
        not isinstance(payload, Mapping)
        or not isinstance(payload.get("diagnostics"), Sequence)
        or isinstance(payload.get("diagnostics"), (str, bytes))
    ):
        return payload
    copied = dict(payload)
    copied["diagnostics"] = [
        row
        for row in payload["diagnostics"]
        if not isinstance(row, Mapping) or (row.get("id"), row.get("variant")) not in unavailable
    ]
    return copied


def _without_findings(payload: Any, unavailable: frozenset[tuple[str, str]]) -> Any:
    if not isinstance(payload, Mapping) or not isinstance(payload.get("findings"), Sequence):
        return payload
    copied = dict(payload)
    findings = []
    for row in payload["findings"]:
        if not isinstance(row, Mapping):
            findings.append(row)
            continue
        updated = dict(row)
        excluded = False
        if (row.get("id"), "base") in unavailable:
            updated["base"] = None
            excluded = True
        variants = row.get("variants")
        if isinstance(variants, Mapping):
            updated["variants"] = {
                variant: None if (row.get("id"), variant) in unavailable else evaluation
                for variant, evaluation in variants.items()
            }
            excluded = excluded or any(
                (row.get("id"), variant) in unavailable for variant in variants
            )
        if excluded:
            updated["robustness_status"] = None
        findings.append(updated)
    copied["findings"] = findings
    return copied


def _compare_sample_artifacts(
    root: Path,
    execution: Mapping[str, Any],
    expected_samples: Any,
    tolerance: Tolerance,
    differences: list[dict[str, Any]],
    unavailable_paths: set[str],
    unavailable_analyses: frozenset[str],
) -> None:
    sample_paths = execution.get("samples", {})
    if not isinstance(expected_samples, Mapping) or not isinstance(sample_paths, Mapping):
        differences.append({"path": "samples", "reason": "replay_samples_missing"})
        return
    withheld_samples = {
        context for context in expected_samples if f"samples/{context}.parquet" in unavailable_paths
    }
    available_expected = set(expected_samples) - unavailable_analyses - withheld_samples
    if available_expected != set(sample_paths) - unavailable_analyses:
        differences.append({"path": "samples", "reason": "sample_inventory_differs"})
    for context, expected in expected_samples.items():
        if context in unavailable_analyses or context in withheld_samples:
            continue
        relative_path = sample_paths.get(context)
        if not isinstance(relative_path, str):
            differences.append({"path": f"samples:{context}", "reason": "missing_path_mapping"})
            continue
        if relative_path != f"samples/{context}.parquet":
            differences.append({"path": f"samples:{context}", "reason": "path_mapping_differs"})
            continue
        if relative_path in unavailable_paths:
            continue
        _compare_at(
            expected,
            _read_parquet_records(root, relative_path),
            relative_path,
            tolerance,
            differences,
        )


def _compare_analysis_statuses(
    manifest: ResultsManifest,
    expected_analyses: Any,
    unavailable_analyses: frozenset[str],
    differences: list[dict[str, Any]],
) -> None:
    if not isinstance(expected_analyses, Mapping):
        differences.append({"path": "analysis_statuses", "reason": "replay_analyses_missing"})
        return
    expected_analyses = {
        key: payload
        for key, payload in expected_analyses.items()
        if key not in unavailable_analyses
    }
    if any(
        not isinstance(payload, Mapping) or "status" not in payload
        for payload in expected_analyses.values()
    ):
        differences.append({"path": "analysis_statuses", "reason": "replay_status_missing"})
        return
    replay_statuses = {
        key: {
            "status": payload["status"],
            "error_codes": _replay_error_codes(payload.get("error")),
        }
        for key, payload in expected_analyses.items()
    }
    bundled = {
        key: {
            "status": record.status.value,
            "error_codes": [error.code for error in record.errors],
        }
        for key, record in manifest.analysis_statuses.items()
        if key not in unavailable_analyses
    }
    _compare_at(
        replay_statuses, bundled, "manifest.json:analysis_statuses", Tolerance(), differences
    )


def _replay_error_codes(error: Any) -> list[str]:
    if error is None:
        return []
    if isinstance(error, Mapping) and isinstance(error.get("code"), str):
        return [error["code"]]
    if isinstance(error, Sequence) and not isinstance(error, (str, bytes)):
        return [
            item["code"]
            for item in error
            if isinstance(item, Mapping) and isinstance(item.get("code"), str)
        ]
    return ["<invalid-error-payload>"]


def _compare_derived_artifacts(
    root: Path,
    execution: Mapping[str, Any],
    expected_derived: Any,
    resolved: Mapping[str, Any],
    tolerance: Tolerance,
    differences: list[dict[str, Any]],
    unavailable_paths: set[str],
    unavailable_panels: frozenset[str],
) -> None:
    derived_paths = execution.get("derived", {})
    if not isinstance(expected_derived, Mapping) or not isinstance(derived_paths, Mapping):
        differences.append({"path": "derived", "reason": "replay_derived_missing"})
        return
    expected_columns = _declared_derived_columns(resolved)
    withheld_derived = {
        panel_name
        for panel_name in expected_columns
        if f"derived/{panel_name}.parquet" in unavailable_paths
    } | set(unavailable_panels)
    available_declared = set(expected_columns) - withheld_derived
    if available_declared != set(derived_paths):
        differences.append({"path": "derived", "reason": "derived_inventory_differs"})
    for panel_name in sorted(available_declared):
        expected = expected_derived.get(panel_name)
        columns = expected_columns[panel_name]
        if not isinstance(expected, pd.DataFrame):
            differences.append({"path": f"derived:{panel_name}", "reason": "replay_output_missing"})
            continue
        missing_columns = [column for column in columns if column not in expected.columns]
        if missing_columns:
            differences.append(
                {
                    "path": f"derived:{panel_name}",
                    "reason": "replay_columns_missing",
                    "columns": missing_columns,
                }
            )
            continue
        relative_path = derived_paths.get(panel_name)
        if not isinstance(relative_path, str):
            differences.append({"path": f"derived:{panel_name}", "reason": "missing_path_mapping"})
            continue
        if relative_path != f"derived/{panel_name}.parquet":
            differences.append({"path": f"derived:{panel_name}", "reason": "path_mapping_differs"})
            continue
        if relative_path in unavailable_paths:
            continue
        actual_frame = _read_parquet_frame(root, relative_path)
        if list(actual_frame.columns) != columns:
            differences.append(
                {
                    "path": relative_path,
                    "reason": "column_inventory_differs",
                    "expected_columns": columns,
                    "actual_columns": list(actual_frame.columns),
                }
            )
            continue
        _compare_at(
            expected.loc[:, columns],
            actual_frame,
            relative_path,
            tolerance,
            differences,
        )


def _declared_derived_columns(resolved: Mapping[str, Any]) -> dict[str, list[str]]:
    recipe = resolved.get("recipe", {})
    plan = resolved.get("plan", {})
    if not isinstance(recipe, Mapping) or not isinstance(plan, Mapping):
        return {}
    panel_plans = plan.get("panels", ())
    derivations = recipe.get("derive", ())
    if not isinstance(panel_plans, Sequence) or not isinstance(derivations, Sequence):
        return {}
    columns: dict[str, list[str]] = {}
    for panel in panel_plans:
        if not isinstance(panel, Mapping):
            continue
        name = panel.get("name")
        geo_key = panel.get("geo_key")
        time_key = panel.get("time_key")
        if not all(isinstance(value, str) for value in (name, geo_key, time_key)):
            continue
        names = [
            item.get("id")
            for item in derivations
            if isinstance(item, Mapping)
            and isinstance(item.get("id"), str)
            and (not item.get("panels") or name in item.get("panels", ()))
        ]
        if names:
            columns[name] = [geo_key, time_key, *names]
    return columns


def _read_parquet_records(root: Path, relative_path: str) -> list[dict[str, Any]]:
    return _read_parquet_frame(root, relative_path).to_dict(orient="records")


def _read_parquet_frame(root: Path, relative_path: str) -> pd.DataFrame:
    from io import BytesIO

    content = _read_regular_file(root, relative_path)
    return pd.read_parquet(BytesIO(content), dtype_backend="pyarrow")


def _compare_at(
    expected: Any,
    actual: Any,
    path: str,
    tolerance: Tolerance,
    differences: list[dict[str, Any]],
) -> None:
    if isinstance(expected, pd.DataFrame):
        expected = expected.to_dict(orient="records")
    if isinstance(actual, pd.DataFrame):
        actual = actual.to_dict(orient="records")
    if isinstance(expected, Mapping) and isinstance(actual, Mapping):
        if expected.keys() != actual.keys():
            differences.append({"path": path, "reason": "mapping_keys_differ"})
            return
        for key in expected:
            _compare_at(expected[key], actual[key], f"{path}.{key}", tolerance, differences)
        return
    if isinstance(expected, Sequence) and not isinstance(expected, (str, bytes)):
        if not isinstance(actual, Sequence) or isinstance(actual, (str, bytes)):
            differences.append({"path": path, "reason": "sequence_type_differs"})
            return
        if len(expected) != len(actual):
            differences.append({"path": path, "reason": "sequence_length_differs"})
            return
        for index, (left, right) in enumerate(zip(expected, actual, strict=True)):
            _compare_at(left, right, f"{path}[{index}]", tolerance, differences)
        return
    if expected is None or actual is None:
        if expected is not None or actual is not None:
            differences.append({"path": path, "reason": "nullness_differs"})
        return
    if isinstance(expected, bool) or isinstance(actual, bool):
        if type(expected) is not type(actual) or expected != actual:
            differences.append({"path": path, "reason": "value_differs"})
        return
    expected_integer = isinstance(expected, numbers.Integral)
    actual_integer = isinstance(actual, numbers.Integral)
    if expected_integer or actual_integer:
        if not (expected_integer and actual_integer) or int(expected) != int(actual):
            differences.append({"path": path, "reason": "value_differs"})
        return
    if isinstance(expected, numbers.Real) and isinstance(actual, numbers.Real):
        left, right = float(expected), float(actual)
        if not math.isfinite(left) or not math.isfinite(right):
            differences.append({"path": path, "reason": "nonfinite_value"})
        elif abs(left - right) > tolerance.atol + tolerance.rtol * abs(right):
            differences.append({"path": path, "reason": "numeric_value_differs"})
        return
    if type(expected) is not type(actual) or expected != actual:
        differences.append({"path": path, "reason": "value_differs"})
