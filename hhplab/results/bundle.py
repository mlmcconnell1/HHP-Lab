"""Portable, policy-filtered results bundle writing and replay loading."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from hhplab.results.lineage import ArtifactLineage, distribution_plan
from hhplab.schema.redistribution import ArtifactScope, SourceRedistribution
from hhplab.schema.results import (
    AnalysisStatusRecord,
    InputIdentity,
    LockStatus,
    ResultContractError,
    ResultsManifest,
    StatusFields,
    Tolerance,
    aggregate_distribution_status,
    canonical_bytes,
    validate_result_rows,
)
from hhplab.storage.provenance import PROVENANCE_KEY, ProvenanceBlock

RESULT_TABLE_NAMES = (
    "regression_estimates",
    "regression_fit",
    "descriptive_stats",
    "correlations",
)
RESULT_TABLE_PATHS = {
    "regression_estimates": "results/regression_estimates.parquet",
    "regression_fit": "results/regression_fit.parquet",
    "descriptive_stats": "results/descriptive_stats.parquet",
    "correlations": "results/correlations.parquet",
}
REPORT_TABLE_BASENAMES = {
    "regression_estimates": "coefficients",
    "regression_fit": "fit",
    "descriptive_stats": "descriptive",
    "correlations": "correlations",
}
RESERVED_PATHS = {
    "README.md",
    "manifest.json",
    "manifest.sha256",
    "recipe/original.yaml",
    "recipe/resolved.json",
    "recipe/execution.json",
    "recipe/lock.json",
    "measures.json",
    "withheld.json",
    "results/results.json",
    *RESULT_TABLE_PATHS.values(),
    "diagnostics.json",
    "findings.json",
}


class BundleError(ValueError):
    """Raised when a results bundle cannot be safely written or read."""


@dataclass(frozen=True)
class BundleArtifact:
    """One caller-produced bundle file with an explicit policy decision."""

    path: str
    contents: bytes
    role: str
    lineage: ArtifactLineage

    def __post_init__(self) -> None:
        _validate_relative_path(self.path)
        if not isinstance(self.contents, bytes):
            raise BundleError(f"artifact {self.path!r} contents must be bytes")
        if not isinstance(self.role, str) or not self.role.strip():
            raise BundleError(f"artifact {self.path!r} role must be a non-empty string")
        if self.lineage.path != self.path:
            raise BundleError("artifact lineage path must equal artifact path")


@dataclass(frozen=True)
class BundlePayload:
    """All immutable inputs required to assemble one standalone run bundle."""

    recipe_id: str
    run_id: str
    created_at: str
    original_recipe: bytes
    recipe: Mapping[str, Any]
    plan: Mapping[str, Any]
    statuses: StatusFields
    analysis_statuses: Mapping[str, AnalysisStatusRecord]
    spec_hash: str
    component_hashes: Mapping[str, str]
    lock_status: LockStatus
    input_identities: tuple[InputIdentity, ...]
    artifacts: tuple[BundleArtifact, ...]
    result_tables: Mapping[str, Sequence[Mapping[str, Any]]]
    result_table_lineage: Mapping[str, ArtifactLineage]
    source_terms: Mapping[str, SourceRedistribution]
    report_payload: Mapping[str, Any]
    report_tables: Mapping[str, Sequence[Mapping[str, Any]]]
    report_lineage: ArtifactLineage
    input_measures: Mapping[str, Any] = field(default_factory=dict)
    measures: Mapping[str, Any] = field(default_factory=dict)
    lock_document: bytes | None = None
    tolerance: Tolerance = field(default_factory=Tolerance)
    private: bool = False
    allow_unknown_aggregates: bool = False
    site_universes: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)


@dataclass(frozen=True)
class BundleWriteResult:
    """Portable paths and validated manifest produced by a bundle write."""

    bundle_dir: Path
    manifest: ResultsManifest
    manifest_sha256: str
    included_paths: tuple[str, ...]
    withheld_paths: tuple[str, ...]


@dataclass(frozen=True)
class BundleContents:
    """Verified, path-relative contents of a portable bundle."""

    bundle_dir: Path
    manifest: ResultsManifest
    resolved_spec: Mapping[str, Any]
    execution: Mapping[str, Any]
    result_tables: Mapping[str, list[dict[str, Any]]]
    documents: Mapping[str, Any]

    def load_replay_inputs(
        self,
    ) -> tuple[Mapping[str, Any], Mapping[str, Any], dict[str, pd.DataFrame]]:
        """Load all included base panels for the results replay engine."""
        panel_paths = self.execution.get("panels", {})
        if not isinstance(panel_paths, Mapping) or not panel_paths:
            raise BundleError("bundle does not contain replayable panel inputs")
        panel_frames = {}
        for name, relative_path in panel_paths.items():
            path = _contained_path(self.bundle_dir, relative_path)
            if not path.is_file():
                raise BundleError(f"bundle panel input is missing: {relative_path!r}")
            panel_frames[name] = pd.read_parquet(path)
        return self.resolved_spec, self.execution, panel_frames

    def load_artifact_frames(self, category: str) -> dict[str, pd.DataFrame]:
        """Load one included panel, derived, or sample parquet mapping."""
        if category not in {"panels", "derived", "samples"}:
            raise BundleError("artifact category must be panels, derived, or samples")
        paths = self.execution.get(category, {})
        if not isinstance(paths, Mapping):
            raise BundleError(f"execution.{category} must be a path mapping")
        return {
            name: pd.read_parquet(_contained_path(self.bundle_dir, relative_path))
            for name, relative_path in paths.items()
        }


class BundleWriter:
    """Write a new immutable bundle directory without replacing an existing run."""

    def __init__(self, bundle_dir: Path | str) -> None:
        self.bundle_dir = Path(bundle_dir)

    def write(self, payload: BundlePayload) -> BundleWriteResult:
        _validate_payload(payload)
        parent = self.bundle_dir.parent
        parent.mkdir(parents=True, exist_ok=True)
        try:
            self.bundle_dir.mkdir()
        except FileExistsError as exc:
            raise BundleError(
                f"bundle path already exists; refusing to overwrite: {self.bundle_dir}"
            ) from exc

        written: list[str] = []
        try:
            return self._write_new(payload, written)
        except Exception as exc:
            marker = {
                "status": "interrupted",
                "error_class": type(exc).__name__,
                "message": _safe_error_message(str(exc)),
                "written_paths": list(written),
            }
            try:
                _write_new_file(
                    self.bundle_dir, ".packaging-incomplete.json", canonical_bytes(marker)
                )
            except Exception:
                pass
            raise BundleError(f"bundle packaging interrupted: {type(exc).__name__}: {exc}") from exc

    def _write_new(self, payload: BundlePayload, written: list[str]) -> BundleWriteResult:
        files = _planned_files(payload)
        supplied_report_lineage = payload.report_lineage
        if supplied_report_lineage.path != "report/index.html":
            raise BundleError("report lineage path must be report/index.html")
        report_sources = {source for source in supplied_report_lineage.sources}
        report_sources.update(
            source
            for lineage in _result_table_lineages(payload).values()
            for source in lineage.sources
        )
        report_sources.update(
            source for artifact in payload.artifacts for source in artifact.lineage.sources
        )
        report_lineage = ArtifactLineage(
            path="report/index.html",
            sources=tuple(sorted(report_sources)),
            scope=(
                ArtifactScope.ROW_LEVEL
                if supplied_report_lineage.scope == ArtifactScope.ROW_LEVEL
                or any(
                    lineage.scope == ArtifactScope.ROW_LEVEL
                    for lineage in _result_table_lineages(payload).values()
                )
                else supplied_report_lineage.scope
            ),
            replay_dependencies=supplied_report_lineage.replay_dependencies,
            companions=supplied_report_lineage.companions,
        )
        report_table_paths = tuple(
            f"tables/{basename}.{extension}"
            for basename in REPORT_TABLE_BASENAMES.values()
            for extension in ("csv", "html")
        )
        report_lineage = ArtifactLineage(
            path="report/index.html",
            sources=report_lineage.sources,
            scope=report_lineage.scope,
            replay_dependencies=report_lineage.replay_dependencies,
            companions=tuple(dict.fromkeys((*report_lineage.companions, *report_table_paths))),
        )
        policy_artifacts = [item["lineage"] for item in files]
        policy_artifacts.append(report_lineage)
        initial_plan = distribution_plan(
            policy_artifacts,
            payload.source_terms,
            private=payload.private,
            allow_unknown_aggregates=payload.allow_unknown_aggregates,
        )
        initial_included = set(initial_plan["included"])
        table_lineages = _result_table_lineages(payload)
        allowed_table_names = {
            name for name, lineage in table_lineages.items() if lineage.path in initial_included
        }
        result_json_sources = tuple(
            sorted(
                {source for name in allowed_table_names for source in table_lineages[name].sources}
            )
        )
        results_json_lineage = ArtifactLineage(
            "results/results.json",
            result_json_sources,
            ArtifactScope.AGGREGATE if result_json_sources else ArtifactScope.SAMPLE,
        )
        policy_artifacts.append(results_json_lineage)
        plan = distribution_plan(
            policy_artifacts,
            payload.source_terms,
            private=payload.private,
            allow_unknown_aggregates=payload.allow_unknown_aggregates,
        )
        included = set(plan["included"])
        withheld = list(plan["withheld"])
        report_allowed = "report/index.html" in included

        self._write_payload_metadata(payload, included, written)
        allowed_table_rows: dict[str, Sequence[Mapping[str, Any]]] = {}
        for name, rows in payload.result_tables.items():
            path = RESULT_TABLE_PATHS[name]
            if path not in included:
                continue
            _write_new_file(self.bundle_dir, path, _result_table_parquet(name, rows))
            written.append(path)
            allowed_table_rows[name] = rows

        results_json_path = "results/results.json"
        if results_json_path in included:
            results_json = {name: list(rows) for name, rows in allowed_table_rows.items()}
            _write_new_file(self.bundle_dir, results_json_path, canonical_bytes(results_json))
            written.append(results_json_path)

        json_documents = {
            "diagnostics.json": payload.report_payload.get("diagnostics", {}),
            "findings.json": payload.report_payload.get("findings", {}),
        }
        for path, document in json_documents.items():
            if path in included:
                _write_new_file(self.bundle_dir, path, canonical_bytes(document))
                written.append(path)

        if report_allowed:
            report_payload = _full_report_payload(payload, withheld)
            from hhplab.results.report import render_readme, render_report, render_tables

            report_html = render_report(report_payload).encode("utf-8")
            rendered_tables = render_tables(
                {name: rows for name, rows in allowed_table_rows.items()}
            )
            _validate_report_table_paths(rendered_tables)
            for relative_name, contents in rendered_tables.items():
                path = f"tables/{relative_name}"
                if path not in included:
                    raise BundleError(f"rendered report artifact was not pre-authorized: {path!r}")
                _write_new_file(self.bundle_dir, path, contents)
                written.append(path)
            _write_new_file(self.bundle_dir, "report/index.html", report_html)
            written.append("report/index.html")
            readme_payload = report_payload
        else:
            from hhplab.results.report import render_readme, render_report

            # A restricted run still publishes a status-only report. It contains
            # no protected analysis values, so its path is included even when
            # the full report is withheld by lineage policy.
            withheld = [item for item in withheld if item.get("path") != "report/index.html"]
            report_payload = _status_only_report_payload(payload, withheld)
            report_html = render_report(report_payload).encode("utf-8")
            _write_new_file(self.bundle_dir, "report/index.html", report_html)
            written.append("report/index.html")
            for relative_name in report_table_paths:
                _add_withheld_record(
                    withheld,
                    relative_name,
                    report_lineage.sources,
                    report_lineage.replay_dependencies,
                )
            readme_payload = report_payload

        readme = render_readme(readme_payload).encode("utf-8")
        _write_new_file(self.bundle_dir, "README.md", readme)
        written.append("README.md")

        withheld_doc = {"withheld": sorted(withheld, key=lambda item: item["path"])}
        _write_new_file(self.bundle_dir, "withheld.json", canonical_bytes(withheld_doc))
        written.append("withheld.json")

        # Metadata files are emitted before the policy pass; their paths have a
        # data-free sample scope and are always included.
        role_by_path = {artifact.path: artifact.role for artifact in payload.artifacts}
        file_records = []
        for relative_path in sorted(written):
            if relative_path in {"manifest.json", "manifest.sha256"}:
                continue
            contents = _contained_path(self.bundle_dir, relative_path).read_bytes()
            file_records.append(
                {
                    "path": relative_path,
                    "sha256": hashlib.sha256(contents).hexdigest(),
                    "bytes": len(contents),
                    "role": role_by_path.get(relative_path, _role_for_path(relative_path)),
                }
            )

        status_fields = _statuses_with_distribution(
            payload.statuses, private=payload.private, withheld=withheld
        )
        manifest = ResultsManifest(
            recipe_id=payload.recipe_id,
            run_id=payload.run_id,
            created_at=payload.created_at,
            statuses=status_fields,
            analysis_statuses=payload.analysis_statuses,
            spec_hash=payload.spec_hash,
            component_hashes=payload.component_hashes,
            lock_status=payload.lock_status,
            input_identities=payload.input_identities,
            private=payload.private,
            files=tuple(file_records),
            site_universes=_included_site_universe_index(payload, included),
        )
        manifest_bytes = canonical_bytes(manifest.to_dict())
        _write_new_file(self.bundle_dir, "manifest.json", manifest_bytes)
        written.append("manifest.json")
        manifest_digest = hashlib.sha256(manifest_bytes).hexdigest()
        _write_new_file(
            self.bundle_dir, "manifest.sha256", f"{manifest_digest}  manifest.json\n".encode()
        )
        written.append("manifest.sha256")
        return BundleWriteResult(
            bundle_dir=self.bundle_dir,
            manifest=manifest,
            manifest_sha256=manifest_digest,
            included_paths=tuple(
                sorted(file_records and [row["path"] for row in file_records] or [])
            ),
            withheld_paths=tuple(item["path"] for item in withheld_doc["withheld"]),
        )

    def _write_payload_metadata(
        self, payload: BundlePayload, included: set[str], written: list[str]
    ) -> None:
        recipe_dir = self.bundle_dir / "recipe"
        recipe_dir.mkdir(parents=True, exist_ok=True)
        core_files = {
            "recipe/original.yaml": payload.original_recipe,
            "recipe/resolved.json": canonical_bytes(
                {"recipe": payload.recipe, "plan": payload.plan}
            ),
            "recipe/execution.json": canonical_bytes(_execution_mapping(payload, included)),
            "measures.json": canonical_bytes(payload.measures),
        }
        if payload.lock_document is not None:
            core_files["recipe/lock.json"] = payload.lock_document
        for path, contents in core_files.items():
            if path not in included:
                raise BundleError(f"required bundle metadata was not authorized: {path!r}")
            _write_new_file(self.bundle_dir, path, contents)
            written.append(path)
        for artifact in payload.artifacts:
            if artifact.path not in included:
                continue
            if artifact.path.endswith(".parquet"):
                _validate_parquet_provenance(artifact.contents, artifact.path)
            _write_new_file(self.bundle_dir, artifact.path, artifact.contents)
            written.append(artifact.path)


def write_bundle(bundle_dir: Path | str, payload: BundlePayload) -> BundleWriteResult:
    """Write a portable run bundle to a path that does not already exist."""
    return BundleWriter(bundle_dir).write(payload)


def read_bundle(bundle_dir: Path | str, *, verify: bool = True) -> BundleContents:
    """Read and validate a bundle using only paths relative to its root."""
    root = Path(bundle_dir)
    if not root.is_dir():
        raise BundleError(f"bundle directory does not exist: {root}")
    try:
        _reject_symlinks(root)
        manifest_bytes = (root / "manifest.json").read_bytes()
        manifest_data = json.loads(manifest_bytes)
        manifest = ResultsManifest.from_dict(manifest_data)
        expected_manifest_digest = hashlib.sha256(manifest_bytes).hexdigest()
        sidecar = (root / "manifest.sha256").read_text(encoding="utf-8").strip().split()
        if (
            len(sidecar) != 2
            or sidecar[0] != expected_manifest_digest
            or sidecar[1] != "manifest.json"
        ):
            raise BundleError("manifest.sha256 does not match manifest.json")
        if verify:
            _verify_inventory(root, manifest)
        resolved_spec = _read_json(root, "recipe/resolved.json")
        execution = _read_json(root, "recipe/execution.json")
        results_path = root / "results/results.json"
        tables = json.loads(results_path.read_bytes()) if results_path.is_file() else {}
        if not isinstance(tables, Mapping):
            raise BundleError("results/results.json must contain a table mapping")
        for name, rows in tables.items():
            validate_result_rows(name, rows)
        inventoried_paths = {record["path"] for record in manifest.files}
        for name, path in RESULT_TABLE_PATHS.items():
            if path not in inventoried_paths:
                continue
            parquet_rows = pq.read_table(_contained_path(root, path)).to_pylist()
            validate_result_rows(name, parquet_rows)
            if parquet_rows != tables.get(name, []):
                raise BundleError(f"JSON and Parquet result tables differ: {name}")
        documents = {}
        for path in ("diagnostics.json", "findings.json", "withheld.json"):
            candidate = root / path
            if candidate.is_file():
                documents[path] = json.loads(candidate.read_bytes())
        return BundleContents(root, manifest, resolved_spec, execution, tables, documents)
    except BundleError:
        raise
    except (OSError, ValueError, TypeError, ResultContractError) as exc:
        raise BundleError(f"bundle validation failed: {type(exc).__name__}: {exc}") from exc


def _planned_files(payload: BundlePayload) -> list[dict[str, Any]]:
    paths = {
        "recipe/original.yaml",
        "recipe/resolved.json",
        "recipe/execution.json",
        "measures.json",
    }
    if payload.lock_document is not None:
        paths.add("recipe/lock.json")
    artifacts = []
    for artifact in payload.artifacts:
        if (
            artifact.path in RESERVED_PATHS
            or artifact.path in paths
            or artifact.path.startswith(("report/", "tables/"))
        ):
            raise BundleError(f"artifact path is reserved by bundle format: {artifact.path!r}")
        if artifact.path in paths:
            raise BundleError(f"duplicate bundle artifact path: {artifact.path!r}")
        paths.add(artifact.path)
    if len(payload.artifacts) != len(paths) - (5 if payload.lock_document is not None else 4):
        raise BundleError("bundle artifact paths must be unique")

    artifacts.extend(_result_table_lineages(payload).values())
    artifacts.extend(
        (
            ArtifactLineage(
                "diagnostics.json", payload.report_lineage.sources, ArtifactScope.AGGREGATE
            ),
            ArtifactLineage(
                "findings.json", payload.report_lineage.sources, ArtifactScope.AGGREGATE
            ),
        )
    )
    artifacts.extend(item.lineage for item in payload.artifacts)
    artifacts.extend(
        ArtifactLineage(path, (), ArtifactScope.SAMPLE)
        for path in (
            "recipe/original.yaml",
            "recipe/resolved.json",
            "recipe/execution.json",
            "measures.json",
            "recipe/lock.json",
        )
        if path != "recipe/lock.json" or payload.lock_document is not None
    )
    # Policy planning consumes lineages, not bytes, so denied result values are
    # never serialized into Parquet, JSON, CSV or HTML.
    return [{"lineage": lineage} for lineage in artifacts]


def _result_table_lineages(payload: BundlePayload) -> dict[str, ArtifactLineage]:
    resolved = {}
    for table_name in RESULT_TABLE_NAMES:
        path = RESULT_TABLE_PATHS[table_name]
        lineage = payload.result_table_lineage.get(table_name)
        if lineage is None:
            lineage = ArtifactLineage(
                path=path,
                sources=(f"unknown-result-table:{table_name}",),
                scope=ArtifactScope.AGGREGATE,
            )
        elif lineage.path != path:
            raise BundleError(f"result table lineage path mismatch for {table_name!r}")
        resolved[table_name] = lineage
    return resolved


def _validate_payload(payload: BundlePayload) -> None:
    if not isinstance(payload, BundlePayload):
        raise TypeError("payload must be BundlePayload")
    if not isinstance(payload.original_recipe, bytes):
        raise BundleError("original recipe must be its submitted bytes")
    if not isinstance(payload.private, bool) or not isinstance(
        payload.allow_unknown_aggregates, bool
    ):
        raise BundleError("distribution overrides must be boolean")
    missing_tables = set(RESULT_TABLE_NAMES) - set(payload.result_tables)
    unknown_tables = set(payload.result_tables) - set(RESULT_TABLE_NAMES)
    if missing_tables or unknown_tables:
        raise BundleError(
            "result_tables must include exactly the four canonical tables; "
            f"missing={sorted(missing_tables)}, unknown={sorted(unknown_tables)}"
        )
    if set(payload.result_table_lineage) - set(RESULT_TABLE_NAMES):
        raise BundleError(
            "lineage supplied for unknown result table(s): "
            f"{sorted(set(payload.result_table_lineage) - set(RESULT_TABLE_NAMES))}"
        )
    _validate_site_universe_payload(payload)
    for name, rows in payload.result_tables.items():
        validate_result_rows(name, rows)
    if not isinstance(payload.report_payload, Mapping):
        raise BundleError("report_payload must be a mapping")
    if not isinstance(payload.report_tables, Mapping):
        raise BundleError("report_tables must be a mapping")
    if set(payload.report_tables) != set(RESULT_TABLE_NAMES):
        raise BundleError("report_tables must include exactly the four canonical result tables")
    # Fail before creating the output directory if caller metadata cannot be
    # represented in the portable JSON contract.
    for label, value in (
        ("recipe", payload.recipe),
        ("plan", payload.plan),
        ("input_measures", payload.input_measures),
        ("measures", payload.measures),
        ("report_payload", payload.report_payload),
        ("diagnostics", payload.report_payload.get("diagnostics", {})),
        ("findings", payload.report_payload.get("findings", {})),
    ):
        try:
            canonical_bytes(value)
        except (ResultContractError, TypeError, ValueError) as exc:
            raise BundleError(f"bundle {label} metadata is not portable JSON: {exc}") from exc
    for name, rows in payload.report_tables.items():
        validate_result_rows(name, rows)
        if canonical_bytes(list(rows)) != canonical_bytes(list(payload.result_tables[name])):
            raise BundleError(f"report table rows must match authorized result table: {name}")
    if not isinstance(payload.tolerance, Tolerance):
        raise BundleError("tolerance must be a Tolerance value")
    _validate_relative_path("recipe/original.yaml")


def _validate_site_universe_payload(payload: BundlePayload) -> None:
    if not isinstance(payload.site_universes, Mapping):
        raise BundleError("site_universes must be a mapping")
    artifacts = {artifact.path for artifact in payload.artifacts}
    seen_paths: set[str] = set()
    for artifact_id, entry in payload.site_universes.items():
        if not isinstance(artifact_id, str) or not artifact_id.strip():
            raise BundleError("site-universes artifact IDs must be non-empty strings")
        if not isinstance(entry, Mapping):
            raise BundleError(f"site-universe index for {artifact_id!r} must be a mapping")
        required = {"schema_version", "sites", "memberships", "summary", "boundary_evidence"}
        optional = set()
        if set(entry) - required - optional or required - set(entry):
            raise BundleError(f"site-universe index for {artifact_id!r} has invalid fields")
        if entry["schema_version"] != "msa_coc_sites/1":
            raise BundleError(f"site-universe index for {artifact_id!r} has unsupported schema")
        for key, path in entry.items():
            if key == "schema_version":
                continue
            if not isinstance(path, str) or path not in artifacts:
                raise BundleError(
                    f"site-universe index {artifact_id!r}.{key} must name a bundle artifact"
                )
            if path in seen_paths:
                raise BundleError(f"site-universe artifact path is indexed more than once: {path}")
            seen_paths.add(path)


def _included_site_universe_index(
    payload: BundlePayload, included: set[str]
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for artifact_id, entry in payload.site_universes.items():
        paths = {value for key, value in entry.items() if key != "schema_version"}
        selected = paths & included
        if selected and selected != paths:
            raise BundleError(
                f"site-universe inputs for {artifact_id!r} were only partly included"
            )
        indexed = dict(entry)
        if paths and not selected:
            indexed["withheld"] = True
        result[artifact_id] = indexed
    return result


def _execution_mapping(payload: BundlePayload, included: set[str]) -> dict[str, Any]:
    maps: dict[str, dict[str, str]] = {"panels": {}, "derived": {}, "samples": {}}
    for artifact in payload.artifacts:
        if artifact.path not in included:
            continue
        parts = PurePosixPath(artifact.path).parts
        if len(parts) == 2 and parts[0] == "panels" and parts[1].endswith(".parquet"):
            maps["panels"][parts[1].removesuffix(".parquet")] = artifact.path
        elif len(parts) == 2 and parts[0] == "derived" and parts[1].endswith(".parquet"):
            maps["derived"][parts[1].removesuffix(".parquet")] = artifact.path
        elif len(parts) == 2 and parts[0] == "samples" and parts[1].endswith(".parquet"):
            maps["samples"][parts[1].removesuffix(".parquet")] = artifact.path
    return {
        "spec_hash": payload.spec_hash,
        **maps,
        "input_measures": dict(payload.input_measures),
        "tolerance": payload.tolerance.to_dict(),
    }


def _result_table_parquet(name: str, rows: Sequence[Mapping[str, Any]]) -> bytes:
    from hhplab.schema.results import RESULT_TABLE_SCHEMAS

    validate_result_rows(name, rows)
    contract = RESULT_TABLE_SCHEMAS[name]
    columns = contract.column_names
    arrow_types = {
        "integer": pa.int64(),
        "number": pa.float64(),
        "string": pa.string(),
        "string_array": pa.list_(pa.string()),
    }
    schema = pa.schema(
        [
            pa.field(column.name, arrow_types[column.value_type], nullable=column.nullable)
            for column in contract.columns
        ]
    )
    table = pa.Table.from_pylist(list(rows), schema=schema)
    provenance = ProvenanceBlock(
        extra={
            "dataset_type": "results_bundle_table",
            "bundle_format_version": "1.0",
            "table": name,
            "columns": list(columns),
            "row_count": len(rows),
        }
    )
    metadata = dict(table.schema.metadata or {})
    metadata[PROVENANCE_KEY] = provenance.to_json().encode("utf-8")
    table = table.replace_schema_metadata(metadata)
    output = pa.BufferOutputStream()
    pq.write_table(table, output, compression="snappy")
    return output.getvalue().to_pybytes()


def _verify_inventory(root: Path, manifest: ResultsManifest) -> None:
    expected = {record["path"] for record in manifest.files} | {"manifest.json", "manifest.sha256"}
    actual = set()
    for path in root.rglob("*"):
        if path.is_symlink():
            raise BundleError(f"bundle cannot contain symlinks: {path.relative_to(root)}")
        if path.is_file():
            actual.add(path.relative_to(root).as_posix())
    if actual != expected:
        raise BundleError(
            "bundle inventory mismatch; "
            f"missing={sorted(expected - actual)}, extra={sorted(actual - expected)}"
        )
    for record in manifest.files:
        path = _contained_path(root, record["path"])
        contents = path.read_bytes()
        if len(contents) != record["bytes"]:
            raise BundleError(f"bundle file byte count mismatch: {record['path']}")
        if hashlib.sha256(contents).hexdigest() != record["sha256"]:
            raise BundleError(f"bundle file hash mismatch: {record['path']}")


def _reject_symlinks(root: Path) -> None:
    if root.is_symlink():
        raise BundleError("bundle root cannot be a symlink")
    for path in root.rglob("*"):
        if path.is_symlink():
            raise BundleError(f"bundle cannot contain symlinks: {path.relative_to(root)}")


def _validate_parquet_provenance(contents: bytes, path: str) -> None:
    if not contents:
        raise BundleError(f"Parquet artifact is empty: {path}")
    try:
        metadata = pq.read_schema(pa.BufferReader(contents)).metadata or {}
    except Exception as exc:
        raise BundleError(f"invalid Parquet artifact {path!r}: {exc}") from exc
    if PROVENANCE_KEY not in metadata:
        raise BundleError(f"Parquet artifact lacks hhplab_provenance metadata: {path}")


def _write_new_file(root: Path, relative_path: str, contents: bytes) -> None:
    _validate_relative_path(relative_path)
    path = _contained_path(root, relative_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb") as stream:
            stream.write(contents)
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError as exc:
        raise BundleError(f"refusing to overwrite bundle file: {relative_path}") from exc


def _contained_path(root: Path, relative_path: str) -> Path:
    _validate_relative_path(relative_path)
    root_resolved = root.resolve()
    candidate = root / Path(*PurePosixPath(relative_path).parts)
    try:
        candidate.resolve(strict=False).relative_to(root_resolved)
    except ValueError as exc:
        raise BundleError(f"bundle path escapes its root: {relative_path!r}") from exc
    return candidate


def _validate_relative_path(path: str) -> None:
    if not isinstance(path, str) or not path:
        raise BundleError("bundle path must be a non-empty relative string")
    posix_path = PurePosixPath(path)
    windows_path = PureWindowsPath(path)
    if (
        posix_path.is_absolute()
        or windows_path.is_absolute()
        or windows_path.drive
        or posix_path.as_posix() != path
        or "\\" in path
        or any(part in {"", ".", ".."} for part in posix_path.parts)
    ):
        raise BundleError(f"bundle path must be normalized and relative: {path!r}")


def _read_json(root: Path, relative_path: str) -> Any:
    return json.loads(_contained_path(root, relative_path).read_bytes())


def _role_for_path(path: str) -> str:
    if path.startswith("panels/"):
        return "panel"
    if path.startswith("derived/"):
        return "derived"
    if path.startswith("samples/"):
        return "sample"
    if path.startswith("results/"):
        return "result"
    if path.startswith("report/"):
        return "report"
    if path.startswith("tables/"):
        return "table"
    if path.startswith("recipe/"):
        return "recipe"
    if path in {"diagnostics.json", "findings.json", "withheld.json", "measures.json"}:
        return "metadata"
    if path.startswith("environment/"):
        return "environment"
    if path.startswith("logs/"):
        return "log"
    if path == "README.md":
        return "readme"
    return "artifact"


def _statuses_with_distribution(
    statuses: StatusFields, *, private: bool, withheld: Sequence[Mapping[str, Any]]
) -> StatusFields:
    distribution = aggregate_distribution_status(private=private, withheld_count=len(withheld))
    return StatusFields(
        execution_status=statuses.execution_status,
        diagnostics_status=statuses.diagnostics_status,
        findings_status=statuses.findings_status,
        distribution_status=distribution,
    )


def _full_report_payload(
    payload: BundlePayload, withheld: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    report_payload = dict(payload.report_payload)
    report_payload["recipe_id"] = payload.recipe_id
    report_payload["statuses"] = _statuses_with_distribution(
        payload.statuses, private=payload.private, withheld=withheld
    ).to_dict()
    report_payload["lock_status"] = payload.lock_status.to_dict()
    report_payload["private"] = payload.private
    return report_payload


def _status_only_report_payload(
    payload: BundlePayload, withheld: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    source_analyses = payload.report_payload.get("analyses", {})
    analyses = (
        {
            key: {"status": record.get("status", "unknown"), "withheld": True}
            for key, record in source_analyses.items()
        }
        if isinstance(source_analyses, Mapping)
        else {}
    )
    statuses = payload.statuses.to_dict()
    statuses["distribution_status"] = "private" if payload.private else "withheld"
    return {
        "recipe_id": payload.recipe_id,
        "title": payload.report_payload.get("title", payload.recipe_id),
        "statuses": statuses,
        "lock_status": payload.lock_status.to_dict(),
        "analyses": analyses,
        "diagnostics": {},
        "findings": {},
        "exclusions": [],
        "errors": [],
        "limitations": ["Report values and claims are withheld by redistribution policy."],
        "withheld": [{"path": item["path"]} for item in withheld],
        "private": payload.private,
    }


def _validate_report_table_paths(rendered: Mapping[str, bytes]) -> None:
    for path, contents in rendered.items():
        _validate_relative_path(path)
        if "/" in path or not isinstance(contents, bytes):
            raise BundleError("rendered report tables must map simple filenames to bytes")


def _add_withheld_record(
    withheld: list[dict[str, Any]],
    path: str,
    sources: Sequence[str],
    replay_dependencies: Sequence[str],
) -> None:
    if any(item["path"] == path for item in withheld):
        return
    withheld.append(
        {
            "path": path,
            "sources": sorted(set(sources)) or ["unknown-artifact"],
            "replay_dependencies": sorted(set(replay_dependencies)),
            "reason": "Source redistribution terms do not permit this artifact.",
            "rebuild": "Restore authorized source inputs and replay the recipe locally.",
        }
    )


def _safe_error_message(message: str) -> str:
    return message.replace("\\", "/")[:1000]
