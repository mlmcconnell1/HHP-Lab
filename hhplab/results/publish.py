"""Build a site publication from the public view of one verified results bundle."""

from __future__ import annotations

import ctypes
import fcntl
import hashlib
import json
import os
import stat
import tempfile
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path, PurePosixPath
from typing import Any

import yaml

from hhplab.results.bundle import RESULT_TABLE_PATHS, BundleContents, BundleError, read_bundle
from hhplab.results.verdicts import PublishedFinding, VerdictResolutionError, resolve_public_finding
from hhplab.results.verify import verify_bundle
from hhplab.schema.publication import (
    SLUG_PATTERN,
    PublicationIndexEntry,
    PublicationPage,
    PublicationProvenance,
    PublicationSource,
)
from hhplab.schema.redistribution import ArtifactDisposition, ArtifactScope, RedistributionStatus
from hhplab.schema.results import AnalysisStatus, ExecutionStatus, FindingsPayload, canonical_bytes


class PublicationError(ValueError):
    """A publication failure with a public-safe explanation."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class PublicationBundle:
    contents: BundleContents
    findings: Mapping[str, PublishedFinding]
    declarations: Mapping[str, Mapping[str, Any]]
    references: tuple[Mapping[str, Any], ...]
    analyses: Mapping[str, Mapping[str, Any]]
    parameters: Mapping[str, Any]
    result_keys: frozenset[str]
    reproducibility: str
    reproducibility_reasons: tuple[str, ...]
    caveats: tuple[Mapping[str, str], ...]
    permission_decisions: dict[str, Any] = field(default_factory=dict)

    def permit(self, path: str, *, membership_count: bool = False) -> None:
        """Require all parents of an inventoried artifact to permit this use."""
        self.permission_decisions[path] = _permission_decision(
            self.contents, path, membership_count=membership_count
        )

    def rows(self, name: str) -> tuple[Mapping[str, Any], ...]:
        """Read only an eligible table and restrict rows to cited result keys."""
        path = RESULT_TABLE_PATHS[name]
        self.permit(path)
        return tuple(
            row
            for row in self.contents.result_tables.get(name, ())
            if _row_result_key(row) in self.result_keys
        )

    def optional_rows(self, name: str) -> tuple[Mapping[str, Any], ...]:
        """Omit optional research tables when their aggregate is unavailable."""
        try:
            return self.rows(name)
        except PublicationError as exc:
            if exc.code not in {"artifact_unavailable", "permission_denied"}:
                raise
            return ()


def _row_result_key(row: Mapping[str, Any]) -> str:
    return f"{row.get('analysis')}@{row.get('variant')}"


def _permission_decision(
    contents: BundleContents, path: str, *, membership_count: bool = False
) -> dict[str, Any]:
    record = contents.permissions
    if path not in {file["path"] for file in contents.manifest.files}:
        raise PublicationError("artifact_unavailable", "A required public artifact is unavailable.")
    parents = record.parents_for_path(path) if record is not None else ()
    scopes = {ArtifactScope.AGGREGATE}
    if membership_count:
        scopes.add(ArtifactScope.SAMPLE)
    if not parents or any(
        parent.scope not in scopes or parent.strict_disposition is not ArtifactDisposition.INCLUDED
        for parent in parents
    ):
        raise PublicationError("permission_denied", "A required artifact has no strict permission.")
    lineage = {source for parent in parents for source in parent.sources}
    approved = {}
    for source in sorted(lineage):
        terms = record.sources[source]
        if (
            terms.aggregate is not RedistributionStatus.ALLOWED
            or terms.terms_origin == "default_unknown"
            or not terms.evidence
        ):
            raise PublicationError(
                "permission_denied", "A contributing source lacks reviewed public permission."
            )
        approved[source] = terms.to_dict()
    return {
        "path": path,
        "strict_disposition": "included",
        "parents": [parent.path for parent in parents],
        "sources": approved,
    }


def _verified_public_bundle(
    source: PublicationSource, bundle_path: Path
) -> BundleContents:
    try:
        contents = read_bundle(bundle_path)
    except BundleError as exc:
        raise PublicationError("integrity_failed", "Bundle integrity verification failed.") from exc
    pointer = source.bundle
    manifest = contents.manifest
    if pointer is None or (
        pointer.recipe_id != manifest.recipe_id
        or pointer.run_id != manifest.run_id
        or pointer.manifest_sha256 != contents.manifest_sha256
    ):
        raise PublicationError(
            "bundle_identity_mismatch",
            "The recipe, run or manifest digest does not match the publication source.",
        )
    if manifest.private:
        raise PublicationError("private_bundle", "Phase A requires a public results bundle.")
    if contents.permissions is None:
        raise PublicationError(
            "permission_record_missing",
            "Bundle predates permission records. Re-run the recipe with hhplab bundle format 1.1.",
        )
    if manifest.statuses.execution_status is ExecutionStatus.FAILED:
        raise PublicationError("run_failed", "The cited bundle records a failed run.")
    return contents


def _reproducibility(
    directory: Path, *, skip: bool, allow_failure: bool
) -> tuple[str, tuple[str, ...]]:
    if skip:
        return "not_run", ("Replay was skipped for this publication.",)
    verification = verify_bundle(directory)
    if verification.integrity != "pass":
        raise PublicationError("integrity_failed", "Bundle integrity verification failed.")
    status = verification.reproducibility
    if status == "fail" and not allow_failure:
        raise PublicationError(
            "replay_failure", "Offline verification could not reproduce the run."
        )
    messages = {
        "fail": ("Offline verification could not reproduce the run.",),
        "not_run": ("Offline verification could not replay the available run inputs.",),
        "pass": (),
    }
    if status != "partial":
        return status, messages[status]
    reasons = []
    if verification.unavailable:
        reasons.append("Some replay inputs or dependent results are unavailable in this bundle.")
    if any("environment" in reason for reason in verification.partial_reasons):
        reasons.append("The verification environment differs from the original run.")
    return status, tuple(reasons or ["Offline verification reproduced only part of the run."])


def _public_caveats(
    annotations: Mapping[str, Any], result_keys: frozenset[str]
) -> tuple[Mapping[str, str], ...]:
    """Keep approved caveat text from the bundle's public annotation projection."""
    notices = []
    if annotations.get("distribution_status") == "withheld":
        notices.append({
            "address": "annotations.json#/distribution_status",
            "text": "Source restrictions limit the available data caveat details.",
        })
        for index, note in enumerate(annotations.get("public_notes", ())):
            text = note.get("text")
            if isinstance(text, str) and note.get("id") != "withheld-caveat-details":
                notices.append({
                    "address": f"annotations.json#/public_notes/{index}/text", "text": text
                })
        return tuple(notices)

    active = annotations.get("active_per_analysis", {})
    applied = {
        (item.get("id"), item.get("revision"))
        for key in result_keys
        for item in active.get(key, ())
    }
    for index, entry in enumerate(annotations.get("records", ())):
        if (entry.get("id"), entry.get("revision")) not in applied:
            continue
        text = entry.get("public_text")
        if isinstance(text, str) and text.strip():
            notices.append({
                "address": f"annotations.json#/records/{index}/public_text",
                "text": text,
            })
        else:
            notices.append({
                "address": "annotations.json#/active_per_analysis",
                "text": "A data caveat applies to the cited analyses.",
            })
    return tuple(notices)


def _unique_records(rows, field_name: str) -> dict[str, Mapping[str, Any]]:
    records = {}
    for row in rows:
        key = row[field_name]
        if key in records:
            raise ValueError("duplicate resolved result key")
        records[key] = row
    return records


def _finding_inputs(
    contents: BundleContents, source: PublicationSource
) -> tuple[
    dict[str, PublishedFinding],
    dict[str, Mapping[str, Any]],
    dict[str, Mapping[str, Any]],
    tuple[Mapping[str, Any], ...],
    Mapping[str, Any],
]:
    try:
        recipe = contents.resolved_spec["recipe"]
        plan = contents.resolved_spec["plan"]
        if recipe["id"] != contents.manifest.recipe_id or plan[
            "component_hashes"
        ] != contents.manifest.component_hashes:
            raise ValueError("resolved recipe identity differs")
        parameters = plan["parameters"]
        declarations = _unique_records(recipe["findings"], "id")
        records = FindingsPayload.from_dict(contents.documents["findings.json"]).findings
        by_id = {record.id: record for record in records}
        if len(by_id) != len(records):
            raise ValueError("duplicate evaluated finding")
        findings = {
            key: resolve_public_finding(
                declarations[key],
                by_id[key],
                variant_descriptions=source.variant_descriptions,
                resolved_parameters=parameters,
            )
            for key in source.findings
        }
        analyses = _unique_records(plan["analyses"], "result_key")
        references = tuple(
            item for item in plan["references"]
            if item["owner_kind"] == "finding" and item["owner_id"] in findings
        )
    except VerdictResolutionError as exc:
        raise PublicationError(
            exc.code, "A cited finding or its declared comparisons cannot be published."
        ) from exc
    except (AttributeError, KeyError, TypeError, ValueError) as exc:
        raise PublicationError(
            "invalid_findings",
            "The bundle lacks unique declarations and results for cited findings.",
        ) from exc
    return findings, {key: declarations[key] for key in findings}, analyses, references, parameters


_STAT_TABLE = {
    "regression": "regression_fit",
    "descriptive": "descriptive_stats",
    "correlation": "correlations",
}


def _check_reference(bundle: PublicationBundle, reference: Mapping[str, Any]) -> None:
    key = reference["result_key"]
    status = bundle.contents.manifest.analysis_statuses.get(key)
    if status is None or status.status is not AnalysisStatus.COMPLETE:
        raise PublicationError("analysis_incomplete", "A required cited analysis is incomplete.")
    term = reference.get("term")
    stat = reference.get("stat")
    if term is not None:
        rows = [
            row for row in bundle.rows("regression_estimates")
            if _row_result_key(row) == key and row.get("term") == term
        ]
        required = ("estimate", "ci_lo", "ci_hi")
    else:
        table = _STAT_TABLE.get(bundle.analyses[key]["method"])
        if table is None:
            raise PublicationError("missing_result", "A cited statistic has no eligible result.")
        rows = [row for row in bundle.rows(table) if _row_result_key(row) == key]
        if table == "descriptive_stats":
            rows = [row for row in rows if row.get("stat") == stat]
        required = ("value",) if table == "descriptive_stats" else (stat,)
    if len(rows) != 1 or any(rows[0].get(field) is None for field in required):
        raise PublicationError("missing_result", "A required cited result is missing or ambiguous.")


def prepare_publication_bundle(
    source: PublicationSource,
    bundle_path: Path,
    *,
    skip_reproduce: bool = False,
    allow_reproducibility_failure: bool = False,
) -> PublicationBundle:
    """Verify exact inputs, strict redistribution, cited findings and public caveats."""
    contents = _verified_public_bundle(source, bundle_path)
    finding_decision = _permission_decision(contents, "findings.json")
    findings, declarations, analyses, references, parameters = _finding_inputs(contents, source)
    usable = tuple(
        reference for reference in references
        if reference["result_key"].endswith("@base") or any(
            variant.evaluation.verdict is not None
            and reference["result_key"].endswith(f"@{variant.id}")
            for variant in findings[reference["owner_id"]].variants
        )
    )
    result_keys = frozenset(ref["result_key"] for ref in usable)
    if any(
        not any(ref["owner_id"] == key and ref["result_key"].endswith("@base") for ref in usable)
        for key in findings
    ) or not result_keys <= analyses.keys():
        raise PublicationError(
            "missing_result", "A cited finding lacks resolved analysis references."
        )
    reproducibility, reasons = _reproducibility(
        bundle_path, skip=skip_reproduce, allow_failure=allow_reproducibility_failure
    )
    try:
        caveats = _public_caveats(contents.annotations, result_keys)
    except (AttributeError, TypeError, ValueError) as exc:
        raise PublicationError(
            "invalid_annotations", "The bundle has invalid publication caveat records."
        ) from exc
    bundle = PublicationBundle(
        contents=contents,
        findings=findings,
        declarations=declarations,
        references=usable,
        analyses=analyses,
        parameters=parameters,
        result_keys=result_keys,
        reproducibility=reproducibility,
        reproducibility_reasons=reasons,
        caveats=caveats,
        permission_decisions={"findings.json": finding_decision},
    )
    for ref in usable:
        _check_reference(bundle, ref)
    return bundle


_INDEX_FILE = "findings-index.json"
_PAGE_FILE = "index.md"
_PROVENANCE_FILE = "provenance.json"
_RENAME_NOREPLACE = 1
_RENAME_EXCHANGE = 2
_AT_FDCWD = -100


def _absolute_path(path: Path) -> Path:
    if ".." in path.parts:
        raise PublicationError("unsafe_path", "Publication paths must be contained and normalized.")
    absolute = path.absolute()
    if absolute.resolve() != absolute:
        raise PublicationError("unsafe_path", "Publication paths must not cross symbolic links.")
    return absolute


def _read_source(path: Path) -> tuple[PublicationSource, dict[str, str], str]:
    path = _absolute_path(path)
    try:
        if path.name != "publication.yaml" or not path.is_file():
            raise ValueError("expected a publication.yaml file")
        source_bytes = path.read_bytes()
        source = PublicationSource.model_validate(yaml.safe_load(source_bytes))
        if path.parent.name != source.slug:
            raise ValueError("publication source directory and slug differ")
        base = f"publications/{source.slug}"
        digests = {f"{base}/publication.yaml": hashlib.sha256(source_bytes).hexdigest()}
        if source.status == "retracted":
            return source, digests, ""
        narrative_path = _absolute_path(path.with_name("narrative.md"))
        if not narrative_path.is_file():
            raise ValueError("expected a narrative.md file")
        narrative_bytes = narrative_path.read_bytes()
        digests[f"{base}/narrative.md"] = hashlib.sha256(narrative_bytes).hexdigest()
        return source, digests, narrative_bytes.decode("utf-8")
    except PublicationError:
        raise
    except (OSError, TypeError, ValueError, yaml.YAMLError) as exc:
        raise PublicationError(
            "invalid_source", "Check publication.yaml, its slug directory and narrative.md."
        ) from exc


def _asset_path(relative: str) -> PurePosixPath:
    path = PurePosixPath(relative)
    extensions = {"data": ".csv", "figures": ".svg"}
    if path.as_posix() != relative or "\\" in relative:
        raise ValueError("publication asset path is not normalized")
    if relative == _PAGE_FILE:
        return path
    if len(path.parts) != 2 or path.suffix != extensions.get(path.parts[0]):
        raise ValueError("publication asset path is outside the package layout")
    return path


def _page_bytes(page: Mapping[str, Any], body: str) -> bytes:
    frontmatter = yaml.safe_dump(dict(page), sort_keys=False, allow_unicode=True)
    return f"---\n{frontmatter}---\n\n{body.rstrip()}\n".encode()


def _validate_package_documents(page: dict[str, Any], provenance: dict[str, Any]) -> None:
    PublicationPage.model_validate(page)
    PublicationProvenance.model_validate(provenance)
    history = page["history"]
    dates = [datetime.fromisoformat(entry["published_at"]) for entry in history]
    if not dates or dates != sorted(dates) or history[-1]["published_at"] != page["published_at"]:
        raise ValueError("publication history is missing or out of order")
    latest = history[-1]
    if latest["status"] != page["status"]:
        raise ValueError("publication state differs from its history")
    if page["status"] == "retracted":
        safe_history_fields = {"published_at", "status", "change_note"}
        if "bundle" in provenance or set(provenance["asset_sha256"]) != {_PAGE_FILE} or any(
            set(entry) - safe_history_fields for entry in history
        ):
            raise ValueError("tombstone contains result assets or withdrawn history")
    else:
        pointer = provenance["bundle"]
        if any(
            latest[field] != pointer[field]
            for field in ("run_id", "manifest_sha256", "lock_status")
        ) or latest["headline_verdict"] != page["headline_verdict"]:
            raise ValueError("publication history differs from current result identity")


def _release_page(directory: Path) -> dict[str, Any]:
    """Validate committed package bytes before using its history or index entry."""
    try:
        directory = _absolute_path(directory)
        provenance_path = _absolute_path(directory / _PROVENANCE_FILE)
        provenance = json.loads(provenance_path.read_bytes())
        if provenance["publication_format_version"] != 1:
            raise ValueError("unsupported provenance format")
        digests = provenance["asset_sha256"]
        if not isinstance(digests, dict) or _PAGE_FILE not in digests:
            raise ValueError("missing publication inventory")
        seen = set()
        for candidate in directory.rglob("*"):
            if candidate.is_symlink():
                raise ValueError("symlink in publication package")
            if candidate.is_file():
                seen.add(candidate.relative_to(directory).as_posix())
        if seen != {*digests, _PROVENANCE_FILE}:
            raise ValueError("publication inventory mismatch")
        for relative, digest in digests.items():
            path = directory / _asset_path(relative)
            if hashlib.sha256(path.read_bytes()).hexdigest() != digest:
                raise ValueError("publication asset hash mismatch")
        prefix, header, _body = (directory / _PAGE_FILE).read_text(
            encoding="utf-8"
        ).split("---\n", 2)
        if prefix:
            raise ValueError("publication page lacks opening frontmatter")
        page = yaml.safe_load(header)
        if page["slug"] != directory.name:
            raise ValueError("committed publication identity is invalid")
        _validate_package_documents(page, provenance)
        return page
    except (AttributeError, KeyError, OSError, TypeError, ValueError, yaml.YAMLError) as exc:
        raise PublicationError(
            "invalid_destination_package", "A committed publication failed validation."
        ) from exc


def _read_releases(dest: Path) -> dict[str, dict[str, Any]]:
    if not dest.exists():
        return {}
    if not dest.is_dir():
        raise PublicationError(
            "unsafe_destination", "The publication destination must be a directory."
        )
    releases = {}
    for path in sorted(dest.iterdir()):
        if not SLUG_PATTERN.fullmatch(path.name):
            continue
        if path.is_symlink():
            raise PublicationError("unsafe_destination", "A destination slug is a symbolic link.")
        if (path / _PROVENANCE_FILE).exists():
            releases[path.name] = _release_page(path)
    return releases


def _check_graph(pages: Mapping[str, Mapping[str, Any]]) -> None:
    for slug in pages:
        seen = {slug}
        target = pages[slug].get("superseded_by")
        while target is not None:
            if target in seen or target not in pages:
                raise PublicationError(
                    "invalid_supersession", "A supersession link is missing or forms a cycle."
                )
            seen.add(target)
            target = pages[target].get("superseded_by")


def _release_root(dest: Path) -> Path:
    if not dest.name:
        raise PublicationError("unsafe_destination", "Choose a named publication destination.")
    return dest.parent / f".{dest.name}.hhplab-publications"


@contextmanager
def _destination_lock(dest: Path, *, preview: bool):
    private_root = _absolute_path(_release_root(dest))
    lock = _absolute_path(private_root / "destination.lock")
    if preview and not lock.exists():
        yield private_root
        return
    if not preview:
        private_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    private_status = private_root.stat()
    if not stat.S_ISDIR(private_status.st_mode) or private_status.st_mode & 0o077:
        raise PublicationError("unsafe_staging", "Publication staging must be private.")
    flags = os.O_RDONLY if preview else os.O_RDWR | os.O_CREAT
    fd = os.open(lock, flags | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise PublicationError("unsafe_staging", "The publication lock must be a regular file.")
        fcntl.flock(fd, fcntl.LOCK_SH if preview else fcntl.LOCK_EX)
        yield private_root
    finally:
        os.close(fd)


def _sync_directory(directory: Path) -> None:
    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _write_stage_file(path: Path, contents: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(contents)
        stream.flush()
        os.fsync(stream.fileno())


def _atomic_move(staged: Path, target: Path, *, replace_release: bool) -> None:
    """Use Linux renameat2 to activate an entire release in one operation."""
    library = ctypes.CDLL(None, use_errno=True)
    rename = getattr(library, "renameat2", None)
    if rename is None:
        raise PublicationError(
            "atomic_install_unsupported", "Atomic publication activation is unavailable here."
        )
    rename.argtypes = (
        ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint
    )
    rename.restype = ctypes.c_int
    flags = _RENAME_EXCHANGE if replace_release else _RENAME_NOREPLACE
    if rename(_AT_FDCWD, os.fsencode(staged), _AT_FDCWD, os.fsencode(target), flags) != 0:
        error = ctypes.get_errno()
        raise PublicationError(
            "atomic_install_failed",
            f"Could not activate the staged release ({os.strerror(error)}).",
        )


def _activate_release(
    dest: Path, slug: str, assets: Mapping[str, bytes], private_root: Path
) -> None:
    stage = Path(tempfile.mkdtemp(prefix=f"{slug}-", dir=private_root))
    # Use the slug directory in both cases so staged validation checks the
    # exact same identity and layout as a previously committed package.
    staged = stage / slug
    for name, contents in assets.items():
        relative = PurePosixPath(name) if name == _PROVENANCE_FILE else _asset_path(name)
        _write_stage_file(staged / relative, contents)
    _release_page(staged)
    for child in staged.iterdir():
        if child.is_dir():
            _sync_directory(child)
    _sync_directory(staged)
    _sync_directory(stage)
    if dest.exists():
        target = dest / slug
        _atomic_move(staged, target, replace_release=target.exists())
        _sync_directory(dest)
        _sync_directory(stage)
    else:
        stage.chmod(0o755)
        _atomic_move(stage, dest, replace_release=False)
        _sync_directory(dest.parent)
        _sync_directory(private_root)


def publish_index(dest: Path) -> dict[str, Any]:
    """Rebuild the public index from validated committed packages."""
    dest = _absolute_path(dest)
    with _destination_lock(dest, preview=False) as private:
        releases = _read_releases(dest)
        _check_graph(releases)
        entries = []
        for slug, page in sorted(releases.items()):
            target = page.get("superseded_by")
            entry = PublicationIndexEntry.model_validate({
                "slug": slug,
                "title": page["title"],
                "question": page["question"],
                "headline_verdict": page.get("headline_verdict"),
                "status": page["status"],
                "tags": page.get("tags", []),
                "published_at": page["published_at"],
                "superseded_by": target,
                "replacement_status": releases[target]["status"] if target else None,
                "supersedes": [
                    other for other, value in sorted(releases.items())
                    if value.get("superseded_by") == slug
                ],
            })
            entries.append(entry.model_dump(mode="json"))
        dest.mkdir(parents=True, exist_ok=True)
        output = _absolute_path(dest / _INDEX_FILE)
        stage = Path(tempfile.mkdtemp(prefix="index-", dir=private)) / _INDEX_FILE
        payload = {"publication_format_version": 1, "findings": entries}
        _write_stage_file(stage, canonical_bytes(payload))
        os.replace(stage, output)
        _sync_directory(dest)
    return {"status": "ok", "paths": [_INDEX_FILE], "findings": entries, "warnings": []}


def _absolute_or_none(path: Path | None) -> Path | None:
    return _absolute_path(path) if path is not None else None


def _band_parameter_names(value: Any):
    if isinstance(value, Mapping):
        if set(value) == {"param"}:
            yield value["param"]
        else:
            for item in value.values():
                yield from _band_parameter_names(item)
    elif isinstance(value, list):
        for item in value:
            yield from _band_parameter_names(item)


def _measure_unit(contents: BundleContents, panel: str, column: str) -> str | None:
    entries = contents.measures.get("panels", {}).get("measures", ())
    for entry in entries:
        if entry.get("panel") == panel and entry.get("column") == column and entry.get("unit"):
            return entry["unit"]
    return contents.measures.get("input_measures", {}).get(panel, {}).get(column, {}).get("unit")


def _field_unit(
    data: PublicationBundle, field_name: str, key: str, base_term: str | None
) -> str | None:
    if field_name in {"p", "r", "r2", "r2_within"}:
        return "proportion" if field_name != "r" else "correlation"
    analysis = data.analyses.get(key)
    if analysis is None or base_term is None:
        return None
    panel = analysis["panel_name"]
    outcome = analysis["effective_spec"].get("outcome")
    term = next(
        (actual for actual, base in analysis["base_term_by_effective_term"].items()
         if base == base_term),
        None,
    )
    outcome_unit = _measure_unit(data.contents, panel, outcome)
    term_unit = _measure_unit(data.contents, panel, term)
    if outcome_unit and term_unit:
        return f"{outcome_unit} per {term_unit}"
    return None


def _history(
    source: PublicationSource,
    existing: Mapping[str, Any] | None,
    data: PublicationBundle | None,
    published_at: str,
) -> list[dict[str, Any]]:
    previous = list(existing["history"]) if existing else []
    retracted = source.status == "retracted"
    if retracted:
        previous = [
            {name: entry[name] for name in ("published_at", "status")}
            for entry in previous
        ]
        current = {
            "published_at": published_at, "status": "retracted", "change_note": source.change_note
        }
    else:
        primary = data.findings[source.primary_finding]
        manifest = data.contents.manifest
        current = {
            "published_at": published_at,
            "run_id": manifest.run_id,
            "manifest_sha256": data.contents.manifest_sha256,
            "headline_verdict": str(primary.headline_verdict),
            "lock_status": manifest.lock_status.to_dict(),
            "status": source.status,
            "change_note": source.change_note,
        }
    changed = bool(existing) and (
        existing["status"] != source.status
        or existing.get("headline_verdict") != current.get("headline_verdict")
        or existing.get("superseded_by") != source.superseded_by
    )
    if changed and not source.change_note:
        raise PublicationError(
            "change_note_required", "A changed verdict or lifecycle state requires a change note."
        )
    return [*previous, current]


def _validate_transition(
    source: PublicationSource, dest: Path, releases: Mapping[str, Mapping[str, Any]]
) -> Mapping[str, Any] | None:
    existing = releases.get(source.slug)
    if existing is None and (dest / source.slug).exists():
        raise PublicationError(
            "destination_conflict", "The slug is already occupied by other site content."
        )
    if existing is not None and existing["status"] == "retracted":
        raise PublicationError(
            "retraction_terminal", "This slug is retracted. Publish a correction under a new slug."
        )
    if source.status == "retracted" and existing is None:
        raise PublicationError("missing_release", "Retraction requires an existing release.")
    if source.superseded_by is not None:
        target = releases.get(source.superseded_by)
        retained_link = existing and existing.get("superseded_by") == source.superseded_by
        if target is None or (target["status"] == "retracted" and not retained_link):
            raise PublicationError(
                "invalid_supersession", "A new supersession link requires a committed replacement."
            )
    proposed = {"superseded_by": source.superseded_by}
    _check_graph({**releases, source.slug: proposed})
    return existing


def _estimates(data: PublicationBundle) -> frozenset[tuple[str, str]]:
    return frozenset(
        (ref["result_key"], ref["base_term"])
        for ref in data.references if ref.get("base_term") is not None
    )


def _finding_page_record(finding: PublishedFinding) -> dict[str, Any]:
    base = finding.base
    return {
        "id": finding.id,
        "claim": finding.claim,
        "base_verdict": str(base.verdict),
        "compatible_with": list(base.compatible_with) if base.compatible_with_text else None,
        "variants": [
            {
                "id": variant.id,
                "description": variant.description,
                "evaluation_status": variant.evaluation.evaluation_status,
                "verdict": str(variant.evaluation.verdict) if variant.evaluation.verdict else None,
                "agrees": variant.agrees,
                "unavailable_reason": (
                    str(variant.unavailable_reason) if variant.unavailable_reason else None
                ),
            }
            for variant in finding.variants
        ],
    }


def _publication_evaluations(data: PublicationBundle) -> dict[tuple[str, str], dict[str, Any]]:
    evaluations = {}
    for key, finding in data.findings.items():
        variants = {"base": finding.base}
        variants.update({variant.id: variant.evaluation for variant in finding.variants})
        for name, evaluation in variants.items():
            verdict = evaluation.verdict
            evaluations[key, name] = {
                "verdict": str(verdict) if verdict else None,
                "verdict_text": evaluation.verdict_text,
                "label": evaluation.band_label,
                "compatible_with": list(evaluation.compatible_with),
                "compatible_text": evaluation.compatible_with_text,
            }
    return evaluations


def _base_page(
    source: PublicationSource, published_at: str, history: list[dict[str, Any]]
) -> dict[str, Any]:
    return {
        "publication_format_version": 1,
        "slug": source.slug,
        "title": source.title,
        "question": source.question,
        "status": source.status,
        "published_at": published_at,
        "history": history,
    }


def _result_page(
    source: PublicationSource, data: PublicationBundle, page: dict[str, Any]
) -> dict[str, Any]:
    manifest = data.contents.manifest
    primary = data.findings[source.primary_finding]
    page.update(
        headline_verdict=str(primary.headline_verdict),
        base_verdict=str(primary.base.verdict),
        robustness_status=str(primary.robustness_status),
        headline_text=primary.headline_text,
        superseded_by=source.superseded_by,
        tags=source.tags,
        geographies=source.geographies,
        lock_status=manifest.lock_status.to_dict(),
        provenance_tier="recipe_bundle",
        reproducibility=data.reproducibility,
        findings=[_finding_page_record(item) for item in data.findings.values()],
        limitations=[
            limitation for declaration in data.declarations.values()
            for limitation in declaration.get("limitations", ())
        ],
        **manifest.statuses.to_dict(),
    )
    return page


def _result_provenance(
    source: PublicationSource,
    data: PublicationBundle,
    published_at: str,
    source_digests: Mapping[str, str],
) -> dict[str, Any]:
    manifest = data.contents.manifest
    return {
        "publication_format_version": 1,
        "bundle": {
            **source.bundle.model_dump(mode="json"),
            "bundle_format_version": manifest.bundle_format_version,
            "created_at": manifest.created_at,
            "spec_hash": manifest.spec_hash,
            "component_hashes": dict(manifest.component_hashes),
            "lock_status": manifest.lock_status.to_dict(),
            **manifest.statuses.to_dict(),
        },
        "publish_tool_version": version("hhp-lab"),
        "published_at": published_at,
        "source_files": dict(source_digests),
        "permission_decisions": [
            data.permission_decisions[path] for path in sorted(data.permission_decisions)
        ],
        "reproducibility": {
            "status": data.reproducibility, "reasons": list(data.reproducibility_reasons)
        },
    }


def _finding_addresses(data: PublicationBundle) -> list[dict[str, Any]]:
    indexes = {
        row["id"]: index
        for index, row in enumerate(data.contents.documents["findings.json"]["findings"])
    }
    return [
        {
            "address": f"findings.json#/findings/{indexes[key]}/base/compatible_with",
            "raw_value": list(finding.base.compatible_with),
        }
        for key, finding in data.findings.items() if finding.base.compatible_with_text
    ]


def _results_body(
    source: PublicationSource,
    data: PublicationBundle,
    narrative: str,
    *,
    strict: bool,
) -> tuple[str, dict[str, bytes], dict[str, Any], tuple[str, ...]]:
    from hhplab.results.figures import FigureError, render_research_assets
    from hhplab.results.placeholders import (
        PlaceholderError,
        escape_markdown_text,
        render_narrative,
    )

    parameters = {
        name: data.parameters[name]
        for declaration in data.declarations.values()
        for name in _band_parameter_names(declaration["bands"])
        if name in data.parameters
    }
    try:
        rendered = render_narrative(
            narrative,
            rows=data.rows,
            allowed_estimates=_estimates(data),
            allowed_result_keys=data.result_keys,
            evaluations=_publication_evaluations(data),
            parameters=parameters,
            unit_for=lambda field, key, term: _field_unit(data, field, key, term),
            strict=strict,
        )
    except PlaceholderError as exc:
        raise PublicationError("placeholder_failed", str(exc)) from exc

    figure_findings = tuple(figure.finding for figure in source.figures)
    if "figures" not in source.model_fields_set:
        estimate_findings = {
            ref["owner_id"] for ref in data.references
            if ref.get("term") is not None and ref["result_key"].endswith("@base")
        }
        figure_findings = tuple(key for key in source.findings if key in estimate_findings)
    try:
        research = render_research_assets(
            figure_findings=figure_findings,
            findings={key: _finding_page_record(item) for key, item in data.findings.items()},
            declarations=_unique_records(
                data.contents.resolved_spec["recipe"]["findings"], "id"
            ),
            references=data.references,
            analyses=data.analyses,
            parameters=parameters,
            rows=data.rows,
            optional_rows=data.optional_rows,
            unit_for=lambda field, key, term: _field_unit(data, field, key, term),
            escape_text=escape_markdown_text,
        )
    except FigureError as exc:
        raise PublicationError("figure_failed", str(exc)) from exc
    primary = data.findings[source.primary_finding]
    sections = [rendered.markdown, "## How sure are we?"]
    if source.superseded_by:
        sections.insert(0, f"This finding was [superseded](../{source.superseded_by}/).")
    sections.extend(escape_markdown_text(item) for item in (
        primary.robustness_text, primary.base.compatible_with_text
    ) if item)
    if data.contents.manifest.statuses.diagnostics_status.value in {"fail", "incomplete"}:
        sections.append("Some diagnostics failed or could not be completed.")
    sections.extend(escape_markdown_text(note["text"]) for note in data.caveats)
    if research.figures_markdown:
        sections.append(research.figures_markdown)
    sections.append("## For researchers")
    sections.append(research.research_markdown)
    sections.append(
        f"[Download the verified results bundle](<{source.bundle.public_url}>) "
        f"(SHA-256 `{data.contents.manifest_sha256}`)."
    )
    provenance = {
        "placeholders": list(rendered.placeholders),
        "result_values": [
            *_finding_addresses(data), *research.result_values, *data.caveats
        ],
        "artifacts": list(research.artifacts),
    }
    return "\n\n".join(sections), research.assets, provenance, rendered.warnings


def publish(
    publication: Path,
    dest: Path,
    *,
    bundle: Path | None = None,
    public_bundle: Path | None = None,
    strict: bool = False,
    skip_reproduce: bool = False,
    allow_reproducibility_failure: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Check or commit a publication using the same source and destination checks."""
    from hhplab.results.placeholders import escape_markdown_text

    source, source_digests, narrative = _read_source(publication)
    dest = _absolute_path(dest)
    if public_bundle is not None:
        raise PublicationError(
            "public_counterpart_unavailable", "Public counterparts require Phase B."
        )
    data = None
    if source.status != "retracted":
        bundle_path = _absolute_or_none(bundle)
        if bundle_path is None:
            raise PublicationError("bundle_required", "Publishing a finding requires --bundle.")
        data = prepare_publication_bundle(
            source,
            bundle_path,
            skip_reproduce=skip_reproduce,
            allow_reproducibility_failure=allow_reproducibility_failure,
        )

    with _destination_lock(dest, preview=dry_run) as private:
        releases = _read_releases(dest)
        existing = _validate_transition(source, dest, releases)
        published_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        history = _history(source, existing, data, published_at)
        page = _base_page(source, published_at, history)
        if source.change_note:
            page["change_note"] = source.change_note
        if data is None:
            page["retraction_note"] = source.retraction_note
            body = escape_markdown_text(source.retraction_note)
            assets, warnings = {}, ()
            provenance = {"publication_format_version": 1}
        else:
            page = _result_page(source, data, page)
            body, assets, records, warnings = _results_body(source, data, narrative, strict=strict)
            provenance = _result_provenance(source, data, published_at, source_digests) | records
        assets[_PAGE_FILE] = _page_bytes(page, body)
        provenance["asset_sha256"] = {
            name: hashlib.sha256(contents).hexdigest() for name, contents in sorted(assets.items())
        }
        try:
            _validate_package_documents(page, provenance)
        except (KeyError, TypeError, ValueError) as exc:
            raise PublicationError(
                "invalid_package", "The publication did not produce a valid package."
            ) from exc
        assets[_PROVENANCE_FILE] = canonical_bytes(provenance)
        if not dry_run:
            _activate_release(dest, source.slug, assets, private)
    return {
        "status": "ok",
        "slug": source.slug,
        "dry_run": dry_run,
        "paths": [f"{source.slug}/{name}" for name in sorted(assets)],
        "findings": page.get("findings", []),
        "warnings": list(warnings),
    }
