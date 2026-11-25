"""Value-free source lineage and conservative artifact distribution planning.

Every renderer uses the same artifact decision, including its companion files.
Unknown columns and missing source declarations retain unknown terms.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from hhplab.covariates.catalog import COVARIATE_SOURCE_SPECS
from hhplab.registry.source_registry import SourceRegistryEntry
from hhplab.schema.redistribution import (
    SMALL_GROUP_THRESHOLD,
    UNKNOWN_TERMS,
    ArtifactDisposition,
    ArtifactScope,
    PermissionArtifact,
    PermissionRecord,
    PermissionSource,
    PermissionTermsOrigin,
    RedistributionEvidence,
    SourceRedistribution,
    artifact_disposition,
    combine_source_terms,
)
from hhplab.schema.results import ResultContractError
from hhplab.sources.coverage import CORE_SOURCE_COVERAGE_SPECS


def declared_source_terms(
    source_ids: Iterable[str], registry_entries: Mapping[str, SourceRegistryEntry] | None = None
) -> dict[str, SourceRedistribution]:
    """Load exact product declarations; explicit ingestion terms take precedence.

    No provider prefix or URL matching broadens a declaration to other products.
    Missing registry terms do not override a reviewed product declaration.
    """
    entries = registry_entries or {}
    return {
        source_id: _resolve_source_declaration(source_id, entries)[0]
        for source_id in source_ids
    }


def _resolve_source_declaration(
    source_id: str,
    registry_entries: Mapping[str, SourceRegistryEntry],
    *,
    include_evidence: bool = False,
) -> tuple[SourceRedistribution, PermissionTermsOrigin, tuple[Any, ...]]:
    """Select the single shared registry/catalog/default declaration branch."""
    entry = registry_entries.get(source_id)
    if entry is not None and "redistribution" in entry.metadata:
        evidence = tuple(entry.redistribution_evidence) if include_evidence else ()
        return entry.redistribution, PermissionTermsOrigin.REGISTRY, evidence

    spec = CORE_SOURCE_COVERAGE_SPECS.get(source_id) or COVARIATE_SOURCE_SPECS.get(source_id)
    if spec is not None:
        evidence = tuple(spec.redistribution_evidence) if include_evidence else ()
        return spec.redistribution, PermissionTermsOrigin.CATALOG, evidence

    candidates = [
        item
        for item in (*CORE_SOURCE_COVERAGE_SPECS.values(), *COVARIATE_SOURCE_SPECS.values())
        if source_id == f"{item.provider}/{item.product}"
    ]
    if candidates:
        terms = combine_source_terms(
            {str(index): item.redistribution for index, item in enumerate(candidates)}
        )
        evidence = (
            tuple(
                evidence
                for item in candidates
                for evidence in item.redistribution_evidence
            )
            if include_evidence
            else ()
        )
        return terms, PermissionTermsOrigin.CATALOG, evidence
    return UNKNOWN_TERMS, PermissionTermsOrigin.DEFAULT_UNKNOWN, ()


def resolve_permission_sources(
    source_ids: Iterable[str],
    registry_entries: Mapping[str, SourceRegistryEntry] | None = None,
    direct_terms: Mapping[str, SourceRedistribution] | None = None,
) -> dict[str, PermissionSource]:
    """Resolve terms and evidence for an exact set of source identities.

    Bare direct terms retain their values for the written-disposition record,
    but have default-unknown provenance and no evidence. They therefore never
    establish publication permission by themselves.
    """
    entries = registry_entries or {}
    direct = direct_terms or {}
    resolved: dict[str, PermissionSource] = {}
    requested = tuple(source_ids)
    for source_id in requested:
        if not isinstance(source_id, str) or not source_id.strip():
            raise ResultContractError("source ids must be non-empty strings")
    for source_id in sorted(set(requested)):
        if source_id in direct:
            terms = direct[source_id]
            if not isinstance(terms, SourceRedistribution):
                raise ResultContractError("direct source terms must be SourceRedistribution values")
            resolved[source_id] = _permission_source(
                terms, PermissionTermsOrigin.DEFAULT_UNKNOWN, ()
            )
            continue

        terms, origin, evidence = _resolve_source_declaration(
            source_id, entries, include_evidence=True
        )
        resolved[source_id] = _permission_source(
            terms, origin, tuple(dict.fromkeys(_evidence_records(evidence)))
        )
    return resolved


def permission_record(
    artifacts: Iterable[ArtifactLineage],
    source_permissions: Mapping[str, PermissionSource],
    *,
    private: bool = False,
    allow_unknown_aggregates: bool = False,
    fallback_terms: Mapping[str, SourceRedistribution] | None = None,
) -> PermissionRecord:
    """Freeze source terms, evidence and both distribution decisions."""
    source_map = dict(source_permissions)
    fallback = fallback_terms or {}
    policy_artifacts: list[PermissionArtifact] = []
    used_sources: set[str] = set()
    for artifact in artifacts:
        if not isinstance(artifact, ArtifactLineage):
            raise ResultContractError("permission record artifacts must be ArtifactLineage values")
        sources = tuple(sorted(set(artifact.sources))) or ("unknown-artifact",)
        used_sources.update(sources)
        for source_id in sources:
            if source_id not in source_map:
                terms = fallback.get(source_id, UNKNOWN_TERMS)
                if not isinstance(terms, SourceRedistribution):
                    raise ResultContractError(
                        "fallback source terms must be SourceRedistribution values"
                    )
                source_map[source_id] = _permission_source(
                    terms, PermissionTermsOrigin.DEFAULT_UNKNOWN, ()
                )
        combined = combine_source_terms(
            {source_id: source_map[source_id].terms for source_id in sources}
        )
        policy_artifacts.append(
            PermissionArtifact(
                path=artifact.path,
                scope=artifact.scope,
                sources=sources,
                combined_terms=combined,
                strict_disposition=artifact_disposition(
                    combined,
                    scope=artifact.scope,
                    private=False,
                    allow_unknown_aggregates=False,
                ),
                written_disposition=artifact_disposition(
                    combined,
                    scope=artifact.scope,
                    private=private,
                    allow_unknown_aggregates=allow_unknown_aggregates,
                ),
                companions=tuple(sorted(set(artifact.companions))),
            )
        )
    exact_sources = {source_id: source_map[source_id] for source_id in sorted(used_sources)}
    return PermissionRecord(
        sources=exact_sources,
        artifacts=tuple(sorted(policy_artifacts, key=lambda item: item.path)),
        private=private,
        allow_unknown_aggregates=allow_unknown_aggregates,
    )


def _permission_source(
    terms: SourceRedistribution,
    origin: PermissionTermsOrigin,
    evidence: tuple[RedistributionEvidence, ...],
) -> PermissionSource:
    if not isinstance(terms, SourceRedistribution):
        raise ResultContractError("resolved terms must be SourceRedistribution values")
    return PermissionSource(terms.row_level, terms.aggregate, origin, evidence)


def _evidence_records(values: Iterable[Any]) -> tuple[RedistributionEvidence, ...]:
    evidence: list[RedistributionEvidence] = []
    for value in values:
        if isinstance(value, RedistributionEvidence):
            evidence.append(value)
        elif isinstance(value, Mapping):
            evidence.append(RedistributionEvidence.from_dict(value))
        else:
            raise ResultContractError("source redistribution evidence has an invalid shape")
    return tuple(evidence)


def union_lineage(columns: Iterable[str], lineage: Mapping[str, Iterable[str]]) -> tuple[str, ...]:
    """Resolve an ordered column dependency set into stable source identities."""
    sources: set[str] = set()
    for column in columns:
        values = tuple(lineage.get(column, ()))
        if not values:
            values = (f"unknown-column:{column}",)
        if any(not isinstance(value, str) or not value for value in values):
            raise ResultContractError(f"invalid source lineage for {column!r}")
        sources.update(values)
    return tuple(sorted(sources))


def derive_lineage(
    lineage: Mapping[str, Iterable[str]], definitions: Mapping[str, Iterable[str]]
) -> dict[str, tuple[str, ...]]:
    """Propagate unions through a derivation graph, rejecting cycles/collisions."""
    resolved = {name: tuple(sorted(set(sources))) for name, sources in lineage.items()}
    pending = {name: tuple(inputs) for name, inputs in definitions.items()}
    if set(resolved) & set(pending):
        raise ResultContractError("derived lineage names collide with input columns")
    while pending:
        ready = [name for name, inputs in pending.items() if not set(inputs) & pending.keys()]
        if not ready:
            raise ResultContractError("derived lineage contains a dependency cycle")
        for name in ready:
            inputs = pending.pop(name)
            resolved[name] = union_lineage(inputs, resolved) or (f"unknown-column:{name}",)
    return resolved


def descriptive_scope(statistic: str, group_rows: int) -> ArtifactScope:
    """Small-group extrema and quantiles are row-level disclosures."""
    if isinstance(group_rows, bool) or not isinstance(group_rows, int) or group_rows < 0:
        raise ResultContractError("group_rows must be a nonnegative integer")
    sensitive = statistic in {
        "min",
        "max",
        "median",
        "p10",
        "p25",
        "p75",
        "p90",
        "quantile",
        "percentile",
    }
    sensitive = sensitive or statistic.startswith(("quantile_", "percentile_"))
    return (
        ArtifactScope.ROW_LEVEL
        if sensitive and group_rows < SMALL_GROUP_THRESHOLD
        else ArtifactScope.AGGREGATE
    )


@dataclass(frozen=True)
class ArtifactLineage:
    """Metadata shared by panel, fit, table, finding and report artifacts."""

    path: str
    sources: tuple[str, ...]
    scope: ArtifactScope
    replay_dependencies: tuple[str, ...] = ()
    companions: tuple[str, ...] = ()


def distribution_plan(
    artifacts: Iterable[ArtifactLineage],
    source_terms: Mapping[str, SourceRedistribution],
    *,
    private: bool = False,
    allow_unknown_aggregates: bool = False,
) -> dict[str, object]:
    """Return only paths and metadata, never source values or rendered content.

    A companion cannot evade its parent's decision. Shared companions receive
    the most conservative decision across all their parents.
    """
    included: set[str] = set()
    withheld: dict[str, dict[str, object]] = {}
    decisions: dict[str, str] = {}
    for artifact in artifacts:
        sources = tuple(sorted(set(artifact.sources))) or ("unknown-artifact",)
        terms = combine_source_terms({s: source_terms.get(s, UNKNOWN_TERMS) for s in sources})
        disposition = artifact_disposition(
            terms,
            scope=artifact.scope,
            private=private,
            allow_unknown_aggregates=allow_unknown_aggregates,
        )
        for path in (artifact.path, *artifact.companions):
            if not path or path.startswith("/") or ".." in path.split("/"):
                raise ResultContractError(f"artifact path must be relative and contained: {path!r}")
            if disposition is ArtifactDisposition.WITHHELD:
                record = withheld.setdefault(
                    path,
                    {
                        "path": path,
                        "sources": [],
                        "replay_dependencies": [],
                        "reason": "Source redistribution terms do not permit this artifact.",
                        "rebuild": (
                            "Restore authorized source inputs and replay the recipe locally."
                        ),
                    },
                )
                record["sources"] = sorted(set(record["sources"]) | set(sources))
                record["replay_dependencies"] = sorted(
                    set(record["replay_dependencies"]) | set(artifact.replay_dependencies)
                )
                decisions[path] = disposition.value
            elif path not in withheld:
                included.add(path)
                decisions[path] = disposition.value
    return {
        "private": private,
        "included": sorted(included - withheld.keys()),
        "withheld": [withheld[path] for path in sorted(withheld)],
        "decisions": dict(sorted(decisions.items())),
    }
