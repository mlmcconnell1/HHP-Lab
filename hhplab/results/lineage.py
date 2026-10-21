"""Value-free source lineage and conservative artifact distribution planning.

Every renderer uses the same artifact decision, including its companion files.
Unknown columns and missing source declarations retain unknown terms.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from hhplab.covariates.catalog import COVARIATE_SOURCE_SPECS
from hhplab.registry.source_registry import SourceRegistryEntry
from hhplab.schema.redistribution import (
    UNKNOWN_TERMS,
    ArtifactDisposition,
    ArtifactScope,
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
    terms = {}
    for source_id in source_ids:
        entry = entries.get(source_id)
        if entry is not None and "redistribution" in entry.metadata:
            terms[source_id] = entry.redistribution
            continue
        spec = CORE_SOURCE_COVERAGE_SPECS.get(source_id) or COVARIATE_SOURCE_SPECS.get(source_id)
        if spec is None:
            candidates = [
                item
                for item in (*CORE_SOURCE_COVERAGE_SPECS.values(), *COVARIATE_SOURCE_SPECS.values())
                if source_id == f"{item.provider}/{item.product}"
            ]
            if candidates:
                terms[source_id] = combine_source_terms(
                    {str(i): item.redistribution for i, item in enumerate(candidates)}
                )
                continue
        terms[source_id] = UNKNOWN_TERMS if spec is None else spec.redistribution
    return terms


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
    return ArtifactScope.ROW_LEVEL if sensitive and group_rows < 10 else ArtifactScope.AGGREGATE


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
