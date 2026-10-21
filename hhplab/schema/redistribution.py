"""Typed redistribution terms and artifact-inclusion decisions.

The contracts here are independent of source providers and result packaging.
Unknown terms remain conservative by default; only aggregate artifacts may
use the explicit unknown-aggregate override. Private bundles may include all
artifacts while recording that private distribution choice.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum

from hhplab.schema.results import ResultContractError, _require_mapping, _require_string


class RedistributionStatus(StrEnum):
    ALLOWED = "allowed"
    RESTRICTED = "restricted"
    UNKNOWN = "unknown"


class ArtifactScope(StrEnum):
    ROW_LEVEL = "row_level"
    AGGREGATE = "aggregate"
    SAMPLE = "sample"


class ArtifactDisposition(StrEnum):
    INCLUDED = "included"
    WITHHELD = "withheld"
    INCLUDED_PRIVATE = "included_private"


@dataclass(frozen=True)
class RedistributionEvidence:
    """Product-specific review evidence; absence never implies permission."""

    url: str
    reviewed_on: str
    scope: str

    def to_dict(self) -> dict[str, str]:
        return {"url": self.url, "reviewed_on": self.reviewed_on, "scope": self.scope}


@dataclass(frozen=True)
class SourceRedistribution:
    """Source terms for row-level and aggregate use."""

    row_level: RedistributionStatus
    aggregate: RedistributionStatus

    def __post_init__(self) -> None:
        for name in ("row_level", "aggregate"):
            try:
                status = RedistributionStatus(getattr(self, name))
            except (TypeError, ValueError) as exc:
                raise ResultContractError(
                    f"invalid source redistribution {name} status: {getattr(self, name)!r}"
                ) from exc
            object.__setattr__(self, name, status)

    def to_dict(self) -> dict[str, str]:
        return {"row_level": self.row_level.value, "aggregate": self.aggregate.value}

    @classmethod
    def from_dict(cls, value: Mapping[str, str]) -> SourceRedistribution:
        value = _require_mapping(value, context="source redistribution")
        if set(value) != {"row_level", "aggregate"}:
            raise ResultContractError(
                "source redistribution requires exactly row_level and aggregate"
            )
        try:
            return cls(
                row_level=RedistributionStatus(value["row_level"]),
                aggregate=RedistributionStatus(value["aggregate"]),
            )
        except (TypeError, ValueError) as exc:
            raise ResultContractError(f"invalid source redistribution status: {exc}") from exc


UNKNOWN_TERMS = SourceRedistribution(RedistributionStatus.UNKNOWN, RedistributionStatus.UNKNOWN)
PUBLIC_DATA_TERMS = SourceRedistribution(RedistributionStatus.ALLOWED, RedistributionStatus.ALLOWED)


def combine_source_terms(
    sources: Mapping[str, SourceRedistribution],
) -> SourceRedistribution:
    """Combine source terms using the most restrictive status in each scope."""
    sources = _require_mapping(sources, context="source redistribution terms")
    if not sources:
        raise ResultContractError("at least one source term is required")
    for source_id, terms in sources.items():
        _require_string(source_id, name="source id")
        if not isinstance(terms, SourceRedistribution):
            raise ResultContractError("source terms must be SourceRedistribution values")
    return SourceRedistribution(
        row_level=_combine_status(term.row_level for term in sources.values()),
        aggregate=_combine_status(term.aggregate for term in sources.values()),
    )


def _combine_status(statuses) -> RedistributionStatus:
    values = tuple(statuses)
    if RedistributionStatus.RESTRICTED in values:
        return RedistributionStatus.RESTRICTED
    if RedistributionStatus.UNKNOWN in values:
        return RedistributionStatus.UNKNOWN
    return RedistributionStatus.ALLOWED


def artifact_disposition(
    terms: SourceRedistribution,
    *,
    scope: ArtifactScope,
    allow_unknown_aggregates: bool = False,
    private: bool = False,
) -> ArtifactDisposition:
    """Decide whether an artifact is included, withheld, or private-only."""
    if not isinstance(terms, SourceRedistribution):
        raise ResultContractError("terms must be SourceRedistribution")
    try:
        scope = ArtifactScope(scope)
    except ValueError as exc:
        raise ResultContractError(f"unknown artifact scope: {scope!r}") from exc
    if not isinstance(allow_unknown_aggregates, bool) or not isinstance(private, bool):
        raise ResultContractError("redistribution overrides must be boolean")
    if private:
        return ArtifactDisposition.INCLUDED_PRIVATE
    if scope is ArtifactScope.SAMPLE:
        return ArtifactDisposition.INCLUDED
    status = terms.row_level if scope is ArtifactScope.ROW_LEVEL else terms.aggregate
    if status is RedistributionStatus.ALLOWED:
        return ArtifactDisposition.INCLUDED
    if status is RedistributionStatus.UNKNOWN and scope is ArtifactScope.AGGREGATE:
        if allow_unknown_aggregates:
            return ArtifactDisposition.INCLUDED
    return ArtifactDisposition.WITHHELD
