"""Typed redistribution terms and artifact-inclusion decisions.

The contracts here are independent of source providers and result packaging.
Unknown terms remain conservative by default; only aggregate artifacts may
use the explicit unknown-aggregate override. Private bundles may include all
artifacts while recording that private distribution choice.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from pathlib import PurePosixPath, PureWindowsPath
from types import MappingProxyType
from typing import Any
from urllib.parse import urlsplit

from hhplab.schema.results import (
    ResultContractError,
    _require_exact_keys,
    _require_mapping,
    _require_string,
)


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


SMALL_GROUP_THRESHOLD = 10


class PermissionTermsOrigin(StrEnum):
    REGISTRY = "registry"
    CATALOG = "catalog"
    DEFAULT_UNKNOWN = "default_unknown"


@dataclass(frozen=True)
class RedistributionEvidence:
    """Product-specific review evidence; absence never implies permission."""

    url: str
    reviewed_on: str
    scope: str

    def to_dict(self) -> dict[str, str]:
        return {"url": self.url, "reviewed_on": self.reviewed_on, "scope": self.scope}

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> RedistributionEvidence:
        value = _require_mapping(value, context="redistribution evidence")
        _require_exact_keys(
            value, required=("url", "reviewed_on", "scope"), context="redistribution evidence"
        )
        return cls(
            url=_require_string(value["url"], name="redistribution evidence URL"),
            reviewed_on=_require_string(
                value["reviewed_on"], name="redistribution evidence review date"
            ),
            scope=_require_string(value["scope"], name="redistribution evidence scope"),
        )


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


@dataclass(frozen=True)
class PermissionSource:
    """Frozen source terms and the evidence/origin that justified them."""

    row_level: RedistributionStatus
    aggregate: RedistributionStatus
    terms_origin: PermissionTermsOrigin
    evidence: tuple[RedistributionEvidence, ...] = ()

    def __post_init__(self) -> None:
        for name in ("row_level", "aggregate"):
            try:
                status = RedistributionStatus(getattr(self, name))
            except (TypeError, ValueError) as exc:
                raise ResultContractError(f"invalid permission source {name} status") from exc
            object.__setattr__(self, name, status)
        try:
            origin = PermissionTermsOrigin(self.terms_origin)
        except (TypeError, ValueError) as exc:
            raise ResultContractError(
                f"invalid permission terms origin: {self.terms_origin!r}"
            ) from exc
        object.__setattr__(self, "terms_origin", origin)
        evidence = tuple(self.evidence)
        if any(not isinstance(item, RedistributionEvidence) for item in evidence):
            raise ResultContractError(
                "permission source evidence must be RedistributionEvidence values"
            )
        for item in evidence:
            _validate_permission_evidence(item)
        if origin is PermissionTermsOrigin.DEFAULT_UNKNOWN and evidence:
            raise ResultContractError("default_unknown permission sources cannot carry evidence")
        object.__setattr__(self, "evidence", evidence)

    @property
    def terms(self) -> SourceRedistribution:
        return SourceRedistribution(self.row_level, self.aggregate)

    def to_dict(self) -> dict[str, object]:
        return {
            "row_level": self.row_level.value,
            "aggregate": self.aggregate.value,
            "terms_origin": self.terms_origin.value,
            "evidence": [item.to_dict() for item in self.evidence],
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> PermissionSource:
        value = _require_mapping(value, context="permission source")
        _require_exact_keys(
            value,
            required=("row_level", "aggregate", "terms_origin", "evidence"),
            context="permission source",
        )
        if not isinstance(value["evidence"], list):
            raise ResultContractError("permission source evidence must be a list")
        try:
            return cls(
                row_level=RedistributionStatus(value["row_level"]),
                aggregate=RedistributionStatus(value["aggregate"]),
                terms_origin=PermissionTermsOrigin(value["terms_origin"]),
                evidence=tuple(
                    RedistributionEvidence.from_dict(item) for item in value["evidence"]
                ),
            )
        except (TypeError, ValueError) as exc:
            raise ResultContractError(f"invalid permission source: {exc}") from exc


@dataclass(frozen=True)
class PermissionArtifact:
    """One policy artifact and the exact source/disposition decision for it."""

    path: str
    scope: ArtifactScope
    sources: tuple[str, ...]
    combined_terms: SourceRedistribution
    strict_disposition: ArtifactDisposition
    written_disposition: ArtifactDisposition
    companions: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _validate_permission_path(self.path)
        try:
            object.__setattr__(self, "scope", ArtifactScope(self.scope))
            object.__setattr__(
                self, "strict_disposition", ArtifactDisposition(self.strict_disposition)
            )
            object.__setattr__(
                self, "written_disposition", ArtifactDisposition(self.written_disposition)
            )
        except (TypeError, ValueError) as exc:
            raise ResultContractError(f"invalid permission artifact enum: {exc}") from exc
        if not isinstance(self.combined_terms, SourceRedistribution):
            raise ResultContractError(
                "permission artifact combined_terms must be SourceRedistribution"
            )
        sources = tuple(self.sources)
        if not sources:
            raise ResultContractError("permission artifact sources must contain at least one id")
        for source_id in sources:
            _require_string(source_id, name="permission artifact source id")
        if sources != tuple(sorted(set(sources))):
            raise ResultContractError("permission artifact sources must be sorted and unique")
        object.__setattr__(self, "sources", sources)
        companions = tuple(self.companions)
        for path in companions:
            _validate_permission_path(path)
        if companions != tuple(sorted(set(companions))) or self.path in companions:
            raise ResultContractError(
                "permission artifact companions must be sorted, unique, and distinct"
            )
        object.__setattr__(self, "companions", companions)

    def to_dict(self) -> dict[str, object]:
        return {
            "path": self.path,
            "scope": self.scope.value,
            "sources": list(self.sources),
            "combined_terms": self.combined_terms.to_dict(),
            "strict_disposition": self.strict_disposition.value,
            "written_disposition": self.written_disposition.value,
            "companions": list(self.companions),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> PermissionArtifact:
        value = _require_mapping(value, context="permission artifact")
        _require_exact_keys(
            value,
            required=(
                "path", "scope", "sources", "combined_terms", "strict_disposition",
                "written_disposition", "companions",
            ),
            context="permission artifact",
        )
        if not isinstance(value["sources"], list) or not isinstance(value["companions"], list):
            raise ResultContractError("permission artifact sources and companions must be lists")
        try:
            return cls(
                path=value["path"],
                scope=ArtifactScope(value["scope"]),
                sources=tuple(value["sources"]),
                combined_terms=SourceRedistribution.from_dict(value["combined_terms"]),
                strict_disposition=ArtifactDisposition(value["strict_disposition"]),
                written_disposition=ArtifactDisposition(value["written_disposition"]),
                companions=tuple(value["companions"]),
            )
        except (TypeError, ValueError) as exc:
            raise ResultContractError(f"invalid permission artifact: {exc}") from exc


@dataclass(frozen=True)
class PermissionRecord:
    """Portable, validated permission evidence for all bundle artifacts."""

    sources: Mapping[str, PermissionSource]
    artifacts: tuple[PermissionArtifact, ...]
    private: bool = False
    allow_unknown_aggregates: bool = False
    small_group_threshold: int = SMALL_GROUP_THRESHOLD
    schema_version: str = "permissions/1"

    def __post_init__(self) -> None:
        if self.schema_version != "permissions/1":
            raise ResultContractError(
                f"unsupported permission schema version: {self.schema_version!r}"
            )
        if not isinstance(self.private, bool) or not isinstance(
            self.allow_unknown_aggregates, bool
        ):
            raise ResultContractError("permission policy overrides must be boolean")
        if (
            isinstance(self.small_group_threshold, bool)
            or not isinstance(self.small_group_threshold, int)
            or self.small_group_threshold != SMALL_GROUP_THRESHOLD
        ):
            raise ResultContractError(
                f"small_group_threshold must equal {SMALL_GROUP_THRESHOLD}"
            )
        sources = _require_mapping(self.sources, context="permission sources")
        normalized_sources: dict[str, PermissionSource] = {}
        for source_id, permission in sources.items():
            _require_string(source_id, name="permission source id")
            if not isinstance(permission, PermissionSource):
                raise ResultContractError(
                    "permission sources must contain PermissionSource values"
                )
            normalized_sources[source_id] = permission
        artifacts = tuple(self.artifacts)
        if not artifacts:
            raise ResultContractError("permission record requires at least one artifact")
        if any(not isinstance(item, PermissionArtifact) for item in artifacts):
            raise ResultContractError("permission artifacts must contain PermissionArtifact values")
        paths = tuple(item.path for item in artifacts)
        if paths != tuple(sorted(paths)) or len(paths) != len(set(paths)):
            raise ResultContractError("permission artifacts must be sorted by unique primary path")
        lineage_sources = {source_id for item in artifacts for source_id in item.sources}
        if lineage_sources != set(normalized_sources):
            raise ResultContractError(
                "permission source records must exactly cover artifact lineage"
            )
        for artifact in artifacts:
            terms = combine_source_terms(
                {source_id: normalized_sources[source_id].terms for source_id in artifact.sources}
            )
            if artifact.combined_terms != terms:
                raise ResultContractError(
                    "permission artifact combined_terms do not match its sources"
                )
            strict = artifact_disposition(
                terms,
                scope=artifact.scope,
                private=False,
                allow_unknown_aggregates=False,
            )
            written = artifact_disposition(
                terms,
                scope=artifact.scope,
                private=self.private,
                allow_unknown_aggregates=self.allow_unknown_aggregates,
            )
            if artifact.strict_disposition is not strict:
                raise ResultContractError(
                    "permission artifact strict_disposition does not match policy"
                )
            if artifact.written_disposition is not written:
                raise ResultContractError(
                    "permission artifact written_disposition does not match policy"
                )
        object.__setattr__(
            self, "sources", MappingProxyType(dict(sorted(normalized_sources.items())))
        )
        object.__setattr__(self, "artifacts", artifacts)

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "policy": {
                "small_group_threshold": self.small_group_threshold,
                "overrides": {
                    "private": self.private,
                    "allow_unknown_aggregates": self.allow_unknown_aggregates,
                },
            },
            "sources": {source_id: source.to_dict() for source_id, source in self.sources.items()},
            "artifacts": [artifact.to_dict() for artifact in self.artifacts],
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> PermissionRecord:
        value = _require_mapping(value, context="permission record")
        _require_exact_keys(
            value,
            required=("schema_version", "policy", "sources", "artifacts"),
            context="permission record",
        )
        policy = _require_mapping(value["policy"], context="permission policy")
        _require_exact_keys(
            policy,
            required=("small_group_threshold", "overrides"),
            context="permission policy",
        )
        overrides = _require_mapping(policy["overrides"], context="permission overrides")
        _require_exact_keys(
            overrides,
            required=("private", "allow_unknown_aggregates"),
            context="permission overrides",
        )
        sources = _require_mapping(value["sources"], context="permission sources")
        if not isinstance(value["artifacts"], list):
            raise ResultContractError("permission artifacts must be a list")
        return cls(
            schema_version=_require_string(
                value["schema_version"], name="permission schema version"
            ),
            small_group_threshold=policy["small_group_threshold"],
            private=overrides["private"],
            allow_unknown_aggregates=overrides["allow_unknown_aggregates"],
            sources={key: PermissionSource.from_dict(item) for key, item in sources.items()},
            artifacts=tuple(PermissionArtifact.from_dict(item) for item in value["artifacts"]),
        )

    def parents_for_path(self, path: str) -> tuple[PermissionArtifact, ...]:
        """Return every parent whose artifact or companion references ``path``."""
        _validate_permission_path(path)
        return tuple(
            artifact
            for artifact in self.artifacts
            if artifact.path == path or path in artifact.companions
        )


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


def _validate_permission_path(path: str) -> None:
    if not isinstance(path, str) or not path:
        raise ResultContractError("permission artifact path must be a non-empty relative string")
    posix_path = PurePosixPath(path)
    windows_path = PureWindowsPath(path)
    if (
        posix_path.is_absolute()
        or windows_path.is_absolute()
        or windows_path.drive
        or posix_path.as_posix() != path
        or "\\" in path
        or not posix_path.parts
        or any(part in {"", ".", ".."} for part in posix_path.parts)
    ):
        raise ResultContractError(
            f"permission artifact path must be normalized and relative: {path!r}"
        )


def _validate_permission_evidence(evidence: RedistributionEvidence) -> None:
    """Require portable web references and canonical ISO review dates."""
    date_value = _require_string(evidence.reviewed_on, name="redistribution evidence review date")
    try:
        parsed_date = date.fromisoformat(date_value)
    except ValueError as exc:
        raise ResultContractError(
            "redistribution evidence review date must be an ISO YYYY-MM-DD date"
        ) from exc
    if parsed_date.isoformat() != date_value:
        raise ResultContractError(
            "redistribution evidence review date must be an ISO YYYY-MM-DD date"
        )
    url = _require_string(evidence.url, name="redistribution evidence URL")
    if any(character.isspace() or ord(character) < 32 for character in url):
        raise ResultContractError("redistribution evidence URL must be a portable HTTP(S) URL")
    try:
        parsed_url = urlsplit(url)
        hostname = parsed_url.hostname
        _ = parsed_url.port
    except ValueError as exc:
        raise ResultContractError(
            "redistribution evidence URL must be a portable HTTP(S) URL"
        ) from exc
    if (
        parsed_url.scheme.lower() not in {"http", "https"}
        or not parsed_url.netloc
        or not hostname
        or parsed_url.username is not None
        or parsed_url.password is not None
        or "@" in parsed_url.netloc
    ):
        raise ResultContractError("redistribution evidence URL must be a portable HTTP(S) URL")
    _require_string(evidence.scope, name="redistribution evidence scope")


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
