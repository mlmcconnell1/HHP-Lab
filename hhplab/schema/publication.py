"""Contracts for researcher-authored recipe-bundle publication sources."""

from __future__ import annotations

import math
import re
from datetime import datetime
from enum import StrEnum
from pathlib import PurePosixPath, PureWindowsPath
from typing import Any, Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, StrictStr, field_validator, model_validator

from hhplab.schema.results import SHA256_PATTERN, RobustnessStatus

type BandVerdict = Literal[
    "supports_claim",
    "partly_supports_claim",
    "no_meaningful_effect",
    "contradicts_claim",
]
"""A verdict that a researcher may assign to a recipe finding band."""

type PublicationStatus = Literal["active", "superseded", "retracted"]
type FigureKind = Literal["estimate_plot"]
type ReproducibilityStatus = Literal["pass", "partial", "fail", "not_run"]
type ProvenanceTier = Literal["recipe_bundle"]

BAND_VERDICT_VALUES = frozenset(
    {
        "supports_claim",
        "partly_supports_claim",
        "no_meaningful_effect",
        "contradicts_claim",
    }
)


class PublicVerdict(StrEnum):
    """Evaluated public verdicts, including an evaluation-derived outcome."""

    SUPPORTS_CLAIM = "supports_claim"
    PARTLY_SUPPORTS_CLAIM = "partly_supports_claim"
    NO_MEANINGFUL_EFFECT = "no_meaningful_effect"
    CONTRADICTS_CLAIM = "contradicts_claim"
    INCONCLUSIVE = "inconclusive"


class HeadlineVerdict(StrEnum):
    """Headline vocabulary; mixed is a display state, never recipe input."""

    SUPPORTS_CLAIM = "supports_claim"
    PARTLY_SUPPORTS_CLAIM = "partly_supports_claim"
    NO_MEANINGFUL_EFFECT = "no_meaningful_effect"
    CONTRADICTS_CLAIM = "contradicts_claim"
    INCONCLUSIVE = "inconclusive"
    MIXED = "mixed"


class SafeUnavailableReason(StrEnum):
    """Closed public wording for an unavailable robustness check."""

    ANALYSIS_NOT_AVAILABLE = "The alternative analysis was not available."
    ANALYSIS_DID_NOT_COMPLETE = "The alternative analysis did not complete."
    REQUIRED_VALUE_NOT_AVAILABLE = (
        "The alternative did not produce all values needed for this finding."
    )
    EVALUATION_NOT_AVAILABLE = "This check could not be evaluated from the available results."

SLUG_PATTERN = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")


class PublicationModel(BaseModel):
    """Strict base for publication source records."""

    model_config = ConfigDict(extra="forbid", strict=True)


class PublicationBundleReference(PublicationModel):
    """Identity and public locator for the one source bundle being cited."""

    recipe_id: StrictStr = Field(min_length=1)
    run_id: StrictStr = Field(min_length=1)
    manifest_sha256: StrictStr
    public_url: StrictStr = Field(min_length=1)
    doi: StrictStr | None = None

    @field_validator("recipe_id", "run_id", "public_url", "doi")
    @classmethod
    def validate_nonblank(cls, value: str | None, info) -> str | None:
        if value is not None and not value.strip():
            raise ValueError(f"{info.field_name} must not be blank")
        return value

    @field_validator("manifest_sha256")
    @classmethod
    def validate_manifest_digest(cls, value: str) -> str:
        if not SHA256_PATTERN.fullmatch(value):
            raise ValueError("manifest_sha256 must be a lowercase 64-character SHA-256 digest")
        return value

    @field_validator("public_url")
    @classmethod
    def validate_public_url(cls, value: str) -> str:
        if any(
            character in "<>"
            or character.isspace()
            or ord(character) < 0x20
            or ord(character) == 0x7F
            for character in value
        ):
            raise ValueError(
                "public_url cannot contain whitespace, control characters, or angle brackets"
            )
        parts = urlsplit(value)
        if (
            parts.scheme not in {"http", "https"}
            or not parts.hostname
            or parts.username is not None
            or parts.password is not None
        ):
            raise ValueError("public_url must be an HTTP(S) URL without embedded credentials")
        return value


class PublicationFigure(PublicationModel):
    """One closed-vocabulary Phase A figure request."""

    kind: FigureKind
    finding: StrictStr = Field(min_length=1)


class PublicationLockStatus(PublicationModel):
    """Normalized lock state copied from the verified bundle."""

    status: Literal["matched", "modified", "unlocked"]
    components: list[StrictStr] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_components(self) -> PublicationLockStatus:
        if self.status == "modified" and not self.components:
            raise ValueError("modified lock status requires components")
        if self.status != "modified" and self.components:
            raise ValueError("only modified lock status may list components")
        if len(self.components) != len(set(self.components)):
            raise ValueError("lock status components must be unique")
        return self


class PublicationPageVariant(PublicationModel):
    """One page-renderable robustness check."""

    id: StrictStr = Field(min_length=1)
    description: StrictStr = Field(min_length=1)
    evaluation_status: StrictStr = Field(min_length=1)
    verdict: PublicVerdict | None = None
    agrees: bool | None = None
    unavailable_reason: SafeUnavailableReason | None = None

    @field_validator("verdict", mode="before")
    @classmethod
    def parse_verdict(cls, value: Any) -> PublicVerdict | None:
        if value is None or isinstance(value, PublicVerdict):
            return value
        if isinstance(value, str):
            try:
                return PublicVerdict(value)
            except ValueError as exc:
                raise ValueError("invalid public verdict") from exc
        raise ValueError("verdict must be a public verdict string")

    @field_validator("unavailable_reason", mode="before")
    @classmethod
    def parse_unavailable_reason(cls, value: Any) -> SafeUnavailableReason | None:
        if value is None or isinstance(value, SafeUnavailableReason):
            return value
        if isinstance(value, str):
            try:
                return SafeUnavailableReason(value)
            except ValueError as exc:
                raise ValueError("invalid safe unavailable reason") from exc
        raise ValueError("unavailable_reason must use the closed public-safe vocabulary")

    @field_validator("id", "description", "evaluation_status")
    @classmethod
    def validate_nonblank(cls, value: str, info) -> str:
        if not value.strip():
            raise ValueError(f"{info.field_name} must not be blank")
        if info.field_name == "description" and ("\n" in value or "\r" in value):
            raise ValueError("variant descriptions must fit on one line")
        return value

    @model_validator(mode="after")
    def validate_availability(self) -> PublicationPageVariant:
        if self.evaluation_status == "not_evaluated":
            if (
                self.verdict is not None
                or self.agrees is not None
                or self.unavailable_reason is None
            ):
                raise ValueError(
                    "not_evaluated variant requires a safe reason and has no verdict or agreement"
                )
        elif self.verdict is None or self.unavailable_reason is not None or self.agrees is None:
            raise ValueError("evaluated variant requires a verdict and agreement value")
        return self


class PublicationPageFinding(PublicationModel):
    """Page representation of one cited finding."""

    id: StrictStr = Field(min_length=1)
    claim: StrictStr = Field(min_length=1)
    base_verdict: PublicVerdict
    compatible_with: list[float | int] | None = None
    variants: list[PublicationPageVariant] = Field(default_factory=list)

    @field_validator("compatible_with")
    @classmethod
    def validate_compatible_values(
        cls, values: list[int | float] | None
    ) -> list[int | float] | None:
        if values is None:
            return None
        for value in values:
            if isinstance(value, bool) or not _is_finite_number(value):
                raise ValueError("compatible_with values must be finite numbers")
        return values

    @field_validator("base_verdict", mode="before")
    @classmethod
    def parse_base_verdict(cls, value: Any) -> PublicVerdict:
        if isinstance(value, PublicVerdict):
            return value
        if isinstance(value, str):
            try:
                return PublicVerdict(value)
            except ValueError as exc:
                raise ValueError("invalid base public verdict") from exc
        raise ValueError("base_verdict must be a public verdict string")

    @model_validator(mode="after")
    def validate_compatible_references(self) -> PublicationPageFinding:
        if self.base_verdict is PublicVerdict.INCONCLUSIVE and self.compatible_with is None:
            raise ValueError(
                "inconclusive finding requires compatible_with, including an empty list"
            )
        if self.base_verdict is not PublicVerdict.INCONCLUSIVE and self.compatible_with is not None:
            raise ValueError("compatible_with is only present for inconclusive findings")
        return self


class PublicationHistoryEntry(PublicationModel):
    """One append-only release history row, including safe tombstone projection."""

    published_at: StrictStr
    run_id: StrictStr | None = None
    manifest_sha256: StrictStr | None = None
    headline_verdict: HeadlineVerdict | None = None
    lock_status: PublicationLockStatus | None = None
    status: PublicationStatus
    change_note: StrictStr | None = None

    @field_validator("headline_verdict", mode="before")
    @classmethod
    def parse_headline_verdict(cls, value: Any) -> HeadlineVerdict | None:
        if value is None or isinstance(value, HeadlineVerdict):
            return value
        if isinstance(value, str):
            try:
                return HeadlineVerdict(value)
            except ValueError as exc:
                raise ValueError("invalid headline verdict") from exc
        raise ValueError("headline_verdict must be a headline verdict string")

    @field_validator("published_at")
    @classmethod
    def validate_published_at(cls, value: str) -> str:
        _require_timezone_timestamp(value, name="published_at")
        return value

    @field_validator("manifest_sha256")
    @classmethod
    def validate_optional_manifest_digest(cls, value: str | None) -> str | None:
        if value is not None and not SHA256_PATTERN.fullmatch(value):
            raise ValueError("manifest_sha256 must be a lowercase 64-character SHA-256 digest")
        return value

    @field_validator("run_id", "change_note")
    @classmethod
    def validate_optional_nonblank(cls, value: str | None, info) -> str | None:
        if value is not None and not value.strip():
            raise ValueError(f"{info.field_name} must not be blank when provided")
        return value

    @model_validator(mode="after")
    def validate_history_release(self) -> PublicationHistoryEntry:
        values = (self.run_id, self.manifest_sha256, self.headline_verdict, self.lock_status)
        if self.has_result_identity and (
            self.status == "retracted" or any(value is None for value in values)
        ):
            raise ValueError("release history requires a complete, publishable result identity")
        return self

    @property
    def has_result_identity(self) -> bool:
        return any(
            field is not None for field in (
                self.run_id, self.manifest_sha256, self.headline_verdict, self.lock_status
            )
        )


class PublicationPage(PublicationModel):
    """Validated `index.md` frontmatter, with a minimal retraction tombstone."""

    publication_format_version: Literal[1]
    slug: StrictStr = Field(min_length=1)
    title: StrictStr = Field(min_length=1)
    question: StrictStr = Field(min_length=1)
    headline_verdict: HeadlineVerdict | None = None
    base_verdict: PublicVerdict | None = None
    robustness_status: RobustnessStatus | None = None
    headline_text: StrictStr | None = None
    status: PublicationStatus
    superseded_by: StrictStr | None = None
    retraction_note: StrictStr | None = None
    change_note: StrictStr | None = None
    tags: list[StrictStr] | None = None
    geographies: StrictStr | None = None
    published_at: StrictStr
    lock_status: PublicationLockStatus | None = None
    provenance_tier: ProvenanceTier | None = None
    reproducibility: ReproducibilityStatus | None = None
    execution_status: Literal["complete", "partial", "failed"] | None = None
    diagnostics_status: Literal[
        "pass", "fail", "incomplete", "not_applicable", "none"
    ] | None = None
    findings_status: Literal["all_evaluated", "some_not_evaluated", "ambiguous"] | None = None
    distribution_status: Literal["full", "withheld", "private"] | None = None
    findings: list[PublicationPageFinding] | None = None
    limitations: list[StrictStr] | None = None
    history: list[PublicationHistoryEntry]

    @field_validator("headline_verdict", mode="before")
    @classmethod
    def parse_headline_verdict(cls, value: Any) -> HeadlineVerdict | None:
        if value is None or isinstance(value, HeadlineVerdict):
            return value
        if isinstance(value, str):
            try:
                return HeadlineVerdict(value)
            except ValueError as exc:
                raise ValueError("invalid headline verdict") from exc
        raise ValueError("headline_verdict must be a headline verdict string")

    @field_validator("base_verdict", mode="before")
    @classmethod
    def parse_base_verdict(cls, value: Any) -> PublicVerdict | None:
        if value is None or isinstance(value, PublicVerdict):
            return value
        if isinstance(value, str):
            try:
                return PublicVerdict(value)
            except ValueError as exc:
                raise ValueError("invalid base verdict") from exc
        raise ValueError("base_verdict must be a public verdict string")

    @field_validator("robustness_status", mode="before")
    @classmethod
    def parse_robustness_status(cls, value: Any) -> RobustnessStatus | None:
        if value is None or isinstance(value, RobustnessStatus):
            return value
        if isinstance(value, str):
            try:
                return RobustnessStatus(value)
            except ValueError as exc:
                raise ValueError("invalid robustness status") from exc
        raise ValueError("robustness_status must be a robustness status string")

    @field_validator("slug", "title", "question")
    @classmethod
    def validate_nonblank_page_identity(cls, value: str, info) -> str:
        if not value.strip():
            raise ValueError(f"{info.field_name} must not be blank")
        return value

    @field_validator("published_at")
    @classmethod
    def validate_page_date(cls, value: str) -> str:
        _require_timezone_timestamp(value, name="published_at")
        return value

    @field_validator("publication_format_version", mode="before")
    @classmethod
    def validate_page_format_version(cls, value: Any) -> int:
        return _publication_format_version(value)

    @model_validator(mode="after")
    def validate_page_state(self) -> PublicationPage:
        if not SLUG_PATTERN.fullmatch(self.slug):
            raise ValueError(f"slug must match {SLUG_PATTERN.pattern!r}")
        if self.superseded_by is not None and not SLUG_PATTERN.fullmatch(self.superseded_by):
            raise ValueError("superseded_by must be a canonical slug")
        if self.status == "superseded":
            if self.superseded_by is None or self.superseded_by == self.slug:
                raise ValueError("superseded page requires a distinct superseded_by slug")
            if self.retraction_note is not None:
                raise ValueError("superseded page cannot set retraction_note")
        elif self.status == "active":
            if self.superseded_by is not None or self.retraction_note is not None:
                raise ValueError("active page cannot set transition-only fields")
        else:
            if self.retraction_note is None or not self.retraction_note.strip():
                raise ValueError("retracted page requires retraction_note")
            if self.superseded_by is not None:
                raise ValueError("retracted page cannot set superseded_by")
            tombstone_fields = (
                self.headline_verdict,
                self.base_verdict,
                self.robustness_status,
                self.headline_text,
                self.lock_status,
                self.provenance_tier,
                self.reproducibility,
                self.execution_status,
                self.diagnostics_status,
                self.findings_status,
                self.distribution_status,
                self.findings,
                self.limitations,
            )
            tombstone_fields += (self.tags, self.geographies)
            if any(value not in (None, [], {}) for value in tombstone_fields):
                raise ValueError("retracted page cannot expose result details")
            if any(entry.has_result_identity for entry in self.history):
                raise ValueError("retracted page cannot expose withdrawn release identities")
        if self.status != "retracted":
            if any(not entry.has_result_identity for entry in self.history):
                raise ValueError("active or superseded page requires complete release history")
            required = {
                "headline_verdict": self.headline_verdict,
                "base_verdict": self.base_verdict,
                "robustness_status": self.robustness_status,
                "headline_text": self.headline_text,
                "tags": self.tags,
                "geographies": self.geographies,
                "lock_status": self.lock_status,
                "provenance_tier": self.provenance_tier,
                "reproducibility": self.reproducibility,
                "execution_status": self.execution_status,
                "diagnostics_status": self.diagnostics_status,
                "findings_status": self.findings_status,
                "distribution_status": self.distribution_status,
                "findings": self.findings,
                "limitations": self.limitations,
            }
            missing = [name for name, value in required.items() if value is None]
            if missing:
                raise ValueError(f"active or superseded page is missing fields: {sorted(missing)}")
            if self.tags is not None and len(self.tags) != len(set(self.tags)):
                raise ValueError("page tags must be unique")
            if self.findings is not None:
                finding_ids = [finding.id for finding in self.findings]
                if len(finding_ids) != len(set(finding_ids)):
                    raise ValueError("page findings must be unique")
        return self


class PublicationProvenanceBundle(PublicationBundleReference):
    """The cited source bundle identity copied into provenance.json."""

    bundle_format_version: StrictStr
    created_at: StrictStr
    spec_hash: StrictStr
    component_hashes: dict[StrictStr, StrictStr]
    lock_status: PublicationLockStatus
    execution_status: Literal["complete", "partial", "failed"]
    diagnostics_status: Literal["pass", "fail", "incomplete", "not_applicable", "none"]
    findings_status: Literal["all_evaluated", "some_not_evaluated", "ambiguous"]
    distribution_status: Literal["full", "withheld", "private"]

    @field_validator("created_at")
    @classmethod
    def validate_created_at(cls, value: str) -> str:
        _require_timezone_timestamp(value, name="created_at")
        return value

    @field_validator("spec_hash")
    @classmethod
    def validate_spec_hash(cls, value: str) -> str:
        if not SHA256_PATTERN.fullmatch(value):
            raise ValueError("spec_hash must be a lowercase 64-character SHA-256 digest")
        return value

    @field_validator("component_hashes")
    @classmethod
    def validate_component_hashes(cls, values: dict[str, str]) -> dict[str, str]:
        if any(
            not name.strip() or not SHA256_PATTERN.fullmatch(digest)
            for name, digest in values.items()
        ):
            raise ValueError("component_hashes must map names to lowercase SHA-256 digests")
        return values


class PublicationProvenance(PublicationModel):
    """Strict provenance.json contract, including the minimal tombstone form."""

    publication_format_version: Literal[1]
    bundle: PublicationProvenanceBundle | None = None
    publish_tool_version: StrictStr | None = None
    published_at: StrictStr | None = None
    source_files: dict[StrictStr, StrictStr] | None = None
    placeholders: list[dict[str, Any]] | None = None
    result_values: list[dict[str, Any]] | None = None
    artifacts: list[dict[str, Any]] | None = None
    permission_decisions: list[dict[str, Any]] | None = None
    reproducibility: dict[str, Any] | None = None
    asset_sha256: dict[StrictStr, StrictStr]

    @field_validator("published_at")
    @classmethod
    def validate_provenance_date(cls, value: str | None) -> str | None:
        if value is not None:
            _require_timezone_timestamp(value, name="published_at")
        return value

    @field_validator("publication_format_version", mode="before")
    @classmethod
    def validate_provenance_format_version(cls, value: Any) -> int:
        return _publication_format_version(value)

    @field_validator("publish_tool_version")
    @classmethod
    def validate_tool_version(cls, value: str | None) -> str | None:
        if value is not None and not value.strip():
            raise ValueError("publish_tool_version must not be blank")
        return value

    @field_validator("source_files", "asset_sha256")
    @classmethod
    def validate_digest_maps(cls, values: dict[str, str] | None, info) -> dict[str, str] | None:
        if values is None:
            return None
        if any(
            not _is_relative_artifact_path(name) or not SHA256_PATTERN.fullmatch(digest)
            for name, digest in values.items()
        ):
            raise ValueError(f"{info.field_name} must map paths to lowercase SHA-256 digests")
        return values

    @model_validator(mode="after")
    def validate_provenance_form(self) -> PublicationProvenance:
        if self.bundle is None:
            allowed_tombstone = {"publication_format_version", "asset_sha256"}
            present = self.model_fields_set
            if present - allowed_tombstone:
                raise ValueError(
                    "retraction provenance may contain only format version and asset_sha256"
                )
            if "index.md" not in self.asset_sha256:
                raise ValueError("retraction provenance asset_sha256 must include index.md")
        else:
            required = {
                "publish_tool_version": self.publish_tool_version,
                "published_at": self.published_at,
                "source_files": self.source_files,
                "placeholders": self.placeholders,
                "result_values": self.result_values,
                "artifacts": self.artifacts,
                "permission_decisions": self.permission_decisions,
                "reproducibility": self.reproducibility,
            }
            missing = [name for name, value in required.items() if value is None]
            if missing:
                raise ValueError(f"provenance is missing fields: {sorted(missing)}")
            if "index.md" not in self.asset_sha256:
                raise ValueError("asset_sha256 must include index.md")
        return self


class PublicationIndexEntry(PublicationModel):
    """One deterministic index entry and its supersession links."""

    slug: StrictStr = Field(min_length=1)
    title: StrictStr = Field(min_length=1)
    question: StrictStr = Field(min_length=1)
    headline_verdict: HeadlineVerdict | None = None
    status: PublicationStatus
    tags: list[StrictStr] = Field(default_factory=list)
    published_at: StrictStr
    superseded_by: StrictStr | None = None
    supersedes: list[StrictStr] = Field(default_factory=list)
    replacement_status: PublicationStatus | None = None

    @field_validator("headline_verdict", mode="before")
    @classmethod
    def parse_index_headline_verdict(cls, value: Any) -> HeadlineVerdict | None:
        if value is None or isinstance(value, HeadlineVerdict):
            return value
        if isinstance(value, str):
            try:
                return HeadlineVerdict(value)
            except ValueError as exc:
                raise ValueError("invalid headline verdict") from exc
        raise ValueError("headline_verdict must be a headline verdict string")

    @field_validator("published_at")
    @classmethod
    def validate_index_date(cls, value: str) -> str:
        _require_timezone_timestamp(value, name="published_at")
        return value

    @model_validator(mode="after")
    def validate_index_links(self) -> PublicationIndexEntry:
        if not SLUG_PATTERN.fullmatch(self.slug):
            raise ValueError("slug must be canonical")
        if self.superseded_by is not None and not SLUG_PATTERN.fullmatch(self.superseded_by):
            raise ValueError("superseded_by must be canonical")
        if any(not SLUG_PATTERN.fullmatch(slug) for slug in self.supersedes):
            raise ValueError("supersedes entries must be canonical slugs")
        if len(self.supersedes) != len(set(self.supersedes)):
            raise ValueError("supersedes entries must be unique")
        if self.status == "retracted" and self.headline_verdict is not None:
            raise ValueError("retracted index entries cannot show a headline verdict")
        if self.status != "retracted" and self.headline_verdict is None:
            raise ValueError("active and superseded index entries require a headline verdict")
        if self.status == "superseded" and self.superseded_by is None:
            raise ValueError("superseded index entries require superseded_by")
        if self.replacement_status is not None and self.superseded_by is None:
            raise ValueError("replacement_status requires superseded_by")
        return self


def _require_timezone_timestamp(value: str, *, name: str) -> None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{name} must include a timezone")


def _publication_format_version(value: Any) -> int:
    if isinstance(value, bool) or value != 1:
        raise ValueError("publication_format_version must be integer 1")
    return 1


def _is_relative_artifact_path(value: str) -> bool:
    path = PurePosixPath(value)
    windows_path = PureWindowsPath(value)
    return (
        bool(value)
        and not path.is_absolute()
        and not windows_path.is_absolute()
        and not windows_path.drive
        and all(part not in {"", ".", ".."} for part in path.parts)
    )


def _is_finite_number(value: int | float) -> bool:
    return isinstance(value, int) or math.isfinite(value)


class PublicationSource(PublicationModel):
    """Validated ``publication.yaml`` metadata, independent of filesystem I/O."""

    publication_format_version: Literal[1]
    slug: StrictStr = Field(min_length=1)
    title: StrictStr = Field(min_length=1)
    question: StrictStr = Field(min_length=1)
    status: PublicationStatus
    superseded_by: StrictStr | None = None
    retraction_note: StrictStr | None = None
    tags: list[StrictStr] = Field(default_factory=list)
    geographies: StrictStr | None = None
    bundle: PublicationBundleReference | None = None
    findings: list[StrictStr] = Field(default_factory=list)
    primary_finding: StrictStr | None = None
    variant_descriptions: dict[StrictStr, StrictStr] = Field(default_factory=dict)
    figures: list[PublicationFigure] = Field(default_factory=list)
    change_note: StrictStr | None = None

    @field_validator("slug", "title", "question", "geographies")
    @classmethod
    def validate_nonblank(cls, value: str | None, info) -> str | None:
        if value is not None and not value.strip():
            raise ValueError(f"{info.field_name} must not be blank")
        return value

    @field_validator("publication_format_version", mode="before")
    @classmethod
    def validate_source_format_version(cls, value: Any) -> int:
        return _publication_format_version(value)

    @field_validator("slug", "superseded_by")
    @classmethod
    def validate_slug(cls, value: str | None, info) -> str | None:
        if value is not None and not SLUG_PATTERN.fullmatch(value):
            raise ValueError(f"{info.field_name} must match {SLUG_PATTERN.pattern!r}")
        return value

    @field_validator("retraction_note", "change_note")
    @classmethod
    def validate_optional_note(cls, value: str | None, info) -> str | None:
        if value is not None and not value.strip():
            raise ValueError(f"{info.field_name} must not be blank when provided")
        return value

    @field_validator("tags")
    @classmethod
    def validate_tags(cls, values: list[str]) -> list[str]:
        if any(not value.strip() for value in values):
            raise ValueError("tags must not contain blank values")
        if len(values) != len(set(values)):
            raise ValueError("tags must be unique")
        return values

    @field_validator("findings")
    @classmethod
    def validate_findings(cls, values: list[str]) -> list[str]:
        if any(not value.strip() for value in values):
            raise ValueError("findings must not contain blank IDs")
        if len(values) != len(set(values)):
            raise ValueError("findings must be unique and retain display order")
        return values

    @field_validator("variant_descriptions")
    @classmethod
    def validate_variant_descriptions(cls, values: dict[str, str]) -> dict[str, str]:
        for variant_id, description in values.items():
            if not variant_id.strip() or not description.strip():
                raise ValueError("variant descriptions require nonblank IDs and text")
            if "\n" in description or "\r" in description:
                raise ValueError("variant descriptions must fit on one line")
        return values

    @model_validator(mode="after")
    def validate_source(self) -> PublicationSource:
        if not SLUG_PATTERN.fullmatch(self.slug):
            raise ValueError(f"slug must match {SLUG_PATTERN.pattern!r}")

        figure_keys = [(figure.kind, figure.finding) for figure in self.figures]
        if len(figure_keys) != len(set(figure_keys)):
            raise ValueError("figure requests must be unique")
        uncited_figures = sorted({figure.finding for figure in self.figures} - set(self.findings))
        if uncited_figures:
            raise ValueError(
                f"figure findings must be cited by this publication: {uncited_figures}"
            )

        if self.status == "active":
            if self.geographies is None:
                raise ValueError("active publication requires geographies")
            if self.superseded_by is not None or self.retraction_note is not None:
                raise ValueError("active publication cannot set superseded_by or retraction_note")
            self._validate_results_release()
        elif self.status == "superseded":
            if self.geographies is None:
                raise ValueError("superseded publication requires geographies")
            if self.superseded_by is None:
                raise ValueError("superseded publication requires superseded_by")
            if self.superseded_by == self.slug:
                raise ValueError("superseded_by must identify a different publication slug")
            if self.retraction_note is not None:
                raise ValueError("superseded publication cannot set retraction_note")
            self._validate_results_release()
        else:
            if self.retraction_note is None:
                raise ValueError("retracted publication requires retraction_note")
            if self.superseded_by is not None:
                raise ValueError("retracted publication cannot set superseded_by")
            if self.bundle is None and (
                self.findings or self.primary_finding is not None or self.figures
            ):
                raise ValueError(
                    "a retraction without a bundle cannot select findings or figures"
                )
            if self.primary_finding is not None and self.primary_finding not in self.findings:
                raise ValueError("primary_finding must be included in findings")
        return self

    def _validate_results_release(self) -> None:
        if self.bundle is None:
            raise ValueError(f"{self.status} publication requires a bundle reference")
        if not self.findings:
            raise ValueError(f"{self.status} publication requires at least one cited finding")
        if self.primary_finding is None or self.primary_finding not in self.findings:
            raise ValueError("primary_finding must be included in findings")


__all__ = [
    "BandVerdict",
    "FigureKind",
    "HeadlineVerdict",
    "PublicationHistoryEntry",
    "PublicationIndexEntry",
    "PublicationLockStatus",
    "PublicationPage",
    "PublicationPageFinding",
    "PublicationPageVariant",
    "PublicationProvenance",
    "PublicationProvenanceBundle",
    "PublicationBundleReference",
    "PublicationFigure",
    "PublicationSource",
    "PublicationStatus",
    "PublicVerdict",
    "ReproducibilityStatus",
    "SafeUnavailableReason",
    "ProvenanceTier",
]
