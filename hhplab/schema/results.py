"""Versioned result-bundle, status, and ledger contracts.

This module is deliberately limited to standard-library types. It owns the
serialized contracts shared by recipe, analysis, results, and storage layers.
The bundle format version changes only when a consumer can no longer interpret
the prior serialized shape; additive schema changes require a new schema
version while preserving the bundle version when readers remain compatible.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import PurePosixPath, PureWindowsPath
from typing import Any

BUNDLE_FORMAT_VERSION = "1.0"
RESULTS_SCHEMA_VERSION = "1.0"
LEDGER_SCHEMA_VERSION = "1.0"
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class ResultContractError(ValueError):
    """Raised when a result artifact violates its versioned contract."""


class ExecutionStatus(StrEnum):
    COMPLETE = "complete"
    PARTIAL = "partial"
    FAILED = "failed"


class DiagnosticsStatus(StrEnum):
    PASS = "pass"
    FAIL = "fail"
    INCOMPLETE = "incomplete"
    NOT_APPLICABLE = "not_applicable"
    NONE = "none"


class FindingsStatus(StrEnum):
    ALL_EVALUATED = "all_evaluated"
    SOME_NOT_EVALUATED = "some_not_evaluated"
    AMBIGUOUS = "ambiguous"


class DistributionStatus(StrEnum):
    FULL = "full"
    WITHHELD = "withheld"
    PRIVATE = "private"


class AnalysisStatus(StrEnum):
    PENDING = "pending"
    COMPLETE = "complete"
    FAILED = "failed"
    SKIPPED = "skipped"


class DiagnosticStatus(StrEnum):
    PASS = "pass"
    FAIL = "fail"
    NOT_EVALUATED = "not_evaluated"
    NOT_APPLICABLE = "not_applicable"


class FindingStatus(StrEnum):
    INCONCLUSIVE = "inconclusive"
    AMBIGUOUS_RULE = "ambiguous_rule"
    NOT_EVALUATED = "not_evaluated"


class RobustnessStatus(StrEnum):
    ROBUST = "robust"
    FRAGILE = "fragile"
    INCOMPLETE = "incomplete"
    UNINFORMATIVE = "uninformative"
    NOT_EVALUATED = "not_evaluated"


class LockState(StrEnum):
    MATCHED = "matched"
    MODIFIED = "modified"
    UNLOCKED = "unlocked"


@dataclass(frozen=True)
class CapabilityContract:
    """One named analysis capability and the phase that provides it."""

    name: str
    phase: int
    available: bool

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "phase": self.phase, "available": self.available}


RESULTS_CAPABILITIES: Mapping[str, CapabilityContract] = {
    contract.name: contract
    for contract in (
        CapabilityContract("descriptive", phase=1, available=True),
        CapabilityContract("pearson_correlation", phase=1, available=True),
        CapabilityContract("spearman_correlation", phase=1, available=True),
        CapabilityContract("ols", phase=1, available=True),
        CapabilityContract("analytic_weights", phase=1, available=True),
        CapabilityContract("absorbed_fixed_effects", phase=1, available=True),
        CapabilityContract("hc1", phase=1, available=True),
        CapabilityContract("clustered_cr1", phase=1, available=True),
        CapabilityContract("wild_cluster_bootstrap_ci", phase=2, available=False),
        CapabilityContract("lagged_association", phase=2, available=False),
        CapabilityContract("weighted_descriptive", phase=2, available=False),
    )
}


def _require_mapping(value: Any, *, context: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ResultContractError(f"{context} must be an object")
    if any(not isinstance(key, str) for key in value):
        raise ResultContractError(f"{context} keys must be strings")
    return value


def _require_exact_keys(
    value: Mapping[str, Any],
    *,
    required: Iterable[str],
    optional: Iterable[str] = (),
    context: str,
) -> None:
    required_set = set(required)
    allowed_set = required_set | set(optional)
    missing = required_set - value.keys()
    extra = value.keys() - allowed_set
    if missing:
        raise ResultContractError(f"{context} missing required fields: {sorted(missing)}")
    if extra:
        raise ResultContractError(f"{context} has unknown fields: {sorted(extra)}")


def _require_string(value: Any, *, name: str, nonempty: bool = True) -> str:
    if not isinstance(value, str) or (nonempty and not value.strip()):
        raise ResultContractError(f"{name} must be a non-empty string")
    return value


def _require_sha256(value: Any, *, name: str) -> str:
    if not isinstance(value, str) or not SHA256_PATTERN.fullmatch(value):
        raise ResultContractError(f"{name} must be a lowercase SHA-256 hex digest")
    return value


def _validate_json_value(value: Any, *, path: str = "value") -> None:
    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ResultContractError(f"{path} contains a nonfinite number")
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ResultContractError(f"{path} object keys must be strings")
            _validate_json_value(item, path=f"{path}.{key}")
        return
    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _validate_json_value(item, path=f"{path}[{index}]")
        return
    raise ResultContractError(f"{path} contains unsupported JSON type {type(value).__name__}")


def canonical_json(value: Any) -> str:
    """Return deterministic UTF-8 JSON text, rejecting NaN and infinity."""
    _validate_json_value(value)
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ResultContractError(f"value cannot be serialized as canonical JSON: {exc}") from exc


def canonical_bytes(value: Any) -> bytes:
    """Return canonical JSON encoded as UTF-8 bytes."""
    return canonical_json(value).encode("utf-8")


def component_hashes(components: Mapping[str, Any]) -> dict[str, str]:
    """Hash each named semantic component independently and deterministically."""
    components = _require_mapping(components, context="components")
    if not components:
        raise ResultContractError("components must contain at least one named component")
    return {
        name: hashlib.sha256(canonical_bytes(components[name])).hexdigest()
        for name in sorted(components)
    }


def validate_reserved_ids(identifiers: Iterable[str], *, reserved: Iterable[str]) -> None:
    """Reject duplicate identifiers and collisions with a reserved namespace."""
    ids = list(identifiers)
    if any(not isinstance(value, str) or not value.strip() for value in ids):
        raise ResultContractError("identifiers must be non-empty strings")
    duplicate_ids = sorted({value for value in ids if ids.count(value) > 1})
    if duplicate_ids:
        raise ResultContractError(f"duplicate identifiers: {duplicate_ids}")
    collisions = sorted(set(ids) & set(reserved))
    if collisions:
        raise ResultContractError(f"identifier collides with reserved id(s): {collisions}")


@dataclass(frozen=True)
class StatusFields:
    """The four independent bundle and report status fields."""

    execution_status: ExecutionStatus
    diagnostics_status: DiagnosticsStatus
    findings_status: FindingsStatus
    distribution_status: DistributionStatus

    def to_dict(self) -> dict[str, str]:
        return {
            "execution_status": self.execution_status.value,
            "diagnostics_status": self.diagnostics_status.value,
            "findings_status": self.findings_status.value,
            "distribution_status": self.distribution_status.value,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> StatusFields:
        value = _require_mapping(value, context="status fields")
        fields = {
            "execution_status",
            "diagnostics_status",
            "findings_status",
            "distribution_status",
        }
        _require_exact_keys(value, required=fields, context="status fields")
        enum_fields = (
            ("execution_status", ExecutionStatus),
            ("diagnostics_status", DiagnosticsStatus),
            ("findings_status", FindingsStatus),
            ("distribution_status", DistributionStatus),
        )
        parsed: dict[str, StrEnum] = {}
        for name, enum_type in enum_fields:
            try:
                parsed[name] = enum_type(value[name])
            except (TypeError, ValueError) as exc:
                raise ResultContractError(f"invalid {name}: {value[name]!r}") from exc
        return cls(**parsed)


@dataclass(frozen=True)
class ResultError:
    """Serializable structured error attached to a run or analysis result."""

    code: str
    message: str
    scope: str
    error_class: str | None = None
    details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_string(self.code, name="error.code")
        _require_string(self.message, name="error.message")
        if self.scope not in {"run", "panel", "analysis"}:
            raise ResultContractError("error.scope must be run, panel, or analysis")
        if self.error_class is not None:
            _require_string(self.error_class, name="error.error_class")
        _validate_json_value(self.details, path="error.details")

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "scope": self.scope,
            "error_class": self.error_class,
            "details": dict(self.details),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> ResultError:
        value = _require_mapping(value, context="error")
        _require_exact_keys(
            value,
            required={"code", "message", "scope"},
            optional={"error_class", "details"},
            context="error",
        )
        return cls(
            code=value["code"],
            message=value["message"],
            scope=value["scope"],
            error_class=value.get("error_class"),
            details=value.get("details", {}),
        )


@dataclass(frozen=True)
class AnalysisStatusRecord:
    """Status and errors for one analysis@variant result key."""

    status: AnalysisStatus
    errors: tuple[ResultError, ...] = ()

    def __post_init__(self) -> None:
        if self.status is AnalysisStatus.FAILED and not self.errors:
            raise ResultContractError("failed analysis status requires at least one error")
        if self.status is not AnalysisStatus.FAILED and self.errors:
            raise ResultContractError("only failed analysis status may carry errors")

    def to_dict(self) -> dict[str, Any]:
        return {"status": self.status.value, "errors": [error.to_dict() for error in self.errors]}

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> AnalysisStatusRecord:
        value = _require_mapping(value, context="analysis status")
        _require_exact_keys(value, required={"status", "errors"}, context="analysis status")
        try:
            status = AnalysisStatus(value["status"])
        except (TypeError, ValueError) as exc:
            raise ResultContractError(f"invalid analysis status: {value.get('status')!r}") from exc
        errors = value["errors"]
        if not isinstance(errors, Sequence) or isinstance(errors, (str, bytes)):
            raise ResultContractError("analysis status errors must be an array")
        return cls(status=status, errors=tuple(ResultError.from_dict(item) for item in errors))


@dataclass(frozen=True)
class InputIdentity:
    """Planned and observed identity for one included panel."""

    panel: str
    build_spec_hash: str
    declared_vintages: Mapping[str, str]
    content_sha256: str

    def __post_init__(self) -> None:
        _require_string(self.panel, name="input_identity.panel")
        _require_sha256(self.build_spec_hash, name="input_identity.build_spec_hash")
        _require_sha256(self.content_sha256, name="input_identity.content_sha256")
        vintages = _require_mapping(self.declared_vintages, context="declared vintages")
        for source, vintage in vintages.items():
            _require_string(source, name="declared vintage source")
            _require_string(vintage, name=f"declared vintage {source}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "panel": self.panel,
            "build_spec_hash": self.build_spec_hash,
            "declared_vintages": dict(self.declared_vintages),
            "content_sha256": self.content_sha256,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> InputIdentity:
        value = _require_mapping(value, context="input identity")
        _require_exact_keys(
            value,
            required={"panel", "build_spec_hash", "declared_vintages", "content_sha256"},
            context="input identity",
        )
        return cls(**value)


@dataclass(frozen=True)
class Tolerance:
    """Absolute and relative tolerance used by bundle verification."""

    atol: float = 1e-10
    rtol: float = 1e-9

    def __post_init__(self) -> None:
        for name, value in (("atol", self.atol), ("rtol", self.rtol)):
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ResultContractError(f"{name} must be numeric")
            if not math.isfinite(value) or value < 0:
                raise ResultContractError(f"{name} must be finite and nonnegative")

    def to_dict(self) -> dict[str, float]:
        return {"atol": float(self.atol), "rtol": float(self.rtol)}

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> Tolerance:
        value = _require_mapping(value, context="tolerance")
        _require_exact_keys(value, required={"atol", "rtol"}, context="tolerance")
        return cls(atol=value["atol"], rtol=value["rtol"])


@dataclass(frozen=True)
class LockStatus:
    """Normalized lock state, including modified component names."""

    status: LockState
    components: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.status is LockState.MODIFIED and not self.components:
            raise ResultContractError("modified lock status requires component names")
        if self.status is not LockState.MODIFIED and self.components:
            raise ResultContractError("only modified lock status may list components")
        if len(self.components) != len(set(self.components)):
            raise ResultContractError("lock status component names must be unique")

    def to_dict(self) -> dict[str, Any]:
        if self.status is not LockState.MODIFIED:
            return {"status": self.status.value}
        return {"status": self.status.value, "components": list(self.components)}

    @classmethod
    def from_value(cls, value: Any) -> LockStatus:
        if isinstance(value, str):
            try:
                return cls(status=LockState(value))
            except ValueError as exc:
                raise ResultContractError(f"invalid lock status: {value!r}") from exc
        value = _require_mapping(value, context="lock status")
        _require_exact_keys(
            value,
            required={"status"},
            optional={"components"},
            context="lock status",
        )
        try:
            status = LockState(value["status"])
        except ValueError as exc:
            raise ResultContractError(f"invalid lock status: {value.get('status')!r}") from exc
        components = value.get("components", ())
        if not isinstance(components, Sequence) or isinstance(components, (str, bytes)):
            raise ResultContractError("lock status components must be an array")
        return cls(status=status, components=tuple(components))


@dataclass(frozen=True)
class ResultsManifest:
    """Validated bundle manifest representation."""

    recipe_id: str
    run_id: str
    created_at: str
    statuses: StatusFields
    analysis_statuses: Mapping[str, AnalysisStatusRecord]
    spec_hash: str
    component_hashes: Mapping[str, str]
    lock_status: LockStatus
    input_identities: tuple[InputIdentity, ...]
    private: bool
    files: tuple[Mapping[str, Any], ...]
    bundle_format_version: str = BUNDLE_FORMAT_VERSION

    def __post_init__(self) -> None:
        if self.bundle_format_version != BUNDLE_FORMAT_VERSION:
            raise ResultContractError(
                f"unsupported bundle format version: {self.bundle_format_version!r}"
            )
        _require_string(self.recipe_id, name="recipe_id")
        _require_string(self.run_id, name="run_id")
        _validate_timestamp(self.created_at, name="created_at")
        _require_sha256(self.spec_hash, name="spec_hash")
        if not isinstance(self.private, bool):
            raise ResultContractError("private must be boolean")
        if self.private != (self.statuses.distribution_status is DistributionStatus.PRIVATE):
            raise ResultContractError("private flag and distribution_status disagree")
        for key, record in self.analysis_statuses.items():
            _require_string(key, name="analysis result key")
            if not isinstance(record, AnalysisStatusRecord):
                raise ResultContractError("analysis_statuses values must be AnalysisStatusRecord")
        for key, digest in self.component_hashes.items():
            _require_string(key, name="component hash name")
            _require_sha256(digest, name=f"component_hashes.{key}")
        for item in self.files:
            _validate_file_record(item)
        if len({identity.panel for identity in self.input_identities}) != len(
            self.input_identities
        ):
            raise ResultContractError("input identity panel names must be unique")
        failed_or_skipped = any(
            record.status in {AnalysisStatus.FAILED, AnalysisStatus.SKIPPED}
            for record in self.analysis_statuses.values()
        )
        if self.statuses.execution_status is ExecutionStatus.COMPLETE and failed_or_skipped:
            raise ResultContractError(
                "complete execution cannot contain failed or skipped analyses"
            )
        if self.statuses.execution_status is ExecutionStatus.COMPLETE and any(
            record.status is not AnalysisStatus.COMPLETE
            for record in self.analysis_statuses.values()
        ):
            raise ResultContractError(
                "complete execution requires every analysis status to be complete"
            )
        if self.statuses.execution_status is ExecutionStatus.PARTIAL and not failed_or_skipped:
            raise ResultContractError("partial execution requires a failed or skipped analysis")
        file_paths = [record["path"] for record in self.files]
        if len(file_paths) != len(set(file_paths)):
            raise ResultContractError("manifest file paths must be unique")

    def to_dict(self) -> dict[str, Any]:
        return {
            "bundle_format_version": self.bundle_format_version,
            "recipe_id": self.recipe_id,
            "run_id": self.run_id,
            "created_at": self.created_at,
            **self.statuses.to_dict(),
            "analysis_statuses": {
                key: record.to_dict() for key, record in sorted(self.analysis_statuses.items())
            },
            "spec_hash": self.spec_hash,
            "component_hashes": dict(sorted(self.component_hashes.items())),
            "lock_status": self.lock_status.to_dict(),
            "input_identities": [item.to_dict() for item in self.input_identities],
            "private": self.private,
            "files": [dict(item) for item in self.files],
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> ResultsManifest:
        value = _require_mapping(value, context="manifest")
        required = {
            "bundle_format_version",
            "recipe_id",
            "run_id",
            "created_at",
            "execution_status",
            "diagnostics_status",
            "findings_status",
            "distribution_status",
            "analysis_statuses",
            "spec_hash",
            "component_hashes",
            "lock_status",
            "input_identities",
            "private",
            "files",
        }
        _require_exact_keys(value, required=required, context="manifest")
        analysis_statuses = _require_mapping(
            value["analysis_statuses"], context="analysis_statuses"
        )
        component_hash_map = _require_mapping(value["component_hashes"], context="component_hashes")
        identities = value["input_identities"]
        files = value["files"]
        if not isinstance(identities, Sequence) or isinstance(identities, (str, bytes)):
            raise ResultContractError("input_identities must be an array")
        if not isinstance(files, Sequence) or isinstance(files, (str, bytes)):
            raise ResultContractError("files must be an array")
        return cls(
            bundle_format_version=value["bundle_format_version"],
            recipe_id=value["recipe_id"],
            run_id=value["run_id"],
            created_at=value["created_at"],
            statuses=StatusFields.from_dict(
                {
                    key: value[key]
                    for key in (
                        "execution_status",
                        "diagnostics_status",
                        "findings_status",
                        "distribution_status",
                    )
                }
            ),
            analysis_statuses={
                key: AnalysisStatusRecord.from_dict(record)
                for key, record in analysis_statuses.items()
            },
            spec_hash=value["spec_hash"],
            component_hashes=dict(component_hash_map),
            lock_status=LockStatus.from_value(value["lock_status"]),
            input_identities=tuple(InputIdentity.from_dict(item) for item in identities),
            private=value["private"],
            files=tuple(files),
        )


def _validate_timestamp(value: Any, *, name: str) -> None:
    _require_string(value, name=name)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ResultContractError(f"{name} must be RFC 3339 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise ResultContractError(f"{name} must be an RFC 3339 UTC timestamp")


def _validate_file_record(value: Mapping[str, Any]) -> None:
    value = _require_mapping(value, context="manifest file record")
    _require_exact_keys(
        value,
        required={"path", "sha256", "bytes", "role"},
        context="manifest file record",
    )
    path = _require_string(value["path"], name="file.path")
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
        raise ResultContractError("file.path must be a normalized relative bundle path")
    if path in {"manifest.json", "manifest.sha256"}:
        raise ResultContractError("manifest inventory must exclude itself and manifest.sha256")
    _require_sha256(value["sha256"], name="file.sha256")
    if (
        isinstance(value["bytes"], bool)
        or not isinstance(value["bytes"], int)
        or value["bytes"] < 0
    ):
        raise ResultContractError("file.bytes must be a nonnegative integer")
    _require_string(value["role"], name="file.role")


@dataclass(frozen=True)
class ColumnContract:
    """One named result-table column and its primitive type/null rule."""

    name: str
    value_type: str
    nullable: bool = False


@dataclass(frozen=True)
class ResultTableSchema:
    """Ordered canonical columns and validation for one result table."""

    name: str
    columns: tuple[ColumnContract, ...]

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.columns)

    def validate_rows(self, rows: Sequence[Mapping[str, Any]]) -> None:
        validate_result_rows(self.name, rows)


REGRESSION_ESTIMATE_COLUMNS = (
    "analysis",
    "variant",
    "term",
    "base_term",
    "role",
    "estimate",
    "se",
    "df",
    "ci_lo",
    "ci_hi",
    "ci_level",
    "ci_method",
    "p",
    "p_status",
    "p_reason",
    "inference_method",
)
REGRESSION_FIT_COLUMNS = (
    "analysis",
    "variant",
    "sample_source",
    "n",
    "n_clusters",
    "design_rank",
    "k_dof",
    "df_resid",
    "r2",
    "r2_status",
    "r2_reason",
    "r2_within",
    "r2_within_status",
    "r2_within_reason",
    "absorbed",
)
DESCRIPTIVE_STATS_COLUMNS = (
    "analysis",
    "variant",
    "column",
    "group",
    "stat",
    "value",
    "status",
    "reason",
)
CORRELATION_COLUMNS = (
    "analysis",
    "variant",
    "col_a",
    "col_b",
    "kind",
    "r",
    "n",
    "status",
    "reason",
)


def _columns(names: Sequence[str], *, nullable: Iterable[str] = ()) -> tuple[ColumnContract, ...]:
    nullable_names = set(nullable)
    return tuple(ColumnContract(name, _column_type(name), name in nullable_names) for name in names)


def _column_type(name: str) -> str:
    if name in {"n", "n_clusters", "design_rank", "k_dof", "df_resid"}:
        return "integer"
    if name in {
        "estimate",
        "se",
        "df",
        "ci_lo",
        "ci_hi",
        "ci_level",
        "p",
        "r2",
        "r2_within",
        "value",
        "r",
    }:
        return "number"
    if name == "absorbed":
        return "string_array"
    return "string"


RESULT_TABLE_SCHEMAS: Mapping[str, ResultTableSchema] = {
    "regression_estimates": ResultTableSchema(
        "regression_estimates",
        _columns(REGRESSION_ESTIMATE_COLUMNS, nullable={"p", "p_reason"}),
    ),
    "regression_fit": ResultTableSchema(
        "regression_fit",
        _columns(
            REGRESSION_FIT_COLUMNS,
            nullable={"n_clusters", "r2", "r2_reason", "r2_within", "r2_within_reason"},
        ),
    ),
    "descriptive_stats": ResultTableSchema(
        "descriptive_stats",
        _columns(DESCRIPTIVE_STATS_COLUMNS, nullable={"value", "reason"}),
    ),
    "correlations": ResultTableSchema(
        "correlations",
        _columns(CORRELATION_COLUMNS, nullable={"r", "reason"}),
    ),
}


def validate_result_rows(table_name: str, rows: Sequence[Mapping[str, Any]]) -> None:
    """Validate ordered fields, primitive types, finite numbers, and null states."""
    if table_name not in RESULT_TABLE_SCHEMAS:
        raise ResultContractError(f"unknown result table: {table_name!r}")
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
        raise ResultContractError(f"{table_name} rows must be an array")
    schema = RESULT_TABLE_SCHEMAS[table_name]
    for row_number, raw_row in enumerate(rows):
        row = _require_mapping(raw_row, context=f"{table_name} row {row_number}")
        _require_exact_keys(
            row,
            required=schema.column_names,
            context=f"{table_name} row {row_number}",
        )
        for column in schema.columns:
            value = row[column.name]
            if value is None:
                if not column.nullable:
                    raise ResultContractError(f"{table_name}.{column.name} cannot be null")
                continue
            _validate_column_value(value, column, table_name=table_name)
        _validate_row_status(table_name, row, row_number=row_number)


def _validate_column_value(value: Any, column: ColumnContract, *, table_name: str) -> None:
    name = f"{table_name}.{column.name}"
    if column.value_type == "string":
        _require_string(value, name=name)
        enum_values = {
            ("regression_estimates", "role"): {"predictor", "control", "intercept"},
            ("regression_estimates", "ci_method"): {"wald", "bootstrap_test_inversion"},
            ("regression_estimates", "inference_method"): {
                "classical",
                "hc1",
                "clustered",
                "wild_cluster_bootstrap",
            },
            ("correlations", "kind"): {"pearson", "spearman"},
        }.get((table_name, column.name))
        if enum_values is not None and value not in enum_values:
            raise ResultContractError(f"{name} has unsupported value {value!r}")
    elif column.value_type == "integer":
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ResultContractError(f"{name} must be a nonnegative integer")
    elif column.value_type == "number":
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ResultContractError(f"{name} must be numeric")
        if not math.isfinite(value):
            raise ResultContractError(f"{name} contains a nonfinite number")
        if column.name == "se" and value < 0:
            raise ResultContractError(f"{name} must be nonnegative")
        if column.name == "df" and value <= 0:
            raise ResultContractError(f"{name} must be positive")
        if column.name == "ci_level" and not 0 < value < 1:
            raise ResultContractError(f"{name} must be strictly between 0 and 1")
        if column.name == "p" and not 0 <= value <= 1:
            raise ResultContractError(f"{name} must be between 0 and 1")
    elif column.value_type == "string_array":
        if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
            raise ResultContractError(f"{name} must be an array of strings")
        if any(not isinstance(item, str) for item in value):
            raise ResultContractError(f"{name} must be an array of strings")


def _validate_row_status(table_name: str, row: Mapping[str, Any], *, row_number: int) -> None:
    status_columns = {
        "regression_estimates": (("p", "p_status", "p_reason"),),
        "regression_fit": (
            ("r2", "r2_status", "r2_reason"),
            ("r2_within", "r2_within_status", "r2_within_reason"),
        ),
        "descriptive_stats": (("value", "status", "reason"),),
        "correlations": (("r", "status", "reason"),),
    }.get(table_name, ())
    for value_name, status_name, reason_name in status_columns:
        value = row[value_name]
        status = row[status_name]
        reason = row[reason_name]
        if status == "not_evaluated":
            if value is not None or not isinstance(reason, str) or not reason:
                raise ResultContractError(
                    f"{table_name} row {row_number}: not_evaluated {value_name} "
                    "requires null and a reason"
                )
        elif status == "evaluated":
            if value is None or reason is not None:
                raise ResultContractError(
                    f"{table_name} row {row_number}: evaluated {value_name} "
                    "requires a value and null reason"
                )
        else:
            raise ResultContractError(
                f"{table_name} row {row_number}: invalid {status_name} {status!r}"
            )
    if table_name == "regression_estimates" and row["ci_lo"] > row["ci_hi"]:
        raise ResultContractError(f"regression_estimates row {row_number}: ci_lo must be <= ci_hi")


def aggregate_diagnostics_status(statuses: Iterable[str | DiagnosticStatus]) -> DiagnosticsStatus:
    """Return diagnostics aggregate status with the declared precedence."""
    values = [DiagnosticStatus(status) for status in statuses]
    if not values:
        return DiagnosticsStatus.NONE
    if DiagnosticStatus.FAIL in values:
        return DiagnosticsStatus.FAIL
    if DiagnosticStatus.NOT_EVALUATED in values:
        return DiagnosticsStatus.INCOMPLETE
    applicable = [value for value in values if value is not DiagnosticStatus.NOT_APPLICABLE]
    if not applicable:
        return DiagnosticsStatus.NOT_APPLICABLE
    return DiagnosticsStatus.PASS


def aggregate_findings_status(statuses: Iterable[str]) -> FindingsStatus:
    """Aggregate base and variant finding outcomes."""
    values = list(statuses)
    if any(value == FindingStatus.AMBIGUOUS_RULE.value for value in values):
        return FindingsStatus.AMBIGUOUS
    if any(value == FindingStatus.NOT_EVALUATED.value for value in values):
        return FindingsStatus.SOME_NOT_EVALUATED
    return FindingsStatus.ALL_EVALUATED


def aggregate_distribution_status(*, private: bool, withheld_count: int) -> DistributionStatus:
    """Derive the bundle distribution status from private and withheld content."""
    if (
        isinstance(withheld_count, bool)
        or not isinstance(withheld_count, int)
        or withheld_count < 0
    ):
        raise ResultContractError("withheld_count must be a nonnegative integer")
    if private:
        return DistributionStatus.PRIVATE
    return DistributionStatus.WITHHELD if withheld_count else DistributionStatus.FULL


@dataclass(frozen=True)
class DiagnosticRecord:
    """One versioned diagnostics.json row."""

    id: str
    variant: str
    status: DiagnosticStatus
    value: Any
    tolerance: float | None
    reason: str | None
    details: Mapping[str, Any]

    def __post_init__(self) -> None:
        _require_string(self.id, name="diagnostic.id")
        _require_string(self.variant, name="diagnostic.variant")
        _validate_json_value(self.value, path="diagnostic.value")
        _validate_json_value(self.details, path="diagnostic.details")
        if self.tolerance is not None and (
            isinstance(self.tolerance, bool)
            or not isinstance(self.tolerance, (int, float))
            or not math.isfinite(self.tolerance)
            or self.tolerance < 0
        ):
            raise ResultContractError("diagnostic tolerance must be finite and nonnegative")
        if self.status in {DiagnosticStatus.NOT_EVALUATED, DiagnosticStatus.NOT_APPLICABLE}:
            if self.value is not None or not self.reason:
                raise ResultContractError(
                    f"{self.status.value} diagnostic requires null value and reason"
                )
        else:
            if self.value is None:
                raise ResultContractError("evaluated diagnostic value cannot be null")
            if self.reason is not None:
                raise ResultContractError("evaluated diagnostic reason must be null")

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "variant": self.variant,
            "status": self.status.value,
            "value": self.value,
            "tolerance": self.tolerance,
            "reason": self.reason,
            "details": dict(self.details),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> DiagnosticRecord:
        value = _require_mapping(value, context="diagnostic record")
        _require_exact_keys(
            value,
            required={"id", "variant", "status", "value", "tolerance", "reason", "details"},
            context="diagnostic record",
        )
        try:
            status = DiagnosticStatus(value["status"])
        except ValueError as exc:
            raise ResultContractError(
                f"invalid diagnostic status: {value.get('status')!r}"
            ) from exc
        return cls(
            id=value["id"],
            variant=value["variant"],
            status=status,
            value=value["value"],
            tolerance=value["tolerance"],
            reason=value["reason"],
            details=value["details"],
        )


@dataclass(frozen=True)
class DiagnosticsPayload:
    bundle_format_version: str
    diagnostics: tuple[DiagnosticRecord, ...]

    def __post_init__(self) -> None:
        _validate_payload_version(self.bundle_format_version)

    def to_dict(self) -> dict[str, Any]:
        return {
            "bundle_format_version": self.bundle_format_version,
            "diagnostics": [record.to_dict() for record in self.diagnostics],
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> DiagnosticsPayload:
        value = _require_mapping(value, context="diagnostics payload")
        _require_exact_keys(
            value,
            required={"bundle_format_version", "diagnostics"},
            context="diagnostics payload",
        )
        _require_record_array(value["diagnostics"], context="diagnostics")
        return cls(
            bundle_format_version=value["bundle_format_version"],
            diagnostics=tuple(DiagnosticRecord.from_dict(item) for item in value["diagnostics"]),
        )


@dataclass(frozen=True)
class FindingRecord:
    """One finding with independently evaluated base and variant outcomes."""

    id: str
    base: Mapping[str, Any]
    variants: Mapping[str, Mapping[str, Any]]
    robustness_status: RobustnessStatus

    def __post_init__(self) -> None:
        _require_string(self.id, name="finding.id")
        _validate_finding_evaluation(self.base, context="finding.base")
        for variant, evaluation in self.variants.items():
            _require_string(variant, name="finding variant")
            _validate_finding_evaluation(evaluation, context=f"finding.variants.{variant}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "base": dict(self.base),
            "variants": {key: dict(value) for key, value in sorted(self.variants.items())},
            "robustness_status": self.robustness_status.value,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> FindingRecord:
        value = _require_mapping(value, context="finding record")
        _require_exact_keys(
            value,
            required={"id", "base", "variants", "robustness_status"},
            context="finding record",
        )
        variants = _require_mapping(value["variants"], context="finding variants")
        try:
            robustness_status = RobustnessStatus(value["robustness_status"])
        except ValueError as exc:
            raise ResultContractError("invalid robustness status") from exc
        return cls(
            id=value["id"],
            base=value["base"],
            variants=variants,
            robustness_status=robustness_status,
        )


@dataclass(frozen=True)
class FindingsPayload:
    bundle_format_version: str
    findings: tuple[FindingRecord, ...]

    def __post_init__(self) -> None:
        _validate_payload_version(self.bundle_format_version)

    def to_dict(self) -> dict[str, Any]:
        return {
            "bundle_format_version": self.bundle_format_version,
            "findings": [record.to_dict() for record in self.findings],
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> FindingsPayload:
        value = _require_mapping(value, context="findings payload")
        _require_exact_keys(
            value,
            required={"bundle_format_version", "findings"},
            context="findings payload",
        )
        _require_record_array(value["findings"], context="findings")
        return cls(
            bundle_format_version=value["bundle_format_version"],
            findings=tuple(FindingRecord.from_dict(item) for item in value["findings"]),
        )


def _validate_finding_evaluation(value: Mapping[str, Any], *, context: str) -> None:
    value = _require_mapping(value, context=context)
    _require_exact_keys(
        value,
        required={"status", "label", "compatible_with", "reason", "criteria"},
        context=context,
    )
    status = _require_string(value["status"], name=f"{context}.status")
    label = value["label"]
    compatible = value["compatible_with"]
    reason = value["reason"]
    criteria = _require_mapping(value["criteria"], context=f"{context}.criteria")
    _validate_json_value(compatible, path=f"{context}.compatible_with")
    if not isinstance(compatible, Sequence) or isinstance(compatible, (str, bytes)):
        raise ResultContractError(f"{context}.compatible_with must be an array")
    for criterion_name, criterion in criteria.items():
        _require_string(criterion_name, name=f"{context}.criteria key")
        criterion = _require_mapping(criterion, context=f"{context}.criteria.{criterion_name}")
        _require_exact_keys(
            criterion,
            required={"value", "reason"},
            context=f"{context}.criteria.{criterion_name}",
        )
        criterion_value = criterion["value"]
        criterion_reason = criterion["reason"]
        if criterion_value is None:
            if not isinstance(criterion_reason, str) or not criterion_reason:
                raise ResultContractError(
                    f"{context}.criteria.{criterion_name} null value requires a reason"
                )
        else:
            if (
                isinstance(criterion_value, bool)
                or not isinstance(criterion_value, (int, float))
                or not math.isfinite(criterion_value)
            ):
                raise ResultContractError(
                    f"{context}.criteria.{criterion_name} value must be finite numeric or null"
                )
            if criterion_reason is not None:
                raise ResultContractError(
                    f"{context}.criteria.{criterion_name} finite value requires null reason"
                )
    _validate_json_value(value, path=context)
    if status in {FindingStatus.NOT_EVALUATED.value, FindingStatus.AMBIGUOUS_RULE.value}:
        if label is not None or not isinstance(reason, str) or not reason:
            raise ResultContractError(f"{context} {status} requires null label and reason")
    elif status == FindingStatus.INCONCLUSIVE.value:
        if label != FindingStatus.INCONCLUSIVE.value or reason is not None:
            raise ResultContractError(
                f"{context} inconclusive status requires its label and null reason"
            )
    elif not label or label != status or reason is not None:
        raise ResultContractError(
            f"{context} decisive status must match its label and have null reason"
        )


def _validate_payload_version(value: Any) -> None:
    if value != BUNDLE_FORMAT_VERSION:
        raise ResultContractError(f"unsupported bundle format version: {value!r}")


def _require_record_array(value: Any, *, context: str) -> None:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ResultContractError(f"{context} must be an array")


def _validate_timestamp_field(value: Any, *, name: str = "ts") -> str:
    _validate_timestamp(value, name=name)
    return value


EVENT_REQUIRED_FIELDS: Mapping[str, frozenset[str]] = {
    "run_started": frozenset(
        {
            "run_id",
            "recipe_id",
            "recipe_sha256",
            "spec_hash",
            "component_hashes",
            "lock_status",
            "lock_id",
            "host",
            "pid",
            "process_start_time",
            "environment",
        }
    ),
    "run_finished": frozenset(
        {
            "run_id",
            "execution_status",
            "diagnostics_status",
            "findings_status",
            "distribution_status",
            "bundle_path",
            "manifest_sha256",
        }
    ),
    "run_failed": frozenset(
        {
            "run_id",
            "failure_scope",
            "error_class",
            "message",
            "failure_bundle_path",
        }
    ),
    "lock_created": frozenset(
        {
            "lock_id",
            "recipe_id",
            "recipe_sha256",
            "spec_hash",
            "component_hashes",
            "export_path",
        }
    ),
    "ledger_repair": frozenset({"byte_offset", "byte_length", "ledger_path"}),
}
EVENT_COMMON_FIELDS = frozenset({"event", "schema_version", "ts"})


def validate_ledger_event(value: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and copy one append-only run-ledger event payload."""
    value = _require_mapping(value, context="ledger event")
    event = _require_string(value.get("event"), name="event")
    if event not in EVENT_REQUIRED_FIELDS:
        raise ResultContractError(f"unknown ledger event: {event!r}")
    required_fields = EVENT_COMMON_FIELDS | EVENT_REQUIRED_FIELDS[event]
    optional_fields: set[str] = set()
    _require_exact_keys(
        value,
        required=required_fields,
        optional=optional_fields,
        context=f"{event} event",
    )
    if value["schema_version"] != LEDGER_SCHEMA_VERSION:
        raise ResultContractError(f"unsupported ledger schema version: {value['schema_version']!r}")
    _validate_timestamp_field(value["ts"])
    _validate_json_value(value, path="ledger event")
    for name in ("run_id", "recipe_id", "lock_id", "host", "error_class", "message"):
        if name in value and value[name] is not None:
            _require_string(value[name], name=name)
    if event == "run_started":
        _require_string(value["run_id"], name="run_id")
        _require_string(value["recipe_id"], name="recipe_id")
        _require_string(value["host"], name="host")
        _require_string(value["process_start_time"], name="process_start_time")
        _require_sha256(value["recipe_sha256"], name="recipe_sha256")
        _require_sha256(value["spec_hash"], name="spec_hash")
        _validate_component_hash_map(value["component_hashes"])
        LockStatus.from_value(value["lock_status"])
        if value["lock_id"] is not None:
            _require_string(value["lock_id"], name="lock_id")
        if isinstance(value["pid"], bool) or not isinstance(value["pid"], int) or value["pid"] < 0:
            raise ResultContractError("pid must be a nonnegative integer")
        _require_mapping(value["environment"], context="environment")
    elif event == "run_finished":
        _require_string(value["run_id"], name="run_id")
        statuses = StatusFields.from_dict(
            {
                name: value[name]
                for name in (
                    "execution_status",
                    "diagnostics_status",
                    "findings_status",
                    "distribution_status",
                )
            }
        )
        if statuses.execution_status is ExecutionStatus.FAILED:
            raise ResultContractError("run_finished cannot carry failed execution_status")
        _require_sha256(value["manifest_sha256"], name="manifest_sha256")
        _require_string(value["bundle_path"], name="bundle_path")
    elif event == "run_failed":
        _require_string(value["run_id"], name="run_id")
        if value["failure_scope"] not in {"run", "panel", "analysis"}:
            raise ResultContractError("failure_scope must be run, panel, or analysis")
        _require_string(value["error_class"], name="error_class")
        _require_string(value["message"], name="message")
        if value["failure_bundle_path"] is not None:
            _require_string(value["failure_bundle_path"], name="failure_bundle_path")
    elif event == "lock_created":
        _require_string(value["lock_id"], name="lock_id")
        _require_string(value["recipe_id"], name="recipe_id")
        _require_sha256(value["recipe_sha256"], name="recipe_sha256")
        _require_sha256(value["spec_hash"], name="spec_hash")
        _validate_component_hash_map(value["component_hashes"])
        if value["export_path"] is not None:
            _require_string(value["export_path"], name="export_path")
    elif event == "ledger_repair":
        for name in ("byte_offset", "byte_length"):
            if isinstance(value[name], bool) or not isinstance(value[name], int) or value[name] < 0:
                raise ResultContractError(f"{name} must be a nonnegative integer")
        _require_string(value["ledger_path"], name="ledger_path")
    return dict(value)


def _validate_component_hash_map(value: Any) -> None:
    value = _require_mapping(value, context="component_hashes")
    for name, digest in value.items():
        _require_string(name, name="component name")
        _require_sha256(digest, name=f"component_hashes.{name}")
