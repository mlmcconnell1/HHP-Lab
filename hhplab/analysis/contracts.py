"""Stable analysis result and error contracts."""

from __future__ import annotations

import math
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import numpy as np
import pandas as pd

from hhplab.schema.results import AnalysisStatus, ResultContractError


class AnalysisError(ValueError):
    """Raised when an analysis request is invalid for the input panel."""


type PanelKeyPart = str | int
type PanelRowKey = tuple[PanelKeyPart, PanelKeyPart]


@dataclass(frozen=True)
class AnalysisErrorRecord:
    """Serializable analysis failure associated with a resolved result key."""

    code: str
    message: str
    scope: Literal["panel", "analysis"] = "analysis"
    details: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        if not self.code.strip() or not self.message.strip():
            raise ValueError("analysis error code and message must be non-empty")
        if self.scope not in {"panel", "analysis"}:
            raise ValueError("analysis error scope must be panel or analysis")
        if self.details is not None:
            try:
                json_safe_contract_value(self.details)
            except ResultContractError as exc:
                raise ValueError(f"analysis error details are not JSON-safe: {exc}") from exc

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "scope": self.scope,
            "details": {} if self.details is None else dict(self.details),
        }


def json_safe_contract_value(value: Any) -> Any:
    """Validate that a value is representable in strict result-contract JSON."""
    from hhplab.schema.results import canonical_json

    canonical_json(value)
    return value


@dataclass(frozen=True)
class ResolvedSampleFrame:
    """Resolved panel rows with exact, ordered identities and key columns.

    Row keys retain their canonical scalar types, so a year remains an integer
    and a geography identifier remains a string or integer as supplied by the
    panel. The frame must already be in the same order as ``row_keys``.
    """

    frame: pd.DataFrame
    row_keys: tuple[PanelRowKey, ...]
    geo_key: str
    time_key: str

    def __post_init__(self) -> None:
        if not isinstance(self.frame, pd.DataFrame):
            raise TypeError("resolved sample frame must be a pandas DataFrame")
        if not self.geo_key or not self.time_key or self.geo_key == self.time_key:
            raise ValueError("geo_key and time_key must be distinct non-empty columns")
        missing = {self.geo_key, self.time_key} - set(self.frame.columns)
        if missing:
            raise ValueError(f"resolved sample frame is missing key column(s): {sorted(missing)}")
        normalized_keys = tuple(_normalize_panel_key(key) for key in self.row_keys)
        object.__setattr__(self, "row_keys", normalized_keys)
        if len(normalized_keys) != len(self.frame):
            raise ValueError("resolved sample frame row_keys must match its row count")
        _require_unique_panel_keys(normalized_keys, context="resolved sample")
        self.validate_frame_identity()

    def validate_frame_identity(self) -> None:
        """Recheck mutable frame keys against the identities captured at resolution."""
        missing = {self.geo_key, self.time_key} - set(self.frame.columns)
        if missing:
            raise ValueError(f"resolved sample frame is missing key column(s): {sorted(missing)}")
        expected_keys = tuple(
            (
                _normalize_key_part(geo, name=self.geo_key),
                _normalize_key_part(year, name=self.time_key),
            )
            for geo, year in zip(self.frame[self.geo_key], self.frame[self.time_key], strict=True)
        )
        if expected_keys != self.row_keys:
            raise ValueError("row_keys must exactly match the ordered frame key columns")

    def validate_result_keys(self, row_keys: tuple[PanelRowKey, ...]) -> None:
        """Require an estimator to consume every included key exactly once."""
        self.validate_frame_identity()
        candidate_keys = tuple(_normalize_panel_key(key) for key in row_keys)
        _require_unique_panel_keys(candidate_keys, context="estimator result")
        expected_counts = Counter(_typed_panel_key(key) for key in self.row_keys)
        observed_counts = Counter(_typed_panel_key(key) for key in candidate_keys)
        if observed_counts != expected_counts:
            missing = [key for key in self.row_keys if _typed_panel_key(key) not in observed_counts]
            extra = [key for key in candidate_keys if _typed_panel_key(key) not in expected_counts]
            raise ValueError(
                "estimator row keys must equal included sample keys exactly; "
                f"missing={missing!r}, extra={extra!r}"
            )


def _normalize_key_part(value: Any, *, name: str) -> PanelKeyPart:
    if isinstance(value, np.generic):
        value = value.item()
    if value is None or value is pd.NA or (isinstance(value, float) and math.isnan(value)):
        raise ValueError(f"panel key {name!r} cannot be null or NaN")
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ValueError(f"panel key {name!r} must be a string or integer")
    return value


def _normalize_panel_key(value: Any) -> PanelRowKey:
    if not isinstance(value, tuple) or len(value) != 2:
        raise ValueError("panel row keys must be two-item tuples")
    return (
        _normalize_key_part(value[0], name="geography"),
        _normalize_key_part(value[1], name="time"),
    )


def _typed_panel_key(key: PanelRowKey) -> tuple[tuple[type, PanelKeyPart], ...]:
    return ((type(key[0]), key[0]), (type(key[1]), key[1]))


def _require_unique_panel_keys(keys: tuple[PanelRowKey, ...], *, context: str) -> None:
    typed_keys = [_typed_panel_key(key) for key in keys]
    duplicates = [key for key, count in Counter(typed_keys).items() if count > 1]
    if duplicates:
        raise ValueError(f"{context} contains duplicate panel row keys: {duplicates!r}")


@dataclass(frozen=True)
class AnalysisExecutionResult:
    """Per-analysis execution status and exact resolved-sample consumption."""

    status: AnalysisStatus
    consumed_row_keys: tuple[PanelRowKey, ...] = ()
    error: AnalysisErrorRecord | None = None

    def __post_init__(self) -> None:
        try:
            status = AnalysisStatus(self.status)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"invalid analysis status: {self.status!r}") from exc
        object.__setattr__(self, "status", status)
        if self.error is not None and not isinstance(self.error, AnalysisErrorRecord):
            raise TypeError("analysis execution error must be an AnalysisErrorRecord")
        if self.status is AnalysisStatus.FAILED and self.error is None:
            raise ValueError("failed analysis result requires an error")
        if self.status is AnalysisStatus.COMPLETE and self.error is not None:
            raise ValueError("complete analysis result cannot carry an error")
        if (
            self.status in {AnalysisStatus.PENDING, AnalysisStatus.SKIPPED}
            and self.consumed_row_keys
        ):
            raise ValueError(f"{self.status.value} analysis result cannot claim consumed rows")

    def validate_sample(self, sample: ResolvedSampleFrame) -> None:
        if self.status is AnalysisStatus.COMPLETE:
            sample.validate_result_keys(self.consumed_row_keys)


@dataclass(frozen=True)
class ResolvedRegressionResult:
    """Schema-ready regression tables and evidence for one resolved sample."""

    regression_estimates: tuple[dict[str, Any], ...]
    regression_fit: dict[str, Any]
    execution: AnalysisExecutionResult
    warnings: tuple[str, ...]
    design_evidence: dict[str, Any]

    def __post_init__(self) -> None:
        from hhplab.schema.results import validate_result_rows

        validate_result_rows("regression_estimates", self.regression_estimates)
        validate_result_rows("regression_fit", [self.regression_fit])
        if self.execution.status is not AnalysisStatus.COMPLETE:
            raise ValueError("resolved regression result requires complete execution status")
        if not isinstance(self.design_evidence, dict):
            raise TypeError("regression design evidence must be a mapping")
        json_safe_contract_value(self.design_evidence)
        if any(not isinstance(warning, str) or not warning for warning in self.warnings):
            raise ValueError("regression warnings must be non-empty strings")

    def to_payload(self) -> dict[str, Any]:
        return _json_safe(
            {
                "status": self.execution.status.value,
                "regression_estimates": self.regression_estimates,
                "regression_fit": self.regression_fit,
                "warnings": self.warnings,
                "design_evidence": self.design_evidence,
            }
        )


@dataclass(frozen=True)
class AnalysisResult:
    """A persisted analysis result plus JSON-ready summary metadata."""

    table: pd.DataFrame
    output_path: Path
    manifest_path: Path
    metadata: dict[str, Any]

    def to_payload(self) -> dict[str, Any]:
        return _json_safe(
            {
                "status": "ok",
                "output_path": str(self.output_path),
                "manifest_path": str(self.manifest_path),
                **self.metadata,
                "records": self.table.to_dict(orient="records"),
            }
        )


InferenceMethod = Literal["none", "wild-cluster", "permutation"]


@dataclass(frozen=True)
class _RegressionFit:
    beta: np.ndarray
    fitted: np.ndarray
    residuals: np.ndarray
    std_errors: np.ndarray
    std_error_type: str
    t_stats: np.ndarray
    p_values: np.ndarray
    r_squared: float


def _json_safe(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if hasattr(value, "item"):
        return _json_safe(value.item())
    if pd.isna(value) and not isinstance(value, (list, tuple, dict)):
        return None
    return value
