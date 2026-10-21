"""Boundary validation utilities for CoC GeoDataFrames.

Validates that boundary data conforms to the canonical schema and
detects anomalies in geometry and data quality.
"""

import hashlib
import json
import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import geopandas as gpd


class Severity(Enum):
    """Severity level for validation issues."""

    ERROR = "error"
    WARNING = "warning"


@dataclass
class ValidationIssue:
    """A single validation issue found in the data."""

    severity: Severity
    code: str
    message: str
    row_index: int | None = None
    column: str | None = None

    def __str__(self) -> str:
        location = ""
        if self.row_index is not None:
            location = f" (row {self.row_index})"
        if self.column:
            location = f" (column: {self.column}){location}"
        return f"[{self.severity.value.upper()}] {self.code}: {self.message}{location}"


@dataclass
class ValidationResult:
    """Result of boundary validation containing all issues found."""

    issues: list[ValidationIssue] = field(default_factory=list)

    @property
    def errors(self) -> list[ValidationIssue]:
        """Return only error-level issues."""
        return [i for i in self.issues if i.severity == Severity.ERROR]

    @property
    def warnings(self) -> list[ValidationIssue]:
        """Return only warning-level issues."""
        return [i for i in self.issues if i.severity == Severity.WARNING]

    @property
    def is_valid(self) -> bool:
        """True if no errors were found (warnings are acceptable)."""
        return len(self.errors) == 0

    def add_error(
        self,
        code: str,
        message: str,
        row_index: int | None = None,
        column: str | None = None,
    ) -> None:
        """Add an error-level issue."""
        self.issues.append(
            ValidationIssue(
                severity=Severity.ERROR,
                code=code,
                message=message,
                row_index=row_index,
                column=column,
            )
        )

    def add_warning(
        self,
        code: str,
        message: str,
        row_index: int | None = None,
        column: str | None = None,
    ) -> None:
        """Add a warning-level issue."""
        self.issues.append(
            ValidationIssue(
                severity=Severity.WARNING,
                code=code,
                message=message,
                row_index=row_index,
                column=column,
            )
        )

    def to_list(self) -> list[str]:
        """Return issues as a list of formatted strings."""
        return [str(issue) for issue in self.issues]

    def __str__(self) -> str:
        if not self.issues:
            return "Validation passed: no issues found"
        error_count = len(self.errors)
        warning_count = len(self.warnings)
        lines = [f"Validation result: {error_count} error(s), {warning_count} warning(s)"]
        for issue in self.issues:
            lines.append(f"  {issue}")
        return "\n".join(lines)


# Canonical schema: column name -> expected dtype category
REQUIRED_COLUMNS = {
    "boundary_vintage": "object",  # string
    "coc_id": "object",
    "coc_name": "object",
    "state_abbrev": "object",
    "source": "object",
    "source_ref": "object",
    "ingested_at": "datetime64",
    "geom_hash": "object",
    "geometry": "geometry",
}

# Minimum area threshold in square degrees (approx 1 sq km at equator ~ 0.0001 sq deg)
MIN_AREA_SQ_DEG = 1e-6


COC_BOUNDARY_COMPATIBILITY_SCHEMA = "coc_boundary_compatibility/1"
COC_BOUNDARY_COMPATIBILITY_STATUSES = frozenset(
    {"compatible", "mapped", "incompatible", "unknown"}
)


class BoundaryCompatibilityError(ValueError):
    """Invalid or mismatched historical CoC boundary evidence."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class BoundaryCompatibilityRecord:
    """Resolved boundary evidence for one site member, source panel, and year."""

    source_panel: str
    msa_id: str
    frozen_coc_id: str
    year: int
    frozen_boundary_vintage: str
    source_boundary_vintage: str
    status: str
    source_coc_id: str | None
    reason: str
    evidence_ref: str | None = None
    approval_ref: str | None = None


@dataclass(frozen=True)
class BoundaryCompatibilityValidation:
    """Validated compatibility rows and a fingerprint of their source artifact."""

    evidence_id: str
    artifact_id: str
    threshold: float
    membership_identity: str
    frozen_boundary_vintage: str
    evidence_fingerprint: str
    records: tuple[BoundaryCompatibilityRecord, ...]

    def for_site(self, msa_id: str, year: int) -> tuple[BoundaryCompatibilityRecord, ...]:
        """Return all panel/member compatibility rows for one site-year."""
        return tuple(
            record
            for record in self.records
            if record.msa_id == msa_id and record.year == year
        )


_COMPATIBILITY_HEADER_FIELDS = frozenset(
    {
        "schema_version",
        "evidence_id",
        "artifact_id",
        "threshold",
        "membership_identity",
        "frozen_boundary_vintage",
        "records",
    }
)
_COMPATIBILITY_RECORD_REQUIRED_FIELDS = frozenset(
    {
        "source_panel",
        "frozen_coc_id",
        "year",
        "frozen_boundary_vintage",
        "source_boundary_vintage",
        "status",
        "reason",
    }
)
_COMPATIBILITY_RECORD_OPTIONAL_FIELDS = frozenset(
    {"evidence_ref", "source_coc_id", "approval_ref"}
)


def validate_coc_boundary_compatibility(
    payload: Mapping[str, object],
    *,
    artifact_id: str,
    threshold: float,
    membership_identity: str,
    frozen_boundary_vintage: str | int,
    frozen_memberships: Iterable[tuple[str, str]],
    requested_years: Iterable[int],
    source_panel_vintages: Mapping[tuple[str, int], str | int],
) -> BoundaryCompatibilityValidation:
    """Validate CoC boundary evidence against a resolved site universe.

    ``source_panel_vintages`` records expected geography vintage independently
    of source-year data coverage. Missing evidence rows therefore resolve to
    explicit ``unknown`` records; data coverage remains the caller's concern.
    """
    _require_exact_fields(
        payload,
        _COMPATIBILITY_HEADER_FIELDS,
        label="Boundary compatibility header",
        code="invalid_header",
    )
    if payload.get("schema_version") != COC_BOUNDARY_COMPATIBILITY_SCHEMA:
        raise BoundaryCompatibilityError(
            "unknown_schema",
            "Unsupported CoC boundary evidence schema; expected "
            f"'{COC_BOUNDARY_COMPATIBILITY_SCHEMA}'.",
        )

    evidence_id = _required_text(payload.get("evidence_id"), "evidence_id")
    declared_artifact_id = _required_text(payload.get("artifact_id"), "artifact_id")
    if declared_artifact_id != artifact_id:
        raise BoundaryCompatibilityError(
            "artifact_mismatch",
            f"Boundary evidence is for artifact '{declared_artifact_id}', "
            f"but the selected site universe is '{artifact_id}'.",
        )
    declared_threshold = payload.get("threshold")
    if (
        isinstance(declared_threshold, bool)
        or not isinstance(declared_threshold, (int, float))
        or not math.isfinite(float(declared_threshold))
        or float(declared_threshold) != float(threshold)
    ):
        raise BoundaryCompatibilityError(
            "threshold_mismatch",
            "Boundary evidence threshold must exactly match the selected site-universe threshold.",
        )
    declared_membership_identity = _required_text(
        payload.get("membership_identity"), "membership_identity"
    )
    if declared_membership_identity != membership_identity:
        raise BoundaryCompatibilityError(
            "membership_mismatch",
            "Boundary evidence membership_identity does not match the resolved selected CoC set.",
        )
    frozen_vintage = _vintage_text(
        frozen_boundary_vintage, "frozen_boundary_vintage"
    )
    declared_frozen_vintage = _vintage_text(
        payload.get("frozen_boundary_vintage"), "frozen_boundary_vintage"
    )
    if declared_frozen_vintage != frozen_vintage:
        raise BoundaryCompatibilityError(
            "frozen_vintage_mismatch",
            "Boundary evidence frozen CoC vintage does not match the producer artifact.",
        )

    site_by_coc: dict[str, str] = {}
    for membership in frozen_memberships:
        if not isinstance(membership, tuple) or len(membership) != 2:
            raise BoundaryCompatibilityError(
                "invalid_membership",
                "Frozen memberships must be (msa_id, coc_id) pairs.",
            )
        msa_id = _required_text(membership[0], "frozen membership msa_id")
        coc_id = _required_text(membership[1], "frozen membership coc_id")
        previous_msa = site_by_coc.get(coc_id)
        if previous_msa is not None:
            raise BoundaryCompatibilityError(
                "shared_coc_membership",
                f"Frozen CoC '{coc_id}' is assigned to multiple sites "
                f"('{previous_msa}' and '{msa_id}').",
            )
        site_by_coc[coc_id] = msa_id
    if not site_by_coc:
        raise BoundaryCompatibilityError(
            "empty_membership",
            "Boundary compatibility cannot be evaluated for an empty selected membership.",
        )

    years = _normalized_years(requested_years)
    if not years:
        raise BoundaryCompatibilityError(
            "empty_years",
            "At least one requested year is required for boundary compatibility.",
        )
    expected_vintages = _normalized_panel_vintages(source_panel_vintages, years)
    panel_ids = sorted({panel_id for panel_id, _ in expected_vintages})
    if not panel_ids:
        raise BoundaryCompatibilityError(
            "empty_source_panels",
            "At least one source panel vintage is required for boundary compatibility.",
        )

    raw_records = payload.get("records")
    if not isinstance(raw_records, list):
        raise BoundaryCompatibilityError(
            "invalid_records",
            "Boundary evidence 'records' must be a JSON array.",
        )

    declared: dict[tuple[str, str, int], BoundaryCompatibilityRecord] = {}
    for row_number, raw in enumerate(raw_records, start=1):
        if not isinstance(raw, Mapping):
            raise BoundaryCompatibilityError(
                "invalid_record",
                f"Boundary evidence record {row_number} must be a JSON object.",
            )
        _require_allowed_fields(
            raw,
            _COMPATIBILITY_RECORD_REQUIRED_FIELDS,
            _COMPATIBILITY_RECORD_OPTIONAL_FIELDS,
            label=f"Boundary evidence record {row_number}",
            code="invalid_record_fields",
        )
        panel_id = _required_text(raw.get("source_panel"), "source_panel")
        coc_id = _required_text(raw.get("frozen_coc_id"), "frozen_coc_id")
        year = _required_year(raw.get("year"), "year")
        key = (panel_id, coc_id, year)
        if key in declared:
            raise BoundaryCompatibilityError(
                "duplicate_record",
                "Duplicate boundary evidence record for "
                f"source_panel='{panel_id}', frozen_coc_id='{coc_id}', year={year}.",
            )
        if coc_id not in site_by_coc or year not in years or panel_id not in panel_ids:
            raise BoundaryCompatibilityError(
                "record_outside_universe",
                "Boundary evidence record does not match a selected member, requested year, "
                f"or bound source panel: {key!r}.",
            )

        record_frozen_vintage = _vintage_text(
            raw.get("frozen_boundary_vintage"), "record frozen_boundary_vintage"
        )
        if record_frozen_vintage != frozen_vintage:
            raise BoundaryCompatibilityError(
                "frozen_vintage_mismatch",
                f"Boundary evidence record for CoC '{coc_id}' in {year} has frozen vintage "
                f"'{record_frozen_vintage}', expected '{frozen_vintage}'.",
            )
        expected_source_vintage = expected_vintages.get((panel_id, year))
        if expected_source_vintage is None:
            raise BoundaryCompatibilityError(
                "missing_source_vintage",
                f"No source boundary vintage is configured for panel '{panel_id}' in {year}.",
            )
        record_source_vintage = _vintage_text(
            raw.get("source_boundary_vintage"), "source_boundary_vintage"
        )
        if record_source_vintage != expected_source_vintage:
            raise BoundaryCompatibilityError(
                "source_vintage_mismatch",
                f"Boundary evidence for panel '{panel_id}' in {year} names source vintage "
                f"'{record_source_vintage}', expected '{expected_source_vintage}'.",
            )

        status = _required_text(raw.get("status"), "status")
        if status not in COC_BOUNDARY_COMPATIBILITY_STATUSES:
            raise BoundaryCompatibilityError(
                "invalid_status",
                f"Boundary evidence status '{status}' is unsupported; expected one of "
                f"{sorted(COC_BOUNDARY_COMPATIBILITY_STATUSES)}.",
            )
        reason = _required_text(raw.get("reason"), "reason")
        evidence_ref = _optional_text(raw.get("evidence_ref"), "evidence_ref")
        source_coc_id = _optional_text(raw.get("source_coc_id"), "source_coc_id")
        approval_ref = _optional_text(raw.get("approval_ref"), "approval_ref")

        if status == "compatible":
            if evidence_ref is None:
                raise BoundaryCompatibilityError(
                    "missing_evidence_reference",
                    f"Compatible record for CoC '{coc_id}' needs an evidence_ref; "
                    "a persistent ID alone is not evidence.",
                )
            if source_coc_id is not None or approval_ref is not None:
                raise BoundaryCompatibilityError(
                    "invalid_compatible_record",
                    f"Compatible record for CoC '{coc_id}' must not declare a mapped "
                    "source ID or approval reference.",
                )
            resolved_coc_id = coc_id
        elif status == "mapped":
            if evidence_ref is None or approval_ref is None or source_coc_id is None:
                raise BoundaryCompatibilityError(
                    "incomplete_mapping",
                    f"Mapped record for CoC '{coc_id}' needs source_coc_id, evidence_ref "
                    "and approval_ref.",
                )
            resolved_coc_id = source_coc_id
        else:
            if source_coc_id is not None or approval_ref is not None:
                raise BoundaryCompatibilityError(
                    "invalid_unresolved_record",
                    f"{status.title()} record for CoC '{coc_id}' cannot resolve a source "
                    "CoC ID or approval.",
                )
            if status == "incompatible" and evidence_ref is None:
                raise BoundaryCompatibilityError(
                    "missing_evidence_reference",
                    f"Incompatible record for CoC '{coc_id}' needs an evidence_ref.",
                )
            resolved_coc_id = None

        declared[key] = BoundaryCompatibilityRecord(
            source_panel=panel_id,
            msa_id=site_by_coc[coc_id],
            frozen_coc_id=coc_id,
            year=year,
            frozen_boundary_vintage=frozen_vintage,
            source_boundary_vintage=record_source_vintage,
            status=status,
            source_coc_id=resolved_coc_id,
            reason=reason,
            evidence_ref=evidence_ref,
            approval_ref=approval_ref,
        )

    resolutions_by_panel_year: dict[tuple[str, int], dict[str, str]] = {}
    for record in declared.values():
        if record.source_coc_id is None:
            continue
        panel_year = (record.source_panel, record.year)
        resolved_ids = resolutions_by_panel_year.setdefault(panel_year, {})
        previous_frozen_coc = resolved_ids.get(record.source_coc_id)
        if previous_frozen_coc is not None:
            raise BoundaryCompatibilityError(
                "non_bijective_mapping",
                f"Frozen CoCs '{previous_frozen_coc}' and '{record.frozen_coc_id}' both resolve "
                f"to source CoC '{record.source_coc_id}' for panel "
                f"'{record.source_panel}' in {record.year}.",
            )
        resolved_ids[record.source_coc_id] = record.frozen_coc_id

    resolved_records: list[BoundaryCompatibilityRecord] = []
    for panel_id in panel_ids:
        for year in years:
            source_vintage = expected_vintages[(panel_id, year)]
            for coc_id, msa_id in sorted(site_by_coc.items()):
                key = (panel_id, coc_id, year)
                row = declared.get(key)
                if row is None:
                    row = BoundaryCompatibilityRecord(
                        source_panel=panel_id,
                        msa_id=msa_id,
                        frozen_coc_id=coc_id,
                        year=year,
                        frozen_boundary_vintage=frozen_vintage,
                        source_boundary_vintage=source_vintage,
                        status="unknown",
                        source_coc_id=None,
                        reason="No boundary compatibility evidence record was supplied.",
                    )
                resolved_records.append(row)

    try:
        canonical_payload = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), allow_nan=False
        )
    except (TypeError, ValueError) as exc:
        raise BoundaryCompatibilityError(
            "invalid_json_value",
            f"Boundary evidence must contain only finite JSON values: {exc}.",
        ) from exc
    fingerprint = "sha256:" + hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest()
    return BoundaryCompatibilityValidation(
        evidence_id=evidence_id,
        artifact_id=declared_artifact_id,
        threshold=float(threshold),
        membership_identity=declared_membership_identity,
        frozen_boundary_vintage=frozen_vintage,
        evidence_fingerprint=fingerprint,
        records=tuple(resolved_records),
    )


def _require_exact_fields(
    value: Mapping[str, object],
    expected: frozenset[str],
    *,
    label: str,
    code: str,
) -> None:
    actual = set(value)
    missing = sorted(expected - actual)
    extra = sorted(actual - expected)
    if missing or extra:
        details = []
        if missing:
            details.append(f"missing fields {missing}")
        if extra:
            details.append(f"unknown fields {extra}")
        raise BoundaryCompatibilityError(code, f"{label} has {', '.join(details)}.")


def _require_allowed_fields(
    value: Mapping[str, object],
    required: frozenset[str],
    optional: frozenset[str],
    *,
    label: str,
    code: str,
) -> None:
    actual = set(value)
    missing = sorted(required - actual)
    extra = sorted(actual - required - optional)
    if missing or extra:
        details = []
        if missing:
            details.append(f"missing fields {missing}")
        if extra:
            details.append(f"unknown fields {extra}")
        raise BoundaryCompatibilityError(code, f"{label} has {', '.join(details)}.")


def _required_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise BoundaryCompatibilityError(
            "invalid_text",
            f"Boundary evidence field '{field_name}' must be a non-empty string.",
        )
    return value.strip()


def _optional_text(value: object, field_name: str) -> str | None:
    if value is None:
        return None
    return _required_text(value, field_name)


def _required_year(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise BoundaryCompatibilityError(
            "invalid_year", f"Boundary evidence field '{field_name}' must be an integer year."
        )
    return value


def _vintage_text(value: object, field_name: str) -> str:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise BoundaryCompatibilityError(
            "invalid_vintage",
            f"Boundary evidence field '{field_name}' must be a string or integer vintage.",
        )
    text = str(value).strip()
    if not text:
        raise BoundaryCompatibilityError(
            "invalid_vintage", f"Boundary evidence field '{field_name}' cannot be empty."
        )
    return text


def _normalized_years(years: Iterable[int]) -> tuple[int, ...]:
    normalized = tuple(_required_year(year, "requested year") for year in years)
    if len(set(normalized)) != len(normalized):
        raise BoundaryCompatibilityError(
            "duplicate_requested_year", "Requested years must be unique."
        )
    return tuple(sorted(normalized))


def _normalized_panel_vintages(
    values: Mapping[tuple[str, int], str | int], years: tuple[int, ...]
) -> dict[tuple[str, int], str]:
    if not isinstance(values, Mapping):
        raise BoundaryCompatibilityError(
            "invalid_source_vintages", "Source panel vintages must be a mapping."
        )
    normalized: dict[tuple[str, int], str] = {}
    for key, vintage in values.items():
        if not isinstance(key, tuple) or len(key) != 2:
            raise BoundaryCompatibilityError(
                "invalid_source_vintages",
                "Source panel vintage keys must be (source_panel, year) pairs.",
            )
        panel_id = _required_text(key[0], "source panel vintage panel ID")
        year = _required_year(key[1], "source panel vintage year")
        if year not in years:
            raise BoundaryCompatibilityError(
                "source_vintage_outside_years",
                f"Source panel vintage '{panel_id}' uses unrequested year {year}.",
            )
        normalized[(panel_id, year)] = _vintage_text(vintage, "source_boundary_vintage")
    panel_ids = {panel_id for panel_id, _ in normalized}
    missing = sorted(
        (panel_id, year)
        for panel_id in panel_ids
        for year in years
        if (panel_id, year) not in normalized
    )
    if missing:
        raise BoundaryCompatibilityError(
            "missing_source_vintage",
            "Source panel boundary vintage is required for each requested panel/year; "
            f"missing {missing}.",
        )
    return normalized


def validate_boundaries(gdf: "gpd.GeoDataFrame") -> ValidationResult:
    """Validate a GeoDataFrame against the canonical boundary schema.

    Checks performed:
    1. Required columns exist with correct types
    2. coc_id is unique within each boundary_vintage
    3. Geometries are non-empty and valid
    4. Anomaly checks (small areas, invalid coordinate ranges)

    Args:
        gdf: GeoDataFrame to validate

    Returns:
        ValidationResult containing all issues found
    """
    result = ValidationResult()

    if gdf is None or len(gdf) == 0:
        result.add_error("EMPTY_DATA", "GeoDataFrame is empty or None")
        return result

    _validate_columns(gdf, result)
    _validate_uniqueness(gdf, result)
    _validate_geometries(gdf, result)
    _validate_anomalies(gdf, result)

    return result


def _validate_columns(gdf: "gpd.GeoDataFrame", result: ValidationResult) -> None:
    """Check that required columns exist with correct types."""
    for col, expected_dtype in REQUIRED_COLUMNS.items():
        if col not in gdf.columns:
            result.add_error(
                "MISSING_COLUMN",
                f"Required column '{col}' is missing",
                column=col,
            )
            continue

        actual_dtype = str(gdf[col].dtype)

        if expected_dtype == "geometry":
            # Check it's actually the geometry column
            if col != gdf.geometry.name:
                result.add_warning(
                    "GEOMETRY_MISMATCH",
                    f"Column '{col}' exists but is not the active geometry column",
                    column=col,
                )
        elif expected_dtype == "datetime64":
            # Check for datetime types including timezone-aware variants
            is_datetime = (
                "datetime64" in actual_dtype
                or hasattr(gdf[col].dtype, "tz")  # timezone-aware datetime
            )
            if not is_datetime:
                result.add_error(
                    "WRONG_DTYPE",
                    f"Column '{col}' should be datetime, got {actual_dtype}",
                    column=col,
                )
        elif expected_dtype == "object":
            # String columns in pandas are typically 'object' or 'string'
            if actual_dtype not in ("object", "string", "str"):
                result.add_warning(
                    "UNEXPECTED_DTYPE",
                    f"Column '{col}' expected string-like, got {actual_dtype}",
                    column=col,
                )


def _validate_uniqueness(gdf: "gpd.GeoDataFrame", result: ValidationResult) -> None:
    """Validate coc_id uniqueness within each boundary_vintage."""
    if "coc_id" not in gdf.columns or "boundary_vintage" not in gdf.columns:
        return  # Already flagged as missing column

    # Group by boundary_vintage and check for duplicates
    for vintage, group in gdf.groupby("boundary_vintage"):
        duplicates = group[group["coc_id"].duplicated(keep=False)]
        if len(duplicates) > 0:
            dup_ids = duplicates["coc_id"].unique().tolist()
            result.add_error(
                "DUPLICATE_COC_ID",
                f"Duplicate coc_id values in vintage '{vintage}': {dup_ids}",
            )


def _validate_geometries(gdf: "gpd.GeoDataFrame", result: ValidationResult) -> None:
    """Validate that geometries are non-empty and valid."""
    if gdf.geometry is None:
        result.add_error("NO_GEOMETRY", "GeoDataFrame has no geometry column set")
        return

    for idx, geom in gdf.geometry.items():
        if geom is None:
            result.add_error(
                "NULL_GEOMETRY",
                "Geometry is null",
                row_index=idx,
            )
            continue

        if geom.is_empty:
            result.add_error(
                "EMPTY_GEOMETRY",
                "Geometry is empty",
                row_index=idx,
            )
            continue

        if not geom.is_valid:
            result.add_warning(
                "INVALID_GEOMETRY",
                f"Geometry is invalid: {_explain_invalid(geom)}",
                row_index=idx,
            )


def _explain_invalid(geom) -> str:
    """Get explanation for why a geometry is invalid."""
    try:
        from shapely.validation import explain_validity

        return explain_validity(geom)
    except ImportError:
        return "unknown reason"


def _validate_anomalies(gdf: "gpd.GeoDataFrame", result: ValidationResult) -> None:
    """Check for anomalies like tiny polygons or invalid coordinate ranges."""
    if gdf.geometry is None:
        return

    is_geographic = gdf.crs is not None and gdf.crs.is_geographic

    for idx, geom in gdf.geometry.items():
        if geom is None or geom.is_empty:
            continue  # Already flagged

        # Check for tiny polygons (only meaningful for geographic CRS)
        if is_geographic:
            try:
                area = geom.area
                if area < MIN_AREA_SQ_DEG:
                    result.add_warning(
                        "SMALL_AREA",
                        f"Polygon area ({area:.2e} sq deg) is below threshold",
                        row_index=idx,
                    )
            except Exception:
                pass  # Area calculation failed, skip this check

        # Check bounding box for valid coordinate ranges (EPSG:4326 only)
        if is_geographic:
            try:
                minx, miny, maxx, maxy = geom.bounds
                if minx < -180 or maxx > 180:
                    result.add_error(
                        "INVALID_LONGITUDE",
                        f"Longitude out of range: [{minx:.4f}, {maxx:.4f}]",
                        row_index=idx,
                    )
                if miny < -90 or maxy > 90:
                    result.add_error(
                        "INVALID_LATITUDE",
                        f"Latitude out of range: [{miny:.4f}, {maxy:.4f}]",
                        row_index=idx,
                    )
            except Exception:
                pass  # Bounds calculation failed, skip this check
