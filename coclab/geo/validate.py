"""Boundary validation utilities for CoC GeoDataFrames.

Validates that boundary data conforms to the canonical schema and
detects anomalies in geometry and data quality.
"""

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

    for idx, geom in gdf.geometry.items():
        if geom is None or geom.is_empty:
            continue  # Already flagged

        # Check for tiny polygons
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

        # Check bounding box for valid coordinate ranges (EPSG:4326)
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
