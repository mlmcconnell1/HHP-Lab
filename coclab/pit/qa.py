"""PIT data quality assurance and validation.

This module provides QA checks for PIT (Point-in-Time) count data to detect
quality issues before they propagate to panels.

QA Checks
---------
1. Duplicate CoC IDs per year - Flag if same CoC appears multiple times
2. Missing CoCs relative to boundary vintages - Compare PIT CoCs against known boundaries
3. Non-integer or negative counts - Validate pit_total, pit_sheltered, pit_unsheltered
4. Extreme year-over-year changes - Flag large changes (e.g., >50%)

Design Philosophy
-----------------
QA functions FLAG issues but do not auto-correct them. All issues are returned
as structured QAIssue objects that can be consumed by CLI or other tools.

Implementation Notes
--------------------
This module is part of WP-3D: PIT QA & Validation for Phase 3.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    import geopandas as gpd

logger = logging.getLogger(__name__)


class Severity(Enum):
    """Severity level for QA issues."""

    ERROR = "error"
    WARNING = "warning"


@dataclass
class QAIssue:
    """A single QA issue found in PIT data.

    Attributes
    ----------
    severity : Severity
        Issue severity (error or warning).
    check_name : str
        Name of the check that found this issue.
    coc_id : str or None
        CoC ID associated with this issue, if applicable.
    year : int or None
        PIT year associated with this issue, if applicable.
    message : str
        Human-readable description of the issue.
    details : dict or None
        Additional structured data about the issue.
    """

    severity: Severity
    check_name: str
    coc_id: str | None
    year: int | None
    message: str
    details: dict | None = None

    def __str__(self) -> str:
        location_parts = []
        if self.coc_id:
            location_parts.append(f"CoC: {self.coc_id}")
        if self.year:
            location_parts.append(f"Year: {self.year}")
        location = f" ({', '.join(location_parts)})" if location_parts else ""
        return f"[{self.severity.value.upper()}] {self.check_name}: {self.message}{location}"

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "severity": self.severity.value,
            "check_name": self.check_name,
            "coc_id": self.coc_id,
            "year": self.year,
            "message": self.message,
            "details": self.details,
        }


@dataclass
class QAReport:
    """Report containing all QA issues found in PIT data.

    Attributes
    ----------
    issues : list[QAIssue]
        All issues found during QA checks.
    summary : dict
        Summary counts by severity and check type.
    passed : bool
        True if no errors were found (warnings are acceptable).
    """

    issues: list[QAIssue] = field(default_factory=list)

    @property
    def errors(self) -> list[QAIssue]:
        """Return only error-level issues."""
        return [i for i in self.issues if i.severity == Severity.ERROR]

    @property
    def warnings(self) -> list[QAIssue]:
        """Return only warning-level issues."""
        return [i for i in self.issues if i.severity == Severity.WARNING]

    @property
    def passed(self) -> bool:
        """True if no errors were found (warnings are acceptable)."""
        return len(self.errors) == 0

    @property
    def summary(self) -> dict:
        """Summary counts by severity and check type."""
        by_severity = {
            "error": len(self.errors),
            "warning": len(self.warnings),
            "total": len(self.issues),
        }
        by_check: dict[str, int] = {}
        for issue in self.issues:
            by_check[issue.check_name] = by_check.get(issue.check_name, 0) + 1
        return {
            "by_severity": by_severity,
            "by_check": by_check,
        }

    def add_issue(self, issue: QAIssue) -> None:
        """Add an issue to the report."""
        self.issues.append(issue)

    def add_error(
        self,
        check_name: str,
        message: str,
        coc_id: str | None = None,
        year: int | None = None,
        details: dict | None = None,
    ) -> None:
        """Add an error-level issue."""
        self.issues.append(
            QAIssue(
                severity=Severity.ERROR,
                check_name=check_name,
                coc_id=coc_id,
                year=year,
                message=message,
                details=details,
            )
        )

    def add_warning(
        self,
        check_name: str,
        message: str,
        coc_id: str | None = None,
        year: int | None = None,
        details: dict | None = None,
    ) -> None:
        """Add a warning-level issue."""
        self.issues.append(
            QAIssue(
                severity=Severity.WARNING,
                check_name=check_name,
                coc_id=coc_id,
                year=year,
                message=message,
                details=details,
            )
        )

    def extend(self, issues: list[QAIssue]) -> None:
        """Extend the report with additional issues."""
        self.issues.extend(issues)

    def to_list(self) -> list[str]:
        """Return issues as a list of formatted strings."""
        return [str(issue) for issue in self.issues]

    def to_dataframe(self) -> pd.DataFrame:
        """Convert issues to a DataFrame for analysis."""
        if not self.issues:
            return pd.DataFrame(columns=["severity", "check_name", "coc_id", "year", "message"])
        return pd.DataFrame([issue.to_dict() for issue in self.issues])

    def __str__(self) -> str:
        if not self.issues:
            return "QA passed: no issues found"
        error_count = len(self.errors)
        warning_count = len(self.warnings)
        lines = [f"QA result: {error_count} error(s), {warning_count} warning(s)"]
        for issue in self.issues:
            lines.append(f"  {issue}")
        return "\n".join(lines)


def check_duplicates(df: pd.DataFrame) -> list[QAIssue]:
    """Check for duplicate CoC IDs within the same year.

    Parameters
    ----------
    df : pd.DataFrame
        PIT data with columns: pit_year, coc_id

    Returns
    -------
    list[QAIssue]
        List of issues for duplicate CoC IDs.
    """
    issues: list[QAIssue] = []

    if df is None or df.empty:
        return issues

    required_cols = {"pit_year", "coc_id"}
    if not required_cols.issubset(df.columns):
        issues.append(
            QAIssue(
                severity=Severity.ERROR,
                check_name="duplicates",
                coc_id=None,
                year=None,
                message=f"Missing required columns for duplicate check: {required_cols - set(df.columns)}",
            )
        )
        return issues

    # Group by year and find duplicates
    for year, group in df.groupby("pit_year"):
        duplicates = group[group["coc_id"].duplicated(keep=False)]
        if len(duplicates) > 0:
            dup_ids = duplicates["coc_id"].unique().tolist()
            for coc_id in dup_ids:
                count = len(duplicates[duplicates["coc_id"] == coc_id])
                issues.append(
                    QAIssue(
                        severity=Severity.ERROR,
                        check_name="duplicates",
                        coc_id=coc_id,
                        year=int(year),
                        message=f"CoC ID appears {count} times in year {year}",
                        details={"occurrence_count": count},
                    )
                )

    return issues


def check_missing_cocs(
    df: pd.DataFrame,
    boundary_vintage: str,
    boundary_gdf: "gpd.GeoDataFrame | None" = None,
    data_dir: Path | str | None = None,
) -> list[QAIssue]:
    """Check for CoCs that exist in boundaries but are missing from PIT data.

    Parameters
    ----------
    df : pd.DataFrame
        PIT data with columns: pit_year, coc_id
    boundary_vintage : str
        Boundary vintage to compare against (e.g., "2024").
    boundary_gdf : gpd.GeoDataFrame, optional
        Pre-loaded boundary GeoDataFrame. If not provided, will attempt
        to load from the standard curated path.
    data_dir : Path or str, optional
        Base data directory for loading boundaries (default: "data").

    Returns
    -------
    list[QAIssue]
        List of issues for missing CoCs.
    """
    issues: list[QAIssue] = []

    if df is None or df.empty:
        return issues

    if "coc_id" not in df.columns:
        issues.append(
            QAIssue(
                severity=Severity.ERROR,
                check_name="missing_cocs",
                coc_id=None,
                year=None,
                message="Missing required column 'coc_id'",
            )
        )
        return issues

    # Get CoC IDs from boundaries
    if boundary_gdf is not None:
        if "coc_id" not in boundary_gdf.columns:
            issues.append(
                QAIssue(
                    severity=Severity.ERROR,
                    check_name="missing_cocs",
                    coc_id=None,
                    year=None,
                    message="Boundary GeoDataFrame missing 'coc_id' column",
                )
            )
            return issues
        boundary_cocs = set(boundary_gdf["coc_id"].unique())
    else:
        # Try to load from curated path
        try:
            from coclab.geo.io import curated_boundary_path, read_geoparquet

            if data_dir is None:
                data_dir = Path("data")
            boundary_path = curated_boundary_path(boundary_vintage, base_dir=data_dir)
            gdf = read_geoparquet(boundary_path)
            boundary_cocs = set(gdf["coc_id"].unique())
        except FileNotFoundError:
            issues.append(
                QAIssue(
                    severity=Severity.WARNING,
                    check_name="missing_cocs",
                    coc_id=None,
                    year=None,
                    message=f"Boundary file not found for vintage '{boundary_vintage}'",
                    details={"boundary_vintage": boundary_vintage},
                )
            )
            return issues
        except Exception as e:
            issues.append(
                QAIssue(
                    severity=Severity.WARNING,
                    check_name="missing_cocs",
                    coc_id=None,
                    year=None,
                    message=f"Could not load boundaries: {e}",
                    details={"boundary_vintage": boundary_vintage, "error": str(e)},
                )
            )
            return issues

    # Find PIT year(s) to report
    pit_year = None
    if "pit_year" in df.columns:
        years = df["pit_year"].unique()
        if len(years) == 1:
            pit_year = int(years[0])

    # Compare CoC sets
    pit_cocs = set(df["coc_id"].unique())
    missing_from_pit = boundary_cocs - pit_cocs
    extra_in_pit = pit_cocs - boundary_cocs

    # Report CoCs in boundaries but missing from PIT data
    for coc_id in sorted(missing_from_pit):
        issues.append(
            QAIssue(
                severity=Severity.WARNING,
                check_name="missing_cocs",
                coc_id=coc_id,
                year=pit_year,
                message=f"CoC exists in boundary vintage '{boundary_vintage}' but missing from PIT data",
                details={"boundary_vintage": boundary_vintage},
            )
        )

    # Report CoCs in PIT but not in boundaries (could indicate stale CoC IDs)
    for coc_id in sorted(extra_in_pit):
        issues.append(
            QAIssue(
                severity=Severity.WARNING,
                check_name="missing_cocs",
                coc_id=coc_id,
                year=pit_year,
                message=f"CoC in PIT data but not found in boundary vintage '{boundary_vintage}'",
                details={"boundary_vintage": boundary_vintage},
            )
        )

    return issues


def check_invalid_counts(df: pd.DataFrame) -> list[QAIssue]:
    """Check for non-integer or negative count values.

    Validates pit_total, pit_sheltered, and pit_unsheltered columns.

    Parameters
    ----------
    df : pd.DataFrame
        PIT data with count columns.

    Returns
    -------
    list[QAIssue]
        List of issues for invalid count values.
    """
    issues: list[QAIssue] = []

    if df is None or df.empty:
        return issues

    count_columns = ["pit_total", "pit_sheltered", "pit_unsheltered"]
    present_columns = [col for col in count_columns if col in df.columns]

    if not present_columns:
        issues.append(
            QAIssue(
                severity=Severity.WARNING,
                check_name="invalid_counts",
                coc_id=None,
                year=None,
                message="No count columns found to validate",
            )
        )
        return issues

    for _, row in df.iterrows():
        coc_id = row.get("coc_id")
        year = row.get("pit_year")
        if pd.notna(year):
            year = int(year)
        else:
            year = None

        for col in present_columns:
            value = row.get(col)

            # Skip null values for optional columns (sheltered/unsheltered)
            if pd.isna(value):
                if col == "pit_total":
                    issues.append(
                        QAIssue(
                            severity=Severity.ERROR,
                            check_name="invalid_counts",
                            coc_id=coc_id,
                            year=year,
                            message=f"Required column '{col}' is null",
                            details={"column": col},
                        )
                    )
                continue

            # Check if value is numeric
            try:
                numeric_value = float(value)
            except (TypeError, ValueError):
                issues.append(
                    QAIssue(
                        severity=Severity.ERROR,
                        check_name="invalid_counts",
                        coc_id=coc_id,
                        year=year,
                        message=f"Non-numeric value in '{col}': {value!r}",
                        details={"column": col, "value": str(value)},
                    )
                )
                continue

            # Check for negative values
            if numeric_value < 0:
                issues.append(
                    QAIssue(
                        severity=Severity.ERROR,
                        check_name="invalid_counts",
                        coc_id=coc_id,
                        year=year,
                        message=f"Negative value in '{col}': {numeric_value}",
                        details={"column": col, "value": numeric_value},
                    )
                )

            # Check for non-integer values (warning, since counts should be whole numbers)
            if numeric_value != int(numeric_value):
                issues.append(
                    QAIssue(
                        severity=Severity.WARNING,
                        check_name="invalid_counts",
                        coc_id=coc_id,
                        year=year,
                        message=f"Non-integer value in '{col}': {numeric_value}",
                        details={"column": col, "value": numeric_value},
                    )
                )

    return issues


def check_yoy_changes(
    df_current: pd.DataFrame,
    df_previous: pd.DataFrame,
    threshold: float = 0.5,
) -> list[QAIssue]:
    """Check for extreme year-over-year changes in PIT counts.

    Flags CoCs where pit_total changed by more than the threshold percentage.
    This is a warning-level check since large changes may be legitimate.

    Parameters
    ----------
    df_current : pd.DataFrame
        Current year PIT data with columns: coc_id, pit_year, pit_total
    df_previous : pd.DataFrame
        Previous year PIT data with columns: coc_id, pit_year, pit_total
    threshold : float, optional
        Percentage threshold for flagging changes (default: 0.5 = 50%).
        A value of 0.5 means changes of +50% or -50% or more are flagged.

    Returns
    -------
    list[QAIssue]
        List of issues for extreme year-over-year changes.
    """
    issues: list[QAIssue] = []

    if df_current is None or df_current.empty:
        return issues

    if df_previous is None or df_previous.empty:
        issues.append(
            QAIssue(
                severity=Severity.WARNING,
                check_name="yoy_changes",
                coc_id=None,
                year=None,
                message="No previous year data for year-over-year comparison",
            )
        )
        return issues

    required_cols = {"coc_id", "pit_total"}
    for name, df in [("current", df_current), ("previous", df_previous)]:
        if not required_cols.issubset(df.columns):
            issues.append(
                QAIssue(
                    severity=Severity.ERROR,
                    check_name="yoy_changes",
                    coc_id=None,
                    year=None,
                    message=f"Missing required columns in {name} data: {required_cols - set(df.columns)}",
                )
            )
            return issues

    # Get years for reporting
    current_year = None
    if "pit_year" in df_current.columns:
        years = df_current["pit_year"].unique()
        if len(years) == 1:
            current_year = int(years[0])

    previous_year = None
    if "pit_year" in df_previous.columns:
        years = df_previous["pit_year"].unique()
        if len(years) == 1:
            previous_year = int(years[0])

    # Create lookup for previous year data
    prev_lookup = df_previous.set_index("coc_id")["pit_total"].to_dict()

    # Check each CoC in current year
    for _, row in df_current.iterrows():
        coc_id = row["coc_id"]
        current_total = row["pit_total"]

        if pd.isna(current_total):
            continue

        if coc_id not in prev_lookup:
            # New CoC, not necessarily an issue
            continue

        prev_total = prev_lookup[coc_id]
        if pd.isna(prev_total):
            continue

        # Calculate percentage change
        if prev_total == 0:
            if current_total > 0:
                # Infinite increase from zero
                issues.append(
                    QAIssue(
                        severity=Severity.WARNING,
                        check_name="yoy_changes",
                        coc_id=coc_id,
                        year=current_year,
                        message=f"Count went from 0 to {current_total} (infinite change)",
                        details={
                            "previous_total": int(prev_total),
                            "current_total": int(current_total),
                            "previous_year": previous_year,
                            "current_year": current_year,
                            "percent_change": None,
                        },
                    )
                )
            continue

        percent_change = (current_total - prev_total) / prev_total

        if abs(percent_change) >= threshold:
            change_pct = percent_change * 100
            direction = "increase" if percent_change > 0 else "decrease"
            issues.append(
                QAIssue(
                    severity=Severity.WARNING,
                    check_name="yoy_changes",
                    coc_id=coc_id,
                    year=current_year,
                    message=f"{abs(change_pct):.1f}% {direction} from {prev_total} to {current_total}",
                    details={
                        "previous_total": int(prev_total),
                        "current_total": int(current_total),
                        "previous_year": previous_year,
                        "current_year": current_year,
                        "percent_change": round(percent_change, 4),
                    },
                )
            )

    return issues


def validate_pit_data(
    df: pd.DataFrame,
    *,
    df_previous: pd.DataFrame | None = None,
    boundary_vintage: str | None = None,
    boundary_gdf: "gpd.GeoDataFrame | None" = None,
    data_dir: Path | str | None = None,
    yoy_threshold: float = 0.5,
) -> QAReport:
    """Run all QA checks on parsed PIT data.

    Parameters
    ----------
    df : pd.DataFrame
        Parsed PIT data in canonical schema.
    df_previous : pd.DataFrame, optional
        Previous year's PIT data for year-over-year comparison.
    boundary_vintage : str, optional
        Boundary vintage to check for missing CoCs.
    boundary_gdf : gpd.GeoDataFrame, optional
        Pre-loaded boundary GeoDataFrame for missing CoC check.
    data_dir : Path or str, optional
        Base data directory for loading boundaries.
    yoy_threshold : float, optional
        Threshold for year-over-year change warnings (default: 0.5 = 50%).

    Returns
    -------
    QAReport
        Report containing all issues found and summary statistics.
    """
    report = QAReport()

    if df is None or df.empty:
        report.add_error(
            check_name="data_quality",
            message="Input DataFrame is empty or None",
        )
        return report

    # Run all checks
    logger.debug("Running duplicate check...")
    report.extend(check_duplicates(df))

    logger.debug("Running invalid counts check...")
    report.extend(check_invalid_counts(df))

    if boundary_vintage is not None:
        logger.debug(f"Running missing CoCs check against vintage '{boundary_vintage}'...")
        report.extend(
            check_missing_cocs(
                df,
                boundary_vintage=boundary_vintage,
                boundary_gdf=boundary_gdf,
                data_dir=data_dir,
            )
        )

    if df_previous is not None:
        logger.debug("Running year-over-year change check...")
        report.extend(
            check_yoy_changes(df, df_previous, threshold=yoy_threshold)
        )

    logger.info(
        f"QA complete: {len(report.errors)} error(s), {len(report.warnings)} warning(s)"
    )
    return report
