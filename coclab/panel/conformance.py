"""Post-build conformance report infrastructure for CoC Lab panels.

This module provides the framework for running conformance checks on
assembled panel DataFrames. Individual checks are registered via the
``register_check`` decorator and executed by ``run_conformance``.

Data completeness checks (coclab-1gmj) are registered in this module:
- ``check_column_null_rates``: Per-column null rate warnings.
- ``check_per_year_completeness``: Per-year null rate warnings.
- ``check_zori_eligibility_rate``: ZORI eligibility rate warning.

Other checks (coclab-2jtk, coclab-2o8i, coclab-1d2j) are defined in
dependent modules and registered at import time.

Usage
-----
    from coclab.panel.conformance import run_conformance, PanelRequest

    request = PanelRequest(start_year=2020, end_year=2024)
    report = run_conformance(panel_df, request)

    if not report.passed:
        print(report.summary())

    # Machine-readable output for --json
    data = report.to_dict()
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Literal

import pandas as pd

# ---------------------------------------------------------------------------
# Column constants
# ---------------------------------------------------------------------------

#: ACS measure columns checked for data completeness.
ACS_MEASURE_COLUMNS: list[str] = [
    "total_population",
    "adult_population",
    "population_below_poverty",
    "median_household_income",
    "median_gross_rent",
]

#: Minimum ZORI eligibility rate before a warning is emitted.
ZORI_MIN_ELIGIBILITY_RATE: float = 0.20

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class PanelRequest:
    """Captures what was asked for in a panel build.

    Attributes
    ----------
    start_year : int
        First PIT year in the requested panel range.
    end_year : int
        Last PIT year in the requested panel range.
    include_zori : bool
        Whether ZORI rent data was requested.
    weighting_method : Literal["area", "population"]
        ACS-to-CoC apportionment method.
    zori_min_coverage : float
        Minimum coverage ratio for ZORI eligibility.
    expected_coc_count : int | None
        Optional hint for how many CoCs should appear.
    null_rate_threshold : float
        Configurable threshold for data completeness checks.
    """

    start_year: int
    end_year: int
    include_zori: bool = False
    weighting_method: Literal["area", "population"] = "population"
    zori_min_coverage: float = 0.90
    expected_coc_count: int | None = None
    null_rate_threshold: float = 0.50


@dataclass
class ConformanceResult:
    """A single conformance check result.

    Attributes
    ----------
    check_name : str
        Machine-readable check identifier (e.g., ``"year_coverage"``).
    severity : Literal["error", "warning"]
        How serious the finding is.
    message : str
        Human-readable description of the finding.
    details : dict[str, Any]
        Structured details suitable for JSON serialisation.
    """

    check_name: str
    severity: Literal["error", "warning"]
    message: str
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialise to a plain dictionary.

        Returns
        -------
        dict[str, Any]
            Dictionary with all fields.
        """
        return {
            "check_name": self.check_name,
            "severity": self.severity,
            "message": self.message,
            "details": self.details,
        }


class ConformanceReport:
    """Collects conformance check results and provides output helpers.

    Attributes
    ----------
    results : list[ConformanceResult]
        All collected conformance results (errors and warnings).
    """

    def __init__(self, results: list[ConformanceResult] | None = None) -> None:
        self.results: list[ConformanceResult] = results or []

    # -- Derived properties --------------------------------------------------

    @property
    def errors(self) -> list[ConformanceResult]:
        """Return only error-severity results."""
        return [r for r in self.results if r.severity == "error"]

    @property
    def warnings(self) -> list[ConformanceResult]:
        """Return only warning-severity results."""
        return [r for r in self.results if r.severity == "warning"]

    @property
    def passed(self) -> bool:
        """``True`` when there are no errors (warnings are acceptable)."""
        return len(self.errors) == 0

    # -- Output helpers ------------------------------------------------------

    def summary(self) -> str:
        """Format a human-readable text summary for CLI output.

        Shows counts of errors and warnings, then lists each result with a
        severity icon (``\\u2717`` for error, ``\\u26a0`` for warning), the
        check name, and the message.

        Returns
        -------
        str
            Formatted multi-line summary.
        """
        error_count = len(self.errors)
        warning_count = len(self.warnings)

        lines: list[str] = []
        lines.append(
            f"Conformance: {error_count} error(s), {warning_count} warning(s)"
        )

        for result in self.results:
            icon = "\u2717" if result.severity == "error" else "\u26a0"
            lines.append(f"  {icon} [{result.check_name}] {result.message}")

        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        """Structured output suitable for ``--json`` CLI flag.

        Returns
        -------
        dict[str, Any]
            Dictionary with ``passed``, ``error_count``, ``warning_count``,
            and ``results`` keys.
        """
        return {
            "passed": self.passed,
            "error_count": len(self.errors),
            "warning_count": len(self.warnings),
            "results": [r.to_dict() for r in self.results],
        }

    # -- Dunder helpers ------------------------------------------------------

    def __len__(self) -> int:
        """Total number of results (errors + warnings)."""
        return len(self.results)

    def __bool__(self) -> bool:
        """``True`` if any results exist."""
        return len(self.results) > 0


# ---------------------------------------------------------------------------
# Check registry
# ---------------------------------------------------------------------------

#: Type alias for a conformance check function.
CheckFn = Callable[[pd.DataFrame, PanelRequest], list[ConformanceResult]]

#: Module-level registry of conformance checks.
_CHECKS: list[CheckFn] = []


def register_check(fn: CheckFn) -> CheckFn:
    """Decorator that registers a conformance check function.

    Parameters
    ----------
    fn : CheckFn
        A callable ``(pd.DataFrame, PanelRequest) -> list[ConformanceResult]``.

    Returns
    -------
    CheckFn
        The original function, unmodified.
    """
    _CHECKS.append(fn)
    return fn


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def run_conformance(
    panel_df: pd.DataFrame, request: PanelRequest
) -> ConformanceReport:
    """Run all registered conformance checks and collect results.

    Parameters
    ----------
    panel_df : pd.DataFrame
        The assembled panel to validate.
    request : PanelRequest
        The parameters that were used to build the panel.

    Returns
    -------
    ConformanceReport
        Report containing all findings from registered checks.
    """
    all_results: list[ConformanceResult] = []
    for check_fn in _CHECKS:
        all_results.extend(check_fn(panel_df, request))
    return ConformanceReport(results=all_results)


# ---------------------------------------------------------------------------
# Data completeness checks  (coclab-1gmj)
# ---------------------------------------------------------------------------


@register_check
def check_column_null_rates(
    df: pd.DataFrame, request: PanelRequest
) -> list[ConformanceResult]:
    """Flag ACS measure columns with high null rates.

    For each ACS measure column present in *df*:
    - 100 % null  -> ``column_fully_null`` warning.
    - null rate > ``request.null_rate_threshold`` (but < 100 %) ->
      ``column_high_null_rate`` warning.

    Parameters
    ----------
    df : pd.DataFrame
        Assembled panel DataFrame.
    request : PanelRequest
        Build request carrying the configurable null_rate_threshold.

    Returns
    -------
    list[ConformanceResult]
        Zero or more warning results.
    """
    results: list[ConformanceResult] = []
    total_count = len(df)
    if total_count == 0:
        return results

    for col in ACS_MEASURE_COLUMNS:
        if col not in df.columns:
            continue

        null_count = int(df[col].isna().sum())
        null_rate = null_count / total_count

        if null_rate == 1.0:
            results.append(
                ConformanceResult(
                    check_name="column_fully_null",
                    severity="warning",
                    message=f"{col} is 100% null across all rows",
                    details={
                        "column": col,
                        "null_rate": 1.0,
                        "row_count": total_count,
                    },
                )
            )
        elif null_rate > request.null_rate_threshold:
            results.append(
                ConformanceResult(
                    check_name="column_high_null_rate",
                    severity="warning",
                    message=(
                        f"{col} is {null_rate:.0%} null "
                        f"({null_count}/{total_count} rows)"
                    ),
                    details={
                        "column": col,
                        "null_rate": null_rate,
                        "null_count": null_count,
                        "total_count": total_count,
                        "threshold": request.null_rate_threshold,
                    },
                )
            )

    return results


@register_check
def check_per_year_completeness(
    df: pd.DataFrame, request: PanelRequest
) -> list[ConformanceResult]:
    """Flag years where ACS measure columns have a high overall null rate.

    For each unique year in *df["year"]*, compute the fraction of null
    values across all ACS measure columns present.  If the rate exceeds
    ``request.null_rate_threshold``, emit a ``year_high_null_rate`` warning.

    Parameters
    ----------
    df : pd.DataFrame
        Assembled panel DataFrame (must have a ``year`` column).
    request : PanelRequest
        Build request carrying the configurable null_rate_threshold.

    Returns
    -------
    list[ConformanceResult]
        Zero or more warning results.
    """
    results: list[ConformanceResult] = []

    if "year" not in df.columns:
        return results

    present_cols = [c for c in ACS_MEASURE_COLUMNS if c in df.columns]
    if not present_cols:
        return results

    total_acs_columns = len(present_cols)

    for year in sorted(df["year"].unique()):
        year_df = df.loc[df["year"] == year, present_cols]
        if year_df.empty:
            continue

        total_cells = year_df.shape[0] * year_df.shape[1]
        null_cells = int(year_df.isna().sum().sum())
        null_rate = null_cells / total_cells

        if null_rate > request.null_rate_threshold:
            # Identify which columns contribute nulls
            null_columns = [
                c for c in present_cols if year_df[c].isna().any()
            ]
            results.append(
                ConformanceResult(
                    check_name="year_high_null_rate",
                    severity="warning",
                    message=(
                        f"Year {year}: {null_rate:.0%} null rate across "
                        f"ACS columns ({len(null_columns)} of "
                        f"{total_acs_columns} columns affected)"
                    ),
                    details={
                        "year": int(year),
                        "null_rate": null_rate,
                        "null_columns": null_columns,
                        "total_acs_columns": total_acs_columns,
                        "threshold": request.null_rate_threshold,
                    },
                )
            )

    return results


@register_check
def check_zori_eligibility_rate(
    df: pd.DataFrame, request: PanelRequest
) -> list[ConformanceResult]:
    """Warn when ZORI eligibility rate is below the minimum threshold.

    Only runs when ``request.include_zori`` is ``True`` and the
    ``zori_is_eligible`` column is present in *df*.

    Parameters
    ----------
    df : pd.DataFrame
        Assembled panel DataFrame.
    request : PanelRequest
        Build request (must have ``include_zori=True`` to trigger).

    Returns
    -------
    list[ConformanceResult]
        Zero or one warning result.
    """
    if not request.include_zori:
        return []

    if "zori_is_eligible" not in df.columns:
        return []

    total_count = len(df)
    if total_count == 0:
        return []

    eligible_count = int(df["zori_is_eligible"].sum())
    rate = eligible_count / total_count

    if rate < ZORI_MIN_ELIGIBILITY_RATE:
        return [
            ConformanceResult(
                check_name="zori_low_eligibility",
                severity="warning",
                message=(
                    f"ZORI eligibility rate is {rate:.0%} "
                    f"({eligible_count}/{total_count} rows) "
                    f"— below 20% threshold"
                ),
                details={
                    "eligibility_rate": rate,
                    "eligible_count": eligible_count,
                    "total_count": total_count,
                    "threshold": ZORI_MIN_ELIGIBILITY_RATE,
                },
            )
        ]

    return []
