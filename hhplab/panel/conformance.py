"""Post-build conformance report infrastructure for HHP-Lab panels.

This module provides the framework for running conformance checks on
assembled panel DataFrames. Individual checks are registered via the
``register_check`` decorator and executed by ``run_conformance``.

Registered checks
-----------------
- ``check_year_coverage`` (coclab-2jtk): Year range overlap
- ``check_schema_measures`` (coclab-2jtk, coclab-d0qm): Measure column presence
- ``check_schema_zori`` (coclab-2jtk): ZORI column presence when requested
- ``check_temporal_variation`` (coclab-1d2j): Suspiciously static values
- ``check_column_null_rates`` (coclab-1gmj): Per-column null rates
- ``check_per_year_completeness`` (coclab-1gmj): Per-year null rates
- ``check_zori_eligibility_rate`` (coclab-1gmj): ZORI eligibility rate
- ``check_pit_exceeds_population`` (coclab-2472): PIT count > total population
- ``check_coc_count`` (coclab-2o8i): Expected CoC count
- ``check_panel_balance`` (coclab-2o8i): Balanced panel (all CoCs in all years)
- ``check_coc_year_gaps`` (coclab-2o8i): Non-contiguous year coverage

Usage
-----
    from hhplab.panel.conformance import run_conformance, PanelRequest

    request = PanelRequest(start_year=2020, end_year=2024)
    report = run_conformance(panel_df, request)

    if not report.passed:
        print(report.summary())

    # Machine-readable output for --json
    data = report.to_dict()
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Literal

import pandas as pd

from hhplab.analysis_geo import resolve_geo_col as _resolve_geo_col
from hhplab.schema.columns import (
    ACS1_MEASURE_COLUMNS,
    ACS_MEASURE_COLUMNS,
    LAUS_MEASURE_COLUMNS,
    SAE_MEASURE_COLUMNS,
    TOTAL_POPULATION,
    ZORI_COLUMNS,
    ZORI_PROVENANCE_COLUMNS,
)
from hhplab.schema.contracts import COC_PANEL_CONTRACT, validate_artifact_contract

# ---------------------------------------------------------------------------
# Column and threshold constants
# ---------------------------------------------------------------------------

#: ACS measure columns checked for schema presence and data completeness.
#: Includes ACS5 tract-level measures (apportioned to analysis geography) and
#: the ACS1-derived metro unemployment rate used in CoC and metro panels.
#: Measures that should normally vary across years. If too many CoC-year
#: pairs show identical values year-over-year, a data broadcast bug is likely.
TEMPORAL_VARIATION_MEASURES: list[str] = [
    TOTAL_POPULATION,
    "pit_total",
]

#: Warn when more than 50% of year-over-year pairs are unchanged.
TEMPORAL_WARN_THRESHOLD: float = 0.50

#: Error when more than 90% of year-over-year pairs are unchanged.
TEMPORAL_ERROR_THRESHOLD: float = 0.90

#: Minimum ZORI eligibility rate before a warning is emitted.
ZORI_MIN_ELIGIBILITY_RATE: float = 0.20

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


def _json_safe(value: Any) -> Any:
    """Recursively coerce values into JSON-serializable Python types."""
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, tuple):
        return [_json_safe(v) for v in value]
    if isinstance(value, set):
        return [_json_safe(v) for v in sorted(value)]
    if hasattr(value, "item") and callable(value.item):
        try:
            return value.item()
        except (TypeError, ValueError):
            pass
    return value


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
    measure_columns : list[str] | None
        Demographic measure columns expected in the panel.  When ``None``
        (default), falls back to ``ACS_MEASURE_COLUMNS``.  Set explicitly
        for non-ACS schemas (e.g., PEP-based panels with ``["population"]``).
    acs_products : list[str]
        Which ACS products are expected in the panel.  Default ``["acs5"]``
        validates only ACS 5-year columns.  Include ``"acs1"`` to also
        validate ACS 1-year columns (e.g., ``["acs5", "acs1"]``).
        Include ``"sae"`` to validate SAE-derived ACS measures without
        requiring direct ACS5/ACS1 columns.
    include_laus : bool
        Whether BLS LAUS metro-native labor-market measures are expected
        (``labor_force``, ``employed``, ``unemployed``, ``unemployment_rate``).
        These are distinct from ACS-derived unemployment measures.
    enforce_schema_contract : bool
        Whether to report canonical schema-contract drift and lineage findings.
    """

    start_year: int
    end_year: int
    include_zori: bool = False
    weighting_method: Literal["area", "population"] = "population"
    zori_min_coverage: float = 0.90
    expected_coc_count: int | None = None
    expected_geo_count: int | None = None
    geo_type: str = "coc"
    null_rate_threshold: float = 0.50
    measure_columns: list[str] | None = None
    acs_products: list[str] = field(default_factory=lambda: ["acs5"])
    include_laus: bool = False
    enforce_schema_contract: bool = False


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
            "details": _json_safe(self.details),
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
# Helpers
# ---------------------------------------------------------------------------


def _effective_measure_columns(request: PanelRequest) -> list[str]:
    """Return the measure columns to validate for *request*.

    When ``request.measure_columns`` is set, those columns are used.
    Otherwise builds the expected set from the union of requested data
    products: ACS5 columns when ``"acs5"`` is requested, ACS1 columns
    when ``"acs1"`` is requested, SAE columns when ``"sae"`` is requested,
    and LAUS columns when
    ``request.include_laus`` is True.
    """
    if request.measure_columns is not None:
        return request.measure_columns
    columns: list[str] = []
    if "acs5" in request.acs_products:
        columns.extend(ACS_MEASURE_COLUMNS)
    if "acs1" in request.acs_products:
        columns.extend(ACS1_MEASURE_COLUMNS)
    if "sae" in request.acs_products:
        columns.extend(SAE_MEASURE_COLUMNS)
    if request.include_laus:
        columns.extend(LAUS_MEASURE_COLUMNS)
    # Deduplicate while preserving order: unemployment_rate appears in both
    # ACS_MEASURE_COLUMNS and LAUS_MEASURE_COLUMNS; only validate it once.
    seen: set[str] = set()
    result: list[str] = []
    for col in columns:
        if col not in seen:
            seen.add(col)
            result.append(col)
    return result


# ---------------------------------------------------------------------------
# Year coverage and schema checks  (coclab-2jtk, coclab-d0qm)
# ---------------------------------------------------------------------------


@register_check
def check_year_coverage(
    panel_df: pd.DataFrame,
    request: PanelRequest,
) -> list[ConformanceResult]:
    """Verify that the panel covers the requested year range."""
    requested = set(range(request.start_year, request.end_year + 1))
    present = set(panel_df["year"].unique()) if "year" in panel_df.columns else set()
    missing = sorted(requested - present)

    if not requested & present:
        return [
            ConformanceResult(
                check_name="check_year_coverage",
                severity="error",
                message=(
                    f"No overlap between requested years "
                    f"({request.start_year}-{request.end_year}) "
                    f"and panel years ({sorted(present) if present else 'none'})"
                ),
                details={
                    "requested_years": sorted(requested),
                    "present_years": sorted(present),
                    "missing_years": sorted(requested),
                    "coverage_fraction": f"0/{len(requested)}",
                },
            )
        ]

    if missing:
        n_present = len(requested) - len(missing)
        return [
            ConformanceResult(
                check_name="check_year_coverage",
                severity="warning",
                message=(
                    f"{n_present}/{len(requested)} requested years present; "
                    f"missing: {', '.join(str(y) for y in missing)}"
                ),
                details={
                    "requested_years": sorted(requested),
                    "present_years": sorted(requested - set(missing)),
                    "missing_years": missing,
                    "coverage_fraction": f"{n_present}/{len(requested)}",
                },
            )
        ]

    return []


@register_check
def check_schema_measures(
    panel_df: pd.DataFrame,
    request: PanelRequest,
) -> list[ConformanceResult]:
    """Verify that at least one expected measure column is present.

    Uses ``request.measure_columns`` when set, otherwise falls back to
    ``ACS_MEASURE_COLUMNS``.  This allows PEP-based and other non-ACS
    panels to pass conformance with their own measure columns
    (e.g., ``["population"]``).
    """
    expected = _effective_measure_columns(request)
    if not expected:
        return []
    present = [c for c in expected if c in panel_df.columns]
    missing = [c for c in expected if c not in panel_df.columns]

    if not present:
        return [
            ConformanceResult(
                check_name="check_schema_measures",
                severity="error",
                message=(
                    "None of the expected measure columns are present "
                    "in the panel schema"
                ),
                details={
                    "expected_columns": list(expected),
                    "present_columns": present,
                },
            )
        ]

    if missing:
        return [
            ConformanceResult(
                check_name="check_schema_measures",
                severity="warning",
                message=(
                    f"Missing {len(missing)} of {len(expected)} expected "
                    f"measure columns: {missing}"
                ),
                details={
                    "expected_columns": list(expected),
                    "present_columns": present,
                    "missing_columns": missing,
                },
            )
        ]

    return []


# Backward-compatible alias (coclab-d0qm).
check_schema_acs = check_schema_measures


@register_check
def check_schema_contract(
    panel_df: pd.DataFrame,
    request: PanelRequest,
) -> list[ConformanceResult]:
    """Report ambiguous drift-prone columns and missing measure lineage."""
    if not request.enforce_schema_contract or request.geo_type != "coc":
        return []

    findings = [
        finding
        for finding in validate_artifact_contract(panel_df, COC_PANEL_CONTRACT)
        if finding.code in {"drift_prone_column", "missing_lineage_columns"}
    ]
    return [
        ConformanceResult(
            check_name="check_schema_contract",
            severity=finding.severity,
            message=finding.message,
            details=finding.to_dict(),
        )
        for finding in findings
    ]


@register_check
def check_schema_zori(
    panel_df: pd.DataFrame,
    request: PanelRequest,
) -> list[ConformanceResult]:
    """Verify ZORI columns are present when ZORI was requested."""
    if not request.include_zori:
        return []

    present_data = [c for c in ZORI_COLUMNS if c in panel_df.columns]

    if not present_data:
        return [
            ConformanceResult(
                check_name="check_schema_zori",
                severity="error",
                message=(
                    "ZORI was requested but none of the ZORI data columns "
                    "are present in the panel schema"
                ),
                details={
                    "expected_columns": list(ZORI_COLUMNS),
                    "present_columns": present_data,
                },
            )
        ]

    missing_provenance = [
        c for c in ZORI_PROVENANCE_COLUMNS if c not in panel_df.columns
    ]

    if missing_provenance:
        return [
            ConformanceResult(
                check_name="check_schema_zori",
                severity="warning",
                message=(
                    f"ZORI provenance columns missing: "
                    f"{', '.join(missing_provenance)}"
                ),
                details={
                    "expected_columns": list(ZORI_COLUMNS + ZORI_PROVENANCE_COLUMNS),
                    "present_columns": present_data
                    + [c for c in ZORI_PROVENANCE_COLUMNS if c in panel_df.columns],
                    "missing_provenance_columns": missing_provenance,
                },
            )
        ]

    return []


# ---------------------------------------------------------------------------
# Temporal variation check  (coclab-1d2j)
# ---------------------------------------------------------------------------


@register_check
def check_temporal_variation(
    df: pd.DataFrame, request: PanelRequest
) -> list[ConformanceResult]:
    """Detect suspiciously static year-over-year values in the panel.

    For measures that should normally vary across years (population, PIT
    counts), flags when an excessive proportion of CoC-year pairs have
    identical consecutive-year values.
    """
    results: list[ConformanceResult] = []

    if "year" not in df.columns or df["year"].nunique() < 2:
        return results

    # Determine the geo ID column (coc_id or geo_id depending on source).
    try:
        geo_col = _resolve_geo_col(df)
    except KeyError:
        return results

    for measure in TEMPORAL_VARIATION_MEASURES:
        if measure not in df.columns:
            continue

        sorted_df = df.sort_values([geo_col, "year"])
        same_coc = sorted_df[geo_col] == sorted_df[geo_col].shift(1)
        current_val = sorted_df[measure]
        prior_val = sorted_df[measure].shift(1)

        both_non_null = current_val.notna() & prior_val.notna()
        comparable = same_coc & both_non_null

        total_pairs = int(comparable.sum())
        if total_pairs == 0:
            continue

        unchanged = comparable & (current_val == prior_val)
        unchanged_count = int(unchanged.sum())
        unchanged_rate = unchanged_count / total_pairs

        if unchanged_rate > TEMPORAL_ERROR_THRESHOLD:
            severity: Literal["error", "warning"] = "error"
            threshold_used = TEMPORAL_ERROR_THRESHOLD
            message = (
                f"{measure}: {unchanged_rate:.0%} of year-over-year pairs "
                f"unchanged ({unchanged_count}/{total_pairs}) "
                f"\u2014 possible data broadcast"
            )
        elif unchanged_rate > TEMPORAL_WARN_THRESHOLD:
            severity = "warning"
            threshold_used = TEMPORAL_WARN_THRESHOLD
            message = (
                f"{measure}: {unchanged_rate:.0%} of year-over-year pairs "
                f"unchanged ({unchanged_count}/{total_pairs})"
            )
        else:
            continue

        results.append(
            ConformanceResult(
                check_name="temporal_variation",
                severity=severity,
                message=message,
                details={
                    "measure": measure,
                    "unchanged_rate": unchanged_rate,
                    "unchanged_count": unchanged_count,
                    "total_pairs": total_pairs,
                    "threshold_used": threshold_used,
                },
            )
        )

    return results


# ---------------------------------------------------------------------------
# Data completeness checks  (coclab-1gmj)
# ---------------------------------------------------------------------------


@register_check
def check_column_null_rates(
    df: pd.DataFrame, request: PanelRequest
) -> list[ConformanceResult]:
    """Flag measure columns with high null rates."""
    results: list[ConformanceResult] = []
    total_count = len(df)
    if total_count == 0:
        return results

    for col in _effective_measure_columns(request):
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
    """Flag years where measure columns have a high overall null rate."""
    results: list[ConformanceResult] = []

    if "year" not in df.columns:
        return results

    effective = _effective_measure_columns(request)
    present_cols = [c for c in effective if c in df.columns]
    if not present_cols:
        return results

    total_measure_columns = len(present_cols)

    for year in sorted(df["year"].unique()):
        year_df = df.loc[df["year"] == year, present_cols]
        if year_df.empty:
            continue

        total_cells = year_df.shape[0] * year_df.shape[1]
        null_cells = int(year_df.isna().sum().sum())
        null_rate = null_cells / total_cells

        if null_rate > request.null_rate_threshold:
            null_columns = [
                c for c in present_cols if year_df[c].isna().any()
            ]
            results.append(
                ConformanceResult(
                    check_name="year_high_null_rate",
                    severity="warning",
                    message=(
                        f"Year {year}: {null_rate:.0%} null rate across "
                        f"measure columns ({len(null_columns)} of "
                        f"{total_measure_columns} columns affected)"
                    ),
                    details={
                        "year": int(year),
                        "null_rate": null_rate,
                        "null_columns": null_columns,
                        "total_measure_columns": total_measure_columns,
                        "threshold": request.null_rate_threshold,
                    },
                )
            )

    return results


@register_check
def check_zori_eligibility_rate(
    df: pd.DataFrame, request: PanelRequest
) -> list[ConformanceResult]:
    """Warn when ZORI eligibility rate is below the minimum threshold."""
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
                    f"\u2014 below 20% threshold"
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


# ---------------------------------------------------------------------------
# PIT vs population plausibility check  (coclab-2472)
# ---------------------------------------------------------------------------


@register_check
def check_pit_exceeds_population(
    df: pd.DataFrame, request: PanelRequest
) -> list[ConformanceResult]:
    """Flag CoC-year rows where PIT count exceeds total population.

    This is logically impossible and indicates a data join or aggregation
    bug (e.g., population was not apportioned while PIT counts were summed
    at the CoC level).
    """
    if "pit_total" not in df.columns or "total_population" not in df.columns:
        return []

    try:
        geo_col = _resolve_geo_col(df)
    except KeyError:
        return []

    comparable = df[df["pit_total"].notna() & df["total_population"].notna()]
    if comparable.empty:
        return []

    bad = comparable[comparable["pit_total"] > comparable["total_population"]]
    if bad.empty:
        return []

    bad_count = len(bad)
    examples = (
        bad[[geo_col, "year", "pit_total", "total_population"]]
        .head(5)
        .to_dict(orient="records")
    )

    return [
        ConformanceResult(
            check_name="pit_exceeds_population",
            severity="error",
            message=(
                f"{bad_count} geo-year row(s) have pit_total > "
                f"total_population"
            ),
            details={
                "bad_row_count": bad_count,
                "total_comparable_rows": len(comparable),
                "examples": examples,
            },
        )
    ]


# ---------------------------------------------------------------------------
# CoC coverage checks  (coclab-2o8i)
# ---------------------------------------------------------------------------


@register_check
def check_coc_count(
    panel_df: pd.DataFrame, request: PanelRequest
) -> list[ConformanceResult]:
    """Check whether the panel has the expected number of unique geo units."""
    expected = request.expected_geo_count or request.expected_coc_count
    if expected is None:
        return []

    try:
        geo_col = _resolve_geo_col(panel_df)
    except KeyError:
        return []

    actual = panel_df[geo_col].nunique()

    if actual < expected:
        return [
            ConformanceResult(
                check_name="coc_count_mismatch",
                severity="warning",
                message=f"{actual}/{expected} expected geo units present",
                details={
                    "actual_count": actual,
                    "expected_count": expected,
                    "deficit": expected - actual,
                },
            )
        ]
    return []


@register_check
def check_panel_balance(
    panel_df: pd.DataFrame, request: PanelRequest
) -> list[ConformanceResult]:
    """Check whether the panel is balanced (all CoCs in all years)."""
    try:
        geo_col = _resolve_geo_col(panel_df)
    except KeyError:
        return []
    if "year" not in panel_df.columns:
        return []

    all_years = set(panel_df["year"].unique())
    if len(all_years) <= 1:
        return []

    geo_years = panel_df.groupby(geo_col)["year"].apply(set)
    incomplete_geos = geo_years[geo_years.apply(len) < len(all_years)]
    incomplete_count = len(incomplete_geos)

    if incomplete_count == 0:
        return []

    missing_years: list[int] = []
    for _geo_id, years_present in incomplete_geos.items():
        for y in all_years - years_present:
            missing_years.append(y)

    most_common_gap = Counter(missing_years).most_common(1)[0][0]
    total_geos = panel_df[geo_col].nunique()

    return [
        ConformanceResult(
            check_name="unbalanced_panel",
            severity="warning",
            message=(
                f"{incomplete_count} geo units have incomplete year coverage "
                f"({incomplete_count}/{total_geos})"
            ),
            details={
                "incomplete_count": incomplete_count,
                "total_geos": total_geos,
                "expected_years": sorted(all_years),
                "most_common_gap": int(most_common_gap),
            },
        )
    ]


@register_check
def check_coc_year_gaps(
    panel_df: pd.DataFrame, request: PanelRequest
) -> list[ConformanceResult]:
    """Check for CoCs with non-contiguous year coverage (internal gaps).

    A gap means a CoC is present in year Y, absent in Y+1, present again
    in Y+2 or later. Missing edge years are NOT gaps.
    """
    try:
        geo_col = _resolve_geo_col(panel_df)
    except KeyError:
        return []

    gap_examples: list[dict[str, Any]] = []

    for geo_id, group in panel_df.groupby(geo_col):
        present_years = sorted(group["year"].unique())
        if len(present_years) <= 1:
            continue

        full_range = set(range(present_years[0], present_years[-1] + 1))
        actual = set(present_years)
        internal_missing = sorted(full_range - actual)

        if internal_missing:
            gap_examples.append({
                "geo_id": geo_id,
                "present_years": present_years,
                "missing_years": internal_missing,
            })

    if not gap_examples:
        return []

    gap_count = len(gap_examples)
    return [
        ConformanceResult(
            check_name="coc_year_gaps",
            severity="warning",
            message=(
                f"{gap_count} geo units have non-contiguous year coverage "
                f"(gaps in their time series)"
            ),
            details={
                "gap_count": gap_count,
                "examples": gap_examples[:5],
            },
        )
    ]
