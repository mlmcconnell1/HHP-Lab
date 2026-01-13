"""Cross-check validator: compare rollup population vs measures population.

This module validates CoC population estimates from the rollup engine (WP-B)
against existing CoC measures from HUD/ACS sources. It identifies discrepancies
that may indicate data quality issues, crosswalk problems, or boundary changes.

Validation Checks
-----------------

1. **Key Matching**: Identifies CoCs present in one dataset but missing from
   the other, which may indicate boundary vintage mismatches or data gaps.

2. **Absolute/Percent Deltas**: Computes the difference between rollup and
   measures population estimates. Large deltas may indicate:
   - Crosswalk coverage issues
   - Boundary changes between vintages
   - ACS vintage mismatches
   - Aggregation methodology differences

3. **Outlier Flags**: Applies configurable thresholds to flag warnings and
   errors based on percent delta magnitude.

4. **Coverage Sanity**: Validates that coverage_ratio from rollup is within
   expected bounds. Values > 1.01 suggest overlapping tract assignments,
   while very low values indicate incomplete tract coverage.

5. **Rank Sanity** (optional): Compares top-N CoCs by population between
   datasets to detect systematic ordering differences.

Output Schema
-------------
- coc_id (str): CoC identifier
- rollup_population (float): Population from rollup aggregation
- measures_population (float): Population from existing measures
- delta (float): rollup - measures
- pct_delta (float): delta / measures
- coverage_ratio (float): From rollup (sum of area_shares)
- status (str): "ok", "warning", or "error"
- issues (str): Semicolon-separated list of issue descriptions

Usage
-----
    from coclab.acs import run_crosscheck, print_crosscheck_report

    result = run_crosscheck(
        boundary_vintage="2025",
        acs_vintage="2019-2023",
        tract_vintage="2023",
        weighting="area",
    )

    print_crosscheck_report(result)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from coclab.provenance import ProvenanceBlock, write_parquet_with_provenance
from coclab import naming

logger = logging.getLogger(__name__)

# Default data directories
DEFAULT_ACS_DIR = Path("data/curated/acs")
DEFAULT_MEASURES_DIR = Path("data/curated/measures")


@dataclass
class CrosscheckResult:
    """Result of population crosscheck validation.

    Attributes
    ----------
    error_count : int
        Number of error-level issues found.
    warning_count : int
        Number of warning-level issues found.
    report_df : pd.DataFrame
        Detailed per-CoC comparison results.
    missing_in_rollup : list[str]
        CoC IDs present in measures but missing from rollup.
    missing_in_measures : list[str]
        CoC IDs present in rollup but missing from measures.
    summary : dict
        Summary statistics including total counts and delta distributions.
    passed : bool
        True if no errors were found (warnings are acceptable).
    """

    error_count: int = 0
    warning_count: int = 0
    report_df: pd.DataFrame = field(default_factory=pd.DataFrame)
    missing_in_rollup: list[str] = field(default_factory=list)
    missing_in_measures: list[str] = field(default_factory=list)
    summary: dict = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        """True if no errors were found."""
        return self.error_count == 0


def crosscheck_population(
    rollup_df: pd.DataFrame,
    measures_df: pd.DataFrame,
    warn_pct: float = 0.01,
    error_pct: float = 0.05,
    min_coverage: float = 0.95,
) -> CrosscheckResult:
    """Compare rollup population estimates against measures population.

    Parameters
    ----------
    rollup_df : pd.DataFrame
        CoC population rollup with columns:
        - coc_id (str)
        - coc_population (float)
        - coverage_ratio (float)
    measures_df : pd.DataFrame
        Existing CoC measures with columns:
        - coc_id (str)
        - total_population (float)
    warn_pct : float
        Percentage threshold for warnings (default: 0.01 = 1%).
        If abs(pct_delta) > warn_pct, flag as warning.
    error_pct : float
        Percentage threshold for errors (default: 0.05 = 5%).
        If abs(pct_delta) > error_pct, flag as error.
    min_coverage : float
        Minimum acceptable coverage_ratio (default: 0.95).
        If coverage_ratio < min_coverage, flag as warning.

    Returns
    -------
    CrosscheckResult
        Validation result with error/warning counts, detailed report,
        and summary statistics.

    Raises
    ------
    ValueError
        If required columns are missing from input DataFrames.

    Examples
    --------
    >>> rollup = pd.DataFrame({
    ...     "coc_id": ["CO-500", "CO-501"],
    ...     "coc_population": [100000, 50000],
    ...     "coverage_ratio": [1.0, 0.98]
    ... })
    >>> measures = pd.DataFrame({
    ...     "coc_id": ["CO-500", "CO-501"],
    ...     "total_population": [99000, 51000]
    ... })
    >>> result = crosscheck_population(rollup, measures)
    >>> result.passed
    True
    """
    # Validate required columns in rollup
    rollup_required = {"coc_id", "coc_population"}
    rollup_missing = rollup_required - set(rollup_df.columns)
    if rollup_missing:
        raise ValueError(f"rollup_df missing required columns: {rollup_missing}")

    # Validate required columns in measures
    measures_required = {"coc_id", "total_population"}
    measures_missing = measures_required - set(measures_df.columns)
    if measures_missing:
        raise ValueError(f"measures_df missing required columns: {measures_missing}")

    # Initialize result
    result = CrosscheckResult()

    # Get CoC ID sets
    rollup_cocs = set(rollup_df["coc_id"].unique())
    measures_cocs = set(measures_df["coc_id"].unique())

    # Check for missing CoCs
    result.missing_in_rollup = sorted(measures_cocs - rollup_cocs)
    result.missing_in_measures = sorted(rollup_cocs - measures_cocs)

    # Log missing CoCs
    if result.missing_in_rollup:
        logger.warning(
            f"Found {len(result.missing_in_rollup)} CoCs in measures but missing "
            f"from rollup: {result.missing_in_rollup[:5]}..."
        )
    if result.missing_in_measures:
        logger.warning(
            f"Found {len(result.missing_in_measures)} CoCs in rollup but missing "
            f"from measures: {result.missing_in_measures[:5]}..."
        )

    # Prepare data for merge - select only needed columns
    rollup_cols = ["coc_id", "coc_population"]
    if "coverage_ratio" in rollup_df.columns:
        rollup_cols.append("coverage_ratio")

    rollup_subset = rollup_df[rollup_cols].copy()
    measures_subset = measures_df[["coc_id", "total_population"]].copy()

    # Merge datasets
    merged = rollup_subset.merge(
        measures_subset,
        on="coc_id",
        how="outer",
        suffixes=("_rollup", "_measures"),
    )

    # Rename columns for clarity
    merged = merged.rename(columns={
        "coc_population": "rollup_population",
        "total_population": "measures_population",
    })

    # Compute delta and pct_delta
    merged["delta"] = merged["rollup_population"] - merged["measures_population"]
    merged["pct_delta"] = merged["delta"] / merged["measures_population"]

    # Handle division by zero (measures_population = 0)
    merged.loc[merged["measures_population"] == 0, "pct_delta"] = float("inf")

    # Ensure coverage_ratio exists
    if "coverage_ratio" not in merged.columns:
        merged["coverage_ratio"] = pd.NA

    # Initialize status and issues columns
    merged["status"] = "ok"
    merged["issues"] = ""

    # Apply validation rules
    error_count = 0
    warning_count = 0

    for idx in merged.index:
        issues = []
        status = "ok"

        rollup_pop = merged.loc[idx, "rollup_population"]
        measures_pop = merged.loc[idx, "measures_population"]
        pct_delta = merged.loc[idx, "pct_delta"]
        coverage = merged.loc[idx, "coverage_ratio"]
        coc_id = merged.loc[idx, "coc_id"]

        # Check for missing data
        if pd.isna(rollup_pop):
            issues.append("missing from rollup")
            status = "error"
        elif pd.isna(measures_pop):
            issues.append("missing from measures")
            status = "warning"
        else:
            # Check pct_delta thresholds
            if pd.notna(pct_delta) and pct_delta != float("inf"):
                abs_pct = abs(pct_delta)
                if abs_pct > error_pct:
                    issues.append(f"pct_delta {pct_delta:.1%} exceeds {error_pct:.0%}")
                    status = "error"
                elif abs_pct > warn_pct:
                    issues.append(f"pct_delta {pct_delta:.1%} exceeds {warn_pct:.0%}")
                    if status != "error":
                        status = "warning"
            elif pct_delta == float("inf"):
                issues.append("measures_population is zero")
                status = "error"

        # Check coverage_ratio
        if pd.notna(coverage):
            if coverage > 1.01:
                issues.append(f"coverage_ratio {coverage:.3f} > 1.01")
                status = "error"
            elif coverage < min_coverage:
                issues.append(f"coverage_ratio {coverage:.3f} < {min_coverage}")
                if status != "error":
                    status = "warning"

        merged.loc[idx, "status"] = status
        merged.loc[idx, "issues"] = "; ".join(issues)

        if status == "error":
            error_count += 1
        elif status == "warning":
            warning_count += 1

    result.error_count = error_count
    result.warning_count = warning_count

    # Reorder columns for output schema
    output_cols = [
        "coc_id",
        "rollup_population",
        "measures_population",
        "delta",
        "pct_delta",
        "coverage_ratio",
        "status",
        "issues",
    ]
    result.report_df = merged[output_cols].copy()

    # Compute summary statistics
    valid_deltas = merged.loc[
        merged["rollup_population"].notna() & merged["measures_population"].notna(),
        "pct_delta"
    ]
    valid_deltas = valid_deltas[valid_deltas != float("inf")]

    result.summary = {
        "total_cocs_rollup": len(rollup_cocs),
        "total_cocs_measures": len(measures_cocs),
        "matched_cocs": len(merged) - len(result.missing_in_rollup) - len(result.missing_in_measures),
        "missing_in_rollup": len(result.missing_in_rollup),
        "missing_in_measures": len(result.missing_in_measures),
        "error_count": error_count,
        "warning_count": warning_count,
        "ok_count": len(merged) - error_count - warning_count,
        "mean_abs_pct_delta": float(valid_deltas.abs().mean()) if len(valid_deltas) > 0 else None,
        "median_abs_pct_delta": float(valid_deltas.abs().median()) if len(valid_deltas) > 0 else None,
        "max_abs_pct_delta": float(valid_deltas.abs().max()) if len(valid_deltas) > 0 else None,
        "total_rollup_population": float(merged["rollup_population"].sum()),
        "total_measures_population": float(merged["measures_population"].sum()),
    }

    return result


def get_rollup_path(
    boundary_vintage: str,
    acs_vintage: str,
    tract_vintage: str,
    weighting: str,
    base_dir: Path | str | None = None,
) -> Path:
    """Get the path to CoC population rollup file.

    Parameters
    ----------
    boundary_vintage : str
        CoC boundary vintage (e.g., "2025").
    acs_vintage : str
        ACS vintage string (e.g., "2019-2023").
    tract_vintage : str
        Tract geography vintage (e.g., "2023").
    weighting : str
        Weighting method ("area" or "population_mass").
    base_dir : Path or str, optional
        Base directory for data. Defaults to 'data/curated/acs'.

    Returns
    -------
    Path
        Path to rollup parquet file.
    """
    if base_dir is None:
        base_dir = DEFAULT_ACS_DIR
    else:
        base_dir = Path(base_dir)
    filename = (
        f"coc_population_rollup__{boundary_vintage}__{acs_vintage}"
        f"__{tract_vintage}__{weighting}.parquet"
    )
    return base_dir / filename


def get_measures_path(
    boundary_vintage: str,
    acs_vintage: str,
    base_dir: Path | str | None = None,
) -> Path:
    """Get the path to CoC measures file.

    Supports both new temporal shorthand naming (measures__A{acs}@B{boundary}.parquet)
    and legacy naming (coc_measures__{boundary}__{acs}.parquet). Returns new naming
    if it exists, otherwise falls back to legacy naming.

    Parameters
    ----------
    boundary_vintage : str
        CoC boundary vintage (e.g., "2025").
    acs_vintage : str
        ACS vintage string (e.g., "2019-2023").
    base_dir : Path or str, optional
        Base directory for measures. Defaults to 'data/curated/measures'.

    Returns
    -------
    Path
        Path to measures parquet file.
    """
    if base_dir is None:
        base_dir = DEFAULT_MEASURES_DIR
    else:
        base_dir = Path(base_dir)

    # Try new naming first
    new_path = base_dir / naming.measures_filename(acs_vintage, boundary_vintage)
    if new_path.exists():
        return new_path

    # Fall back to legacy naming
    legacy_path = base_dir / f"coc_measures__{boundary_vintage}__{acs_vintage}.parquet"
    return legacy_path


def get_crosscheck_output_path(
    boundary_vintage: str,
    acs_vintage: str,
    tract_vintage: str,
    weighting: str,
    base_dir: Path | str | None = None,
) -> Path:
    """Get the canonical output path for crosscheck results.

    Parameters
    ----------
    boundary_vintage : str
        CoC boundary vintage (e.g., "2025").
    acs_vintage : str
        ACS vintage string (e.g., "2019-2023").
    tract_vintage : str
        Tract geography vintage (e.g., "2023").
    weighting : str
        Weighting method ("area" or "population_mass").
    base_dir : Path or str, optional
        Base directory for output. Defaults to 'data/curated/acs'.

    Returns
    -------
    Path
        Output path like
        'data/curated/acs/acs_population_crosscheck__2025__2019-2023__2023__area.parquet'.
    """
    if base_dir is None:
        base_dir = DEFAULT_ACS_DIR
    else:
        base_dir = Path(base_dir)
    filename = (
        f"acs_population_crosscheck__{boundary_vintage}__{acs_vintage}"
        f"__{tract_vintage}__{weighting}.parquet"
    )
    return base_dir / filename


def run_crosscheck(
    boundary_vintage: str,
    acs_vintage: str,
    tract_vintage: str,
    weighting: str = "area",
    warn_pct: float = 0.01,
    error_pct: float = 0.05,
    min_coverage: float = 0.95,
    acs_dir: Path | str | None = None,
    measures_dir: Path | str | None = None,
    output_dir: Path | str | None = None,
    save_report: bool = True,
) -> CrosscheckResult:
    """Run population crosscheck from files and optionally save report.

    Loads rollup and measures parquet files, runs crosscheck validation,
    and optionally saves the detailed report to a parquet file.

    Parameters
    ----------
    boundary_vintage : str
        CoC boundary vintage (e.g., "2025").
    acs_vintage : str
        ACS vintage string (e.g., "2019-2023").
    tract_vintage : str
        Tract geography vintage (e.g., "2023").
    weighting : str
        Weighting method ("area" or "population_mass"). Default is "area".
    warn_pct : float
        Percentage threshold for warnings (default: 0.01 = 1%).
    error_pct : float
        Percentage threshold for errors (default: 0.05 = 5%).
    min_coverage : float
        Minimum acceptable coverage_ratio (default: 0.95).
    acs_dir : Path or str, optional
        Directory containing rollup data. Defaults to 'data/curated/acs'.
    measures_dir : Path or str, optional
        Directory containing measures data. Defaults to 'data/curated/measures'.
    output_dir : Path or str, optional
        Output directory. Defaults to 'data/curated/acs'.
    save_report : bool
        If True, save the report DataFrame to parquet. Default is True.

    Returns
    -------
    CrosscheckResult
        Validation result with error/warning counts and detailed report.

    Raises
    ------
    FileNotFoundError
        If rollup or measures file is not found.

    Examples
    --------
    >>> result = run_crosscheck(
    ...     boundary_vintage="2025",
    ...     acs_vintage="2019-2023",
    ...     tract_vintage="2023",
    ...     weighting="area"
    ... )
    >>> print(f"Errors: {result.error_count}, Warnings: {result.warning_count}")
    """
    # Resolve directories
    acs_dir = Path(acs_dir) if acs_dir else DEFAULT_ACS_DIR
    measures_dir = Path(measures_dir) if measures_dir else DEFAULT_MEASURES_DIR
    output_dir = Path(output_dir) if output_dir else DEFAULT_ACS_DIR

    # Get input paths
    rollup_path = get_rollup_path(
        boundary_vintage, acs_vintage, tract_vintage, weighting, acs_dir
    )
    measures_path = get_measures_path(boundary_vintage, acs_vintage, measures_dir)

    # Validate inputs exist
    if not rollup_path.exists():
        raise FileNotFoundError(
            f"Rollup file not found: {rollup_path}. "
            f"Run 'coclab rollup-acs-population --boundary {boundary_vintage} "
            f"--acs {acs_vintage} --tracts {tract_vintage}' first."
        )
    if not measures_path.exists():
        raise FileNotFoundError(
            f"Measures file not found: {measures_path}. "
            f"Run 'coclab build-measures --boundary {boundary_vintage} "
            f"--acs {acs_vintage}' first."
        )

    # Load data
    logger.info(f"Loading rollup from {rollup_path}")
    rollup_df = pd.read_parquet(rollup_path)

    logger.info(f"Loading measures from {measures_path}")
    measures_df = pd.read_parquet(measures_path)

    # Run crosscheck
    logger.info("Running population crosscheck...")
    result = crosscheck_population(
        rollup_df,
        measures_df,
        warn_pct=warn_pct,
        error_pct=error_pct,
        min_coverage=min_coverage,
    )

    logger.info(
        f"Crosscheck complete: {result.error_count} errors, "
        f"{result.warning_count} warnings"
    )

    # Save report if requested
    if save_report:
        output_path = get_crosscheck_output_path(
            boundary_vintage, acs_vintage, tract_vintage, weighting, output_dir
        )

        # Build provenance metadata
        provenance = ProvenanceBlock(
            boundary_vintage=boundary_vintage,
            tract_vintage=tract_vintage,
            acs_vintage=acs_vintage,
            weighting=weighting,
            extra={
                "dataset": "acs_population_crosscheck",
                "source_rollup": str(rollup_path),
                "source_measures": str(measures_path),
                "warn_pct": warn_pct,
                "error_pct": error_pct,
                "min_coverage": min_coverage,
                "error_count": result.error_count,
                "warning_count": result.warning_count,
                "passed": result.passed,
            },
        )

        write_parquet_with_provenance(result.report_df, output_path, provenance)
        logger.info(f"Wrote crosscheck report to {output_path}")

    return result


def print_crosscheck_report(
    result: CrosscheckResult,
    top_n: int = 25,
    show_all_issues: bool = False,
) -> int:
    """Print human-readable crosscheck report to console.

    Parameters
    ----------
    result : CrosscheckResult
        Result from crosscheck_population or run_crosscheck.
    top_n : int
        Number of worst deltas to show (by abs pct). Default is 25.
    show_all_issues : bool
        If True, show all rows with issues. Default is False.

    Returns
    -------
    int
        Exit code: 0 if no errors, 2 if errors found.
    """
    print("\n" + "=" * 70)
    print("ACS Population Crosscheck Report")
    print("=" * 70)

    # Summary
    summary = result.summary
    print(f"\nSummary:")
    print(f"  CoCs in rollup:   {summary.get('total_cocs_rollup', 'N/A')}")
    print(f"  CoCs in measures: {summary.get('total_cocs_measures', 'N/A')}")
    print(f"  Matched CoCs:     {summary.get('matched_cocs', 'N/A')}")
    print(f"  Missing in rollup:   {summary.get('missing_in_rollup', 0)}")
    print(f"  Missing in measures: {summary.get('missing_in_measures', 0)}")

    print(f"\nValidation Results:")
    print(f"  Errors:   {result.error_count}")
    print(f"  Warnings: {result.warning_count}")
    print(f"  OK:       {summary.get('ok_count', 0)}")

    # Delta statistics
    print(f"\nDelta Statistics (matched CoCs):")
    mean_pct = summary.get('mean_abs_pct_delta')
    median_pct = summary.get('median_abs_pct_delta')
    max_pct = summary.get('max_abs_pct_delta')
    if mean_pct is not None:
        print(f"  Mean |pct_delta|:   {mean_pct:.2%}")
        print(f"  Median |pct_delta|: {median_pct:.2%}")
        print(f"  Max |pct_delta|:    {max_pct:.2%}")
    else:
        print("  No valid deltas to compute")

    # Total population comparison
    total_rollup = summary.get('total_rollup_population', 0)
    total_measures = summary.get('total_measures_population', 0)
    if total_measures > 0:
        total_pct_diff = (total_rollup - total_measures) / total_measures
        print(f"\nTotal Population:")
        print(f"  Rollup total:   {total_rollup:,.0f}")
        print(f"  Measures total: {total_measures:,.0f}")
        print(f"  Difference:     {total_rollup - total_measures:+,.0f} ({total_pct_diff:+.2%})")

    # Missing CoCs
    if result.missing_in_rollup:
        print(f"\nCoCs Missing in Rollup ({len(result.missing_in_rollup)}):")
        for coc_id in result.missing_in_rollup[:10]:
            print(f"  - {coc_id}")
        if len(result.missing_in_rollup) > 10:
            print(f"  ... and {len(result.missing_in_rollup) - 10} more")

    if result.missing_in_measures:
        print(f"\nCoCs Missing in Measures ({len(result.missing_in_measures)}):")
        for coc_id in result.missing_in_measures[:10]:
            print(f"  - {coc_id}")
        if len(result.missing_in_measures) > 10:
            print(f"  ... and {len(result.missing_in_measures) - 10} more")

    # Top worst deltas
    df = result.report_df.copy()

    # Filter to rows with valid pct_delta for sorting
    valid_df = df[
        df["rollup_population"].notna() &
        df["measures_population"].notna() &
        (df["pct_delta"] != float("inf"))
    ].copy()

    if len(valid_df) > 0:
        valid_df["abs_pct_delta"] = valid_df["pct_delta"].abs()
        worst = valid_df.nlargest(top_n, "abs_pct_delta")

        print(f"\nTop {min(top_n, len(worst))} Worst Deltas (by |pct_delta|):")
        print("-" * 70)
        print(f"{'CoC ID':<12} {'Rollup':>12} {'Measures':>12} {'Delta':>10} {'Pct':>8} {'Status':<8}")
        print("-" * 70)

        for _, row in worst.iterrows():
            pct_str = f"{row['pct_delta']:+.1%}" if pd.notna(row['pct_delta']) else "N/A"
            delta_str = f"{row['delta']:+,.0f}" if pd.notna(row['delta']) else "N/A"
            rollup_str = f"{row['rollup_population']:,.0f}" if pd.notna(row['rollup_population']) else "N/A"
            measures_str = f"{row['measures_population']:,.0f}" if pd.notna(row['measures_population']) else "N/A"
            print(
                f"{row['coc_id']:<12} {rollup_str:>12} {measures_str:>12} "
                f"{delta_str:>10} {pct_str:>8} {row['status']:<8}"
            )

    # Show all issues if requested
    if show_all_issues:
        issues_df = df[df["status"] != "ok"]
        if len(issues_df) > 0:
            print(f"\nAll Issues ({len(issues_df)} rows):")
            print("-" * 70)
            for _, row in issues_df.iterrows():
                print(f"  [{row['status'].upper()}] {row['coc_id']}: {row['issues']}")

    # Final status
    print("\n" + "=" * 70)
    if result.passed:
        print("PASSED: No errors found")
        return 0
    else:
        print(f"FAILED: {result.error_count} error(s) found")
        return 2
