"""Diagnostics and reporting for CoC-level ZORI data.

This module implements Agent E from the ZORI spec: Diagnostics + Reporting.

It provides:
- Per-CoC diagnostic summaries across a time window (per spec section 4.3)
- Console-friendly text summaries for CLI output
- CLI integration function for the zori-diagnostics command

Diagnostics Output Schema (per spec section 4.3):
------------------------------------------------
- coc_id: CoC identifier
- months_total: Total number of months in the time window
- months_covered: Number of months with valid ZORI (coverage >= threshold)
- coverage_ratio_mean: Mean coverage ratio across all months
- coverage_ratio_p10: 10th percentile coverage ratio
- coverage_ratio_p50: Median coverage ratio
- coverage_ratio_p90: 90th percentile coverage ratio
- max_geo_contribution_p90: 90th percentile of max geo contribution
- flag_low_coverage: True if mean coverage < threshold
- flag_high_dominance: True if single county dominates

Usage
-----
    from coclab.rents.diagnostics import summarize_coc_zori

    # Generate diagnostics from a DataFrame or file path
    text_summary, diagnostics_df = summarize_coc_zori(
        coc_zori_df_or_path="data/curated/rents/coc_zori__county__b2025__c2023__acs2019-2023__wrenter_households.parquet",
        min_coverage=0.90,
        dominance_threshold=0.80,
    )
    print(text_summary)
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

# Default thresholds (per spec section 10)
DEFAULT_MIN_COVERAGE = 0.90
DEFAULT_DOMINANCE_THRESHOLD = 0.80
DEFAULT_TOP_N = 10


# =============================================================================
# Per-CoC Diagnostic Computation
# =============================================================================


def compute_coc_diagnostics(
    coc_zori_df: pd.DataFrame,
    min_coverage: float = DEFAULT_MIN_COVERAGE,
    dominance_threshold: float = DEFAULT_DOMINANCE_THRESHOLD,
) -> pd.DataFrame:
    """Compute per-CoC diagnostic summary across a time window.

    Implements the diagnostics output schema from spec section 4.3.

    Parameters
    ----------
    coc_zori_df : pd.DataFrame
        CoC-level ZORI data with columns:
        - coc_id: CoC identifier
        - date: month start date
        - zori_coc: aggregated ZORI value
        - coverage_ratio: coverage ratio for the month
        - max_geo_contribution: dominance of largest contributor
    min_coverage : float
        Threshold below which coverage is flagged as low. Default 0.90.
    dominance_threshold : float
        Threshold above which single-county dominance is flagged. Default 0.80.

    Returns
    -------
    pd.DataFrame
        Per-CoC diagnostics with columns:
        - coc_id
        - months_total
        - months_covered
        - coverage_ratio_mean
        - coverage_ratio_p10
        - coverage_ratio_p50
        - coverage_ratio_p90
        - max_geo_contribution_p90
        - flag_low_coverage
        - flag_high_dominance
    """
    required_cols = {"coc_id", "date", "coverage_ratio"}
    missing_cols = required_cols - set(coc_zori_df.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    results = []

    for coc_id, group in coc_zori_df.groupby("coc_id"):
        months_total = len(group)

        # Count months with valid ZORI (zori_coc not null)
        if "zori_coc" in group.columns:
            months_covered = int(group["zori_coc"].notna().sum())
        else:
            # Fallback: count months with coverage >= threshold
            months_covered = int((group["coverage_ratio"] >= min_coverage).sum())

        # Coverage ratio statistics
        coverage_ratios = group["coverage_ratio"]
        coverage_mean = coverage_ratios.mean()
        coverage_p10 = coverage_ratios.quantile(0.10)
        coverage_p50 = coverage_ratios.quantile(0.50)
        coverage_p90 = coverage_ratios.quantile(0.90)

        # Max geo contribution statistics
        if "max_geo_contribution" in group.columns:
            max_contributions = group["max_geo_contribution"].dropna()
            if len(max_contributions) > 0:
                max_geo_p90 = max_contributions.quantile(0.90)
            else:
                max_geo_p90 = None
        else:
            max_geo_p90 = None

        # Compute flags
        flag_low_coverage = coverage_mean < min_coverage
        flag_high_dominance = (
            max_geo_p90 is not None and max_geo_p90 > dominance_threshold
        )

        results.append({
            "coc_id": coc_id,
            "months_total": months_total,
            "months_covered": months_covered,
            "coverage_ratio_mean": coverage_mean,
            "coverage_ratio_p10": coverage_p10,
            "coverage_ratio_p50": coverage_p50,
            "coverage_ratio_p90": coverage_p90,
            "max_geo_contribution_p90": max_geo_p90,
            "flag_low_coverage": flag_low_coverage,
            "flag_high_dominance": flag_high_dominance,
        })

    result_df = pd.DataFrame(results)
    result_df = result_df.sort_values("coc_id").reset_index(drop=True)

    return result_df


# =============================================================================
# Text Summary Generation
# =============================================================================


def generate_text_summary(
    coc_zori_df: pd.DataFrame,
    diagnostics_df: pd.DataFrame,
    top_n: int = DEFAULT_TOP_N,
    min_coverage: float = DEFAULT_MIN_COVERAGE,
    dominance_threshold: float = DEFAULT_DOMINANCE_THRESHOLD,
) -> str:
    """Generate a console-friendly text summary of ZORI diagnostics.

    Parameters
    ----------
    coc_zori_df : pd.DataFrame
        CoC-level ZORI data (for date range and CoC counts).
    diagnostics_df : pd.DataFrame
        Per-CoC diagnostics from compute_coc_diagnostics.
    top_n : int
        Number of worst-coverage CoCs to display. Default 10.
    min_coverage : float
        Minimum coverage threshold used for flagging.
    dominance_threshold : float
        Dominance threshold used for flagging.

    Returns
    -------
    str
        Multi-line text summary suitable for console output.
    """
    lines = []
    lines.append("=" * 70)
    lines.append("COC ZORI DIAGNOSTICS SUMMARY")
    lines.append("=" * 70)
    lines.append("")

    # Date range and overall counts
    n_cocs = len(diagnostics_df)
    n_months = diagnostics_df["months_total"].iloc[0] if len(diagnostics_df) > 0 else 0

    lines.append("OVERVIEW")
    lines.append("-" * 50)
    lines.append(f"  Total CoCs:          {n_cocs}")
    lines.append(f"  Months per CoC:      {n_months}")

    if "date" in coc_zori_df.columns:
        date_min = coc_zori_df["date"].min()
        date_max = coc_zori_df["date"].max()
        lines.append(f"  Date range:          {date_min} to {date_max}")

    lines.append(f"  Coverage threshold:  {min_coverage:.0%}")
    lines.append(f"  Dominance threshold: {dominance_threshold:.0%}")
    lines.append("")

    # Overall coverage statistics
    lines.append("COVERAGE STATISTICS")
    lines.append("-" * 50)

    cov_mean = diagnostics_df["coverage_ratio_mean"]
    lines.append(f"  Mean coverage (across all CoCs):   {cov_mean.mean():.3f}")
    lines.append(f"  Median coverage:                   {cov_mean.median():.3f}")
    lines.append(f"  Min coverage:                      {cov_mean.min():.3f}")
    lines.append(f"  Max coverage:                      {cov_mean.max():.3f}")
    lines.append("")

    # Count CoCs by coverage level
    full_coverage = (cov_mean >= 0.99).sum()
    lines.append(
        f"  CoCs with >= 99% coverage:   {full_coverage} "
        f"({100 * full_coverage / n_cocs:.1f}%)"
    )

    good_coverage = (cov_mean >= min_coverage).sum()
    lines.append(
        f"  CoCs with >= {min_coverage:.0%} coverage:  {good_coverage} "
        f"({100 * good_coverage / n_cocs:.1f}%)"
    )

    low_coverage = (cov_mean < min_coverage).sum()
    lines.append(
        f"  CoCs with < {min_coverage:.0%} coverage:   {low_coverage} "
        f"({100 * low_coverage / n_cocs:.1f}%)"
    )
    lines.append("")

    # Months covered statistics
    lines.append("MONTHS COVERED")
    lines.append("-" * 50)

    months_covered = diagnostics_df["months_covered"]
    lines.append(f"  Mean months covered:   {months_covered.mean():.1f} / {n_months}")
    lines.append(f"  Median months covered: {months_covered.median():.1f} / {n_months}")
    lines.append(f"  Min months covered:    {months_covered.min()} / {n_months}")
    lines.append(f"  Max months covered:    {months_covered.max()} / {n_months}")

    # CoCs with no valid months
    no_valid_months = (months_covered == 0).sum()
    if no_valid_months > 0:
        lines.append(f"  CoCs with zero valid months: {no_valid_months}")
    lines.append("")

    # Dominance statistics
    if "max_geo_contribution_p90" in diagnostics_df.columns:
        max_geo = diagnostics_df["max_geo_contribution_p90"].dropna()
        if len(max_geo) > 0:
            lines.append("DOMINANCE STATISTICS (max single-county contribution)")
            lines.append("-" * 50)
            lines.append(f"  Mean (p90 across time):    {max_geo.mean():.3f}")
            lines.append(f"  Median (p90 across time):  {max_geo.median():.3f}")
            lines.append(f"  Max (p90 across time):     {max_geo.max():.3f}")

            high_dominance = (max_geo > dominance_threshold).sum()
            lines.append(
                f"  CoCs with > {dominance_threshold:.0%} dominance:  {high_dominance} "
                f"({100 * high_dominance / n_cocs:.1f}%)"
            )
            lines.append("")

    # Flagged CoCs summary
    flagged_low = diagnostics_df["flag_low_coverage"].sum()
    flagged_high = diagnostics_df["flag_high_dominance"].sum()
    flagged_either = (
        diagnostics_df["flag_low_coverage"] | diagnostics_df["flag_high_dominance"]
    ).sum()

    lines.append("FLAGGED CoCs")
    lines.append("-" * 50)
    lines.append(f"  Low coverage (< {min_coverage:.0%}):         {flagged_low}")
    lines.append(f"  High dominance (> {dominance_threshold:.0%}):      {flagged_high}")
    lines.append(f"  Either flag:                  {flagged_either}")
    lines.append("")

    # Top N worst coverage CoCs
    lines.append(f"TOP {top_n} WORST COVERAGE CoCs")
    lines.append("-" * 50)

    worst_coverage = diagnostics_df.nsmallest(top_n, "coverage_ratio_mean")

    for _, row in worst_coverage.iterrows():
        flags = []
        if row["flag_low_coverage"]:
            flags.append("LOW_COV")
        if row["flag_high_dominance"]:
            flags.append("HIGH_DOM")
        flag_str = f" [{', '.join(flags)}]" if flags else ""

        lines.append(
            f"  {row['coc_id']}: "
            f"cov={row['coverage_ratio_mean']:.3f}, "
            f"months={row['months_covered']}/{row['months_total']}"
            f"{flag_str}"
        )

    lines.append("")
    lines.append("=" * 70)

    return "\n".join(lines)


# =============================================================================
# Main Summarize Function
# =============================================================================


def summarize_coc_zori(
    coc_zori_df_or_path: pd.DataFrame | Path | str,
    min_coverage: float = DEFAULT_MIN_COVERAGE,
    dominance_threshold: float = DEFAULT_DOMINANCE_THRESHOLD,
    top_n: int = DEFAULT_TOP_N,
) -> tuple[str, pd.DataFrame]:
    """Generate diagnostics summary for CoC-level ZORI data.

    This is the main entry point for the diagnostics module. It accepts
    either a DataFrame or a path to a parquet file, computes per-CoC
    diagnostics, and returns both a human-readable text summary and
    the detailed diagnostics DataFrame.

    Parameters
    ----------
    coc_zori_df_or_path : pd.DataFrame, Path, or str
        CoC-level ZORI data. Can be:
        - A pandas DataFrame with the CoC ZORI schema
        - A Path or string path to a parquet file
    min_coverage : float
        Minimum coverage ratio threshold. CoCs with mean coverage below
        this threshold are flagged. Default 0.90.
    dominance_threshold : float
        Maximum single-county contribution threshold. CoCs where one
        county dominates above this threshold are flagged. Default 0.80.
    top_n : int
        Number of worst-coverage CoCs to display in the summary. Default 10.

    Returns
    -------
    tuple[str, pd.DataFrame]
        - text_summary: Console-friendly multi-line summary string
        - diagnostics_df: Per-CoC diagnostics DataFrame

    Raises
    ------
    FileNotFoundError
        If a path is provided and the file does not exist.
    ValueError
        If required columns are missing from the data.

    Examples
    --------
    >>> from coclab.rents.diagnostics import summarize_coc_zori
    >>> path = "data/curated/rents/coc_zori__county__b2025.parquet"
    >>> text, df = summarize_coc_zori(path)
    >>> print(text)
    """
    # Load data if path is provided
    if isinstance(coc_zori_df_or_path, (str, Path)):
        path = Path(coc_zori_df_or_path)
        if not path.exists():
            raise FileNotFoundError(f"CoC ZORI file not found: {path}")

        logger.info(f"Loading CoC ZORI data from {path}")
        coc_zori_df = pd.read_parquet(path)
    else:
        coc_zori_df = coc_zori_df_or_path

    # Validate required columns
    required_cols = {"coc_id", "date", "coverage_ratio"}
    missing_cols = required_cols - set(coc_zori_df.columns)
    if missing_cols:
        raise ValueError(
            f"Missing required columns in CoC ZORI data: {missing_cols}. "
            f"Expected columns: {required_cols}"
        )

    # Add max_geo_contribution if missing (for backwards compatibility)
    if "max_geo_contribution" not in coc_zori_df.columns:
        coc_zori_df["max_geo_contribution"] = None

    logger.info(
        f"Computing diagnostics for {coc_zori_df['coc_id'].nunique()} CoCs "
        f"across {coc_zori_df['date'].nunique()} months"
    )

    # Compute per-CoC diagnostics
    diagnostics_df = compute_coc_diagnostics(
        coc_zori_df,
        min_coverage=min_coverage,
        dominance_threshold=dominance_threshold,
    )

    # Generate text summary
    text_summary = generate_text_summary(
        coc_zori_df,
        diagnostics_df,
        top_n=top_n,
        min_coverage=min_coverage,
        dominance_threshold=dominance_threshold,
    )

    return text_summary, diagnostics_df


# =============================================================================
# CLI Integration
# =============================================================================


def run_zori_diagnostics(
    coc_zori_path: Path | str,
    output_path: Path | str | None = None,
    min_coverage: float = DEFAULT_MIN_COVERAGE,
    dominance_threshold: float = DEFAULT_DOMINANCE_THRESHOLD,
    top_n: int = DEFAULT_TOP_N,
) -> tuple[str, pd.DataFrame]:
    """Run ZORI diagnostics from a file path (CLI integration point).

    This function provides a clean interface for CLI commands. It loads
    the CoC ZORI data, computes diagnostics, optionally saves the
    diagnostics DataFrame, and returns the results.

    Parameters
    ----------
    coc_zori_path : Path or str
        Path to CoC ZORI parquet file.
    output_path : Path, str, or None
        Optional path to save diagnostics DataFrame (as CSV or Parquet).
        File extension determines format (.csv or .parquet).
    min_coverage : float
        Minimum coverage ratio threshold. Default 0.90.
    dominance_threshold : float
        Maximum single-county contribution threshold. Default 0.80.
    top_n : int
        Number of worst-coverage CoCs to display. Default 10.

    Returns
    -------
    tuple[str, pd.DataFrame]
        - text_summary: Console-friendly summary string
        - diagnostics_df: Per-CoC diagnostics DataFrame

    Raises
    ------
    FileNotFoundError
        If the input file does not exist.
    """
    path = Path(coc_zori_path)
    if not path.exists():
        raise FileNotFoundError(f"CoC ZORI file not found: {path}")

    # Run diagnostics
    text_summary, diagnostics_df = summarize_coc_zori(
        path,
        min_coverage=min_coverage,
        dominance_threshold=dominance_threshold,
        top_n=top_n,
    )

    # Save output if requested
    if output_path is not None:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if output_path.suffix == ".parquet":
            diagnostics_df.to_parquet(output_path, index=False)
            logger.info(f"Saved diagnostics to {output_path}")
        else:
            # Default to CSV for .csv or any other extension
            diagnostics_df.to_csv(output_path, index=False)
            logger.info(f"Saved diagnostics to {output_path}")

    return text_summary, diagnostics_df


# =============================================================================
# Identify Problem CoCs
# =============================================================================


def identify_problem_cocs(
    diagnostics_df: pd.DataFrame,
    min_coverage: float = DEFAULT_MIN_COVERAGE,
    dominance_threshold: float = DEFAULT_DOMINANCE_THRESHOLD,
) -> pd.DataFrame:
    """Identify CoCs with potential data quality issues.

    Returns a subset of the diagnostics DataFrame containing only CoCs
    that have been flagged for low coverage or high dominance.

    Parameters
    ----------
    diagnostics_df : pd.DataFrame
        Per-CoC diagnostics from compute_coc_diagnostics.
    min_coverage : float
        Minimum coverage threshold. Default 0.90.
    dominance_threshold : float
        Dominance threshold. Default 0.80.

    Returns
    -------
    pd.DataFrame
        Subset of diagnostics for flagged CoCs with an 'issues' column
        describing the problems.
    """
    # Filter to flagged CoCs
    flagged = diagnostics_df[
        diagnostics_df["flag_low_coverage"] | diagnostics_df["flag_high_dominance"]
    ].copy()

    if flagged.empty:
        return pd.DataFrame(columns=["coc_id", "issues"] + list(diagnostics_df.columns))

    # Build issues string
    def build_issues(row):
        issues = []
        if row["flag_low_coverage"]:
            issues.append(f"low_coverage ({row['coverage_ratio_mean']:.3f})")
        if row["flag_high_dominance"]:
            max_geo = row.get("max_geo_contribution_p90")
            if max_geo is not None:
                issues.append(f"high_dominance ({max_geo:.3f})")
            else:
                issues.append("high_dominance")
        return "; ".join(issues)

    flagged["issues"] = flagged.apply(build_issues, axis=1)

    # Reorder columns to put issues near the front
    cols = ["coc_id", "issues"] + [
        c for c in flagged.columns if c not in ["coc_id", "issues"]
    ]
    flagged = flagged[cols]

    return flagged.sort_values("coverage_ratio_mean").reset_index(drop=True)
