"""Panel diagnostics and sensitivity checks for CoC Lab Phase 3.

This module provides diagnostic functions to validate panel integrity before
modeling. These diagnostics help identify data quality issues, coverage gaps,
and sensitivity to methodological choices.

Diagnostics
-----------
1. Coverage ratio distribution over time
2. Boundary change flags by CoC/year
3. PIT rate sensitivity to weighting method
4. Missingness summaries

Usage
-----
    from coclab.panel.diagnostics import generate_diagnostics_report

    # Run all diagnostics on a panel
    report = generate_diagnostics_report(panel_df)

    # Get text summary for CLI
    print(report.summary())

    # Export to CSV files
    report.to_csv(output_dir)

    # Serialize for storage
    data = report.to_dict()
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from coclab.analysis_geo import infer_geo_type, resolve_geo_col

logger = logging.getLogger(__name__)


def coverage_summary(panel_df: pd.DataFrame) -> pd.DataFrame:
    """Compute coverage ratio statistics by year.

    Analyzes how well ACS measures cover each CoC over years by computing
    descriptive statistics of the coverage_ratio column.

    Parameters
    ----------
    panel_df : pd.DataFrame
        Panel DataFrame with columns: year, coverage_ratio.

    Returns
    -------
    pd.DataFrame
        Summary statistics with columns:
        - year: PIT year
        - count: Number of observations
        - mean: Mean coverage ratio
        - std: Standard deviation
        - min: Minimum coverage ratio
        - q25: 25th percentile
        - median: Median coverage ratio
        - q75: 75th percentile
        - max: Maximum coverage ratio
        - low_coverage_count: Number of CoCs with coverage < 0.9

    Notes
    -----
    If the panel is empty or lacks the required columns, returns an empty
    DataFrame with the expected columns.
    """
    required_cols = {"year", "coverage_ratio"}
    if panel_df is None or panel_df.empty:
        logger.warning("Empty panel provided to coverage_summary")
        return pd.DataFrame(
            columns=[
                "year",
                "count",
                "mean",
                "std",
                "min",
                "q25",
                "median",
                "q75",
                "max",
                "low_coverage_count",
            ]
        )

    if not required_cols.issubset(panel_df.columns):
        missing = required_cols - set(panel_df.columns)
        logger.debug(f"Skipping coverage_summary (columns not present: {missing})")
        return pd.DataFrame(
            columns=[
                "year",
                "count",
                "mean",
                "std",
                "min",
                "q25",
                "median",
                "q75",
                "max",
                "low_coverage_count",
            ]
        )

    # Filter to rows with non-null coverage_ratio
    df = panel_df[panel_df["coverage_ratio"].notna()].copy()

    if df.empty:
        logger.warning("No non-null coverage_ratio values in panel")
        return pd.DataFrame(
            columns=[
                "year",
                "count",
                "mean",
                "std",
                "min",
                "q25",
                "median",
                "q75",
                "max",
                "low_coverage_count",
            ]
        )

    # Compute statistics by year
    results = []
    for year, group in df.groupby("year"):
        coverage = group["coverage_ratio"]
        results.append(
            {
                "year": year,
                "count": len(coverage),
                "mean": coverage.mean(),
                "std": coverage.std() if len(coverage) > 1 else 0.0,
                "min": coverage.min(),
                "q25": coverage.quantile(0.25),
                "median": coverage.median(),
                "q75": coverage.quantile(0.75),
                "max": coverage.max(),
                "low_coverage_count": int((coverage < 0.9).sum()),
            }
        )

    result_df = pd.DataFrame(results)
    result_df = result_df.sort_values("year").reset_index(drop=True)

    return result_df


def boundary_change_summary(panel_df: pd.DataFrame) -> pd.DataFrame:
    """Summarize which geography units had vintage changes and when.

    Identifies CoCs that experienced boundary vintage changes between
    consecutive years, which may affect time-series comparability.

    Parameters
    ----------
    panel_df : pd.DataFrame
        Panel DataFrame with columns: coc_id, year, boundary_changed.

    Returns
    -------
    pd.DataFrame
        Summary with columns:
        - coc_id: CoC identifier
        - change_years: List of years when boundary changed
        - change_count: Total number of boundary changes

    Notes
    -----
    If the panel is empty or lacks the required columns, returns an empty
    DataFrame with the expected columns.
    """
    required_cols = {"year", "boundary_changed"}
    geo_col = None
    if panel_df is None or panel_df.empty:
        logger.warning("Empty panel provided to boundary_change_summary")
        return pd.DataFrame(columns=["coc_id", "change_years", "change_count"])

    if not required_cols.issubset(panel_df.columns):
        missing = required_cols - set(panel_df.columns)
        logger.warning(f"Missing columns for boundary_change_summary: {missing}")
        return pd.DataFrame(columns=["coc_id", "change_years", "change_count"])
    try:
        geo_col = resolve_geo_col(panel_df)
    except KeyError:
        logger.warning("Missing geography identifier column for boundary_change_summary")
        return pd.DataFrame(columns=["coc_id", "change_years", "change_count"])

    # Filter to rows where boundary changed
    changes = panel_df[panel_df["boundary_changed"]].copy()

    if changes.empty:
        logger.info("No boundary changes found in panel")
        return pd.DataFrame(columns=[geo_col, "change_years", "change_count"])

    # Group by CoC and collect change years
    results = []
    for geo_id, group in changes.groupby(geo_col):
        years = sorted(group["year"].dropna().unique().tolist())
        results.append(
            {
                geo_col: geo_id,
                "change_years": years,
                "change_count": len(years),
            }
        )

    result_df = pd.DataFrame(results)
    result_df = result_df.sort_values(geo_col).reset_index(drop=True)

    return result_df


def weighting_sensitivity(
    panel_df_area: pd.DataFrame,
    panel_df_pop: pd.DataFrame,
) -> pd.DataFrame:
    """Compare PIT rates under different weighting methods.

    Calculates the PIT rate (pit_total / total_population) for each CoC/year
    under area-weighted vs population-weighted ACS measures, allowing
    assessment of sensitivity to the weighting method choice.

    Parameters
    ----------
    panel_df_area : pd.DataFrame
        Panel DataFrame built with area weighting.
    panel_df_pop : pd.DataFrame
        Panel DataFrame built with population weighting.

    Returns
    -------
    pd.DataFrame
        Comparison with columns:
        - coc_id: CoC identifier
        - year: PIT year
        - pit_total: Total homeless count
        - pop_area: Total population (area-weighted)
        - pop_pop: Total population (population-weighted)
        - rate_area: PIT rate per 10k (area-weighted)
        - rate_pop: PIT rate per 10k (population-weighted)
        - rate_diff: Absolute difference in rates
        - rate_pct_diff: Percentage difference in rates

    Raises
    ------
    ValueError
        If panels have different CoC/year combinations.

    Notes
    -----
    PIT rates are calculated as (pit_total / total_population) * 10000
    to express rates per 10,000 population.
    """
    required_cols = {"year", "pit_total", "total_population"}
    geo_col_area = None
    geo_col_pop = None

    for name, df in [("area", panel_df_area), ("pop", panel_df_pop)]:
        if df is None or df.empty:
            logger.warning(f"Empty {name} panel provided to weighting_sensitivity")
            return pd.DataFrame(
                columns=[
                    "coc_id",
                    "year",
                    "pit_total",
                    "pop_area",
                    "pop_pop",
                    "rate_area",
                    "rate_pop",
                    "rate_diff",
                    "rate_pct_diff",
                ]
            )
        if not required_cols.issubset(df.columns):
            missing = required_cols - set(df.columns)
            logger.warning(f"Missing columns in {name} panel: {missing}")
            return pd.DataFrame(
                columns=[
                    "geo_id",
                    "year",
                    "pit_total",
                    "pop_area",
                    "pop_pop",
                    "rate_area",
                    "rate_pop",
                    "rate_diff",
                    "rate_pct_diff",
                ]
            )
    try:
        geo_col_area = resolve_geo_col(panel_df_area)
        geo_col_pop = resolve_geo_col(panel_df_pop)
    except KeyError:
        logger.warning("Missing geography identifier column in weighting_sensitivity input")
        return pd.DataFrame(
            columns=[
                "geo_id",
                "year",
                "pit_total",
                "pop_area",
                "pop_pop",
                "rate_area",
                "rate_pop",
                "rate_diff",
                "rate_pct_diff",
            ]
        )

    if geo_col_area != geo_col_pop:
        logger.warning(
            "weighting_sensitivity requires matching geography identifier columns "
            f"but got {geo_col_area!r} and {geo_col_pop!r}"
        )
        return pd.DataFrame(
            columns=[
                "geo_id",
                "year",
                "pit_total",
                "pop_area",
                "pop_pop",
                "rate_area",
                "rate_pop",
                "rate_diff",
                "rate_pct_diff",
            ]
        )

    out_geo_col = geo_col_area

    # Merge on geography identifier and year
    df_area = panel_df_area[[geo_col_area, "year", "pit_total", "total_population"]].copy()
    df_area = df_area.rename(columns={"total_population": "pop_area"})

    df_pop = panel_df_pop[[geo_col_pop, "year", "pit_total", "total_population"]].copy()
    df_pop = df_pop.rename(
        columns={
            "total_population": "pop_pop",
            "pit_total": "pit_total_pop",
        }
    )

    merged = df_area.merge(df_pop, on=[out_geo_col, "year"], how="outer")

    # Verify PIT totals match (they should be the same)
    if not merged.empty:
        mismatch = merged[merged["pit_total"] != merged["pit_total_pop"]]
        if not mismatch.empty:
            logger.warning(
                f"PIT totals differ between panels for {len(mismatch)} geo/year pairs"
            )

    # Drop the redundant pit_total column
    merged = merged.drop(columns=["pit_total_pop"])

    # Calculate rates per 10,000 population
    merged["rate_area"] = (merged["pit_total"] / merged["pop_area"]) * 10000
    merged["rate_pop"] = (merged["pit_total"] / merged["pop_pop"]) * 10000

    # Calculate differences
    merged["rate_diff"] = (merged["rate_area"] - merged["rate_pop"]).abs()

    # Percentage difference relative to the mean of the two rates
    mean_rate = (merged["rate_area"] + merged["rate_pop"]) / 2
    merged["rate_pct_diff"] = (merged["rate_diff"] / mean_rate * 100).fillna(0)

    # Sort and reset index
    merged = merged.sort_values([out_geo_col, "year"]).reset_index(drop=True)

    return merged


def missingness_report(panel_df: pd.DataFrame) -> pd.DataFrame:
    """Report missing data patterns per column per year.

    Provides a comprehensive view of data completeness across the panel,
    identifying which columns have missing values and in which years.

    Parameters
    ----------
    panel_df : pd.DataFrame
        Panel DataFrame with a year column and various data columns.

    Returns
    -------
    pd.DataFrame
        Missingness statistics with columns:
        - column: Column name
        - year: PIT year (or "all" for overall stats)
        - missing_count: Number of missing values
        - total_count: Total number of rows
        - missing_pct: Percentage missing
        - complete_pct: Percentage complete

    Notes
    -----
    The returned DataFrame includes both per-year and overall ("all")
    statistics for each column.
    """
    if panel_df is None or panel_df.empty:
        logger.warning("Empty panel provided to missingness_report")
        return pd.DataFrame(
            columns=[
                "column",
                "year",
                "missing_count",
                "total_count",
                "missing_pct",
                "complete_pct",
            ]
        )

    if "year" not in panel_df.columns:
        logger.warning("Missing 'year' column in panel")
        return pd.DataFrame(
            columns=[
                "column",
                "year",
                "missing_count",
                "total_count",
                "missing_pct",
                "complete_pct",
            ]
        )

    results = []

    # Columns to check (excluding year itself)
    columns_to_check = [col for col in panel_df.columns if col != "year"]

    # Per-year statistics
    for year, group in panel_df.groupby("year"):
        total = len(group)
        for col in columns_to_check:
            missing = int(group[col].isna().sum())
            results.append(
                {
                    "column": col,
                    "year": year,
                    "missing_count": missing,
                    "total_count": total,
                    "missing_pct": (missing / total * 100) if total > 0 else 0.0,
                    "complete_pct": ((total - missing) / total * 100) if total > 0 else 0.0,
                }
            )

    # Overall statistics
    total = len(panel_df)
    for col in columns_to_check:
        missing = int(panel_df[col].isna().sum())
        results.append(
            {
                "column": col,
                "year": "all",
                "missing_count": missing,
                "total_count": total,
                "missing_pct": (missing / total * 100) if total > 0 else 0.0,
                "complete_pct": ((total - missing) / total * 100) if total > 0 else 0.0,
            }
        )

    result_df = pd.DataFrame(results)

    # Sort by column name, then year (with "all" at the end)
    def sort_key(x):
        if x == "all":
            return (1, 9999)
        return (0, x)

    result_df["_sort_key"] = result_df["year"].apply(sort_key)
    result_df = result_df.sort_values(["column", "_sort_key"]).reset_index(drop=True)
    result_df = result_df.drop(columns=["_sort_key"])

    return result_df


@dataclass
class DiagnosticsReport:
    """Container for all panel diagnostic results.

    Holds the results of running all diagnostic checks on a panel,
    providing methods for serialization and export.

    Attributes
    ----------
    coverage : pd.DataFrame
        Coverage ratio summary by year (from coverage_summary).
    boundary_changes : pd.DataFrame
        Boundary change summary by CoC (from boundary_change_summary).
    missingness : pd.DataFrame
        Missingness report by column/year (from missingness_report).
    weighting : pd.DataFrame or None
        Weighting sensitivity analysis (from weighting_sensitivity).
        Only populated if two panels with different weighting were provided.
    panel_info : dict
        Basic information about the panel (row count, year range, etc.).
    """

    coverage: pd.DataFrame = field(default_factory=pd.DataFrame)
    boundary_changes: pd.DataFrame = field(default_factory=pd.DataFrame)
    missingness: pd.DataFrame = field(default_factory=pd.DataFrame)
    weighting: pd.DataFrame | None = None
    panel_info: dict = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize report to dictionary for storage.

        Returns
        -------
        dict
            Dictionary with all diagnostic results as serializable objects.
            DataFrames are converted to list of dicts format.
        """
        return {
            "coverage": self.coverage.to_dict(orient="records"),
            "boundary_changes": self.boundary_changes.to_dict(orient="records"),
            "missingness": self.missingness.to_dict(orient="records"),
            "weighting": (
                self.weighting.to_dict(orient="records") if self.weighting is not None else None
            ),
            "panel_info": self.panel_info,
        }

    def to_csv(self, output_dir: Path | str) -> dict[str, Path]:
        """Export individual diagnostics to CSV files.

        Parameters
        ----------
        output_dir : Path or str
            Directory to write CSV files.

        Returns
        -------
        dict[str, Path]
            Mapping of diagnostic name to output file path.
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        paths = {}

        # Coverage summary
        if not self.coverage.empty:
            coverage_path = output_dir / "coverage_summary.csv"
            self.coverage.to_csv(coverage_path, index=False)
            paths["coverage"] = coverage_path
            logger.info(f"Wrote coverage summary to {coverage_path}")

        # Boundary changes
        if not self.boundary_changes.empty:
            # Convert change_years list to string for CSV
            boundary_df = self.boundary_changes.copy()
            if "change_years" in boundary_df.columns:
                boundary_df["change_years"] = boundary_df["change_years"].apply(
                    lambda x: ",".join(map(str, x)) if isinstance(x, list) else str(x)
                )
            boundary_path = output_dir / "boundary_changes.csv"
            boundary_df.to_csv(boundary_path, index=False)
            paths["boundary_changes"] = boundary_path
            logger.info(f"Wrote boundary changes to {boundary_path}")

        # Missingness report
        if not self.missingness.empty:
            missingness_path = output_dir / "missingness_report.csv"
            self.missingness.to_csv(missingness_path, index=False)
            paths["missingness"] = missingness_path
            logger.info(f"Wrote missingness report to {missingness_path}")

        # Weighting sensitivity
        if self.weighting is not None and not self.weighting.empty:
            weighting_path = output_dir / "weighting_sensitivity.csv"
            self.weighting.to_csv(weighting_path, index=False)
            paths["weighting"] = weighting_path
            logger.info(f"Wrote weighting sensitivity to {weighting_path}")

        return paths

    def summary(self) -> str:
        """Generate CLI-readable text summary.

        Returns
        -------
        str
            Human-readable summary of all diagnostics.
        """
        lines = ["=" * 60]
        lines.append("PANEL DIAGNOSTICS REPORT")
        lines.append("=" * 60)

        # Panel info
        if self.panel_info:
            lines.append("")
            lines.append("PANEL INFO")
            lines.append("-" * 40)
            for key, value in self.panel_info.items():
                lines.append(f"  {key}: {value}")

        # Coverage summary
        lines.append("")
        lines.append("COVERAGE SUMMARY")
        lines.append("-" * 40)
        if self.coverage.empty:
            lines.append("  No coverage data available")
        else:
            lines.append(f"  Years analyzed: {len(self.coverage)}")
            mean_coverage = self.coverage["mean"].mean()
            lines.append(f"  Overall mean coverage: {mean_coverage:.3f}")
            total_low = self.coverage["low_coverage_count"].sum()
            lines.append(f"  Total low coverage observations (<0.9): {total_low}")

        # Boundary changes
        lines.append("")
        lines.append("BOUNDARY CHANGES")
        lines.append("-" * 40)
        if self.boundary_changes.empty:
            lines.append("  No boundary changes detected")
        else:
            total_cocs = len(self.boundary_changes)
            total_changes = self.boundary_changes["change_count"].sum()
            lines.append(f"  Geo units with boundary changes: {total_cocs}")
            lines.append(f"  Total boundary change events: {total_changes}")

        # Missingness
        lines.append("")
        lines.append("MISSINGNESS")
        lines.append("-" * 40)
        if self.missingness.empty:
            lines.append("  No missingness data available")
        else:
            # Get overall stats
            overall = self.missingness[self.missingness["year"] == "all"]
            if not overall.empty:
                cols_with_missing = overall[overall["missing_count"] > 0]
                if cols_with_missing.empty:
                    lines.append("  No missing data detected")
                else:
                    lines.append(f"  Columns with missing data: {len(cols_with_missing)}")
                    for _, row in cols_with_missing.iterrows():
                        lines.append(
                            f"    {row['column']}: {row['missing_count']} missing "
                            f"({row['missing_pct']:.1f}%)"
                        )

        # Weighting sensitivity
        lines.append("")
        lines.append("WEIGHTING SENSITIVITY")
        lines.append("-" * 40)
        if self.weighting is None or self.weighting.empty:
            lines.append("  Weighting sensitivity not analyzed")
            lines.append("  (requires two panels with different weighting methods)")
        else:
            mean_diff = self.weighting["rate_pct_diff"].mean()
            max_diff = self.weighting["rate_pct_diff"].max()
            lines.append(f"  Mean rate difference: {mean_diff:.2f}%")
            lines.append(f"  Max rate difference: {max_diff:.2f}%")

            # Find CoC with largest difference
            if max_diff > 0:
                max_row = self.weighting.loc[self.weighting["rate_pct_diff"].idxmax()]
                weight_geo_col = resolve_geo_col(self.weighting)
                lines.append(
                    f"  Largest difference: {max_row[weight_geo_col]} in {max_row['year']}"
                )

        lines.append("")
        lines.append("=" * 60)

        return "\n".join(lines)


def generate_diagnostics_report(
    panel_df: pd.DataFrame,
    panel_df_alt: pd.DataFrame | None = None,
) -> DiagnosticsReport:
    """Generate a complete diagnostics report for a panel.

    Main entry point for running all panel diagnostics. Runs coverage
    summary, boundary change detection, missingness analysis, and
    optionally weighting sensitivity if an alternative panel is provided.

    Parameters
    ----------
    panel_df : pd.DataFrame
        Primary panel DataFrame to analyze.
    panel_df_alt : pd.DataFrame, optional
        Alternative panel built with a different weighting method.
        If provided, weighting sensitivity analysis will be performed.

    Returns
    -------
    DiagnosticsReport
        Complete diagnostics report with all results.

    Examples
    --------
    >>> # Basic usage with single panel
    >>> report = generate_diagnostics_report(panel_df)
    >>> print(report.summary())

    >>> # With weighting sensitivity
    >>> panel_area = build_panel(2020, 2024, policy=area_policy)
    >>> panel_pop = build_panel(2020, 2024, policy=pop_policy)
    >>> report = generate_diagnostics_report(panel_area, panel_pop)
    """
    logger.info("Generating panel diagnostics report")

    # Collect panel info
    panel_info = {}
    if panel_df is not None and not panel_df.empty:
        panel_info["row_count"] = len(panel_df)
        try:
            geo_col = resolve_geo_col(panel_df)
            panel_info["geo_count"] = int(panel_df[geo_col].nunique())
            if "coc_id" in panel_df.columns:
                panel_info["coc_count"] = int(panel_df["coc_id"].nunique())
            panel_info["geo_type"] = infer_geo_type(panel_df)
        except (KeyError, ValueError):
            panel_info["geo_count"] = 0
        panel_info["year_count"] = (
            int(panel_df["year"].nunique()) if "year" in panel_df.columns else 0
        )
        if "year" in panel_df.columns:
            panel_info["year_min"] = int(panel_df["year"].min())
            panel_info["year_max"] = int(panel_df["year"].max())
        if "weighting_method" in panel_df.columns:
            methods = panel_df["weighting_method"].unique().tolist()
            panel_info["weighting_methods"] = methods

    # Run diagnostics
    logger.debug("Running coverage summary")
    coverage = coverage_summary(panel_df)

    logger.debug("Running boundary change summary")
    boundary_changes = boundary_change_summary(panel_df)

    logger.debug("Running missingness report")
    missingness = missingness_report(panel_df)

    # Weighting sensitivity (if alternative panel provided)
    weighting = None
    if panel_df_alt is not None:
        logger.debug("Running weighting sensitivity analysis")
        weighting = weighting_sensitivity(panel_df, panel_df_alt)

    report = DiagnosticsReport(
        coverage=coverage,
        boundary_changes=boundary_changes,
        missingness=missingness,
        weighting=weighting,
        panel_info=panel_info,
    )

    logger.info("Diagnostics report generated successfully")

    return report
