"""Shared coverage diagnostics helpers."""

from __future__ import annotations

import pandas as pd


def coverage_diagnostics(
    df: pd.DataFrame,
    *,
    geo_id_col: str,
    min_coverage: float,
    coverage_col: str = "coverage_ratio",
    value_col: str | None = None,
    period_col: str | None = None,
    unique_periods: bool = False,
    missing_coverage_is_full: bool = False,
    include_quantiles: bool = True,
    dominance_col: str | None = None,
    dominance_threshold: float | None = None,
) -> pd.DataFrame:
    """Compute coverage diagnostics grouped by geography.

    Returns generic ``periods_total`` and ``periods_covered`` columns so callers
    can rename them to domain-specific labels such as years or months.
    """
    if geo_id_col not in df.columns:
        raise ValueError(f"Missing required column: {geo_id_col}")

    has_coverage = coverage_col in df.columns
    if not has_coverage and not missing_coverage_is_full:
        raise ValueError(f"Missing required column: {coverage_col}")

    rows: list[dict[str, object]] = []
    for geo_id, group in df.groupby(geo_id_col):
        if period_col is not None and period_col in group.columns and unique_periods:
            periods_total = group[period_col].nunique()
        else:
            periods_total = len(group)

        if value_col is not None and value_col in group.columns:
            periods_covered = int(group[value_col].notna().sum())
        elif has_coverage:
            periods_covered = int((group[coverage_col] >= min_coverage).sum())
        else:
            periods_covered = int(periods_total)

        if has_coverage:
            coverage = group[coverage_col]
            coverage_mean = coverage.mean()
            coverage_p10 = coverage.quantile(0.10)
            coverage_p50 = coverage.quantile(0.50)
            coverage_p90 = coverage.quantile(0.90)
        else:
            coverage_mean = 1.0
            coverage_p10 = 1.0
            coverage_p50 = 1.0
            coverage_p90 = 1.0

        row: dict[str, object] = {
            geo_id_col: geo_id,
            "periods_total": periods_total,
            "periods_covered": periods_covered,
            "coverage_ratio_mean": coverage_mean,
            "flag_low_coverage": coverage_mean < min_coverage,
        }

        if include_quantiles:
            row.update(
                {
                    "coverage_ratio_p10": coverage_p10,
                    "coverage_ratio_p50": coverage_p50,
                    "coverage_ratio_p90": coverage_p90,
                }
            )

        if dominance_col is not None and dominance_threshold is not None:
            if dominance_col in group.columns:
                dominance = group[dominance_col].dropna()
                dominance_p90 = dominance.quantile(0.90) if len(dominance) > 0 else None
            else:
                dominance_p90 = None
            row[f"{dominance_col}_p90"] = dominance_p90
            row["flag_high_dominance"] = (
                dominance_p90 is not None and dominance_p90 > dominance_threshold
            )

        rows.append(row)

    return pd.DataFrame(rows).sort_values(geo_id_col).reset_index(drop=True)


def coverage_group_summary(
    df: pd.DataFrame,
    *,
    group_col: str,
    coverage_col: str,
    min_coverage: float,
) -> pd.DataFrame:
    """Summarize coverage distributions grouped by a column such as year."""
    rows: list[dict[str, object]] = []
    for group_id, group in df.groupby(group_col):
        coverage = group[coverage_col]
        rows.append(
            {
                group_col: group_id,
                "count": len(coverage),
                "mean": coverage.mean(),
                "std": coverage.std() if len(coverage) > 1 else 0.0,
                "min": coverage.min(),
                "q25": coverage.quantile(0.25),
                "median": coverage.median(),
                "q75": coverage.quantile(0.75),
                "max": coverage.max(),
                "low_coverage_count": int((coverage < min_coverage).sum()),
            }
        )

    return pd.DataFrame(rows).sort_values(group_col).reset_index(drop=True)
