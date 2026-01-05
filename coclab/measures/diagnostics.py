"""Attribution diagnostics and coverage reporting for CoC crosswalks.

Computes per-CoC diagnostics to validate crosswalk quality and compare
area-weighted vs population-weighted estimates.
"""

import pandas as pd


def compute_crosswalk_diagnostics(crosswalk: pd.DataFrame) -> pd.DataFrame:
    """Compute diagnostics for a tract crosswalk.

    Analyzes crosswalk quality by computing per-CoC metrics including
    tract count, maximum contribution, and coverage ratios.

    Parameters
    ----------
    crosswalk : pd.DataFrame
        Tract-to-CoC crosswalk with columns:
        - coc_id: CoC identifier
        - area_share: Area-weighted share of tract in CoC
        - pop_share: Population-weighted share (optional, may be None)

    Returns
    -------
    pd.DataFrame
        Diagnostics per CoC with columns:
        - coc_id: CoC identifier
        - num_tracts: Number of tracts intersecting this CoC
        - max_tract_contribution: Maximum single-tract area_share
        - coverage_ratio_area: Sum of area_share (should be ~1)
        - coverage_ratio_pop: Sum of pop_share (when available)
    """
    if "coc_id" not in crosswalk.columns:
        raise ValueError("Crosswalk must have 'coc_id' column")
    if "area_share" not in crosswalk.columns:
        raise ValueError("Crosswalk must have 'area_share' column")

    # Group by CoC and compute diagnostics
    grouped = crosswalk.groupby("coc_id")

    diagnostics = pd.DataFrame({
        "num_tracts": grouped.size(),
        "max_tract_contribution": grouped["area_share"].max(),
        "coverage_ratio_area": grouped["area_share"].sum(),
    })
    diagnostics = diagnostics.reset_index()

    # Compute population coverage if pop_share is available and has values
    if "pop_share" in crosswalk.columns:
        # Check if pop_share has any non-null values
        has_pop_share = crosswalk["pop_share"].notna().any()
        if has_pop_share:
            pop_coverage = grouped["pop_share"].sum().reset_index()
            pop_coverage.columns = ["coc_id", "coverage_ratio_pop"]
            diagnostics = diagnostics.merge(pop_coverage, on="coc_id", how="left")
        else:
            diagnostics["coverage_ratio_pop"] = pd.NA
    else:
        diagnostics["coverage_ratio_pop"] = pd.NA

    # Reorder columns for clarity
    col_order = [
        "coc_id",
        "num_tracts",
        "max_tract_contribution",
        "coverage_ratio_area",
        "coverage_ratio_pop",
    ]
    diagnostics = diagnostics[col_order]

    return diagnostics.sort_values("coc_id").reset_index(drop=True)


def compute_measure_diagnostics(
    area_measures: pd.DataFrame,
    pop_measures: pd.DataFrame,
) -> pd.DataFrame:
    """Compare area vs population weighted measures.

    Computes the difference between area-weighted and population-weighted
    estimates for each CoC and measure, helping identify where weighting
    method significantly impacts results.

    Parameters
    ----------
    area_measures : pd.DataFrame
        CoC-level measures computed with area weighting.
        Must have 'coc_id' column.
    pop_measures : pd.DataFrame
        CoC-level measures computed with population weighting.
        Must have 'coc_id' column.

    Returns
    -------
    pd.DataFrame
        Diagnostics per CoC with columns:
        - coc_id: CoC identifier
        - {measure}_area: Area-weighted value
        - {measure}_pop: Population-weighted value
        - {measure}_delta: Difference (area - pop)
        - {measure}_pct_diff: Percentage difference
    """
    if "coc_id" not in area_measures.columns:
        raise ValueError("area_measures must have 'coc_id' column")
    if "coc_id" not in pop_measures.columns:
        raise ValueError("pop_measures must have 'coc_id' column")

    # Identify numeric measure columns (exclude metadata)
    exclude_cols = {
        "coc_id",
        "boundary_vintage",
        "acs_vintage",
        "weighting_method",
        "source",
        "coverage_ratio",
    }

    area_numeric = [
        c for c in area_measures.columns
        if c not in exclude_cols and pd.api.types.is_numeric_dtype(area_measures[c])
    ]
    pop_numeric = [
        c for c in pop_measures.columns
        if c not in exclude_cols and pd.api.types.is_numeric_dtype(pop_measures[c])
    ]

    # Find common measures
    common_measures = set(area_numeric) & set(pop_numeric)

    if not common_measures:
        raise ValueError("No common numeric measures found between area and pop DataFrames")

    # Merge on coc_id
    merged = area_measures[["coc_id"] + list(common_measures)].merge(
        pop_measures[["coc_id"] + list(common_measures)],
        on="coc_id",
        suffixes=("_area", "_pop"),
        how="outer",
    )

    # Compute deltas for each measure
    result_cols = ["coc_id"]

    for measure in sorted(common_measures):
        area_col = f"{measure}_area"
        pop_col = f"{measure}_pop"
        delta_col = f"{measure}_delta"
        pct_diff_col = f"{measure}_pct_diff"

        # Compute absolute difference
        merged[delta_col] = merged[area_col] - merged[pop_col]

        # Compute percentage difference (relative to population-weighted)
        # Avoid division by zero
        merged[pct_diff_col] = (
            merged[delta_col] / merged[pop_col].replace(0, pd.NA) * 100
        )

        result_cols.extend([area_col, pop_col, delta_col, pct_diff_col])

    return merged[result_cols].sort_values("coc_id").reset_index(drop=True)


def summarize_diagnostics(diagnostics: pd.DataFrame) -> str:
    """Generate CLI-readable summary of diagnostics.

    Produces a human-readable text summary of crosswalk diagnostics
    suitable for display in command-line interfaces.

    Parameters
    ----------
    diagnostics : pd.DataFrame
        Diagnostics DataFrame from compute_crosswalk_diagnostics or
        compute_measure_diagnostics.

    Returns
    -------
    str
        Multi-line summary string with key statistics.
    """
    lines = []
    lines.append("=" * 60)
    lines.append("CROSSWALK DIAGNOSTICS SUMMARY")
    lines.append("=" * 60)

    n_cocs = len(diagnostics)
    lines.append(f"Total CoCs: {n_cocs}")
    lines.append("")

    # Check for crosswalk diagnostics columns
    if "num_tracts" in diagnostics.columns:
        lines.append("TRACT COVERAGE:")
        lines.append("-" * 40)

        tract_stats = diagnostics["num_tracts"]
        lines.append(f"  Tracts per CoC (mean):   {tract_stats.mean():.1f}")
        lines.append(f"  Tracts per CoC (median): {tract_stats.median():.1f}")
        lines.append(f"  Tracts per CoC (min):    {tract_stats.min()}")
        lines.append(f"  Tracts per CoC (max):    {tract_stats.max()}")
        lines.append("")

    if "max_tract_contribution" in diagnostics.columns:
        lines.append("MAX SINGLE-TRACT CONTRIBUTION:")
        lines.append("-" * 40)

        max_contrib = diagnostics["max_tract_contribution"]
        lines.append(f"  Mean max contribution:   {max_contrib.mean():.3f}")
        lines.append(f"  Median max contribution: {max_contrib.median():.3f}")

        # Count CoCs where one tract dominates (>50% contribution)
        high_contrib = (max_contrib > 0.5).sum()
        lines.append(f"  CoCs with >50% from single tract: {high_contrib} ({100*high_contrib/n_cocs:.1f}%)")
        lines.append("")

    if "coverage_ratio_area" in diagnostics.columns:
        lines.append("AREA COVERAGE RATIO:")
        lines.append("-" * 40)

        area_cov = diagnostics["coverage_ratio_area"]
        lines.append(f"  Mean:   {area_cov.mean():.4f}")
        lines.append(f"  Median: {area_cov.median():.4f}")
        lines.append(f"  Min:    {area_cov.min():.4f}")
        lines.append(f"  Max:    {area_cov.max():.4f}")

        # Count CoCs with good coverage (0.99 to 1.01)
        good_coverage = ((area_cov >= 0.99) & (area_cov <= 1.01)).sum()
        lines.append(f"  CoCs with coverage 0.99-1.01: {good_coverage} ({100*good_coverage/n_cocs:.1f}%)")
        lines.append("")

    if "coverage_ratio_pop" in diagnostics.columns:
        pop_cov = diagnostics["coverage_ratio_pop"]
        if pop_cov.notna().any():
            lines.append("POPULATION COVERAGE RATIO:")
            lines.append("-" * 40)

            pop_valid = pop_cov.dropna()
            lines.append(f"  Mean:   {pop_valid.mean():.4f}")
            lines.append(f"  Median: {pop_valid.median():.4f}")
            lines.append(f"  Min:    {pop_valid.min():.4f}")
            lines.append(f"  Max:    {pop_valid.max():.4f}")

            # Count missing
            n_missing = pop_cov.isna().sum()
            if n_missing > 0:
                lines.append(f"  CoCs missing pop_share: {n_missing}")
            lines.append("")

    # Check for measure comparison diagnostics
    delta_cols = [c for c in diagnostics.columns if c.endswith("_delta")]
    if delta_cols:
        lines.append("AREA VS POPULATION WEIGHTING DIFFERENCES:")
        lines.append("-" * 40)

        for delta_col in delta_cols:
            measure_name = delta_col.replace("_delta", "")
            delta_vals = diagnostics[delta_col].dropna()

            if len(delta_vals) > 0:
                lines.append(f"  {measure_name}:")
                lines.append(f"    Mean delta:   {delta_vals.mean():,.2f}")
                lines.append(f"    Median delta: {delta_vals.median():,.2f}")
                lines.append(f"    Max |delta|:  {delta_vals.abs().max():,.2f}")

                pct_col = f"{measure_name}_pct_diff"
                if pct_col in diagnostics.columns:
                    pct_vals = diagnostics[pct_col].dropna()
                    if len(pct_vals) > 0:
                        lines.append(f"    Mean pct diff: {pct_vals.mean():.2f}%")
                        # Count significant differences (>10%)
                        sig_diff = (pct_vals.abs() > 10).sum()
                        lines.append(f"    CoCs with >10% diff: {sig_diff}")
                lines.append("")

    lines.append("=" * 60)

    return "\n".join(lines)


def identify_problem_cocs(
    diagnostics: pd.DataFrame,
    coverage_threshold: float = 0.95,
    max_contribution_threshold: float = 0.8,
) -> pd.DataFrame:
    """Identify CoCs with potential attribution problems.

    Flags CoCs that may have data quality issues based on crosswalk
    diagnostics.

    Parameters
    ----------
    diagnostics : pd.DataFrame
        Diagnostics from compute_crosswalk_diagnostics.
    coverage_threshold : float
        CoCs with coverage below this are flagged. Default 0.95.
    max_contribution_threshold : float
        CoCs with max_tract_contribution above this are flagged. Default 0.8.

    Returns
    -------
    pd.DataFrame
        Subset of diagnostics for flagged CoCs with additional 'issues' column.
    """
    issues = []

    for idx, row in diagnostics.iterrows():
        coc_issues = []

        # Check area coverage
        if "coverage_ratio_area" in row and row["coverage_ratio_area"] < coverage_threshold:
            coc_issues.append(f"low_area_coverage ({row['coverage_ratio_area']:.3f})")

        # Check if single tract dominates
        if "max_tract_contribution" in row and row["max_tract_contribution"] > max_contribution_threshold:
            coc_issues.append(f"high_tract_concentration ({row['max_tract_contribution']:.3f})")

        # Check population coverage if available
        if "coverage_ratio_pop" in row and pd.notna(row["coverage_ratio_pop"]):
            if row["coverage_ratio_pop"] < coverage_threshold:
                coc_issues.append(f"low_pop_coverage ({row['coverage_ratio_pop']:.3f})")

        if coc_issues:
            issues.append({
                "coc_id": row["coc_id"],
                "issues": "; ".join(coc_issues),
            })

    if not issues:
        return pd.DataFrame(columns=["coc_id", "issues"])

    problem_cocs = pd.DataFrame(issues)
    problem_cocs = problem_cocs.merge(diagnostics, on="coc_id", how="left")

    # Reorder to put issues first after coc_id
    cols = ["coc_id", "issues"] + [c for c in problem_cocs.columns if c not in ["coc_id", "issues"]]
    return problem_cocs[cols]
