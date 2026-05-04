"""Attribution diagnostics and coverage reporting for crosswalks.

Computes per-geography diagnostics to validate crosswalk quality and compare
area-weighted vs population-weighted estimates.  All functions accept a
``geo_id_col`` parameter (default ``"coc_id"``) so they work with any
analysis geography.
"""

import pandas as pd


def compute_crosswalk_diagnostics(
    crosswalk: pd.DataFrame,
    *,
    geo_id_col: str = "coc_id",
) -> pd.DataFrame:
    """Compute diagnostics for a tract crosswalk.

    Analyzes crosswalk quality by computing per-geography metrics including
    tract count, maximum contribution, and coverage ratios.

    Parameters
    ----------
    crosswalk : pd.DataFrame
        Tract-to-geo crosswalk with columns:
        - ``geo_id_col``: Geography identifier
        - area_share: Area-weighted share of tract in geography unit
        - pop_share: Population-weighted share (optional, may be None)
    geo_id_col : str
        Name of the geography identifier column.  Defaults to ``"coc_id"``.

    Returns
    -------
    pd.DataFrame
        Diagnostics per geography unit with columns:
        - ``geo_id_col``: Geography identifier
        - num_tracts: Number of tracts intersecting this unit
        - max_tract_contribution: Maximum single-tract area_share
        - coverage_ratio_area: Sum of area_share (should be ~1)
        - coverage_ratio_pop: Sum of pop_share (when available)
    """
    if geo_id_col not in crosswalk.columns:
        raise ValueError(f"Crosswalk must have '{geo_id_col}' column")
    if "intersection_area" not in crosswalk.columns:
        raise ValueError("Crosswalk must have 'intersection_area' column")

    # Compute geo-normalized area shares
    xwalk = crosswalk.copy()
    geo_total_area = xwalk.groupby(geo_id_col)["intersection_area"].transform("sum")
    xwalk["geo_area_share"] = xwalk["intersection_area"] / geo_total_area

    # Group by geography unit and compute diagnostics
    grouped = xwalk.groupby(geo_id_col)

    diagnostics = pd.DataFrame(
        {
            "num_tracts": grouped.size(),
            "max_tract_contribution": grouped["geo_area_share"].max(),
            "coverage_ratio_area": grouped["geo_area_share"].sum(),
        }
    )
    diagnostics = diagnostics.reset_index()

    # Compute population coverage if pop_share is available and has values
    if "pop_share" in crosswalk.columns:
        has_pop_share = crosswalk["pop_share"].notna().any()
        if has_pop_share:
            pop_coverage = grouped["pop_share"].sum().reset_index()
            pop_coverage.columns = [geo_id_col, "coverage_ratio_pop"]
            diagnostics = diagnostics.merge(pop_coverage, on=geo_id_col, how="left")
        else:
            diagnostics["coverage_ratio_pop"] = pd.NA
    else:
        diagnostics["coverage_ratio_pop"] = pd.NA

    # Reorder columns for clarity
    col_order = [
        geo_id_col,
        "num_tracts",
        "max_tract_contribution",
        "coverage_ratio_area",
        "coverage_ratio_pop",
    ]
    diagnostics = diagnostics[col_order]

    return diagnostics.sort_values(geo_id_col).reset_index(drop=True)


def compute_measure_diagnostics(
    area_measures: pd.DataFrame,
    pop_measures: pd.DataFrame,
    *,
    geo_id_col: str = "coc_id",
) -> pd.DataFrame:
    """Compare area vs population weighted measures.

    Computes the difference between area-weighted and population-weighted
    estimates for each geography unit and measure, helping identify where
    weighting method significantly impacts results.

    Parameters
    ----------
    area_measures : pd.DataFrame
        Measures computed with area weighting.
        Must have ``geo_id_col`` column.
    pop_measures : pd.DataFrame
        Measures computed with population weighting.
        Must have ``geo_id_col`` column.
    geo_id_col : str
        Name of the geography identifier column.  Defaults to ``"coc_id"``.

    Returns
    -------
    pd.DataFrame
        Diagnostics per geography unit with columns:
        - ``geo_id_col``: Geography identifier
        - {measure}_area: Area-weighted value
        - {measure}_pop: Population-weighted value
        - {measure}_delta: Difference (area - pop)
        - {measure}_pct_diff: Percentage difference
    """
    if geo_id_col not in area_measures.columns:
        raise ValueError(f"area_measures must have '{geo_id_col}' column")
    if geo_id_col not in pop_measures.columns:
        raise ValueError(f"pop_measures must have '{geo_id_col}' column")

    # Identify numeric measure columns (exclude metadata)
    exclude_cols = {
        geo_id_col,
        "boundary_vintage",
        "acs_vintage",
        "weighting_method",
        "source",
        "coverage_ratio",
    }

    area_numeric = [
        c
        for c in area_measures.columns
        if c not in exclude_cols and pd.api.types.is_numeric_dtype(area_measures[c])
    ]
    pop_numeric = [
        c
        for c in pop_measures.columns
        if c not in exclude_cols and pd.api.types.is_numeric_dtype(pop_measures[c])
    ]

    # Find common measures
    common_measures = set(area_numeric) & set(pop_numeric)

    if not common_measures:
        raise ValueError("No common numeric measures found between area and pop DataFrames")

    # Merge on geo_id_col
    merged = area_measures[[geo_id_col] + list(common_measures)].merge(
        pop_measures[[geo_id_col] + list(common_measures)],
        on=geo_id_col,
        suffixes=("_area", "_pop"),
        how="outer",
    )

    # Compute deltas for each measure
    result_cols = [geo_id_col]

    for measure in sorted(common_measures):
        area_col = f"{measure}_area"
        pop_col = f"{measure}_pop"
        delta_col = f"{measure}_delta"
        pct_diff_col = f"{measure}_pct_diff"

        # Compute absolute difference
        merged[delta_col] = merged[area_col] - merged[pop_col]

        # Compute percentage difference (relative to population-weighted)
        # Avoid division by zero
        merged[pct_diff_col] = merged[delta_col] / merged[pop_col].replace(0, pd.NA) * 100

        result_cols.extend([area_col, pop_col, delta_col, pct_diff_col])

    return merged[result_cols].sort_values(geo_id_col).reset_index(drop=True)


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
        lines.append(
            f"  CoCs with >50% from single tract: {high_contrib} "
            f"({100 * high_contrib / n_cocs:.1f}%)"
        )
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
        lines.append(
            f"  CoCs with coverage 0.99-1.01: {good_coverage} ({100 * good_coverage / n_cocs:.1f}%)"
        )
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


def identify_problem_geos(
    diagnostics: pd.DataFrame,
    coverage_threshold: float = 0.95,
    max_contribution_threshold: float = 0.8,
    *,
    geo_id_col: str = "coc_id",
) -> pd.DataFrame:
    """Identify geography units with potential attribution problems.

    Flags units that may have data quality issues based on crosswalk
    diagnostics.

    Parameters
    ----------
    diagnostics : pd.DataFrame
        Diagnostics from compute_crosswalk_diagnostics.
    coverage_threshold : float
        Units with coverage below this are flagged. Default 0.95.
    max_contribution_threshold : float
        Units with max_tract_contribution above this are flagged. Default 0.8.
    geo_id_col : str
        Name of the geography identifier column.  Defaults to ``"coc_id"``.

    Returns
    -------
    pd.DataFrame
        Subset of diagnostics for flagged units with additional 'issues' column.
    """
    issues = []

    for _idx, row in diagnostics.iterrows():
        geo_issues = []

        # Check area coverage
        if "coverage_ratio_area" in row and row["coverage_ratio_area"] < coverage_threshold:
            geo_issues.append(f"low_area_coverage ({row['coverage_ratio_area']:.3f})")

        # Check if single tract dominates
        if (
            "max_tract_contribution" in row
            and row["max_tract_contribution"] > max_contribution_threshold
        ):
            geo_issues.append(f"high_tract_concentration ({row['max_tract_contribution']:.3f})")

        # Check population coverage if available
        if "coverage_ratio_pop" in row and pd.notna(row["coverage_ratio_pop"]):
            if row["coverage_ratio_pop"] < coverage_threshold:
                geo_issues.append(f"low_pop_coverage ({row['coverage_ratio_pop']:.3f})")

        if geo_issues:
            issues.append(
                {
                    geo_id_col: row[geo_id_col],
                    "issues": "; ".join(geo_issues),
                }
            )

    if not issues:
        return pd.DataFrame(columns=[geo_id_col, "issues"])

    problem_geos = pd.DataFrame(issues)
    problem_geos = problem_geos.merge(diagnostics, on=geo_id_col, how="left")

    # Reorder to put issues first after geo_id_col
    keep = {geo_id_col, "issues"}
    cols = [geo_id_col, "issues"] + [
        c for c in problem_geos.columns if c not in keep
    ]
    return problem_geos[cols]


def identify_problem_cocs(
    diagnostics: pd.DataFrame,
    coverage_threshold: float = 0.95,
    max_contribution_threshold: float = 0.8,
) -> pd.DataFrame:
    """Identify CoCs with potential attribution problems.

    Convenience wrapper around :func:`identify_problem_geos` with
    ``geo_id_col="coc_id"``.  See that function for full documentation.
    """
    return identify_problem_geos(
        diagnostics,
        coverage_threshold,
        max_contribution_threshold,
        geo_id_col="coc_id",
    )
