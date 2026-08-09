"""Reusable diagnostics for crosswalk validation."""

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class StatePopulationComparison:
    """Population validation metrics for one state FIPS code."""

    state: str
    acs_total: float
    coc_total: float
    diff: float
    ratio: float


@dataclass(frozen=True)
class AreaShareValidation:
    """Tract-level area share coverage diagnostics."""

    overcounted_count: int
    undercounted_count: int
    balanced_count: int
    overcounted_samples: tuple[tuple[str, float], ...]


@dataclass(frozen=True)
class PopulationValidationResult:
    """Structured result for CoC population crosswalk validation."""

    national_total: float
    relationship_count: int
    unique_crosswalk_tracts: int
    unique_population_tracts: int
    unique_geographies: int
    missing_tract_count: int
    extra_tract_count: int
    missing_population: float
    total_coc_population: float
    diff: float
    ratio: float
    within_threshold: bool
    area_share: AreaShareValidation
    state_comparison: tuple[StatePopulationComparison, ...]


REQUIRED_CROSSWALK_COLUMNS = frozenset({"tract_geoid", "area_share"})
REQUIRED_POPULATION_COLUMNS = frozenset({"tract_geoid", "total_population"})


def validate_population_crosswalk(
    crosswalk: pd.DataFrame,
    tract_population: pd.DataFrame,
    *,
    warn_threshold: float = 0.05,
    geo_id_col: str = "coc_id",
    include_state: bool = False,
) -> PopulationValidationResult:
    """Validate area-weighted crosswalk population totals against tract totals."""
    _require_columns(crosswalk, REQUIRED_CROSSWALK_COLUMNS | {geo_id_col}, "crosswalk")
    _require_columns(tract_population, REQUIRED_POPULATION_COLUMNS, "tract_population")

    xwalk = crosswalk.copy()
    population = tract_population.copy()
    xwalk["tract_geoid"] = xwalk["tract_geoid"].astype("string")
    population["tract_geoid"] = population["tract_geoid"].astype("string")

    national_total = float(pd.to_numeric(population["total_population"], errors="coerce").sum())
    acs_tracts = set(population["tract_geoid"].dropna())
    xwalk_tracts = set(xwalk["tract_geoid"].dropna())
    missing_tracts = acs_tracts - xwalk_tracts
    extra_tracts = xwalk_tracts - acs_tracts
    missing_population = float(
        pd.to_numeric(
            population.loc[population["tract_geoid"].isin(missing_tracts), "total_population"],
            errors="coerce",
        ).sum()
    )

    merged = xwalk.merge(
        population[["tract_geoid", "total_population"]],
        on="tract_geoid",
        how="left",
    )
    merged["weighted_pop"] = pd.to_numeric(merged["total_population"], errors="coerce").fillna(
        0
    ) * pd.to_numeric(merged["area_share"], errors="coerce").fillna(0)
    total_coc_population = float(merged.groupby(geo_id_col)["weighted_pop"].sum().sum())
    diff = total_coc_population - national_total
    ratio = _safe_ratio(total_coc_population, national_total)

    tract_area_sums = (
        merged.assign(area_share=pd.to_numeric(merged["area_share"], errors="coerce").fillna(0))
        .groupby("tract_geoid")["area_share"]
        .sum()
    )
    overcounted = tract_area_sums[tract_area_sums > 1.01]
    undercounted = tract_area_sums[tract_area_sums < 0.99]
    area_share = AreaShareValidation(
        overcounted_count=int(len(overcounted)),
        undercounted_count=int(len(undercounted)),
        balanced_count=int(len(tract_area_sums) - len(overcounted) - len(undercounted)),
        overcounted_samples=tuple(
            (str(geoid), float(value)) for geoid, value in overcounted.head(5).items()
        ),
    )

    state_comparison = _state_comparison(population, merged) if include_state else ()

    return PopulationValidationResult(
        national_total=national_total,
        relationship_count=int(len(xwalk)),
        unique_crosswalk_tracts=int(xwalk["tract_geoid"].nunique()),
        unique_population_tracts=int(population["tract_geoid"].nunique()),
        unique_geographies=int(xwalk[geo_id_col].nunique()),
        missing_tract_count=int(len(missing_tracts)),
        extra_tract_count=int(len(extra_tracts)),
        missing_population=missing_population,
        total_coc_population=total_coc_population,
        diff=diff,
        ratio=ratio,
        within_threshold=abs(1 - ratio) <= warn_threshold,
        area_share=area_share,
        state_comparison=state_comparison,
    )


def _state_comparison(
    population: pd.DataFrame,
    merged: pd.DataFrame,
) -> tuple[StatePopulationComparison, ...]:
    population_with_state = population.copy()
    population_with_state["state"] = population_with_state["tract_geoid"].str[:2]
    state_acs = population_with_state.groupby("state")["total_population"].sum()

    merged_with_state = merged.copy()
    merged_with_state["state"] = merged_with_state["tract_geoid"].str[:2]
    state_coc = merged_with_state.groupby("state")["weighted_pop"].sum()

    comparison = pd.DataFrame({"acs_total": state_acs, "coc_total": state_coc}).fillna(0)
    comparison["diff"] = comparison["coc_total"] - comparison["acs_total"]
    comparison["ratio"] = [
        _safe_ratio(coc_total, acs_total)
        for coc_total, acs_total in zip(
            comparison["coc_total"],
            comparison["acs_total"],
            strict=True,
        )
    ]

    return tuple(
        StatePopulationComparison(
            state=str(state),
            acs_total=float(row["acs_total"]),
            coc_total=float(row["coc_total"]),
            diff=float(row["diff"]),
            ratio=float(row["ratio"]),
        )
        for state, row in comparison.sort_index().iterrows()
    )


def _require_columns(
    df: pd.DataFrame,
    required: frozenset[str],
    frame_name: str,
) -> None:
    missing = sorted(required - set(df.columns))
    if missing:
        joined = ", ".join(missing)
        raise ValueError(f"{frame_name} missing required column(s): {joined}")


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0 if numerator == 0 else float("inf")
    return float(numerator / denominator)


def compute_crosswalk_diagnostics(
    crosswalk: pd.DataFrame,
    *,
    geo_id_col: str = "coc_id",
) -> pd.DataFrame:
    """Compute per-geography diagnostics for a tract crosswalk."""
    if geo_id_col not in crosswalk.columns:
        raise ValueError(f"Crosswalk must have '{geo_id_col}' column")
    if "intersection_area" not in crosswalk.columns:
        raise ValueError("Crosswalk must have 'intersection_area' column")

    xwalk = crosswalk.copy()
    geo_total_area = xwalk.groupby(geo_id_col)["intersection_area"].transform("sum")
    xwalk["geo_area_share"] = xwalk["intersection_area"] / geo_total_area
    grouped = xwalk.groupby(geo_id_col)
    diagnostics = pd.DataFrame(
        {
            "num_tracts": grouped.size(),
            "max_tract_contribution": grouped["geo_area_share"].max(),
            "coverage_ratio_area": grouped["geo_area_share"].sum(),
        }
    ).reset_index()

    if "pop_share" in crosswalk.columns and crosswalk["pop_share"].notna().any():
        pop_coverage = grouped["pop_share"].sum().reset_index()
        pop_coverage.columns = [geo_id_col, "coverage_ratio_pop"]
        diagnostics = diagnostics.merge(pop_coverage, on=geo_id_col, how="left")
    else:
        diagnostics["coverage_ratio_pop"] = pd.NA

    columns = [
        geo_id_col,
        "num_tracts",
        "max_tract_contribution",
        "coverage_ratio_area",
        "coverage_ratio_pop",
    ]
    return diagnostics[columns].sort_values(geo_id_col).reset_index(drop=True)


def compute_measure_diagnostics(
    area_measures: pd.DataFrame,
    pop_measures: pd.DataFrame,
    *,
    geo_id_col: str = "coc_id",
) -> pd.DataFrame:
    """Compare area-weighted and population-weighted measure estimates."""
    if geo_id_col not in area_measures.columns:
        raise ValueError(f"area_measures must have '{geo_id_col}' column")
    if geo_id_col not in pop_measures.columns:
        raise ValueError(f"pop_measures must have '{geo_id_col}' column")

    excluded = {
        geo_id_col,
        "boundary_vintage",
        "acs_vintage",
        "weighting_method",
        "source",
        "coverage_ratio",
    }
    area_numeric = [
        column
        for column in area_measures.columns
        if column not in excluded and pd.api.types.is_numeric_dtype(area_measures[column])
    ]
    pop_numeric = [
        column
        for column in pop_measures.columns
        if column not in excluded and pd.api.types.is_numeric_dtype(pop_measures[column])
    ]
    common_measures = set(area_numeric) & set(pop_numeric)
    if not common_measures:
        raise ValueError("No common numeric measures found between area and pop DataFrames")

    merged = area_measures[[geo_id_col] + list(common_measures)].merge(
        pop_measures[[geo_id_col] + list(common_measures)],
        on=geo_id_col,
        suffixes=("_area", "_pop"),
        how="outer",
    )
    result_columns = [geo_id_col]
    for measure in sorted(common_measures):
        area_column = f"{measure}_area"
        pop_column = f"{measure}_pop"
        delta_column = f"{measure}_delta"
        pct_column = f"{measure}_pct_diff"
        merged[delta_column] = merged[area_column] - merged[pop_column]
        merged[pct_column] = merged[delta_column] / merged[pop_column].replace(0, pd.NA) * 100
        result_columns.extend([area_column, pop_column, delta_column, pct_column])
    return merged[result_columns].sort_values(geo_id_col).reset_index(drop=True)


def summarize_diagnostics(diagnostics: pd.DataFrame) -> str:
    """Generate the stable human-readable diagnostics summary."""
    lines = ["=" * 60, "CROSSWALK DIAGNOSTICS SUMMARY", "=" * 60]
    n_geos = len(diagnostics)
    lines.extend([f"Total CoCs: {n_geos}", ""])

    if "num_tracts" in diagnostics.columns:
        lines.extend(["TRACT COVERAGE:", "-" * 40])
        tract_stats = diagnostics["num_tracts"]
        lines.extend(
            [
                f"  Tracts per CoC (mean):   {tract_stats.mean():.1f}",
                f"  Tracts per CoC (median): {tract_stats.median():.1f}",
                f"  Tracts per CoC (min):    {tract_stats.min()}",
                f"  Tracts per CoC (max):    {tract_stats.max()}",
                "",
            ]
        )

    if "max_tract_contribution" in diagnostics.columns:
        lines.extend(["MAX SINGLE-TRACT CONTRIBUTION:", "-" * 40])
        max_contribution = diagnostics["max_tract_contribution"]
        high_contribution = (max_contribution > 0.5).sum()
        lines.extend(
            [
                f"  Mean max contribution:   {max_contribution.mean():.3f}",
                f"  Median max contribution: {max_contribution.median():.3f}",
                f"  CoCs with >50% from single tract: {high_contribution} "
                f"({100 * high_contribution / n_geos:.1f}%)",
                "",
            ]
        )

    if "coverage_ratio_area" in diagnostics.columns:
        lines.extend(["AREA COVERAGE RATIO:", "-" * 40])
        area_coverage = diagnostics["coverage_ratio_area"]
        good_coverage = ((area_coverage >= 0.99) & (area_coverage <= 1.01)).sum()
        lines.extend(
            [
                f"  Mean:   {area_coverage.mean():.4f}",
                f"  Median: {area_coverage.median():.4f}",
                f"  Min:    {area_coverage.min():.4f}",
                f"  Max:    {area_coverage.max():.4f}",
                f"  CoCs with coverage 0.99-1.01: {good_coverage} "
                f"({100 * good_coverage / n_geos:.1f}%)",
                "",
            ]
        )

    if "coverage_ratio_pop" in diagnostics.columns:
        population_coverage = diagnostics["coverage_ratio_pop"]
        if population_coverage.notna().any():
            lines.extend(["POPULATION COVERAGE RATIO:", "-" * 40])
            valid_coverage = population_coverage.dropna()
            lines.extend(
                [
                    f"  Mean:   {valid_coverage.mean():.4f}",
                    f"  Median: {valid_coverage.median():.4f}",
                    f"  Min:    {valid_coverage.min():.4f}",
                    f"  Max:    {valid_coverage.max():.4f}",
                ]
            )
            missing = population_coverage.isna().sum()
            if missing > 0:
                lines.append(f"  CoCs missing pop_share: {missing}")
            lines.append("")

    delta_columns = [column for column in diagnostics.columns if column.endswith("_delta")]
    if delta_columns:
        lines.extend(["AREA VS POPULATION WEIGHTING DIFFERENCES:", "-" * 40])
        for delta_column in delta_columns:
            measure = delta_column.removesuffix("_delta")
            deltas = diagnostics[delta_column].dropna()
            if len(deltas) == 0:
                continue
            lines.extend(
                [
                    f"  {measure}:",
                    f"    Mean delta:   {deltas.mean():,.2f}",
                    f"    Median delta: {deltas.median():,.2f}",
                    f"    Max |delta|:  {deltas.abs().max():,.2f}",
                ]
            )
            pct_column = f"{measure}_pct_diff"
            if pct_column in diagnostics.columns:
                percentages = diagnostics[pct_column].dropna()
                if len(percentages) > 0:
                    significant = (percentages.abs() > 10).sum()
                    lines.extend(
                        [
                            f"    Mean pct diff: {percentages.mean():.2f}%",
                            f"    CoCs with >10% diff: {significant}",
                        ]
                    )
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
    """Identify geographies with low coverage or concentrated attribution."""
    issues = []
    for _index, row in diagnostics.iterrows():
        geo_issues = []
        if "coverage_ratio_area" in row and row["coverage_ratio_area"] < coverage_threshold:
            geo_issues.append(f"low_area_coverage ({row['coverage_ratio_area']:.3f})")
        if (
            "max_tract_contribution" in row
            and row["max_tract_contribution"] > max_contribution_threshold
        ):
            geo_issues.append(f"high_tract_concentration ({row['max_tract_contribution']:.3f})")
        if "coverage_ratio_pop" in row and pd.notna(row["coverage_ratio_pop"]):
            if row["coverage_ratio_pop"] < coverage_threshold:
                geo_issues.append(f"low_pop_coverage ({row['coverage_ratio_pop']:.3f})")
        if geo_issues:
            issues.append({geo_id_col: row[geo_id_col], "issues": "; ".join(geo_issues)})

    if not issues:
        return pd.DataFrame(columns=[geo_id_col, "issues"])
    problem_geos = pd.DataFrame(issues).merge(diagnostics, on=geo_id_col, how="left")
    keep = {geo_id_col, "issues"}
    columns = [geo_id_col, "issues"] + [
        column for column in problem_geos.columns if column not in keep
    ]
    return problem_geos[columns]


def identify_problem_cocs(
    diagnostics: pd.DataFrame,
    coverage_threshold: float = 0.95,
    max_contribution_threshold: float = 0.8,
) -> pd.DataFrame:
    """Backward-compatible CoC wrapper around :func:`identify_problem_geos`."""
    return identify_problem_geos(
        diagnostics,
        coverage_threshold,
        max_contribution_threshold,
        geo_id_col="coc_id",
    )


__all__ = [
    "AreaShareValidation",
    "PopulationValidationResult",
    "StatePopulationComparison",
    "compute_crosswalk_diagnostics",
    "compute_measure_diagnostics",
    "identify_problem_cocs",
    "identify_problem_geos",
    "summarize_diagnostics",
    "validate_population_crosswalk",
]
