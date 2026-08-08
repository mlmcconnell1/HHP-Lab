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
    merged["weighted_pop"] = (
        pd.to_numeric(merged["total_population"], errors="coerce").fillna(0)
        * pd.to_numeric(merged["area_share"], errors="coerce").fillna(0)
    )
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
