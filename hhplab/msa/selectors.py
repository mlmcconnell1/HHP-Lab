"""Reusable Census MSA selectors for recipe workflows."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

import pandas as pd

PopulationRankingSource = Literal["pep", "acs5"]


@dataclass(frozen=True)
class TopMsaSelectionDiagnostics:
    """Diagnostics for a top-N MSA population selector run."""

    ranking_source: PopulationRankingSource
    reference_year: int
    requested_n: int
    eligible_msa_count: int
    selected_count: int
    expected_msa_count: int
    missing_msa_count: int
    missing_msa_ids: tuple[str, ...]
    incomplete_msa_count: int
    incomplete_msa_ids: tuple[str, ...]
    tied_cutoff: bool
    cutoff_population: float | None
    tied_cutoff_msa_ids: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "ranking_source": self.ranking_source,
            "reference_year": self.reference_year,
            "requested_n": self.requested_n,
            "eligible_msa_count": self.eligible_msa_count,
            "selected_count": self.selected_count,
            "expected_msa_count": self.expected_msa_count,
            "missing_msa_count": self.missing_msa_count,
            "missing_msa_ids": list(self.missing_msa_ids),
            "incomplete_msa_count": self.incomplete_msa_count,
            "incomplete_msa_ids": list(self.incomplete_msa_ids),
            "tied_cutoff": self.tied_cutoff,
            "cutoff_population": self.cutoff_population,
            "tied_cutoff_msa_ids": list(self.tied_cutoff_msa_ids),
        }


@dataclass(frozen=True)
class TopMsaSelection:
    """Selected MSA IDs and ranking diagnostics."""

    selector_ids: tuple[str, ...]
    ranking: pd.DataFrame
    diagnostics: TopMsaSelectionDiagnostics

    def to_dict(self) -> dict[str, object]:
        return {
            "selector_ids": list(self.selector_ids),
            "ranking": self.ranking.to_dict(orient="records"),
            "diagnostics": self.diagnostics.to_dict(),
        }


def select_top_msa_ids_by_population(
    population_df: pd.DataFrame,
    msa_county_membership: pd.DataFrame,
    *,
    ranking_source: PopulationRankingSource,
    reference_year: int,
    top_n: int,
    population_column: str = "population",
) -> TopMsaSelection:
    """Resolve top-N Census MSA selector IDs from source population rows.

    ``ranking_source='pep'`` expects county-level rows with ``county_fips``,
    ``year``, and a population column. ``ranking_source='acs5'`` accepts
    tract-level rows with ``tract_geoid`` or ``GEOID``, county-level rows with
    ``county_fips``, or MSA-level rows with ``msa_id``.
    """
    if top_n < 1:
        raise ValueError("top_n must be at least 1.")
    if ranking_source not in ("pep", "acs5"):
        raise ValueError("ranking_source must be one of: pep, acs5.")

    membership = _normalize_membership(msa_county_membership)
    yearly = _reference_year_frame(population_df, reference_year)
    if population_column not in yearly.columns:
        raise ValueError(
            f"Population input is missing column '{population_column}'. "
            f"Available columns: {list(population_df.columns)}"
        )

    if ranking_source == "pep":
        msa_population = _roll_county_population_to_msa(
            yearly,
            membership,
            population_column=population_column,
            source_label="PEP",
        )
    else:
        msa_population = _roll_acs5_population_to_msa(
            yearly,
            membership,
            population_column=population_column,
        )

    ranking = _rank_msa_population(msa_population)
    selector_ids = tuple(ranking.head(top_n)["msa_id"].astype(str))
    diagnostics = _selection_diagnostics(
        ranking,
        membership,
        ranking_source=ranking_source,
        reference_year=reference_year,
        requested_n=top_n,
        selected_count=len(selector_ids),
    )
    return TopMsaSelection(
        selector_ids=selector_ids,
        ranking=ranking,
        diagnostics=diagnostics,
    )


def select_top_msa_ids_for_panel_spec(
    panel_spec: Any,
    population_df: pd.DataFrame,
    msa_county_membership: pd.DataFrame,
    *,
    population_column: str = "population",
) -> TopMsaSelection:
    """Resolve selector IDs from a recipe ``MsaCocPanelSpec``-like object."""
    return select_top_msa_ids_by_population(
        population_df,
        msa_county_membership,
        ranking_source=panel_spec.ranking_population_source,
        reference_year=panel_spec.ranking_reference_year,
        top_n=panel_spec.top_n,
        population_column=population_column,
    )


def _normalize_membership(msa_county_membership: pd.DataFrame) -> pd.DataFrame:
    required = {"msa_id", "county_fips"}
    missing = sorted(required - set(msa_county_membership.columns))
    if missing:
        raise ValueError(
            "MSA county membership is missing required columns "
            f"{missing}. Available columns: {list(msa_county_membership.columns)}"
        )
    membership = msa_county_membership[["msa_id", "county_fips"]].copy()
    membership["msa_id"] = membership["msa_id"].astype(str).str.zfill(5)
    membership["county_fips"] = membership["county_fips"].astype(str).str.zfill(5)
    return membership.drop_duplicates().reset_index(drop=True)


def _reference_year_frame(population_df: pd.DataFrame, reference_year: int) -> pd.DataFrame:
    if "year" not in population_df.columns:
        raise ValueError(
            "Population input is missing required column 'year'. "
            f"Available columns: {list(population_df.columns)}"
        )
    yearly = population_df.loc[population_df["year"] == reference_year].copy()
    if yearly.empty:
        raise ValueError(f"No population rows found for reference_year {reference_year}.")
    return yearly


def _roll_county_population_to_msa(
    yearly: pd.DataFrame,
    membership: pd.DataFrame,
    *,
    population_column: str,
    source_label: str,
) -> pd.DataFrame:
    if "county_fips" not in yearly.columns:
        raise ValueError(
            f"{source_label} population ranking requires 'county_fips'. "
            f"Available columns: {list(yearly.columns)}"
        )
    county_pop = yearly[["county_fips", population_column]].copy()
    county_pop["county_fips"] = county_pop["county_fips"].astype(str).str.zfill(5)
    county_pop[population_column] = pd.to_numeric(county_pop[population_column], errors="coerce")
    joined = membership.merge(county_pop, on="county_fips", how="left")
    rows: list[dict[str, object]] = []
    for msa_id, group in joined.groupby("msa_id", sort=True):
        population = group[population_column].sum(min_count=1)
        observed = int(group[population_column].notna().sum())
        expected = int(group["county_fips"].nunique())
        missing = tuple(sorted(group.loc[group[population_column].isna(), "county_fips"]))
        rows.append(
            {
                "msa_id": msa_id,
                "population": population,
                "expected_county_count": expected,
                "observed_county_count": observed,
                "coverage_ratio": observed / expected if expected else pd.NA,
                "missing_counties": ",".join(missing),
            }
        )
    return pd.DataFrame(rows)


def _roll_acs5_population_to_msa(
    yearly: pd.DataFrame,
    membership: pd.DataFrame,
    *,
    population_column: str,
) -> pd.DataFrame:
    if "msa_id" in yearly.columns:
        msa_pop = yearly[["msa_id", population_column]].copy()
        msa_pop["msa_id"] = msa_pop["msa_id"].astype(str).str.zfill(5)
        msa_pop[population_column] = pd.to_numeric(msa_pop[population_column], errors="coerce")
        return (
            msa_pop.groupby("msa_id", as_index=False)[population_column]
            .sum(min_count=1)
            .rename(columns={population_column: "population"})
        )

    if "county_fips" in yearly.columns:
        return _roll_county_population_to_msa(
            yearly,
            membership,
            population_column=population_column,
            source_label="ACS5",
        )

    tract_col = "tract_geoid" if "tract_geoid" in yearly.columns else "GEOID"
    if tract_col not in yearly.columns:
        raise ValueError(
            "ACS5 population ranking requires one of 'msa_id', 'county_fips', "
            f"'tract_geoid', or 'GEOID'. Available columns: {list(yearly.columns)}"
        )
    tract_pop = yearly[[tract_col, population_column]].copy()
    tract_pop["county_fips"] = tract_pop[tract_col].astype(str).str[:5].str.zfill(5)
    return _roll_county_population_to_msa(
        tract_pop.assign(year=yearly["year"].to_numpy()),
        membership,
        population_column=population_column,
        source_label="ACS5",
    )


def _rank_msa_population(msa_population: pd.DataFrame) -> pd.DataFrame:
    ranking = msa_population.copy()
    ranking["population"] = pd.to_numeric(ranking["population"], errors="coerce")
    ranking = ranking.dropna(subset=["population"])
    return ranking.sort_values(["population", "msa_id"], ascending=[False, True]).reset_index(
        drop=True
    )


def _selection_diagnostics(
    ranking: pd.DataFrame,
    membership: pd.DataFrame,
    *,
    ranking_source: PopulationRankingSource,
    reference_year: int,
    requested_n: int,
    selected_count: int,
) -> TopMsaSelectionDiagnostics:
    expected_ids = tuple(sorted(membership["msa_id"].drop_duplicates()))
    ranked_ids = set(ranking["msa_id"].astype(str))
    missing_ids = tuple(msa_id for msa_id in expected_ids if msa_id not in ranked_ids)
    incomplete_ids: tuple[str, ...] = ()
    if "coverage_ratio" in ranking.columns:
        incomplete = ranking[ranking["coverage_ratio"].fillna(0.0) < 1.0]["msa_id"].astype(str)
        incomplete_ids = tuple(sorted(incomplete))

    cutoff_population: float | None = None
    tied_ids: tuple[str, ...] = ()
    tied_cutoff = False
    if selected_count > 0 and selected_count == min(requested_n, len(ranking)):
        cutoff_population = float(ranking.iloc[selected_count - 1]["population"])
        selected_ids = set(ranking.head(selected_count)["msa_id"].astype(str))
        tied = ranking[ranking["population"] == cutoff_population]["msa_id"].astype(str)
        tied_ids = tuple(tied)
        tied_cutoff = any(msa_id not in selected_ids for msa_id in tied_ids)

    return TopMsaSelectionDiagnostics(
        ranking_source=ranking_source,
        reference_year=reference_year,
        requested_n=requested_n,
        eligible_msa_count=len(ranking),
        selected_count=selected_count,
        expected_msa_count=len(expected_ids),
        missing_msa_count=len(missing_ids),
        missing_msa_ids=missing_ids,
        incomplete_msa_count=len(incomplete_ids),
        incomplete_msa_ids=incomplete_ids,
        tied_cutoff=tied_cutoff,
        cutoff_population=cutoff_population,
        tied_cutoff_msa_ids=tied_ids if tied_cutoff else (),
    )
