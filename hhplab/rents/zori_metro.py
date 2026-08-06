"""Metro-level ZORI aggregation from county-native inputs.

ZORI (Zillow Observed Rent Index) data is native at the county level.
Metro ZORI values are derived by computing a population-weighted mean
of county ZORI for all member counties defined in the metro-county
membership table.

For multi-county metros (e.g., NYC with 5 boroughs, Denver with 7 counties),
this module computes a weighted mean using ACS-based county weights.
For single-county metros, it is a 1:1 passthrough.

Coverage tracking: when a member county lacks ZORI data for a given month,
``coverage_ratio`` records the fraction of member-county weight mass
with data available.
"""

from __future__ import annotations

import logging

import pandas as pd

from hhplab.geographies.gf_metro.metro_definitions import (
    DEFINITION_VERSION,
    build_county_membership_df,
)
from hhplab.geographies.msa.msa_io import read_msa_county_membership
from hhplab.rents.zori_aggregate import (
    YearlyMethod,
    aggregate_monthly,
    collapse_to_yearly,
)

logger = logging.getLogger(__name__)


def _build_metro_county_crosswalk() -> pd.DataFrame:
    """Build a metro-county crosswalk suitable for ZORI aggregation.

    Each metro-county pair gets ``area_share=1.0`` because metros fully
    contain their member counties (no partial overlap).
    """
    membership = build_county_membership_df()
    membership["area_share"] = 1.0
    return membership


def aggregate_zori_to_metro(
    zori_df: pd.DataFrame,
    weights_df: pd.DataFrame,
    *,
    definition_version: str = DEFINITION_VERSION,
    min_coverage: float = 0.90,
    county_membership_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Aggregate county-level ZORI to Glynn/Fox metro areas (monthly).

    Parameters
    ----------
    zori_df : pd.DataFrame
        County-level ZORI data with columns:
        - ``geo_id``: county FIPS code
        - ``date``: month start date
        - ``zori``: ZORI value
    weights_df : pd.DataFrame
        County ACS weights with columns:
        - ``county_fips``: 5-digit FIPS code
        - ``weight_value``: ACS-based weight (e.g., renter households)
    definition_version : str
        Metro definition version to use.
    min_coverage : float
        Minimum coverage ratio threshold.  Metro-months below this have
        zori_coc set to null.  Default 0.90.
    county_membership_df : pd.DataFrame, optional
        Override metro-county membership table.  If None, uses the
        built-in membership from ``hhplab.geographies.gf_metro.metro_definitions``.

    Returns
    -------
    pd.DataFrame
        Monthly metro-level ZORI with columns:
        - ``metro_id``, ``date``, ``zori_coc``
        - ``coverage_ratio``, ``max_geo_contribution``, ``geo_count``
        - ``definition_version``
    """
    # Build crosswalk
    if county_membership_df is not None:
        xwalk = county_membership_df.copy()
        if "area_share" not in xwalk.columns:
            xwalk["area_share"] = 1.0
    else:
        xwalk = _build_metro_county_crosswalk()

    # Delegate to the generalized monthly aggregation
    result_df = aggregate_monthly(
        zori_df,
        xwalk,
        weights_df,
        min_coverage=min_coverage,
        geo_id_col="metro_id",
    )

    # Add definition version
    result_df["definition_version"] = definition_version

    logger.info(
        f"Metro ZORI aggregation: {result_df['metro_id'].nunique()} metros, "
        f"{result_df['date'].nunique()} months"
    )

    return result_df


def collapse_zori_to_yearly(
    monthly_df: pd.DataFrame,
    method: YearlyMethod = "pit_january",
) -> pd.DataFrame:
    """Collapse monthly metro ZORI to yearly values.

    Thin wrapper around :func:`hhplab.rents.zori_aggregate.collapse_to_yearly`
    with ``geo_id_col="metro_id"``.

    Parameters
    ----------
    monthly_df : pd.DataFrame
        Monthly metro ZORI from :func:`aggregate_zori_to_metro`.
    method : str
        Yearly collapse method: ``"pit_january"``, ``"calendar_mean"``,
        or ``"calendar_median"``.

    Returns
    -------
    pd.DataFrame
        Yearly metro ZORI with ``metro_id``, ``year``, ``zori_coc``, etc.
    """
    return collapse_to_yearly(monthly_df, method, geo_id_col="metro_id")


def aggregate_yearly_zori_to_metro(
    zori_yearly: pd.DataFrame,
    county_population: pd.DataFrame,
    *,
    county_membership_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Aggregate yearly county ZORI to metros using per-year population weights.

    Unlike :func:`aggregate_zori_to_metro` (which operates on monthly data with
    a single weight snapshot), this function applies year-specific population
    weights — appropriate for multi-year panels where county populations shift
    over time.

    Parameters
    ----------
    zori_yearly : pd.DataFrame
        Yearly county-level ZORI with columns:
        - ``county_fips`` (str): 5-digit FIPS code
        - ``year`` (int): year
        - ``zori`` (float): ZORI value (e.g., January observation)
    county_population : pd.DataFrame
        County population by year with columns:
        - ``county_fips`` (str): 5-digit FIPS code
        - ``year`` (int): year
        - ``population`` (numeric): population count used as weight
    county_membership_df : pd.DataFrame, optional
        Metro-county membership override.  Must have ``metro_id`` and
        ``county_fips`` columns.  If None, uses the built-in membership.

    Returns
    -------
    pd.DataFrame
        Yearly metro ZORI with columns: ``metro_id``, ``year``, ``zori``.
    """
    if county_membership_df is not None:
        membership = county_membership_df[["metro_id", "county_fips"]].copy()
    else:
        membership = build_county_membership_df()[["metro_id", "county_fips"]]

    # Detect orphan ZORI counties absent from metro membership
    zori_counties = set(zori_yearly["county_fips"].unique())
    membership_counties = set(membership["county_fips"].unique())
    orphan_counties = zori_counties - membership_counties
    if orphan_counties:
        logger.warning(
            f"{len(orphan_counties)} ZORI counties absent from metro membership "
            f"(these will not contribute to any metro): "
            f"{sorted(orphan_counties)[:10]}"
            f"{'...' if len(orphan_counties) > 10 else ''}"
        )

    # Count expected member counties per metro before the inner join so we
    # can detect metros where some counties are absent from the ZORI data.
    expected_n = membership.groupby("metro_id")["county_fips"].nunique()

    merged = membership.merge(zori_yearly, on="county_fips", how="inner")
    merged = merged.merge(county_population, on=["county_fips", "year"], how="left")

    # Compute per-metro-year normalised weights.
    # Null out metro-years where county ZORI coverage is incomplete (fewer
    # counties survived the inner join than are in the membership).
    actual_n = merged.groupby(["metro_id", "year"])["county_fips"].transform("nunique")
    incomplete_zori = actual_n < merged["metro_id"].map(expected_n)

    # Also null out metro-years where any county population is missing to
    # avoid silently renormalizing weights over a subset of counties.
    any_missing_pop = merged.groupby(["metro_id", "year"])["population"].transform(
        lambda s: s.isna().any()
    )
    pop_for_weight = merged["population"].where(~incomplete_zori & ~any_missing_pop)
    pop_sum = merged.groupby(["metro_id", "year"])["population"].transform("sum")
    merged["weight"] = pop_for_weight / pop_sum
    merged["weighted_zori"] = merged["zori"] * merged["weight"]

    result = (
        merged.groupby(["metro_id", "year"], as_index=False)["weighted_zori"]
        .sum(min_count=1)
        .rename(columns={"weighted_zori": "zori"})
    )
    return result.sort_values(["metro_id", "year"]).reset_index(drop=True)


def aggregate_yearly_zori_to_msa(
    zori_yearly: pd.DataFrame,
    county_population: pd.DataFrame,
    *,
    msa_definition_version: str = "census_msa_2023",
    county_membership_df: pd.DataFrame | None = None,
    data_root: str | None = None,
    years: list[int] | None = None,
    min_coverage: float | None = None,
    balanced_composition: bool = True,
) -> pd.DataFrame:
    """Aggregate county-year ZORI to Census MSA-year rows.

    ZORI is an intensive rent index, so member counties are combined with a
    population-weighted mean.  In balanced-composition mode, each MSA's
    contributing county set is fixed to counties with both ZORI and population
    data in every requested year.  Coverage diagnostics still compare that fixed
    set with the full MSA county membership for each year.
    """
    required_zori = {"county_fips", "year", "zori"}
    missing_zori = sorted(required_zori - set(zori_yearly.columns))
    if missing_zori:
        raise ValueError(f"Yearly ZORI data missing required columns: {missing_zori}")
    required_population = {"county_fips", "year", "population"}
    missing_population = sorted(required_population - set(county_population.columns))
    if missing_population:
        raise ValueError(
            "County population weights are missing required columns: "
            f"{missing_population}. Provide county_fips, year, population."
        )

    if min_coverage is not None and not 0 <= min_coverage <= 1:
        raise ValueError("min_coverage must be between 0 and 1 when provided.")

    membership = (
        county_membership_df.copy()
        if county_membership_df is not None
        else read_msa_county_membership(msa_definition_version, data_root)
    )
    missing_membership = sorted({"msa_id", "county_fips"} - set(membership.columns))
    if missing_membership:
        raise ValueError(
            "MSA county membership is missing required columns: "
            f"{missing_membership}."
        )
    membership = membership[["msa_id", "county_fips"]].drop_duplicates().copy()
    membership["msa_id"] = membership["msa_id"].astype("string")
    membership["county_fips"] = membership["county_fips"].astype("string").str.zfill(5)

    zori = zori_yearly[["county_fips", "year", "zori"]].copy()
    zori["county_fips"] = zori["county_fips"].astype("string").str.zfill(5)
    zori["year"] = pd.to_numeric(zori["year"], errors="coerce").astype("Int64")
    zori["zori"] = pd.to_numeric(zori["zori"], errors="coerce")
    zori = zori[zori["year"].notna()].copy()
    zori["year"] = zori["year"].astype(int)

    population = county_population[["county_fips", "year", "population"]].copy()
    population["county_fips"] = population["county_fips"].astype("string").str.zfill(5)
    population["year"] = pd.to_numeric(population["year"], errors="coerce").astype("Int64")
    population["population"] = pd.to_numeric(population["population"], errors="coerce")
    population = population[population["year"].notna()].copy()
    population["year"] = population["year"].astype(int)

    requested_years = sorted(set(int(year) for year in years)) if years is not None else None
    if requested_years is None:
        requested_years = sorted(set(zori["year"].unique()) & set(population["year"].unique()))
    if not requested_years:
        raise ValueError("No overlapping ZORI/population years are available for MSA rollup.")
    population_years = set(population["year"].unique())
    missing_population_years = [
        year for year in requested_years if year not in population_years
    ]
    if missing_population_years:
        missing = ", ".join(str(year) for year in missing_population_years)
        available = ", ".join(str(year) for year in sorted(population_years)) or "none"
        raise ValueError(
            "County population weights are missing for requested year(s): "
            f"{missing}. Available population years: {available}. "
            "Refresh PEP county population data or request only covered years before "
            "running MSA ZORI aggregation."
        )
    zori = zori[zori["year"].isin(requested_years)]
    population = population[population["year"].isin(requested_years)]

    member_counties_by_msa = (
        membership.groupby("msa_id")["county_fips"]
        .agg(lambda values: set(values.dropna().astype(str)))
        .to_dict()
    )
    membership_county_count = {
        msa_id: len(counties) for msa_id, counties in member_counties_by_msa.items()
    }

    county_year = membership.merge(zori, on="county_fips", how="left").merge(
        population,
        on=["county_fips", "year"],
        how="left",
    )
    county_year = county_year[county_year["year"].isin(requested_years)].copy()
    county_year["has_zori"] = county_year["zori"].notna()
    county_year["has_population"] = county_year["population"].notna() & (
        county_year["population"] > 0
    )
    county_year["usable"] = county_year["has_zori"] & county_year["has_population"]

    if balanced_composition:
        usable_year_counts = (
            county_year[county_year["usable"]]
            .groupby(["msa_id", "county_fips"])["year"]
            .nunique()
        )
        complete_keys = {
            key for key, count in usable_year_counts.items() if count == len(requested_years)
        }
    else:
        complete_keys = set()

    rows: list[dict[str, object]] = []
    for msa_id in sorted(member_counties_by_msa):
        msa_counties = member_counties_by_msa[msa_id]
        for year in requested_years:
            current_msa_id = msa_id
            group = county_year[
                (county_year["msa_id"] == current_msa_id) & (county_year["year"] == year)
            ].copy()
            if balanced_composition:
                balanced_mask = group["county_fips"].map(
                    lambda county, msa=current_msa_id: (msa, county) in complete_keys
                )
                group = group.loc[balanced_mask.astype(bool)].copy()
            else:
                group = group[group["usable"]]

            total_pop_rows = population[
                (population["year"] == year) & (population["county_fips"].isin(msa_counties))
            ]
            total_population = float(total_pop_rows["population"].sum())
            covered_population = float(group["population"].sum())
            coverage_ratio = (
                covered_population / total_population if total_population > 0 else pd.NA
            )
            zori_value = (
                float((group["zori"] * group["population"]).sum() / covered_population)
                if covered_population > 0
                else pd.NA
            )
            if min_coverage is not None and pd.notna(coverage_ratio):
                if float(coverage_ratio) < min_coverage:
                    zori_value = pd.NA

            county_count = int(group["county_fips"].nunique())
            covered_counties = set(group["county_fips"].dropna().astype(str))
            contribution_shares = (
                group["population"] / covered_population
                if covered_population > 0
                else pd.Series([])
            )
            rows.append(
                {
                    "geo_type": "msa",
                    "geo_id": str(current_msa_id),
                    "msa_id": str(current_msa_id),
                    "year": int(year),
                    "zori_coc": zori_value,
                    "coverage_ratio": coverage_ratio,
                    "covered_population": covered_population,
                    "total_population": total_population,
                    "population_weight_denominator": covered_population,
                    "county_count": county_count,
                    "membership_county_count": int(membership_county_count[msa_id]),
                    "missing_counties": ",".join(sorted(msa_counties - covered_counties)),
                    "max_geo_contribution": (
                        float(contribution_shares.max()) if not contribution_shares.empty else pd.NA
                    ),
                    "definition_version": msa_definition_version,
                    "balanced_composition": bool(balanced_composition),
                }
            )

    return pd.DataFrame(rows).sort_values(["msa_id", "year"]).reset_index(drop=True)


def to_msa_zori_yearly_artifact(msa_zori: pd.DataFrame) -> pd.DataFrame:
    """Convert internal MSA ZORI rollup output to the curated artifact schema."""
    from hhplab.schema import (
        MSA_ZORI_YEARLY_COLUMNS,
        MSA_ZORI_YEARLY_CONTRACT,
        validate_artifact_contract,
    )

    if "zori_coc" not in msa_zori.columns:
        raise ValueError("MSA ZORI rollup is missing internal column 'zori_coc'.")

    artifact = msa_zori.rename(columns={"zori_coc": "zori"}).copy()
    findings = validate_artifact_contract(artifact, MSA_ZORI_YEARLY_CONTRACT)
    errors = [finding for finding in findings if finding.severity == "error"]
    if errors:
        details = "; ".join(finding.message for finding in errors)
        raise ValueError(f"MSA ZORI yearly artifact schema validation failed: {details}")

    return artifact.loc[:, list(MSA_ZORI_YEARLY_COLUMNS)]
