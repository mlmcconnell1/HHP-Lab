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

from hhplab.metro.metro_definitions import (
    DEFINITION_VERSION,
    build_county_membership_df,
)
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
        built-in membership from ``hhplab.metro.metro_definitions``.

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
