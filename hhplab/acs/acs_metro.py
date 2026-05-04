"""Metro-level ACS measure aggregation from tract-native inputs.

ACS (American Community Survey) data is available at the census tract
level. Metro ACS measures are derived by building a metro-tract crosswalk from
the metro-county membership table (since tracts nest perfectly within
counties, every tract in a member county has ``area_share=1.0``), then
aggregating using :func:`hhplab.acs.acs_aggregate.aggregate_to_geo`.

For multi-county metros (e.g., NYC with 5 boroughs, Denver with 7 counties),
all tracts from all member counties contribute.  For single-county metros,
only tracts from that county contribute.
"""

from __future__ import annotations

import logging

import pandas as pd

from hhplab.acs.acs_aggregate import aggregate_to_geo
from hhplab.metro.metro_definitions import (
    DEFINITION_VERSION,
    build_county_membership_df,
)

logger = logging.getLogger(__name__)


def build_metro_tract_crosswalk(
    acs_data: pd.DataFrame,
    *,
    county_membership_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build a metro-tract crosswalk from ACS data and county membership.

    Since census tracts nest perfectly within counties, every tract in a
    member county is fully contained in the metro (``area_share=1.0``).
    Tract GEOID prefix (first 5 digits) equals the county FIPS code.

    Parameters
    ----------
    acs_data : pd.DataFrame
        Tract-level ACS data with a ``GEOID`` column (11-digit tract codes).
    county_membership_df : pd.DataFrame, optional
        Override metro-county membership table.  If None, uses the
        built-in membership from ``hhplab.metro.metro_definitions``.

    Returns
    -------
    pd.DataFrame
        Metro-tract crosswalk with columns: ``metro_id``, ``GEOID``,
        ``area_share``, ``definition_version``.
    """
    if county_membership_df is None:
        membership = build_county_membership_df()
    else:
        membership = county_membership_df.copy()

    # Extract county FIPS from tract GEOID (first 5 digits)
    tracts = acs_data[["GEOID"]].copy()
    tracts["county_fips"] = tracts["GEOID"].str[:5]

    # Join tracts with metro-county membership
    xwalk = membership.merge(tracts, on="county_fips", how="inner")

    # Warn about tracts dropped due to no county match
    n_total = len(tracts)
    n_matched = tracts["county_fips"].isin(membership["county_fips"]).sum()
    n_dropped = n_total - n_matched
    if n_dropped > 0:
        dropout_pct = n_dropped / n_total * 100
        logger.warning(
            f"build_metro_tract_crosswalk: {n_dropped} of {n_total} tracts "
            f"({dropout_pct:.1f}%) dropped — county_fips not in metro membership"
        )

    # All tracts are fully contained (area_share = 1.0)
    xwalk["area_share"] = 1.0

    # Rename for compatibility with aggregate_to_geo
    xwalk = xwalk.rename(columns={"GEOID": "tract_geoid"})

    logger.info(
        f"Built metro-tract crosswalk: {xwalk['metro_id'].nunique()} metros, "
        f"{len(xwalk)} tract rows"
    )

    return xwalk[["metro_id", "tract_geoid", "area_share", "definition_version"]]


def aggregate_acs_to_metro(
    acs_data: pd.DataFrame,
    *,
    weighting: str = "area",
    definition_version: str = DEFINITION_VERSION,
    county_membership_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Aggregate tract-level ACS data to Glynn/Fox metro areas.

    Parameters
    ----------
    acs_data : pd.DataFrame
        Tract-level ACS data with ``GEOID`` and measure columns
        (total_population, adult_population, median_household_income, etc.).
    weighting : str
        Weighting method for median variables: ``"area"`` or
        ``"population"``.  Count variables always use area_share.
    definition_version : str
        Metro definition version to use.
    county_membership_df : pd.DataFrame, optional
        Override metro-county membership table.

    Returns
    -------
    pd.DataFrame
        Metro-level ACS measures with columns:
        - ``metro_id``: Glynn/Fox identifier
        - ``total_population``, ``adult_population``, etc.
        - ``median_household_income``, ``median_gross_rent``
        - ``coverage_ratio``, ``weighting_method``, ``source``
        - ``definition_version``
    """
    # Build metro-tract crosswalk from ACS data
    xwalk = build_metro_tract_crosswalk(
        acs_data,
        county_membership_df=county_membership_df,
    )

    # Delegate to the generalized ACS aggregation
    result_df = aggregate_to_geo(
        acs_data,
        xwalk,
        weighting,
        geo_id_col="metro_id",
    )

    # Add definition version
    result_df["definition_version"] = definition_version

    logger.info(f"Metro ACS aggregation: {result_df['metro_id'].nunique()} metros")

    return result_df
