"""Metro-level PEP population aggregation from county-native inputs.

PEP (Population Estimates Program) data is native at the county level.
Metro PEP totals are derived by summing county population for all member
counties defined in the metro-county membership table.

For multi-county metros (e.g., NYC with 5 boroughs, Denver with 7 counties),
this module sums population across all member counties.  For single-county
metros, it is a 1:1 passthrough.

Coverage tracking: when a member county lacks PEP data for a given year,
``coverage_ratio`` records the fraction of member counties with data.
"""

from __future__ import annotations

import logging

import pandas as pd

from coclab.metro.definitions import (
    DEFINITION_VERSION,
    build_county_membership_df,
)
from coclab.pep.aggregate import aggregate_pep_counties

logger = logging.getLogger(__name__)


def _build_metro_county_crosswalk() -> pd.DataFrame:
    """Build a metro-county crosswalk suitable for aggregate_pep_counties.

    Each metro-county pair gets ``area_share=1.0`` because metros fully
    contain their member counties (no partial overlap).
    """
    membership = build_county_membership_df()
    membership["area_share"] = 1.0
    return membership


def aggregate_pep_to_metro(
    pep_df: pd.DataFrame,
    *,
    definition_version: str = DEFINITION_VERSION,
    weighting: str = "area_share",
    min_coverage: float = 0.0,
    county_membership_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Aggregate county-level PEP population to Glynn/Fox metro areas.

    Parameters
    ----------
    pep_df : pd.DataFrame
        County-level PEP data with columns:
        - ``county_fips``: 5-digit FIPS code
        - ``year``: estimate year
        - ``population``: population count
    definition_version : str
        Metro definition version to use.
    weighting : str
        Weighting method: ``"area_share"`` (all 1.0 since metros fully
        contain counties) or ``"equal"``.
    min_coverage : float
        Minimum coverage ratio threshold.  Metro-years below this have
        population set to null.  Default 0.0 (allow partial coverage).
    county_membership_df : pd.DataFrame, optional
        Override metro-county membership table.  If None, uses the
        built-in membership from ``coclab.metro.definitions``.

    Returns
    -------
    pd.DataFrame
        Metro-level PEP with columns:
        - ``metro_id``, ``year``, ``reference_date``, ``population``
        - ``coverage_ratio``, ``county_count``, ``max_county_contribution``
        - ``county_expected``, ``missing_counties``, ``definition_version``
    """
    # Build crosswalk
    if county_membership_df is not None:
        xwalk = county_membership_df.copy()
        if "area_share" not in xwalk.columns:
            xwalk["area_share"] = 1.0
    else:
        xwalk = _build_metro_county_crosswalk()

    # Count expected counties per metro
    expected_counts = xwalk.groupby("metro_id")["county_fips"].count()
    expected_counties = xwalk.groupby("metro_id")["county_fips"].apply(set)

    # Delegate to the generalized aggregation function
    result_df = aggregate_pep_counties(
        pep_df,
        xwalk,
        geo_id_col="metro_id",
        weighting=weighting,
        min_coverage=min_coverage,
    )

    # Add metro-specific coverage columns
    years = sorted(result_df["year"].unique())
    enriched_rows = []

    for _, row in result_df.iterrows():
        metro_id = row["metro_id"]
        n_expected = expected_counts.get(metro_id, 0)
        member_counties = expected_counties.get(metro_id, set())

        # Determine missing counties for this metro-year
        year_pep = pep_df[pep_df["year"] == row["year"]]
        available = set(year_pep["county_fips"].unique()) & member_counties
        missing = member_counties - available

        enriched_row = row.to_dict()
        enriched_row["county_expected"] = n_expected
        enriched_row["missing_counties"] = ",".join(sorted(missing)) if missing else ""
        enriched_rows.append(enriched_row)

    result_df = pd.DataFrame(enriched_rows)

    # Add definition version
    result_df["definition_version"] = definition_version

    # Sort
    result_df = result_df.sort_values(["metro_id", "year"]).reset_index(drop=True)

    logger.info(
        f"Metro PEP aggregation: {result_df['metro_id'].nunique()} metros, "
        f"{len(years)} years"
    )

    return result_df
