"""Metro-level PIT aggregation from CoC-native PIT counts.

PIT (Point-in-Time) data is collected at the CoC level.  Metro PIT
totals are derived by summing CoC PIT values for all member CoCs
defined in the metro-CoC membership table.

For multi-CoC metros (e.g., LA with CA-600/606/607/612), this module
sums ``pit_total``, ``pit_sheltered``, and ``pit_unsheltered`` across
all member CoCs.  For single-CoC metros, it is a 1:1 passthrough.

Coverage tracking: when a member CoC lacks PIT data for a given year,
``coc_coverage_ratio`` records the fraction of member CoCs with data,
and ``missing_cocs`` lists the absent identifiers.
"""

from __future__ import annotations

import pandas as pd

from hhplab.metro.metro_definitions import (
    DEFINITION_VERSION,
    build_coc_membership_df,
)


def aggregate_pit_to_metro(
    pit_df: pd.DataFrame,
    *,
    definition_version: str = DEFINITION_VERSION,
    coc_membership_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Aggregate CoC-level PIT counts to Glynn/Fox metro areas.

    Parameters
    ----------
    pit_df : pd.DataFrame
        CoC-level PIT data with columns:
        - ``coc_id``: CoC identifier (e.g., "NY-600")
        - ``pit_year`` or ``year``: PIT count year
        - ``pit_total``: Total persons experiencing homelessness
        - ``pit_sheltered``: Sheltered count (optional, nullable)
        - ``pit_unsheltered``: Unsheltered count (optional, nullable)
    definition_version : str
        Metro definition version to use.  Defaults to the current
        canonical version (``glynn_fox_v1``).
    coc_membership_df : pd.DataFrame, optional
        Override metro-CoC membership table.  If None, uses the
        built-in membership from ``hhplab.metro.metro_definitions``.

    Returns
    -------
    pd.DataFrame
        Metro-level PIT with columns:
        - ``metro_id``: Glynn/Fox metro identifier (e.g., "GF01")
        - ``year``: PIT year
        - ``pit_total``: Summed total across member CoCs
        - ``pit_sheltered``: Summed sheltered (nullable)
        - ``pit_unsheltered``: Summed unsheltered (nullable)
        - ``coc_count``: Number of member CoCs with data
        - ``coc_expected``: Total member CoCs for this metro
        - ``coc_coverage_ratio``: Fraction of member CoCs with data
        - ``missing_cocs``: Comma-separated list of missing CoC IDs
        - ``definition_version``: Metro definition version
    """
    # Load membership
    if coc_membership_df is None:
        membership = build_coc_membership_df()
    else:
        membership = coc_membership_df.copy()

    # Normalize year column name
    df = pit_df.copy()
    if "pit_year" in df.columns and "year" not in df.columns:
        df = df.rename(columns={"pit_year": "year"})
    if "year" not in df.columns:
        raise ValueError(
            f"PIT data must have 'year' or 'pit_year' column. Available: {list(df.columns)}"
        )

    # Ensure required columns
    if "coc_id" not in df.columns:
        raise ValueError(f"PIT data must have 'coc_id' column. Available: {list(df.columns)}")
    if "pit_total" not in df.columns:
        raise ValueError(f"PIT data must have 'pit_total' column. Available: {list(df.columns)}")

    # Count expected CoCs per metro
    expected_counts = membership.groupby("metro_id")["coc_id"].count()
    expected_cocs = membership.groupby("metro_id")["coc_id"].apply(set)

    # Get all years in the PIT data
    years = sorted(df["year"].unique())

    # Aggregate
    results = []

    for year in years:
        year_pit = df[df["year"] == year].copy()

        for metro_id in expected_counts.index:
            member_cocs = expected_cocs[metro_id]
            n_expected = expected_counts[metro_id]

            # Find PIT rows for member CoCs
            metro_pit = year_pit[year_pit["coc_id"].isin(member_cocs)]

            # Track coverage
            cocs_with_data = set(metro_pit["coc_id"].unique())
            missing = member_cocs - cocs_with_data
            n_found = len(cocs_with_data)
            coverage = n_found / n_expected if n_expected > 0 else 0.0

            if n_found == 0:
                results.append(
                    {
                        "metro_id": metro_id,
                        "year": year,
                        "pit_total": None,
                        "pit_sheltered": None,
                        "pit_unsheltered": None,
                        "coc_count": 0,
                        "coc_expected": n_expected,
                        "coc_coverage_ratio": 0.0,
                        "missing_cocs": ",".join(sorted(missing)),
                    }
                )
                continue

            # Sum PIT counts across member CoCs
            pit_total = metro_pit["pit_total"].sum()

            pit_sheltered = None
            if "pit_sheltered" in metro_pit.columns:
                sheltered_vals = metro_pit["pit_sheltered"].dropna()
                if len(sheltered_vals) > 0:
                    pit_sheltered = int(sheltered_vals.sum())

            pit_unsheltered = None
            if "pit_unsheltered" in metro_pit.columns:
                unsheltered_vals = metro_pit["pit_unsheltered"].dropna()
                if len(unsheltered_vals) > 0:
                    pit_unsheltered = int(unsheltered_vals.sum())

            results.append(
                {
                    "metro_id": metro_id,
                    "year": year,
                    "pit_total": int(pit_total),
                    "pit_sheltered": pit_sheltered,
                    "pit_unsheltered": pit_unsheltered,
                    "coc_count": n_found,
                    "coc_expected": n_expected,
                    "coc_coverage_ratio": coverage,
                    "missing_cocs": ",".join(sorted(missing)) if missing else "",
                }
            )

    result_df = pd.DataFrame(results)

    # Add definition version
    result_df["definition_version"] = definition_version

    # Use nullable integer types for PIT counts
    for col in ("pit_total", "pit_sheltered", "pit_unsheltered"):
        if col in result_df.columns:
            result_df[col] = result_df[col].astype("Int64")

    result_df = result_df.sort_values(["metro_id", "year"]).reset_index(drop=True)

    return result_df
