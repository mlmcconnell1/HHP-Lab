"""Panel assembly engine for CoC Lab Phase 3.

This module builds analysis-ready CoC x year panels by joining PIT counts
with ACS measures according to explicit alignment policies.

Panel Schema (Canonical Columns)
--------------------------------
- coc_id: CoC identifier (ST-NNN format)
- year: PIT year
- pit_total: Total homeless count
- pit_sheltered: Sheltered count (nullable)
- pit_unsheltered: Unsheltered count (nullable)
- boundary_vintage_used: Which boundary vintage was used
- acs_vintage_used: Which ACS vintage was used
- weighting_method: "area" or "population"
- total_population: From ACS measures
- adult_population: From ACS measures (nullable)
- population_below_poverty: From ACS measures (nullable)
- median_household_income: From ACS measures (nullable)
- median_gross_rent: From ACS measures (nullable)
- coverage_ratio: From ACS diagnostics (0-1)
- boundary_changed: bool - did boundary change from prior year?
- source: = "coclab_panel"

Usage
-----
    from coclab.panel.assemble import build_panel, save_panel

    # Build a panel for 2020-2024
    panel_df = build_panel(2020, 2024)

    # Save to curated output
    output_path = save_panel(panel_df, 2020, 2024)
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Literal

import pandas as pd

from coclab.panel.policies import AlignmentPolicy, DEFAULT_POLICY
from coclab.pit.registry import get_pit_path
from coclab.provenance import ProvenanceBlock, write_parquet_with_provenance

logger = logging.getLogger(__name__)

# Default paths
DEFAULT_PIT_DIR = Path("data/curated/pit")
DEFAULT_MEASURES_DIR = Path("data/curated/measures")
DEFAULT_PANEL_DIR = Path("data/curated/panel")

# Canonical panel columns in desired order
PANEL_COLUMNS = [
    "coc_id",
    "year",
    "pit_total",
    "pit_sheltered",
    "pit_unsheltered",
    "boundary_vintage_used",
    "acs_vintage_used",
    "weighting_method",
    "total_population",
    "adult_population",
    "population_below_poverty",
    "median_household_income",
    "median_gross_rent",
    "coverage_ratio",
    "boundary_changed",
    "source",
]


def _load_pit_for_year(
    year: int,
    pit_dir: Path | None = None,
) -> pd.DataFrame:
    """Load PIT data for a specific year.

    Attempts to load PIT data in the following order:
    1. From the PIT registry (if registered)
    2. From the curated PIT directory using canonical naming

    Parameters
    ----------
    year : int
        The PIT survey year.
    pit_dir : Path, optional
        Directory containing curated PIT files.
        Defaults to 'data/curated/pit'.

    Returns
    -------
    pd.DataFrame
        PIT data with columns: coc_id, pit_total, pit_sheltered, pit_unsheltered.
        Returns empty DataFrame if no data found.

    Notes
    -----
    The returned DataFrame will have coc_id as a string column and
    pit_total as an integer. pit_sheltered and pit_unsheltered may be
    nullable integers (Int64 dtype).
    """
    # If pit_dir is explicitly provided, use it directly (skip registry)
    # This supports testing with isolated data directories
    if pit_dir is not None:
        canonical_path = pit_dir / f"pit_counts__{year}.parquet"
        if canonical_path.exists():
            logger.info(f"Loading PIT {year} from provided path: {canonical_path}")
            df = pd.read_parquet(canonical_path)
        else:
            logger.warning(f"No PIT data found for year {year}")
            return pd.DataFrame(columns=["coc_id", "pit_total", "pit_sheltered", "pit_unsheltered"])
    else:
        # Try registry first
        registry_path = get_pit_path(year)
        if registry_path is not None and Path(registry_path).exists():
            logger.info(f"Loading PIT {year} from registry: {registry_path}")
            df = pd.read_parquet(registry_path)
        else:
            # Fall back to canonical path
            canonical_path = DEFAULT_PIT_DIR / f"pit_counts__{year}.parquet"
            if canonical_path.exists():
                logger.info(f"Loading PIT {year} from canonical path: {canonical_path}")
                df = pd.read_parquet(canonical_path)
            else:
                logger.warning(f"No PIT data found for year {year}")
                return pd.DataFrame(columns=["coc_id", "pit_total", "pit_sheltered", "pit_unsheltered"])

    # Standardize column names and select relevant columns
    if "pit_year" in df.columns:
        df = df[df["pit_year"] == year].copy()

    # Select and rename columns as needed
    result_cols = ["coc_id", "pit_total"]
    for col in ["pit_sheltered", "pit_unsheltered"]:
        if col in df.columns:
            result_cols.append(col)

    df = df[result_cols].copy()

    # Ensure proper dtypes
    df["coc_id"] = df["coc_id"].astype(str)
    df["pit_total"] = df["pit_total"].astype(int)

    return df


def _load_acs_measures(
    boundary_vintage: str,
    acs_vintage: str,
    weighting: Literal["area", "population"],
    measures_dir: Path | None = None,
) -> pd.DataFrame:
    """Load ACS measures for a specific vintage combination.

    Parameters
    ----------
    boundary_vintage : str
        CoC boundary vintage (e.g., "2024").
    acs_vintage : str
        ACS 5-year estimate end year (e.g., "2023").
    weighting : {"area", "population"}
        Weighting method used for aggregation.
    measures_dir : Path, optional
        Directory containing curated ACS measure files.
        Defaults to 'data/curated/measures'.

    Returns
    -------
    pd.DataFrame
        ACS measures with columns: coc_id, total_population, adult_population,
        population_below_poverty, median_household_income, median_gross_rent,
        coverage_ratio.
        Returns empty DataFrame if no data found.

    Notes
    -----
    The file naming convention is:
    coc_measures__{boundary_vintage}__{acs_vintage}.parquet

    If weighting-specific files exist, they will be preferred:
    coc_measures__{boundary_vintage}__{acs_vintage}__population.parquet
    """
    measures_dir = measures_dir or DEFAULT_MEASURES_DIR

    # Try weighting-specific file first
    weighting_path = measures_dir / f"coc_measures__{boundary_vintage}__{acs_vintage}__{weighting}.parquet"
    generic_path = measures_dir / f"coc_measures__{boundary_vintage}__{acs_vintage}.parquet"

    measures_path = None
    if weighting_path.exists():
        measures_path = weighting_path
    elif generic_path.exists():
        measures_path = generic_path

    if measures_path is None:
        logger.warning(
            f"No ACS measures found for boundary={boundary_vintage}, "
            f"acs={acs_vintage}, weighting={weighting}"
        )
        return pd.DataFrame(columns=[
            "coc_id",
            "total_population",
            "adult_population",
            "population_below_poverty",
            "median_household_income",
            "median_gross_rent",
            "coverage_ratio",
        ])

    logger.info(f"Loading ACS measures from: {measures_path}")
    df = pd.read_parquet(measures_path)

    # Filter by weighting method if column exists and file has multiple methods
    if "weighting_method" in df.columns:
        if weighting in df["weighting_method"].values:
            df = df[df["weighting_method"] == weighting].copy()

    # Select relevant columns
    result_cols = ["coc_id"]
    measure_cols = [
        "total_population",
        "adult_population",
        "population_below_poverty",
        "median_household_income",
        "median_gross_rent",
        "coverage_ratio",
    ]

    for col in measure_cols:
        if col in df.columns:
            result_cols.append(col)

    df = df[result_cols].copy()
    df["coc_id"] = df["coc_id"].astype(str)

    return df


def _detect_boundary_changes(df: pd.DataFrame) -> pd.Series:
    """Detect boundary changes between consecutive years for each CoC.

    A boundary change is detected when the boundary_vintage_used differs
    from the prior year's boundary_vintage_used for the same CoC.

    Parameters
    ----------
    df : pd.DataFrame
        Panel DataFrame with coc_id, year, and boundary_vintage_used columns.
        Must be sorted by coc_id and year.

    Returns
    -------
    pd.Series
        Boolean series indicating whether boundary changed from prior year.
        First year for each CoC will be False (no prior year to compare).

    Notes
    -----
    This detection is based on vintage labels, not actual geometry comparison.
    A True value indicates the boundary vintage changed, which typically
    corresponds to a boundary geometry change, though not always.
    """
    if df.empty:
        return pd.Series(dtype=bool)

    # Sort by CoC and year
    df = df.sort_values(["coc_id", "year"]).copy()

    # Get prior year's boundary vintage for each CoC
    df["_prior_vintage"] = df.groupby("coc_id")["boundary_vintage_used"].shift(1)

    # Boundary changed if vintage differs from prior (and prior exists)
    boundary_changed = (
        df["_prior_vintage"].notna() &
        (df["boundary_vintage_used"] != df["_prior_vintage"])
    )

    # Restore original index order
    return boundary_changed.reindex(df.index)


def build_panel(
    start_year: int,
    end_year: int,
    policy: AlignmentPolicy | None = None,
    pit_dir: Path | None = None,
    measures_dir: Path | None = None,
) -> pd.DataFrame:
    """Build analysis-ready CoC x year panel.

    Joins PIT counts with ACS measures for each year in the range,
    using the specified alignment policy to determine which boundary
    and ACS vintages to use.

    Parameters
    ----------
    start_year : int
        First PIT year to include (inclusive).
    end_year : int
        Last PIT year to include (inclusive).
    policy : AlignmentPolicy, optional
        Alignment policy for vintage selection.
        Defaults to DEFAULT_POLICY (same-year boundaries, ACS lag of 1).
    pit_dir : Path, optional
        Directory containing curated PIT files.
    measures_dir : Path, optional
        Directory containing curated ACS measure files.

    Returns
    -------
    pd.DataFrame
        Panel DataFrame with canonical columns:
        coc_id, year, pit_total, pit_sheltered, pit_unsheltered,
        boundary_vintage_used, acs_vintage_used, weighting_method,
        total_population, adult_population, population_below_poverty,
        median_household_income, median_gross_rent, coverage_ratio,
        boundary_changed, source.

    Raises
    ------
    ValueError
        If start_year > end_year.

    Notes
    -----
    - CoCs present in PIT data but missing from ACS measures will have
      null values for ACS columns.
    - CoCs present in ACS measures but missing from PIT data will NOT
      be included (PIT data is the anchor).
    - The boundary_changed column indicates whether the boundary vintage
      changed from the prior year for each CoC.
    """
    if start_year > end_year:
        raise ValueError(f"start_year ({start_year}) must be <= end_year ({end_year})")

    if policy is None:
        policy = DEFAULT_POLICY

    logger.info(f"Building panel for {start_year}-{end_year}")
    logger.info(f"Policy: weighting={policy.weighting_method}")

    year_dfs = []

    for year in range(start_year, end_year + 1):
        logger.info(f"Processing year {year}")

        # Determine vintages using policy
        boundary_vintage = policy.boundary_vintage_func(year)
        acs_vintage = policy.acs_vintage_func(year)
        weighting = policy.weighting_method

        logger.debug(
            f"Year {year}: boundary={boundary_vintage}, acs={acs_vintage}, "
            f"weighting={weighting}"
        )

        # Load PIT data
        pit_df = _load_pit_for_year(year, pit_dir=pit_dir)

        if pit_df.empty:
            logger.warning(f"No PIT data for year {year}, skipping")
            continue

        # Load ACS measures
        acs_df = _load_acs_measures(
            boundary_vintage=boundary_vintage,
            acs_vintage=acs_vintage,
            weighting=weighting,
            measures_dir=measures_dir,
        )

        # Start with PIT data as anchor
        year_df = pit_df.copy()
        year_df["year"] = year
        year_df["boundary_vintage_used"] = boundary_vintage
        year_df["acs_vintage_used"] = acs_vintage
        year_df["weighting_method"] = weighting
        year_df["source"] = "coclab_panel"

        # Left join with ACS measures (PIT is anchor)
        if not acs_df.empty:
            year_df = year_df.merge(acs_df, on="coc_id", how="left")
            logger.info(
                f"Year {year}: {len(pit_df)} PIT records, "
                f"{len(acs_df)} ACS records, "
                f"{year_df['total_population'].notna().sum()} matched"
            )
        else:
            # Add empty ACS columns
            for col in [
                "total_population",
                "adult_population",
                "population_below_poverty",
                "median_household_income",
                "median_gross_rent",
                "coverage_ratio",
            ]:
                year_df[col] = pd.NA

        year_dfs.append(year_df)

    if not year_dfs:
        logger.warning("No data found for any year in range")
        return pd.DataFrame(columns=PANEL_COLUMNS)

    # Combine all years
    panel_df = pd.concat(year_dfs, ignore_index=True)

    # Sort by CoC and year for boundary change detection
    panel_df = panel_df.sort_values(["coc_id", "year"]).reset_index(drop=True)

    # Detect boundary changes
    panel_df["boundary_changed"] = _detect_boundary_changes(panel_df)

    # Ensure all canonical columns exist
    for col in PANEL_COLUMNS:
        if col not in panel_df.columns:
            panel_df[col] = pd.NA

    # Reorder columns to canonical order
    panel_df = panel_df[PANEL_COLUMNS].copy()

    # Final dtype cleanup
    panel_df["coc_id"] = panel_df["coc_id"].astype(str)
    panel_df["year"] = panel_df["year"].astype(int)
    panel_df["pit_total"] = panel_df["pit_total"].astype(int)
    panel_df["boundary_vintage_used"] = panel_df["boundary_vintage_used"].astype(str)
    panel_df["acs_vintage_used"] = panel_df["acs_vintage_used"].astype(str)
    panel_df["weighting_method"] = panel_df["weighting_method"].astype(str)
    panel_df["source"] = panel_df["source"].astype(str)
    panel_df["boundary_changed"] = panel_df["boundary_changed"].astype(bool)

    # Nullable integer columns
    for col in ["pit_sheltered", "pit_unsheltered"]:
        if col in panel_df.columns:
            panel_df[col] = panel_df[col].astype("Int64")

    logger.info(
        f"Built panel: {len(panel_df)} rows, "
        f"{panel_df['coc_id'].nunique()} CoCs, "
        f"{panel_df['year'].nunique()} years"
    )

    return panel_df


def save_panel(
    df: pd.DataFrame,
    start_year: int,
    end_year: int,
    output_dir: Path | None = None,
    policy: AlignmentPolicy | None = None,
) -> Path:
    """Save panel DataFrame to Parquet with embedded provenance.

    Parameters
    ----------
    df : pd.DataFrame
        Panel DataFrame to save.
    start_year : int
        First year in the panel (for filename).
    end_year : int
        Last year in the panel (for filename).
    output_dir : Path, optional
        Output directory. Defaults to 'data/curated/panel'.
    policy : AlignmentPolicy, optional
        Policy used to build the panel (for provenance).

    Returns
    -------
    Path
        Path to the saved Parquet file.

    Notes
    -----
    Output filename: coc_panel__{start_year}_{end_year}.parquet

    Provenance metadata includes:
    - Panel date range
    - Row and CoC counts
    - Policy settings (if provided)
    """
    output_dir = output_dir or DEFAULT_PANEL_DIR
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    filename = f"coc_panel__{start_year}_{end_year}.parquet"
    output_path = output_dir / filename

    # Build provenance
    extra = {
        "dataset_type": "coc_panel",
        "start_year": start_year,
        "end_year": end_year,
        "row_count": len(df),
        "coc_count": int(df["coc_id"].nunique()) if not df.empty else 0,
        "year_count": int(df["year"].nunique()) if not df.empty else 0,
    }

    if policy is not None:
        extra["policy"] = policy.to_dict()

    # Extract vintages from data if available
    boundary_vintage = None
    acs_vintage = None
    weighting = None

    if not df.empty:
        boundary_vintages = df["boundary_vintage_used"].unique()
        if len(boundary_vintages) == 1:
            boundary_vintage = str(boundary_vintages[0])

        acs_vintages = df["acs_vintage_used"].unique()
        if len(acs_vintages) == 1:
            acs_vintage = str(acs_vintages[0])

        weighting_methods = df["weighting_method"].unique()
        if len(weighting_methods) == 1:
            weighting = str(weighting_methods[0])

    provenance = ProvenanceBlock(
        boundary_vintage=boundary_vintage,
        acs_vintage=acs_vintage,
        weighting=weighting,
        extra=extra,
    )

    write_parquet_with_provenance(df, output_path, provenance)
    logger.info(f"Saved panel to {output_path}")

    return output_path
