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
- tract_vintage_used: Which tract vintage was used for crosswalk
- alignment_type: period_faithful, retrospective, or custom
- weighting_method: "area" or "population"
- total_population: From ACS measures
- adult_population: From ACS measures (nullable)
- population_below_poverty: From ACS measures (nullable)
- median_household_income: From ACS measures (nullable)
- median_gross_rent: From ACS measures (nullable)
- coverage_ratio: From ACS diagnostics (0-1)
- boundary_changed: bool - did boundary change from prior year?
- source: = "coclab_panel"

ZORI-Extended Columns (when --include-zori is enabled)
------------------------------------------------------
- zori_coc: CoC-level ZORI (yearly)
- zori_coverage_ratio: Coverage of base geography weights
- zori_is_eligible: Boolean eligibility flag (reserved for Agent C)
- rent_to_income: ZORI divided by monthly median income

Usage
-----
    from coclab.panel.assemble import build_panel, save_panel

    # Build a panel for 2020-2024
    panel_df = build_panel(2020, 2024)

    # Save to curated output
    output_path = save_panel(panel_df, 2020, 2024)

    # Build a panel with ZORI integration
    panel_df = build_panel(2020, 2024, include_zori=True)
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Literal

import pandas as pd

from coclab import naming
from coclab.panel.policies import DEFAULT_POLICY, AlignmentPolicy
from coclab.panel.zori_eligibility import (
    DEFAULT_ZORI_MIN_COVERAGE,
    ZoriProvenance,
    add_provenance_columns,
    apply_zori_eligibility,
    compute_rent_to_income,
    summarize_zori_eligibility,
)
from coclab.pit.ingest import parse_pit_file
from coclab.pit.ingest.hud_exchange import MIN_PIT_YEAR as MIN_PIT_VINTAGE_YEAR
from coclab.pit.registry import get_pit_path
from coclab.provenance import ProvenanceBlock, read_provenance, write_parquet_with_provenance

logger = logging.getLogger(__name__)

# Default paths
DEFAULT_PIT_DIR = Path("data/curated/pit")
DEFAULT_MEASURES_DIR = Path("data/curated/measures")
DEFAULT_PANEL_DIR = Path("data/curated/panel")
DEFAULT_RENTS_DIR = Path("data/curated/zori")

# Canonical panel columns in desired order
PANEL_COLUMNS = [
    "coc_id",
    "year",
    "pit_total",
    "pit_sheltered",
    "pit_unsheltered",
    "boundary_vintage_used",
    "acs_vintage_used",
    "tract_vintage_used",
    "alignment_type",
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

# Additional columns added when ZORI is enabled
ZORI_COLUMNS = [
    "zori_coc",
    "zori_coverage_ratio",
    "zori_is_eligible",
    "zori_excluded_reason",
    "rent_to_income",
]

# Provenance columns added when ZORI is enabled
ZORI_PROVENANCE_COLUMNS = [
    "rent_metric",
    "rent_alignment",
    "zori_min_coverage",
]

# Default raw PIT directory
DEFAULT_RAW_PIT_DIR = Path("data/raw/pit")


def _find_latest_raw_vintage_file(
    raw_pit_dir: Path | None = None,
) -> tuple[Path, int] | None:
    """Find the most recent raw PIT vintage file.

    Parameters
    ----------
    raw_pit_dir : Path, optional
        Directory containing raw PIT vintage files.
        Defaults to 'data/raw/pit'.

    Returns
    -------
    tuple[Path, int] or None
        Tuple of (file_path, vintage_year) for the most recent vintage file,
        or None if no vintage files are found.
    """
    raw_pit_dir = raw_pit_dir or DEFAULT_RAW_PIT_DIR

    if not raw_pit_dir.exists():
        return None

    # Look for vintage directories (named by year)
    vintage_dirs = []
    for d in raw_pit_dir.iterdir():
        if d.is_dir() and d.name.isdigit():
            vintage_year = int(d.name)
            # Only consider vintages that have actual files
            files = list(d.glob("*.xls*"))
            if files:
                vintage_dirs.append((vintage_year, files[0]))

    if not vintage_dirs:
        return None

    # Return the highest vintage year
    vintage_dirs.sort(key=lambda x: x[0], reverse=True)
    latest_year, latest_file = vintage_dirs[0]
    return latest_file, latest_year


def _load_pit_from_raw_vintage(
    year: int,
    vintage_file: Path,
    vintage_year: int,
) -> pd.DataFrame:
    """Load PIT data for a specific year from a raw vintage file.

    Parameters
    ----------
    year : int
        The PIT year to extract.
    vintage_file : Path
        Path to the raw vintage Excel file.
    vintage_year : int
        The vintage year of the file (for logging/provenance).

    Returns
    -------
    pd.DataFrame
        PIT data with columns: coc_id, pit_total, pit_sheltered, pit_unsheltered.
        Returns empty DataFrame if extraction fails.
    """
    try:
        parse_result = parse_pit_file(
            file_path=vintage_file,
            year=year,
            source="hud_user",
            source_ref=f"vintage_{vintage_year}_fallback",
        )
        df = parse_result.df

        # Filter to just the requested year
        if "pit_year" in df.columns:
            df = df[df["pit_year"] == year].copy()

        # Select relevant columns
        result_cols = ["coc_id", "pit_total"]
        for col in ["pit_sheltered", "pit_unsheltered"]:
            if col in df.columns:
                result_cols.append(col)

        df = df[result_cols].copy()
        df["coc_id"] = df["coc_id"].astype(str)
        df["pit_total"] = df["pit_total"].astype(int)

        return df

    except Exception as e:
        logger.warning(f"Failed to extract year {year} from vintage file {vintage_file}: {e}")
        return pd.DataFrame(columns=["coc_id", "pit_total", "pit_sheltered", "pit_unsheltered"])


def _load_pit_for_year(
    year: int,
    pit_dir: Path | None = None,
    raw_pit_dir: Path | None = None,
) -> pd.DataFrame:
    """Load PIT data for a specific year.

    Attempts to load PIT data in the following order:
    1. From the PIT registry for the original vintage (if registered)
    2. From the curated PIT directory using canonical naming (original vintage)
    3. Fall back to extracting from the most recent raw vintage file

    For years before 2013 (where no original vintage file exists), the fallback
    to raw vintage files is the only option.

    Parameters
    ----------
    year : int
        The PIT survey year.
    pit_dir : Path, optional
        Directory containing curated PIT files.
        Defaults to 'data/curated/pit'.
    raw_pit_dir : Path, optional
        Directory containing raw PIT vintage files.
        Defaults to 'data/raw/pit'.

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

    When using fallback to raw vintage files, a message is printed to inform
    the user which vintage is being used.
    """
    df = None
    use_fallback = True  # Whether to try raw vintage fallback

    # If pit_dir is explicitly provided, use it directly (skip registry and fallback)
    # This supports testing with isolated data directories
    from coclab.naming import pit_filename

    if pit_dir is not None:
        use_fallback = False  # Explicit pit_dir disables fallback
        # Try new naming first, then legacy
        canonical_path = pit_dir / pit_filename(year)
        legacy_path = pit_dir / f"pit_counts__{year}.parquet"
        if canonical_path.exists():
            logger.info(f"Loading PIT {year} from provided path: {canonical_path}")
            df = pd.read_parquet(canonical_path)
        elif legacy_path.exists():
            logger.info(f"Loading PIT {year} from legacy path: {legacy_path}")
            df = pd.read_parquet(legacy_path)
    else:
        # Only try original vintage for years >= MIN_PIT_VINTAGE_YEAR (2013+)
        if year >= MIN_PIT_VINTAGE_YEAR:
            # Try registry first (for original vintage)
            registry_path = get_pit_path(year)
            if registry_path is not None and Path(registry_path).exists():
                logger.info(f"Loading PIT {year} from registry: {registry_path}")
                df = pd.read_parquet(registry_path)
            else:
                # Fall back to canonical path (try new naming first, then legacy)
                canonical_path = DEFAULT_PIT_DIR / pit_filename(year)
                legacy_path = DEFAULT_PIT_DIR / f"pit_counts__{year}.parquet"
                if canonical_path.exists():
                    logger.info(f"Loading PIT {year} from canonical path: {canonical_path}")
                    df = pd.read_parquet(canonical_path)
                elif legacy_path.exists():
                    logger.info(f"Loading PIT {year} from legacy path: {legacy_path}")
                    df = pd.read_parquet(legacy_path)

    # If no original vintage found, fall back to raw vintage file
    if df is None and use_fallback:
        raw_vintage = _find_latest_raw_vintage_file(raw_pit_dir)
        if raw_vintage is not None:
            vintage_file, vintage_year = raw_vintage
            # Only use vintage file if it contains data for our year
            if year <= vintage_year:
                print(
                    f"[info] No original vintage PIT file for {year}; "
                    f"using {vintage_year} vintage file"
                )
                logger.info(
                    f"Falling back to raw vintage file for year {year}: "
                    f"{vintage_file} (vintage {vintage_year})"
                )
                df = _load_pit_from_raw_vintage(year, vintage_file, vintage_year)

    if df is None or df.empty:
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
) -> tuple[pd.DataFrame, str | None]:
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
    tuple[pd.DataFrame, str | None]
        Tuple of (DataFrame, tract_vintage) where:
        - DataFrame contains ACS measures with columns: coc_id, total_population,
          adult_population, population_below_poverty, median_household_income,
          median_gross_rent, coverage_ratio. Returns empty DataFrame if no data found.
        - tract_vintage is the tract vintage from provenance metadata, or None if
          not available.

    Notes
    -----
    Supports both new temporal shorthand naming (measures__A{acs}@B{boundary}.parquet)
    and legacy naming (coc_measures__{boundary}__{acs}.parquet). Tries new naming first,
    then falls back to legacy naming.
    """
    measures_dir = measures_dir or DEFAULT_MEASURES_DIR

    # Try new naming conventions in order:
    # 1. Without tract: measures__A{acs}@B{boundary}.parquet
    # 2. With tract: measures__A{acs}@B{boundary}xT*.parquet (glob pattern)
    # 3. Legacy formats
    new_path_no_tract = measures_dir / naming.measures_filename(acs_vintage, boundary_vintage)

    # Also check for files with tract suffix using glob
    acs_year = naming._normalize_acs_vintage(acs_vintage)
    tract_pattern = f"measures__A{acs_year}@B{boundary_vintage}xT*.parquet"
    tract_matches = list(measures_dir.glob(tract_pattern))

    # Legacy paths
    weighting_fname = f"coc_measures__{boundary_vintage}__{acs_vintage}__{weighting}.parquet"
    legacy_weighting_path = measures_dir / weighting_fname
    legacy_generic_path = measures_dir / f"coc_measures__{boundary_vintage}__{acs_vintage}.parquet"

    measures_path = None
    if new_path_no_tract.exists():
        measures_path = new_path_no_tract
    elif tract_matches:
        # Use the first match (there should typically be only one)
        measures_path = tract_matches[0]
        if len(tract_matches) > 1:
            logger.warning(
                f"Multiple measures files found for boundary={boundary_vintage}, "
                f"acs={acs_vintage}. Using: {measures_path.name}"
            )
    elif legacy_weighting_path.exists():
        measures_path = legacy_weighting_path
    elif legacy_generic_path.exists():
        measures_path = legacy_generic_path

    if measures_path is None:
        logger.warning(
            f"No ACS measures found for boundary={boundary_vintage}, "
            f"acs={acs_vintage}, weighting={weighting}"
        )
        return pd.DataFrame(
            columns=[
                "coc_id",
                "total_population",
                "adult_population",
                "population_below_poverty",
                "median_household_income",
                "median_gross_rent",
                "coverage_ratio",
            ]
        ), None

    logger.info(f"Loading ACS measures from: {measures_path}")
    df = pd.read_parquet(measures_path)

    # Extract tract_vintage from provenance metadata
    tract_vintage = None
    provenance = read_provenance(measures_path)
    if provenance is not None:
        tract_vintage = provenance.tract_vintage

    # Filter by weighting method if column exists and file has multiple methods
    if "weighting_method" in df.columns:
        df_weighted = df[df["weighting_method"] == weighting].copy()
        if df_weighted.empty:
            logger.warning(
                f"ACS measures file has no rows for weighting={weighting}; "
                "returning empty dataset to avoid mixing weightings"
            )
            return pd.DataFrame(
                columns=[
                    "coc_id",
                    "total_population",
                    "adult_population",
                    "population_below_poverty",
                    "median_household_income",
                    "median_gross_rent",
                    "coverage_ratio",
                ]
            ), None
        df = df_weighted

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

    return df, tract_vintage


def _load_zori_yearly(
    zori_yearly_path: Path | str | None = None,
    rents_dir: Path | None = None,
) -> pd.DataFrame | None:
    """Load yearly ZORI data for panel integration.

    Attempts to load yearly ZORI data from the provided path or by
    discovering the latest available file in the rents directory.

    Parameters
    ----------
    zori_yearly_path : Path or str, optional
        Explicit path to yearly ZORI parquet file.
        If provided, this takes precedence over auto-discovery.
    rents_dir : Path, optional
        Directory to search for yearly ZORI files.
        Defaults to 'data/curated/zori'.

    Returns
    -------
    pd.DataFrame or None
        ZORI yearly data with columns: coc_id, year, zori_coc, coverage_ratio.
        Returns None if no data is found.

    Notes
    -----
    When auto-discovering, looks for files matching patterns (in order):
    1. zori_yearly__A*@B*xC*__w*__m*.parquet (new naming)
    2. coc_zori_yearly__*.parquet (legacy naming)

    The coverage_ratio column from ZORI is renamed to zori_coverage_ratio
    to avoid collision with the ACS coverage_ratio column.
    """
    rents_dir = rents_dir or DEFAULT_RENTS_DIR

    if zori_yearly_path is not None:
        path = Path(zori_yearly_path)
        if not path.exists():
            logger.warning(f"Specified ZORI yearly path not found: {path}")
            return None
        logger.info(f"Loading ZORI yearly from explicit path: {path}")
    else:
        # Auto-discover: try new naming first, then legacy
        # New naming: zori_yearly__A*@B*xC*__w*__m*.parquet
        new_pattern = "zori_yearly__A*.parquet"
        candidates = sorted(rents_dir.glob(new_pattern), reverse=True)

        if not candidates:
            # Fall back to legacy naming: coc_zori_yearly__*.parquet
            legacy_pattern = "coc_zori_yearly__*.parquet"
            candidates = sorted(rents_dir.glob(legacy_pattern), reverse=True)

        if not candidates:
            logger.info(f"No yearly ZORI files found in {rents_dir}")
            return None
        # Use the most recent (alphabetically last, which is typically newest vintage)
        path = candidates[0]
        logger.info(f"Auto-discovered ZORI yearly file: {path}")

    df = pd.read_parquet(path)

    # Validate required columns
    required_cols = {"coc_id", "year", "zori_coc", "coverage_ratio"}
    missing_cols = required_cols - set(df.columns)
    if missing_cols:
        logger.warning(
            f"ZORI yearly file missing required columns: {missing_cols}. "
            f"Available columns: {list(df.columns)}"
        )
        return None

    # Rename columns to avoid collision with ACS columns
    rename_map = {"coverage_ratio": "zori_coverage_ratio"}
    if "max_geo_contribution" in df.columns:
        rename_map["max_geo_contribution"] = "zori_max_geo_contribution"
    df = df.rename(columns=rename_map)

    # Select relevant columns
    result_cols = ["coc_id", "year", "zori_coc", "zori_coverage_ratio"]
    # Include optional metadata columns if present
    for col in ["method", "zori_max_geo_contribution", "geo_count"]:
        if col in df.columns:
            result_cols.append(col)

    df = df[result_cols].copy()
    df["coc_id"] = df["coc_id"].astype(str)
    df["year"] = df["year"].astype(int)

    logger.info(
        f"Loaded ZORI yearly: {len(df)} rows, "
        f"{df['coc_id'].nunique()} CoCs, "
        f"{df['year'].nunique()} years"
    )

    return df


def _compute_rent_to_income(
    zori_coc: pd.Series,
    median_household_income: pd.Series,
) -> pd.Series:
    """Compute rent-to-income ratio with proper null handling.

    The rent-to-income ratio is computed as:
        rent_to_income = zori_coc / (median_household_income / 12.0)

    This represents the fraction of monthly income that would be spent on rent.

    Parameters
    ----------
    zori_coc : pd.Series
        CoC-level ZORI (monthly rent).
    median_household_income : pd.Series
        Annual median household income.

    Returns
    -------
    pd.Series
        Rent-to-income ratio with null values where:
        - zori_coc is null
        - median_household_income is null or zero

    Notes
    -----
    A ratio of 0.30 means 30% of monthly income is spent on rent.
    HUD considers >0.30 as "cost-burdened" and >0.50 as "severely cost-burdened".
    """

    # Compute monthly income
    monthly_income = median_household_income / 12.0

    # Initialize result with nulls
    result = pd.Series(index=zori_coc.index, dtype=float)

    # Mask for valid computation:
    # - zori_coc must not be null
    # - median_household_income must not be null
    # - median_household_income must not be zero
    valid_mask = zori_coc.notna() & median_household_income.notna() & (median_household_income != 0)

    # Compute only for valid rows
    result.loc[valid_mask] = zori_coc.loc[valid_mask] / monthly_income.loc[valid_mask]

    return result


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
    boundary_changed = df["_prior_vintage"].notna() & (
        df["boundary_vintage_used"] != df["_prior_vintage"]
    )

    # Restore original index order
    return boundary_changed.reindex(df.index)


def _determine_alignment_type(pit_year: int, boundary_vintage: str) -> str:
    """Classify alignment type for a PIT year and boundary vintage.

    Returns:
    - period_faithful: boundary vintage matches PIT year.
    - retrospective: boundary vintage is newer or same-year than PIT year.
    - custom: boundary vintage is older or non-numeric.
    """
    if boundary_vintage == str(pit_year):
        return "period_faithful"

    try:
        boundary_year = int(boundary_vintage)
    except (TypeError, ValueError):
        return "custom"

    if boundary_year >= pit_year:
        return "retrospective"

    return "custom"


def build_panel(
    start_year: int,
    end_year: int,
    policy: AlignmentPolicy | None = None,
    pit_dir: Path | None = None,
    measures_dir: Path | None = None,
    include_zori: bool = False,
    zori_yearly_path: Path | str | None = None,
    rents_dir: Path | None = None,
    zori_min_coverage: float = DEFAULT_ZORI_MIN_COVERAGE,
) -> pd.DataFrame:
    """Build analysis-ready CoC x year panel.

    Joins PIT counts with ACS measures for each year in the range,
    using the specified alignment policy to determine which boundary
    and ACS vintages to use. Optionally includes ZORI rent data and
    computes the rent_to_income derived metric.

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
    include_zori : bool, optional
        If True, include ZORI data and compute rent_to_income.
        Default is False.
    zori_yearly_path : Path or str, optional
        Explicit path to yearly ZORI parquet file.
        If None and include_zori=True, auto-discovers from rents_dir.
    rents_dir : Path, optional
        Directory containing curated rent files.
        Defaults to 'data/curated/zori'.
    zori_min_coverage : float, optional
        Minimum coverage ratio for ZORI eligibility. CoC-years with coverage
        below this threshold will have zori_coc and rent_to_income set to null.
        Default is 0.90 (90%).

    Returns
    -------
    pd.DataFrame
        Panel DataFrame with canonical columns:
        coc_id, year, pit_total, pit_sheltered, pit_unsheltered,
        boundary_vintage_used, acs_vintage_used, tract_vintage_used,
        alignment_type, weighting_method, total_population, adult_population,
        population_below_poverty, median_household_income, median_gross_rent,
        coverage_ratio, boundary_changed, source.

        When include_zori=True, also includes:
        zori_coc, zori_coverage_ratio, zori_is_eligible, zori_excluded_reason,
        rent_to_income, rent_metric, rent_alignment, zori_min_coverage.

    Raises
    ------
    ValueError
        If start_year > end_year.
        If include_zori=True but no ZORI data is available.

    Notes
    -----
    - CoCs present in PIT data but missing from ACS measures will have
      null values for ACS columns.
    - CoCs present in ACS measures but missing from PIT data will NOT
      be included (PIT data is the anchor).
    - The boundary_changed column indicates whether the boundary vintage
      changed from the prior year for each CoC.
    - When ZORI is included, rent_to_income is computed as:
      zori_coc / (median_household_income / 12.0)
    - rent_to_income is null when zori_coc is null, income is null, or
      income is zero.
    - ZORI eligibility is determined by coverage_ratio >= zori_min_coverage.
    - CoCs with zero coverage are explicitly excluded (not imputed).
    - High dominance generates warnings but is NOT a hard exclusion.
    """
    if start_year > end_year:
        raise ValueError(f"start_year ({start_year}) must be <= end_year ({end_year})")

    if policy is None:
        policy = DEFAULT_POLICY

    logger.info(f"Building panel for {start_year}-{end_year}")
    logger.info(f"Policy: weighting={policy.weighting_method}")

    # Load ZORI data if requested
    zori_df = None
    if include_zori:
        logger.info("ZORI integration enabled, loading yearly ZORI data...")
        zori_df = _load_zori_yearly(
            zori_yearly_path=zori_yearly_path,
            rents_dir=rents_dir,
        )
        if zori_df is None:
            raise ValueError(
                "ZORI integration requested but no ZORI yearly data available. "
                "Run 'coclab aggregate-zori --to-yearly' first, or provide "
                "--zori-yearly-path explicitly."
            )

    year_dfs = []

    for year in range(start_year, end_year + 1):
        logger.info(f"Processing year {year}")

        # Determine vintages using policy
        boundary_vintage = policy.boundary_vintage_func(year)
        acs_vintage = policy.acs_vintage_func(year)
        weighting = policy.weighting_method

        logger.debug(
            f"Year {year}: boundary={boundary_vintage}, acs={acs_vintage}, weighting={weighting}"
        )

        # Load PIT data
        pit_df = _load_pit_for_year(year, pit_dir=pit_dir)

        if pit_df.empty:
            logger.warning(f"No PIT data for year {year}, skipping")
            continue

        # Load ACS measures (returns DataFrame and tract_vintage from provenance)
        acs_df, tract_vintage = _load_acs_measures(
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
        year_df["tract_vintage_used"] = tract_vintage
        year_df["alignment_type"] = _determine_alignment_type(year, boundary_vintage)
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

    # =========================================================================
    # ZORI Integration (if enabled)
    # =========================================================================
    if include_zori and zori_df is not None:
        logger.info("Integrating ZORI data into panel...")

        # Left join with ZORI yearly data
        panel_df = panel_df.merge(
            zori_df,
            on=["coc_id", "year"],
            how="left",
        )

        # Determine the rent alignment method from the ZORI data
        rent_alignment = "pit_january"  # Default
        if "method" in zori_df.columns:
            methods = zori_df["method"].dropna().unique()
            if len(methods) == 1:
                rent_alignment = methods[0]

        # Apply ZORI eligibility rules (Agent C logic)
        # This adds zori_is_eligible, zori_excluded_reason, and nulls out
        # ineligible rows
        panel_df = apply_zori_eligibility(
            panel_df,
            zori_col="zori_coc",
            coverage_col="zori_coverage_ratio",
            min_coverage=zori_min_coverage,
            dominance_col="zori_max_geo_contribution",
        )

        # Compute rent_to_income for eligible rows
        panel_df = compute_rent_to_income(
            panel_df,
            zori_col="zori_coc",
            income_col="median_household_income",
            eligibility_col="zori_is_eligible",
        )

        # Create provenance metadata for ZORI integration
        zori_provenance = ZoriProvenance(
            rent_alignment=rent_alignment,
            zori_min_coverage=zori_min_coverage,
        )

        # Add provenance columns
        panel_df = add_provenance_columns(panel_df, zori_provenance)

        # Remove temporary columns that shouldn't be in final output
        cols_to_drop = ["method", "geo_count"]
        for col in cols_to_drop:
            if col in panel_df.columns:
                panel_df = panel_df.drop(columns=[col])

        # Log ZORI integration summary
        summary = summarize_zori_eligibility(panel_df)
        logger.info(
            f"ZORI integration complete: "
            f"{summary.get('zori_eligible_count', 0)} eligible observations "
            f"({summary.get('zori_eligible_pct', 0):.1f}%)"
        )

        # Add ZORI columns to the ordering
        all_columns = PANEL_COLUMNS + ZORI_COLUMNS + ZORI_PROVENANCE_COLUMNS
        # Also keep zori_max_geo_contribution if present
        if "zori_max_geo_contribution" in panel_df.columns:
            all_columns.append("zori_max_geo_contribution")
    else:
        all_columns = PANEL_COLUMNS

    # Ensure all required columns exist
    for col in all_columns:
        if col not in panel_df.columns:
            panel_df[col] = pd.NA

    # Reorder columns to canonical order (only keep columns that exist)
    final_columns = [col for col in all_columns if col in panel_df.columns]
    panel_df = panel_df[final_columns].copy()

    # Final dtype cleanup
    panel_df["coc_id"] = panel_df["coc_id"].astype(str)
    panel_df["year"] = panel_df["year"].astype(int)
    panel_df["pit_total"] = panel_df["pit_total"].astype(int)
    panel_df["boundary_vintage_used"] = panel_df["boundary_vintage_used"].astype(str)
    panel_df["acs_vintage_used"] = panel_df["acs_vintage_used"].astype(str)
    # tract_vintage_used may be None if provenance not available, use nullable string
    if "tract_vintage_used" in panel_df.columns:
        panel_df["tract_vintage_used"] = panel_df["tract_vintage_used"].astype("string")
    if "alignment_type" in panel_df.columns:
        panel_df["alignment_type"] = panel_df["alignment_type"].astype("string")
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
    zori_provenance: ZoriProvenance | None = None,
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
    zori_provenance : ZoriProvenance, optional
        ZORI integration provenance (for provenance).

    Returns
    -------
    Path
        Path to the saved Parquet file.

    Notes
    -----
    Output filename: panel__Y{start}-{end}@B{boundary}.parquet

    Provenance metadata includes:
    - Panel date range
    - Row and CoC counts
    - Policy settings (if provided)
    - ZORI provenance (if provided)
    """
    from coclab.naming import panel_filename

    output_dir = output_dir or DEFAULT_PANEL_DIR
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Extract vintages from data if available
    boundary_vintage = None
    acs_vintage = None
    weighting = None

    if not df.empty:
        boundary_vintages = df["boundary_vintage_used"].unique()
        if len(boundary_vintages) == 1:
            boundary_vintage = str(boundary_vintages[0])
        else:
            # Use the most common boundary vintage if multiple exist
            boundary_vintage = str(df["boundary_vintage_used"].mode().iloc[0])

    # Generate filename with temporal shorthand
    if boundary_vintage:
        filename = panel_filename(start_year, end_year, boundary_vintage)
    else:
        # Fallback if no boundary vintage can be determined
        filename = f"panel__Y{start_year}-{end_year}.parquet"
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

    # Add ZORI provenance if provided
    if zori_provenance is not None:
        extra["zori"] = zori_provenance.to_dict()
        # Also add ZORI eligibility summary from the data
        summary = summarize_zori_eligibility(df)
        if summary.get("zori_integrated"):
            extra["zori_summary"] = summary

    # Extract remaining vintages from data
    if not df.empty:
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
