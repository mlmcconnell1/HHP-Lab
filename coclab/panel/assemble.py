"""Panel assembly engine for CoC Lab Phase 3.

This module builds analysis-ready analysis-geography x year panels by joining
PIT counts with ACS measures according to explicit alignment policies.

Panel Schema (Canonical Columns)
--------------------------------
- coc panel: uses ``PANEL_COLUMNS`` and preserves ``coc_id`` for compatibility
- metro panel: uses ``METRO_PANEL_COLUMNS`` with ``metro_id`` plus canonical
  ``geo_type`` / ``geo_id`` columns
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
from typing import TYPE_CHECKING, Literal

import pandas as pd

from coclab import naming
from coclab.analysis_geo import (
    GEO_ID_COL,
    GEO_TYPE_COL,
    GEO_TYPE_COC,
    GEO_TYPE_METRO,
    ensure_canonical_geo_columns,
    resolve_geo_col,
)
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

if TYPE_CHECKING:
    from coclab.panel.conformance import ConformanceReport

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

METRO_PANEL_COLUMNS = [
    "metro_id",
    "geo_type",
    "geo_id",
    "year",
    "pit_total",
    "pit_sheltered",
    "pit_unsheltered",
    "definition_version_used",
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


def _panel_geo_id_col(geo_type: str) -> str:
    """Return the native identifier column for a panel geo type."""
    if geo_type == GEO_TYPE_METRO:
        return "metro_id"
    return "coc_id"


def _panel_columns_for_geo_type(geo_type: str) -> list[str]:
    """Return the canonical ordered panel columns for a geo type."""
    if geo_type == GEO_TYPE_METRO:
        return METRO_PANEL_COLUMNS
    return PANEL_COLUMNS


def _panel_dataset_type(geo_type: str) -> str:
    """Return the provenance dataset type token for a panel."""
    if geo_type == GEO_TYPE_METRO:
        return "metro_panel"
    return "coc_panel"


def _align_label(geo_type: str) -> str:
    """Return the alignment label for a geography family."""
    if geo_type == GEO_TYPE_METRO:
        return "definition_fixed"
    return "boundary_aligned"


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
    *,
    geo_type: str = GEO_TYPE_COC,
    definition_version: str | None = None,
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
        PIT data with geography-native ID column, pit_total, pit_sheltered,
        pit_unsheltered.
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

    if geo_type == GEO_TYPE_METRO:
        if definition_version is None:
            raise ValueError("definition_version is required for geo_type='metro'")

        geo_col = _panel_geo_id_col(geo_type)
        if pit_dir is None:
            pit_dir = DEFAULT_PIT_DIR

        exact_path = pit_dir / naming.metro_pit_filename(year, definition_version)
        candidates: list[Path] = []
        if exact_path.exists():
            candidates = [exact_path]
        else:
            candidates = sorted(pit_dir.glob(f"pit__metro__P{year}@D*.parquet"))

        if not candidates:
            logger.warning(
                f"No metro PIT data found for year {year} and definition {definition_version}"
            )
            return pd.DataFrame(
                columns=[geo_col, "pit_total", "pit_sheltered", "pit_unsheltered"]
            )

        df = pd.read_parquet(candidates[0])
        if "pit_year" in df.columns and "year" not in df.columns:
            df = df.rename(columns={"pit_year": "year"})
        if "year" in df.columns:
            df = df[df["year"] == year].copy()

        if geo_col not in df.columns:
            if GEO_ID_COL in df.columns:
                df = df.rename(columns={GEO_ID_COL: geo_col})
            else:
                raise ValueError(
                    f"Metro PIT file {candidates[0]} is missing '{geo_col}' column."
                )

        result_cols = [geo_col, "pit_total"]
        for col in ["pit_sheltered", "pit_unsheltered"]:
            if col in df.columns:
                result_cols.append(col)
        df = df[result_cols].copy()
        df[geo_col] = df[geo_col].astype(str)
        df["pit_total"] = pd.to_numeric(df["pit_total"], errors="raise").astype(int)
        for col in ["pit_sheltered", "pit_unsheltered"]:
            if col in df.columns:
                df[col] = df[col].astype("Int64")
        return df

    # If pit_dir is explicitly provided, use it directly (skip registry and fallback)
    # This supports testing with isolated data directories
    from coclab.naming import pit_filename

    if pit_dir is not None:
        use_fallback = False  # Explicit pit_dir disables fallback
        # Try new naming first, then build-scoped (@B suffix), then legacy
        canonical_path = pit_dir / pit_filename(year)
        legacy_path = pit_dir / f"pit_counts__{year}.parquet"
        if canonical_path.exists():
            logger.info(f"Loading PIT {year} from provided path: {canonical_path}")
            df = pd.read_parquet(canonical_path)
        else:
            # Try build-scoped naming: pit__P{year}@B{boundary}.parquet
            scoped_matches = sorted(pit_dir.glob(f"pit__P{year}@B*.parquet"))
            if scoped_matches:
                logger.info(f"Loading PIT {year} from build-scoped path: {scoped_matches[0]}")
                df = pd.read_parquet(scoped_matches[0])
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
    boundary_vintage: str | None,
    acs_vintage: str,
    weighting: Literal["area", "population"],
    measures_dir: Path | None = None,
    *,
    geo_type: str = GEO_TYPE_COC,
    definition_version: str | None = None,
) -> tuple[pd.DataFrame, str | None]:
    """Load ACS measures for a specific vintage combination.

    Parameters
    ----------
    boundary_vintage : str
        CoC boundary vintage (e.g., "2024"). Ignored for metro targets.
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
        - DataFrame contains ACS measures with geography-native ID column,
          total_population,
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
    geo_col = _panel_geo_id_col(geo_type)

    if geo_type == GEO_TYPE_METRO:
        if definition_version is None:
            raise ValueError("definition_version is required for geo_type='metro'")

        exact_path = measures_dir / naming.metro_measures_filename(
            acs_vintage,
            definition_version,
        )
        acs_year = naming._normalize_acs_vintage(acs_vintage)
        tract_pattern = f"measures__metro__A{acs_year}@D*xT*.parquet"
        tract_matches = list(measures_dir.glob(tract_pattern))
        measures_path = None
        if exact_path.exists():
            measures_path = exact_path
        elif tract_matches:
            measures_path = tract_matches[0]

        if measures_path is None:
            logger.warning(
                f"No metro ACS measures found for acs={acs_vintage}, "
                f"definition={definition_version}, weighting={weighting}"
            )
            return pd.DataFrame(
                columns=[
                    geo_col,
                    "total_population",
                    "adult_population",
                    "population_below_poverty",
                    "median_household_income",
                    "median_gross_rent",
                    "coverage_ratio",
                ]
            ), None

        logger.info(f"Loading metro ACS measures from: {measures_path}")
        df = pd.read_parquet(measures_path)
        provenance = read_provenance(measures_path)
        tract_vintage = provenance.tract_vintage if provenance is not None else None

        if geo_col not in df.columns:
            if GEO_ID_COL in df.columns:
                df = df.rename(columns={GEO_ID_COL: geo_col})
            else:
                raise ValueError(
                    f"Metro ACS measures file {measures_path.name} is missing "
                    f"'{geo_col}' column."
                )

        if "weighting_method" in df.columns:
            df_weighted = df[df["weighting_method"] == weighting].copy()
            if df_weighted.empty:
                available = sorted(df["weighting_method"].unique())
                raise ValueError(
                    f"Metro ACS measures file {measures_path.name} has no rows for "
                    f"weighting={weighting}; available weightings: {available}. "
                    f"Re-run metro ACS aggregation with weighting {weighting}."
                )
            df = df_weighted

        result_cols = [geo_col]
        for col in [
            "total_population",
            "adult_population",
            "population_below_poverty",
            "median_household_income",
            "median_gross_rent",
            "coverage_ratio",
        ]:
            if col in df.columns:
                result_cols.append(col)

        df = df[result_cols].copy()
        df[geo_col] = df[geo_col].astype(str)
        return df, tract_vintage

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

    # Fallback: discover any measures file for this boundary vintage
    # regardless of ACS vintage. This handles the case where aggregate
    # acs uses a different ACS alignment than the panel policy expects
    # (e.g., aggregate produces A2024@B2024 but panel asks for A2023@B2024).
    if measures_path is None:
        boundary_matches = sorted(
            measures_dir.glob(f"measures__A*@B{boundary_vintage}*.parquet")
        )
        if boundary_matches:
            measures_path = boundary_matches[0]
            logger.info(
                f"Exact ACS vintage '{acs_vintage}' not found for boundary "
                f"{boundary_vintage}; falling back to {measures_path.name}"
            )

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
            available = sorted(df["weighting_method"].unique())
            raise ValueError(
                f"ACS measures file {measures_path.name} has no rows for "
                f"weighting={weighting}; available weightings: {available}. "
                f"Re-run 'coclab aggregate acs' with --weighting {weighting}, "
                f"or use --weighting {available[0]} for this panel."
            )
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
    *,
    geo_type: str = GEO_TYPE_COC,
    definition_version: str | None = None,
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
        ZORI yearly data with geography-native ID column, year, zori_coc,
        coverage_ratio.
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
    geo_col = _panel_geo_id_col(geo_type)

    if zori_yearly_path is not None:
        path = Path(zori_yearly_path)
        if not path.exists():
            logger.warning(f"Specified ZORI yearly path not found: {path}")
            return None
        logger.info(f"Loading ZORI yearly from explicit path: {path}")
    else:
        if geo_type == GEO_TYPE_METRO:
            if definition_version is None:
                raise ValueError("definition_version is required for geo_type='metro'")
            candidates = sorted(rents_dir.glob("zori_yearly__metro__*.parquet"), reverse=True)
            if not candidates:
                candidates = sorted(rents_dir.glob("zori__metro__*.parquet"), reverse=True)
        else:
            # Auto-discover: try new naming first, then legacy
            candidates = sorted(rents_dir.glob("zori_yearly__A*.parquet"), reverse=True)
            if not candidates:
                candidates = sorted(rents_dir.glob("coc_zori_yearly__*.parquet"), reverse=True)

        if not candidates:
            logger.info(f"No yearly ZORI files found in {rents_dir}")
            return None
        # Use the most recent (alphabetically last, which is typically newest vintage)
        path = candidates[0]
        logger.info(f"Auto-discovered ZORI yearly file: {path}")

    df = pd.read_parquet(path)

    # Validate required columns
    required_cols = {geo_col, "year", "zori_coc", "coverage_ratio"}
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
    result_cols = [geo_col, "year", "zori_coc", "zori_coverage_ratio"]
    # Include optional metadata columns if present
    for col in ["method", "zori_max_geo_contribution", "geo_count"]:
        if col in df.columns:
            result_cols.append(col)

    df = df[result_cols].copy()
    df[geo_col] = df[geo_col].astype(str)
    df["year"] = df["year"].astype(int)

    logger.info(
        f"Loaded ZORI yearly: {len(df)} rows, "
        f"{df[geo_col].nunique()} {'metros' if geo_type == GEO_TYPE_METRO else 'CoCs'}, "
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


def _detect_boundary_changes(
    df: pd.DataFrame,
    *,
    geo_col: str = "coc_id",
    vintage_col: str = "boundary_vintage_used",
) -> pd.Series:
    """Detect vintage changes between consecutive years for each geo unit.

    A boundary change is detected when the boundary_vintage_used differs
    from the prior year's boundary_vintage_used for the same CoC.

    Parameters
    ----------
    df : pd.DataFrame
        Panel DataFrame with geo identifier, year, and vintage columns.
        Must be sorted by geo identifier and year.

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

    if geo_col not in df.columns or vintage_col not in df.columns:
        return pd.Series(False, index=df.index, dtype=bool)

    # Sort by geography and year
    df = df.sort_values([geo_col, "year"]).copy()

    # Get prior year's vintage for each geography unit
    df["_prior_vintage"] = df.groupby(geo_col)[vintage_col].shift(1)

    # Boundary changed if vintage differs from prior (and prior exists)
    boundary_changed = df["_prior_vintage"].notna() & (
        df[vintage_col] != df["_prior_vintage"]
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
    *,
    geo_type: str = GEO_TYPE_COC,
    definition_version: str | None = None,
) -> pd.DataFrame:
    """Build analysis-ready analysis-geography x year panel.

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
        Panel DataFrame with canonical columns for the requested geography.
        CoC builds preserve the legacy ``coc_id`` schema. Metro builds use
        ``metro_id`` plus canonical ``geo_type`` / ``geo_id`` columns.

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
    if geo_type not in {GEO_TYPE_COC, GEO_TYPE_METRO}:
        raise ValueError(f"Unsupported geo_type: {geo_type!r}")
    if geo_type == GEO_TYPE_METRO and not definition_version:
        raise ValueError("definition_version is required for geo_type='metro'")

    if policy is None:
        policy = DEFAULT_POLICY

    geo_col = _panel_geo_id_col(geo_type)
    panel_columns = _panel_columns_for_geo_type(geo_type)
    logger.info(f"Building {geo_type} panel for {start_year}-{end_year}")
    logger.info(f"Policy: weighting={policy.weighting_method}")

    # Load ZORI data if requested
    zori_df = None
    if include_zori:
        logger.info("ZORI integration enabled, loading yearly ZORI data...")
        zori_df = _load_zori_yearly(
            zori_yearly_path=zori_yearly_path,
            rents_dir=rents_dir,
            geo_type=geo_type,
            definition_version=definition_version,
        )
        if zori_df is None:
            raise ValueError(
                "ZORI integration requested but no ZORI yearly data available. "
                "Run 'coclab aggregate zori --build <BUILD>' first, or provide "
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
        pit_df = _load_pit_for_year(
            year,
            pit_dir=pit_dir,
            geo_type=geo_type,
            definition_version=definition_version,
        )

        if pit_df.empty:
            logger.warning(f"No PIT data for year {year}, skipping")
            continue

        # Load ACS measures (returns DataFrame and tract_vintage from provenance)
        acs_df, tract_vintage = _load_acs_measures(
            boundary_vintage=boundary_vintage if geo_type == GEO_TYPE_COC else None,
            acs_vintage=acs_vintage,
            weighting=weighting,
            measures_dir=measures_dir,
            geo_type=geo_type,
            definition_version=definition_version,
        )

        # Start with PIT data as anchor
        year_df = pit_df.copy()
        year_df["year"] = year
        if geo_type == GEO_TYPE_COC:
            year_df["boundary_vintage_used"] = boundary_vintage
        else:
            year_df["definition_version_used"] = definition_version
            year_df = ensure_canonical_geo_columns(
                year_df,
                GEO_TYPE_METRO,
                geo_id_source_col=geo_col,
            )
        year_df["acs_vintage_used"] = acs_vintage
        year_df["tract_vintage_used"] = tract_vintage
        year_df["alignment_type"] = (
            _determine_alignment_type(year, boundary_vintage)
            if geo_type == GEO_TYPE_COC
            else _align_label(geo_type)
        )
        year_df["weighting_method"] = weighting
        year_df["source"] = "coclab_panel" if geo_type == GEO_TYPE_COC else "metro_panel"

        # Left join with ACS measures (PIT is anchor)
        if not acs_df.empty:
            year_df = year_df.merge(acs_df, on=geo_col, how="left")
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
        return pd.DataFrame(columns=panel_columns)

    # Combine all years
    panel_df = pd.concat(year_dfs, ignore_index=True)

    # Fail fast if every ACS column is null (panel is unusable for analysis)
    acs_cols = [
        "total_population",
        "adult_population",
        "population_below_poverty",
        "median_household_income",
        "median_gross_rent",
        "coverage_ratio",
    ]
    present = [c for c in acs_cols if c in panel_df.columns]
    if present and len(panel_df) > 0 and panel_df[present].isna().all().all():
        raise ValueError(
            f"All ACS-derived columns are null for every row in the panel "
            f"({start_year}-{end_year}, weighting={policy.weighting_method}). "
            f"No usable ACS measures were found. Check that measures have been "
            f"built for the required vintage range with the requested weighting."
        )

    # Sort by geography and year for vintage change detection
    panel_df = panel_df.sort_values([geo_col, "year"]).reset_index(drop=True)

    # Detect geometry-definition changes over time
    vintage_col = (
        "boundary_vintage_used" if geo_type == GEO_TYPE_COC else "definition_version_used"
    )
    panel_df["boundary_changed"] = _detect_boundary_changes(
        panel_df,
        geo_col=geo_col,
        vintage_col=vintage_col,
    )

    # Ensure all canonical columns exist
    for col in panel_columns:
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
            on=[geo_col, "year"],
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
        all_columns = panel_columns + ZORI_COLUMNS + ZORI_PROVENANCE_COLUMNS
        # Also keep zori_max_geo_contribution if present
        if "zori_max_geo_contribution" in panel_df.columns:
            all_columns.append("zori_max_geo_contribution")
    else:
        all_columns = panel_columns

    # Ensure all required columns exist
    for col in all_columns:
        if col not in panel_df.columns:
            panel_df[col] = pd.NA

    # Reorder columns to canonical order (only keep columns that exist)
    final_columns = [col for col in all_columns if col in panel_df.columns]
    panel_df = panel_df[final_columns].copy()

    # Final dtype cleanup
    panel_df[geo_col] = panel_df[geo_col].astype(str)
    if geo_type == GEO_TYPE_METRO:
        panel_df[GEO_ID_COL] = panel_df[GEO_ID_COL].astype(str)
        panel_df[GEO_TYPE_COL] = panel_df[GEO_TYPE_COL].astype(str)
    panel_df["year"] = panel_df["year"].astype(int)
    panel_df["pit_total"] = panel_df["pit_total"].astype(int)
    if "boundary_vintage_used" in panel_df.columns:
        panel_df["boundary_vintage_used"] = panel_df["boundary_vintage_used"].astype(str)
    if "definition_version_used" in panel_df.columns:
        panel_df["definition_version_used"] = panel_df["definition_version_used"].astype(str)
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
        f"{panel_df[geo_col].nunique()} {'metros' if geo_type == GEO_TYPE_METRO else 'CoCs'}, "
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
    conformance_report: ConformanceReport | None = None,
    *,
    geo_type: str | None = None,
    definition_version: str | None = None,
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
    Output filename is geography-aware:
    - CoC: ``panel__Y{start}-{end}@B{boundary}.parquet``
    - Metro: ``panel__metro__Y{start}-{end}@D{definition}.parquet``

    Provenance metadata includes:
    - Panel date range
    - Row and CoC counts
    - Policy settings (if provided)
    - ZORI provenance (if provided)
    """
    output_dir = output_dir or DEFAULT_PANEL_DIR
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Extract vintages from data if available
    boundary_vintage = None
    acs_vintage = None
    weighting = None
    if geo_type is None:
        geo_type = GEO_TYPE_METRO if "metro_id" in df.columns else GEO_TYPE_COC

    if not df.empty:
        if geo_type == GEO_TYPE_COC and "boundary_vintage_used" in df.columns:
            boundary_vintages = df["boundary_vintage_used"].unique()
            if len(boundary_vintages) == 1:
                boundary_vintage = str(boundary_vintages[0])
            else:
                boundary_vintage = str(df["boundary_vintage_used"].mode().iloc[0])
        if geo_type == GEO_TYPE_METRO and definition_version is None:
            if "definition_version_used" in df.columns:
                versions = df["definition_version_used"].dropna().unique()
                if len(versions) == 1:
                    definition_version = str(versions[0])

    # Generate filename with temporal shorthand
    if geo_type == GEO_TYPE_METRO:
        if definition_version is None:
            raise ValueError(
                "definition_version is required to save a metro panel."
            )
        filename = naming.geo_panel_filename(
            start_year,
            end_year,
            geo_type=geo_type,
            definition_version=definition_version,
        )
    elif boundary_vintage:
        filename = naming.geo_panel_filename(
            start_year,
            end_year,
            geo_type=geo_type,
            boundary_vintage=boundary_vintage,
        )
    else:
        # Fallback if no boundary vintage can be determined
        filename = f"panel__Y{start_year}-{end_year}.parquet"
    output_path = output_dir / filename

    # Build provenance
    extra = {
        "dataset_type": _panel_dataset_type(geo_type),
        "start_year": start_year,
        "end_year": end_year,
        "row_count": len(df),
        "geo_type": geo_type,
        "geo_count": int(df[resolve_geo_col(df)].nunique()) if not df.empty else 0,
        "year_count": int(df["year"].nunique()) if not df.empty else 0,
    }
    if geo_type == GEO_TYPE_COC:
        extra["coc_count"] = int(df["coc_id"].nunique()) if not df.empty else 0
    if definition_version is not None:
        extra["definition_version"] = definition_version

    if policy is not None:
        extra["policy"] = policy.to_dict()

    # Add ZORI provenance if provided
    if zori_provenance is not None:
        extra["zori"] = zori_provenance.to_dict()
        # Also add ZORI eligibility summary from the data
        summary = summarize_zori_eligibility(df)
        if summary.get("zori_integrated"):
            extra["zori_summary"] = summary

    # Add conformance report if provided
    if conformance_report is not None:
        extra["conformance"] = conformance_report.to_dict()

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
        geo_type=geo_type,
        definition_version=definition_version,
        extra=extra,
    )

    write_parquet_with_provenance(df, output_path, provenance)
    logger.info(f"Saved panel to {output_path}")

    return output_path
