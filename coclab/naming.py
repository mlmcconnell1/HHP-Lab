"""Centralized filename generation using temporal shorthand notation.

This module provides canonical filename generation for all CoC Lab datasets.
All filenames follow the pattern: {dataset}__{temporal-notation}.parquet

Temporal notation uses single-letter prefixes:
- B{year}: CoC boundary version (e.g., B2025)
- T{year}: Census tract geometry (e.g., T2023)
- C{year}: Census county geometry (e.g., C2023)
- A{year}: ACS vintage end year (e.g., A2023)
- P{year}: PIT count year (e.g., P2024)
- Y{year}: Panel year (e.g., Y2023)

Compound notation:
- @ means "analyzed using" (e.g., A2023@B2025 = ACS 2023 on 2025 boundaries)
- x means crosswalk join (e.g., B2025xT2023 = boundaries crossed with tracts)

See background/temporal-terminology.md for full specification.
"""

from pathlib import Path

# =============================================================================
# Simple datasets (single vintage)
# =============================================================================


def boundary_filename(boundary_vintage: str) -> str:
    """Generate filename for CoC boundary data.

    Args:
        boundary_vintage: Boundary vintage year (e.g., "2025")

    Returns:
        Filename like 'boundaries__B2025.parquet'
    """
    return f"boundaries__B{boundary_vintage}.parquet"


def tract_filename(tract_vintage: str | int) -> str:
    """Generate filename for census tract geometry.

    Args:
        tract_vintage: TIGER tract vintage year (e.g., 2023 or "2023")

    Returns:
        Filename like 'tracts__T2023.parquet'
    """
    return f"tracts__T{tract_vintage}.parquet"


def county_filename(county_vintage: str | int) -> str:
    """Generate filename for census county geometry.

    Args:
        county_vintage: TIGER county vintage year (e.g., 2023 or "2023")

    Returns:
        Filename like 'counties__C2023.parquet'
    """
    return f"counties__C{county_vintage}.parquet"


def pit_filename(pit_year: str | int) -> str:
    """Generate filename for PIT count data.

    Args:
        pit_year: PIT count year (e.g., 2024 or "2024")

    Returns:
        Filename like 'pit__P2024.parquet'
    """
    return f"pit__P{pit_year}.parquet"


def pit_vintage_filename(vintage: str | int) -> str:
    """Generate filename for PIT vintage file (containing all years from one release).

    Args:
        vintage: PIT release vintage year (e.g., 2024)

    Returns:
        Filename like 'pit_vintage__P2024.parquet'
    """
    return f"pit_vintage__P{vintage}.parquet"


# =============================================================================
# Crosswalks (join two geometry vintages)
# =============================================================================


def tract_xwalk_filename(boundary_vintage: str, tract_vintage: str | int) -> str:
    """Generate filename for CoC-to-tract crosswalk.

    Args:
        boundary_vintage: CoC boundary vintage (e.g., "2025")
        tract_vintage: Tract geometry vintage (e.g., 2023)

    Returns:
        Filename like 'xwalk__B2025xT2023.parquet'
    """
    return f"xwalk__B{boundary_vintage}xT{tract_vintage}.parquet"


def county_xwalk_filename(boundary_vintage: str, county_vintage: str | int) -> str:
    """Generate filename for CoC-to-county crosswalk.

    Args:
        boundary_vintage: CoC boundary vintage (e.g., "2025")
        county_vintage: County geometry vintage (e.g., 2023)

    Returns:
        Filename like 'xwalk__B2025xC2023.parquet'
    """
    return f"xwalk__B{boundary_vintage}xC{county_vintage}.parquet"


def tract_relationship_filename(
    from_vintage: str | int = 2010,
    to_vintage: str | int = 2020,
) -> str:
    """Generate filename for Census tract relationship file.

    Args:
        from_vintage: Source tract vintage (e.g., 2010)
        to_vintage: Target tract vintage (e.g., 2020)

    Returns:
        Filename like 'tract_relationship__T2010xT2020.parquet'
    """
    return f"tract_relationship__T{from_vintage}xT{to_vintage}.parquet"


# =============================================================================
# Derived datasets (compound notation)
# =============================================================================


def measures_filename(
    acs_vintage: str,
    boundary_vintage: str,
    tract_vintage: str | int | None = None,
) -> str:
    """Generate filename for CoC measures dataset.

    Args:
        acs_vintage: ACS vintage (e.g., "2019-2023" or "2023")
        boundary_vintage: CoC boundary vintage (e.g., "2025")
        tract_vintage: Optional tract vintage used in crosswalk

    Returns:
        Filename like 'measures__A2023@B2025.parquet' or
        'measures__A2023@B2025xT2023.parquet' if tract_vintage specified

    Note:
        The ACS vintage is normalized to just the end year (e.g., "2019-2023" -> "A2023")
    """
    # Normalize ACS vintage to end year
    acs_year = _normalize_acs_vintage(acs_vintage)

    if tract_vintage is not None:
        return f"measures__A{acs_year}@B{boundary_vintage}xT{tract_vintage}.parquet"
    return f"measures__A{acs_year}@B{boundary_vintage}.parquet"


def panel_filename(
    start_year: int,
    end_year: int,
    boundary_vintage: str,
) -> str:
    """Generate filename for CoC panel dataset.

    Args:
        start_year: First year in panel (e.g., 2015)
        end_year: Last year in panel (e.g., 2024)
        boundary_vintage: Target CoC boundary vintage (e.g., "2025")

    Returns:
        Filename like 'panel__Y2015-2024@B2025.parquet'
    """
    return f"panel__Y{start_year}-{end_year}@B{boundary_vintage}.parquet"


def zori_filename(
    acs_vintage: str,
    boundary_vintage: str,
    county_vintage: str | int,
    weighting: str,
) -> str:
    """Generate filename for CoC ZORI dataset.

    Args:
        acs_vintage: ACS vintage for weights (e.g., "2019-2023" or "2023")
        boundary_vintage: CoC boundary vintage (e.g., "2025")
        county_vintage: County geometry vintage (e.g., 2023)
        weighting: Weighting method (e.g., "renter_households")

    Returns:
        Filename like 'zori__A2023@B2025xC2023__wrenter.parquet'

    Note:
        Weighting is abbreviated: "renter_households" -> "renter"
    """
    acs_year = _normalize_acs_vintage(acs_vintage)
    weight_abbrev = _abbreviate_weighting(weighting)
    return f"zori__A{acs_year}@B{boundary_vintage}xC{county_vintage}__w{weight_abbrev}.parquet"


def zori_yearly_filename(
    acs_vintage: str,
    boundary_vintage: str,
    county_vintage: str | int,
    weighting: str,
    yearly_method: str,
) -> str:
    """Generate filename for yearly CoC ZORI dataset.

    Args:
        acs_vintage: ACS vintage for weights (e.g., "2019-2023" or "2023")
        boundary_vintage: CoC boundary vintage (e.g., "2025")
        county_vintage: County geometry vintage (e.g., 2023)
        weighting: Weighting method (e.g., "renter_households")
        yearly_method: Yearly collapse method (e.g., "pit_january")

    Returns:
        Filename like 'zori_yearly__A2023@B2025xC2023__wrenter__mpit_january.parquet'
    """
    acs_year = _normalize_acs_vintage(acs_vintage)
    weight_abbrev = _abbreviate_weighting(weighting)
    return (
        f"zori_yearly__A{acs_year}@B{boundary_vintage}xC{county_vintage}"
        f"__w{weight_abbrev}__m{yearly_method}.parquet"
    )


# =============================================================================
# ACS tract population files
# =============================================================================


def acs_tracts_filename(acs_vintage: str, tract_vintage: str | int) -> str:
    """Generate filename for ACS tract population data.

    Args:
        acs_vintage: ACS vintage (e.g., "2019-2023" or "2023")
        tract_vintage: Tract geometry vintage (e.g., 2023)

    Returns:
        Filename like 'acs_tracts__A2023xT2023.parquet'
    """
    acs_year = _normalize_acs_vintage(acs_vintage)
    return f"acs_tracts__A{acs_year}xT{tract_vintage}.parquet"


def county_weights_filename(acs_vintage: str, weighting: str) -> str:
    """Generate filename for county-level ACS weights.

    Args:
        acs_vintage: ACS vintage (e.g., "2019-2023" or "2023")
        weighting: Weighting method (e.g., "renter_households")

    Returns:
        Filename like 'county_weights__A2023__wrenter.parquet'
    """
    acs_year = _normalize_acs_vintage(acs_vintage)
    weight_abbrev = _abbreviate_weighting(weighting)
    return f"county_weights__A{acs_year}__w{weight_abbrev}.parquet"


# =============================================================================
# Path helpers (combine filename with directory)
# =============================================================================


def boundary_path(boundary_vintage: str, base_dir: Path | str | None = None) -> Path:
    """Get canonical path for curated boundary file.

    Args:
        boundary_vintage: Boundary vintage year
        base_dir: Base data directory (defaults to "data")

    Returns:
        Path like data/curated/coc_boundaries/boundaries__B2025.parquet
    """
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return base_dir / "curated" / "coc_boundaries" / boundary_filename(boundary_vintage)


def tract_path(tract_vintage: str | int, base_dir: Path | str | None = None) -> Path:
    """Get canonical path for census tract file.

    Args:
        tract_vintage: TIGER tract vintage year
        base_dir: Base data directory (defaults to "data")

    Returns:
        Path like data/curated/census/tracts__T2023.parquet
    """
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return base_dir / "curated" / "census" / tract_filename(tract_vintage)


def county_path(county_vintage: str | int, base_dir: Path | str | None = None) -> Path:
    """Get canonical path for census county file.

    Args:
        county_vintage: TIGER county vintage year
        base_dir: Base data directory (defaults to "data")

    Returns:
        Path like data/curated/census/counties__C2023.parquet
    """
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return base_dir / "curated" / "census" / county_filename(county_vintage)


def pit_path(pit_year: str | int, base_dir: Path | str | None = None) -> Path:
    """Get canonical path for PIT count file.

    Args:
        pit_year: PIT count year
        base_dir: Base data directory (defaults to "data")

    Returns:
        Path like data/curated/pit/pit__P2024.parquet
    """
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return base_dir / "curated" / "pit" / pit_filename(pit_year)


def tract_xwalk_path(
    boundary_vintage: str,
    tract_vintage: str | int,
    base_dir: Path | str | None = None,
) -> Path:
    """Get canonical path for tract crosswalk file.

    Args:
        boundary_vintage: CoC boundary vintage
        tract_vintage: Tract geometry vintage
        base_dir: Base data directory (defaults to "data")

    Returns:
        Path like data/curated/xwalks/xwalk__B2025xT2023.parquet
    """
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return base_dir / "curated" / "xwalks" / tract_xwalk_filename(boundary_vintage, tract_vintage)


def county_xwalk_path(
    boundary_vintage: str,
    county_vintage: str | int,
    base_dir: Path | str | None = None,
) -> Path:
    """Get canonical path for county crosswalk file.

    Args:
        boundary_vintage: CoC boundary vintage
        county_vintage: County geometry vintage
        base_dir: Base data directory (defaults to "data")

    Returns:
        Path like data/curated/xwalks/xwalk__B2025xC2023.parquet
    """
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return base_dir / "curated" / "xwalks" / county_xwalk_filename(boundary_vintage, county_vintage)


def measures_path(
    acs_vintage: str,
    boundary_vintage: str,
    tract_vintage: str | int | None = None,
    base_dir: Path | str | None = None,
) -> Path:
    """Get canonical path for measures file.

    Args:
        acs_vintage: ACS vintage
        boundary_vintage: CoC boundary vintage
        tract_vintage: Optional tract vintage
        base_dir: Base data directory (defaults to "data")

    Returns:
        Path like data/curated/measures/measures__A2023@B2025.parquet
    """
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return (
        base_dir
        / "curated"
        / "measures"
        / measures_filename(acs_vintage, boundary_vintage, tract_vintage)
    )


def panel_path(
    start_year: int,
    end_year: int,
    boundary_vintage: str,
    base_dir: Path | str | None = None,
) -> Path:
    """Get canonical path for panel file.

    Args:
        start_year: First panel year
        end_year: Last panel year
        boundary_vintage: Target CoC boundary vintage
        base_dir: Base data directory (defaults to "data")

    Returns:
        Path like data/curated/panel/panel__Y2015-2024@B2025.parquet
    """
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return base_dir / "curated" / "panel" / panel_filename(start_year, end_year, boundary_vintage)


def county_weights_path(
    acs_vintage: str,
    weighting: str,
    base_dir: Path | str | None = None,
) -> Path:
    """Get canonical path for county weights file.

    Args:
        acs_vintage: ACS vintage (e.g., "2019-2023" or "2023")
        weighting: Weighting method (e.g., "renter_households")
        base_dir: Base data directory (defaults to "data")

    Returns:
        Path like data/curated/acs/county_weights__A2023__wrenter.parquet
    """
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return base_dir / "curated" / "acs" / county_weights_filename(acs_vintage, weighting)


# =============================================================================
# Helper functions
# =============================================================================


def _normalize_acs_vintage(acs_vintage: str) -> str:
    """Normalize ACS vintage to just the end year.

    Args:
        acs_vintage: ACS vintage like "2019-2023" or "2023"

    Returns:
        End year as string, e.g., "2023"
    """
    if "-" in acs_vintage:
        # Format like "2019-2023", extract end year
        return acs_vintage.split("-")[1]
    return acs_vintage


def expand_acs_vintage(acs_vintage: str) -> str:
    """Expand ACS end year to full 5-year range for display.

    Args:
        acs_vintage: ACS vintage as end year ("2023") or range ("2019-2023")

    Returns:
        Full 5-year range, e.g., "2019-2023"
    """
    if "-" in acs_vintage:
        # Already a range
        return acs_vintage
    # Single year - expand to 5-year range
    end_year = int(acs_vintage)
    start_year = end_year - 4
    return f"{start_year}-{end_year}"


def _abbreviate_weighting(weighting: str) -> str:
    """Abbreviate weighting method for filename.

    Args:
        weighting: Full weighting name like "renter_households"

    Returns:
        Abbreviated form like "renter"
    """
    # Common abbreviations
    abbreviations = {
        "renter_households": "renter",
        "total_population": "pop",
        "area": "area",
    }
    return abbreviations.get(weighting, weighting)
