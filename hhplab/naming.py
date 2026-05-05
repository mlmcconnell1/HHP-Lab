"""Centralized filename generation using temporal shorthand notation.

This module provides canonical filename generation for all HHP-Lab datasets.
All filenames follow the pattern: {dataset}__{temporal-notation}.parquet

Temporal notation uses single-letter prefixes:
- B{year}: CoC boundary version (e.g., B2025)
- T{year}: Census tract geometry (e.g., T2023)
- C{year}: Census county geometry (e.g., C2023)
- A{year}: ACS vintage end year (e.g., A2023)
- P{year}: PIT count year (e.g., P2024)
- Y{year}: Panel year (e.g., Y2023)
- D{version}: Synthetic geography definition version (e.g., Dglynnfoxv1)

Compound notation:
- @ means "analyzed using" (e.g., A2023@B2025 = ACS 2023 on 2025 boundaries)
- x means crosswalk join (e.g., B2025xT2023 = boundaries crossed with tracts)

Geography-scoped naming:
- Metro outputs include a ``metro`` geography segment to avoid collision
  with CoC outputs. Example: ``measures__metro__A2023@Dglynnfoxv1xT2020.parquet``
- CoC outputs retain their existing names for backward compatibility.

See background/temporal-terminology.md for full specification.
"""

import re
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


def coc_base_filename(boundary_vintage: str) -> str:
    """Generate filename for CoC base boundary data.

    Args:
        boundary_vintage: Boundary vintage year (e.g., "2025")

    Returns:
        Filename like 'coc__B2025.parquet'
    """
    return f"coc__B{boundary_vintage}.parquet"


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


def coc_pit_filename(pit_year: str | int, boundary_vintage: str | int) -> str:
    """Generate filename for PIT data aligned to a boundary vintage.

    Args:
        pit_year: PIT count year (e.g., 2024)
        boundary_vintage: CoC boundary vintage (e.g., 2024)

    Returns:
        Filename like 'pit__P2024@B2024.parquet'
    """
    return f"pit__P{pit_year}@B{boundary_vintage}.parquet"


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


def msa_coc_xwalk_filename(
    boundary_vintage: str,
    definition_version: str,
    county_vintage: str | int,
) -> str:
    """Generate filename for CoC-to-MSA crosswalk.

    Args:
        boundary_vintage: CoC boundary vintage (e.g., "2025")
        definition_version: MSA definition version (e.g., "census_msa_2023")
        county_vintage: County geometry vintage used to derive the overlap

    Returns:
        Filename like
        ``msa_coc_xwalk__B2025xMcensus_msa_2023xC2023.parquet``
    """
    return (
        f"msa_coc_xwalk__B{boundary_vintage}"
        f"xM{definition_version}xC{county_vintage}.parquet"
    )


def containment_filename(
    *,
    container_type: str,
    candidate_type: str,
    output_id: str,
    container_vintage: str | int | None = None,
    candidate_vintage: str | int | None = None,
    definition_version: str | None = None,
) -> str:
    """Generate filename for a recipe containment-list output."""
    container_token = _containment_geometry_token(
        container_type,
        vintage=container_vintage,
        definition_version=definition_version,
    )
    candidate_token = _containment_geometry_token(
        candidate_type,
        vintage=candidate_vintage,
        definition_version=None,
    )
    return (
        f"containment__{container_token}x{candidate_token}"
        f"__{_slug_output_id(output_id)}.parquet"
    )


def _containment_geometry_token(
    geometry_type: str,
    *,
    vintage: str | int | None,
    definition_version: str | None,
) -> str:
    if geometry_type == "coc":
        if vintage is None:
            raise ValueError("CoC containment filenames require a boundary vintage.")
        return f"B{vintage}"
    if geometry_type == "county":
        if vintage is None:
            raise ValueError("County containment filenames require a county vintage.")
        return f"C{vintage}"
    if geometry_type == "msa":
        if definition_version:
            return f"M{definition_version}"
        if vintage is None:
            raise ValueError(
                "MSA containment filenames require a definition version or vintage."
            )
        return f"M{vintage}"
    raise ValueError(
        f"Unsupported containment geometry type '{geometry_type}'. "
        "Supported types: coc, county, msa."
    )


def _slug_output_id(output_id: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "-", output_id.strip().lower())
    normalized = re.sub(r"-{2,}", "-", normalized).strip("-.")
    return normalized or "containment"


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
    alignment_year: int | None = None,
) -> str:
    """Generate filename for CoC measures dataset.

    Args:
        acs_vintage: ACS vintage (e.g., "2019-2023" or "2023")
        boundary_vintage: CoC boundary vintage (e.g., "2025")
        tract_vintage: Optional tract vintage used in crosswalk
        alignment_year: Optional alignment year for window_center_year mode.
            When the ACS end year differs from the boundary year, shows
            which hub year the ACS vintage was aligned to.
            E.g., ``measures__A2015(2013)@B2013xT2010.parquet``

    Returns:
        Filename like 'measures__A2023@B2025.parquet' or
        'measures__A2023@B2025xT2023.parquet' if tract_vintage specified

    Note:
        The ACS vintage is normalized to just the end year (e.g., "2019-2023" -> "A2023")
    """
    # Normalize ACS vintage to end year
    acs_year = _normalize_acs_vintage(acs_vintage)

    acs_part = f"A{acs_year}"
    if alignment_year is not None:
        acs_part += f"({alignment_year})"

    if tract_vintage is not None:
        return f"measures__{acs_part}@B{boundary_vintage}xT{tract_vintage}.parquet"
    return f"measures__{acs_part}@B{boundary_vintage}.parquet"


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


def acs5_tracts_filename(acs_vintage: str, tract_vintage: str | int) -> str:
    """Generate filename for ACS 5-year tract population data.

    Args:
        acs_vintage: ACS vintage (e.g., "2019-2023" or "2023")
        tract_vintage: Tract geometry vintage (e.g., 2023)

    Returns:
        Filename like 'acs5_tracts__A2023xT2023.parquet'
    """
    acs_year = _normalize_acs_vintage(acs_vintage)
    return f"acs5_tracts__A{acs_year}xT{tract_vintage}.parquet"


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

    .. deprecated::
        Use :func:`coc_base_path` instead. This function uses the legacy
        ``boundaries__B`` naming convention.

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


def coc_base_path(boundary_vintage: str, base_dir: Path | str | None = None) -> Path:
    """Get canonical path for curated CoC boundary file using preferred naming.

    Args:
        boundary_vintage: Boundary vintage year (e.g., "2025")
        base_dir: Base data directory (defaults to "data")

    Returns:
        Path like data/curated/coc_boundaries/coc__B2025.parquet
    """
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return base_dir / "curated" / "coc_boundaries" / coc_base_filename(boundary_vintage)


def tract_path(tract_vintage: str | int, base_dir: Path | str | None = None) -> Path:
    """Get canonical path for census tract file.

    Args:
        tract_vintage: TIGER tract vintage year
        base_dir: Base data directory (defaults to "data")

    Returns:
        Path like data/curated/tiger/tracts__T2023.parquet
    """
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return base_dir / "curated" / "tiger" / tract_filename(tract_vintage)


def county_path(county_vintage: str | int, base_dir: Path | str | None = None) -> Path:
    """Get canonical path for census county file.

    Args:
        county_vintage: TIGER county vintage year
        base_dir: Base data directory (defaults to "data")

    Returns:
        Path like data/curated/tiger/counties__C2023.parquet
    """
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return base_dir / "curated" / "tiger" / county_filename(county_vintage)


def msa_coc_xwalk_path(
    boundary_vintage: str,
    definition_version: str,
    county_vintage: str | int,
    base_dir: Path | str | None = None,
) -> Path:
    """Get canonical path for CoC-to-MSA crosswalk file.

    Args:
        boundary_vintage: CoC boundary vintage
        definition_version: MSA definition version
        county_vintage: County geometry vintage used to derive the overlap
        base_dir: Base data directory (defaults to ``data``)

    Returns:
        Path like
        ``data/curated/xwalks/msa_coc_xwalk__B2025xMcensus_msa_2023xC2023.parquet``
    """
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return (
        base_dir / "curated" / "xwalks"
        / msa_coc_xwalk_filename(boundary_vintage, definition_version, county_vintage)
    )


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


def pit_vintage_path(vintage: str | int, base_dir: Path | str | None = None) -> Path:
    """Get canonical path for PIT vintage file.

    Args:
        vintage: PIT release vintage year (e.g., 2024)
        base_dir: Base data directory (defaults to "data")

    Returns:
        Path like data/curated/pit/pit_vintage__P2024.parquet
    """
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return base_dir / "curated" / "pit" / pit_vintage_filename(vintage)


def discover_pit_vintages(base_dir: Path | str | None = None) -> list[int]:
    """Discover available PIT vintage files, sorted descending by year.

    Scans the curated PIT directory for files matching
    ``pit_vintage__P{year}.parquet`` and returns the vintage years
    found, with the latest vintage first.

    Args:
        base_dir: Base data directory (defaults to "data")

    Returns:
        List of vintage years (ints) sorted descending, e.g. [2024, 2023].
        Empty list if no vintage files are found.
    """
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)

    pit_dir = base_dir / "curated" / "pit"
    if not pit_dir.is_dir():
        return []

    vintages: list[int] = []
    for p in pit_dir.glob("pit_vintage__P*.parquet"):
        stem = p.stem  # e.g. "pit_vintage__P2024"
        suffix = stem.removeprefix("pit_vintage__P")
        if suffix.isdigit():
            vintages.append(int(suffix))

    return sorted(vintages, reverse=True)


# =============================================================================
# ZORI ingest (single-geography, pre-aggregation)
# =============================================================================


def zori_ingest_filename(geography: str, max_year: int | str) -> str:
    """Generate filename for ZORI ingest data.

    Args:
        geography: Geography level ("county" or "zip")
        max_year: Maximum year in the ZORI series (e.g., 2026)

    Returns:
        Filename like 'zori__county__Z2026.parquet'
    """
    return f"zori__{geography}__Z{max_year}.parquet"


def zori_ingest_path(
    geography: str,
    max_year: int | str,
    base_dir: Path | str | None = None,
) -> Path:
    """Get canonical path for ZORI ingest file.

    Args:
        geography: Geography level ("county" or "zip")
        max_year: Maximum year in the ZORI series
        base_dir: Base data directory (defaults to "data")

    Returns:
        Path like data/curated/zori/zori__county__Z2026.parquet
    """
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return base_dir / "curated" / "zori" / zori_ingest_filename(geography, max_year)


def discover_zori_ingest(
    geography: str,
    output_dir: Path | str | None = None,
) -> Path | None:
    """Discover the most recent ZORI ingest file for a geography.

    Scans the ZORI output directory for files matching the temporal
    pattern ``zori__{geography}__Z{year}.parquet``. If multiple Z-year
    files exist, returns the one with the highest year. Falls back to
    the legacy name ``zori__{geography}.parquet`` if no temporal file
    is found.

    Args:
        geography: Geography level ("county" or "zip")
        output_dir: ZORI output directory (defaults to "data/curated/zori")

    Returns:
        Path to the most recent file, or None if no file exists.
    """
    if output_dir is None:
        from hhplab.paths import curated_dir
        output_dir = curated_dir("zori")
    else:
        output_dir = Path(output_dir)

    if not output_dir.is_dir():
        return None

    # Look for temporal-named files first
    candidates: list[tuple[int, Path]] = []
    for p in output_dir.glob(f"zori__{geography}__Z*.parquet"):
        stem = p.stem  # e.g. "zori__county__Z2026"
        z_suffix = stem.split("__Z")[-1]
        if z_suffix.isdigit():
            candidates.append((int(z_suffix), p))

    if candidates:
        candidates.sort(reverse=True)
        return candidates[0][1]

    # Fall back to legacy name
    legacy = output_dir / f"zori__{geography}.parquet"
    if legacy.exists():
        return legacy

    return None


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


# ---------------------------------------------------------------------------
# PEP (Population Estimates Program) filenames
# ---------------------------------------------------------------------------


def coc_pep_filename(
    boundary_vintage: int | str,
    county_vintage: int | str,
    weighting: str,
    start_year: int,
    end_year: int,
) -> str:
    """Canonical filename for CoC-level PEP aggregate output.

    Pattern: ``coc_pep__B{boundary}xC{county}__w{weighting}__{start}_{end}.parquet``
    """
    return (
        f"coc_pep__B{boundary_vintage}xC{county_vintage}"
        f"__w{weighting}__{start_year}_{end_year}.parquet"
    )


# =============================================================================
# Definition-version token helper
# =============================================================================


def _normalize_definition_version(definition_version: str) -> str:
    """Normalize a definition version string for use in filenames.

    Strips non-alphanumeric characters (except underscores) and
    lowercases. Example: ``"glynn_fox_v1"`` -> ``"glynnfoxv1"``.
    """
    return "".join(
        c for c in definition_version.lower() if c.isalnum()
    )


# =============================================================================
# Metro (geography-scoped) filenames
# =============================================================================


def metro_measures_filename(
    acs_vintage: str,
    definition_version: str,
    tract_vintage: str | int | None = None,
) -> str:
    """Generate filename for metro-scoped ACS measures.

    Pattern: ``measures__metro__A{acs}@D{def}xT{tract}.parquet``

    The ``metro`` segment prevents collision with CoC measures files.
    """
    acs_year = _normalize_acs_vintage(acs_vintage)
    defn = _normalize_definition_version(definition_version)
    if tract_vintage is not None:
        return f"measures__metro__A{acs_year}@D{defn}xT{tract_vintage}.parquet"
    return f"measures__metro__A{acs_year}@D{defn}.parquet"


def metro_panel_filename(
    start_year: int,
    end_year: int,
    definition_version: str,
    profile_definition_version: str | None = None,
) -> str:
    """Generate filename for metro-scoped panel.

    Pattern: ``panel__metro__Y{start}-{end}@D{def}[xS{subset}].parquet``
    """
    defn = _normalize_definition_version(definition_version)
    subset = ""
    if profile_definition_version is not None:
        subset = f"xS{_normalize_definition_version(profile_definition_version)}"
    return f"panel__metro__Y{start_year}-{end_year}@D{defn}{subset}.parquet"


def msa_panel_filename(
    start_year: int,
    end_year: int,
    definition_version: str,
) -> str:
    """Generate filename for MSA-scoped panel.

    Pattern: ``panel__msa__Y{start}-{end}@M{def}.parquet``
    """
    defn = _normalize_definition_version(definition_version)
    return f"panel__msa__Y{start_year}-{end_year}@M{defn}.parquet"


def metro_pit_filename(
    pit_year: str | int,
    definition_version: str,
) -> str:
    """Generate filename for metro-scoped PIT aggregate.

    Pattern: ``pit__metro__P{year}@D{def}.parquet``
    """
    defn = _normalize_definition_version(definition_version)
    return f"pit__metro__P{pit_year}@D{defn}.parquet"


def msa_pit_filename(
    pit_year: str | int,
    definition_version: str,
    boundary_vintage: str | int,
    county_vintage: str | int,
) -> str:
    """Generate filename for MSA-scoped PIT aggregate.

    Pattern: ``pit__msa__P{year}@M{def}xB{boundary}xC{county}.parquet``
    """
    defn = _normalize_definition_version(definition_version)
    return (
        f"pit__msa__P{pit_year}@M{defn}"
        f"xB{boundary_vintage}xC{county_vintage}.parquet"
    )


def metro_pep_filename(
    definition_version: str,
    county_vintage: int | str,
    weighting: str,
    start_year: int,
    end_year: int,
) -> str:
    """Canonical filename for metro-level PEP aggregate output.

    Pattern: ``pep__metro__D{def}xC{county}__w{weighting}__{start}_{end}.parquet``
    """
    defn = _normalize_definition_version(definition_version)
    return (
        f"pep__metro__D{defn}xC{county_vintage}"
        f"__w{weighting}__{start_year}_{end_year}.parquet"
    )


def acs1_metro_filename(acs1_vintage: int, definition_version: str) -> str:
    """Generate filename for curated ACS 1-year metro-level ingest artifact.

    Args:
        acs1_vintage: ACS 1-year vintage end year (e.g., 2023)
        definition_version: Synthetic geography definition version (e.g., "glynn_fox_v1")

    Returns:
        Filename like 'acs1_metro__A2023@Dglynnfoxv1.parquet'
    """
    defn = _normalize_definition_version(definition_version)
    return f"acs1_metro__A{acs1_vintage}@D{defn}.parquet"


def acs1_metro_path(
    acs1_vintage: int,
    definition_version: str,
    base_dir: Path | str | None = None,
) -> Path:
    """Get canonical path for ACS 1-year metro ingest artifact.

    Args:
        acs1_vintage: ACS 1-year vintage end year (e.g., 2023)
        definition_version: Synthetic geography definition version (e.g., "glynn_fox_v1")
        base_dir: Base data directory (defaults to "data")

    Returns:
        Path like data/curated/acs/acs1_metro__A2023@Dglynnfoxv1.parquet
    """
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return base_dir / "curated" / "acs" / acs1_metro_filename(acs1_vintage, definition_version)


def acs1_county_filename(acs1_vintage: int) -> str:
    """Generate filename for curated ACS 1-year county-level ingest artifact.

    Args:
        acs1_vintage: ACS 1-year vintage end year (e.g., 2023)

    Returns:
        Filename like 'acs1_county__A2023.parquet'
    """
    return f"acs1_county__A{acs1_vintage}.parquet"


def acs1_county_path(
    acs1_vintage: int,
    base_dir: Path | str | None = None,
) -> Path:
    """Get canonical path for ACS 1-year county ingest artifact."""
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return base_dir / "curated" / "acs" / acs1_county_filename(acs1_vintage)


def metro_measures_acs1_filename(
    acs1_vintage: int,
    definition_version: str,
) -> str:
    """Generate filename for metro ACS1 measures artifact (post-aggregation).

    The ``__acs1__`` segment prevents collision with ACS5 metro measures files.

    Args:
        acs1_vintage: ACS 1-year vintage end year (e.g., 2023)
        definition_version: Synthetic geography definition version (e.g., "glynn_fox_v1")

    Returns:
        Filename like 'measures__metro__acs1__A2023@Dglynnfoxv1.parquet'
    """
    defn = _normalize_definition_version(definition_version)
    return f"measures__metro__acs1__A{acs1_vintage}@D{defn}.parquet"


def metro_measures_acs1_path(
    acs1_vintage: int,
    definition_version: str,
    base_dir: Path | str | None = None,
) -> Path:
    """Get canonical path for metro ACS1 measures artifact.

    Args:
        acs1_vintage: ACS 1-year vintage end year (e.g., 2023)
        definition_version: Synthetic geography definition version (e.g., "glynn_fox_v1")
        base_dir: Base data directory (defaults to "data")

    Returns:
        Path like data/curated/measures/measures__metro__acs1__A2023@Dglynnfoxv1.parquet
    """
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return (
        base_dir
        / "curated"
        / "measures"
        / metro_measures_acs1_filename(acs1_vintage, definition_version)
    )


def metro_zori_filename(
    acs_vintage: str,
    definition_version: str,
    county_vintage: str | int,
    weighting: str,
) -> str:
    """Generate filename for metro-scoped ZORI dataset.

    Pattern: ``zori__metro__A{acs}@D{def}xC{county}__w{weight}.parquet``
    """
    acs_year = _normalize_acs_vintage(acs_vintage)
    defn = _normalize_definition_version(definition_version)
    weight_abbrev = _abbreviate_weighting(weighting)
    return (
        f"zori__metro__A{acs_year}@D{defn}xC{county_vintage}"
        f"__w{weight_abbrev}.parquet"
    )


# =============================================================================
# Metro definition artifact filenames
# =============================================================================


def metro_definitions_filename(definition_version: str) -> str:
    """Filename for metro definitions table.

    Pattern: ``metro_definitions__{version}.parquet``

    Note: definition/membership filenames preserve the raw version string
    (e.g., ``glynn_fox_v1``) for human readability. Data artifact filenames
    (PIT, ACS, ZORI, panels) normalize to alphanumeric (``glynnfoxv1``).
    """
    return f"metro_definitions__{definition_version}.parquet"


def metro_coc_membership_filename(definition_version: str) -> str:
    """Filename for metro-to-CoC membership table.

    Pattern: ``metro_coc_membership__{version}.parquet``

    See :func:`metro_definitions_filename` for normalization note.
    """
    return f"metro_coc_membership__{definition_version}.parquet"


def metro_county_membership_filename(definition_version: str) -> str:
    """Filename for metro-to-county membership table.

    Pattern: ``metro_county_membership__{version}.parquet``

    See :func:`metro_definitions_filename` for normalization note.
    """
    return f"metro_county_membership__{definition_version}.parquet"


def metro_universe_filename(definition_version: str) -> str:
    """Filename for canonical metro-universe definitions."""
    return f"metro_universe__{definition_version}.parquet"


def metro_subset_membership_filename(
    profile_definition_version: str,
    metro_definition_version: str,
) -> str:
    """Filename for a subset-profile over the canonical metro universe."""
    return (
        "metro_subset_membership__"
        f"{profile_definition_version}xM{metro_definition_version}.parquet"
    )


def metro_boundaries_filename(
    definition_version: str,
    county_vintage: str | int,
) -> str:
    """Filename for materialized metro boundary polygons.

    Pattern: ``metro_boundaries__{version}xC{county}.parquet``

    See :func:`metro_definitions_filename` for normalization note.
    """
    return f"metro_boundaries__{definition_version}xC{county_vintage}.parquet"


# =============================================================================
# Metro definition artifact paths
# =============================================================================


def metro_definitions_path(
    definition_version: str,
    base_dir: Path | str | None = None,
) -> Path:
    """Canonical path for metro definitions file.

    Returns:
        Path like data/curated/metro/metro_definitions__glynn_fox_v1.parquet
    """
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return (
        base_dir / "curated" / "metro"
        / metro_definitions_filename(definition_version)
    )


def metro_coc_membership_path(
    definition_version: str,
    base_dir: Path | str | None = None,
) -> Path:
    """Canonical path for metro-to-CoC membership file.

    Returns:
        Path like data/curated/metro/metro_coc_membership__glynn_fox_v1.parquet
    """
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return (
        base_dir / "curated" / "metro"
        / metro_coc_membership_filename(definition_version)
    )


def metro_county_membership_path(
    definition_version: str,
    base_dir: Path | str | None = None,
) -> Path:
    """Canonical path for metro-to-county membership file.

    Returns:
        Path like data/curated/metro/metro_county_membership__glynn_fox_v1.parquet
    """
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return (
        base_dir / "curated" / "metro"
        / metro_county_membership_filename(definition_version)
    )


def metro_boundaries_path(
    definition_version: str,
    county_vintage: str | int,
    base_dir: Path | str | None = None,
) -> Path:
    """Canonical path for metro boundary polygons file.

    Returns:
        Path like data/curated/metro/metro_boundaries__glynn_fox_v1xC2025.parquet
    """
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return (
        base_dir / "curated" / "metro"
        / metro_boundaries_filename(definition_version, county_vintage)
    )


def metro_universe_path(
    definition_version: str,
    base_dir: Path | str | None = None,
) -> Path:
    """Canonical path for the metro-universe file."""
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return (
        base_dir / "curated" / "metro"
        / metro_universe_filename(definition_version)
    )


def metro_subset_membership_path(
    profile_definition_version: str,
    metro_definition_version: str,
    base_dir: Path | str | None = None,
) -> Path:
    """Canonical path for the metro subset-profile file."""
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return (
        base_dir / "curated" / "metro"
        / metro_subset_membership_filename(
            profile_definition_version,
            metro_definition_version,
        )
    )


# =============================================================================
# MSA definition artifact filenames
# =============================================================================


def msa_definitions_filename(definition_version: str) -> str:
    """Filename for MSA definitions table.

    Pattern: ``msa_definitions__{version}.parquet``.
    """
    return f"msa_definitions__{definition_version}.parquet"


def msa_county_membership_filename(definition_version: str) -> str:
    """Filename for MSA-to-county membership table.

    Pattern: ``msa_county_membership__{version}.parquet``.
    """
    return f"msa_county_membership__{definition_version}.parquet"


def msa_boundaries_filename(definition_version: str) -> str:
    """Filename for curated MSA boundary polygons.

    Pattern: ``msa_boundaries__{version}.parquet``.
    """
    return f"msa_boundaries__{definition_version}.parquet"


# =============================================================================
# MSA definition artifact paths
# =============================================================================


def msa_definitions_path(
    definition_version: str,
    base_dir: Path | str | None = None,
) -> Path:
    """Canonical path for MSA definitions file."""
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return (
        base_dir / "curated" / "msa"
        / msa_definitions_filename(definition_version)
    )


def msa_county_membership_path(
    definition_version: str,
    base_dir: Path | str | None = None,
) -> Path:
    """Canonical path for MSA-to-county membership file."""
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return (
        base_dir / "curated" / "msa"
        / msa_county_membership_filename(definition_version)
    )


def msa_boundaries_path(
    definition_version: str,
    base_dir: Path | str | None = None,
) -> Path:
    """Canonical path for curated MSA boundary polygons."""
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return (
        base_dir / "curated" / "msa"
        / msa_boundaries_filename(definition_version)
    )


# =============================================================================
# BLS LAUS metro artifact filenames
# =============================================================================


def laus_metro_filename(year: int | str, definition_version: str) -> str:
    """Generate filename for a curated BLS LAUS metro yearly ingest artifact.

    Args:
        year: Reference year for the annual-average LAUS data (e.g., 2023).
        definition_version: Synthetic geography definition version (e.g., "glynn_fox_v1").

    Returns:
        Filename like 'laus_metro__A2023@Dglynnfoxv1.parquet'
    """
    defn = _normalize_definition_version(definition_version)
    return f"laus_metro__A{year}@D{defn}.parquet"


def laus_metro_path(
    year: int | str,
    definition_version: str,
    base_dir: Path | str | None = None,
) -> Path:
    """Get canonical path for a curated BLS LAUS metro yearly ingest artifact.

    Args:
        year: Reference year for the annual-average LAUS data (e.g., 2023).
        definition_version: Synthetic geography definition version (e.g., "glynn_fox_v1").
        base_dir: Base data directory (defaults to "data").

    Returns:
        Path like data/curated/laus/laus_metro__A2023@Dglynnfoxv1.parquet
    """
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return base_dir / "curated" / "laus" / laus_metro_filename(year, definition_version)


# =============================================================================
# Geography-aware filename dispatcher
# =============================================================================


def geo_panel_filename(
    start_year: int,
    end_year: int,
    *,
    geo_type: str = "coc",
    boundary_vintage: str | None = None,
    definition_version: str | None = None,
    profile_definition_version: str | None = None,
) -> str:
    """Return the panel filename for any supported analysis geography.

    For ``geo_type="coc"``, delegates to :func:`panel_filename`.
    For ``geo_type="metro"``, delegates to :func:`metro_panel_filename`.
    For ``geo_type="msa"``, delegates to :func:`msa_panel_filename`.
    """
    if geo_type == "coc":
        if boundary_vintage is None:
            raise ValueError("boundary_vintage is required for geo_type='coc'")
        return panel_filename(start_year, end_year, boundary_vintage)
    if geo_type == "metro":
        if definition_version is None:
            raise ValueError("definition_version is required for geo_type='metro'")
        return metro_panel_filename(
            start_year,
            end_year,
            definition_version,
            profile_definition_version=profile_definition_version,
        )
    if geo_type == "msa":
        if definition_version is None:
            raise ValueError("definition_version is required for geo_type='msa'")
        return msa_panel_filename(start_year, end_year, definition_version)
    raise ValueError(f"Unsupported geo_type: {geo_type!r}")


def geo_map_filename(
    start_year: int,
    end_year: int,
    *,
    geo_type: str = "coc",
    boundary_vintage: str | None = None,
    definition_version: str | None = None,
    profile_definition_version: str | None = None,
) -> str:
    """Return the HTML map filename for any supported analysis geography."""
    panel_name = geo_panel_filename(
        start_year,
        end_year,
        geo_type=geo_type,
        boundary_vintage=boundary_vintage,
        definition_version=definition_version,
        profile_definition_version=profile_definition_version,
    )
    return panel_name.replace("panel__", "map__", 1).replace(".parquet", ".html")
