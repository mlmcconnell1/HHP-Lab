"""Provider-owned source artifact naming."""

from __future__ import annotations

from pathlib import Path

from .shared import (
    _normalize_acs_vintage,
    _normalize_definition_version,
)

__all__ = [
    "acs1_county_filename",
    "acs1_county_path",
    "acs1_metro_filename",
    "acs1_metro_path",
    "acs1_poverty_tracts_filename",
    "acs1_poverty_tracts_path",
    "acs5_tracts_filename",
    "acs5_tracts_glob_pattern",
    "cdc_overdose_county_filename",
    "cdc_overdose_msa_filename",
    "coc_pep_filename",
    "coc_pit_filename",
    "cpi_u_filename",
    "cpi_u_path",
    "decennial_tracts_filename",
    "discover_pit_vintages",
    "hic_filename",
    "hic_path",
    "laus_metro_filename",
    "laus_metro_path",
    "medsl_president_county_filename",
    "medsl_president_county_path",
    "metro_pep_filename",
    "metro_pit_filename",
    "msa_pep_filename",
    "msa_pep_path",
    "msa_pit_filename",
    "pit_filename",
    "pit_path",
    "pit_vintage_filename",
    "pit_vintage_path",
    "pl_block_population_filename",
    "pl_block_population_path",
    "prism_county_monthly_filename",
    "prism_county_monthly_path",
    "sanctuary_msa_matches_filename",
    "sanctuary_msa_matches_path",
    "vera_incarceration_county_filename",
    "vera_incarceration_county_path",
]

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

def acs1_poverty_tracts_filename(acs1_vintage: int, tract_vintage: str | int) -> str:
    """Generate filename for curated ACS 1-year tract poverty-rate artifacts."""
    return f"acs1_poverty_tracts__A{acs1_vintage}xT{tract_vintage}.parquet"

def acs1_poverty_tracts_path(
    acs1_vintage: int,
    tract_vintage: str | int,
    base_dir: Path | str | None = None,
) -> Path:
    """Get canonical path for ACS 1-year tract poverty-rate artifacts."""
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return (
        base_dir
        / "curated"
        / "acs"
        / acs1_poverty_tracts_filename(
            acs1_vintage,
            tract_vintage,
        )
    )

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

def acs5_tracts_glob_pattern(acs_vintage: str | None = None) -> str:
    """Return the canonical glob pattern for ACS 5-year tract artifacts."""
    acs_token = "*" if acs_vintage is None else _normalize_acs_vintage(acs_vintage)
    return f"acs5_tracts__A{acs_token}xT*.parquet"

def cdc_overdose_county_filename(
    start_year: int,
    end_year: int,
    county_vintage: str | int,
) -> str:
    """Canonical filename for CDC county overdose annual January extracts.

    Pattern: ``cdc_overdose__county__Y{start}-{end}@C{county}.parquet``
    """
    return f"cdc_overdose__county__Y{start_year}-{end_year}@C{county_vintage}.parquet"

def cdc_overdose_msa_filename(
    start_year: int,
    end_year: int,
    definition_version: str,
    county_vintage: str | int,
) -> str:
    """Canonical filename for CDC overdose MSA rollups.

    Pattern: ``cdc_overdose__msa__Y{start}-{end}@M{def}xC{county}.parquet``
    """
    defn = _normalize_definition_version(definition_version)
    return (
        f"cdc_overdose__msa__Y{start_year}-{end_year}"
        f"@M{defn}xC{county_vintage}.parquet"
    )

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

def coc_pit_filename(pit_year: str | int, boundary_vintage: str | int) -> str:
    """Generate filename for PIT data aligned to a boundary vintage.

    Args:
        pit_year: PIT count year (e.g., 2024)
        boundary_vintage: CoC boundary vintage (e.g., 2024)

    Returns:
        Filename like 'pit__P2024@B2024.parquet'
    """
    return f"pit__P{pit_year}@B{boundary_vintage}.parquet"

def cpi_u_filename() -> str:
    """Generate filename for the curated annual CPI-U index artifact.

    Returns:
        Filename ``cpi_u__Aall.parquet``.
    """
    return "cpi_u__Aall.parquet"

def cpi_u_path(base_dir: Path | str | None = None) -> Path:
    """Get canonical path for the curated annual CPI-U index artifact.

    Args:
        base_dir: Base data directory (defaults to "data").

    Returns:
        Path like ``data/curated/cpi/cpi_u__Aall.parquet``.
    """
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return base_dir / "curated" / "cpi" / cpi_u_filename()


# =============================================================================
# Geography-aware filename dispatcher
# =============================================================================

def decennial_tracts_filename(
    decennial_vintage: str | int,
    tract_vintage: str | int | None = None,
) -> str:
    """Generate filename for decennial tract population denominators.

    Args:
        decennial_vintage: Decennial census year, currently 2010 or 2020.
        tract_vintage: Target tract geometry vintage. Defaults to the decennial
            vintage because decennial denominators are native to their era.

    Returns:
        Filename like ``decennial_tracts__N2020xT2020.parquet``.
    """
    resolved_tract_vintage = decennial_vintage if tract_vintage is None else tract_vintage
    return f"decennial_tracts__N{decennial_vintage}xT{resolved_tract_vintage}.parquet"

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

def hic_filename(hic_year: str | int) -> str:
    """Generate filename for HUD HIC count data.

    Args:
        hic_year: Housing Inventory Count year (e.g., 2024 or "2024")

    Returns:
        Filename like 'hic__H2024.parquet'
    """
    return f"hic__H{hic_year}.parquet"

def hic_path(hic_year: str | int, base_dir: Path | str | None = None) -> Path:
    """Generate full path for HIC count data.

    Args:
        hic_year: HIC inventory year
        base_dir: Base data directory. If None, uses config asset store root.

    Returns:
        Path like data/curated/hic/hic__H2024.parquet
    """
    if base_dir is None:
        from hhplab.storage.paths import asset_store_root

        base_dir = asset_store_root()
    else:
        base_dir = Path(base_dir)
    return base_dir / "curated" / "hic" / hic_filename(hic_year)

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
# BLS CPI-U artifact filenames
# =============================================================================

def medsl_president_county_filename(
    start_year: str | int,
    end_year: str | int,
    county_vintage: str | int,
) -> str:
    """Generate filename for MEDSL county presidential leaning measures."""
    return f"medsl_president_county__Y{start_year}-{end_year}@C{county_vintage}.parquet"

def medsl_president_county_path(
    start_year: str | int,
    end_year: str | int,
    county_vintage: str | int,
    base_dir: Path | str | None = None,
) -> Path:
    """Get canonical path for MEDSL county presidential leaning measures."""
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return (
        base_dir
        / "curated"
        / "medsl"
        / medsl_president_county_filename(start_year, end_year, county_vintage)
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
    return f"pep__metro__D{defn}xC{county_vintage}__w{weighting}__{start_year}_{end_year}.parquet"

def metro_pit_filename(
    pit_year: str | int,
    definition_version: str,
) -> str:
    """Generate filename for metro-scoped PIT aggregate.

    Pattern: ``pit__metro__P{year}@D{def}.parquet``
    """
    defn = _normalize_definition_version(definition_version)
    return f"pit__metro__P{pit_year}@D{defn}.parquet"

def msa_pep_filename(
    year: int | str,
    definition_version: str,
    county_vintage: int | str,
    weighting: str = "population",
) -> str:
    """Canonical filename for one MSA-level PEP aggregate output.

    Pattern: ``pep__msa__Y{year}@M{def}xC{county}__w{weighting}.parquet``.
    """
    defn = _normalize_definition_version(definition_version)
    return f"pep__msa__Y{year}@M{defn}xC{county_vintage}__w{weighting}.parquet"

def msa_pep_path(
    year: int | str,
    definition_version: str,
    county_vintage: int | str,
    weighting: str = "population",
    base_dir: Path | str | None = None,
) -> Path:
    """Get canonical path for one MSA-level PEP aggregate output."""
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return (
        base_dir
        / "curated"
        / "pep"
        / msa_pep_filename(year, definition_version, county_vintage, weighting)
    )

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
    return f"pit__msa__P{pit_year}@M{defn}xB{boundary_vintage}xC{county_vintage}.parquet"

def pit_filename(pit_year: str | int) -> str:
    """Generate filename for PIT count data.

    Args:
        pit_year: PIT count year (e.g., 2024 or "2024")

    Returns:
        Filename like 'pit__P2024.parquet'
    """
    return f"pit__P{pit_year}.parquet"

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

def pit_vintage_filename(vintage: str | int) -> str:
    """Generate filename for PIT vintage file (containing all years from one release).

    Args:
        vintage: PIT release vintage year (e.g., 2024)

    Returns:
        Filename like 'pit_vintage__P2024.parquet'
    """
    return f"pit_vintage__P{vintage}.parquet"

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

def pl_block_population_filename(
    decennial_vintage: str | int,
    block_vintage: str | int | None = None,
) -> str:
    """Generate filename for PL 94-171 block population denominators.

    Args:
        decennial_vintage: Decennial census vintage, currently 2010 or 2020.
        block_vintage: Block geometry vintage. Defaults to the decennial vintage.

    Returns:
        Filename like ``pl_blocks__N2020xK2020.parquet``.
    """
    resolved_block_vintage = decennial_vintage if block_vintage is None else block_vintage
    return f"pl_blocks__N{decennial_vintage}xK{resolved_block_vintage}.parquet"

def pl_block_population_path(
    decennial_vintage: str | int,
    block_vintage: str | int | None = None,
    base_dir: Path | str | None = None,
) -> Path:
    """Get canonical path for PL 94-171 block population denominators."""
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return (
        base_dir
        / "curated"
        / "census"
        / pl_block_population_filename(decennial_vintage, block_vintage)
    )

def prism_county_monthly_filename(
    variable: str,
    year: str | int,
    month: str | int,
    county_vintage: str | int,
) -> str:
    """Generate filename for curated PRISM county-month temperature artifacts."""
    month_int = int(month)
    return f"prism_county_monthly__{variable}__Y{year}M{month_int:02d}@C{county_vintage}.parquet"

def prism_county_monthly_path(
    variable: str,
    year: str | int,
    month: str | int,
    county_vintage: str | int,
    base_dir: Path | str | None = None,
) -> Path:
    """Get canonical path for curated PRISM county-month temperature artifacts."""
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return (
        base_dir
        / "curated"
        / "prism"
        / prism_county_monthly_filename(variable, year, month, county_vintage)
    )

def sanctuary_msa_matches_filename(
    source_date: str,
    msa_definition_version: str,
) -> str:
    """Canonical filename for DOJ sanctuary jurisdiction MSA matches."""
    compact_date = source_date.replace("-", "")
    return f"sanctuary_msa_matches__D{compact_date}xM{msa_definition_version}.parquet"

def sanctuary_msa_matches_path(
    source_date: str,
    msa_definition_version: str,
    base_dir: Path | str | None = None,
) -> Path:
    """Canonical path for DOJ sanctuary jurisdiction MSA matches."""
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return (
        base_dir
        / "curated"
        / "sanctuary"
        / sanctuary_msa_matches_filename(source_date, msa_definition_version)
    )

def vera_incarceration_county_filename(
    start_year: str | int,
    end_year: str | int,
    county_vintage: str | int,
) -> str:
    """Generate filename for Vera county incarceration trends measures."""
    return f"vera_incarceration_county__Y{start_year}-{end_year}@C{county_vintage}.parquet"

def vera_incarceration_county_path(
    start_year: str | int,
    end_year: str | int,
    county_vintage: str | int,
    base_dir: Path | str | None = None,
) -> Path:
    """Get canonical path for Vera county incarceration trends measures."""
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return (
        base_dir
        / "curated"
        / "vera"
        / vera_incarceration_county_filename(start_year, end_year, county_vintage)
    )

