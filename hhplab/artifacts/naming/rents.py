"""Rent and covariate artifact naming."""

from __future__ import annotations

from pathlib import Path

from .shared import (
    _abbreviate_weighting,
    _normalize_acs_vintage,
    _normalize_definition_version,
)

__all__ = [
    "discover_zori_ingest",
    "metro_zori_filename",
    "msa_zori_yearly_filename",
    "zori_filename",
    "zori_ingest_filename",
    "zori_ingest_path",
    "zori_yearly_filename",
]

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
        from hhplab.storage.paths import curated_dir

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
    return f"zori__metro__A{acs_year}@D{defn}xC{county_vintage}__w{weight_abbrev}.parquet"


# =============================================================================
# Metro definition artifact filenames
# =============================================================================

def msa_zori_yearly_filename(
    start_year: str | int,
    end_year: str | int,
    definition_version: str,
    county_vintage: str | int,
    weighting: str,
    yearly_method: str,
    *,
    balanced_composition: bool = True,
) -> str:
    """Generate filename for MSA-scoped yearly ZORI panels.

    Pattern:
    ``zori__msa__Y{start}-{end}@M{def}xC{county}__w{weight}__m{method}__balanced.parquet``
    """
    defn = _normalize_definition_version(definition_version)
    weight_abbrev = _abbreviate_weighting(weighting)
    balance_token = "__balanced" if balanced_composition else ""
    return (
        f"zori__msa__Y{start_year}-{end_year}@M{defn}xC{county_vintage}"
        f"__w{weight_abbrev}__m{yearly_method}{balance_token}.parquet"
    )


# =============================================================================
# ACS tract population files
# =============================================================================

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

