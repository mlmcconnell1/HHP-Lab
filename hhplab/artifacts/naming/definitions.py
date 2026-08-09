"""Synthetic geography definition artifact naming."""

from __future__ import annotations

from pathlib import Path

__all__ = [
    "metro_boundaries_filename",
    "metro_boundaries_path",
    "metro_coc_membership_filename",
    "metro_coc_membership_path",
    "metro_county_membership_filename",
    "metro_county_membership_path",
    "metro_definitions_filename",
    "metro_definitions_path",
    "metro_subset_membership_filename",
    "metro_subset_membership_path",
    "metro_universe_filename",
    "metro_universe_path",
    "msa_boundaries_filename",
    "msa_boundaries_path",
    "msa_county_membership_filename",
    "msa_county_membership_path",
    "msa_definitions_filename",
    "msa_definitions_path",
]

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
        base_dir
        / "curated"
        / "metro"
        / metro_boundaries_filename(definition_version, county_vintage)
    )

def metro_coc_membership_filename(definition_version: str) -> str:
    """Filename for metro-to-CoC membership table.

    Pattern: ``metro_coc_membership__{version}.parquet``

    See :func:`metro_definitions_filename` for normalization note.
    """
    return f"metro_coc_membership__{definition_version}.parquet"

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
    return base_dir / "curated" / "metro" / metro_coc_membership_filename(definition_version)

def metro_county_membership_filename(definition_version: str) -> str:
    """Filename for metro-to-county membership table.

    Pattern: ``metro_county_membership__{version}.parquet``

    See :func:`metro_definitions_filename` for normalization note.
    """
    return f"metro_county_membership__{definition_version}.parquet"

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
    return base_dir / "curated" / "metro" / metro_county_membership_filename(definition_version)

def metro_definitions_filename(definition_version: str) -> str:
    """Filename for metro definitions table.

    Pattern: ``metro_definitions__{version}.parquet``

    Note: definition/membership filenames preserve the raw version string
    (e.g., ``glynn_fox_v1``) for human readability. Data artifact filenames
    (PIT, ACS, ZORI, panels) normalize to alphanumeric (``glynnfoxv1``).
    """
    return f"metro_definitions__{definition_version}.parquet"

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
    return base_dir / "curated" / "metro" / metro_definitions_filename(definition_version)

def metro_subset_membership_filename(
    profile_definition_version: str,
    metro_definition_version: str,
) -> str:
    """Filename for a subset-profile over the canonical metro universe."""
    return (
        f"metro_subset_membership__{profile_definition_version}xM{metro_definition_version}.parquet"
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
        base_dir
        / "curated"
        / "metro"
        / metro_subset_membership_filename(
            profile_definition_version,
            metro_definition_version,
        )
    )


# =============================================================================
# MSA definition artifact filenames
# =============================================================================

def metro_universe_filename(definition_version: str) -> str:
    """Filename for canonical metro-universe definitions."""
    return f"metro_universe__{definition_version}.parquet"

def metro_universe_path(
    definition_version: str,
    base_dir: Path | str | None = None,
) -> Path:
    """Canonical path for the metro-universe file."""
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return base_dir / "curated" / "metro" / metro_universe_filename(definition_version)

def msa_boundaries_filename(definition_version: str) -> str:
    """Filename for curated MSA boundary polygons.

    Pattern: ``msa_boundaries__{version}.parquet``.
    """
    return f"msa_boundaries__{definition_version}.parquet"


# =============================================================================
# MSA definition artifact paths
# =============================================================================

def msa_boundaries_path(
    definition_version: str,
    base_dir: Path | str | None = None,
) -> Path:
    """Canonical path for curated MSA boundary polygons."""
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return base_dir / "curated" / "msa" / msa_boundaries_filename(definition_version)


# =============================================================================
# Sanctuary jurisdiction regression artifact paths
# =============================================================================

def msa_county_membership_filename(definition_version: str) -> str:
    """Filename for MSA-to-county membership table.

    Pattern: ``msa_county_membership__{version}.parquet``.
    """
    return f"msa_county_membership__{definition_version}.parquet"

def msa_county_membership_path(
    definition_version: str,
    base_dir: Path | str | None = None,
) -> Path:
    """Canonical path for MSA-to-county membership file."""
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return base_dir / "curated" / "msa" / msa_county_membership_filename(definition_version)

def msa_definitions_filename(definition_version: str) -> str:
    """Filename for MSA definitions table.

    Pattern: ``msa_definitions__{version}.parquet``.
    """
    return f"msa_definitions__{definition_version}.parquet"

def msa_definitions_path(
    definition_version: str,
    base_dir: Path | str | None = None,
) -> Path:
    """Canonical path for MSA definitions file."""
    if base_dir is None:
        base_dir = Path("data")
    else:
        base_dir = Path(base_dir)
    return base_dir / "curated" / "msa" / msa_definitions_filename(definition_version)

