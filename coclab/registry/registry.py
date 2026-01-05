"""Registry for tracking and managing boundary vintages."""

import hashlib
import re
from datetime import datetime
from pathlib import Path

import pandas as pd

from coclab.registry.schema import REGISTRY_COLUMNS, RegistryEntry

# Default registry location
DEFAULT_REGISTRY_PATH = Path("data/curated/boundary_registry.parquet")


def _get_registry_path(registry_path: Path | None = None) -> Path:
    """Get the registry path, using default if not specified."""
    return registry_path or DEFAULT_REGISTRY_PATH


def _load_registry(registry_path: Path) -> pd.DataFrame:
    """Load the registry from disk, or return empty DataFrame if not exists."""
    if registry_path.exists():
        df = pd.read_parquet(registry_path)
        # Ensure ingested_at is datetime
        df["ingested_at"] = pd.to_datetime(df["ingested_at"], utc=True)
        return df
    # Return empty DataFrame with proper dtypes
    df = pd.DataFrame(columns=REGISTRY_COLUMNS)
    return df


def _prepare_for_save(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare DataFrame for saving to Parquet with consistent types."""
    df = df.copy()
    # Ensure ingested_at is datetime for proper Parquet serialization
    df["ingested_at"] = pd.to_datetime(df["ingested_at"], utc=True)
    # Ensure path is string
    df["path"] = df["path"].astype(str)
    return df


def _save_registry(df: pd.DataFrame, registry_path: Path) -> None:
    """Save registry to disk as Parquet."""
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    df = _prepare_for_save(df)
    df.to_parquet(registry_path, index=False)


def compute_file_hash(file_path: Path) -> str:
    """Compute SHA-256 hash of a file."""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def register_vintage(
    boundary_vintage: str,
    source: str,
    path: Path,
    feature_count: int,
    ingested_at: datetime | None = None,
    hash_of_file: str | None = None,
    registry_path: Path | None = None,
) -> RegistryEntry:
    """Register a new boundary vintage in the registry.

    Idempotent: if a vintage with the same boundary_vintage and source already
    exists with the same hash, the existing entry is returned. If the hash
    differs, the entry is updated.

    Args:
        boundary_vintage: Version identifier (e.g., '2025')
        source: Origin of the data ('hud_exchange_gis_tools' | 'hud_opendata_arcgis')
        path: Path to the curated GeoParquet file
        feature_count: Number of features in the dataset
        ingested_at: UTC timestamp (defaults to now)
        hash_of_file: SHA-256 of file (computed if not provided)
        registry_path: Custom registry path (uses default if not specified)

    Returns:
        The registered RegistryEntry
    """
    reg_path = _get_registry_path(registry_path)
    df = _load_registry(reg_path)

    # Compute hash if not provided
    if hash_of_file is None:
        hash_of_file = compute_file_hash(path)

    # Use current time if not provided
    if ingested_at is None:
        ingested_at = datetime.now().astimezone()

    entry = RegistryEntry(
        boundary_vintage=boundary_vintage,
        source=source,
        ingested_at=ingested_at,
        path=path,
        feature_count=feature_count,
        hash_of_file=hash_of_file,
    )

    # Check for existing entry with same vintage and source
    mask = (df["boundary_vintage"] == boundary_vintage) & (df["source"] == source)

    if mask.any():
        existing = df.loc[mask].iloc[0]
        if existing["hash_of_file"] == hash_of_file:
            # Same content, return existing entry
            return RegistryEntry.from_dict(existing.to_dict())
        # Different content, update existing entry
        df.loc[mask, "ingested_at"] = ingested_at.isoformat()
        df.loc[mask, "path"] = str(path)
        df.loc[mask, "feature_count"] = feature_count
        df.loc[mask, "hash_of_file"] = hash_of_file
    else:
        # New entry
        new_row = pd.DataFrame([entry.to_dict()])
        df = pd.concat([df, new_row], ignore_index=True)

    _save_registry(df, reg_path)
    return entry


def list_vintages(registry_path: Path | None = None) -> list[RegistryEntry]:
    """List all registered vintages.

    Args:
        registry_path: Custom registry path (uses default if not specified)

    Returns:
        List of RegistryEntry objects, sorted by ingested_at descending
    """
    reg_path = _get_registry_path(registry_path)
    df = _load_registry(reg_path)

    if df.empty:
        return []

    # Sort by ingested_at descending
    df = df.sort_values("ingested_at", ascending=False)

    return [RegistryEntry.from_dict(row.to_dict()) for _, row in df.iterrows()]


def _extract_year(vintage: str) -> int | None:
    """Extract year from a vintage string."""
    # Try direct year match (e.g., "2025")
    if vintage.isdigit() and len(vintage) == 4:
        return int(vintage)
    # Try to extract year from pattern like "HUDOpenData_2025-08-19"
    match = re.search(r"(\d{4})", vintage)
    if match:
        return int(match.group(1))
    return None


def latest_vintage(
    source: str | None = None,
    registry_path: Path | None = None,
) -> str | None:
    """Get the latest boundary vintage.

    Selection policy:
    - For hud_exchange_gis_tools: prefer highest year number
    - For hud_opendata_arcgis: prefer most recent ingested_at
    - If source not specified: apply source-specific policy, then pick overall latest

    Args:
        source: Optionally filter by source
        registry_path: Custom registry path (uses default if not specified)

    Returns:
        The boundary_vintage string of the latest entry, or None if empty
    """
    reg_path = _get_registry_path(registry_path)
    df = _load_registry(reg_path)

    if df.empty:
        return None

    # Filter by source if specified
    if source:
        df = df[df["source"] == source]
        if df.empty:
            return None

    # Apply source-specific selection policy
    if source == "hud_exchange_gis_tools":
        # For HUD Exchange, prefer highest year
        df = df.copy()
        df["_year"] = df["boundary_vintage"].apply(_extract_year)
        valid = df[df["_year"].notna()]
        if not valid.empty:
            return valid.loc[valid["_year"].idxmax(), "boundary_vintage"]
        # Fallback to most recent ingested_at
        return df.loc[df["ingested_at"].idxmax(), "boundary_vintage"]

    if source == "hud_opendata_arcgis":
        # For OpenData, prefer most recent ingested_at
        return df.loc[df["ingested_at"].idxmax(), "boundary_vintage"]

    # No source specified: prefer hud_exchange with highest year, then fallback
    hud_exchange = df[df["source"] == "hud_exchange_gis_tools"]
    if not hud_exchange.empty:
        hud_exchange = hud_exchange.copy()
        hud_exchange["_year"] = hud_exchange["boundary_vintage"].apply(_extract_year)
        valid = hud_exchange[hud_exchange["_year"].notna()]
        if not valid.empty:
            return valid.loc[valid["_year"].idxmax(), "boundary_vintage"]

    # Fallback to most recent ingested_at across all sources
    return df.loc[df["ingested_at"].idxmax(), "boundary_vintage"]
