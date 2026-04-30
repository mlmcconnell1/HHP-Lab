"""Unified source data registry for tracking external data ingestion.

This module provides a centralized registry for tracking all external data sources
ingested into the HHP-Lab pipeline. It records:
- Source URL and type
- Download timestamps
- Raw content hashes (SHA-256)
- File sizes

This enables detection of upstream data changes without notification, which is
critical for data pipeline integrity and reproducibility.

Usage
-----
    from hhplab.source_registry import (
        register_source,
        check_source_changed,
        list_sources,
        get_source_history,
    )

    # Register a new download
    entry = register_source(
        source_type="zori",
        source_url="https://files.zillowstatic.com/...",
        raw_sha256="abc123...",
        file_size=1234567,
        local_path="data/raw/zori/zori__county__2025-01-06.csv",
    )

    # Check if upstream data has changed
    changed, details = check_source_changed(
        source_type="zori",
        source_url="https://files.zillowstatic.com/...",
        current_sha256="def456...",
    )
    if changed:
        print(f"WARNING: Upstream data changed! Previous: {details['previous_sha256']}")

    # List all tracked sources
    for entry in list_sources():
        print(f"{entry.source_type}: {entry.source_url} ({entry.ingested_at})")
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import pandas as pd

from hhplab.paths import curated_root

logger = logging.getLogger(__name__)

# Supported source types
SourceType = Literal[
    "zori",  # Zillow ZORI rent data
    "boundary",  # HUD CoC boundaries
    "census_tract",  # TIGER tract geometries
    "census_county",  # TIGER county geometries
    "nhgis_tract",  # NHGIS tract geometries
    "nhgis_county",  # NHGIS county geometries
    "tract_relationship",  # Census tract relationship file (e.g. 2010-2020)
    "acs5_tract",  # ACS 5-year tract-level data
    "acs5_county",  # ACS 5-year county-level data
    "pep_county",  # Census PEP county population estimates
    "census_cbsa",  # Census CBSA/MSA delineation workbook
    "pit",  # HUD PIT counts
    "other",  # Other external sources
]


# Registry columns
REGISTRY_COLUMNS = [
    "source_type",  # Type of data source (zori, boundary, etc.)
    "source_url",  # URL or identifier of the source
    "source_name",  # Human-readable name (e.g., "ZORI County Monthly")
    "raw_sha256",  # SHA-256 hash of raw downloaded content
    "file_size",  # Size in bytes of the raw artifact
    "local_path",  # Path to retained raw artifact (file or directory)
    "ingested_at",  # UTC timestamp of ingestion
    "metadata",  # JSON string with additional metadata
]
# Semantic note: ``local_path`` always points to the **retained raw
# artifact** (a file under ``data/raw/`` or a snapshot directory).  If a
# curated output lives at a different location, store its path in
# ``metadata["curated_path"]``.


@dataclass
class SourceRegistryEntry:
    """A single entry in the source data registry.

    ``local_path`` references the retained raw artifact (ZIP, CSV, or
    API snapshot directory under ``data/raw/``).  The curated output
    path, when different, is stored in ``metadata["curated_path"]``.
    """

    source_type: str
    source_url: str
    raw_sha256: str
    ingested_at: datetime
    source_name: str = ""
    file_size: int = 0
    local_path: str = ""
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Convert to dictionary for DataFrame row."""
        return {
            "source_type": self.source_type,
            "source_url": self.source_url,
            "source_name": self.source_name,
            "raw_sha256": self.raw_sha256,
            "file_size": self.file_size,
            "local_path": self.local_path,
            "ingested_at": self.ingested_at,
            "metadata": json.dumps(self.metadata) if self.metadata else "{}",
        }

    @classmethod
    def from_dict(cls, d: dict) -> SourceRegistryEntry:
        """Create from dictionary (DataFrame row)."""
        metadata = d.get("metadata", "{}")
        if isinstance(metadata, str):
            metadata = json.loads(metadata) if metadata else {}
        return cls(
            source_type=d["source_type"],
            source_url=d["source_url"],
            source_name=d.get("source_name", ""),
            raw_sha256=d["raw_sha256"],
            file_size=d.get("file_size", 0),
            local_path=d.get("local_path", ""),
            ingested_at=d["ingested_at"],
            metadata=metadata,
        )


def _load_registry(registry_path: Path | None = None) -> pd.DataFrame:
    """Load the source registry from disk.

    Returns empty DataFrame with correct schema if file doesn't exist.
    """
    if registry_path is None:
        registry_path = curated_root() / "source_registry.parquet"

    if not registry_path.exists():
        return pd.DataFrame(columns=REGISTRY_COLUMNS)

    return pd.read_parquet(registry_path)


def _save_registry(df: pd.DataFrame, registry_path: Path | None = None) -> None:
    """Save the source registry to disk with embedded provenance."""
    from hhplab.provenance import ProvenanceBlock, write_parquet_with_provenance

    if registry_path is None:
        registry_path = curated_root() / "source_registry.parquet"

    registry_path.parent.mkdir(parents=True, exist_ok=True)

    # Ensure proper types
    if "ingested_at" in df.columns:
        df["ingested_at"] = pd.to_datetime(df["ingested_at"], utc=True)

    provenance = ProvenanceBlock(
        extra={
            "dataset_type": "source_registry",
            "entry_count": len(df),
        }
    )
    write_parquet_with_provenance(df, registry_path, provenance)
    logger.debug(f"Saved source registry to {registry_path}")


def register_source(
    source_type: str,
    source_url: str,
    raw_sha256: str,
    source_name: str = "",
    file_size: int = 0,
    local_path: str | Path = "",
    metadata: dict | None = None,
    registry_path: Path | None = None,
) -> SourceRegistryEntry:
    """Register a new source data ingestion.

    This records that data was downloaded from an external source. Each
    ingestion creates a new entry (history is preserved).

    Parameters
    ----------
    source_type : str
        Type of data source (zori, boundary, census_tract, etc.).
    source_url : str
        URL or identifier of the source.
    raw_sha256 : str
        SHA-256 hash of the raw downloaded content.
    source_name : str, optional
        Human-readable name for the source.
    file_size : int, optional
        Size of the downloaded file in bytes.
    local_path : str or Path, optional
        Path to the retained raw artifact (file or snapshot directory
        under ``data/raw/``).  Store the curated output path in
        ``metadata["curated_path"]`` when it differs.
    metadata : dict, optional
        Additional metadata to store.
    registry_path : Path, optional
        Path to registry file. Defaults to data/curated/source_registry.parquet.

    Returns
    -------
    SourceRegistryEntry
        The registered entry.
    """
    entry = SourceRegistryEntry(
        source_type=source_type,
        source_url=source_url,
        source_name=source_name,
        raw_sha256=raw_sha256,
        file_size=file_size,
        local_path=str(local_path) if local_path else "",
        ingested_at=datetime.now(UTC),
        metadata=metadata or {},
    )

    # Load existing registry
    df = _load_registry(registry_path)

    # Append new entry
    new_row = pd.DataFrame([entry.to_dict()])
    if df.empty:
        df = new_row
    else:
        df = pd.concat([df, new_row], ignore_index=True)

    # Save
    _save_registry(df, registry_path)

    logger.info(
        f"Registered source: {source_type} from {source_url[:50]}... (sha256: {raw_sha256[:10]}...)"
    )

    return entry


def get_latest_source(
    source_type: str,
    source_url: str | None = None,
    registry_path: Path | None = None,
) -> SourceRegistryEntry | None:
    """Get the most recent entry for a source type (and optionally URL).

    Parameters
    ----------
    source_type : str
        Type of data source.
    source_url : str, optional
        If provided, filter to this specific URL.
    registry_path : Path, optional
        Path to registry file.

    Returns
    -------
    SourceRegistryEntry or None
        The most recent entry, or None if not found.
    """
    df = _load_registry(registry_path)

    if df.empty:
        return None

    # Filter by type
    mask = df["source_type"] == source_type
    if source_url is not None:
        mask &= df["source_url"] == source_url

    filtered = df[mask]

    if filtered.empty:
        return None

    # Get most recent
    filtered = filtered.sort_values("ingested_at", ascending=False)
    row = filtered.iloc[0].to_dict()

    return SourceRegistryEntry.from_dict(row)


def check_source_changed(
    source_type: str,
    source_url: str,
    current_sha256: str,
    registry_path: Path | None = None,
) -> tuple[bool, dict]:
    """Check if a source's data has changed from the last registered version.

    Parameters
    ----------
    source_type : str
        Type of data source.
    source_url : str
        URL of the source.
    current_sha256 : str
        SHA-256 hash of the current download.
    registry_path : Path, optional
        Path to registry file.

    Returns
    -------
    tuple[bool, dict]
        (changed, details) where:
        - changed: True if data differs from last registered version
        - details: Dict with 'previous_sha256', 'previous_ingested_at', 'is_new'
    """
    latest = get_latest_source(source_type, source_url, registry_path)

    if latest is None:
        return False, {"is_new": True, "previous_sha256": None, "previous_ingested_at": None}

    changed = latest.raw_sha256 != current_sha256

    return changed, {
        "is_new": False,
        "previous_sha256": latest.raw_sha256,
        "previous_ingested_at": latest.ingested_at,
        "changed": changed,
    }


def get_source_history(
    source_type: str,
    source_url: str | None = None,
    registry_path: Path | None = None,
) -> list[SourceRegistryEntry]:
    """Get all historical entries for a source type.

    Parameters
    ----------
    source_type : str
        Type of data source.
    source_url : str, optional
        If provided, filter to this specific URL.
    registry_path : Path, optional
        Path to registry file.

    Returns
    -------
    list[SourceRegistryEntry]
        All entries, sorted by ingested_at descending (most recent first).
    """
    df = _load_registry(registry_path)

    if df.empty:
        return []

    # Filter by type
    mask = df["source_type"] == source_type
    if source_url is not None:
        mask &= df["source_url"] == source_url

    filtered = df[mask].sort_values("ingested_at", ascending=False)

    return [SourceRegistryEntry.from_dict(row.to_dict()) for _, row in filtered.iterrows()]


def list_sources(
    source_type: str | None = None,
    registry_path: Path | None = None,
) -> list[SourceRegistryEntry]:
    """List all registered sources (most recent entry per type/URL).

    Parameters
    ----------
    source_type : str, optional
        If provided, filter to this source type.
    registry_path : Path, optional
        Path to registry file.

    Returns
    -------
    list[SourceRegistryEntry]
        Most recent entry for each unique (source_type, source_url) pair.
    """
    df = _load_registry(registry_path)

    if df.empty:
        return []

    if source_type is not None:
        df = df[df["source_type"] == source_type]

    if df.empty:
        return []

    # Get most recent per (source_type, source_url)
    df = df.sort_values("ingested_at", ascending=False)
    df = df.drop_duplicates(subset=["source_type", "source_url"], keep="first")

    return [SourceRegistryEntry.from_dict(row.to_dict()) for _, row in df.iterrows()]


def detect_upstream_changes(
    registry_path: Path | None = None,
) -> pd.DataFrame:
    """Detect which sources have multiple different hashes (indicating upstream changes).

    This identifies sources where the upstream data has changed over time,
    which may indicate silent updates that weren't communicated.

    Parameters
    ----------
    registry_path : Path, optional
        Path to registry file.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns:
        - source_type, source_url
        - hash_count: Number of distinct hashes seen
        - first_seen, last_seen: Timestamps
        - first_hash, last_hash: SHA-256 values
    """
    df = _load_registry(registry_path)

    if df.empty:
        return pd.DataFrame(
            columns=[
                "source_type",
                "source_url",
                "hash_count",
                "first_seen",
                "last_seen",
                "first_hash",
                "last_hash",
            ]
        )

    # Group by source and analyze hash history
    results = []
    for (stype, surl), group in df.groupby(["source_type", "source_url"]):
        group = group.sort_values("ingested_at")
        unique_hashes = group["raw_sha256"].nunique()

        if unique_hashes > 1:
            results.append(
                {
                    "source_type": stype,
                    "source_url": surl,
                    "hash_count": unique_hashes,
                    "first_seen": group["ingested_at"].iloc[0],
                    "last_seen": group["ingested_at"].iloc[-1],
                    "first_hash": group["raw_sha256"].iloc[0],
                    "last_hash": group["raw_sha256"].iloc[-1],
                }
            )

    return pd.DataFrame(results)


def delete_by_local_path(
    local_path: str | Path,
    registry_path: Path | None = None,
) -> int:
    """Delete all source registry entries matching a local path.

    Parameters
    ----------
    local_path : str or Path
        The local_path value to match (exact match).
    registry_path : Path, optional
        Path to registry file.

    Returns
    -------
    int
        Number of entries deleted.
    """
    df = _load_registry(registry_path)

    if df.empty:
        return 0

    local_path_str = str(local_path)
    mask = df["local_path"] == local_path_str
    count = mask.sum()

    if count > 0:
        df = df[~mask]
        _save_registry(df, registry_path)
        logger.info(f"Deleted {count} source registry entries for path: {local_path_str}")

    return count


def delete_by_curated_path(
    curated_path: str | Path,
    registry_path: Path | None = None,
) -> int:
    """Delete source registry entries whose metadata.curated_path matches.

    Parameters
    ----------
    curated_path : str or Path
        The curated path value to match.
    registry_path : Path, optional
        Path to registry file.

    Returns
    -------
    int
        Number of entries deleted.
    """
    df = _load_registry(registry_path)
    if df.empty:
        return 0

    curated_str = str(curated_path)

    def _matches(meta_json: str) -> bool:
        try:
            meta = json.loads(meta_json) if isinstance(meta_json, str) else {}
            return meta.get("curated_path") == curated_str
        except (json.JSONDecodeError, TypeError):
            return False

    mask = df["metadata"].apply(_matches)
    count = int(mask.sum())
    if count > 0:
        df = df[~mask]
        _save_registry(df, registry_path)
        logger.info(f"Deleted {count} source registry entries for curated_path: {curated_str}")
    return count


def summarize_registry(registry_path: Path | None = None) -> str:
    """Generate a text summary of the source registry.

    Parameters
    ----------
    registry_path : Path, optional
        Path to registry file.

    Returns
    -------
    str
        Human-readable summary of tracked sources.
    """
    df = _load_registry(registry_path)

    if df.empty:
        return "Source registry is empty. No external data sources have been tracked yet."

    lines = [
        "=" * 70,
        "SOURCE DATA REGISTRY SUMMARY",
        "=" * 70,
        "",
    ]

    # Summary by type
    type_counts = df.groupby("source_type").agg(
        {
            "source_url": "nunique",
            "raw_sha256": "nunique",
            "ingested_at": ["min", "max", "count"],
        }
    )

    lines.append("SOURCES BY TYPE")
    lines.append("-" * 50)

    for stype in type_counts.index:
        urls = type_counts.loc[stype, ("source_url", "nunique")]
        hashes = type_counts.loc[stype, ("raw_sha256", "nunique")]
        count = type_counts.loc[stype, ("ingested_at", "count")]
        first = type_counts.loc[stype, ("ingested_at", "min")]
        last = type_counts.loc[stype, ("ingested_at", "max")]

        lines.append(f"  {stype}:")
        lines.append(f"    URLs tracked:      {urls}")
        lines.append(f"    Total ingestions:  {count}")
        lines.append(f"    Unique hashes:     {hashes}")
        lines.append(f"    First ingestion:   {first}")
        lines.append(f"    Last ingestion:    {last}")
        lines.append("")

    # Check for upstream changes
    changes = detect_upstream_changes(registry_path)
    if not changes.empty:
        lines.append("DETECTED UPSTREAM CHANGES")
        lines.append("-" * 50)
        lines.append("⚠️  The following sources have changed over time:")
        for _, row in changes.iterrows():
            lines.append(f"  {row['source_type']}: {row['source_url'][:50]}...")
            lines.append(f"    Versions seen: {row['hash_count']}")
            lines.append(f"    First: {row['first_seen']} ({row['first_hash'][:10]}...)")
            lines.append(f"    Last:  {row['last_seen']} ({row['last_hash'][:10]}...)")
            lines.append("")
    else:
        lines.append("No upstream changes detected (all sources have consistent hashes).")

    lines.append("=" * 70)

    return "\n".join(lines)
