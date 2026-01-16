"""MANIFEST.json generation for export bundles."""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow.parquet as pq

from coclab.export.hashing import compute_sha256

if TYPE_CHECKING:
    from coclab.export.types import ArtifactRecord


def get_coclab_info() -> dict:
    """
    Get coclab version info.

    Returns:
        Dict with keys:
        - version: coclab package version
        - git_commit: git commit hash if available (None otherwise)
        - python: Python version string
    """
    # Get coclab version from package metadata
    try:
        coclab_version = version("coc-lab")
    except PackageNotFoundError:
        coclab_version = "unknown"

    # Get git commit hash if available
    git_commit = None
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0:
            git_commit = result.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        pass

    # Get Python version
    python_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"

    return {
        "version": coclab_version,
        "git_commit": git_commit,
        "python": python_version,
    }


def extract_parquet_metadata(path: Path) -> dict:
    """
    Extract metadata from a parquet file.

    Args:
        path: Path to parquet file

    Returns:
        Dict with keys:
        - rows: total row count
        - columns: list of column names
        - key_columns: list of key column names (heuristically determined)

    Raises:
        FileNotFoundError: If file doesn't exist
        Exception: If parquet cannot be read
    """
    if not path.exists():
        raise FileNotFoundError(f"Parquet file not found: {path}")

    # Read parquet metadata and schema
    metadata = pq.read_metadata(path)
    schema = pq.read_schema(path)

    # Get row count
    rows = metadata.num_rows

    # Get column names from schema
    columns = [field.name for field in schema]

    # Heuristically determine key columns
    # Key columns are typically identifiers (coc_id, year) and important metrics
    potential_key_columns = [
        "coc_id",
        "coc_number",
        "year",
        "pit_total",
        "population",
        "zori_is_eligible",
        "rent_to_income",
        "median_hh_income",
        "median_gross_rent",
    ]
    key_columns = [col for col in potential_key_columns if col in columns]

    return {
        "rows": rows,
        "columns": columns,
        "key_columns": key_columns,
    }


def build_artifact_entry(artifact: ArtifactRecord, bundle_root: Path) -> dict:
    """
    Build artifact entry for manifest.

    Args:
        artifact: ArtifactRecord with artifact info
        bundle_root: Root directory of bundle

    Returns:
        Dict suitable for manifest artifacts list with keys:
        - role: artifact role (panel, input, derived, diagnostic, codebook)
        - path: relative path within bundle
        - sha256: hash of file
        - bytes: file size
        - rows: row count (for parquet files)
        - columns: column count (for parquet files)
        - key_columns: list of key columns (for parquet files)
    """
    full_path = bundle_root / artifact.dest_path

    # Compute hash and size if not already set
    sha256 = artifact.sha256
    if sha256 is None and full_path.exists():
        sha256 = compute_sha256(full_path)

    file_bytes = artifact.bytes
    if file_bytes is None and full_path.exists():
        file_bytes = full_path.stat().st_size

    entry: dict = {
        "role": artifact.role,
        "path": artifact.dest_path,
        "sha256": sha256,
        "bytes": file_bytes,
    }

    # Extract parquet metadata if applicable
    rows = artifact.rows
    columns = artifact.columns
    key_columns = artifact.key_columns

    if full_path.exists() and full_path.suffix == ".parquet":
        try:
            parquet_meta = extract_parquet_metadata(full_path)
            if rows is None:
                rows = parquet_meta["rows"]
            if columns is None:
                columns = len(parquet_meta["columns"])
            if not key_columns:
                key_columns = parquet_meta["key_columns"]
        except Exception:
            # If parquet extraction fails, use whatever we have
            pass

    if rows is not None:
        entry["rows"] = rows
    if columns is not None:
        entry["columns"] = columns
    if key_columns:
        entry["key_columns"] = key_columns

    # Include provenance if available
    if artifact.provenance:
        entry["provenance"] = artifact.provenance

    return entry


def get_zillow_attribution() -> dict:
    """
    Return Zillow attribution dict for sources section.

    Returns:
        Dict with Zillow attribution information per licensing requirements.
    """
    return {
        "name": "Zillow Economic Research",
        "metric": "ZORI",
        "attribution": (
            "ZORI data provided by Zillow through the Zillow Research Data portal. "
            "ZORI is a repeat-rent index that measures typical observed market rate rent "
            "across a given region. "
            "Visit https://www.zillow.com/research/data/ for more information."
        ),
        "license_notes": "Public use with required attribution (see Zillow Terms of Use).",
    }


def build_manifest(
    bundle_root: Path,
    bundle_name: str,
    export_id: str,
    artifacts: list[ArtifactRecord],
    parameters: dict,
    notes: str = "",
) -> dict:
    """
    Build complete MANIFEST.json dict.

    Args:
        bundle_root: Root directory of bundle
        bundle_name: Logical name for the bundle
        export_id: Export identifier (e.g., "export-7")
        artifacts: List of ArtifactRecord objects
        parameters: Dict of export parameters (vintages, years, copy_mode, etc.)
        notes: Optional notes string

    Returns:
        Complete manifest dict ready for JSON serialization
    """
    # Build artifact entries
    artifact_entries = [build_artifact_entry(artifact, bundle_root) for artifact in artifacts]

    # Determine if ZORI data is present by checking artifact paths and key columns
    has_zori = False
    for artifact in artifacts:
        # Check if ZORI is in the path name
        if "zori" in artifact.dest_path.lower():
            has_zori = True
            break
        # Check if ZORI-related columns are present
        zori_cols = ("zori", "rent_to_income")
        if any(any(z in col.lower() for z in zori_cols) for col in artifact.key_columns):
            has_zori = True
            break

    # Build sources list
    sources: list[dict] = []
    if has_zori:
        sources.append(get_zillow_attribution())

    # Get current UTC timestamp in ISO format
    created_at = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

    manifest = {
        "bundle_name": bundle_name,
        "export_id": export_id,
        "created_at_utc": created_at,
        "coclab": get_coclab_info(),
        "parameters": parameters,
        "artifacts": artifact_entries,
        "sources": sources,
        "notes": notes,
    }

    return manifest


def write_manifest(manifest: dict, bundle_root: Path) -> Path:
    """
    Write MANIFEST.json to bundle root.

    Args:
        manifest: Complete manifest dict
        bundle_root: Root directory of bundle

    Returns:
        Path to written MANIFEST.json file

    Raises:
        OSError: If file cannot be written
    """
    manifest_path = bundle_root / "MANIFEST.json"

    with manifest_path.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
        f.write("\n")  # Add trailing newline

    return manifest_path
