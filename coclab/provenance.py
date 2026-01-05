"""Dataset provenance tracking via Parquet metadata.

This module provides an extensible provenance system that embeds dataset
lineage information directly into Parquet file metadata. Provenance travels
with the data file, ensuring reproducibility without sidecar files.

Usage
-----
Writing provenance:

    from coclab.provenance import ProvenanceBlock, write_parquet_with_provenance

    provenance = ProvenanceBlock(
        boundary_vintage="2025",
        tract_vintage="2023",
        acs_vintage="2022",
        weighting="population",
    )
    write_parquet_with_provenance(df, path, provenance)

Reading provenance:

    from coclab.provenance import read_provenance

    provenance = read_provenance(path)
    print(provenance)  # ProvenanceBlock(boundary_vintage='2025', ...)
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Metadata key used in Parquet schema
PROVENANCE_KEY = b"coclab_provenance"


@dataclass
class ProvenanceBlock:
    """Extensible provenance metadata for CoC Lab datasets.

    Attributes
    ----------
    boundary_vintage : str, optional
        CoC boundary vintage (e.g., "2025").
    tract_vintage : str, optional
        Census tract vintage (e.g., "2023").
    acs_vintage : str, optional
        ACS 5-year estimate end year (e.g., "2022").
    weighting : str, optional
        Weighting method ("area" or "population").
    created_at : str
        ISO 8601 timestamp of dataset creation (auto-generated).
    coclab_version : str
        Version of CoC Lab that produced this dataset.
    extra : dict
        Additional extensible metadata fields.
    """

    boundary_vintage: str | None = None
    tract_vintage: str | None = None
    acs_vintage: str | None = None
    weighting: str | None = None
    created_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    coclab_version: str = "0.1.0"
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        d = asdict(self)
        # Remove None values for cleaner JSON
        return {k: v for k, v in d.items() if v is not None and v != {}}

    def to_json(self) -> str:
        """Serialize to JSON string."""
        return json.dumps(self.to_dict(), indent=2)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ProvenanceBlock:
        """Create from dictionary."""
        # Extract known fields
        known_fields = {
            "boundary_vintage",
            "tract_vintage",
            "acs_vintage",
            "weighting",
            "created_at",
            "coclab_version",
            "extra",
        }
        kwargs = {k: v for k, v in data.items() if k in known_fields}
        # Put unknown fields into extra
        unknown = {k: v for k, v in data.items() if k not in known_fields}
        if unknown:
            kwargs.setdefault("extra", {}).update(unknown)
        return cls(**kwargs)

    @classmethod
    def from_json(cls, json_str: str) -> ProvenanceBlock:
        """Deserialize from JSON string."""
        return cls.from_dict(json.loads(json_str))


def write_parquet_with_provenance(
    df: pd.DataFrame,
    path: Path | str,
    provenance: ProvenanceBlock,
    *,
    compression: str = "snappy",
) -> Path:
    """Write DataFrame to Parquet with embedded provenance metadata.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to write.
    path : Path | str
        Output file path.
    provenance : ProvenanceBlock
        Provenance metadata to embed.
    compression : str
        Parquet compression codec (default: snappy).

    Returns
    -------
    Path
        Path to written file.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    # Convert DataFrame to PyArrow Table
    table = pa.Table.from_pandas(df, preserve_index=False)

    # Add provenance to schema metadata
    existing_meta = table.schema.metadata or {}
    new_meta = {
        **existing_meta,
        PROVENANCE_KEY: provenance.to_json().encode("utf-8"),
    }
    table = table.replace_schema_metadata(new_meta)

    # Write with metadata
    pq.write_table(table, path, compression=compression)

    return path


def read_provenance(path: Path | str) -> ProvenanceBlock | None:
    """Read provenance metadata from a Parquet file.

    Parameters
    ----------
    path : Path | str
        Path to Parquet file.

    Returns
    -------
    ProvenanceBlock | None
        Provenance metadata if present, None otherwise.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    # Read schema metadata without loading data
    parquet_file = pq.ParquetFile(path)
    metadata = parquet_file.schema_arrow.metadata

    if metadata is None or PROVENANCE_KEY not in metadata:
        return None

    json_bytes = metadata[PROVENANCE_KEY]
    return ProvenanceBlock.from_json(json_bytes.decode("utf-8"))


def has_provenance(path: Path | str) -> bool:
    """Check if a Parquet file has provenance metadata.

    Parameters
    ----------
    path : Path | str
        Path to Parquet file.

    Returns
    -------
    bool
        True if provenance metadata exists.
    """
    try:
        return read_provenance(path) is not None
    except (FileNotFoundError, Exception):
        return False
