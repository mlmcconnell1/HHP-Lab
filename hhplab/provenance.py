"""Dataset provenance tracking via Parquet metadata.

This module provides an extensible provenance system that embeds dataset
lineage information directly into Parquet file metadata. Provenance travels
with the data file, ensuring reproducibility without sidecar files.

Usage
-----
Writing provenance:

    from hhplab.provenance import ProvenanceBlock, write_parquet_with_provenance

    provenance = ProvenanceBlock(
        boundary_vintage="2025",
        tract_vintage="2023",
        acs_vintage="2022",
        weighting="population",
    )
    write_parquet_with_provenance(df, path, provenance)

Reading provenance:

    from hhplab.provenance import read_provenance

    provenance = read_provenance(path)
    print(provenance)  # ProvenanceBlock(boundary_vintage='2025', ...)
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from hhplab._version import __version__

logger = logging.getLogger(__name__)

# Metadata key used in Parquet schema
PROVENANCE_KEY = b"hhplab_provenance"


@dataclass
class ProvenanceBlock:
    """Extensible provenance metadata for HHP-Lab datasets.

    Attributes
    ----------
    boundary_vintage : str, optional
        CoC boundary vintage (e.g., "2025").
    tract_vintage : str, optional
        Census tract vintage (e.g., "2023").
    county_vintage : str, optional
        Census county vintage (e.g., "2023").
    acs_vintage : str, optional
        ACS 5-year estimate end year (e.g., "2022").
    notation : str, optional
        Compound temporal notation (e.g., "A2022@B2025×T2023").
        Can be auto-generated via generate_notation().
    weighting : str, optional
        Weighting method ("area" or "population").
    geo_type : str, optional
        Analysis geography type (e.g., "coc", "metro").
    definition_version : str, optional
        Synthetic geography definition version (e.g., "glynn_fox_v1").
        Used for metro and other non-polygonal geography families.
    created_at : str
        ISO 8601 timestamp of dataset creation (auto-generated).
    hhplab_version : str
        Version of HHP-Lab that produced this dataset.
    extra : dict
        Additional extensible metadata fields.
    """

    boundary_vintage: str | None = None
    tract_vintage: str | None = None
    county_vintage: str | None = None
    acs_vintage: str | None = None
    notation: str | None = None
    weighting: str | None = None
    geo_type: str | None = None
    definition_version: str | None = None
    created_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    hhplab_version: str = __version__
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        d = asdict(self)
        # Remove None values for cleaner JSON
        return {k: v for k, v in d.items() if v is not None and v != {}}

    def to_json(self) -> str:
        """Serialize to JSON string."""
        return json.dumps(self.to_dict(), indent=2)

    def generate_notation(self) -> str | None:
        """Generate compound temporal notation from vintage fields.

        Builds notation using the conventions from temporal-terminology.md:
        - Source data prefix: A{year} for ACS
        - Target boundaries: @B{year}
        - Intermediary geometry: ×T{year} or ×C{year}

        Returns
        -------
        str | None
            Notation string (e.g., "A2022@B2025×T2023"), or None if
            insufficient vintage information is available.
        """
        parts = []

        # Source data (ACS is the primary aggregated data type)
        if self.acs_vintage:
            parts.append(f"A{self.acs_vintage}")

        # Target boundaries
        if self.boundary_vintage:
            parts.append(f"@B{self.boundary_vintage}")

        # Intermediary geometry (tract takes precedence over county)
        if self.tract_vintage:
            parts.append(f"×T{self.tract_vintage}")
        elif self.county_vintage:
            parts.append(f"×C{self.county_vintage}")

        # Need at least source and target for meaningful notation
        if len(parts) < 2:
            return None

        return "".join(parts)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ProvenanceBlock:
        """Create from dictionary."""
        # Extract known fields
        known_fields = {
            "boundary_vintage",
            "tract_vintage",
            "county_vintage",
            "acs_vintage",
            "notation",
            "weighting",
            "geo_type",
            "definition_version",
            "created_at",
            "hhplab_version",
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
    try:
        return ProvenanceBlock.from_json(json_bytes.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError, KeyError, TypeError):
        logger.warning("Malformed provenance metadata in %s — returning None", path)
        return None


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
