"""Schema definitions for the boundary registry."""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass
class RegistryEntry:
    """A single entry in the boundary vintage registry.

    Attributes:
        boundary_vintage: Version identifier (e.g., '2025', 'HUDOpenData_2025-08-19')
        source: Origin of the data ('hud_exchange' | 'hud_opendata')
        ingested_at: UTC timestamp when data was ingested
        path: File path to the curated GeoParquet file
        feature_count: Number of features (CoC boundaries) in the dataset
        hash_of_file: SHA-256 hash of the file for integrity/change detection
    """

    boundary_vintage: str
    source: str
    ingested_at: datetime
    path: Path
    feature_count: int
    hash_of_file: str

    def to_dict(self) -> dict:
        """Convert entry to dictionary for serialization."""
        return {
            "boundary_vintage": self.boundary_vintage,
            "source": self.source,
            "ingested_at": self.ingested_at.isoformat(),
            "path": str(self.path),
            "feature_count": self.feature_count,
            "hash_of_file": self.hash_of_file,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "RegistryEntry":
        """Create entry from dictionary."""
        ingested_at = data["ingested_at"]
        # Handle both string (from JSON) and datetime/Timestamp (from pandas)
        if isinstance(ingested_at, str):
            ingested_at = datetime.fromisoformat(ingested_at)
        elif hasattr(ingested_at, "to_pydatetime"):
            # pandas Timestamp
            ingested_at = ingested_at.to_pydatetime()
        return cls(
            boundary_vintage=data["boundary_vintage"],
            source=data["source"],
            ingested_at=ingested_at,
            path=Path(data["path"]),
            feature_count=data["feature_count"],
            hash_of_file=data["hash_of_file"],
        )


# Registry column names for Parquet serialization
REGISTRY_COLUMNS = [
    "boundary_vintage",
    "source",
    "ingested_at",
    "path",
    "feature_count",
    "hash_of_file",
]
