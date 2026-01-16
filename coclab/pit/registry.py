"""Registry for tracking and managing PIT survey data vintages."""

import hashlib
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd

# Default registry location
DEFAULT_PIT_REGISTRY_PATH = Path("data/curated/pit/pit_registry.parquet")
DEFAULT_PIT_VINTAGE_REGISTRY_PATH = Path("data/curated/pit/pit_vintage_registry.parquet")

# Registry column names for Parquet serialization
PIT_REGISTRY_COLUMNS = [
    "pit_year",
    "source",
    "ingested_at",
    "path",
    "row_count",
    "hash_of_file",
]

PIT_VINTAGE_REGISTRY_COLUMNS = [
    "vintage",
    "source",
    "ingested_at",
    "path",
    "row_count",
    "years_included",
    "hash_of_file",
]


@dataclass
class PitRegistryEntry:
    """A single entry in the PIT year registry.

    Attributes:
        pit_year: PIT survey year (e.g., 2023, 2024)
        source: Origin of the data (e.g., 'hud_exchange')
        ingested_at: UTC timestamp when data was ingested
        path: File path to the curated Parquet file
        row_count: Number of CoCs in the dataset
        hash_of_file: SHA-256 hash of the file for integrity/change detection
    """

    pit_year: int
    source: str
    ingested_at: datetime
    path: Path
    row_count: int
    hash_of_file: str

    def to_dict(self) -> dict:
        """Convert entry to dictionary for serialization."""
        return {
            "pit_year": self.pit_year,
            "source": self.source,
            "ingested_at": self.ingested_at.isoformat(),
            "path": str(self.path),
            "row_count": self.row_count,
            "hash_of_file": self.hash_of_file,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "PitRegistryEntry":
        """Create entry from dictionary."""
        ingested_at = data["ingested_at"]
        if isinstance(ingested_at, str):
            ingested_at = datetime.fromisoformat(ingested_at)
        elif hasattr(ingested_at, "to_pydatetime"):
            ingested_at = ingested_at.to_pydatetime()
        return cls(
            pit_year=int(data["pit_year"]),
            source=data["source"],
            ingested_at=ingested_at,
            path=Path(data["path"]),
            row_count=int(data["row_count"]),
            hash_of_file=data["hash_of_file"],
        )


def _get_registry_path(registry_path: Path | None = None) -> Path:
    """Get the registry path, using default if not specified."""
    return registry_path or DEFAULT_PIT_REGISTRY_PATH


def _load_registry(registry_path: Path) -> pd.DataFrame:
    """Load the registry from disk, or return empty DataFrame if not exists."""
    if registry_path.exists():
        df = pd.read_parquet(registry_path)
        df["ingested_at"] = pd.to_datetime(df["ingested_at"], utc=True)
        df["pit_year"] = df["pit_year"].astype(int)
        return df
    return pd.DataFrame(columns=PIT_REGISTRY_COLUMNS)


def _prepare_for_save(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare DataFrame for saving to Parquet with consistent types."""
    df = df.copy()
    df["ingested_at"] = pd.to_datetime(df["ingested_at"], utc=True)
    df["path"] = df["path"].astype(str)
    df["pit_year"] = df["pit_year"].astype(int)
    df["row_count"] = df["row_count"].astype(int)
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


def register_pit_year(
    pit_year: int,
    source: str,
    path: Path,
    row_count: int,
    ingested_at: datetime | None = None,
    hash_of_file: str | None = None,
    registry_path: Path | None = None,
) -> PitRegistryEntry:
    """Register a new PIT year in the registry.

    Idempotent: if a PIT year with the same pit_year and source already
    exists with the same hash, the existing entry is returned. If the hash
    differs, the entry is updated.

    Args:
        pit_year: PIT survey year (e.g., 2023, 2024)
        source: Origin of the data (e.g., 'hud_exchange')
        path: Path to the curated Parquet file
        row_count: Number of CoCs in the dataset
        ingested_at: UTC timestamp (defaults to now)
        hash_of_file: SHA-256 of file (computed if not provided)
        registry_path: Custom registry path (uses default if not specified)

    Returns:
        The registered PitRegistryEntry
    """
    reg_path = _get_registry_path(registry_path)
    df = _load_registry(reg_path)

    if hash_of_file is None:
        hash_of_file = compute_file_hash(path)

    if ingested_at is None:
        ingested_at = datetime.now().astimezone()

    entry = PitRegistryEntry(
        pit_year=pit_year,
        source=source,
        ingested_at=ingested_at,
        path=path,
        row_count=row_count,
        hash_of_file=hash_of_file,
    )

    mask = (df["pit_year"] == pit_year) & (df["source"] == source)

    if mask.any():
        existing = df.loc[mask].iloc[0]
        if existing["hash_of_file"] == hash_of_file:
            return PitRegistryEntry.from_dict(existing.to_dict())
        df.loc[mask, "ingested_at"] = ingested_at.isoformat()
        df.loc[mask, "path"] = str(path)
        df.loc[mask, "row_count"] = row_count
        df.loc[mask, "hash_of_file"] = hash_of_file
    else:
        new_row = pd.DataFrame([entry.to_dict()])
        df = pd.concat([df, new_row], ignore_index=True)

    _save_registry(df, reg_path)
    return entry


def list_pit_years(registry_path: Path | None = None) -> list[PitRegistryEntry]:
    """List all registered PIT years.

    Args:
        registry_path: Custom registry path (uses default if not specified)

    Returns:
        List of PitRegistryEntry objects, sorted by ingested_at descending
    """
    reg_path = _get_registry_path(registry_path)
    df = _load_registry(reg_path)

    if df.empty:
        return []

    df = df.sort_values("ingested_at", ascending=False)
    return [PitRegistryEntry.from_dict(row.to_dict()) for _, row in df.iterrows()]


def get_pit_path(pit_year: int, registry_path: Path | None = None) -> Path | None:
    """Get the file path for a specific PIT year.

    Args:
        pit_year: PIT survey year to look up
        registry_path: Custom registry path (uses default if not specified)

    Returns:
        Path to the curated Parquet file, or None if not found
    """
    reg_path = _get_registry_path(registry_path)
    df = _load_registry(reg_path)

    if df.empty:
        return None

    mask = df["pit_year"] == pit_year
    if not mask.any():
        return None

    matching = df[mask].sort_values("ingested_at", ascending=False)
    return Path(matching.iloc[0]["path"])


def latest_pit_year(
    source: str | None = None,
    registry_path: Path | None = None,
) -> int | None:
    """Get the latest PIT year.

    Selection policy: prefer highest pit_year number.

    Args:
        source: Optionally filter by source
        registry_path: Custom registry path (uses default if not specified)

    Returns:
        The pit_year of the latest entry, or None if empty
    """
    reg_path = _get_registry_path(registry_path)
    df = _load_registry(reg_path)

    if df.empty:
        return None

    if source:
        df = df[df["source"] == source]
        if df.empty:
            return None

    return int(df["pit_year"].max())


# =============================================================================
# Vintage Registry (for multi-year vintage files)
# =============================================================================


@dataclass
class PitVintageRegistryEntry:
    """A single entry in the PIT vintage registry.

    Attributes:
        vintage: Vintage/release year (e.g., 2024 for the 2024 HUD release)
        source: Origin of the data (e.g., 'hud_user')
        ingested_at: UTC timestamp when data was ingested
        path: File path to the curated Parquet file
        row_count: Total number of CoC-year records in the dataset
        years_included: List of PIT years included in this vintage
        hash_of_file: SHA-256 hash of the file for integrity/change detection
    """

    vintage: int
    source: str
    ingested_at: datetime
    path: Path
    row_count: int
    years_included: list[int]
    hash_of_file: str

    def to_dict(self) -> dict:
        """Convert entry to dictionary for serialization."""
        return {
            "vintage": self.vintage,
            "source": self.source,
            "ingested_at": self.ingested_at.isoformat(),
            "path": str(self.path),
            "row_count": self.row_count,
            "years_included": self.years_included,
            "hash_of_file": self.hash_of_file,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "PitVintageRegistryEntry":
        """Create entry from dictionary."""
        ingested_at = data["ingested_at"]
        if isinstance(ingested_at, str):
            ingested_at = datetime.fromisoformat(ingested_at)
        elif hasattr(ingested_at, "to_pydatetime"):
            ingested_at = ingested_at.to_pydatetime()
        years = data["years_included"]
        if isinstance(years, str):
            import json

            years = json.loads(years)
        return cls(
            vintage=int(data["vintage"]),
            source=data["source"],
            ingested_at=ingested_at,
            path=Path(data["path"]),
            row_count=int(data["row_count"]),
            years_included=list(years),
            hash_of_file=data["hash_of_file"],
        )


def _get_vintage_registry_path(registry_path: Path | None = None) -> Path:
    """Get the vintage registry path, using default if not specified."""
    return registry_path or DEFAULT_PIT_VINTAGE_REGISTRY_PATH


def _load_vintage_registry(registry_path: Path) -> pd.DataFrame:
    """Load the vintage registry from disk, or return empty DataFrame if not exists."""
    if registry_path.exists():
        df = pd.read_parquet(registry_path)
        df["ingested_at"] = pd.to_datetime(df["ingested_at"], utc=True)
        df["vintage"] = df["vintage"].astype(int)
        return df
    return pd.DataFrame(columns=PIT_VINTAGE_REGISTRY_COLUMNS)


def _prepare_vintage_for_save(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare vintage DataFrame for saving to Parquet with consistent types."""
    import json

    df = df.copy()
    df["ingested_at"] = pd.to_datetime(df["ingested_at"], utc=True)
    df["path"] = df["path"].astype(str)
    df["vintage"] = df["vintage"].astype(int)
    df["row_count"] = df["row_count"].astype(int)
    # Store years_included as JSON string for Parquet compatibility
    df["years_included"] = df["years_included"].apply(
        lambda x: json.dumps(x) if isinstance(x, list) else x
    )
    return df


def _save_vintage_registry(df: pd.DataFrame, registry_path: Path) -> None:
    """Save vintage registry to disk as Parquet."""
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    df = _prepare_vintage_for_save(df)
    df.to_parquet(registry_path, index=False)


def register_pit_vintage(
    vintage: int,
    source: str,
    path: Path,
    row_count: int,
    years_included: list[int],
    ingested_at: datetime | None = None,
    hash_of_file: str | None = None,
    registry_path: Path | None = None,
) -> PitVintageRegistryEntry:
    """Register a new PIT vintage in the registry.

    Idempotent: if a vintage with the same vintage and source already
    exists with the same hash, the existing entry is returned. If the hash
    differs, the entry is updated.

    Args:
        vintage: Vintage/release year (e.g., 2024)
        source: Origin of the data (e.g., 'hud_user')
        path: Path to the curated Parquet file
        row_count: Total number of CoC-year records
        years_included: List of PIT years in this vintage
        ingested_at: UTC timestamp (defaults to now)
        hash_of_file: SHA-256 of file (computed if not provided)
        registry_path: Custom registry path (uses default if not specified)

    Returns:
        The registered PitVintageRegistryEntry
    """
    reg_path = _get_vintage_registry_path(registry_path)
    df = _load_vintage_registry(reg_path)

    if hash_of_file is None:
        hash_of_file = compute_file_hash(path)

    if ingested_at is None:
        ingested_at = datetime.now().astimezone()

    entry = PitVintageRegistryEntry(
        vintage=vintage,
        source=source,
        ingested_at=ingested_at,
        path=path,
        row_count=row_count,
        years_included=sorted(years_included),
        hash_of_file=hash_of_file,
    )

    mask = (df["vintage"] == vintage) & (df["source"] == source)

    if mask.any():
        import json

        existing = df.loc[mask].iloc[0]
        if existing["hash_of_file"] == hash_of_file:
            return PitVintageRegistryEntry.from_dict(existing.to_dict())
        df.loc[mask, "ingested_at"] = ingested_at.isoformat()
        df.loc[mask, "path"] = str(path)
        df.loc[mask, "row_count"] = row_count
        df.loc[mask, "years_included"] = json.dumps(sorted(years_included))
        df.loc[mask, "hash_of_file"] = hash_of_file
    else:
        new_row = pd.DataFrame([entry.to_dict()])
        df = pd.concat([df, new_row], ignore_index=True)

    _save_vintage_registry(df, reg_path)
    return entry


def list_pit_vintages(registry_path: Path | None = None) -> list[PitVintageRegistryEntry]:
    """List all registered PIT vintages.

    Args:
        registry_path: Custom registry path (uses default if not specified)

    Returns:
        List of PitVintageRegistryEntry objects, sorted by vintage descending
    """
    reg_path = _get_vintage_registry_path(registry_path)
    df = _load_vintage_registry(reg_path)

    if df.empty:
        return []

    df = df.sort_values("vintage", ascending=False)
    return [PitVintageRegistryEntry.from_dict(row.to_dict()) for _, row in df.iterrows()]


def get_pit_vintage_path(vintage: int, registry_path: Path | None = None) -> Path | None:
    """Get the file path for a specific PIT vintage.

    Args:
        vintage: PIT vintage year to look up
        registry_path: Custom registry path (uses default if not specified)

    Returns:
        Path to the curated Parquet file, or None if not found
    """
    reg_path = _get_vintage_registry_path(registry_path)
    df = _load_vintage_registry(reg_path)

    if df.empty:
        return None

    mask = df["vintage"] == vintage
    if not mask.any():
        return None

    matching = df[mask].sort_values("ingested_at", ascending=False)
    return Path(matching.iloc[0]["path"])
