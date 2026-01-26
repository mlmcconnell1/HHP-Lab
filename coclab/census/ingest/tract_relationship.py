"""Census tract relationship file ingestion (2010↔2020).

Downloads and processes the Census Bureau tract-to-tract relationship file
that maps 2010 census tracts to 2020 census tracts.

Data Source:
    https://www2.census.gov/geo/docs/maps-data/data/rel2020/tract/tab20_tract20_tract10_natl.txt

Documentation:
    https://www2.census.gov/geo/pdfs/maps-data/data/rel2020/tract/explanation_tab20_tract20_tract10.pdf
"""

import hashlib
import io
import logging
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pandas as pd

from coclab.naming import tract_relationship_filename
from coclab.provenance import ProvenanceBlock, write_parquet_with_provenance
from coclab.source_registry import check_source_changed, register_source

logger = logging.getLogger(__name__)


class TractRelationshipNotFoundError(FileNotFoundError):
    """Raised when the tract relationship file is required but not found.

    This error provides a helpful message directing users to run the
    ingest-tract-relationship command.
    """

    def __init__(self, path: Path | None = None):
        self.path = path
        message = (
            "Tract relationship file (2010↔2020) not found.\n"
            "This file is required for translating ACS data between "
            "2010 and 2020 tract geographies.\n\n"
            "To download it, run:\n"
            "  coclab ingest-tract-relationship"
        )
        if path:
            message = (
                f"Tract relationship file not found: {path}\n\n"
                + message[message.find("This file") :]
            )
        super().__init__(message)


# Census Bureau relationship file URL
RELATIONSHIP_URL = (
    "https://www2.census.gov/geo/docs/maps-data/data/rel2020/tract/tab20_tract20_tract10_natl.txt"
)

# Output directory
OUTPUT_DIR = Path("data/curated/census")


def download_tract_relationship() -> tuple[pd.DataFrame, str, int]:
    """Download and parse the Census tract relationship file.

    Returns:
        Tuple of (DataFrame, sha256_hash, content_size) where DataFrame has:
        - tract_geoid_2010: 11-char GEOID
        - tract_geoid_2020: 11-char GEOID
        - area_2010_to_2020_weight: AREALAND_PART / AREALAND_TRACT_10
        - area_2020_to_2010_weight: AREALAND_PART / AREALAND_TRACT_20

    Raises:
        httpx.HTTPStatusError: If download fails.
        ValueError: If file format is unexpected.
    """
    logger.info(f"Downloading tract relationship file from {RELATIONSHIP_URL}")

    with httpx.Client(timeout=120.0) as client:
        response = client.get(RELATIONSHIP_URL, follow_redirects=True)
        response.raise_for_status()
        raw_content = response.content

    # Compute SHA-256 hash
    content_sha256 = hashlib.sha256(raw_content).hexdigest()
    content_size = len(raw_content)

    # Parse pipe-delimited file
    # The file has a header row with column names
    df = pd.read_csv(
        io.BytesIO(raw_content),
        sep="|",
        dtype=str,
        encoding="utf-8",
    )

    logger.info(f"Downloaded {len(df)} tract relationship records")

    # Validate expected columns exist
    required_cols = [
        "GEOID_TRACT_20",
        "GEOID_TRACT_10",
        "AREALAND_PART",
        "AREALAND_TRACT_10",
        "AREALAND_TRACT_20",
    ]
    missing = set(required_cols) - set(df.columns)
    if missing:
        raise ValueError(f"Missing expected columns: {missing}")

    # Convert area columns to numeric
    df["AREALAND_PART"] = pd.to_numeric(df["AREALAND_PART"], errors="coerce")
    df["AREALAND_TRACT_10"] = pd.to_numeric(df["AREALAND_TRACT_10"], errors="coerce")
    df["AREALAND_TRACT_20"] = pd.to_numeric(df["AREALAND_TRACT_20"], errors="coerce")

    # Compute bidirectional weights
    # area_2010_to_2020_weight: fraction of 2010 tract that maps to this 2020 tract
    # area_2020_to_2010_weight: fraction of 2020 tract that maps to this 2010 tract
    result = pd.DataFrame(
        {
            "tract_geoid_2010": df["GEOID_TRACT_10"],
            "tract_geoid_2020": df["GEOID_TRACT_20"],
            "area_2010_to_2020_weight": df["AREALAND_PART"] / df["AREALAND_TRACT_10"],
            "area_2020_to_2010_weight": df["AREALAND_PART"] / df["AREALAND_TRACT_20"],
        }
    )

    # Handle division by zero (tracts with zero land area)
    result["area_2010_to_2020_weight"] = result["area_2010_to_2020_weight"].fillna(0)
    result["area_2020_to_2010_weight"] = result["area_2020_to_2010_weight"].fillna(0)

    # Sort for consistent output
    result = result.sort_values(["tract_geoid_2010", "tract_geoid_2020"]).reset_index(drop=True)

    return result, content_sha256, content_size


def save_tract_relationship(df: pd.DataFrame) -> Path:
    """Save tract relationship DataFrame to parquet with provenance.

    Args:
        df: DataFrame with tract relationship data.

    Returns:
        Path to saved parquet file.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / tract_relationship_filename(2010, 2020)

    provenance = ProvenanceBlock(
        extra={
            "dataset_type": "tract_relationship",
            "from_vintage": "2010",
            "to_vintage": "2020",
            "source_url": RELATIONSHIP_URL,
            "row_count": len(df),
            "ingested_at": datetime.now(UTC).isoformat(),
        }
    )

    write_parquet_with_provenance(df, output_path, provenance)
    return output_path


def ingest_tract_relationship(force: bool = False) -> Path:
    """Download and save Census tract relationship file.

    Args:
        force: If True, re-download even if file exists.

    Returns:
        Path to saved parquet file.
    """
    output_path = OUTPUT_DIR / tract_relationship_filename(2010, 2020)

    # Check for existing file
    if output_path.exists() and not force:
        logger.info(f"Using cached file: {output_path}")
        return output_path

    # Download and parse
    df, content_sha256, content_size = download_tract_relationship()

    # Save to parquet
    output_path = save_tract_relationship(df)

    # Check for upstream changes
    changed, details = check_source_changed(
        source_type="tract_relationship",
        source_url=RELATIONSHIP_URL,
        current_sha256=content_sha256,
    )

    if changed:
        logger.warning(
            f"UPSTREAM DATA CHANGED: Tract relationship file has changed! "
            f"Previous hash: {details['previous_sha256'][:16]}... "
            f"Current hash: {content_sha256[:16]}... "
            f"Last ingested: {details['previous_ingested_at']}"
        )
    elif details.get("is_new"):
        logger.info("First time tracking tract relationship file in source registry")

    # Register in source registry
    register_source(
        source_type="tract_relationship",
        source_url=RELATIONSHIP_URL,
        source_name="Census Tract Relationship File 2010-2020",
        raw_sha256=content_sha256,
        file_size=content_size,
        local_path=str(output_path),
        metadata={
            "from_vintage": "2010",
            "to_vintage": "2020",
            "row_count": len(df),
        },
    )

    logger.info(f"Ingested tract relationship file to {output_path}")

    return output_path


def get_tract_relationship_path() -> Path:
    """Get the path to the tract relationship file.

    Returns:
        Path to the tract relationship parquet file.

    Raises:
        TractRelationshipNotFoundError: If the file does not exist.
    """
    path = OUTPUT_DIR / tract_relationship_filename(2010, 2020)
    if not path.exists():
        raise TractRelationshipNotFoundError(path)
    return path


def load_tract_relationship() -> pd.DataFrame:
    """Load the tract relationship file.

    Returns:
        DataFrame with columns:
        - tract_geoid_2010: 11-char GEOID
        - tract_geoid_2020: 11-char GEOID
        - area_2010_to_2020_weight: fraction of 2010 tract in this 2020 tract
        - area_2020_to_2010_weight: fraction of 2020 tract in this 2010 tract

    Raises:
        TractRelationshipNotFoundError: If the file does not exist.
    """
    path = get_tract_relationship_path()
    logger.info(f"Loading tract relationship file from {path}")
    return pd.read_parquet(path)


if __name__ == "__main__":
    output = ingest_tract_relationship()
    print(f"Saved tract relationship to {output}")
