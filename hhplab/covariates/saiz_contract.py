"""Saiz (2010) MSA supply elasticity covariate contract."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Final, Iterable

SAIZ_SOURCE_ID: Final = "saiz_supply_elasticity"
SAIZ_PROVIDER: Final = "saiz"
SAIZ_PRODUCT: Final = "supply_elasticity_2010"
SAIZ_SOURCE_PAGE: Final = "https://real-faculty.wharton.upenn.edu/saiz/"
SAIZ_SOURCE_URL: Final = "https://real-faculty.wharton.upenn.edu/saiz/"
SAIZ_ESTIMATE_YEAR: Final = 2010
SAIZ_MEASURE_COLUMNS: Final = (
    "saiz_elasticity",
    "saiz_inverse_elasticity",
    "saiz_undevelopable_share",
    "saiz_wrluri",
)
SAIZ_REQUIRED_CURATED_COLUMNS: Final = ("msa_id", "year")
SAIZ_REQUIRED_RAW_COLUMNS: Final = (
    "msanecma",
    "population",
    "msaname",
    "WRLURI",
    "unaval",
    "elasticity",
)
SAIZ_MATCH_DIAGNOSTIC_COLUMNS: Final = (
    "msa_id",
    "msa_name",
    "saiz_msanecma",
    "saiz_name",
    "saiz_match_rule",
)


@dataclass(frozen=True)
class SaizSourceContract:
    """Declarative layout and schema contract for the staged Saiz source file."""

    source_id: str = SAIZ_SOURCE_ID
    source_year: int = SAIZ_ESTIMATE_YEAR
    required_raw_columns: tuple[str, ...] = SAIZ_REQUIRED_RAW_COLUMNS
    required_curated_columns: tuple[str, ...] = SAIZ_REQUIRED_CURATED_COLUMNS
    measure_columns: tuple[str, ...] = SAIZ_MEASURE_COLUMNS
    diagnostic_columns: tuple[str, ...] = SAIZ_MATCH_DIAGNOSTIC_COLUMNS


SAIZ_SOURCE_CONTRACT: Final = SaizSourceContract()


def validate_saiz_source_contract(
    path: Path | str,
    *,
    raw_columns: Iterable[str] | None = None,
) -> SaizSourceContract:
    """Validate the staged Saiz Stata file needed by the ingest implementation."""
    source_path = Path(path)
    if not source_path.exists():
        raise FileNotFoundError(
            f"Saiz source file not found: {source_path}. Stage the Saiz (2010) "
            "supply elasticity .dta under data/raw/saiz_elasticity before ingest."
        )
    if source_path.suffix.lower() != ".dta":
        raise ValueError(
            f"Unsupported Saiz source type '{source_path.suffix}'. Expected the "
            "staged Saiz (2010) supply elasticity .dta file."
        )

    if raw_columns is None:
        try:
            import pandas as pd
        except ImportError as exc:  # pragma: no cover - dependency is present in project env
            raise RuntimeError(
                "pandas is required to inspect the Saiz source file; install the "
                "project dependencies with `uv sync --extra dev`."
            ) from exc

        raw_columns = pd.read_stata(source_path).columns
    available_columns = tuple(raw_columns)
    missing = [
        column for column in SAIZ_REQUIRED_RAW_COLUMNS if column not in available_columns
    ]
    if missing:
        raise ValueError(
            f"Saiz raw data is missing required columns {missing}. Update the "
            "Saiz source contract or stage the expected Saiz (2010) .dta file."
        )
    return SAIZ_SOURCE_CONTRACT
