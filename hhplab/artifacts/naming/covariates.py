"""Canonical filenames for external covariate artifacts."""

from __future__ import annotations

import re


def covariate_curated_filename(
    source_id: str,
    first_year: str | int,
    last_year: str | int | None,
) -> str:
    """Generate filename for normalized expanded covariate source data."""
    source = _normalize_covariate_source_id(source_id)
    year_range = _covariate_year_range_token(first_year, last_year)
    return f"covariate__{source}__{year_range}.parquet"


def covariate_pair_filename(
    source_id: str,
    first_year: str | int,
    last_year: str | int | None,
) -> str:
    """Generate filename for normalized pair-level expanded covariate data."""
    source = _normalize_covariate_source_id(source_id)
    year_range = _covariate_year_range_token(first_year, last_year)
    return f"covariate_pairs__{source}__{year_range}.parquet"


def covariate_panel_filename(
    source_id: str,
    first_year: str | int,
    last_year: str | int | None,
) -> str:
    """Generate filename for panel-ready expanded covariate data."""
    source = _normalize_covariate_source_id(source_id)
    year_range = _covariate_year_range_token(first_year, last_year)
    return f"covariate_panel__{source}__{year_range}.parquet"


def _covariate_year_range_token(
    first_year: str | int,
    last_year: str | int | None,
) -> str:
    end = "ongoing" if last_year is None else str(last_year)
    return f"Y{first_year}-{end}"


def _normalize_covariate_source_id(source_id: str) -> str:
    source = source_id.lower().strip()
    if not re.fullmatch(r"[a-z0-9_]+", source):
        raise ValueError(
            "Covariate source ids in filenames may contain only lowercase letters, "
            "digits, and underscores."
        )
    return source
