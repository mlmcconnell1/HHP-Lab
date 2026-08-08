"""Tests for Vera naming helpers."""

from hhplab.artifacts.naming.naming import (
    vera_incarceration_county_filename,
    vera_incarceration_county_path,
)


def test_vera_incarceration_county_filename() -> None:
    assert (
        vera_incarceration_county_filename(1970, 2026, 2020)
        == "vera_incarceration_county__Y1970-2026@C2020.parquet"
    )


def test_vera_incarceration_county_path() -> None:
    assert str(vera_incarceration_county_path(1970, 2026, 2020)).endswith(
        "data/curated/vera/vera_incarceration_county__Y1970-2026@C2020.parquet"
    )
