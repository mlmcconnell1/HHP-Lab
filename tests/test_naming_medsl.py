"""Tests for MEDSL naming helpers."""

from hhplab.naming import medsl_president_county_filename, medsl_president_county_path


def test_medsl_president_county_filename() -> None:
    assert (
        medsl_president_county_filename(2000, 2024, 2020)
        == "medsl_president_county__Y2000-2024@C2020.parquet"
    )


def test_medsl_president_county_path() -> None:
    assert str(medsl_president_county_path(2000, 2024, 2020)).endswith(
        "data/curated/medsl/medsl_president_county__Y2000-2024@C2020.parquet"
    )
