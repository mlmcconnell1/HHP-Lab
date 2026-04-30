"""Tests for MSA definition naming helpers."""

from hhplab.naming import (
    msa_county_membership_filename,
    msa_county_membership_path,
    msa_definitions_filename,
    msa_definitions_path,
)


def test_msa_definitions_filename():
    assert (
        msa_definitions_filename("census_msa_2023")
        == "msa_definitions__census_msa_2023.parquet"
    )


def test_msa_county_membership_filename():
    assert (
        msa_county_membership_filename("census_msa_2023")
        == "msa_county_membership__census_msa_2023.parquet"
    )


def test_msa_definitions_path():
    assert str(msa_definitions_path("census_msa_2023")).endswith(
        "data/curated/msa/msa_definitions__census_msa_2023.parquet"
    )


def test_msa_county_membership_path():
    assert str(msa_county_membership_path("census_msa_2023")).endswith(
        "data/curated/msa/msa_county_membership__census_msa_2023.parquet"
    )

