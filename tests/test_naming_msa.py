"""Tests for MSA definition naming helpers."""

from hhplab.naming import (
    geo_panel_filename,
    msa_boundaries_filename,
    msa_boundaries_path,
    msa_coc_xwalk_filename,
    msa_coc_xwalk_path,
    msa_county_membership_filename,
    msa_county_membership_path,
    msa_definitions_filename,
    msa_definitions_path,
    msa_pit_filename,
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


def test_msa_boundaries_filename():
    assert (
        msa_boundaries_filename("census_msa_2023")
        == "msa_boundaries__census_msa_2023.parquet"
    )


def test_msa_definitions_path():
    assert str(msa_definitions_path("census_msa_2023")).endswith(
        "data/curated/msa/msa_definitions__census_msa_2023.parquet"
    )


def test_msa_county_membership_path():
    assert str(msa_county_membership_path("census_msa_2023")).endswith(
        "data/curated/msa/msa_county_membership__census_msa_2023.parquet"
    )


def test_msa_boundaries_path():
    assert str(msa_boundaries_path("census_msa_2023")).endswith(
        "data/curated/msa/msa_boundaries__census_msa_2023.parquet"
    )


def test_msa_coc_xwalk_filename():
    assert (
        msa_coc_xwalk_filename("2025", "census_msa_2023", 2023)
        == "msa_coc_xwalk__B2025xMcensus_msa_2023xC2023.parquet"
    )


def test_msa_coc_xwalk_path():
    assert str(msa_coc_xwalk_path("2025", "census_msa_2023", 2023)).endswith(
        "data/curated/xwalks/msa_coc_xwalk__B2025xMcensus_msa_2023xC2023.parquet"
    )


def test_msa_pit_filename():
    assert (
        msa_pit_filename(2024, "census_msa_2023", 2024, 2024)
        == "pit__msa__P2024@Mcensusmsa2023xB2024xC2024.parquet"
    )


def test_msa_panel_filename():
    assert (
        geo_panel_filename(
            2020,
            2024,
            geo_type="msa",
            definition_version="census_msa_2023",
        )
        == "panel__msa__Y2020-2024@Mcensusmsa2023.parquet"
    )
