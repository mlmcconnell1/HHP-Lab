"""Tests for MSA definition naming helpers."""

from hhplab.naming import (
    coc_urban_area_detail_filename,
    coc_urban_area_detail_path,
    coc_urban_fraction_filename,
    coc_urban_fraction_path,
    geo_panel_filename,
    msa_boundaries_filename,
    msa_boundaries_path,
    msa_coc_coverage_filename,
    msa_coc_coverage_path,
    msa_coc_xwalk_filename,
    msa_coc_xwalk_path,
    msa_county_membership_filename,
    msa_county_membership_path,
    msa_definitions_filename,
    msa_definitions_path,
    msa_pit_filename,
    pl_block_population_filename,
    pl_block_population_path,
    urban_area_filename,
    urban_area_path,
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


def test_msa_coc_coverage_filename():
    assert (
        msa_coc_coverage_filename(
            2024,
            2025,
            "census_msa_2023",
            2023,
            100,
            ("population", "area"),
        )
        == (
            "msa_coc_coverage__Y2024@B2025xMcensus_msa_2023xC2023"
            "__top100__basis-area-population.parquet"
        )
    )


def test_msa_coc_coverage_path():
    assert str(
        msa_coc_coverage_path(
            2024,
            2025,
            "census_msa_2023",
            2023,
            100,
            ("area", "population"),
        )
    ).endswith(
        "data/curated/msa/"
        "msa_coc_coverage__Y2024@B2025xMcensus_msa_2023xC2023"
        "__top100__basis-area-population.parquet"
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


def test_urban_area_filename_uses_distinct_urban_token():
    assert urban_area_filename(2020) == "urban_areas__U2020.parquet"
    assert urban_area_filename("2010") == "urban_areas__U2010.parquet"


def test_pl_block_population_filename_uses_decennial_and_block_tokens():
    assert pl_block_population_filename(2020) == "pl_blocks__N2020xK2020.parquet"
    assert pl_block_population_filename("2010", "2010") == "pl_blocks__N2010xK2010.parquet"


def test_coc_urban_fraction_filename_avoids_boundary_token_ambiguity():
    assert (
        coc_urban_fraction_filename(
            boundary_vintage=2025,
            urban_area_vintage=2020,
            block_vintage=2020,
            decennial_vintage=2020,
        )
        == "coc_urban_fraction__N2020@B2025xU2020xK2020.parquet"
    )


def test_coc_urban_area_detail_filename():
    assert (
        coc_urban_area_detail_filename(
            boundary_vintage=2025,
            urban_area_vintage=2010,
            block_vintage=2010,
            decennial_vintage=2010,
        )
        == "coc_urban_area_detail__N2010@B2025xU2010xK2010.parquet"
    )


def test_urban_paths_are_discoverable_without_globs():
    assert str(urban_area_path(2020)).endswith(
        "data/curated/tiger/urban_areas__U2020.parquet"
    )
    assert str(pl_block_population_path(2020)).endswith(
        "data/curated/census/pl_blocks__N2020xK2020.parquet"
    )
    assert str(coc_urban_fraction_path(2025, 2020, 2020, 2020)).endswith(
        "data/curated/measures/coc_urban_fraction__N2020@B2025xU2020xK2020.parquet"
    )
    assert str(coc_urban_area_detail_path(2025, 2020, 2020, 2020)).endswith(
        "data/curated/measures/coc_urban_area_detail__N2020@B2025xU2020xK2020.parquet"
    )
