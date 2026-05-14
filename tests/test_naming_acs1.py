"""Tests for ACS 1-year naming functions added in coclab-425s."""

from pathlib import Path

from hhplab.naming import (
    acs1_county_filename,
    acs1_county_path,
    acs1_metro_filename,
    acs1_metro_path,
    acs1_poverty_tracts_filename,
    acs1_poverty_tracts_path,
    metro_measures_acs1_filename,
    metro_measures_acs1_path,
)


class TestAcs1MetroFilename:
    def test_basic(self):
        assert acs1_metro_filename(2023, "glynn_fox_v1") == "acs1_metro__A2023@Dglynnfoxv1.parquet"

    def test_definition_version_sanitized(self):
        # Underscores and mixed case should be stripped/lowered
        assert acs1_metro_filename(2022, "Glynn_Fox_V2") == "acs1_metro__A2022@Dglynnfoxv2.parquet"

    def test_different_vintage(self):
        assert acs1_metro_filename(2019, "mydef") == "acs1_metro__A2019@Dmydef.parquet"


class TestAcs1MetroPath:
    def test_default_base(self):
        result = acs1_metro_path(2023, "glynn_fox_v1")
        assert result == Path("data/curated/acs/acs1_metro__A2023@Dglynnfoxv1.parquet")

    def test_custom_base(self):
        result = acs1_metro_path(2023, "glynn_fox_v1", base_dir="/tmp/project/data")
        assert result == Path("/tmp/project/data/curated/acs/acs1_metro__A2023@Dglynnfoxv1.parquet")

    def test_subdirectory_is_acs(self):
        result = acs1_metro_path(2023, "glynn_fox_v1")
        assert result.parent == Path("data/curated/acs")


class TestAcs1CountyFilename:
    def test_basic(self):
        assert acs1_county_filename(2023) == "acs1_county__A2023.parquet"


class TestAcs1CountyPath:
    def test_default_base(self):
        result = acs1_county_path(2023)
        assert result == Path("data/curated/acs/acs1_county__A2023.parquet")

    def test_custom_base(self):
        result = acs1_county_path(2023, base_dir="/tmp/project/data")
        assert result == Path("/tmp/project/data/curated/acs/acs1_county__A2023.parquet")


class TestAcs1PovertyTractsFilename:
    def test_basic(self):
        assert (
            acs1_poverty_tracts_filename(2023, 2020) == "acs1_poverty_tracts__A2023xT2020.parquet"
        )


class TestAcs1PovertyTractsPath:
    def test_default_base(self):
        result = acs1_poverty_tracts_path(2023, 2020)
        assert result == Path("data/curated/acs/acs1_poverty_tracts__A2023xT2020.parquet")

    def test_custom_base(self):
        result = acs1_poverty_tracts_path(2023, 2020, base_dir="/tmp/project/data")
        assert result == Path(
            "/tmp/project/data/curated/acs/acs1_poverty_tracts__A2023xT2020.parquet"
        )


class TestMetroMeasuresAcs1Filename:
    def test_basic(self):
        assert (
            metro_measures_acs1_filename(2023, "glynn_fox_v1")
            == "measures__metro__acs1__A2023@Dglynnfoxv1.parquet"
        )

    def test_acs1_segment_present(self):
        """The __acs1__ segment must be present to avoid collision with ACS5."""
        name = metro_measures_acs1_filename(2023, "glynn_fox_v1")
        assert "__acs1__" in name

    def test_definition_version_sanitized(self):
        assert (
            metro_measures_acs1_filename(2023, "Glynn_Fox_V1")
            == "measures__metro__acs1__A2023@Dglynnfoxv1.parquet"
        )


class TestMetroMeasuresAcs1Path:
    def test_default_base(self):
        result = metro_measures_acs1_path(2023, "glynn_fox_v1")
        assert result == Path(
            "data/curated/measures/measures__metro__acs1__A2023@Dglynnfoxv1.parquet"
        )

    def test_custom_base(self):
        result = metro_measures_acs1_path(2023, "glynn_fox_v1", base_dir="/tmp/data")
        assert result == Path(
            "/tmp/data/curated/measures/measures__metro__acs1__A2023@Dglynnfoxv1.parquet"
        )

    def test_subdirectory_is_measures(self):
        result = metro_measures_acs1_path(2023, "glynn_fox_v1")
        assert result.parent == Path("data/curated/measures")
