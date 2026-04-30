"""Tests for metro-scoped naming functions added in coclab-djrh.1."""

import pytest

from hhplab.naming import (
    geo_map_filename,
    geo_panel_filename,
    metro_coc_membership_filename,
    metro_county_membership_filename,
    metro_definitions_filename,
    metro_measures_filename,
    metro_panel_filename,
    metro_pep_filename,
    metro_pit_filename,
    metro_zori_filename,
)


class TestMetroFilenames:
    def test_metro_measures_no_tract(self):
        assert (
            metro_measures_filename("2023", "glynn_fox_v1")
            == "measures__metro__A2023@Dglynnfoxv1.parquet"
        )

    def test_metro_measures_with_tract(self):
        assert (
            metro_measures_filename("2023", "glynn_fox_v1", tract_vintage=2020)
            == "measures__metro__A2023@Dglynnfoxv1xT2020.parquet"
        )

    def test_metro_measures_acs_range(self):
        assert (
            metro_measures_filename("2019-2023", "glynn_fox_v1")
            == "measures__metro__A2023@Dglynnfoxv1.parquet"
        )

    def test_metro_panel(self):
        assert (
            metro_panel_filename(2011, 2016, "glynn_fox_v1")
            == "panel__metro__Y2011-2016@Dglynnfoxv1.parquet"
        )

    def test_metro_pit(self):
        assert (
            metro_pit_filename(2015, "glynn_fox_v1")
            == "pit__metro__P2015@Dglynnfoxv1.parquet"
        )

    def test_metro_pep(self):
        assert (
            metro_pep_filename("glynn_fox_v1", 2020, "pop", 2011, 2016)
            == "pep__metro__Dglynnfoxv1xC2020__wpop__2011_2016.parquet"
        )

    def test_metro_zori(self):
        assert (
            metro_zori_filename("2023", "glynn_fox_v1", 2020, "renter_households")
            == "zori__metro__A2023@Dglynnfoxv1xC2020__wrenter.parquet"
        )


class TestMetroDefinitionFilenames:
    def test_definitions(self):
        assert (
            metro_definitions_filename("glynn_fox_v1")
            == "metro_definitions__glynn_fox_v1.parquet"
        )

    def test_coc_membership(self):
        assert (
            metro_coc_membership_filename("glynn_fox_v1")
            == "metro_coc_membership__glynn_fox_v1.parquet"
        )

    def test_county_membership(self):
        assert (
            metro_county_membership_filename("glynn_fox_v1")
            == "metro_county_membership__glynn_fox_v1.parquet"
        )


class TestGeoPanelFilename:
    def test_coc(self):
        assert (
            geo_panel_filename(2015, 2024, geo_type="coc", boundary_vintage="2025")
            == "panel__Y2015-2024@B2025.parquet"
        )

    def test_metro(self):
        assert (
            geo_panel_filename(
                2011, 2016, geo_type="metro", definition_version="glynn_fox_v1"
            )
            == "panel__metro__Y2011-2016@Dglynnfoxv1.parquet"
        )

    def test_coc_missing_boundary_raises(self):
        with pytest.raises(ValueError, match="boundary_vintage"):
            geo_panel_filename(2015, 2024, geo_type="coc")

    def test_metro_missing_definition_raises(self):
        with pytest.raises(ValueError, match="definition_version"):
            geo_panel_filename(2011, 2016, geo_type="metro")

    def test_unknown_geo_type_raises(self):
        with pytest.raises(ValueError, match="Unsupported geo_type"):
            geo_panel_filename(2015, 2024, geo_type="county")


class TestGeoMapFilename:
    def test_coc(self):
        assert (
            geo_map_filename(2015, 2024, geo_type="coc", boundary_vintage="2025")
            == "map__Y2015-2024@B2025.html"
        )

    def test_metro(self):
        assert (
            geo_map_filename(
                2011, 2016, geo_type="metro", definition_version="glynn_fox_v1"
            )
            == "map__metro__Y2011-2016@Dglynnfoxv1.html"
        )
