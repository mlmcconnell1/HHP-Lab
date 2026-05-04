"""Tests for source-owned package entrypoints."""

from hhplab import census, hud, nhgis, pit
from hhplab.census.ingest import ingest_tiger_counties, ingest_tiger_tracts, load_tract_relationship
from hhplab.hud.exchange_gis import ingest_hud_exchange
from hhplab.hud.opendata_arcgis import ingest_hud_opendata
from hhplab.nhgis.nhgis_ingest import ingest_nhgis_counties, ingest_nhgis_tracts
from hhplab.pit.ingest import download_pit_data, parse_pit_file
from hhplab.pit.qa import validate_pit_data


def test_package_root_lazy_exports() -> None:
    """The package root should lazily expose common source-owned subpackages."""
    import hhplab

    assert hhplab.census is census
    assert hhplab.hud is hud
    assert hhplab.nhgis is nhgis
    assert hhplab.pit is pit


def test_census_root_reexports_ingest_helpers() -> None:
    """Census root should expose its canonical ingest surface."""
    assert census.ingest_tiger_counties is ingest_tiger_counties
    assert census.ingest_tiger_tracts is ingest_tiger_tracts
    assert census.load_tract_relationship is load_tract_relationship


def test_hud_root_reexports_boundary_helpers() -> None:
    """HUD root should own boundary ingest entrypoints."""
    assert hud.ingest_hud_exchange is ingest_hud_exchange
    assert hud.ingest_hud_opendata is ingest_hud_opendata


def test_nhgis_root_reexports_ingest_helpers() -> None:
    """NHGIS root should expose both tract and county ingest helpers."""
    assert nhgis.ingest_nhgis_counties is ingest_nhgis_counties
    assert nhgis.ingest_nhgis_tracts is ingest_nhgis_tracts


def test_pit_root_reexports_ingest_and_qa_helpers() -> None:
    """PIT root should expose both ingest and QA helpers."""
    assert pit.download_pit_data is download_pit_data
    assert pit.parse_pit_file is parse_pit_file
    assert pit.validate_pit_data is validate_pit_data
