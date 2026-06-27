"""Tests for source-owned package entrypoints."""

import hhplab.census.ingest as census_ingest
from hhplab import cdc, census, hud, medsl, nhgis, pit, vera
from hhplab.cdc import aggregate_county_overdose_to_msa, ingest_county_overdose
from hhplab.census.ingest import ingest_tiger_counties, ingest_tiger_tracts, load_tract_relationship
from hhplab.hud.exchange_gis import ingest_hud_exchange
from hhplab.hud.opendata_arcgis import ingest_hud_opendata
from hhplab.nhgis.ingest import ingest_nhgis_counties, ingest_nhgis_tracts
from hhplab.pit.ingest import download_pit_data, parse_pit_file
from hhplab.pit.qa import validate_pit_data


def test_package_root_lazy_exports() -> None:
    """The package root should lazily expose common source-owned subpackages."""
    import hhplab

    assert hhplab.cdc is cdc
    assert hhplab.census is census
    assert hhplab.hud is hud
    assert hhplab.medsl is medsl
    assert hhplab.nhgis is nhgis
    assert hhplab.pit is pit
    assert hhplab.vera is vera


def test_census_root_reexports_ingest_helpers() -> None:
    """Census root should expose its canonical ingest surface."""
    assert census.ingest_tiger_counties is ingest_tiger_counties
    assert census.ingest_tiger_tracts is ingest_tiger_tracts
    assert census.load_tract_relationship is load_tract_relationship


def test_cdc_root_reexports_overdose_helpers() -> None:
    """CDC root should expose county overdose ingest and aggregation helpers."""
    assert cdc.ingest_county_overdose is ingest_county_overdose
    assert cdc.aggregate_county_overdose_to_msa is aggregate_county_overdose_to_msa


def test_census_ingest_does_not_export_in_memory_block_download() -> None:
    """The package-level ingest API should not expose the OOM-prone block downloader."""
    assert "download_block_geometry" not in census_ingest.__all__
    assert not hasattr(census_ingest, "download_block_geometry")


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


def test_medsl_root_reexports_ingest_helpers() -> None:
    """MEDSL root should expose county presidential ingest helpers."""
    assert callable(medsl.build_county_political_leaning_measures)
    assert callable(medsl.ingest_county_presidential_returns)
    assert callable(medsl.materialize_county_political_leaning)
    assert callable(medsl.parse_county_presidential_returns)


def test_vera_root_reexports_ingest_helpers() -> None:
    """Vera root should expose county incarceration ingest helpers."""
    assert callable(vera.ingest_county_incarceration_trends)
    assert callable(vera.parse_county_incarceration_trends)
