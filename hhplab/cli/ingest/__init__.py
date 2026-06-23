"""Ingest command registration."""

import typer

from hhplab.cli.ingest.acs1_county import ingest_acs1_county
from hhplab.cli.ingest.acs1_metro import ingest_acs1_metro
from hhplab.cli.ingest.acs_population import ingest_acs_population
from hhplab.cli.ingest.census import (
    ingest_block_geometry_cmd,
    ingest_tiger,
    ingest_urban_areas_cmd,
)
from hhplab.cli.ingest.cpi import ingest_cpi_u
from hhplab.cli.ingest.decennial_tract_population import ingest_decennial_tract_population
from hhplab.cli.ingest.hic import ingest_hic
from hhplab.cli.ingest.laus_metro import ingest_laus_metro
from hhplab.cli.ingest.medsl import ingest_medsl_presidential
from hhplab.cli.ingest.msa_boundaries import ingest_msa_boundaries
from hhplab.cli.ingest.nhgis import ingest_nhgis
from hhplab.cli.ingest.pep import ingest_pep
from hhplab.cli.ingest.pit import ingest_pit
from hhplab.cli.ingest.pit_vintage import ingest_pit_vintage
from hhplab.cli.ingest.pl_block_population import ingest_pl_block_population
from hhplab.cli.ingest.prism import ingest_prism
from hhplab.cli.ingest.tract_relationship import ingest_tract_relationship
from hhplab.cli.ingest.zori import ingest_zori
from hhplab.cli.shared.boundaries import ingest_boundaries


def register_commands(app: typer.Typer) -> None:
    """Register ingest commands."""
    app.command("acs1-metro")(ingest_acs1_metro)
    app.command("acs1-county")(ingest_acs1_county)
    app.command("laus-metro")(ingest_laus_metro)
    app.command("cpi-u")(ingest_cpi_u)
    app.command("acs5-tract")(ingest_acs_population)
    app.command("decennial-tracts")(ingest_decennial_tract_population)
    app.command("pl-blocks")(ingest_pl_block_population)
    app.command("block-geometry")(ingest_block_geometry_cmd)
    app.command("urban-areas")(ingest_urban_areas_cmd)
    app.command("boundaries")(ingest_boundaries)
    app.command("msa-boundaries")(ingest_msa_boundaries)
    app.command("tiger")(ingest_tiger)
    app.command("nhgis")(ingest_nhgis)
    app.command("pit")(ingest_pit)
    app.command("pit-vintage")(ingest_pit_vintage)
    app.command("hic")(ingest_hic)
    app.command("prism")(ingest_prism)
    app.command("tract-relationship")(ingest_tract_relationship)
    app.command("zori")(ingest_zori)
    app.command("pep")(ingest_pep)
    app.command("medsl-presidential")(ingest_medsl_presidential)
