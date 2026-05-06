"""Typer command registration for the hhplab CLI."""

import typer

from hhplab.cli.agents import agents
from hhplab.cli.aggregate_cli import aggregate_app
from hhplab.cli.boundaries import (
    delete_boundaries,
    ingest_boundaries,
    list_boundaries_cmd,
    show,
    validate_boundaries,
)
from hhplab.cli.build_xwalks import build_xwalks
from hhplab.cli.compare_vintages import compare_vintages
from hhplab.cli.crosscheck_pit_vintages import validate_pit_vintages
from hhplab.cli.crosscheck_population import validate_population
from hhplab.cli.diagnostics_cli import diagnostics
from hhplab.cli.generate_metro import generate_metro, generate_metro_universe
from hhplab.cli.generate_metro_boundaries import generate_metro_boundaries
from hhplab.cli.generate_msa import generate_msa
from hhplab.cli.generate_msa_xwalk import generate_msa_xwalk
from hhplab.cli.ingest_acs1_county import ingest_acs1_county
from hhplab.cli.ingest_acs1_metro import ingest_acs1_metro
from hhplab.cli.ingest_acs_population import ingest_acs_population
from hhplab.cli.ingest_census import ingest_tiger
from hhplab.cli.ingest_decennial_tract_population import (
    ingest_decennial_tract_population,
)
from hhplab.cli.ingest_laus_metro import ingest_laus_metro
from hhplab.cli.ingest_msa_boundaries import ingest_msa_boundaries
from hhplab.cli.ingest_nhgis import ingest_nhgis
from hhplab.cli.ingest_pit import ingest_pit
from hhplab.cli.ingest_pit_vintage import ingest_pit_vintage
from hhplab.cli.ingest_tract_relationship import ingest_tract_relationship
from hhplab.cli.list_census import list_census
from hhplab.cli.list_curated import list_curated
from hhplab.cli.list_measures import list_measures
from hhplab.cli.list_xwalks import list_xwalks
from hhplab.cli.migrate_curated import migrate_curated_cmd
from hhplab.cli.panel_diagnostics_cli import panel_diagnostics
from hhplab.cli.pep_cli import ingest_pep
from hhplab.cli.recipe import (
    recipe_cmd,
    recipe_export_cmd,
    recipe_plan_cmd,
    recipe_preflight_cmd,
    recipe_provenance_cmd,
)
from hhplab.cli.registry_rebuild import registry_rebuild
from hhplab.cli.show_measures import show_measures
from hhplab.cli.source_status import source_status
from hhplab.cli.status import status_cmd
from hhplab.cli.validate_curated import validate_curated_layout_cmd
from hhplab.cli.validate_metro import validate_metro, validate_metro_universe
from hhplab.cli.validate_msa import validate_msa
from hhplab.cli.zori_cli import ingest_zori, zori_diagnostics


def register_commands(
    *,
    app: typer.Typer,
    ingest_app: typer.Typer,
    list_app: typer.Typer,
    validate_app: typer.Typer,
    diagnostics_app: typer.Typer,
    migrate_app: typer.Typer,
    generate_app: typer.Typer,
    build_app: typer.Typer,
    show_app: typer.Typer,
    registry_app: typer.Typer,
) -> None:
    """Register command groups and command functions on the root Typer app."""
    app.command(
        "agents",
        help="Information for agents who are using the hhplab package.",
    )(agents)
    app.command("status")(status_cmd)
    app.add_typer(ingest_app, name="ingest")
    app.add_typer(list_app, name="list")
    app.add_typer(validate_app, name="validate")
    app.add_typer(diagnostics_app, name="diagnostics")
    app.add_typer(generate_app, name="generate")
    app.add_typer(build_app, name="build")
    app.add_typer(aggregate_app, name="aggregate")
    app.add_typer(show_app, name="show")
    app.add_typer(registry_app, name="registry")
    app.add_typer(migrate_app, name="migrate")

    ingest_app.command("acs1-metro")(ingest_acs1_metro)
    ingest_app.command("acs1-county")(ingest_acs1_county)
    ingest_app.command("laus-metro")(ingest_laus_metro)
    ingest_app.command("acs5-tract")(ingest_acs_population)
    ingest_app.command("decennial-tracts")(ingest_decennial_tract_population)
    ingest_app.command("boundaries")(ingest_boundaries)
    ingest_app.command("msa-boundaries")(ingest_msa_boundaries)
    ingest_app.command("tiger")(ingest_tiger)
    ingest_app.command("nhgis")(ingest_nhgis)
    ingest_app.command("pit")(ingest_pit)
    ingest_app.command("pit-vintage")(ingest_pit_vintage)
    ingest_app.command("tract-relationship")(ingest_tract_relationship)
    ingest_app.command("zori")(ingest_zori)
    ingest_app.command("pep")(ingest_pep)
    list_app.command("boundaries")(list_boundaries_cmd)
    list_app.command("census")(list_census)
    list_app.command("curated")(list_curated)
    list_app.command("measures")(list_measures)
    list_app.command("xwalks")(list_xwalks)
    validate_app.command("boundaries")(validate_boundaries)
    validate_app.command("metro")(validate_metro)
    validate_app.command("metro-universe")(validate_metro_universe)
    validate_app.command("msa")(validate_msa)
    validate_app.command("pit-vintages")(validate_pit_vintages)
    validate_app.command("population")(validate_population)
    validate_app.command("curated-layout")(validate_curated_layout_cmd)
    diagnostics_app.command("panel")(panel_diagnostics)
    diagnostics_app.command("xwalk")(diagnostics)
    diagnostics_app.command("zori")(zori_diagnostics)
    generate_app.command("xwalks")(build_xwalks)
    generate_app.command("metro")(generate_metro)
    generate_app.command("metro-universe")(generate_metro_universe)
    generate_app.command("metro-boundaries")(generate_metro_boundaries)
    generate_app.command("msa")(generate_msa)
    generate_app.command("msa-xwalk")(generate_msa_xwalk)
    build_app.command("recipe")(recipe_cmd)
    build_app.command("recipe-plan")(recipe_plan_cmd)
    build_app.command("recipe-provenance")(recipe_provenance_cmd)
    build_app.command("recipe-export")(recipe_export_cmd)
    build_app.command("recipe-preflight")(recipe_preflight_cmd)
    show_app.command("vintage-diffs")(compare_vintages)
    show_app.command("map")(show)
    show_app.command("measures")(show_measures)
    show_app.command("sources")(source_status)
    registry_app.command("delete-entry")(delete_boundaries)
    registry_app.command("rebuild")(registry_rebuild)
    migrate_app.command("curated-layout")(migrate_curated_cmd)
