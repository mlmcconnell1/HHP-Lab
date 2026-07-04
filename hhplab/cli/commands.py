"""Typer command registration for the hhplab CLI."""

import typer

from hhplab.cli import build_cmds as build_commands
from hhplab.cli import diagnostics as diagnostics_commands
from hhplab.cli import generate as generate_commands
from hhplab.cli import ingest as ingest_commands
from hhplab.cli import list as list_commands
from hhplab.cli import migrate as migrate_commands
from hhplab.cli import registry as registry_commands
from hhplab.cli import show as show_commands
from hhplab.cli import validate as validate_commands
from hhplab.cli.agents import agents
from hhplab.cli.aggregate_cli import aggregate_app
from hhplab.cli.analyze import analyze_app
from hhplab.cli.build_cmds.recipe import recipe_init_cmd
from hhplab.cli.status import status_cmd


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
    recipe_app: typer.Typer,
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
    app.add_typer(recipe_app, name="recipe")
    app.add_typer(aggregate_app, name="aggregate")
    app.add_typer(show_app, name="show")
    app.add_typer(registry_app, name="registry")
    app.add_typer(analyze_app, name="analyze")
    app.add_typer(migrate_app, name="migrate")

    ingest_commands.register_commands(ingest_app)
    list_commands.register_commands(list_app)
    validate_commands.register_commands(validate_app)
    diagnostics_commands.register_commands(diagnostics_app)
    generate_commands.register_commands(generate_app)
    build_commands.register_commands(build_app)
    recipe_app.command("init")(recipe_init_cmd)
    show_commands.register_commands(show_app)
    registry_commands.register_commands(registry_app)
    migrate_commands.register_commands(migrate_app)
