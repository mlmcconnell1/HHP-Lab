"""Migrate command registration."""

import typer

from hhplab.cli.migrate.curated import migrate_curated_cmd


def register_commands(app: typer.Typer) -> None:
    """Register migrate commands."""
    app.command("curated-layout")(migrate_curated_cmd)
