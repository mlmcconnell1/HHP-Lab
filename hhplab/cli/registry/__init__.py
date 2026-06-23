"""Registry command registration."""

import typer

from hhplab.cli.registry.rebuild import registry_rebuild
from hhplab.cli.shared.boundaries import delete_boundaries


def register_commands(app: typer.Typer) -> None:
    """Register registry commands."""
    app.command("delete-entry")(delete_boundaries)
    app.command("rebuild")(registry_rebuild)
