"""Show command registration."""

import typer

from hhplab.cli.shared.boundaries import show
from hhplab.cli.show.covariate_findings import show_covariate_finding
from hhplab.cli.show.measures import show_measures
from hhplab.cli.show.sources import source_status
from hhplab.cli.show.vintage_diffs import compare_vintages


def register_commands(app: typer.Typer) -> None:
    """Register show commands."""
    app.command("vintage-diffs")(compare_vintages)
    app.command("covariate-finding")(show_covariate_finding)
    app.command("map")(show)
    app.command("measures")(show_measures)
    app.command("sources")(source_status)
