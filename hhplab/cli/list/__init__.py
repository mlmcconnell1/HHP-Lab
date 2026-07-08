"""List command registration."""

import typer

from hhplab.cli.list.acs_variables import list_acs_variables
from hhplab.cli.list.census import list_census
from hhplab.cli.list.covariates import list_covariates
from hhplab.cli.list.curated import list_curated
from hhplab.cli.list.measures import list_measures
from hhplab.cli.list.sources import list_sources
from hhplab.cli.list.xwalks import list_xwalks
from hhplab.cli.shared.boundaries import list_boundaries_cmd


def register_commands(app: typer.Typer) -> None:
    """Register list commands."""
    app.command("acs-variables")(list_acs_variables)
    app.command("boundaries")(list_boundaries_cmd)
    app.command("census")(list_census)
    app.command("curated")(list_curated)
    app.command("covariates")(list_covariates)
    app.command("measures")(list_measures)
    app.command("sources")(list_sources)
    app.command("xwalks")(list_xwalks)
