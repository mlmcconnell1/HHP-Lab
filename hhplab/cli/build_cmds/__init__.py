"""Build command registration."""

import typer

from hhplab.cli.build_cmds.medsl import build_medsl_president_county
from hhplab.cli.build_cmds.prism_county import build_prism_county
from hhplab.cli.build_cmds.recipe import (
    recipe_cmd,
    recipe_export_cmd,
    recipe_plan_cmd,
    recipe_preflight_cmd,
    recipe_provenance_cmd,
)
from hhplab.cli.build_cmds.urban_fraction import build_urban_fraction


def register_commands(app: typer.Typer) -> None:
    """Register build commands."""
    app.command("recipe")(recipe_cmd)
    app.command("urban-fraction")(build_urban_fraction)
    app.command("prism-county")(build_prism_county)
    app.command("medsl-president-county")(build_medsl_president_county)
    app.command("recipe-plan")(recipe_plan_cmd)
    app.command("recipe-provenance")(recipe_provenance_cmd)
    app.command("recipe-export")(recipe_export_cmd)
    app.command("recipe-preflight")(recipe_preflight_cmd)
