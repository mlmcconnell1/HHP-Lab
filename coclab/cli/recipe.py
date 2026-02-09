"""CLI command for recipe-driven builds."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from coclab.recipe.adapters import (
    ValidationDiagnostic,
    dataset_registry,
    geometry_registry,
    validate_recipe_adapters,
)
from coclab.recipe.loader import RecipeLoadError, load_recipe


def recipe_cmd(
    recipe: Annotated[
        Path,
        typer.Option(
            "--recipe",
            "-r",
            help="Path to a YAML recipe file.",
        ),
    ],
) -> None:
    """Load, validate, and execute a build recipe.

    Parses the recipe YAML, validates it against the versioned schema,
    then runs runtime adapter validation for geometry and dataset
    compatibility.

    Examples:

        coclab build recipe --recipe my_build.yaml
    """
    # 1. Load and structurally validate the recipe
    try:
        parsed = load_recipe(recipe)
    except RecipeLoadError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=2) from exc

    typer.echo(f"Loaded recipe: {parsed.name} (version {parsed.version})")

    # 2. Run adapter registry validation
    diagnostics = validate_recipe_adapters(
        parsed, geometry_registry, dataset_registry,
    )

    errors = [d for d in diagnostics if d.level == "error"]
    warnings = [d for d in diagnostics if d.level == "warning"]

    for w in warnings:
        typer.echo(f"  Warning: {w.message}", err=True)

    if errors:
        for e in errors:
            typer.echo(f"  Error: {e.message}", err=True)
        typer.echo(
            f"\nRecipe validation failed with {len(errors)} error(s).",
            err=True,
        )
        raise typer.Exit(code=1)

    if warnings:
        typer.echo(f"Recipe validated with {len(warnings)} warning(s).")
    else:
        typer.echo("Recipe validated successfully.")
