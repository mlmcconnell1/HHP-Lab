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
from coclab.recipe.recipe_schema import RecipeV1, expand_year_spec


def _check_dataset_paths(parsed: RecipeV1, recipe_dir: Path) -> list[str]:
    """Check that all referenced dataset files exist on disk.

    Returns a list of error messages for missing files.
    """
    missing: list[str] = []

    for ds_id, ds in parsed.datasets.items():
        # Static path
        if ds.path is not None:
            resolved = recipe_dir / ds.path
            if not resolved.exists():
                missing.append(
                    f"Dataset '{ds_id}' path not found: {ds.path}"
                )

        # File set: check template-expanded paths and overrides
        if ds.file_set is not None:
            for seg in ds.file_set.segments:
                seg_years = expand_year_spec(seg.years)
                for year in seg_years:
                    if year in seg.overrides:
                        p = seg.overrides[year]
                    else:
                        p = ds.file_set.path_template.format(year=year)
                    resolved = recipe_dir / p
                    if not resolved.exists():
                        missing.append(
                            f"Dataset '{ds_id}' year {year} file not found: {p}"
                        )

    return missing


def recipe_cmd(
    recipe: Annotated[
        Path,
        typer.Option(
            "--recipe",
            "-r",
            help="Path to a YAML recipe file.",
        ),
    ],
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            "-n",
            help="Validate only; do not execute the build.",
        ),
    ] = False,
) -> None:
    """Load, validate, and execute a build recipe.

    Parses the recipe YAML, validates it against the versioned schema,
    then runs runtime adapter validation for geometry and dataset
    compatibility.

    Examples:

        coclab build recipe --recipe my_build.yaml

        coclab build recipe --recipe my_build.yaml --dry-run
    """
    # 1. Load and structurally validate the recipe
    try:
        parsed = load_recipe(recipe)
    except RecipeLoadError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=2) from exc

    typer.echo(f"Loaded recipe: {parsed.name} (version {parsed.version})")

    # 1b. Warn about missing transforms/pipelines
    if not parsed.transforms:
        typer.echo("  Warning: No transforms defined; no build output will be produced.", err=True)
    if not parsed.pipelines:
        typer.echo("  Warning: No pipelines defined; no build output will be produced.", err=True)

    # 1c. Pre-flight: check that referenced data files exist
    recipe_dir = Path(recipe).resolve().parent
    path_errors = _check_dataset_paths(parsed, recipe_dir)
    for msg in path_errors:
        typer.echo(f"  Missing file: {msg}", err=True)

    # 2. Run adapter registry validation
    diagnostics = validate_recipe_adapters(
        parsed, geometry_registry, dataset_registry,
    )

    errors = [d for d in diagnostics if d.level == "error"]
    warnings = [d for d in diagnostics if d.level == "warning"]

    for w in warnings:
        typer.echo(f"  Warning: {w.message}", err=True)

    all_errors = path_errors + [e.message for e in errors]
    if all_errors:
        for e in errors:
            typer.echo(f"  Error: {e.message}", err=True)
        typer.echo(
            f"\nRecipe validation failed with {len(all_errors)} error(s).",
            err=True,
        )
        raise typer.Exit(code=1)

    if warnings:
        typer.echo(f"Recipe validated with {len(warnings)} warning(s).")
    else:
        typer.echo("Recipe validated successfully.")

    if dry_run:
        return

    # TODO: execute the build pipeline here
