"""CLI command for curated layout validation."""

from pathlib import Path
from typing import Annotated

import typer

from coclab.curated_policy import validate_curated_layout


def validate_curated_layout_cmd(
    base_dir: Annotated[
        Path,
        typer.Option(
            "--dir",
            "-d",
            help="Path to the curated data directory.",
        ),
    ] = Path("data/curated"),
) -> None:
    """Validate curated data directory for naming and layout policy violations."""
    violations = validate_curated_layout(base_dir)

    if not violations:
        typer.echo("Curated layout validation passed: no violations found.")
        return

    # Group by category
    by_category: dict[str, list] = {}
    for v in violations:
        by_category.setdefault(v.category, []).append(v)

    for cat, items in sorted(by_category.items()):
        label = cat.replace("_", " ").title()
        typer.echo(f"\n{label} ({len(items)}):")
        for v in items:
            typer.echo(f"  {v.message}")

    typer.echo(f"\nTotal violations: {len(violations)}")
    raise typer.Exit(code=1)
