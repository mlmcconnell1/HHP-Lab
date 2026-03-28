"""CLI command for curated layout validation."""

import json
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
    json_output: Annotated[
        bool,
        typer.Option(
            "--json",
            help="Output structured JSON instead of human-readable text.",
        ),
    ] = False,
) -> None:
    """Validate curated data directory for naming and layout policy violations."""
    violations = validate_curated_layout(base_dir)

    if json_output:
        if not violations:
            typer.echo(json.dumps({"status": "ok", "violations": []}))
            return
        by_category: dict[str, list[str]] = {}
        for v in violations:
            by_category.setdefault(v.category, []).append(v.message)
        typer.echo(
            json.dumps(
                {
                    "status": "error",
                    "total_violations": len(violations),
                    "by_category": by_category,
                },
            )
        )
        raise typer.Exit(code=1)

    if not violations:
        typer.echo("Curated layout validation passed: no violations found.")
        return

    # Group by category
    by_category_display: dict[str, list] = {}
    for v in violations:
        by_category_display.setdefault(v.category, []).append(v)

    for cat, items in sorted(by_category_display.items()):
        label = cat.replace("_", " ").title()
        typer.echo(f"\n{label} ({len(items)}):")
        for v in items:
            typer.echo(f"  {v.message}")

    typer.echo(f"\nTotal violations: {len(violations)}")
    raise typer.Exit(code=1)
