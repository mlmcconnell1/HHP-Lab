"""CLI command for curated layout validation."""

from pathlib import Path
from typing import Annotated

import typer

from hhplab.cli.shared.output import JsonOutput, emit_result
from hhplab.curated_policy import validate_curated_layout
from hhplab.curated_schema import validate_curated_schemas
from hhplab.paths import curated_root


def validate_curated_layout_cmd(
    base_dir: Annotated[
        Path | None,
        typer.Option(
            "--dir",
            "-d",
            help="Path to the curated data directory.",
        ),
    ] = None,
    json_output: JsonOutput = False,
) -> None:
    """Validate curated data directory for naming and layout policy violations."""
    if base_dir is None:
        base_dir = curated_root()

    violations = validate_curated_layout(base_dir)
    schema_issues = validate_curated_schemas(base_dir)

    if json_output:
        if not violations and not schema_issues:
            emit_result(
                {"status": "ok", "violations": [], "schema_staleness": []},
                json_output,
            )
            return
        by_category: dict[str, list[str]] = {}
        for v in violations:
            by_category.setdefault(v.category, []).append(v.message)
        if schema_issues:
            by_category["stale_schema"] = [issue.message for issue in schema_issues]
        emit_result(
            {
                "status": "error",
                "total_violations": len(violations) + len(schema_issues),
                "by_category": by_category,
                "schema_staleness": [issue.to_dict() for issue in schema_issues],
            },
            json_output,
        )
        raise typer.Exit(code=1)

    if not violations and not schema_issues:
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

    if schema_issues:
        typer.echo(f"\nStale Schema ({len(schema_issues)}):")
        for issue in schema_issues:
            typer.echo(f"  {issue.message}")

    typer.echo(f"\nTotal violations: {len(violations) + len(schema_issues)}")
    raise typer.Exit(code=1)
