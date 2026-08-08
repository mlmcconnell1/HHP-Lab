"""CLI command for HIC/PIT coverage diagnostics."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer

from hhplab.recipe.year_spec import parse_year_spec
from hhplab.sources.hud.hic.coverage import validate_hic_pit_coverage


def hic_coverage_diagnostics(
    pit_dir: Annotated[
        Path,
        typer.Option(
            "--pit-dir",
            help="Directory containing canonical PIT parquet files.",
        ),
    ] = Path("data/curated/pit"),
    hic_dir: Annotated[
        Path,
        typer.Option(
            "--hic-dir",
            help="Directory containing canonical HIC parquet files.",
        ),
    ] = Path("data/curated/hic"),
    years: Annotated[
        str | None,
        typer.Option(
            "--years",
            help="Optional year list/range, e.g. '2020,2022-2024'.",
        ),
    ] = None,
    yoy_threshold: Annotated[
        float,
        typer.Option(
            "--yoy-threshold",
            help="Relative total-bed change threshold for HIC year-over-year warnings.",
        ),
    ] = 0.75,
    json_output: Annotated[
        bool,
        typer.Option(
            "--json",
            help="Output machine-readable JSON instead of human text.",
        ),
    ] = False,
) -> None:
    """Compare canonical HIC CoC-year coverage against PIT coverage."""
    try:
        requested_years = parse_year_spec(years) if years else None
        result = validate_hic_pit_coverage(
            pit_dir=pit_dir,
            hic_dir=hic_dir,
            years=requested_years,
            yoy_threshold=yoy_threshold,
        )
    except (FileNotFoundError, ValueError) as exc:
        if json_output:
            typer.echo(json.dumps({"status": "error", "error": str(exc)}, indent=2))
        else:
            typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1) from exc

    if json_output:
        payload = {"status": "ok", **result.to_dict()}
        typer.echo(json.dumps(payload, indent=2, default=str))
        if not result.report.passed:
            raise typer.Exit(1)
        return

    typer.echo("HIC/PIT Coverage Diagnostics")
    typer.echo("=" * 50)
    typer.echo(f"PIT files: {len(result.pit_files)}")
    typer.echo(f"HIC files: {len(result.hic_files)}")
    typer.echo("")
    if result.coverage.empty:
        typer.echo("No overlapping PIT/HIC years found.")
    else:
        typer.echo(result.coverage.to_string(index=False))
    typer.echo("")
    typer.echo(str(result.report))
    if not result.report.passed:
        raise typer.Exit(1)
