"""CLI command for running panel diagnostics."""

import json
from pathlib import Path
from typing import Annotated

import typer


def panel_diagnostics(
    panel: Annotated[
        Path,
        typer.Option(
            "--panel",
            "-p",
            help="Path to the panel Parquet file to analyze.",
        ),
    ],
    output_dir: Annotated[
        Path | None,
        typer.Option(
            "--output-dir",
            "-o",
            help="Directory to save diagnostic output files.",
        ),
    ] = None,
    format_: Annotated[
        str,
        typer.Option(
            "--format",
            "-f",
            help="Output format: 'text' (summary only), 'csv' (export CSVs).",
        ),
    ] = "text",
    json_output: Annotated[
        bool,
        typer.Option(
            "--json",
            help="Output machine-readable JSON instead of human text.",
        ),
    ] = False,
) -> None:
    """Run diagnostics on a CoC panel.

    Analyzes a panel Parquet file and generates diagnostic reports including
    coverage statistics, boundary change detection, and missingness analysis.

    Examples:

        coclab diagnostics panel --panel data/curated/panel/coc_panel__2018_2024.parquet

        coclab diagnostics panel --panel panel.parquet --output-dir ./diagnostics/

        coclab diagnostics panel --panel panel.parquet --format csv

        coclab diagnostics panel --panel panel.parquet --format text
    """
    import pandas as pd

    from coclab.panel import generate_diagnostics_report

    # Validate format
    valid_formats = {"text", "csv"}
    if format_ not in valid_formats:
        typer.echo(
            f"Error: Invalid format '{format_}'. "
            f"Must be one of: {', '.join(sorted(valid_formats))}",
            err=True,
        )
        raise typer.Exit(1)

    # Check if panel file exists
    if not panel.exists():
        typer.echo(f"Error: Panel file not found: {panel}", err=True)
        raise typer.Exit(1)

    if not json_output:
        typer.echo(f"Loading panel from {panel}...")

    # Load the panel
    try:
        panel_df = pd.read_parquet(panel)
        if not json_output:
            typer.echo(f"Loaded {len(panel_df)} rows")
    except Exception as e:
        if json_output:
            typer.echo(json.dumps({"status": "error", "error": str(e)}, indent=2))
        else:
            typer.echo(f"Error loading panel: {e}", err=True)
        raise typer.Exit(1) from e

    # Run diagnostics
    if not json_output:
        typer.echo("Running diagnostics...")
    try:
        report = generate_diagnostics_report(panel_df)
    except Exception as e:
        if json_output:
            typer.echo(json.dumps({"status": "error", "error": str(e)}, indent=2))
        else:
            typer.echo(f"Error generating diagnostics: {e}", err=True)
        raise typer.Exit(1) from e

    if json_output:
        payload: dict = {
            "status": "ok",
            "panel_info": report.panel_info or {},
        }
        if report.coverage is not None:
            payload["coverage"] = report.coverage.to_dict(orient="records")
        if report.missingness is not None:
            payload["missingness"] = report.missingness.to_dict(orient="records")
        if report.boundary_changes is not None:
            payload["boundary_changes"] = report.boundary_changes.to_dict(orient="records")
        typer.echo(json.dumps(payload, indent=2, default=str))
        return

    # Output based on format
    if format_ == "text":
        # Print text summary
        typer.echo("")
        typer.echo(report.summary())

    elif format_ == "csv":
        # Export to CSV files
        if output_dir is None:
            output_dir = Path(".")

        typer.echo(f"Exporting diagnostics to {output_dir}...")
        try:
            paths = report.to_csv(output_dir)
            typer.echo("")
            typer.echo("Exported files:")
            for name, path in paths.items():
                typer.echo(f"  {name}: {path}")
        except Exception as e:
            typer.echo(f"Error exporting CSVs: {e}", err=True)
            raise typer.Exit(1) from e

        # Also print summary
        typer.echo("")
        typer.echo(report.summary())

    # Quick stats for both formats
    typer.echo("")
    typer.echo("Quick Stats:")
    if report.panel_info:
        for key, value in report.panel_info.items():
            typer.echo(f"  {key}: {value}")
