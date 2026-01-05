"""CLI command for ingesting PIT (Point-in-Time) count data from HUD Exchange."""

from pathlib import Path
from typing import Annotated

import typer

from coclab.pit.ingest import get_canonical_output_path


def ingest_pit(
    year: Annotated[
        int,
        typer.Option(
            "--year",
            "-y",
            help="PIT survey year to ingest (e.g., 2024).",
        ),
    ],
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Re-download and re-process even if files exist.",
        ),
    ] = False,
    parse_only: Annotated[
        bool,
        typer.Option(
            "--parse-only",
            help="Skip download if file exists, only parse and process.",
        ),
    ] = False,
) -> None:
    """Ingest PIT (Point-in-Time) count data from HUD Exchange.

    Downloads PIT data, parses it into canonical schema, saves as Parquet,
    registers in the PIT registry, and runs QA validation.

    Examples:

        coclab ingest-pit --year 2024

        coclab ingest-pit --year 2024 --force

        coclab ingest-pit --year 2024 --parse-only
    """
    from coclab.pit.ingest import (
        download_pit_data,
        get_pit_source_url,
        parse_pit_file,
        write_pit_parquet,
    )
    from coclab.pit.qa import validate_pit_data
    from coclab.pit.registry import register_pit_year

    typer.echo(f"Ingesting PIT data for year {year}...")

    # Step 1: Download PIT data
    raw_dir = Path("data/raw/pit") / str(year)
    # Get filename from URL (format changed from .xlsx to .xlsb in 2024)
    source_url = get_pit_source_url(year)
    expected_filename = source_url.split("/")[-1]
    raw_file = raw_dir / expected_filename

    if parse_only and raw_file.exists():
        typer.echo(f"Using existing file: {raw_file}")
    else:
        typer.echo("Downloading PIT data from HUD Exchange...")
        try:
            result = download_pit_data(year, force=force)
            raw_file = result.path
            source_url = result.source_url
            typer.echo(f"Downloaded: {raw_file} ({result.file_size:,} bytes)")
        except Exception as e:
            typer.echo(f"Error downloading PIT data: {e}", err=True)
            raise typer.Exit(1) from e

    # Step 2: Parse PIT file
    typer.echo("Parsing PIT file...")
    try:
        df = parse_pit_file(
            file_path=raw_file,
            year=year,
            source="hud_exchange",
            source_ref=source_url,
        )
        typer.echo(f"Parsed {len(df)} CoC records")
    except Exception as e:
        typer.echo(f"Error parsing PIT file: {e}", err=True)
        raise typer.Exit(1) from e

    # Step 3: Write canonical Parquet
    output_path = get_canonical_output_path(year)
    typer.echo(f"Writing canonical Parquet to {output_path}...")
    try:
        write_pit_parquet(df, output_path)
        typer.echo(f"Wrote: {output_path}")
    except Exception as e:
        typer.echo(f"Error writing Parquet: {e}", err=True)
        raise typer.Exit(1) from e

    # Step 4: Register in PIT registry
    typer.echo("Registering in PIT registry...")
    try:
        entry = register_pit_year(
            pit_year=year,
            source="hud_exchange",
            path=output_path,
            row_count=len(df),
        )
        typer.echo(f"Registered: year={entry.pit_year}, rows={entry.row_count}")
    except Exception as e:
        typer.echo(f"Error registering in registry: {e}", err=True)
        raise typer.Exit(1) from e

    # Step 5: Run QA validation
    typer.echo("Running QA validation...")
    try:
        qa_report = validate_pit_data(df)
        if qa_report.passed:
            typer.echo("QA passed: no errors found")
        else:
            typer.echo(f"QA result: {len(qa_report.errors)} error(s), {len(qa_report.warnings)} warning(s)")

        # Display issues if any
        if qa_report.issues:
            typer.echo("")
            typer.echo("QA Issues:")
            for issue in qa_report.issues[:10]:  # Limit to first 10
                typer.echo(f"  {issue}")
            if len(qa_report.issues) > 10:
                typer.echo(f"  ... and {len(qa_report.issues) - 10} more issues")
    except Exception as e:
        typer.echo(f"Warning: QA validation failed: {e}", err=True)
        # Don't fail the command for QA issues

    # Summary
    typer.echo("")
    typer.echo("PIT ingestion complete:")
    typer.echo(f"  Year: {year}")
    typer.echo(f"  CoCs: {len(df)}")
    typer.echo(f"  Output: {output_path}")
