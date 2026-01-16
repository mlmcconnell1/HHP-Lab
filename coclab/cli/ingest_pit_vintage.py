"""CLI command for ingesting all years from a PIT vintage file."""

import logging
from pathlib import Path
from typing import Annotated

import typer

from coclab.pit.ingest import get_vintage_output_path

# Configure logging to show INFO messages from PIT parser
logging.basicConfig(
    format="%(message)s",
    level=logging.WARNING,
)
# Show INFO for PIT ingest to see CoC ID mapping messages
logging.getLogger("coclab.pit.ingest.parser").setLevel(logging.INFO)


def ingest_pit_vintage(
    vintage: Annotated[
        int,
        typer.Option(
            "--vintage",
            "-v",
            help="PIT vintage year to ingest (e.g., 2024). This is the release year.",
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
    """Ingest all years from a PIT vintage file.

    Downloads PIT data for the specified vintage year and parses ALL year
    tabs from the Excel file (not just the vintage year). This captures
    the complete historical record as published in each vintage release.

    The resulting file contains all years from 2007 (or earliest available)
    through the vintage year, allowing comparison of how historical data
    may have been revised between releases.

    Examples:

        coclab ingest-pit-vintage --vintage 2024

        coclab ingest-pit-vintage --vintage 2024 --force

        coclab ingest-pit-vintage --vintage 2024 --parse-only
    """
    from coclab.pit.ingest import (
        download_pit_data,
        get_pit_source_url,
        parse_pit_vintage,
        write_pit_parquet,
    )
    from coclab.pit.registry import register_pit_vintage

    typer.echo(f"Ingesting PIT vintage {vintage} (all years)...")

    # Step 1: Download PIT data
    raw_dir = Path("data/raw/pit") / str(vintage)
    try:
        source_url = get_pit_source_url(vintage)
    except ValueError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1) from e
    expected_filename = source_url.split("/")[-1]
    raw_file = raw_dir / expected_filename

    if parse_only and raw_file.exists():
        typer.echo(f"Using existing file: {raw_file}")
    else:
        typer.echo("Downloading PIT data from HUD User...")
        try:
            result = download_pit_data(vintage, force=force)
            raw_file = result.path
            source_url = result.source_url
            typer.echo(f"Downloaded: {raw_file} ({result.file_size:,} bytes)")
        except Exception as e:
            typer.echo(f"Error downloading PIT data: {e}", err=True)
            raise typer.Exit(1) from e

    # Step 2: Parse all year tabs from vintage file
    typer.echo("Parsing all year tabs from vintage file...")
    try:
        parse_result = parse_pit_vintage(
            file_path=raw_file,
            vintage=vintage,
            source="hud_user",
            source_ref=source_url,
        )
        df = parse_result.df
        typer.echo(f"Parsed {len(df)} total records across {len(parse_result.years_parsed)} years")
        typer.echo(f"  Years: {parse_result.years_parsed[0]}-{parse_result.years_parsed[-1]}")
    except Exception as e:
        typer.echo(f"Error parsing PIT vintage file: {e}", err=True)
        raise typer.Exit(1) from e

    # Step 3: Write canonical Parquet with provenance
    output_path = get_vintage_output_path(vintage)
    typer.echo(f"Writing vintage Parquet to {output_path}...")
    try:
        write_pit_parquet(
            df,
            output_path,
            cross_state_mappings=parse_result.cross_state_mappings,
            rows_read=parse_result.total_rows_read,
            rows_skipped=parse_result.total_rows_skipped,
        )
        typer.echo(f"Wrote: {output_path}")
    except Exception as e:
        typer.echo(f"Error writing Parquet: {e}", err=True)
        raise typer.Exit(1) from e

    # Step 4: Register in PIT vintage registry
    typer.echo("Registering in PIT vintage registry...")
    try:
        entry = register_pit_vintage(
            vintage=vintage,
            source="hud_user",
            path=output_path,
            row_count=len(df),
            years_included=parse_result.years_parsed,
        )
        typer.echo(
            f"Registered: vintage={entry.vintage}, rows={entry.row_count}, "
            f"years={len(entry.years_included)}"
        )
    except Exception as e:
        typer.echo(f"Error registering in registry: {e}", err=True)
        raise typer.Exit(1) from e

    # Summary
    typer.echo("")
    typer.echo("PIT vintage ingestion complete:")
    typer.echo(f"  Vintage: {vintage}")
    typer.echo(f"  Years: {parse_result.years_parsed[0]}-{parse_result.years_parsed[-1]}")
    typer.echo(f"  Total records: {len(df)}")
    if parse_result.cross_state_mappings:
        typer.echo(f"  Cross-state mappings: {len(parse_result.cross_state_mappings)}")
    typer.echo(f"  Output: {output_path}")
