"""CLI command for ingesting all years from a PIT vintage file."""

import logging
from pathlib import Path
from typing import Annotated

import typer

from hhplab.paths import raw_root
from hhplab.pit.ingest import get_vintage_output_path

# Configure logging to show INFO messages from PIT parser
logging.basicConfig(
    format="%(message)s",
    level=logging.WARNING,
)
# Show INFO for PIT ingest to see CoC ID mapping messages
logging.getLogger("hhplab.pit.ingest.parser").setLevel(logging.INFO)


def _find_existing_pit_vintage_file(
    raw_dir: Path,
    vintage: int,
    source_urls: list[str],
) -> tuple[Path, str] | None:
    """Find a manually placed PIT vintage workbook in known HUD filename variants."""
    by_filename = {url.split("/")[-1]: url for url in source_urls}
    candidates = [raw_dir / filename for filename in by_filename]
    candidates.extend(sorted(raw_dir.glob(f"2007-{vintage}-*.xls*")))

    seen: set[Path] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        if candidate.exists() and candidate.stat().st_size > 0:
            return candidate, by_filename.get(candidate.name, source_urls[0])
    return None


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

        hhplab ingest pit-vintage --vintage 2024

        hhplab ingest pit-vintage --vintage 2024 --force

        hhplab ingest pit-vintage --vintage 2024 --parse-only
    """
    from hhplab.pit.ingest import (
        download_pit_data,
        get_pit_source_url,
        pit_source_url_candidates,
        parse_pit_vintage,
        write_pit_parquet,
    )
    from hhplab.pit.pit_registry import register_pit_vintage

    typer.echo(f"Ingesting PIT vintage {vintage} (all years)...")

    # Step 1: Download PIT data
    raw_dir = raw_root() / "pit" / str(vintage)
    try:
        source_url = get_pit_source_url(vintage)
    except ValueError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1) from e
    source_urls = pit_source_url_candidates(vintage)
    existing = _find_existing_pit_vintage_file(raw_dir, vintage, source_urls)

    if parse_only and existing is not None:
        raw_file, source_url = existing
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

    # Step 5: Run QA validation (per-year)
    from hhplab.pit.qa import validate_pit_data

    typer.echo("Running QA validation...")
    try:
        qa_report = validate_pit_data(df)
        if qa_report.passed:
            typer.echo("QA passed: no errors found")
        else:
            typer.echo(
                f"QA result: {len(qa_report.errors)} error(s), {len(qa_report.warnings)} warning(s)"
            )
            if qa_report.issues:
                typer.echo("")
                typer.echo("QA Issues:")
                for issue in qa_report.issues[:10]:
                    typer.echo(f"  {issue}")
                if len(qa_report.issues) > 10:
                    typer.echo(f"  ... and {len(qa_report.issues) - 10} more issues")
    except Exception as e:
        typer.echo(f"Warning: QA validation failed: {e}", err=True)

    # Summary
    typer.echo("")
    typer.echo("PIT vintage ingestion complete:")
    typer.echo(f"  Vintage: {vintage}")
    typer.echo(f"  Years parsed: {parse_result.years_parsed}")
    if parse_result.years_failed:
        typer.echo(f"  Years FAILED: {parse_result.years_failed}")
    typer.echo(f"  Total records: {len(df)}")
    if parse_result.cross_state_mappings:
        typer.echo(f"  Cross-state mappings: {len(parse_result.cross_state_mappings)}")
    typer.echo(f"  Output: {output_path}")
