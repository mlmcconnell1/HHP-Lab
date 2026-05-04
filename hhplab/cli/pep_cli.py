"""CLI commands for PEP (Population Estimates Program) ingestion.

Provides Typer commands for:
- ingest pep: Download and normalize PEP county population data

Aggregation of PEP data to CoC geography is handled by ``hhplab aggregate pep``.
"""

from pathlib import Path
from typing import Annotated

import httpx
import typer

from hhplab.paths import curated_dir, raw_root


def ingest_pep(
    series: Annotated[
        str,
        typer.Option(
            "--series",
            "-s",
            help="Series to ingest: 'auto' or 'postcensal'.",
        ),
    ] = "auto",
    vintage: Annotated[
        str | None,
        typer.Option(
            "--vintage",
            "-v",
            help="Postcensal vintage year. Defaults to latest.",
        ),
    ] = None,
    url: Annotated[
        str | None,
        typer.Option(
            "--url",
            help="Override download URL (single vintage only).",
        ),
    ] = None,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            "-f",
            help="Re-download and reprocess even if cached.",
        ),
    ] = False,
    output_dir: Annotated[
        Path | None,
        typer.Option(
            "--output-dir",
            "-o",
            help="Output directory for curated parquet.",
        ),
    ] = None,
    raw_dir: Annotated[
        Path | None,
        typer.Option(
            "--raw-dir",
            help="Directory for raw downloads.",
        ),
    ] = None,
    start: Annotated[
        int | None,
        typer.Option(
            "--start",
            help="First year to include (YYYY). Defaults to earliest in data.",
        ),
    ] = None,
    end: Annotated[
        int | None,
        typer.Option(
            "--end",
            help="Last year to include (YYYY). Defaults to latest in data.",
        ),
    ] = None,
) -> None:
    """Download and normalize PEP county population estimates from Census Bureau.

    Ingests Census Bureau Population Estimates Program (PEP) county-level
    annual population estimates. Supports postcensal estimates by vintage.

    Population estimates are as of July 1 of each year.

    Exit codes:
    - 0: Success
    - 2: Validation/parse error
    - 3: Download error

    Examples:

        hhplab ingest pep --series auto

        hhplab ingest pep --series postcensal --vintage 2024

        hhplab ingest pep --series postcensal --vintage 2024 --start 2015 --end 2020
    """
    if output_dir is None:
        output_dir = curated_dir("pep")
    if raw_dir is None:
        raw_dir = raw_root() / "pep"

    from hhplab.pep.pep_ingest import (
        AUTO_SERIES,
        PEP_URLS,
        POSTCENSAL_SERIES,
        get_output_path,
        ingest_pep_county,
    )
    from hhplab.provenance import read_provenance

    if start is not None and end is not None and start > end:
        typer.echo("Error: --start must be <= --end.", err=True)
        raise typer.Exit(2)

    if series not in {AUTO_SERIES, POSTCENSAL_SERIES}:
        typer.echo(
            f"Error: Invalid series '{series}'. "
            f"Expected one of: {AUTO_SERIES}, {POSTCENSAL_SERIES}.",
            err=True,
        )
        raise typer.Exit(2)

    parsed_vintage: int | None = None
    if vintage is None:
        parsed_vintage = max(PEP_URLS.keys())
    else:
        try:
            parsed_vintage = int(vintage)
        except ValueError as exc:
            typer.echo(
                f"Error: Invalid vintage '{vintage}'. Must be a year (e.g., 2024).",
                err=True,
            )
            raise typer.Exit(2) from exc

    output_path = get_output_path(parsed_vintage, output_dir, start_year=start, end_year=end)

    # Check for existing output
    if output_path.exists() and not force:
        typer.echo(f"PEP county data already exists at: {output_path}")
        typer.echo("Use --force to re-download and reprocess.")
        raise typer.Exit(0)

    typer.echo(
        "Ingesting PEP county population estimates "
        f"(series: {series}, vintage: {parsed_vintage or 'n/a'})..."
    )

    try:
        result_path = ingest_pep_county(
            series=series,
            vintage=parsed_vintage,
            url=url,
            force=force,
            output_dir=output_dir,
            raw_dir=raw_dir,
            start_year=start,
            end_year=end,
        )

        # Report results
        import pandas as pd

        df = pd.read_parquet(result_path)
        county_count = df["county_fips"].nunique()
        year_range = f"{df['year'].min()}-{df['year'].max()}"

        provenance = read_provenance(result_path)
        series_note = None
        if provenance and provenance.extra:
            series_note = provenance.extra.get("series")
            if series_note == "postcensal" and provenance.extra.get("vintage") is not None:
                series_note = f"postcensal (vintage {provenance.extra['vintage']})"

        typer.echo(f"Successfully ingested PEP data to: {result_path}")
        typer.echo(f"  Counties: {county_count}")
        typer.echo(f"  Years: {year_range}")
        typer.echo(f"  Records: {len(df):,}")
        if series_note:
            typer.echo(f"  Series used: {series_note}")

    except httpx.HTTPStatusError as e:
        typer.echo(f"Error: Download failed: {e}", err=True)
        raise typer.Exit(3) from e

    except ValueError as e:
        typer.echo(f"Error: Validation failed: {e}", err=True)
        raise typer.Exit(2) from e

    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1) from e
