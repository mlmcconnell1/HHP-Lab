"""CLI command for ingesting PL 94-171 block population denominators."""

from __future__ import annotations

import json
from typing import Annotated

import pandas as pd
import typer

from hhplab.sources.census.census.ingest.pl_block_population import (
    get_pl_block_population_output_path,
)


def ingest_pl_block_population(
    decennial: Annotated[
        str,
        typer.Option(
            "--decennial",
            help="Decennial PL 94-171 vintage for block denominators: 2010 or 2020.",
        ),
    ],
    blocks: Annotated[
        str | None,
        typer.Option(
            "--blocks",
            help="Target block vintage. Defaults to the decennial vintage.",
        ),
    ] = None,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Re-ingest even if cached file exists.",
        ),
    ] = False,
    api_key: Annotated[
        str | None,
        typer.Option(
            "--api-key",
            help="Census API key. Falls back to CENSUS_API_KEY env var.",
        ),
    ] = None,
    output_json: Annotated[
        bool,
        typer.Option(
            "--json",
            help="Output structured JSON instead of human-readable text.",
        ),
    ] = False,
) -> None:
    """Ingest block-level PL 94-171 total population denominators."""
    from hhplab.sources.census.census.ingest.pl_block_population import (
        ingest_pl_block_population as ingest,
    )

    output_path = get_pl_block_population_output_path(decennial, blocks)
    if output_path.exists() and not force:
        df = pd.read_parquet(output_path)
        if output_json:
            typer.echo(
                json.dumps(
                    {
                        "status": "ok",
                        "cached": True,
                        "decennial_vintage": decennial,
                        "block_vintage": blocks or decennial,
                        "output_path": str(output_path),
                        "total_blocks": int(len(df)),
                        "total_population": int(df["total_population"].sum()),
                    }
                )
            )
            return
        typer.echo(f"Cached file found: {output_path}")
        typer.echo(f"Rows: {len(df):,}")
        typer.echo("Use --force to re-ingest.")
        return

    try:
        path = ingest(decennial, block_vintage=blocks, force=force, api_key=api_key)
    except Exception as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1) from exc

    df = pd.read_parquet(path)
    if output_json:
        typer.echo(
            json.dumps(
                {
                    "status": "ok",
                    "cached": False,
                    "decennial_vintage": decennial,
                    "block_vintage": blocks or decennial,
                    "output_path": str(path),
                    "total_blocks": int(len(df)),
                    "total_population": int(df["total_population"].sum()),
                }
            )
        )
        return

    typer.echo("Ingested PL 94-171 block population denominators.")
    typer.echo(f"Output file: {path}")
    typer.echo(f"Total blocks: {len(df):,}")
    typer.echo(f"Total population: {df['total_population'].sum():,.0f}")
