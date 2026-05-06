"""CLI command for ingesting decennial tract population denominators."""

from typing import Annotated

import pandas as pd
import typer

from hhplab.census.ingest.decennial_tract_population import get_output_path


def ingest_decennial_tract_population(
    decennial: Annotated[
        str,
        typer.Option(
            "--decennial",
            help="Decennial census vintage for tract denominators: 2010 or 2020.",
        ),
    ],
    tracts: Annotated[
        str | None,
        typer.Option(
            "--tracts",
            help="Target tract vintage. Defaults to the decennial vintage.",
        ),
    ] = None,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Re-ingest even if cached file exists.",
        ),
    ] = False,
    output_json: Annotated[
        bool,
        typer.Option(
            "--json",
            help="Output structured JSON instead of human-readable text.",
        ),
    ] = False,
) -> None:
    """Ingest tract-level decennial total population denominators."""
    import json

    from hhplab.census.ingest.decennial_tract_population import (
        ingest_decennial_tract_population as ingest,
    )

    output_path = get_output_path(decennial, tracts)
    if output_path.exists() and not force:
        df = pd.read_parquet(output_path)
        if output_json:
            typer.echo(
                json.dumps(
                    {
                        "status": "ok",
                        "cached": True,
                        "decennial_vintage": decennial,
                        "tract_vintage": tracts or decennial,
                        "output_path": str(output_path),
                        "total_tracts": int(len(df)),
                        "total_population": int(df["total_population"].sum()),
                    }
                )
            )
            return
        typer.echo(f"Cached file found: {output_path}")
        typer.echo(f"Rows: {len(df)}")
        typer.echo("Use --force to re-ingest.")
        return

    try:
        path = ingest(decennial, tract_vintage=tracts, force=force)
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
                    "tract_vintage": tracts or decennial,
                    "output_path": str(path),
                    "total_tracts": int(len(df)),
                    "total_population": int(df["total_population"].sum()),
                }
            )
        )
        return

    typer.echo("Ingested decennial tract population denominators.")
    typer.echo(f"Output file: {path}")
    typer.echo(f"Total tracts: {len(df):,}")
    typer.echo(f"Total population: {df['total_population'].sum():,.0f}")
