"""CLI command for ingesting BLS CPI-U annual index data."""

from __future__ import annotations

import json
from typing import Annotated

import typer


def ingest_cpi_u(
    start_year: Annotated[
        int,
        typer.Option("--start-year", help="First CPI-U annual index year to ingest."),
    ],
    end_year: Annotated[
        int,
        typer.Option("--end-year", help="Last CPI-U annual index year to ingest."),
    ],
    api_key: Annotated[
        str | None,
        typer.Option("--api-key", help="BLS registration key. Falls back to BLS_API_KEY env var."),
    ] = None,
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Emit machine-readable JSON output."),
    ] = False,
) -> None:
    """Ingest BLS CPI-U annual-average index values for inflation adjustment."""
    import pandas as pd

    from hhplab.bls import BlsQuotaExhausted
    from hhplab.bls import ingest_cpi_u as _ingest

    try:
        path = _ingest(start_year=start_year, end_year=end_year, api_key=api_key)
        df = pd.read_parquet(path)
    except BlsQuotaExhausted as exc:
        if json_output:
            typer.echo(
                json.dumps(
                    {
                        "status": "error",
                        "reason": "bls_quota_exhausted",
                        "error": str(exc),
                    }
                )
            )
        else:
            typer.echo(f"BLS quota exhausted: {exc}", err=True)
        raise typer.Exit(1) from exc
    except Exception as exc:
        if json_output:
            typer.echo(json.dumps({"status": "error", "error": str(exc)}))
        else:
            typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc

    if json_output:
        typer.echo(
            json.dumps(
                {
                    "status": "ok",
                    "output_path": str(path),
                    "start_year": int(df["year"].min()),
                    "end_year": int(df["year"].max()),
                    "rows": len(df),
                    "columns": list(df.columns),
                },
                indent=2,
            )
        )
        return

    typer.echo(f"Wrote BLS CPI-U annual index to {path}")
    typer.echo(f"Years: {int(df['year'].min())}-{int(df['year'].max())} ({len(df)} rows)")
