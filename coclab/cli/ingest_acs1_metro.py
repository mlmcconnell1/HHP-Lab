"""CLI command for ingesting ACS 1-year metro-native unemployment data."""

from __future__ import annotations

import json
from typing import Annotated

import typer


def ingest_acs1_metro(
    vintage: Annotated[
        int,
        typer.Option(
            "--vintage",
            "-v",
            help="ACS 1-year vintage year to fetch (e.g., 2023).",
        ),
    ],
    definition_version: Annotated[
        str,
        typer.Option(
            "--definition-version",
            "-d",
            help="Metro definition version.",
        ),
    ] = "glynn_fox_v1",
    api_key: Annotated[
        str | None,
        typer.Option(
            "--api-key",
            help="Census API key. Falls back to CENSUS_API_KEY env var.",
        ),
    ] = None,
    json_output: Annotated[
        bool,
        typer.Option(
            "--json",
            help="Emit machine-readable JSON output.",
        ),
    ] = False,
) -> None:
    """Ingest ACS 1-year unemployment data at CBSA geography for metros.

    Fetches Table B23025 (Employment Status for Population 16+) from the
    Census Bureau API at metropolitan/micropolitan statistical area geography,
    maps CBSAs to Glynn/Fox metro IDs, computes unemployment rates, and writes
    a curated Parquet file.

    ACS 1-year data is available only for geographies with population >= 65,000.
    All 25 Glynn/Fox metros meet this threshold.

    Examples:

        coclab ingest acs1-metro --vintage 2023

        coclab ingest acs1-metro --vintage 2022 --json

        coclab ingest acs1-metro --vintage 2023 --api-key YOUR_KEY
    """
    import pandas as pd

    from coclab.acs.ingest.metro_acs1 import ingest_metro_acs1

    if not json_output:
        typer.echo("Ingesting ACS 1-year metro unemployment data...")
        typer.echo(f"  Vintage:    {vintage}")
        typer.echo(f"  Definition: {definition_version}")
        typer.echo("  Table:      B23025 (Employment Status)")
        typer.echo("")

    try:
        path = ingest_metro_acs1(
            vintage=vintage,
            definition_version=definition_version,
            api_key=api_key,
        )
    except Exception as e:
        if json_output:
            typer.echo(json.dumps({"status": "error", "error": str(e)}))
        else:
            typer.echo(f"Error: {e}", err=True)
            typer.echo(
                "Verify that ACS 1-year data is available for the requested vintage. "
                "ACS 1-year estimates are typically released ~1 year after the survey year.",
                err=True,
            )
        raise typer.Exit(1) from e

    # Load and summarize results
    df = pd.read_parquet(path)

    if json_output:
        result = {
            "status": "ok",
            "output_path": str(path),
            "vintage": vintage,
            "definition_version": definition_version,
            "metros": len(df),
            "columns": list(df.columns),
        }
        if "unemployment_rate_acs1" in df.columns:
            rates = df["unemployment_rate_acs1"].dropna()
            has_rates = len(rates) > 0
            result["unemployment_rate_mean"] = (
                round(float(rates.mean()), 6) if has_rates else None
            )
            result["unemployment_rate_min"] = (
                round(float(rates.min()), 6) if has_rates else None
            )
            result["unemployment_rate_max"] = (
                round(float(rates.max()), 6) if has_rates else None
            )
        typer.echo(json.dumps(result, indent=2))
    else:
        typer.echo("=" * 60)
        typer.echo("INGEST SUMMARY")
        typer.echo("=" * 60)
        typer.echo(f"Output file:   {path}")
        typer.echo(f"Metros:        {len(df)}")

        if "civilian_labor_force" in df.columns:
            typer.echo(f"Labor force:   {df['civilian_labor_force'].sum():,.0f}")
        if "unemployed_count" in df.columns:
            typer.echo(f"Unemployed:    {df['unemployed_count'].sum():,.0f}")
        if "unemployment_rate_acs1" in df.columns:
            rates = df["unemployment_rate_acs1"].dropna()
            if len(rates) > 0:
                typer.echo(f"Unemp rate:    {rates.mean():.1%} (mean)")
                typer.echo(f"               {rates.min():.1%} - {rates.max():.1%} (range)")
        typer.echo("")

        # Show per-metro summary
        typer.echo(f"{'Metro ID':<10} {'Name':<45} {'Unemp Rate'}")
        typer.echo("-" * 70)
        for _, row in df.iterrows():
            rate_str = (
                f"{row['unemployment_rate_acs1']:.1%}"
                if pd.notna(row.get("unemployment_rate_acs1"))
                else "N/A"
            )
            name = str(row.get("metro_name", ""))[:43]
            typer.echo(f"{row['metro_id']:<10} {name:<45} {rate_str}")

        typer.echo("")
        typer.echo("=" * 60)
        typer.echo("Ingest complete!")
