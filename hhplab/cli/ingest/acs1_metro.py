"""CLI command for ingesting ACS 1-year metro-native detailed-table data."""

from __future__ import annotations

from typing import Annotated

import typer

from hhplab.cli.shared.output import JsonOutput, cli_error, emit_result
from hhplab.metro.metro_definitions import CANONICAL_UNIVERSE_DEFINITION_VERSION


def _acs1_command_help(*, geography: str) -> str:
    from hhplab.acs.variables_acs1 import ACS1_TABLES, acs1_measure_names

    tables = ", ".join(ACS1_TABLES)
    registry_measures = acs1_measure_names()
    featured_measures = [
        measure
        for measure in (
            "pop_16_plus",
            "civilian_labor_force",
            "unemployed_count",
            "median_gross_rent",
            "median_contract_rent",
            "gross_rent_distribution_total",
            "contract_rent_distribution_total",
            "gross_rent_pct_income_total",
            "owner_costs_pct_income_total",
            "rent_burden_40_plus",
            "rent_burden_50_plus",
            "unemployment_rate_acs1",
        )
        if measure in registry_measures
    ]
    measures = ", ".join(featured_measures)
    return f"""Ingest ACS 1-year detailed-table data at {geography} geography.

Fetches the ACS 1-year table set declared in hhplab.acs.variables_acs1:

    {tables}

Output measure columns are derived from that registry and include:

    {measures}

Availability is vintage-specific; JSON output reports the exact tables
available for the requested vintage and the full supported measure list.
"""


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
    ] = CANONICAL_UNIVERSE_DEFINITION_VERSION,
    api_key: Annotated[
        str | None,
        typer.Option(
            "--api-key",
            help="Census API key. Falls back to CENSUS_API_KEY env var.",
        ),
    ] = None,
    json_output: JsonOutput = False,
) -> None:
    """Ingest ACS 1-year detailed-table data at CBSA geography for metros."""
    import pandas as pd

    from hhplab.acs.ingest.metro_acs1 import ingest_metro_acs1
    from hhplab.acs.variables_acs1 import (
        acs1_measure_names,
        acs1_tables_for_vintage,
        acs1_unavailable_tables_for_vintage,
    )

    if not json_output:
        typer.echo("Ingesting ACS 1-year metro data...")
        typer.echo(f"  Vintage:    {vintage}")
        typer.echo(f"  Definition: {definition_version}")
        typer.echo("  Product:    ACS 1-year detailed tables")
        typer.echo("")

    try:
        path = ingest_metro_acs1(
            vintage=vintage,
            definition_version=definition_version,
            api_key=api_key,
        )
    except Exception as e:
        if not json_output:
            typer.echo(
                "Verify that ACS 1-year data is available for the requested vintage. "
                "ACS 1-year estimates are typically released ~1 year after the survey year.",
                err=True,
            )
        cli_error(e, json_output)

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
            "supported_acs_tables": acs1_tables_for_vintage(vintage),
            "unavailable_acs_tables": acs1_unavailable_tables_for_vintage(vintage),
            "supported_measures": acs1_measure_names(),
        }
        if "unemployment_rate_acs1" in df.columns:
            rates = df["unemployment_rate_acs1"].dropna()
            has_rates = len(rates) > 0
            result["unemployment_rate_mean"] = round(float(rates.mean()), 6) if has_rates else None
            result["unemployment_rate_min"] = round(float(rates.min()), 6) if has_rates else None
            result["unemployment_rate_max"] = round(float(rates.max()), 6) if has_rates else None
        emit_result(result, json_output, indent=2)
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


ingest_acs1_metro.__doc__ = _acs1_command_help(geography="CBSA metro")
