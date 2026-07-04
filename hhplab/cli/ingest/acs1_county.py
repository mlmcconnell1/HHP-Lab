"""CLI command for ingesting ACS 1-year county-native detailed-table data."""

from __future__ import annotations

from typing import Annotated

import typer

from hhplab.cli.shared.output import JsonOutput, cli_error, emit_result


def _acs1_county_command_help() -> str:
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
            "unemployment_rate_acs1",
        )
        if measure in registry_measures
    ]
    measures = ", ".join(featured_measures)
    return f"""Ingest ACS 1-year detailed-table data at county geography.

Fetches the ACS 1-year table set declared in hhplab.acs.variables_acs1:

    {tables}

Output measure columns are derived from that registry and include:

    {measures}

Availability is vintage-specific; JSON output reports the exact tables
available for the requested vintage and the full supported measure list.
"""


def ingest_acs1_county(
    vintage: Annotated[
        int,
        typer.Option(
            "--vintage",
            "-v",
            help="ACS 1-year vintage year to fetch (e.g., 2023).",
        ),
    ],
    api_key: Annotated[
        str | None,
        typer.Option(
            "--api-key",
            help="Census API key. Falls back to CENSUS_API_KEY env var.",
        ),
    ] = None,
    json_output: JsonOutput = False,
) -> None:
    """Ingest ACS 1-year detailed-table data at county geography."""
    import pandas as pd

    from hhplab.acs.ingest.county_acs1 import ingest_county_acs1
    from hhplab.acs.variables_acs1 import (
        acs1_measure_names,
        acs1_tables_for_vintage,
        acs1_unavailable_tables_for_vintage,
    )

    if not json_output:
        typer.echo("Ingesting ACS 1-year county data...")
        typer.echo(f"  Vintage: {vintage}")
        typer.echo("  Product: ACS 1-year detailed tables")
        typer.echo("")

    try:
        path = ingest_county_acs1(vintage=vintage, api_key=api_key)
    except Exception as e:
        if not json_output:
            typer.echo(
                "Verify that ACS 1-year data is available for the requested vintage. "
                "County ACS1 only includes counties that meet Census publication "
                "thresholds; non-threshold counties are not returned.",
                err=True,
            )
        cli_error(e, json_output)

    df = pd.read_parquet(path)

    if json_output:
        result = {
            "status": "ok",
            "output_path": str(path),
            "vintage": vintage,
            "counties": len(df),
            "row_count": len(df),
            "columns": list(df.columns),
            "supported_acs_tables": acs1_tables_for_vintage(vintage),
            "unavailable_acs_tables": acs1_unavailable_tables_for_vintage(vintage),
            "supported_measures": acs1_measure_names(),
        }
        if "unemployment_rate_acs1" in df.columns:
            rates = df["unemployment_rate_acs1"].dropna()
            has_rates = len(rates) > 0
            result["unemployment_summary"] = {
                "mean": round(float(rates.mean()), 6) if has_rates else None,
                "min": round(float(rates.min()), 6) if has_rates else None,
                "max": round(float(rates.max()), 6) if has_rates else None,
            }
        emit_result(result, json_output, indent=2)
        return

    typer.echo("=" * 60)
    typer.echo("INGEST SUMMARY")
    typer.echo("=" * 60)
    typer.echo(f"Output file: {path}")
    typer.echo(f"Counties:    {len(df)}")
    if "unemployment_rate_acs1" in df.columns:
        rates = df["unemployment_rate_acs1"].dropna()
        if len(rates) > 0:
            typer.echo(f"Unemp rate: {rates.mean():.1%} (mean)")
            typer.echo(f"            {rates.min():.1%} - {rates.max():.1%} (range)")
    typer.echo("")
    typer.echo("Ingest complete!")


ingest_acs1_county.__doc__ = _acs1_county_command_help()
