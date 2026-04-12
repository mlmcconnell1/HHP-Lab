"""CLI command for ingesting BLS LAUS yearly metro labor-market data."""

from __future__ import annotations

import json
from typing import Annotated

import typer


def ingest_laus_metro(
    year: Annotated[
        int | None,
        typer.Option(
            "--year",
            "-y",
            help="Reference year for annual-average LAUS data (e.g., 2023).",
        ),
    ] = None,
    start_year: Annotated[
        int | None,
        typer.Option(
            "--start-year",
            help="First year of a backfill range (inclusive). Use with --end-year.",
        ),
    ] = None,
    end_year: Annotated[
        int | None,
        typer.Option(
            "--end-year",
            help="Last year of a backfill range (inclusive). Use with --start-year.",
        ),
    ] = None,
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
            help="BLS registration key. Falls back to BLS_API_KEY env var.",
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
    """Ingest BLS LAUS annual-average labor-market data for Glynn/Fox metros.

    Fetches Local Area Unemployment Statistics (LAUS) from the BLS Public
    API v2 for the 25 Glynn/Fox metropolitan areas, writes a curated
    Parquet file with unemployment rate, unemployed count, employed count,
    and civilian labor force for the requested year(s).

    BLS LAUS annual averages are typically released in February or March
    following the reference year.

    Specify a single year with --year, or a backfill range with
    --start-year and --end-year.

    Examples:

        coclab ingest laus-metro --year 2023

        coclab ingest laus-metro --start-year 2015 --end-year 2023

        coclab ingest laus-metro --year 2022 --json

        coclab ingest laus-metro --year 2023 --api-key YOUR_KEY
    """
    import pandas as pd

    from coclab.ingest.bls_laus import BlsQuotaExhausted
    from coclab.ingest.bls_laus import ingest_laus_metro as _ingest

    # Resolve years to process
    if year is not None and (start_year is not None or end_year is not None):
        raise typer.BadParameter(
            "--year cannot be combined with --start-year/--end-year"
        )
    if year is not None:
        years = [year]
    elif start_year is not None and end_year is not None:
        if start_year > end_year:
            raise typer.BadParameter("--start-year must be <= --end-year")
        years = list(range(start_year, end_year + 1))
    elif start_year is not None or end_year is not None:
        raise typer.BadParameter("--start-year and --end-year must be used together")
    else:
        raise typer.BadParameter(
            "Specify --year for a single year, or --start-year/--end-year for a range."
        )

    if not json_output:
        if len(years) == 1:
            typer.echo("Ingesting BLS LAUS metro labor-market data...")
            typer.echo(f"  Year:       {years[0]}")
        else:
            typer.echo("Ingesting BLS LAUS metro labor-market data (backfill)...")
            typer.echo(f"  Years:      {years[0]}-{years[-1]} ({len(years)} years)")
        typer.echo(f"  Definition: {definition_version}")
        typer.echo("  Measures:   unemployment_rate, unemployed, employed, labor_force")
        typer.echo("")

    results = []
    errors = []
    quota_exhausted = False

    for y in years:
        try:
            path = _ingest(
                year=y,
                definition_version=definition_version,
                api_key=api_key,
            )
            df = pd.read_parquet(path)
            results.append({"year": y, "path": str(path), "metros": len(df), "df": df})
            if not json_output and len(years) > 1:
                typer.echo(f"  ✓ {y}: {len(df)} metros → {path.name}")
        except BlsQuotaExhausted as e:
            # BLS daily threshold hit — there is no point continuing the
            # remaining years in a backfill loop, since they would all fail
            # with the same condition.  Record the error for every remaining
            # year and break out so the user gets a single actionable message.
            quota_exhausted = True
            errors.append({"year": y, "error": str(e), "reason": "bls_quota_exhausted"})
            if not json_output:
                typer.echo(f"  ✗ {y}: BLS quota exhausted — {e}", err=True)
            for remaining in years[years.index(y) + 1 :]:
                errors.append({
                    "year": remaining,
                    "error": "skipped: BLS quota already exhausted",
                    "reason": "bls_quota_exhausted",
                })
            if len(years) == 1:
                if json_output:
                    typer.echo(json.dumps({
                        "status": "error",
                        "year": y,
                        "error": str(e),
                        "reason": "bls_quota_exhausted",
                    }))
                raise typer.Exit(1) from e
            break
        except Exception as e:
            errors.append({"year": y, "error": str(e)})
            if not json_output:
                typer.echo(f"  ✗ {y}: {e}", err=True)
            if len(years) == 1:
                if json_output:
                    typer.echo(json.dumps({"status": "error", "year": y, "error": str(e)}))
                else:
                    typer.echo(
                        "Verify that BLS LAUS annual-average data is available for the "
                        "requested year. Annual averages are typically released in "
                        "February or March of the following year.",
                        err=True,
                    )
                raise typer.Exit(1) from e

    if json_output:
        if len(years) == 1 and results:
            df = results[0]["df"]
            result = {
                "status": "ok",
                "output_path": results[0]["path"],
                "year": years[0],
                "definition_version": definition_version,
                "metros": len(df),
                "columns": list(df.columns),
            }
            if "unemployment_rate" in df.columns:
                rates = df["unemployment_rate"].dropna()
                has_rates = len(rates) > 0
                result["unemployment_rate_mean"] = (
                    round(float(rates.mean()), 4) if has_rates else None
                )
                result["unemployment_rate_min"] = (
                    round(float(rates.min()), 4) if has_rates else None
                )
                result["unemployment_rate_max"] = (
                    round(float(rates.max()), 4) if has_rates else None
                )
            if "labor_force" in df.columns:
                result["labor_force_total"] = int(df["labor_force"].sum())
            typer.echo(json.dumps(result, indent=2))
        else:
            if not results:
                status = "error"
            elif errors:
                status = "partial"
            else:
                status = "ok"
            payload: dict = {
                "status": status,
                "years_requested": years,
                "years_succeeded": [r["year"] for r in results],
                "years_failed": [e["year"] for e in errors],
                "outputs": [
                    {"year": r["year"], "path": r["path"], "metros": r["metros"]}
                    for r in results
                ],
                "errors": errors,
            }
            if quota_exhausted:
                payload["reason"] = "bls_quota_exhausted"
            typer.echo(json.dumps(payload, indent=2))
        if errors:
            raise typer.Exit(1)
        return

    if not results:
        typer.echo("No data was successfully ingested.", err=True)
        if quota_exhausted:
            typer.echo(
                "BLS quota exhausted: register for a BLS API key and re-run "
                "with --api-key <KEY> (or set BLS_API_KEY), or wait for the "
                "BLS daily threshold to reset (midnight US Eastern time).",
                err=True,
            )
        raise typer.Exit(1)

    # Human-readable summary for single-year case
    if len(years) == 1 and results:
        df = results[0]["df"]
        path_str = results[0]["path"]
        typer.echo("=" * 60)
        typer.echo("INGEST SUMMARY")
        typer.echo("=" * 60)
        typer.echo(f"Output file:   {path_str}")
        typer.echo(f"Metros:        {len(df)}")

        if "labor_force" in df.columns:
            typer.echo(f"Labor force:   {df['labor_force'].sum():,.0f}")
        if "unemployed" in df.columns:
            typer.echo(f"Unemployed:    {df['unemployed'].sum():,.0f}")
        if "unemployment_rate" in df.columns:
            rates = df["unemployment_rate"].dropna()
            if len(rates) > 0:
                typer.echo(f"Unemp rate:    {rates.mean():.2f}% (mean)")
                typer.echo(f"               {rates.min():.2f}% - {rates.max():.2f}% (range)")
        typer.echo("")

        typer.echo(f"{'Metro ID':<10} {'Name':<45} {'Unemp Rate'}")
        typer.echo("-" * 70)
        for _, row in df.iterrows():
            rate_str = (
                f"{row['unemployment_rate']:.1f}%"
                if pd.notna(row.get("unemployment_rate"))
                else "N/A"
            )
            name = str(row.get("metro_name", ""))[:43]
            typer.echo(f"{row['metro_id']:<10} {name:<45} {rate_str}")

        typer.echo("")
        typer.echo("=" * 60)
        typer.echo("Ingest complete!")
    else:
        typer.echo("")
        typer.echo(f"Backfill complete: {len(results)}/{len(years)} years ingested.")
        if errors:
            typer.echo(f"Failed years: {[e['year'] for e in errors]}", err=True)
            if quota_exhausted:
                typer.echo(
                    "BLS quota exhausted: register for a BLS API key and re-run "
                    "with --api-key <KEY> (or set BLS_API_KEY), or wait for the "
                    "BLS daily threshold to reset (midnight US Eastern time).",
                    err=True,
                )
            raise typer.Exit(1)
