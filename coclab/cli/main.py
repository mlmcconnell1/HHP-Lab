"""CLI commands for CoC Lab using Typer.

Provides commands for ingesting CoC boundary data and visualizing boundaries.
"""

from pathlib import Path
from typing import Annotated

import typer

app = typer.Typer(
    name="coclab",
    help="CoC Lab - Continuum of Care boundary data infrastructure CLI",
    no_args_is_help=True,
)


@app.command()
def ingest(
    source: Annotated[
        str,
        typer.Option(
            "--source",
            "-s",
            help="Data source: 'hud_exchange' or 'hud_opendata'",
        ),
    ],
    vintage: Annotated[
        str | None,
        typer.Option(
            "--vintage",
            "-v",
            help="Boundary vintage year (e.g., '2025') for hud_exchange source",
        ),
    ] = None,
    snapshot: Annotated[
        str,
        typer.Option(
            "--snapshot",
            help="Snapshot tag for hud_opendata source (default: 'latest')",
        ),
    ] = "latest",
) -> None:
    """Ingest CoC boundary data from HUD sources.

    Examples:

        coclab ingest --source hud_exchange --vintage 2025

        coclab ingest --source hud_opendata --snapshot latest
    """
    if source == "hud_exchange":
        if vintage is None:
            typer.echo("Error: --vintage is required for hud_exchange source", err=True)
            raise typer.Exit(1)

        from coclab.ingest.hud_exchange_gis import ingest_hud_exchange

        typer.echo(f"Ingesting HUD Exchange CoC boundaries for vintage {vintage}...")
        try:
            output_path = ingest_hud_exchange(vintage)
            typer.echo(f"Successfully ingested to: {output_path}")
        except Exception as e:
            typer.echo(f"Error: {e}", err=True)
            raise typer.Exit(1) from e

    elif source == "hud_opendata":
        from coclab.ingest.hud_opendata_arcgis import ingest_hud_opendata

        typer.echo(f"Ingesting HUD Open Data CoC boundaries (snapshot: {snapshot})...")
        try:
            output_path = ingest_hud_opendata(snapshot_tag=snapshot)
            typer.echo(f"Successfully ingested to: {output_path}")
        except Exception as e:
            typer.echo(f"Error: {e}", err=True)
            raise typer.Exit(1) from e

    else:
        typer.echo(
            f"Error: Unknown source '{source}'. Use 'hud_exchange' or 'hud_opendata'.",
            err=True,
        )
        raise typer.Exit(1)


@app.command("list-vintages")
def list_vintages_cmd() -> None:
    """List all available boundary vintages in the registry."""
    from coclab.registry.registry import list_vintages

    vintages = list_vintages()

    if not vintages:
        typer.echo("No vintages registered yet.")
        return

    typer.echo("Available boundary vintages:\n")
    typer.echo(f"{'Vintage':<30} {'Source':<25} {'Features':<10} {'Ingested At'}")
    typer.echo("-" * 85)

    for entry in vintages:
        ingested_str = entry.ingested_at.strftime("%Y-%m-%d %H:%M")
        typer.echo(
            f"{entry.boundary_vintage:<30} {entry.source:<25} "
            f"{entry.feature_count:<10} {ingested_str}"
        )


@app.command()
def show(
    coc: Annotated[
        str,
        typer.Option(
            "--coc",
            "-c",
            help="CoC identifier (e.g., 'CO-500')",
        ),
    ],
    vintage: Annotated[
        str | None,
        typer.Option(
            "--vintage",
            "-v",
            help="Boundary vintage to use. If not specified, uses the latest.",
        ),
    ] = None,
    output: Annotated[
        Path | None,
        typer.Option(
            "--output",
            "-o",
            help="Output path for the HTML map file",
        ),
    ] = None,
) -> None:
    """Render an interactive map for a CoC boundary.

    Examples:

        coclab show --coc CO-500

        coclab show --coc CO-500 --vintage 2025
    """
    from coclab.viz.map_folium import render_coc_map

    vintage_display = vintage or "latest"
    typer.echo(f"Rendering map for CoC {coc} (vintage: {vintage_display})...")

    try:
        output_path = render_coc_map(coc_id=coc, vintage=vintage, out_html=output)
        typer.echo(f"Map saved to: {output_path}")
    except FileNotFoundError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1) from e
    except ValueError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1) from e


if __name__ == "__main__":
    app()
