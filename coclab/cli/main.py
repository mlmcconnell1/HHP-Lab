"""CLI commands for CoC Lab using Typer.

Provides commands for ingesting CoC boundary data, building crosswalks,
computing measures, and visualizing boundaries.
"""

from pathlib import Path
from typing import Annotated

import typer

from coclab.cli.build_measures import build_measures
from coclab.cli.build_panel import build_panel_cmd
from coclab.cli.build_xwalks import build_xwalks
from coclab.cli.compare_vintages import compare_vintages
from coclab.cli.crosscheck_acs_population import crosscheck_acs_population
from coclab.cli.crosscheck_pit_vintages import crosscheck_pit_vintages
from coclab.cli.diagnostics import diagnostics
from coclab.cli.export_bundle import export_bundle
from coclab.cli.ingest_acs_population import ingest_acs_population
from coclab.cli.ingest_census import ingest_census
from coclab.cli.ingest_pit import ingest_pit
from coclab.cli.ingest_pit_vintage import ingest_pit_vintage
from coclab.cli.list_measures import list_measures
from coclab.cli.list_xwalks import list_xwalks
from coclab.cli.panel_diagnostics import panel_diagnostics
from coclab.cli.rollup_acs_population import rollup_acs_population
from coclab.cli.show_measures import show_measures
from coclab.cli.verify_acs_population import verify_acs_population
from coclab.cli.zori import aggregate_zori, ingest_zori, zori_diagnostics


def _check_working_directory() -> None:
    """Warn if the current directory doesn't look like the CoC-PIT project root."""
    cwd = Path.cwd()
    expected_markers = [
        cwd / "pyproject.toml",
        cwd / "coclab",
        cwd / "data",
    ]
    missing = [p for p in expected_markers if not p.exists()]

    if missing:
        missing_names = ", ".join(p.name for p in missing)
        typer.echo(
            f"Warning: Current directory may not be the CoC-PIT project root. "
            f"Missing: {missing_names}",
            err=True,
        )


app = typer.Typer(
    name="coclab",
    help="CoC Lab - Continuum of Care boundary data infrastructure CLI",
    no_args_is_help=True,
)


@app.callback()
def main_callback() -> None:
    """Check working directory before running any command."""
    _check_working_directory()


# Register crosswalk, measures, and diagnostics commands
app.command("build-xwalks")(build_xwalks)
app.command("build-measures")(build_measures)
app.command("build-panel")(build_panel_cmd)
app.command("compare-vintages")(compare_vintages)
app.command("crosscheck-acs-population")(crosscheck_acs_population)
app.command("crosscheck-pit-vintages")(crosscheck_pit_vintages)
app.command("xwalk-diagnostics")(diagnostics)
app.command("ingest-acs-population")(ingest_acs_population)
app.command("ingest-census")(ingest_census)
app.command("ingest-pit")(ingest_pit)
app.command("ingest-pit-vintage")(ingest_pit_vintage)
app.command("list-measures")(list_measures)
app.command("list-xwalks")(list_xwalks)
app.command("panel-diagnostics")(panel_diagnostics)
app.command("rollup-acs-population")(rollup_acs_population)
app.command("show-measures")(show_measures)
app.command("verify-acs-population")(verify_acs_population)

# ZORI commands (spec section 3)
app.command("ingest-zori")(ingest_zori)
app.command("aggregate-zori")(aggregate_zori)
app.command("zori-diagnostics")(zori_diagnostics)

# Export bundle command
app.command("export-bundle")(export_bundle)


@app.command("source-status")
def source_status(
    source_type: Annotated[
        str | None,
        typer.Option(
            "--type",
            "-t",
            help="Filter to specific source type (zori, boundary, census_tract, etc.)",
        ),
    ] = None,
    check_changes: Annotated[
        bool,
        typer.Option(
            "--check-changes",
            "-c",
            help="Highlight sources that have changed over time",
        ),
    ] = False,
) -> None:
    """Show status of tracked external data sources.

    Displays all registered data source ingestions with their hashes,
    timestamps, and change detection information.

    Examples:

        coclab source-status

        coclab source-status --type zori

        coclab source-status --check-changes
    """
    from coclab.source_registry import (
        detect_upstream_changes,
        summarize_registry,
    )

    if check_changes:
        changes = detect_upstream_changes()
        if changes.empty:
            typer.echo("No upstream changes detected. All sources have consistent hashes.")
        else:
            typer.echo("⚠️  UPSTREAM DATA CHANGES DETECTED:\n")
            for _, row in changes.iterrows():
                typer.echo(f"  {row['source_type']}: {row['source_url'][:60]}...")
                typer.echo(f"    Versions seen: {row['hash_count']}")
                typer.echo(f"    First: {row['first_seen']} (hash: {row['first_hash'][:12]}...)")
                typer.echo(f"    Last:  {row['last_seen']} (hash: {row['last_hash'][:12]}...)")
                typer.echo("")
        return

    # Show full summary
    summary = summarize_registry()
    typer.echo(summary)


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
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            "-f",
            help="Force re-ingest even if vintage already exists",
        ),
    ] = False,
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

        from coclab.geo.io import curated_boundary_path
        from coclab.ingest.hud_exchange_gis import ingest_hud_exchange
        from coclab.registry.registry import list_vintages

        output_path = curated_boundary_path(vintage)
        registered_vintages = [v.boundary_vintage for v in list_vintages()]
        file_exists = output_path.exists()
        in_registry = vintage in registered_vintages

        if file_exists and in_registry and not force:
            typer.echo(f"Vintage {vintage} already exists at: {output_path}")
            typer.echo("Use --force to re-ingest.")
            raise typer.Exit(0)
        if file_exists and not in_registry and not force:
            typer.echo(
                f"Warning: File exists at {output_path} but not in registry.",
                err=True,
            )
            typer.echo("Re-ingesting to ensure proper registration...")
        if not file_exists and in_registry:
            typer.echo(
                f"Warning: Vintage {vintage} is in registry but file is missing.",
                err=True,
            )
            typer.echo("Re-ingesting...")

        typer.echo(f"Ingesting HUD Exchange CoC boundaries for vintage {vintage}...")
        try:
            output_path = ingest_hud_exchange(vintage, show_progress=True)
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
